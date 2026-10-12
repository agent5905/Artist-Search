-- Artist Search schema. Every statement is idempotent, so the file is safe to rerun.
--
-- Hard rule 1: no column holds image bytes. There is no bytea anywhere, every URL (the URL columns
-- and each entry of artists.links) must pass clean_url, which a data: URL cannot, and every text
-- column and list entry is too short to carry an image as text (the *_sizes constraints below).
-- Hard rule 7: a handle and a site have exactly one spelling here. Anything else is refused at
-- write time, so a removed artist cannot come back under a variant the removal keys do not match.
-- Row level security is on for every table with no policies: the public role reads only through
-- the functions in 002_functions.sql.

create extension if not exists vector with schema extensions;
create extension if not exists pg_trgm with schema extensions; -- for misspelled search words

-- A strict http(s) URL, at most 2,048 characters:
--   * the host is a public DNS name in its one canonical spelling: lowercase ASCII (punycode for
--     other scripts), no user, no trailing dot, no IP address. The same rule as pipeline/store.py.
--   * the rest is URL characters only (RFC 3986): no spaces, control or non-ASCII characters.
--     Encode those as %XX before storing.
--   * no fragment. It never reaches the server, so it is only a place to hide data.
-- A DeviantArt deviation page and its signed thumbnail URL (a long token in the query) both pass.
create or replace function public.clean_url(p_url text) returns boolean
language sql immutable set search_path = ''
as $fn$
  select length(p_url) <= 2048 and p_url ~ (
    '^https?://(?=[^/?:]{4,253}([/?:]|$))'
    || '([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+([a-z]{2,63}|xn--[a-z0-9-]{1,59})'
    || '(:[0-9]{1,5})?'
    || '([/?][A-Za-z0-9._~:/?@!$&''()*+,;=%-]*)?$')
$fn$;

-- artists.links: a list of at most 50 clean URLs.
create or replace function public.clean_links(p_links jsonb) returns boolean
language sql immutable set search_path = ''
as $$
  select case when jsonb_typeof(p_links) = 'array' then
    jsonb_array_length(p_links) <= 50 and not exists (
      select 1 from jsonb_array_elements(p_links) link
      where jsonb_typeof(link) <> 'string' or public.clean_url(link #>> '{}') is not true)
  else false end
$$;

-- The host a site is known by: www.example.com and example.com are one site.
-- Null for deviantart.com, which is never an artist's own site (the same rule as pipeline/store.py),
-- so one removal can never put the host every profile links to on the removal list.
create or replace function public.site_host(p_url text) returns text
language sql immutable set search_path = ''
as $$
  select nullif(regexp_replace(host, '^(.*\.)?deviantart\.com$', ''), '')
  from (select regexp_replace(substring(p_url from '^https?://([a-z0-9.-]+)'), '^www\.(?=.*\.)', '') as host) url
$$;

-- A list of at most p_max_items entries, each at most p_max_length characters.
create or replace function public.short_list(p_list text[], p_max_items int, p_max_length int) returns boolean
language sql immutable set search_path = ''
as $$
  select coalesce(cardinality(p_list), 0) <= p_max_items
     and not exists (select 1 from unnest(p_list) item where item is null or length(item) > p_max_length)
$$;

-- What keyword search reads of an artist. The name counts most, then the artist's own short words and
-- the stated facts, then the written profile, then the rest of their own words.
-- The generated column stores the result: after changing this, rewrite the rows (update ... set name = name).
create or replace function public.artist_fts(
  p_name text, p_tagline text, p_profile text, p_specialties text[], p_mediums text[], p_top_tags text[],
  p_search_text text
) returns tsvector
language sql immutable set search_path = ''
as $$
  select setweight(to_tsvector('english', coalesce(p_name, '')), 'A')
      || setweight(to_tsvector('english', concat_ws(' ', p_tagline, array_to_string(p_specialties, ' '),
                                                    array_to_string(p_mediums, ' '), array_to_string(p_top_tags, ' '))), 'B')
      || setweight(to_tsvector('english', coalesce(p_profile, '')), 'C')
      || setweight(to_tsvector('english', coalesce(p_search_text, '')), 'D')
$$;

create table if not exists public.artists (
  id bigint generated always as identity primary key,
  slug text not null unique check (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
  name text not null,
  da_username text not null,
  site_url text, -- most artists have none
  links jsonb not null default '[]',
  tagline text, -- the artist's own words
  top_tags text[] not null default '{}', -- the tags the artist uses most
  search_text text, -- more of the artist's own words, gathered by the push stage: titles, tags, stated specialty
  profile text, -- written from the analysis
  specialties text[] not null default '{}',
  mediums text[] not null default '{}',
  open_for_work boolean, -- null means unknown
  profile_embedding extensions.halfvec(1024), -- null until the artist is analyzed
  -- The sign of each dimension: 128 bytes kept in the row, so a search can order every artist by
  -- meaning without fetching every 2 kB vector from out-of-line storage.
  profile_bits bit(1024) generated always as (extensions.binary_quantize(profile_embedding)) stored,
  hidden boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  fts tsvector generated always as (
    public.artist_fts(name, tagline, profile, specialties, mediums, top_tags, search_text)
  ) stored
);

create table if not exists public.works (
  id bigint generated always as identity primary key,
  artist_id bigint not null references public.artists (id) on delete cascade,
  source text not null, -- 'deviantart' or 'site': where the work is shown, and whom the thumbnail credits
  page_url text not null, -- the deviation page, or the page on the artist's own site
  image_url text not null, -- the source thumbnail: the URL the DeviantArt API gives, or one on the artist's site
  title text,
  description text, -- null until the work is analyzed
  tags text[] not null default '{}',
  -- Kept inline (plain): out of line, every distance pays a TOAST fetch. Null until the work is analyzed.
  style_embedding extensions.halfvec(768) storage plain,
  position int not null default 0,
  -- What makes a work the same work on the next load. A DeviantArt thumbnail URL carries a signed token
  -- that changes, so there it is the deviation page.
  key text generated always as (case when source = 'deviantart' then page_url else image_url end) stored
);

-- What similar_artists reads, so that a call never walks the works table or the wide artists rows.
-- Derived: refresh_style_centroids() rebuilds it from works, and db.after_load() calls that.
-- It exists because of memory, not arithmetic: on the free tier (0.5 GB) the first call after a big
-- write has to bring every page it touches back into memory, and the public role is cut off at 3 s.
create table if not exists public.artist_styles (
  artist_id bigint primary key references public.artists (id) on delete cascade,
  -- The sign of each dimension of the artist's mean style vector: 96 bytes to shortlist by.
  centroid_bits bit(768) not null,
  -- The artist's first 10 style vectors by position, to score the shortlist exactly. A second copy of
  -- them (about 15 kB an artist, 46 MB for 3,000); external: half floats do not compress.
  first_works extensions.halfvec(768)[] storage external not null check (cardinality(first_works) between 1 and 10)
);

-- The words artists use, for putting a misspelled search word right. Derived: refresh_search_words()
-- rebuilds it, and db.after_load() calls that.
create table if not exists public.search_words (
  word text primary key check (length(word) between 4 and 40),
  artists int not null -- how many artists use it
);
create index if not exists search_words_trgm_idx on public.search_words using gin (word extensions.gin_trgm_ops);

-- ponytail: no vector indexes. search_artists orders every artist by 128 bytes of sign bits and
-- compares exactly only the best 100; similar_artists does the same with a shortlist of 100.
-- Measured numbers are printed by the RUN_SCALE=1 test in tests/test_db.py.
-- Ceiling: both still read one row per artist. Past about 30,000 artists add HNSW on
-- artists.profile_embedding and keep the mean style vector to index as well.

-- Removal requests waiting for the owner: logged, never acted on by themselves (owner decision 6).
-- At most 5 per artist; the owner clears them with purge_removed or reject_removal.
create table if not exists public.removal_requests (
  id bigint generated always as identity primary key,
  artist_slug text not null,
  note text,
  created_at timestamptz not null default now()
);

-- One open proof per artist: the hash of the code they were asked to put on their own page, and
-- when it stops counting. The code itself is never stored.
create table if not exists public.removal_proofs (
  artist_slug text primary key check (length(artist_slug) <= 80),
  code_hash text not null check (code_hash ~ '^[0-9a-f]{64}$'),
  expires_at timestamptz not null
);

-- sha256 hex of 'da:<handle>' and 'site:<host>' for every removed artist, so they are never re-added.
create table if not exists public.removed (
  key_hash text primary key,
  removed_at timestamptz not null default now()
);

-- A database made by an earlier version of this file: bring it up to the tables above.
alter table public.artists alter column da_username set not null;
alter table public.artists drop column if exists style_centroid;
alter table public.artists drop constraint if exists artists_site_url_check;
alter table public.artists
  add column if not exists tagline text,
  add column if not exists top_tags text[] not null default '{}',
  add column if not exists search_text text,
  add column if not exists profile_bits bit(1024)
    generated always as (extensions.binary_quantize(profile_embedding)) stored;
do $$
begin
  if (select pg_get_expr(d.adbin, d.adrelid) not like '%artist_fts%'
      from pg_attrdef d join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
      where d.adrelid = 'public.artists'::regclass and a.attname = 'fts') then
    alter table public.artists alter column fts set expression as (
      public.artist_fts(name, tagline, profile, specialties, mediums, top_tags, search_text)
    );
  end if;
end
$$;
drop function if exists public.words(text[]); -- what the first keyword index was built with

alter table public.works
  drop constraint if exists works_page_url_check,
  drop constraint if exists works_image_url_check,
  drop constraint if exists works_artist_id_image_url_key; -- replaced by one work per key
alter table public.works add column if not exists source text not null default 'site';
alter table public.works alter column source drop default; -- every load must say where a work is from
alter table public.works add column if not exists key text
  generated always as (case when source = 'deviantart' then page_url else image_url end) stored;

-- Search reads every artist's row, and after a big write each page of them has to come back into
-- memory before the public's 3 s run out. So the row keeps only what search reads (the keyword
-- index, the sign bits, the facts it filters on) and everything else is stored out of line.
alter table public.artists set (toast_tuple_target = 512);
alter table public.artists
  alter column fts set storage main,
  alter column profile_bits set storage plain,
  alter column specialties set storage main,
  alter column mediums set storage main;

do $$
begin
  if exists (
    select 1 from pg_constraint
    where conrelid = 'public.removal_requests'::regclass and pg_get_constraintdef(oid) = 'PRIMARY KEY (artist_slug)'
  ) then
    alter table public.removal_requests drop constraint removal_requests_pkey; -- it allowed one request per artist
  end if;
end
$$;
alter table public.removal_requests add column if not exists id bigint generated always as identity;
alter table public.removal_requests drop column if exists hidden_at; -- requests no longer hide anyone

-- Every constraint that is not part of a "create table" above, added once ("add constraint" has no
-- "if not exists"). The sizes: a name, tagline or title is a line, a profile or description a
-- paragraph, a list entry a few words.
do $do$
declare
  wanted record;
begin
  for wanted in
    select * from (values
      -- A DeviantArt handle, the same pattern as pipeline/store.py.
      ('artists', 'artists_da_username_clean', $$check (da_username ~ '^[A-Za-z0-9_-]{1,40}$')$$),
      ('artists', 'artists_site_url_clean', 'check (public.clean_url(site_url))'),
      ('artists', 'artists_links_clean', 'check (public.clean_links(links))'),
      ('artists', 'artists_sizes',
       'check (length(slug) <= 80 and length(name) between 1 and 120 and length(profile) <= 2000'
       ' and public.short_list(specialties, 20, 60) and public.short_list(mediums, 20, 60))'),
      ('artists', 'artists_own_words_sizes',
       'check (length(tagline) <= 300 and length(search_text) <= 4000 and public.short_list(top_tags, 40, 60))'),
      ('works', 'works_urls_clean', 'check (public.clean_url(page_url) and public.clean_url(image_url))'),
      ('works', 'works_sizes',
       'check (length(title) <= 200 and length(description) <= 2000 and public.short_list(tags, 40, 60))'),
      ('works', 'works_source_known', $$check (source in ('deviantart', 'site'))$$),
      ('works', 'works_one_per_key', 'unique (artist_id, key)'),
      ('removal_requests', 'removal_requests_pkey', 'primary key (id)'),
      ('removal_requests', 'removal_requests_sizes', 'check (length(artist_slug) <= 80 and length(note) <= 1000)'),
      ('removed', 'removed_key_is_a_hash', $$check (key_hash ~ '^[0-9a-f]{64}$')$$)
    ) as listed (table_name, name, definition)
  loop
    if not exists (
      select 1 from pg_constraint
      where conrelid = format('public.%I', wanted.table_name)::regclass and conname = wanted.name
    ) then
      execute format('alter table public.%I add constraint %I %s', wanted.table_name, wanted.name, wanted.definition);
    end if;
  end loop;
end
$do$;

create index if not exists artists_fts_idx on public.artists using gin (fts);
-- Every search would otherwise read the whole list of entries a load left pending.
alter index public.artists_fts_idx set (fastupdate = off);
-- One row per DeviantArt account and one per site. No two rows can share a removal key, so removing
-- an artist leaves no second copy behind, and removing one artist never blocks another.
create unique index if not exists artists_da_username_idx on public.artists (lower(da_username));
create unique index if not exists artists_site_host_idx on public.artists (public.site_host(site_url));
create index if not exists removal_requests_slug_idx on public.removal_requests (artist_slug, created_at, id);
-- A card shows an artist's first four works: with this, it reads four rows instead of all of them.
create index if not exists works_artist_position_idx on public.works (artist_id, position, id);

alter table public.artists enable row level security;
alter table public.works enable row level security;
alter table public.artist_styles enable row level security;
alter table public.search_words enable row level security;
alter table public.removal_requests enable row level security;
alter table public.removal_proofs enable row level security;
alter table public.removed enable row level security;

revoke all on table
  public.artists, public.works, public.artist_styles, public.search_words,
  public.removal_requests, public.removal_proofs, public.removed
from public, anon, authenticated;
grant all on table
  public.artists, public.works, public.artist_styles, public.search_words,
  public.removal_requests, public.removal_proofs, public.removed
to service_role;
revoke all on sequence public.artists_id_seq, public.works_id_seq, public.removal_requests_id_seq
  from public, anon, authenticated;

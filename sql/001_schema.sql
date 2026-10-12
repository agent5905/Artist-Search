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

-- array_to_string is only STABLE, so a generated column cannot call it directly.
create or replace function public.words(text[]) returns text
language sql immutable set search_path = ''
as $$ select coalesce(array_to_string($1, ' '), '') $$;

-- A strict http(s) URL, at most 2,048 characters:
--   * the host is a public DNS name in its one canonical spelling: lowercase ASCII (punycode for
--     other scripts), no user, no trailing dot, no IP address. The same rule as pipeline/store.py.
--   * the rest is URL characters only (RFC 3986): no spaces, control or non-ASCII characters.
--     Encode those as %XX before storing.
--   * no fragment. It never reaches the server, so it is only a place to hide data.
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

create table if not exists public.artists (
  id bigint generated always as identity primary key,
  slug text not null unique check (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
  name text not null,
  da_username text not null,
  site_url text,
  links jsonb not null default '[]',
  profile text,
  specialties text[] not null default '{}',
  mediums text[] not null default '{}',
  open_for_work boolean, -- null means unknown
  profile_embedding extensions.halfvec(1024),
  hidden boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  fts tsvector generated always as (
    setweight(to_tsvector('english', name), 'A')
    || setweight(to_tsvector('english', public.words(specialties) || ' ' || public.words(mediums)), 'B')
    || setweight(to_tsvector('english', coalesce(profile, '')), 'C')
  ) stored
);

create table if not exists public.works (
  id bigint generated always as identity primary key,
  artist_id bigint not null references public.artists (id) on delete cascade,
  page_url text not null,
  image_url text not null, -- the source thumbnail on the artist's own site
  title text,
  description text,
  tags text[] not null default '{}',
  -- Kept inline (plain): out of line, every distance pays a TOAST fetch and similar_artists
  -- measured 2 to 4 s on 30,000 works instead of 0.4 s.
  style_embedding extensions.halfvec(768) storage plain,
  position int not null default 0,
  unique (artist_id, image_url)
);

-- What similar_artists reads, so that a call never walks the works table or the wide artists rows.
-- Derived: refresh_style_centroids() rebuilds it from works, and the push stage calls that after a load.
-- It exists because of memory, not arithmetic: on the free tier (0.5 GB) the first call after a big
-- write has to bring every page it touches back into memory, and the public role is cut off at 3 s.
-- Measured at 3,000 artists right after all 30,000 works were rewritten: reading the works of 100
-- artists took 1.9 to 2.7 s on the first call and sometimes timed out; reading this table takes
-- 1.6 to 1.8 s. Every later call takes 0.1 to 0.2 s either way.
create table if not exists public.artist_styles (
  artist_id bigint primary key references public.artists (id) on delete cascade,
  -- The sign of each dimension of the artist's mean style vector: 96 bytes to shortlist by.
  centroid_bits bit(768) not null,
  -- The artist's first 10 style vectors by position, to score the shortlist exactly. A second copy of
  -- them (about 15 kB an artist, 46 MB for 3,000); external: half floats do not compress.
  first_works extensions.halfvec(768)[] storage external not null check (cardinality(first_works) between 1 and 10)
);

-- ponytail: no vector indexes. search_artists scans one profile vector per artist, and similar_artists
-- 96 bytes per artist before it scores a shortlist of 100. Measured numbers are printed by the
-- RUN_SCALE=1 test in tests/test_db.py.
-- Ceiling: search_artists with an embedding still reads every profile vector (about 1,300 pages for
-- 3,000 artists), so its first call after a big write is the slow one now. Past that, shortlist it by
-- sign bits the same way, or add HNSW on artists.profile_embedding.

-- Pending removal requests. The owner reviews them, then purge_removed or reject_removal clears them.
create table if not exists public.removal_requests (
  artist_slug text primary key,
  note text,
  created_at timestamptz not null default now(),
  hidden_at timestamptz -- when this request hid the artist. Null: logged only, the artist stayed as it was.
);

-- sha256 hex of 'da:<handle>' and 'site:<host>' for every removed artist, so they are never re-added.
create table if not exists public.removed (
  key_hash text primary key,
  removed_at timestamptz not null default now()
);

-- A database made by the first version of this file: bring it up to the tables above.
alter table public.removal_requests add column if not exists hidden_at timestamptz;
alter table public.artists alter column da_username set not null;
alter table public.artists drop column if exists style_centroid; -- an earlier draft kept the centroid here
alter table public.artists drop constraint if exists artists_site_url_check;
alter table public.works
  drop constraint if exists works_page_url_check,
  drop constraint if exists works_image_url_check;

-- Every check constraint, added once ("add constraint" has no "if not exists").
-- The sizes: a name or title is a line, a profile or description a paragraph, a list entry a few words.
do $do$
declare
  wanted record;
begin
  for wanted in
    select * from (values
      -- A DeviantArt handle, the same pattern as pipeline/store.py.
      ('artists', 'artists_da_username_clean', $$da_username ~ '^[A-Za-z0-9_-]{1,40}$'$$),
      ('artists', 'artists_site_url_clean', 'public.clean_url(site_url)'),
      ('artists', 'artists_links_clean', 'public.clean_links(links)'),
      ('artists', 'artists_sizes',
       'length(slug) <= 80 and length(name) between 1 and 120 and length(profile) <= 2000'
       ' and public.short_list(specialties, 20, 60) and public.short_list(mediums, 20, 60)'),
      ('works', 'works_urls_clean', 'public.clean_url(page_url) and public.clean_url(image_url)'),
      ('works', 'works_sizes',
       'length(title) <= 200 and length(description) <= 2000 and public.short_list(tags, 40, 60)'),
      ('removal_requests', 'removal_requests_sizes', 'length(artist_slug) <= 80 and length(note) <= 1000'),
      ('removed', 'removed_key_is_a_hash', $$key_hash ~ '^[0-9a-f]{64}$'$$)
    ) as listed (table_name, name, rule)
  loop
    if not exists (
      select 1 from pg_constraint
      where conrelid = format('public.%I', wanted.table_name)::regclass and conname = wanted.name
    ) then
      execute format('alter table public.%I add constraint %I check (%s)', wanted.table_name, wanted.name, wanted.rule);
    end if;
  end loop;
end
$do$;

create index if not exists artists_fts_idx on public.artists using gin (fts);
-- One row per DeviantArt account and one per site. No two rows can share a removal key, so purging
-- an artist leaves no second copy behind, and removing one artist never blocks another.
create unique index if not exists artists_da_username_idx on public.artists (lower(da_username));
create unique index if not exists artists_site_host_idx on public.artists (public.site_host(site_url));

alter table public.artists enable row level security;
alter table public.works enable row level security;
alter table public.removal_requests enable row level security;
alter table public.removed enable row level security;
alter table public.artist_styles enable row level security;

revoke all on table public.artists, public.works, public.removal_requests, public.removed, public.artist_styles
  from public, anon, authenticated;
grant all on table public.artists, public.works, public.removal_requests, public.removed, public.artist_styles
  to service_role;
revoke all on sequence public.artists_id_seq, public.works_id_seq from public, anon, authenticated;

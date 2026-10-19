-- Artist Search schema. Every statement is idempotent, so the file is safe to rerun.
--
-- Hard rule 1: no column holds image bytes. There is no bytea anywhere, every URL must pass
-- clean_url (which nothing carrying "data:" can) and sit on the host its source says it is on, and
-- every text column and list entry is too short to carry an image as text (the *_sizes constraints).
-- Hard rule 7: an artist is known by a DeviantArt handle, their account's hash, and, if they have a site, its
-- registrable domain. Each has exactly one spelling here, so a removed artist cannot come back under a variant.
-- Row level security is on for every table with no policies: the public role reads only through
-- the functions in 003_limits.sql, which the site's server calls with a token of its own.

create extension if not exists vector with schema extensions;
create extension if not exists pg_trgm with schema extensions; -- for misspelled search words
create extension if not exists pgcrypto with schema extensions; -- for the removal proof code

-- A function created later is not the public's to call until it is granted. (The first line takes back
-- what every function gets by default; the second, what this project's defaults added in this schema.)
alter default privileges for role postgres revoke execute on functions from public;
alter default privileges for role postgres in schema public revoke execute on functions from anon, authenticated;

-- A strict http(s) URL, at most 2,048 characters:
--   * the host is a public DNS name in its one canonical spelling: lowercase ASCII (punycode for
--     other scripts), no user, no trailing dot, no IP address. The same rule as pipeline/store.py.
--   * the rest is URL characters only (RFC 3986): no spaces, control or non-ASCII characters.
--     Encode those as %XX before storing.
--   * no fragment (it never reaches the server, so it is only a place to hide data) and no "data:"
--     anywhere, in any case.
-- A DeviantArt deviation page and its signed thumbnail URL (a long token in the query) both pass.
create or replace function public.clean_url(p_url text) returns boolean
language sql immutable set search_path = ''
as $fn$
  select length(p_url) <= 2048 and p_url !~* 'data:' and p_url ~ (
    '^https?://(?=[^/?:]{4,253}([/?:]|$))'
    || '([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+([a-z]{2,63}|xn--[a-z0-9-]{1,59})'
    || '(:[0-9]{1,5})?'
    || '([/?][A-Za-z0-9._~:/?@!$&''()*+,;=%-]*)?$')
$fn$;

-- The host of a clean URL.
create or replace function public.url_host(p_url text) returns text
language sql immutable set search_path = ''
as $$ select substring(p_url from '^https?://([a-z0-9.-]+)') $$;

-- Whether a host is a domain or lies under it: example.com and shop.example.com, not notexample.com.
create or replace function public.host_under(p_host text, p_domain text) returns boolean
language sql immutable set search_path = ''
as $$ select p_host = p_domain or right(p_host, length(p_domain) + 1) = '.' || p_domain $$;

-- A list of at most p_max_items entries, each at most p_max_length characters.
create or replace function public.short_list(p_list text[], p_max_items int, p_max_length int) returns boolean
language sql immutable set search_path = ''
as $$
  select coalesce(cardinality(p_list), 0) <= p_max_items
     and not exists (select 1 from unnest(p_list) item where item is null or length(item) > p_max_length)
$$;

-- What the removal list holds of an identifier ('da:<handle>' or 'site:<domain>'): its sha256, so the
-- list names nobody. The same hash as pipeline/store.py makes.
create or replace function public.key_hash(p_key text) returns text
language sql immutable set search_path = ''
as $$ select encode(sha256(convert_to(p_key, 'UTF8')), 'hex') $$;

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
  da_username text not null, -- the profile is https://www.deviantart.com/<da_username>
  site_url text, -- the artist's own site; most artists have none
  site_domain text, -- its registrable domain (janedoe.art for portfolio.janedoe.art), worked out by the push stage
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
  ) stored,
  -- What the removal list would hold for this artist. Kept in the row so that a removal finds its
  -- rows, and a reader leaves a removed artist out, by an index lookup.
  da_key text generated always as (public.key_hash('da:' || lower(da_username))) stored,
  site_key text generated always as (public.key_hash('site:' || site_domain)) stored
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
  position int not null default 0,
  width int, -- the image's size in pixels, so the site can lay it out uncropped; null when not known
  height int,
  -- What makes a work the same work on the next load. A DeviantArt thumbnail URL carries a signed token
  -- that changes, so there it is the deviation page.
  key text generated always as (case when source = 'deviantart' then page_url else image_url end) stored
);

-- Each artist's nearest artists by style, best first. The ranking is worked out on the owner's
-- machine, where the style vectors live (it needs statistics over every artist at once); the database
-- only stores the result and similar_artists reads it. db.load() writes it; a removed artist's rows
-- go with them, on either side.
create table if not exists public.artist_neighbors (
  artist_id bigint not null references public.artists (id) on delete cascade,
  neighbor_id bigint not null references public.artists (id) on delete cascade,
  rank smallint not null, -- 1 is the nearest
  score real not null,
  -- Which of the neighbour's works to show beside this artist, best first, and no others: those closest in
  -- style to this artist's own and close enough (pipeline/push.py), each named by its works.position. Null in
  -- a row from before picks: the neighbour's own order.
  picks smallint[],
  primary key (artist_id, rank),
  constraint artist_neighbors_not_itself check (neighbor_id <> artist_id)
);
-- Deleting an artist finds the rows that name them as a neighbour by this.
create index if not exists artist_neighbors_neighbor_idx on public.artist_neighbors (neighbor_id);

-- The words visible artists use, for putting a misspelled search word right. Derived:
-- refresh_search_words() rebuilds it; db.load() and every removal call that.
create table if not exists public.search_words (
  word text primary key check (length(word) between 4 and 40),
  artists int not null -- how many artists use it
);
create index if not exists search_words_trgm_idx on public.search_words using gin (word extensions.gin_trgm_ops);

-- ponytail: no vector index. search_artists orders every artist by 128 bytes of sign bits and
-- compares exactly only the best 100. Measured numbers are printed by the RUN_SCALE=1 test in
-- tests/test_db.py. Ceiling: it still reads one row per artist. Past about 30,000 artists add HNSW
-- on artists.profile_embedding.

-- Removal requests (owner decision 6): a lasting record, which nothing but a proven removal or the owner's
-- answer closes. No load, hold-back, rename or deletion of the artist's row touches it. A request is bound,
-- when it is filed, to the artist the claim-or-remove page showed (its ticket: claim_ticket in
-- sql/002_functions.sql), listed then or not: their handle, their site and the account the local library keeps
-- for them (artists.da_account). Nothing afterwards changes what it is bound to, so the verifier finds that
-- artist while they are not listed, and only that artist: a slug can pass to another handle, and a site to
-- another artist. A later request for the same handle and site is the same request and
-- only counts (and fills in an account that was not known yet); one for anything else is a request of its
-- own (removal_requests_open_idx below). Nothing a visitor sends can erase what an artist wrote.
create table if not exists public.removal_requests (
  id bigint generated always as identity primary key, -- how the verifier and the owner name a request
  artist_slug text not null, -- the slug asked about; its proof code is that slug's. 'closed:<uuid>' once closed
  note text, -- the first request's note
  first_at timestamptz not null default now(),
  last_at timestamptz not null default now(),
  requests bigint not null default 1
);
alter table public.removal_requests
  add column if not exists id bigint generated always as identity,
  add column if not exists da_username text, -- the public handle and site the artist was listed with when asked
  add column if not exists site_url text,
  add column if not exists site_domain text,
  add column if not exists da_account text, -- and the account the library keeps for them; null when it keeps none
  -- The removal list's two keys for them, as artists.da_key and site_key are made.
  add column if not exists da_key text generated always as (public.key_hash('da:' || lower(da_username))) stored,
  add column if not exists site_key text generated always as (public.key_hash('site:' || site_domain)) stored,
  -- The verifier's queue (pipeline/removals.py). code_at: when a proof code was last asked for; null for a
  -- request without one, which only the owner answers. due_at: when the verifier looks for the code next.
  add column if not exists code_at timestamptz,
  add column if not exists due_at timestamptz,
  -- The network the code was last asked for from, as an HMAC (client_wait keys it so): the verifier takes
  -- turns between networks (due_removals), so that one asking for many codes holds back only its own.
  add column if not exists network text,
  -- Null while open. 'proven' (the verifier saw the code), 'granted' or 'rejected' (the owner's answer), or
  -- 'removed' (the artist was removed another way). A closed row keeps nothing of the artist: its slug gives
  -- way to a random key and the handle, site, account and note go.
  add column if not exists outcome text;
-- An open request filed before those columns gets the handle and site its artist is listed with now, so
-- that it can be granted while they are not listed. It fills only what is empty: rerun, it changes nothing.
update public.removal_requests r
set da_username = a.da_username, site_url = a.site_url, site_domain = a.site_domain
from public.artists a
where a.slug = r.artist_slug and r.da_username is null and r.outcome is null;
-- A table made before requests had an id was keyed by the slug, one request a slug. The id is the key now.
do $$
begin
  if exists (
    select 1 from pg_constraint
    where conrelid = 'public.removal_requests'::regclass and conname = 'removal_requests_pkey'
      and pg_get_constraintdef(oid) = 'PRIMARY KEY (artist_slug)'
  ) then
    alter table public.removal_requests drop constraint removal_requests_pkey, add primary key (id);
  end if;
end
$$;
alter table public.removal_requests alter column artist_slug set not null;
create unique index if not exists removal_requests_open_idx
on public.removal_requests (artist_slug, da_key, site_key) nulls not distinct where outcome is null;

-- The one secret a removal proof code is derived from. Readable by the table owner only, which the
-- two functions that need it run as. Made once, with a random value.
create table if not exists public.removal_secret (
  only_row boolean primary key default true check (only_row),
  secret text not null default encode(extensions.gen_random_bytes(32), 'hex')
);
insert into public.removal_secret default values on conflict do nothing;

-- sha256 hex of 'da:<handle>' and 'site:<domain>' for every removed artist, and of their account as the
-- library hashes it (artists.da_account), so they are never re-added.
create table if not exists public.removed (
  key_hash text primary key,
  removed_at timestamptz not null default now()
);

-- A database made by the previous version of this file: bring it up to the tables above. The
-- database holds real artists now, so a step here only ever adds; the steps that took earlier
-- versions apart are gone, and nothing in this file deletes, rewrites or drops a row.
alter table public.works
  add column if not exists width int,
  add column if not exists height int;
alter table public.artist_neighbors add column if not exists picks smallint[];
-- The hash the local library knows the artist's DeviantArt account by (library.account_hash), sent by the push
-- stage; null when it keeps none. A removal request is bound to it, a proof on DeviantArt is read only on that
-- account's profile, and a removal bars it, so the account is not listed again under any handle.
alter table public.artists add column if not exists da_account text;

-- Search reads every artist's row, and after a big write each page of them has to come back into
-- memory before the public's 3 s run out. So the row keeps only what search reads (the keyword
-- index, the sign bits, the facts it filters on) and everything else is stored out of line.
alter table public.artists set (toast_tuple_target = 512);
alter table public.artists
  alter column fts set storage main,
  alter column profile_bits set storage plain,
  alter column specialties set storage main,
  alter column mediums set storage main;

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
      -- A site comes with its registrable domain, and lies on it or under it. The domain is a plain
      -- lowercase DNS name, and never DeviantArt's or its image host's: those are nobody's own site.
      ('artists', 'artists_site_domain_clean',
       $$check ((site_url is null) = (site_domain is null) and (site_domain is null or (
           length(site_domain) <= 253
           and site_domain ~ '^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+([a-z]{2,63}|xn--[a-z0-9-]{1,59})$'
           and site_domain !~ '(^|\.)(deviantart|wixmp)\.com$'
           and public.host_under(public.url_host(site_url), site_domain))))$$),
      ('artists', 'artists_sizes',
       'check (length(slug) <= 80 and length(name) between 1 and 120 and length(profile) <= 2000'
       ' and public.short_list(specialties, 20, 60) and public.short_list(mediums, 20, 60))'),
      ('artists', 'artists_own_words_sizes',
       'check (length(tagline) <= 300 and length(search_text) <= 4000 and public.short_list(top_tags, 40, 60))'),
      ('artists', 'artists_da_account_hash', $$check (da_account ~ '^[0-9a-f]{64}$')$$),
      ('works', 'works_urls_clean', 'check (public.clean_url(page_url) and public.clean_url(image_url))'),
      -- A DeviantArt work is a deviation page and a thumbnail on DeviantArt's image host. (A work on
      -- the artist's own site is checked against that site by the trigger works_before_write.)
      ('works', 'works_deviantart_hosts',
       $$check (source <> 'deviantart' or (public.url_host(page_url) = 'www.deviantart.com'
                                           and public.host_under(public.url_host(image_url), 'wixmp.com')
                                           and public.url_host(image_url) <> 'wixmp.com'))$$),
      ('works', 'works_sizes',
       'check (length(title) <= 200 and length(description) <= 2000 and public.short_list(tags, 40, 60))'),
      ('works', 'works_source_known', $$check (source in ('deviantart', 'site'))$$),
      -- An image's size is two whole numbers of pixels, given together or not at all.
      ('works', 'works_dimensions',
       'check ((width is null) = (height is null) and width between 1 and 100000 and height between 1 and 100000)'),
      ('works', 'works_one_per_key', 'unique (artist_id, key)'),
      -- As many as a card shows, so the list is too short to carry anything else (hard rule 1). And never none:
      -- the card would show no work.
      ('artist_neighbors', 'artist_neighbors_few_picks', 'check (cardinality(picks) <= 4)'),
      ('artist_neighbors', 'artist_neighbors_some_picks', 'check (cardinality(picks) >= 1)'),
      ('removal_requests', 'removal_requests_short', 'check (length(artist_slug) <= 80 and length(note) <= 300)'),
      ('removal_requests', 'removal_requests_outcome',
       $$check (outcome in ('proven', 'granted', 'rejected', 'removed'))$$),
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
create unique index if not exists artists_da_key_idx on public.artists (da_key);
create unique index if not exists artists_site_key_idx on public.artists (site_key);
create index if not exists artists_da_account_idx on public.artists (da_account); -- for a removal, too
-- A card shows an artist's first four works: with this, it reads four rows instead of all of them.
create index if not exists works_artist_position_idx on public.works (artist_id, position, id);

alter table public.artists enable row level security;
alter table public.works enable row level security;
alter table public.artist_neighbors enable row level security;
alter table public.search_words enable row level security;
alter table public.removal_requests enable row level security;
alter table public.removal_secret enable row level security;
alter table public.removed enable row level security;

revoke all on table
  public.artists, public.works, public.artist_neighbors, public.search_words,
  public.removal_requests, public.removal_secret, public.removed
from public, anon, authenticated;
revoke all on table public.removal_secret from service_role;
grant all on table
  public.artists, public.works, public.artist_neighbors, public.search_words, public.removal_requests, public.removed
to service_role;
revoke all on sequence public.artists_id_seq, public.works_id_seq, public.removal_requests_id_seq
from public, anon, authenticated;

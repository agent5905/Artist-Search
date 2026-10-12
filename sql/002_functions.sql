-- Search, profile and removal functions. Safe to rerun.
--
-- The four public functions are SECURITY DEFINER with an empty search_path: they run as the
-- table owner, which is how the public role reads tables it has no access to. Every one of them
-- filters out hidden artists.

-- What a purge remembers of an artist without keeping their name (hard rule 7): sha256 hex of
-- 'da:<lowercased handle>' and 'site:<host, without a leading www.>'.
-- The same keys as pipeline/store.py, so a hash made here matches one made on the owner's machine.
-- Nothing is normalized here beyond that: the constraints in 001_schema.sql refuse any other spelling.
create or replace function public.removal_keys(p_da_username text, p_site_url text) returns text[]
language sql immutable set search_path = ''
as $$
  select coalesce(array_agg(encode(sha256(convert_to(k, 'UTF8')), 'hex')), '{}')
  from unnest(array['da:' || lower(p_da_username), 'site:' || public.site_host(p_site_url)]) k
  where k is not null
$$;

-- Every key that bars a row, as pipeline/store.py decides it: the handle; the site and each link,
-- with their parent domains (removing example.com also removes shop.example.com); and the handle
-- named by a DeviantArt profile link.
create or replace function public.barring_keys(p_da_username text, p_site_url text, p_links jsonb) returns text[]
language sql immutable set search_path = ''
as $$
  with urls as (
    select p_site_url as url
    union all
    select link #>> '{}'
    from jsonb_array_elements(case when jsonb_typeof(p_links) = 'array' then p_links else '[]' end) link
  ),
  hosts as (
    select string_to_array(public.site_host(url), '.') as labels from urls
  ),
  keys as (
    select 'da:' || lower(p_da_username) as k
    union all
    select 'da:' || lower(substring(
      url from '^https?://(?:www\.)?deviantart\.com(?::[0-9]+)?/+([A-Za-z0-9_-]{1,40})(?:[/?]|$)'))
    from urls
    union all
    select 'site:' || array_to_string(labels[i:], '.')
    from hosts, generate_series(1, cardinality(labels) - 1) i
  )
  select coalesce(array_agg(encode(sha256(convert_to(k, 'UTF8')), 'hex')), '{}') from keys where k is not null
$$;

create or replace function public.is_barred(p_da_username text, p_site_url text, p_links jsonb) returns boolean
language sql stable set search_path = ''
as $$
  select exists (
    select 1 from public.removed
    where key_hash = any (public.barring_keys(p_da_username, p_site_url, p_links))
  )
$$;

-- Hard rule 7, enforced where no pipeline bug can get around it. No row may take on a handle, site
-- or link that a removed artist is known by: an insert of one is skipped (so an upsert carries on
-- with the other rows) and an update onto one is refused. A write that leaves the three alone is
-- always let through, so an artist can be hidden or edited whatever the removal list says.
-- An artist hidden by a pending request stays hidden.
create or replace function public.artists_before_write() returns trigger
language plpgsql set search_path = ''
as $$
begin
  if tg_op = 'UPDATE'
     and (new.da_username, new.site_url, new.links) is not distinct from (old.da_username, old.site_url, old.links)
  then
    null;
  elsif public.is_barred(new.da_username, new.site_url, new.links) then
    if tg_op = 'INSERT' then
      return null;
    end if;
    raise exception 'this handle, site or link belongs to a removed artist' using errcode = 'check_violation';
  end if;
  if exists (select 1 from public.removal_requests where artist_slug = new.slug and hidden_at is not null) then
    new.hidden := true;
  end if;
  new.updated_at := now();
  return new;
end
$$;

create or replace trigger artists_before_write before insert or update on public.artists
for each row execute function public.artists_before_write();

-- The other half of the rule: when keys join the removal list, by a purge or by hand, every artist
-- they bar is deleted (works cascade). So no row a removed key bars ever stays in the index.
create or replace function public.removed_after_insert() returns trigger
language plpgsql set search_path = ''
as $$
begin
  delete from public.artists a where public.is_barred(a.da_username, a.site_url, a.links);
  return null;
end
$$;

create or replace trigger removed_after_insert after insert on public.removed
for each statement execute function public.removed_after_insert();

-- An upsert of a row that already holds the same values writes nothing: no new row version, no dead
-- one for vacuum to clear. (Postgres's own suppress_redundant_updates_trigger compares bytes, and a
-- compressed stored value never equals a fresh one.)
create or replace function public.skip_unchanged() returns trigger
language plpgsql set search_path = ''
as $$
begin
  if new is not distinct from old then
    return null;
  end if;
  return new;
end
$$;

create or replace trigger works_skip_unchanged before update on public.works
for each row execute function public.skip_unchanged();

-- An artist's works as card data, in display order. p_limit null means all of them.
create or replace function public.work_cards(p_artist_id bigint, p_limit int default null) returns jsonb
language sql stable set search_path = ''
as $$
  select coalesce(
    jsonb_agg(jsonb_build_object('image_url', w.image_url, 'page_url', w.page_url, 'title', w.title)
              order by w.position, w.id),
    '[]')
  from (
    select * from public.works where artist_id = p_artist_id order by position, id limit p_limit
  ) w
$$;

create or replace function public.search_artists(
  q text,
  q_embedding extensions.halfvec(1024) default null,
  p_mediums text[] default null,
  p_specialties text[] default null,
  p_open_only boolean default false,
  p_limit int default 24,
  p_offset int default 0
)
returns table (
  slug text, name text, profile text, specialties text[], mediums text[], open_for_work boolean, works jsonb
)
language sql stable security definer set search_path = ''
as $$
  with query as (
    -- Only the first 200 characters are read: a brief fits, and a huge query cannot be used to stall the database.
    -- ponytail: any word of the query may match; no phrases, no exclusions.
    -- Ceiling: the text search critic (piece 5) asks for more.
    select replace(plainto_tsquery('english', left(coalesce(q, ''), 200))::text, '&', '|')::tsquery as tsq
  ),
  -- Only the first 20 values of a filter list are read, and values longer than a stored one can be
  -- (60 characters) are dropped, so a huge list costs nothing. A list that was given always filters,
  -- even when none of its values is left.
  wanted as (
    select coalesce(cardinality(p_mediums), 0) > 0 as by_medium,
           array(select lower(v) from (select v from unnest(p_mediums[1:20]) v limit 20) first
                 where length(v) <= 60) as mediums,
           coalesce(cardinality(p_specialties), 0) > 0 as by_specialty,
           array(select lower(v) from (select v from unnest(p_specialties[1:20]) v limit 20) first
                 where length(v) <= 60) as specialties
  ),
  -- Pass 1: rule out on hard facts. A null or empty list means no filter; a list matches any of its values.
  cand as (
    select a.id, a.fts, a.profile_embedding
    from public.artists a, wanted
    where not a.hidden
      and (not coalesce(p_open_only, false) or a.open_for_work)
      and (not wanted.by_medium or exists (
        select 1 from unnest(a.mediums) have where lower(have) = any (wanted.mediums)))
      and (not wanted.by_specialty or exists (
        select 1 from unnest(a.specialties) have where lower(have) = any (wanted.specialties)))
  ),
  -- Pass 2: rank what is left by keywords and, when an embedding is given, by meaning.
  kw as (
    select c.id, row_number() over (order by ts_rank(c.fts, query.tsq) desc, c.id) as r
    from cand c, query
    where c.fts @@ query.tsq
  ),
  sem as (
    select c.id, row_number() over (order by c.profile_embedding operator(extensions.<=>) q_embedding, c.id) as r
    from cand c
    where q_embedding is not null and c.profile_embedding is not null
  ),
  ranked as (
    -- Reciprocal rank fusion with the usual constant of 60.
    -- ponytail: no similarity floor, so with an embedding every candidate is returned, best first.
    -- Ceiling: piece 5 wants a cutoff.
    select id, coalesce(1.0 / (60 + kw.r), 0) + coalesce(1.0 / (60 + sem.r), 0) as score
    from kw full join sem using (id)
    union all
    -- No query at all: browse the filtered set.
    select c.id, 0 from cand c, query where numnode(query.tsq) = 0 and q_embedding is null
  )
  select a.slug, a.name, a.profile, a.specialties, a.mediums, a.open_for_work, public.work_cards(a.id, 4)
  from ranked join public.artists a using (id)
  order by ranked.score desc, a.name, a.id
  limit least(greatest(coalesce(p_limit, 24), 1), 100) offset greatest(coalesce(p_offset, 0), 0)
$$;

-- Service role only. Rebuilds artist_styles from works: each artist's first 10 style vectors by
-- position, and the sign bits of their mean. The push stage calls it after loading works
-- (db.refresh_style_centroids); similar_artists cannot see an artist, as the seed or as a result,
-- until it has run. Returns how many rows changed: an artist whose vectors are the same is not rewritten.
create or replace function public.refresh_style_centroids() returns int
language sql set search_path = ''
as $$
  with fresh as (
    select a.id as artist_id, style.first_works, style.centroid_bits
    from public.artists a
    cross join lateral (
      select array_agg(w.style_embedding order by w.position, w.id) as first_works,
             extensions.binary_quantize(extensions.avg(extensions.l2_normalize(w.style_embedding))) as centroid_bits
      from (
        select style_embedding, position, id from public.works where artist_id = a.id order by position, id limit 10
      ) w
      where w.style_embedding is not null
    ) style
    where style.first_works is not null
  ),
  gone as (
    delete from public.artist_styles s
    where not exists (select 1 from fresh where fresh.artist_id = s.artist_id)
    returning 1
  ),
  written as (
    insert into public.artist_styles as s (artist_id, centroid_bits, first_works)
    select artist_id, centroid_bits, first_works from fresh
    on conflict (artist_id) do update
      set centroid_bits = excluded.centroid_bits, first_works = excluded.first_works
      where s.first_works is distinct from excluded.first_works
    returning 1
  )
  select (select count(*) from gone)::int + (select count(*) from written)::int
$$;

-- Other artists ranked by style, in two stages, both read from artist_styles:
--   1. shortlist the 100 artists whose mean style is nearest the seed's, by the sign bits alone;
--   2. score only those exactly: the mean, over the seed's works, of the best cosine match among
--      the candidate's works.
-- Only an artist's first 10 works by position take part, on both sides.
-- ponytail: sign bits are a coarse measure, and an artist outside the nearest 100 cannot be returned
-- however well one work matches. Ceiling: the style critic (piece 6) finds a miss; then widen the
-- shortlist, or keep the mean vector itself in artist_styles and rank by it.
create or replace function public.similar_artists(p_slug text, p_limit int default 24)
returns table (
  slug text, name text, profile text, specialties text[], mediums text[], open_for_work boolean, works jsonb
)
language sql stable security definer set search_path = ''
as $$
  with seed as materialized (
    select s.artist_id, s.centroid_bits, s.first_works
    from public.artists a join public.artist_styles s on s.artist_id = a.id
    where a.slug = p_slug and not a.hidden
  ),
  wanted as materialized (
    select w.e, w.n from seed, unnest(seed.first_works) with ordinality as w (e, n)
  ),
  shortlist as materialized (
    select c.artist_id
    from public.artist_styles c, seed
    where c.artist_id <> seed.artist_id
    order by c.centroid_bits operator(extensions.<~>) seed.centroid_bits, c.artist_id
    limit 100
  ),
  candidates as materialized (
    -- Fenced off on purpose. Reading first_works is the expensive part (15 kB an artist, stored out of
    -- line) and the planner cannot see that: left free, it may unpack every artist's before it joins.
    select c.artist_id, c.first_works
    from shortlist
    join public.artist_styles c on c.artist_id = shortlist.artist_id
    join public.artists a on a.id = shortlist.artist_id and not a.hidden
  ),
  best as (
    select candidates.artist_id, wanted.n, max(1 - (theirs.e operator(extensions.<=>) wanted.e)) as sim
    from candidates
    cross join lateral unnest(candidates.first_works) as theirs (e)
    cross join wanted
    group by candidates.artist_id, wanted.n
  ),
  scored as (
    select artist_id, avg(sim) as score from best group by artist_id
  )
  select a.slug, a.name, a.profile, a.specialties, a.mediums, a.open_for_work, public.work_cards(a.id, 4)
  from scored join public.artists a on a.id = scored.artist_id
  order by scored.score desc, a.id
  limit least(greatest(coalesce(p_limit, 24), 1), 100)
$$;

create or replace function public.get_artist(p_slug text)
returns table (
  slug text, name text, profile text, specialties text[], mediums text[], open_for_work boolean, works jsonb,
  links jsonb
)
language sql stable security definer set search_path = ''
as $$
  select a.slug, a.name, a.profile, a.specialties, a.mediums, a.open_for_work, public.work_cards(a.id), a.links
  from public.artists a
  where a.slug = p_slug and not a.hidden
$$;

-- Hard rule 7: anyone can ask for a profile to be removed. The request is logged for the owner (one
-- pending request per artist, so the table cannot be flooded) and the profile disappears at once,
-- unless 20 profiles were already hidden this way in the last 24 hours. Past that cap the request
-- is still logged and the artist stays visible until the owner has looked, so nobody can hide the index.
-- Always returns true, whether the slug is unknown, hidden or visible, so it gives nothing away.
-- ponytail: no proof that the requester is the artist, and the cap is global. Ceiling: an attacker can
-- use up the 20 and delay real requests until the owner reviews; then add a confirmation step.
create or replace function public.request_removal(p_slug text, p_note text default null) returns boolean
language sql security definer set search_path = ''
as $$
  -- One request at a time, so two cannot both take the last free slot.
  select pg_advisory_xact_lock(hashtext('public.request_removal'));
  with room as (
    select count(*) < 20 as free from public.removal_requests where hidden_at > now() - interval '24 hours'
  ),
  logged as (
    insert into public.removal_requests (artist_slug, note, hidden_at)
    select a.slug, left(p_note, 1000), case when room.free and not a.hidden then now() end
    from public.artists a, room
    where a.slug = p_slug
    on conflict (artist_slug) do nothing
    returning artist_slug, hidden_at
  ),
  hide as (
    update public.artists a set hidden = true
    from logged
    where a.slug = logged.artist_slug and logged.hidden_at is not null
  )
  select true
$$;

-- Service role only. The owner turns a request down: it is deleted, and the artist is shown again
-- if the request is what hid them. Returns whether there was a request.
create or replace function public.reject_removal(p_slug text) returns boolean
language plpgsql set search_path = ''
as $$
declare
  hid boolean;
begin
  delete from public.removal_requests where artist_slug = p_slug returning hidden_at is not null into hid;
  if hid then
    update public.artists set hidden = false where slug = p_slug;
  end if;
  return hid is not null;
end
$$;

-- Service role only. Honors the requests the owner has reviewed, and only those: for each slug in
-- p_slugs that has a pending request, the artist's keys go on the removal list. That deletes the
-- artist (works cascade) and every other row those keys bar: a second copy on a subdomain, or one
-- that only links to the removed site or profile. Returns how many artists were deleted.
-- ponytail: no check that the site is the artist's alone. Ceiling: an artist whose site_url is a
-- host that many others link to; purging them deletes all of those. The count returned shows it.
drop function if exists public.purge_removed(); -- the first version purged every pending request
create or replace function public.purge_removed(p_slugs text[]) returns int
language plpgsql set search_path = ''
as $$
declare
  before int := (select count(*) from public.artists);
begin
  insert into public.removed (key_hash)
  select distinct k
  from public.artists a
  join public.removal_requests r on r.artist_slug = a.slug,
  unnest(public.removal_keys(a.da_username, a.site_url)) k
  where a.slug = any (p_slugs)
  on conflict (key_hash) do nothing;
  delete from public.removal_requests r
  where not exists (select 1 from public.artists a where a.slug = r.artist_slug);
  return before - (select count(*) from public.artists);
end
$$;

-- New functions are executable by everyone by default. Take that back, then grant exactly what is meant.
revoke all on function
  public.words(text[]),
  public.clean_url(text),
  public.clean_links(jsonb),
  public.site_host(text),
  public.short_list(text[], int, int),
  public.removal_keys(text, text),
  public.barring_keys(text, text, jsonb),
  public.is_barred(text, text, jsonb),
  public.artists_before_write(),
  public.removed_after_insert(),
  public.skip_unchanged(),
  public.work_cards(bigint, int),
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.refresh_style_centroids(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.reject_removal(text),
  public.purge_removed(text[])
from public, anon, authenticated;

grant execute on function
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text)
to anon;

grant execute on function
  public.words(text[]),
  public.clean_url(text),
  public.clean_links(jsonb),
  public.site_host(text),
  public.short_list(text[], int, int),
  public.removal_keys(text, text),
  public.barring_keys(text, text, jsonb),
  public.is_barred(text, text, jsonb),
  public.work_cards(bigint, int),
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.refresh_style_centroids(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.reject_removal(text),
  public.purge_removed(text[])
to service_role;

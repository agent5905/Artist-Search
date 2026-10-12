-- Search, profile and removal functions. Safe to rerun.
--
-- The public functions are SECURITY DEFINER with an empty search_path: they run as the table
-- owner, which is how the public role reads tables it has no access to. Every one that returns
-- artists filters out hidden ones, and returns each as one JSON card (see artist_card).

-- An earlier version returned rows, which "create or replace" cannot change into cards. Dropped once.
do $$
declare
  old regprocedure;
begin
  for old in
    select p.oid::regprocedure from pg_proc p
    where p.pronamespace = 'public'::regnamespace
      and p.proname in ('search_artists', 'similar_artists', 'get_artist')
      and p.prorettype <> 'jsonb'::regtype
  loop
    execute format('drop function %s', old);
  end loop;
end
$$;
drop function if exists public.work_cards(bigint, int); -- now part of artist_card
drop function if exists public.purge_removed(); -- the first version purged every pending request

-- What a removal remembers of an artist without keeping their name (hard rule 7): sha256 hex of
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
-- And an update that changes nothing writes nothing (see skip_unchanged): a load that sends every
-- artist again leaves the table, and the keyword index, exactly as they were.
create or replace function public.artists_before_write() returns trigger
language plpgsql set search_path = ''
as $$
declare
  derived constant text[] := '{fts,profile_bits,updated_at}';
begin
  if tg_op = 'UPDATE' and to_jsonb(new) - derived = to_jsonb(old) - derived then
    return null;
  end if;
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
  new.updated_at := now();
  return new;
end
$$;

create or replace trigger artists_before_write before insert or update on public.artists
for each row execute function public.artists_before_write();

-- The other half of the rule: when keys join the removal list, by a removal or by hand, every artist
-- they bar is deleted (works cascade), with what was pending for them. So no row a removed key bars
-- ever stays in the index.
create or replace function public.removed_after_insert() returns trigger
language plpgsql set search_path = ''
as $$
begin
  delete from public.artists a where public.is_barred(a.da_username, a.site_url, a.links);
  delete from public.removal_requests r
  where not exists (select 1 from public.artists a where a.slug = r.artist_slug);
  delete from public.removal_proofs p
  where not exists (select 1 from public.artists a where a.slug = p.artist_slug);
  return null;
end
$$;

create or replace trigger removed_after_insert after insert on public.removed
for each statement execute function public.removed_after_insert();

-- An upsert of a row that already holds the same values writes nothing: no new row version, no dead
-- one for vacuum to clear. (Postgres's own suppress_redundant_updates_trigger compares bytes, and a
-- compressed stored value never equals a fresh one.) The trigger's arguments name the generated
-- columns, which are not filled in yet on the new row and so are left out of the comparison.
create or replace function public.skip_unchanged() returns trigger
language plpgsql set search_path = ''
as $$
begin
  if to_jsonb(new) - tg_argv = to_jsonb(old) - tg_argv then
    return null;
  end if;
  return new;
end
$$;

create or replace trigger works_skip_unchanged before update on public.works
for each row execute function public.skip_unchanged('key');

-- Service role only. Every artist's id by slug, for the push stage to attach works with: an upsert
-- does not return the artists it left unchanged.
create or replace function public.artist_ids() returns jsonb
language sql stable set search_path = ''
as $$
  select coalesce(jsonb_object_agg(slug, id), '{}') from public.artists
$$;

-- What every public function returns for an artist: the card. Each work names its source, so the
-- site can credit a DeviantArt thumbnail and link back. p_work_limit null means every work.
create or replace function public.artist_card(p_artist_id bigint, p_work_limit int default 4) returns jsonb
language sql stable set search_path = ''
as $$
  select jsonb_build_object(
    'slug', a.slug, 'name', a.name, 'tagline', a.tagline, 'profile', a.profile,
    'specialties', a.specialties, 'mediums', a.mediums, 'open_for_work', a.open_for_work,
    'works', (
      select coalesce(
        jsonb_agg(jsonb_build_object(
          'image_url', w.image_url, 'page_url', w.page_url, 'title', w.title, 'source', w.source
        ) order by w.position, w.id),
        '[]')
      from (
        select * from public.works where artist_id = a.id order by position, id limit p_work_limit
      ) w
    ))
  from public.artists a
  where a.id = p_artist_id
$$;

create or replace function public.search_artists(
  q text,
  q_embedding extensions.halfvec(1024) default null,
  p_mediums text[] default null,
  p_specialties text[] default null,
  p_open_only boolean default false,
  p_limit int default 24,
  p_offset int default 0
) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  with typed as materialized (
    -- Only the first 200 characters are read: a brief fits, and a huge query cannot be used to stall the database.
    select left(coalesce(q, ''), 200) as q
  ),
  respelled as materialized (
    -- A word of four letters or more that no artist uses is probably misspelled: the nearest word
    -- that is used (by shared letter triples, pg_trgm's default 0.3 or more) is searched for as well.
    -- ponytail: plain a-z words only, and a rare correct word can pick up a wrong neighbour.
    -- Ceiling: the text search critic (piece 5) finds a harmful correction.
    select coalesce(string_agg(nearest.word, ' '), '') as q
    from typed
    cross join lateral unnest(tsvector_to_array(to_tsvector('simple', typed.q))) as written (word)
    cross join lateral (
      select known.word
      from public.search_words known
      where length(written.word) >= 4
        and not exists (select 1 from public.search_words same where same.word = written.word)
        and known.word operator(extensions.%) written.word
      order by extensions.similarity(known.word, written.word) desc, known.artists desc, known.word
      limit 1
    ) nearest
  ),
  query as materialized (
    -- ponytail: any word of the query may match; no phrases, no exclusions.
    -- Ceiling: the text search critic (piece 5) asks for more.
    select replace(
      (plainto_tsquery('english', typed.q) || plainto_tsquery('english', respelled.q))::text, '&', '|'
    )::tsquery as tsq
    from typed, respelled
  ),
  -- Only the first 20 values of a filter list are read, and values longer than a stored one can be
  -- (60 characters) are dropped, so a huge list costs nothing. A list that was given always filters,
  -- even when none of its values is left.
  wanted as materialized (
    select coalesce(cardinality(p_mediums), 0) > 0 as by_medium,
           array(select lower(v) from (select v from unnest(p_mediums[1:20]) v limit 20) first
                 where length(v) <= 60) as mediums,
           coalesce(cardinality(p_specialties), 0) > 0 as by_specialty,
           array(select lower(v) from (select v from unnest(p_specialties[1:20]) v limit 20) first
                 where length(v) <= 60) as specialties
  ),
  target as materialized (
    select extensions.binary_quantize(q_embedding) as bits where q_embedding is not null
  ),
  -- Pass 1: rule out on hard facts. A null or empty list means no filter; a list matches any of its values.
  -- Not materialized, so the keyword pass below can go through the index instead of reading every artist.
  cand as not materialized (
    select a.id, a.name, a.fts, a.profile_bits
    from public.artists a, wanted
    where not a.hidden
      and (not coalesce(p_open_only, false) or a.open_for_work)
      and (not wanted.by_medium or exists (
        select 1 from unnest(a.mediums) have where lower(have) = any (wanted.mediums)))
      and (not wanted.by_specialty or exists (
        select 1 from unnest(a.specialties) have where lower(have) = any (wanted.specialties)))
  ),
  -- Pass 2: rank what is left by keywords and, when an embedding is given, by meaning.
  kw as materialized (
    select c.id, c.name, c.profile_bits is not null as analyzed,
           row_number() over (order by ts_rank(c.fts, query.tsq) desc, c.id) as r
    from cand c, query
    where c.fts @@ query.tsq
  ),
  -- Meaning, in two steps so that only 100 full vectors are fetched: every analyzed artist in order
  -- of sign-bit distance (128 bytes kept in the row), then the nearest 100 put in exact order.
  coarse as materialized (
    select c.id, c.name, row_number() over (order by c.profile_bits operator(extensions.<~>) target.bits, c.id) as r
    from cand c, target
    where c.profile_bits is not null
  ),
  fine as materialized (
    select coarse.id,
           row_number() over (order by a.profile_embedding operator(extensions.<=>) q_embedding, coarse.id) as r
    from coarse join public.artists a using (id)
    where coarse.r <= 100
  ),
  -- An artist not analyzed yet has no place in the order by meaning. Two things keep that from
  -- counting for or against them: their keyword place stands in for it, and everyone else's place
  -- by meaning is stretched as if the unanalyzed artists were ranked among them (by the share of
  -- keyword matches that are analyzed).
  stretch as materialized (
    select greatest(count(*), 1)::numeric / greatest(count(*) filter (where analyzed), 1) as factor from kw
  ),
  sem as (
    select coarse.id, coarse.name, coalesce(fine.r, coarse.r) * stretch.factor as r
    from coarse left join fine using (id), stretch
  ),
  ranked as (
    -- Reciprocal rank fusion with the usual constant of 60.
    -- ponytail: no similarity floor, so with an embedding every analyzed candidate is returned, best first.
    -- Ceiling: piece 5 wants a cutoff.
    select coalesce(kw.id, sem.id) as id, coalesce(kw.name, sem.name) as name,
           coalesce(1.0 / (60 + kw.r), 0) + coalesce(1.0 / (60 + sem.r), 1.0 / (60 + kw.r), 0) as score
    from kw full join sem on sem.id = kw.id
    union all
    -- No query at all: browse the filtered set.
    select c.id, c.name, 0 from cand c, query where numnode(query.tsq) = 0 and q_embedding is null
  )
  select public.artist_card(ranked.id, 4)
  from ranked
  order by ranked.score desc, ranked.name, ranked.id
  limit least(greatest(coalesce(p_limit, 24), 1), 100) offset greatest(coalesce(p_offset, 0), 0)
$$;

-- Service role only. Rebuilds artist_styles from works: each artist's first 10 style vectors by
-- position, and the sign bits of their mean. db.after_load() calls it; similar_artists cannot see
-- an artist, as the seed or as a result, until it has run. Returns how many rows changed: an artist
-- whose vectors are the same is not rewritten.
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

-- Service role only. Rebuilds search_words: every plain word of four letters or more in what
-- keyword search reads of an artist, with how many artists use it. db.after_load() calls it; until
-- then a misspelling can only be put right to words that were already known. Returns how many rows changed.
create or replace function public.refresh_search_words() returns int
language sql set search_path = ''
as $$
  with fresh as (
    select word, ndoc as artists
    from ts_stat($words$
      select to_tsvector('simple', concat_ws(' ', name, tagline, profile, search_text,
        array_to_string(specialties, ' '), array_to_string(mediums, ' '), array_to_string(top_tags, ' ')))
      from public.artists
    $words$)
    where word ~ '^[a-z]{4,40}$'
  ),
  gone as (
    delete from public.search_words w
    where not exists (select 1 from fresh where fresh.word = w.word)
    returning 1
  ),
  written as (
    insert into public.search_words as w (word, artists)
    select word, artists from fresh
    on conflict (word) do update set artists = excluded.artists where w.artists is distinct from excluded.artists
    returning 1
  )
  select (select count(*) from gone)::int + (select count(*) from written)::int
$$;

-- Other artists ranked by style, in two stages, both read from artist_styles:
--   1. shortlist the 100 artists whose mean style is nearest the seed's, by the sign bits alone;
--   2. score only those exactly: the mean, over the seed's works, of the best cosine match among
--      the candidate's works.
-- Only an artist's first 10 works by position take part, on both sides. An artist with no style
-- vectors yet gets an empty answer.
-- ponytail: sign bits are a coarse measure, and an artist outside the nearest 100 cannot be returned
-- however well one work matches. Ceiling: the style critic (piece 6) finds a miss; then widen the
-- shortlist, or keep the mean vector itself in artist_styles and rank by it.
create or replace function public.similar_artists(p_slug text, p_limit int default 24) returns setof jsonb
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
  select public.artist_card(scored.artist_id, 4)
  from scored
  order by scored.score desc, scored.artist_id
  limit least(greatest(coalesce(p_limit, 24), 1), 100)
$$;

-- The profile page: the card with every work, the artist's own tags and links, and their DeviantArt page.
create or replace function public.get_artist(p_slug text) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.artist_card(a.id, null) || jsonb_build_object(
    'top_tags', a.top_tags, 'links', a.links, 'deviantart_url', 'https://www.deviantart.com/' || a.da_username)
  from public.artists a
  where a.slug = p_slug and not a.hidden
$$;

-- Removal (hard rule 7, owner decision 6). Nothing the public can call changes what is shown:
--   * request_removal logs a request for the owner;
--   * start_removal_proof hands out a code to put on the artist's own site or DeviantArt profile;
--   * confirm_removal (service role: the verifier, once it has seen the code there) removes the
--     artist at once and for good;
--   * purge_removed and reject_removal (service role) are the owner's answer to an unproven request.

-- Logs a request. The newest five per artist are kept, so a new one is always recorded and the
-- table cannot be flooded. Always returns true, whatever the slug, so it gives nothing away.
create or replace function public.request_removal(p_slug text, p_note text default null) returns boolean
language sql security definer set search_path = ''
as $$
  -- One request per artist at a time, so two cannot both count four and leave six.
  select pg_advisory_xact_lock(7, hashtext(p_slug));
  with logged as (
    insert into public.removal_requests (artist_slug, note)
    select a.slug, left(p_note, 1000) from public.artists a where a.slug = p_slug
  ),
  displaced as (
    delete from public.removal_requests
    where id in (
        select id from public.removal_requests
        where artist_slug = p_slug
        order by created_at desc, id desc
        offset 4)
      and exists (select 1 from public.artists a where a.slug = p_slug)
  )
  select true
$$;

-- Returns a fresh code and stores only its hash, good for 7 days. One code per artist: asking again
-- replaces it. For a slug that is unknown or hidden the code looks the same and is stored nowhere,
-- so the answer gives nothing away.
-- ponytail: anyone can ask, so anyone can replace the code an artist is in the middle of placing.
-- Ceiling: the first artist it happens to; then keep the last few codes valid together.
create or replace function public.start_removal_proof(p_slug text) returns text
language sql security definer set search_path = ''
as $$
  with fresh as materialized (
    select 'artist-search-removal-' || replace(gen_random_uuid()::text, '-', '') as code
  ),
  stored as (
    insert into public.removal_proofs (artist_slug, code_hash, expires_at)
    select a.slug, encode(sha256(convert_to(fresh.code, 'UTF8')), 'hex'), now() + interval '7 days'
    from public.artists a, fresh
    where a.slug = p_slug and not a.hidden
    on conflict (artist_slug) do update set code_hash = excluded.code_hash, expires_at = excluded.expires_at
  )
  select code from fresh
$$;

-- Service role only. Removes artists outright: their keys go on the removal list, which deletes
-- them (works cascade) and every other row those keys bar: a second copy on a subdomain, or one
-- that only links to the removed site or profile. Returns how many artists were deleted.
-- ponytail: no check that the site is the artist's alone. Ceiling: an artist whose site_url is a
-- host that many others link to; removing them deletes all of those. The count returned shows it.
create or replace function public.remove_artists(p_slugs text[]) returns int
language plpgsql set search_path = ''
as $$
declare
  before int := (select count(*) from public.artists);
begin
  insert into public.removed (key_hash)
  select distinct k
  from public.artists a, unnest(public.removal_keys(a.da_username, a.site_url)) k
  where a.slug = any (p_slugs)
  on conflict (key_hash) do nothing;
  return before - (select count(*) from public.artists);
end
$$;

-- Service role only: the verifier calls it once it has seen the code on the artist's own page.
-- True when the code is the artist's current one and has not expired: the artist is then removed at
-- once and can never be re-added. False, and nothing changes, for any other code.
create or replace function public.confirm_removal(p_slug text, p_code text) returns boolean
language plpgsql set search_path = ''
as $$
begin
  if not exists (
    select 1 from public.removal_proofs
    where artist_slug = p_slug and expires_at > now()
      and code_hash = encode(sha256(convert_to(p_code, 'UTF8')), 'hex')
  ) then
    return false;
  end if;
  return public.remove_artists(array[p_slug]) > 0;
end
$$;

-- Service role only. The owner turns down an artist's unproven requests: they are deleted, and
-- nothing else changes. Returns whether there were any.
create or replace function public.reject_removal(p_slug text) returns boolean
language sql set search_path = ''
as $$
  with gone as (
    delete from public.removal_requests where artist_slug = p_slug returning 1
  )
  select exists (select 1 from gone)
$$;

-- Service role only. The owner grants unproven requests after reviewing them: each slug in p_slugs
-- that has a pending request is removed as confirm_removal would. Returns how many artists were deleted.
create or replace function public.purge_removed(p_slugs text[]) returns int
language sql set search_path = ''
as $$
  select public.remove_artists(array(
    select distinct r.artist_slug from public.removal_requests r where r.artist_slug = any (p_slugs)
  ))
$$;

-- New functions are executable by everyone by default. Take that back, then grant exactly what is meant.
revoke all on function
  public.clean_url(text),
  public.clean_links(jsonb),
  public.site_host(text),
  public.short_list(text[], int, int),
  public.artist_fts(text, text, text, text[], text[], text[], text),
  public.removal_keys(text, text),
  public.barring_keys(text, text, jsonb),
  public.is_barred(text, text, jsonb),
  public.artists_before_write(),
  public.removed_after_insert(),
  public.skip_unchanged(),
  public.artist_ids(),
  public.artist_card(bigint, int),
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.refresh_style_centroids(),
  public.refresh_search_words(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.start_removal_proof(text),
  public.remove_artists(text[]),
  public.confirm_removal(text, text),
  public.reject_removal(text),
  public.purge_removed(text[])
from public, anon, authenticated;

grant execute on function
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.start_removal_proof(text)
to anon;

grant execute on function
  public.clean_url(text),
  public.clean_links(jsonb),
  public.site_host(text),
  public.short_list(text[], int, int),
  public.artist_fts(text, text, text, text[], text[], text[], text),
  public.removal_keys(text, text),
  public.barring_keys(text, text, jsonb),
  public.is_barred(text, text, jsonb),
  public.artist_ids(),
  public.artist_card(bigint, int),
  public.search_artists(text, extensions.halfvec, text[], text[], boolean, int, int),
  public.refresh_style_centroids(),
  public.refresh_search_words(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.start_removal_proof(text),
  public.remove_artists(text[]),
  public.confirm_removal(text, text),
  public.reject_removal(text),
  public.purge_removed(text[])
to service_role;

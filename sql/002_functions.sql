-- Search, profile and removal functions. Safe to rerun.
--
-- The public functions are SECURITY DEFINER with an empty search_path: they run as the table
-- owner, which is how the public role reads tables it has no access to. Every one that returns
-- artists leaves out hidden ones and removed ones, and returns each as one JSON card (see artist_card).

-- A function of the previous version whose arguments changed.
drop function if exists public.artist_card(bigint, int); -- it takes the search's words as well now

-- What a removal remembers of an artist without keeping their name (hard rule 7): the hashes of
-- 'da:<lowercased handle>' and 'site:<registrable domain>'. The same keys as pipeline/store.py, so
-- a hash made here matches one made on the owner's machine. They are also what bars a row: nothing
-- is normalized here, because the constraints in 001_schema.sql refuse any other spelling.
create or replace function public.artist_keys(p_da_username text, p_site_domain text) returns text[]
language sql immutable set search_path = ''
as $$
  select array_remove(array[public.key_hash('da:' || lower(p_da_username)), public.key_hash('site:' || p_site_domain)],
                      null)
$$;

-- Hard rule 7, enforced where no pipeline bug can get around it. No row may take on a handle or a
-- site that a removed artist is known by: an insert of one is skipped (so an upsert carries on with
-- the other rows) and an update onto one is refused. A write that leaves the two alone is always
-- let through, so an artist can be hidden or edited whatever the removal list says.
-- And an update that changes nothing writes nothing: a load that sends every artist again leaves the
-- table, and the keyword index, exactly as they were. (Derived columns are left out of the
-- comparison: the generated ones are not filled in yet on the new row.)
create or replace function public.artists_before_write() returns trigger
language plpgsql set search_path = ''
as $$
declare
  derived constant text[] := '{fts,profile_bits,da_key,site_key,updated_at}';
begin
  if tg_op = 'UPDATE' and to_jsonb(new) - derived = to_jsonb(old) - derived then
    return null;
  end if;
  if tg_op = 'UPDATE'
     and (new.da_username, new.site_domain) is not distinct from (old.da_username, old.site_domain)
  then
    null;
  elsif exists (
    select 1 from public.removed where key_hash = any (public.artist_keys(new.da_username, new.site_domain))
  ) then
    if tg_op = 'INSERT' then
      return null;
    end if;
    raise exception 'this handle or site belongs to a removed artist' using errcode = 'check_violation';
  end if;
  new.updated_at := now();
  return new;
end
$$;

create or replace trigger artists_before_write before insert or update on public.artists
for each row execute function public.artists_before_write();

-- A write that races a removal passes the check above, because the removal had not committed yet.
-- This one runs when the write commits, by which time it has: the write then fails as a whole.
create or replace function public.artists_not_removed() returns trigger
language plpgsql set search_path = ''
as $$
begin
  if exists (select 1 from public.removed where key_hash in (new.da_key, new.site_key)) then
    raise exception 'this handle or site belongs to a removed artist' using errcode = 'check_violation';
  end if;
  return null;
end
$$;

do $$
begin
  if not exists (
    select 1 from pg_trigger where tgrelid = 'public.artists'::regclass and tgname = 'artists_not_removed'
  ) then
    create constraint trigger artists_not_removed after insert or update on public.artists
    deferrable initially deferred for each row execute function public.artists_not_removed();
  end if;
end
$$;

-- Service role only. Deletes the artists that keys on the removal list bar (works cascade), found by
-- index, with what was pending for them. p_keys null means every key on the list: db.after_load()
-- runs that as a sweep. A row another transaction holds is skipped rather than waited for, so a
-- removal never queues behind a load; the public functions leave such a row out until it is swept.
-- Returns how many artists were deleted.
create or replace function public.delete_removed(p_keys text[] default null) returns int
language plpgsql set search_path = ''
as $$
declare
  keys text[] := coalesce(p_keys, array(select key_hash from public.removed));
  gone int;
begin
  with deleted as (
    delete from public.artists
    where id in (
      select a.id from public.artists a
      where a.da_key = any (keys) or a.site_key = any (keys)
      for update skip locked)
    returning 1
  )
  select count(*) into gone from deleted;
  delete from public.removal_requests r
  where not exists (select 1 from public.artists a where a.slug = r.artist_slug);
  return gone;
end
$$;

-- When keys join the removal list, by a removal or by hand, the artists they bar go at once, and
-- the word list is rebuilt so that none of their words is left to steer a search.
create or replace function public.removed_after_insert() returns trigger
language plpgsql set search_path = ''
as $$
begin
  perform public.delete_removed(array(select key_hash from added));
  perform public.refresh_search_words();
  return null;
end
$$;

create or replace trigger removed_after_insert after insert on public.removed
referencing new table as added
for each statement execute function public.removed_after_insert();

-- A work on the artist's own site must have both URLs on that site (hard rule 1: a thumbnail is
-- shown from where the artist put it). And an upsert of a work that already holds the same values
-- writes nothing: no new row version, no dead one for vacuum to clear. (The generated key is not
-- filled in yet on the new row, so it is left out of the comparison.)
create or replace function public.works_before_write() returns trigger
language plpgsql set search_path = ''
as $$
declare
  domain text;
begin
  if tg_op = 'UPDATE' and to_jsonb(new) - 'key' = to_jsonb(old) - 'key' then
    return null;
  end if;
  if new.source = 'site' then
    select site_domain into domain from public.artists where id = new.artist_id;
    if domain is null
       or not public.host_under(public.url_host(new.page_url), domain)
       or not public.host_under(public.url_host(new.image_url), domain)
    then
      raise exception 'a work on an artist''s own site must have its page and image on that site'
        using errcode = 'check_violation';
    end if;
  end if;
  return new;
end
$$;

create or replace trigger works_before_write before insert or update on public.works
for each row execute function public.works_before_write();

-- Service role only. Artists' ids by slug, for the push stage to attach works with: an upsert does
-- not return the artists it left unchanged. p_slugs null means every artist.
create or replace function public.artist_ids(p_slugs text[] default null) returns jsonb
language sql stable set search_path = ''
as $$
  select coalesce(jsonb_object_agg(slug, id), '{}') from public.artists
  where p_slugs is null or slug = any (p_slugs)
$$;

-- What every public function returns for an artist: the card. Each work names its source, so the
-- site can credit a DeviantArt thumbnail and link back, and its size in pixels when that is known
-- (width and height, both null otherwise). p_work_limit null means every work.
-- The works come in the artist's own order (position), unless p_matching is given: search_artists
-- passes the words searched for, and then the works that say those words of themselves come first,
-- the best match first (the tags count most, then the title, then the description), and the rest
-- follow in the artist's order. So a card found for "creature design" leads with the creatures.
create or replace function public.artist_card(
  p_artist_id bigint, p_work_limit int default 4, p_matching tsquery default null
) returns jsonb
language sql stable set search_path = ''
as $$
  select jsonb_build_object(
    'slug', a.slug, 'name', a.name, 'tagline', a.tagline, 'profile', a.profile,
    'specialties', a.specialties, 'mediums', a.mediums, 'open_for_work', a.open_for_work,
    'works', (
      select coalesce(
        jsonb_agg(jsonb_build_object(
          'image_url', w.image_url, 'page_url', w.page_url, 'title', w.title, 'source', w.source,
          'width', w.width, 'height', w.height
        ) order by w.fit desc, w.position, w.id),
        '[]')
      from (
        -- No words: the first works by position, and no others are read.
        (select works.*, 0::real as fit
         from public.works
         where p_matching is null and artist_id = a.id
         order by position, id limit p_work_limit)
        union all
        -- ponytail: with words, every work of the artist is weighed on the spot (about ten a card).
        -- Ceiling: artists with hundreds of works; then store the work's tsvector in a generated column.
        (select works.*,
                ts_rank(
                  setweight(to_tsvector('english', array_to_string(tags, ' ')), 'A')
                  || setweight(to_tsvector('english', coalesce(title, '')), 'B')
                  || setweight(to_tsvector('english', coalesce(description, '')), 'C'),
                  p_matching) as fit
         from public.works
         where p_matching is not null and artist_id = a.id
         order by fit desc, position, id limit p_work_limit)
      ) w
    ))
  from public.artists a
  where a.id = p_artist_id
$$;

-- The search embedding as a vector. A list of any other length is refused with a plain message,
-- not passed on to fail somewhere it cannot explain itself.
create or replace function public.query_embedding(p_numbers real[]) returns extensions.halfvec
language plpgsql immutable set search_path = ''
as $$
begin
  if p_numbers is null then
    return null;
  end if;
  if coalesce(array_ndims(p_numbers), 0) <> 1 or cardinality(p_numbers) <> 1024 then
    raise exception 'the embedding must be a list of 1,024 numbers' using errcode = 'invalid_parameter_value';
  end if;
  return p_numbers::extensions.halfvec(1024);
end
$$;

create or replace function public.search_artists(
  q text,
  q_embedding real[] default null,
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
    select t.e, extensions.binary_quantize(t.e) as bits
    from (select public.query_embedding(q_embedding) as e) t
    where t.e is not null
  ),
  -- Pass 1: rule out on hard facts. A null or empty list means no filter; a list matches any of its values.
  -- Not materialized, so the keyword pass below can go through the index instead of reading every artist.
  cand as not materialized (
    select a.id, a.name, a.fts, a.profile_bits
    from public.artists a, wanted
    where not a.hidden
      and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key))
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
           row_number() over (order by a.profile_embedding operator(extensions.<=>) target.e, coarse.id) as r
    from coarse join public.artists a using (id), target
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
    select c.id, c.name, 0 from cand c, query where numnode(query.tsq) = 0 and not exists (select 1 from target)
  ),
  -- The page is picked before any card is built, so a card is built only for the rows returned.
  -- Pages start no deeper than the 1,000th result: past that the answer is empty.
  page as materialized (
    select ranked.id, ranked.score, ranked.name
    from ranked
    where coalesce(p_offset, 0) <= 1000
    order by ranked.score desc, ranked.name, ranked.id
    limit least(greatest(coalesce(p_limit, 24), 1), 100) offset greatest(coalesce(p_offset, 0), 0)
  )
  -- Each card leads with the works that match the words searched for, if there are any.
  select public.artist_card(page.id, 4, case when numnode(query.tsq) > 0 then query.tsq end)
  from page, query
  order by page.score desc, page.name, page.id
$$;

-- Service role only: db.load() calls it. p_rows is a JSON list of
-- {"artist_slug", "neighbor_slug", "rank", "score"}. Every artist named as an artist_slug ends up with
-- exactly the listed rows whose two artists are both in the database: what they had at any other rank
-- is deleted, and a row that already holds the same values is not rewritten. A row naming an artist
-- who is not in the database (removed, or never loaded) is skipped. Returns how many were skipped.
-- An artist named in no row keeps the neighbours they have. To leave an artist with none, send one
-- row for them that names no neighbour: {"artist_slug": ..., "neighbor_slug": null, "rank": null,
-- "score": null}. It writes nothing, so what the artist had is deleted, and it is counted with the
-- skipped rows: a caller that sends such rows takes their number off the answer.
create or replace function public.replace_neighbors(p_rows jsonb) returns int
language sql set search_path = ''
as $$
  with listed as materialized (
    select r.artist_slug, r.neighbor_slug, r.rank, r.score
    from jsonb_to_recordset(p_rows) as r (artist_slug text, neighbor_slug text, rank smallint, score real)
  ),
  known as materialized (
    select a.id as artist_id, b.id as neighbor_id, l.rank, l.score
    from listed l
    join public.artists a on a.slug = l.artist_slug
    join public.artists b on b.slug = l.neighbor_slug
  ),
  stale as (
    delete from public.artist_neighbors n
    using public.artists a
    where a.id = n.artist_id and a.slug in (select artist_slug from listed)
      and not exists (select 1 from known k where k.artist_id = n.artist_id and k.rank = n.rank)
  ),
  written as (
    insert into public.artist_neighbors as n (artist_id, neighbor_id, rank, score)
    select artist_id, neighbor_id, rank, score from known
    on conflict (artist_id, rank) do update
      set neighbor_id = excluded.neighbor_id, score = excluded.score
      where (n.neighbor_id, n.score) is distinct from (excluded.neighbor_id, excluded.score)
  )
  select (select count(*) from listed)::int - (select count(*) from known)::int
$$;

-- Service role only. Rebuilds search_words: every plain word of four letters or more in what
-- keyword search reads of an artist the public can see, with how many artists use it. A hidden or
-- removed artist's words are not in it. db.after_load() and every removal call it; until then a
-- misspelling can only be put right to words that were already known. Returns how many rows changed.
create or replace function public.refresh_search_words() returns int
language sql set search_path = ''
as $$
  with fresh as (
    select word, ndoc as artists
    from ts_stat($words$
      select to_tsvector('simple', concat_ws(' ', a.name, a.tagline, a.profile, a.search_text,
        array_to_string(a.specialties, ' '), array_to_string(a.mediums, ' '), array_to_string(a.top_tags, ' ')))
      from public.artists a
      where not a.hidden
        and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key))
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

-- Other artists nearest in style: the artist's stored neighbours, best first, read by key. Nothing is
-- computed here, so a call costs a handful of rows whatever the size of the index. An artist with no
-- stored neighbours gets an empty answer. At most 24 are returned, whatever p_limit says, and a
-- hidden or removed neighbour takes up none of them.
create or replace function public.similar_artists(p_slug text, p_limit int default 24) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  with page as materialized (
    select n.neighbor_id, n.rank
    from public.artists seed
    join public.artist_neighbors n on n.artist_id = seed.id
    join public.artists a on a.id = n.neighbor_id
    where seed.slug = p_slug and not seed.hidden and not a.hidden
      and not exists (
        select 1 from public.removed gone
        where gone.key_hash in (seed.da_key, seed.site_key, a.da_key, a.site_key))
    order by n.rank
    limit least(greatest(coalesce(p_limit, 24), 1), 24)
  )
  select public.artist_card(page.neighbor_id, 4)
  from page
  order by page.rank
$$;

-- The profile page: the card with every work, the artist's own tags, their site and their DeviantArt page.
create or replace function public.get_artist(p_slug text) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.artist_card(a.id, null) || jsonb_build_object(
    'top_tags', a.top_tags, 'site_url', a.site_url, 'deviantart_url', 'https://www.deviantart.com/' || a.da_username)
  from public.artists a
  where a.slug = p_slug and not a.hidden
    and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key))
$$;

-- Removal (hard rule 7, owner decision 6). Nothing the public can call changes what is shown, and
-- nothing the public can call undoes what an artist did:
--   * request_removal logs a request for the owner;
--   * start_removal_proof tells the artist the code to put on their own site or DeviantArt profile;
--   * confirm_removal (service role: the verifier, once it has seen the code there) removes the
--     artist at once and for good;
--   * purge_removed and reject_removal (service role) are the owner's answer to an unproven request.

-- Logs a request: one row per artist, with the first request's note. Asking again only counts.
-- Always returns true, whatever the slug, so it gives nothing away.
create or replace function public.request_removal(p_slug text, p_note text default null) returns boolean
language sql security definer set search_path = ''
as $$
  with logged as (
    insert into public.removal_requests as r (artist_slug, note)
    select a.slug, left(p_note, 300) from public.artists a where a.slug = p_slug
    on conflict (artist_slug) do update set last_at = now(), requests = r.requests + 1
  )
  select true
$$;

-- The proof code for a slug: derived from the slug and a secret, so it is the same every time, it is
-- stored nowhere, and nobody without the secret can work it out. Not callable directly by any role.
create or replace function public.removal_code(p_slug text) returns text
language sql stable security definer set search_path = ''
as $$
  select 'artist-search-removal-'
      || left(encode(extensions.hmac(coalesce(p_slug, ''), secret, 'sha256'), 'hex'), 32)
  from public.removal_secret
$$;

-- Tells the artist their code. It never changes and nothing is written, so no other caller can
-- spoil it, and the answer has the same shape whether the slug is unknown, hidden or visible.
create or replace function public.start_removal_proof(p_slug text) returns text
language sql stable security definer set search_path = ''
as $$
  select public.removal_code(p_slug)
$$;

-- Service role only. Removes artists outright: their keys go on the removal list, which deletes
-- them (works cascade) and bars their handle and site for good. It reads the artists without
-- locking them and never waits for a load. Returns how many artists were deleted; one that a load
-- held at that moment is not counted, is hidden from the public at once all the same, and goes with
-- the next sweep.
create or replace function public.remove_artists(p_slugs text[]) returns int
language plpgsql set search_path = ''
as $$
declare
  before int := (select count(*) from public.artists);
begin
  insert into public.removed (key_hash)
  select distinct k
  from public.artists a, unnest(public.artist_keys(a.da_username, a.site_domain)) k
  where a.slug = any (p_slugs)
  on conflict (key_hash) do nothing;
  return before - (select count(*) from public.artists);
end
$$;

-- Service role only: the verifier calls it once it has seen the code on the artist's own page.
-- True when the slug is an artist's and the code is theirs: the artist is then removed at once and
-- can never be re-added. False, and nothing changes, for anything else.
create or replace function public.confirm_removal(p_slug text, p_code text) returns boolean
language plpgsql security definer set search_path = ''
as $$
begin
  if p_code is distinct from public.removal_code(p_slug)
     or not exists (select 1 from public.artists where slug = p_slug)
  then
    return false;
  end if;
  perform public.remove_artists(array[p_slug]);
  return true;
end
$$;

-- Service role only. The owner turns down an artist's unproven request: it is deleted, and nothing
-- else changes. Returns whether there was one.
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
    select r.artist_slug from public.removal_requests r where r.artist_slug = any (p_slugs)
  ))
$$;

-- Functions are not executable by the public unless granted (see the default privileges in
-- 001_schema.sql). Take back anything an earlier version left, then grant exactly what is meant.
revoke all on function
  public.clean_url(text),
  public.url_host(text),
  public.host_under(text, text),
  public.short_list(text[], int, int),
  public.key_hash(text),
  public.artist_fts(text, text, text, text[], text[], text[], text),
  public.artist_keys(text, text),
  public.artists_before_write(),
  public.artists_not_removed(),
  public.delete_removed(text[]),
  public.removed_after_insert(),
  public.works_before_write(),
  public.artist_ids(text[]),
  public.artist_card(bigint, int, tsquery),
  public.query_embedding(real[]),
  public.search_artists(text, real[], text[], text[], boolean, int, int),
  public.replace_neighbors(jsonb),
  public.refresh_search_words(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.removal_code(text),
  public.start_removal_proof(text),
  public.remove_artists(text[]),
  public.confirm_removal(text, text),
  public.reject_removal(text),
  public.purge_removed(text[])
from public, anon, authenticated, service_role;

grant execute on function
  public.search_artists(text, real[], text[], text[], boolean, int, int),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text),
  public.start_removal_proof(text)
to anon;

grant execute on function
  public.clean_url(text),
  public.url_host(text),
  public.host_under(text, text),
  public.short_list(text[], int, int),
  public.key_hash(text),
  public.artist_fts(text, text, text, text[], text[], text[], text),
  public.artist_keys(text, text),
  public.delete_removed(text[]),
  public.artist_ids(text[]),
  public.artist_card(bigint, int, tsquery),
  public.query_embedding(real[]),
  public.search_artists(text, real[], text[], text[], boolean, int, int),
  public.replace_neighbors(jsonb),
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

-- Search, profile and removal functions. Safe to rerun.
--
-- The public functions are SECURITY DEFINER with an empty search_path: they run as the table
-- owner, which is how the public role reads tables it has no access to. Every one that returns
-- artists leaves out hidden ones and removed ones, and returns each as one JSON card (see artist_card).

-- A function of the previous version whose arguments changed.
drop function if exists public.artist_card(bigint, int); -- it takes the search's words as well now
drop function if exists public.artist_card(bigint, int, tsquery); -- and the works to lead with
-- A removal request is named by its id now, not its slug (sql/001_schema.sql: removal_requests).
drop function if exists public.close_request(text, text);
drop function if exists public.grant_request(text, text);
drop function if exists public.confirm_removal(text, text);
drop function if exists public.reject_removal(text);
drop function if exists public.purge_removed(text[]);
-- A request is bound to what the claim-or-remove page showed now (its ticket), and a code to the network it was
-- asked from.
drop function if exists public.request_removal(text, text);
drop function if exists public.start_removal_proof(text);

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

-- Hard rule 7, enforced where no pipeline bug can get around it. No row may take on a handle, a site or
-- an account that a removed artist is known by: an insert of one is skipped (so an upsert carries on with
-- the other rows) and an update onto one is refused. A write that leaves the three alone is always
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
  if tg_op = 'UPDATE' and (new.da_username, new.site_domain, new.da_account)
                          is not distinct from (old.da_username, old.site_domain, old.da_account)
  then
    null;
  elsif exists (
    select 1 from public.removed
    where key_hash = any (public.artist_keys(new.da_username, new.site_domain) || new.da_account)
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
  if exists (select 1 from public.removed where key_hash in (new.da_key, new.site_key, new.da_account)) then
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
-- index. p_keys null means every key on the list: db.after_load() runs that as a sweep. A row another
-- transaction holds is skipped rather than waited for, so a removal never queues behind a load; the
-- public functions leave such a row out until it is swept. Removal requests are not touched: only a
-- proven removal or the owner's answer closes one. Returns how many artists were deleted.
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
      where a.da_key = any (keys) or a.site_key = any (keys) or a.da_account = any (keys)
      for update skip locked)
    returning 1
  )
  select count(*) into gone from deleted;
  return gone;
end
$$;

-- When keys join the removal list, by a removal or by hand, the artists they bar go at once, and
-- the word list is rebuilt so that none of their words is left to steer a search. An open removal
-- request whose handle or account they bar has nothing left to ask, and is closed as 'removed', so it
-- keeps their handle no longer (close_request is in the removal functions below). Its site alone does
-- not close it: a site can pass to somebody else, and the request's artist is still there to ask.
-- The removals' lock first (see request_binding): a request filed meanwhile is closed here, or files nothing.
create or replace function public.removed_after_insert() returns trigger
language plpgsql set search_path = ''
as $$
begin
  perform pg_advisory_xact_lock(4207); -- the removals' lock
  perform public.delete_removed(array(select key_hash from added));
  perform public.close_request(r.id, 'removed')
  from public.removal_requests r
  where r.outcome is null
    and (r.da_key in (select key_hash from added) or r.da_account in (select key_hash from added));
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
-- Without words, p_first does the same by position: similar_artists passes a neighbour's picks, and the
-- works at those positions come first, in the order given, and the rest follow in the artist's order.
create or replace function public.artist_card(
  p_artist_id bigint, p_work_limit int default 4, p_matching tsquery default null, p_first smallint[] default null
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
        -- No words and none to lead with: the first works by position, and no others are read.
        (select works.*, 0::real as fit
         from public.works
         where p_matching is null and p_first is null and artist_id = a.id
         order by position, id limit p_work_limit)
        union all
        -- Works to lead with: every work of the artist is read (about ten a card), as with words below.
        (select works.*, coalesce(-array_position(p_first::int[], position)::real, '-infinity') as fit
         from public.works
         where p_matching is null and p_first is not null and artist_id = a.id
         order by fit desc, position, id limit p_work_limit)
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
      and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key, a.da_account))
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
-- {"artist_slug", "neighbor_slug", "rank", "score"}, each with "picks" when the neighbour's works to
-- show were picked (see artist_neighbors). Every artist named as an artist_slug ends up with
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
    select r.artist_slug, r.neighbor_slug, r.rank, r.score, r.picks
    from jsonb_to_recordset(p_rows)
      as r (artist_slug text, neighbor_slug text, rank smallint, score real, picks smallint[])
  ),
  known as materialized (
    select a.id as artist_id, b.id as neighbor_id, l.rank, l.score, l.picks
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
    insert into public.artist_neighbors as n (artist_id, neighbor_id, rank, score, picks)
    select artist_id, neighbor_id, rank, score, picks from known
    on conflict (artist_id, rank) do update
      set neighbor_id = excluded.neighbor_id, score = excluded.score, picks = excluded.picks
      where (n.neighbor_id, n.score, n.picks) is distinct from (excluded.neighbor_id, excluded.score, excluded.picks)
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
        and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key, a.da_account))
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
-- hidden or removed neighbour takes up none of them. Each card shows the neighbour's works that were
-- picked as closest to the seed's (artist_neighbors.picks), however few, and no others; with none
-- picked, their first four.
-- ponytail: a pick is a position, read when the card is built. Ceiling: while a load is between an
-- artist's works and the neighbour rows that name them, a card can show other works of theirs.
create or replace function public.similar_artists(p_slug text, p_limit int default 24) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  with page as materialized (
    select n.neighbor_id, n.rank, n.picks
    from public.artists seed
    join public.artist_neighbors n on n.artist_id = seed.id
    join public.artists a on a.id = n.neighbor_id
    where seed.slug = p_slug and not seed.hidden and not a.hidden
      and not exists (
        select 1 from public.removed gone
        where gone.key_hash in (seed.da_key, seed.site_key, seed.da_account, a.da_key, a.site_key, a.da_account))
    order by n.rank
    limit least(greatest(coalesce(p_limit, 24), 1), 24)
  )
  select public.artist_card(page.neighbor_id, coalesce(cardinality(page.picks), 4), null, page.picks)
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
    and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key, a.da_account))
$$;

-- Removal (hard rule 7, owner decision 6). Nothing a visitor can reach changes what is shown, and
-- nothing a visitor can reach undoes what an artist did:
--   * request_removal logs a request for the owner;
--   * start_removal_proof tells the artist the code to put on their own site or DeviantArt profile, and
--     from then on the verifier looks for it;
--   * confirm_removal (the verifier, once it has seen the code there) removes the artist at once and
--     for good;
--   * grant_request and reject_removal are the owner's answer to an unproven request.
-- A request stays open until one of the last three closes it, whatever happens to the artist's row
-- meanwhile, and it removes only the artist it was bound to when it was filed (sql/001_schema.sql:
-- removal_requests). All of them are the service role's: visitors reach the first two through the site's
-- server functions (site_removal_code and site_request_removal in sql/003_limits.sql), which count them.

-- What the claim-or-remove page shows of an artist (site_claim in sql/003_limits.sql) comes with a ticket: whom a
-- request from the page is to be bound to (p_bound: their slug, handle, site, its domain and their account), in
-- base64, and signed with the removal secret. So a push that holds the artist back while the page is open loses
-- nothing the artist then sends, and nobody can bind a request to anything a page did not show.
-- ponytail: the ticket is signed, not sealed, so it shows the account's hash, a hash of the user id the API
-- gives anybody. Seal it (pgcrypto's pgp_sym_encrypt) if that ever matters.
create or replace function public.claim_ticket(p_bound jsonb) returns text
language sql stable set search_path = ''
as $$
  select t.b || '.' || encode(extensions.hmac('claim ticket ' || t.b, s.secret, 'sha256'), 'hex')
  from (select translate(encode(convert_to(p_bound::text, 'UTF8'), 'base64'), E'\n', '') as b) t,
       public.removal_secret s
$$;

-- What a ticket binds a request to; null for anything claim_ticket did not make. In plpgsql, so that nothing a
-- visitor sends is decoded before its signature holds.
create or replace function public.ticket_binding(p_ticket text) returns jsonb
language plpgsql stable set search_path = ''
as $$
declare
  body constant text := split_part(p_ticket, '.', 1);
begin
  if exists (select 1 from public.removal_secret s where split_part(p_ticket, '.', 2)
             = encode(extensions.hmac('claim ticket ' || body, s.secret, 'sha256'), 'hex')) then
    return convert_from(decode(body, 'base64'), 'UTF8')::jsonb;
  end if;
  return null;
end
$$;

-- Whom a request for the slug is bound to: what its page showed (the ticket), listed now or not; without a
-- ticket, the artist listed under the slug now. Nobody whose handle or account is on the removal list already:
-- they are gone, and a request would only keep their handle.
-- That is asked under the removals' lock: a transaction advisory lock that whatever files a request
-- (request_removal, start_removal_proof) and whatever puts keys on the list (grant_request, removed_after_insert)
-- takes first and holds until it commits. So one of the two always sees what the other did: "Get my code" pressed
-- while a proof is confirmed either files nothing or is closed by the removal. Without it, the request read the
-- list as it was before the proof committed, and filed a new open request for the artist just removed, which
-- was then looked at for good. Both are plpgsql, whose every statement reads the database afresh: every statement
-- of an SQL function reads it as the first one did, before the lock was had.
-- ponytail: one lock for every request and removal, a handful a day. Lock by key if they ever queue on it.
create or replace function public.request_binding(p_slug text, p_ticket text)
returns table (slug text, da_username text, site_url text, site_domain text, da_account text)
language sql stable set search_path = ''
as $$
  select b.slug, b.da_username, b.site_url, b.site_domain, b.da_account
  from (
    select 1 as pick, t.bound ->> 'slug' as slug, t.bound ->> 'da_username' as da_username,
           t.bound ->> 'site_url' as site_url, t.bound ->> 'site_domain' as site_domain,
           t.bound ->> 'da_account' as da_account
    from public.ticket_binding(p_ticket) as t (bound)
    union all
    select 2, a.slug, a.da_username, a.site_url, a.site_domain, a.da_account from public.artists a
  ) b
  where b.slug = p_slug
    and not exists (select 1 from public.removed gone
                    where gone.key_hash in (public.key_hash('da:' || lower(b.da_username)), b.da_account))
  order by b.pick
  limit 1
$$;

-- Logs a request for the artist its page showed (request_binding), bound to their handle, site and account,
-- with its note. Asking again for the same only counts, and moves nothing in the verifier's queue; a note that
-- says something takes the place of the one kept, so a stranger's empty first request hides nothing the artist
-- writes after it. Always returns true, whatever the slug, so it gives nothing away.
create or replace function public.request_removal(p_slug text, p_note text default null, p_ticket text default null)
returns boolean
language plpgsql security definer set search_path = ''
as $$
begin
  perform pg_advisory_xact_lock(4207); -- the removals' lock: see request_binding
  insert into public.removal_requests as r (artist_slug, note, da_username, site_url, site_domain, da_account)
  select b.slug, left(nullif(btrim(p_note), ''), 300), b.da_username, b.site_url, b.site_domain, b.da_account
  from public.request_binding(p_slug, p_ticket) b
  on conflict (artist_slug, da_key, site_key) where outcome is null do update set
    last_at = now(), requests = r.requests + 1, da_account = coalesce(r.da_account, excluded.da_account),
    note = coalesce(excluded.note, r.note);
  return true;
end
$$;

-- The proof code for a slug: derived from the slug and a secret, so it is the same every time, it is
-- stored nowhere, and nobody without the secret can work it out. Service role only: the verifier asks
-- it for the code to look for. Null when the secret is missing.
create or replace function public.removal_code(p_slug text) returns text
language sql stable security definer set search_path = ''
as $$
  select 'artist-search-removal-'
      || left(encode(extensions.hmac(coalesce(p_slug, ''), secret, 'sha256'), 'hex'), 32)
  from public.removal_secret
$$;

-- Tells the artist their code, and from then on the verifier looks for it: the request for the artist its
-- page showed (request_binding) is filed if it was not, and its checks start over (hourly for a day, then
-- every six hours for two weeks, then weekly: pipeline/removals.py), due at once unless it already was, in the
-- turn of the network asking (p_network: due_removals). The code never changes. The answer has the same shape
-- whether the slug is unknown, hidden or visible; only an artist's slug is filed.
create or replace function public.start_removal_proof(p_slug text, p_ticket text default null, p_network text default null)
returns text
language plpgsql security definer set search_path = ''
as $$
begin
  perform pg_advisory_xact_lock(4207); -- the removals' lock: see request_binding
  insert into public.removal_requests as r
    (artist_slug, da_username, site_url, site_domain, da_account, code_at, due_at, network)
  select b.slug, b.da_username, b.site_url, b.site_domain, b.da_account, now(), now(),
         encode(extensions.hmac('removal network ' || coalesce(p_network, ''), s.secret, 'sha256'), 'hex')
  from public.request_binding(p_slug, p_ticket) b, public.removal_secret s
  on conflict (artist_slug, da_key, site_key) where outcome is null do update set
    code_at = now(), due_at = least(r.due_at, now()), da_account = coalesce(r.da_account, excluded.da_account),
    network = excluded.network;
  return public.removal_code(p_slug);
end
$$;

-- Service role only: the open requests a code was given out for that are due by p_now, p_limit at most, for
-- the verifier (pipeline/removals.py). In turns between the networks the codes were asked from: each one's
-- request due longest, then each one's next, and so on, the one due longest first within a turn. So a network
-- that asks for many codes holds back only its own, and a flood from many is as slow as their number, not as
-- the number of codes they asked for.
create or replace function public.due_removals(p_now timestamptz, p_limit int)
returns table (id bigint, artist_slug text, code_at timestamptz, da_username text, site_url text, site_domain text,
               da_account text)
language sql stable set search_path = ''
as $$
  select q.id, q.artist_slug, q.code_at, q.da_username, q.site_url, q.site_domain, q.da_account
  from (
    select r.*, row_number() over (partition by r.network order by r.due_at, r.code_at, r.id) as turn
    from public.removal_requests r
    where r.outcome is null and r.code_at is not null and r.due_at <= p_now
  ) q
  order by q.turn, q.due_at, q.code_at, q.id
  limit p_limit
$$;

-- Service role only. Removes artists outright: their keys go on the removal list, which deletes
-- them (works cascade) and bars their handle, site and account for good. It reads the artists without
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
  from public.artists a, unnest(array_remove(public.artist_keys(a.da_username, a.site_domain) || a.da_account, null)) k
  where a.slug = any (p_slugs)
  on conflict (key_hash) do nothing;
  return before - (select count(*) from public.artists);
end
$$;

-- Service role only. Closes an open request with the verifier's or the owner's answer, and keeps nothing
-- of the artist in it: its slug gives way to a random key and the handle, site, account and note go.
-- Returns whether there was one open.
create or replace function public.close_request(p_id bigint, p_outcome text) returns boolean
language sql set search_path = ''
as $$
  with closed as (
    update public.removal_requests
    set artist_slug = 'closed:' || gen_random_uuid(), outcome = p_outcome, note = null, da_username = null,
        site_url = null, site_domain = null, da_account = null, due_at = null, network = null
    where id = p_id and outcome is null
    returning 1
  )
  select exists (select 1 from closed)
$$;

-- Service role only. Grants an open request, the owner's answer ('granted') or confirm_removal's ('proven'):
-- what it was bound to goes on the removal list in one statement (which deletes whoever that bars), and the
-- request is closed. That is the handle and the account, and the site when p_site says the proof covered it
-- (the owner's grant: always) and no other handle is listed with it now: a site can pass to another artist.
-- Never whoever the slug lists now. Whether the artist is listed at this moment does not matter: the next
-- push bars and purges them everywhere. Returns the keys it put on the list, for the local store; null, and
-- nothing changes, without one open, or without a key to bar (a request older than its handle column, for
-- an artist not listed now).
create or replace function public.grant_request(p_id bigint, p_outcome text, p_site boolean) returns text[]
language plpgsql set search_path = ''
as $$
declare
  keys text[];
begin
  -- The removals' lock (see request_binding) before the request's row: a request being filed holds the lock and
  -- may wait on that row, so the other order could deadlock.
  perform pg_advisory_xact_lock(4207);
  select array_remove(array[r.da_key, r.da_account, case when p_site and not exists (
           select 1 from public.artists a where a.site_key = r.site_key and a.da_key is distinct from r.da_key)
         then r.site_key end], null)
  into keys
  from public.removal_requests r where r.id = p_id and r.outcome is null
  for update of r;
  if coalesce(cardinality(keys), 0) = 0 then
    return null;
  end if;
  perform public.close_request(p_id, p_outcome); -- first, so the removal below does not close it as 'removed'
  insert into public.removed (key_hash) select distinct unnest(keys) on conflict (key_hash) do nothing;
  return keys;
end
$$;

-- Service role only: the verifier calls it once it has seen the request's code where only its artist can
-- write, with p_site true when the artist's profile, as it read it, still names the request's site. When the
-- request is open and the code is its slug's, the artist is removed at once and for good (grant_request),
-- listed at this moment or not, and the keys barred are returned. Null, and nothing changes, for anything
-- else, and when the secret is missing (no code is then anyone's).
create or replace function public.confirm_removal(p_id bigint, p_code text, p_site boolean) returns text[]
language plpgsql security definer set search_path = ''
as $$
begin
  if not coalesce(p_code = (select public.removal_code(r.artist_slug) from public.removal_requests r
                            where r.id = p_id and r.outcome is null), false) then
    return null;
  end if;
  return public.grant_request(p_id, 'proven', p_site);
end
$$;

-- Service role only. The owner turns down an artist's unproven request: it is closed, and nothing else
-- changes. Returns whether there was one open.
create or replace function public.reject_removal(p_id bigint) returns boolean
language sql set search_path = ''
as $$ select public.close_request(p_id, 'rejected') $$;

-- Service role only. How many requests there are in each state, for the verifier's report: open with a
-- code and without one, proven, and closed otherwise (granted, rejected, or removed another way).
create or replace function public.removal_tally() returns jsonb
language sql stable set search_path = ''
as $$
  select jsonb_build_object(
    'requests_open_with_code', count(*) filter (where outcome is null and code_at is not null),
    'requests_open_without_code', count(*) filter (where outcome is null and code_at is null),
    'requests_proven', count(*) filter (where outcome = 'proven'),
    'requests_closed', count(*) filter (where outcome <> 'proven'))
  from public.removal_requests
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
  public.artist_card(bigint, int, tsquery, smallint[]),
  public.query_embedding(real[]),
  public.search_artists(text, real[], text[], text[], boolean, int, int),
  public.replace_neighbors(jsonb),
  public.refresh_search_words(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.claim_ticket(jsonb),
  public.ticket_binding(text),
  public.request_binding(text, text),
  public.request_removal(text, text, text),
  public.removal_code(text),
  public.start_removal_proof(text, text, text),
  public.due_removals(timestamptz, int),
  public.remove_artists(text[]),
  public.close_request(bigint, text),
  public.grant_request(bigint, text, boolean),
  public.confirm_removal(bigint, text, boolean),
  public.reject_removal(bigint),
  public.removal_tally()
from public, anon, authenticated, service_role;

-- None of these is the public's. The site's server functions reach the five a visitor needs through
-- sql/003_limits.sql, with a token of their own, once the visitor is counted.

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
  public.artist_card(bigint, int, tsquery, smallint[]),
  public.query_embedding(real[]),
  public.search_artists(text, real[], text[], text[], boolean, int, int),
  public.replace_neighbors(jsonb),
  public.refresh_search_words(),
  public.similar_artists(text, int),
  public.get_artist(text),
  public.request_removal(text, text, text),
  public.removal_code(text),
  public.start_removal_proof(text, text, text),
  public.due_removals(timestamptz, int),
  public.remove_artists(text[]),
  public.close_request(bigint, text),
  public.grant_request(bigint, text, boolean),
  public.confirm_removal(bigint, text, boolean),
  public.reject_removal(bigint),
  public.removal_tally()
to service_role;

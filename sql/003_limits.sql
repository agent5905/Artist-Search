-- Limits on what visitors can ask, and the verifier's queue of removal requests. Safe to rerun, and
-- it only adds: a table, two columns, two functions. db.migrate() runs the files in name order in one
-- transaction, so this one runs after 002_functions.sql.

-- The verifier's queue (pipeline/removals.py). code_at: when a proof code was first given out for the
-- request; null for a request without one, which is the owner's alone and never looked at. due_at:
-- when the verifier looks for the code next; it moves that on after each look.
alter table public.removal_requests
  add column if not exists code_at timestamptz,
  add column if not exists due_at timestamptz;

-- Requests counted per visitor, for client_wait. One row per kind and visitor, keyed by an HMAC of the two
-- made with the removal secret, so the table holds no address; one row per kind for the day's total.
-- Rows nobody has been counted in for two days are deleted.
create table if not exists public.client_counts (
  client_key text primary key,
  slot bigint not null, -- which window of the kind's length `hits` are counted in
  hits int not null,
  hits_before int not null, -- in the window before that one
  seen_at timestamptz not null default now()
);
alter table public.client_counts enable row level security;
revoke all on table public.client_counts from public, anon, authenticated, service_role;

-- Service role only: the site's server functions call it first, with the visitor as Cloudflare saw them
-- (an IPv4 address, or the /56 of an IPv6 one). It counts the request, refused or not, and answers how
-- many seconds the visitor should wait before the next one: 0 means go ahead.
--   search:  60 per visitor in any ten minutes, and 1,000 a day over everyone. A person searching,
--            ticking filters and loading more makes a few a minute and seldom 20 in their busiest ten.
--            The day's total keeps searching inside the database's free 5 GB of egress a month: a page of
--            24 cards is about 140 kB before compression (each thumbnail URL carries a signed token of
--            about 1,000 characters), so 1,000 a day is at most about 4 GB a month, and profile pages
--            have the rest. Measure a real page and change the number here if it differs. (A refused
--            search still runs the server function, so this does not guard Cloudflare's 100,000 function
--            requests a day: only a rate limiting rule in front of the site can.)
--   removal: 10 per visitor in any day, for asking for a code and asking without one. An artist needs one
--            or two. There is no total: an artist has one request at most, so everyone together can file
--            no more requests than there are artists.
-- A visitor's count is one row, written by one upsert that returns it, so requests sent all at once are
-- each counted. The window slides: the window before counts for the share of it still within the last
-- ten minutes (or day), so a burst on either side of a window's edge gets no more than the limit through.
-- ponytail: every search let through also writes the day's total, one row for everyone. Ceiling: about a
-- hundred searches a second, far past the free tier; then spread the total over a few rows.
create or replace function public.client_wait(p_kind text, p_client text) returns int
language plpgsql security definer set search_path = ''
as $$
declare
  per_client constant int := case p_kind when 'search' then 60 when 'removal' then 10 end;
  span constant int := case p_kind when 'search' then 600 when 'removal' then 86400 end; -- seconds
  per_day constant int := case p_kind when 'search' then 1000 end; -- over everyone; null for no total
  now_s constant float8 := extract(epoch from clock_timestamp());
  this_slot constant bigint := floor(now_s / span);
  into_slot constant float8 := now_s - this_slot * span; -- seconds since this window began
  counted public.client_counts;
  total int;
begin
  if per_client is null then
    raise exception 'unknown kind of request' using errcode = 'invalid_parameter_value';
  end if;
  insert into public.client_counts as c (client_key, slot, hits, hits_before)
  select encode(extensions.hmac(p_kind || ':' || coalesce(p_client, ''), s.secret, 'sha256'), 'hex'), this_slot, 1, 0
  from public.removal_secret s
  on conflict (client_key) do update set
    hits_before = case when c.slot = excluded.slot then c.hits_before
                       when c.slot = excluded.slot - 1 then c.hits else 0 end,
    hits = case when c.slot = excluded.slot then c.hits + 1 else 1 end,
    slot = excluded.slot,
    seen_at = now()
  returning * into counted;
  -- What the last `span` seconds hold: this window, and the share of the one before still inside them.
  if counted.hits + counted.hits_before * (1 - into_slot / span) > per_client then
    -- How long until one more would be within the limit, if none is sent meanwhile: later in this window, as
    -- the one before slides out, or else some way into the next.
    return greatest(1, ceil(case
      when counted.hits < per_client and counted.hits_before > 0
        then span * (1 - (per_client - counted.hits - 1)::float8 / counted.hits_before) - into_slot
      else span - into_slot + greatest(0, span * (1 - (per_client - 1)::float8 / counted.hits))
    end))::int;
  end if;
  if per_day is not null then
    insert into public.client_counts as c (client_key, slot, hits, hits_before)
    values (p_kind || ':all', floor(now_s / 86400), 1, 0)
    on conflict (client_key) do update set
      hits = case when c.slot = excluded.slot then c.hits + 1 else 1 end, slot = excluded.slot, seen_at = now()
    returning hits into total;
    if total = 1 then -- the first of a new day: forget the visitors nobody has counted since the day before
      delete from public.client_counts where seen_at < now() - interval '2 days';
    end if;
    if total > per_day then
      return ceil((floor(now_s / 86400) + 1) * 86400 - now_s)::int;
    end if;
  end if;
  return 0;
end
$$;

-- Tells the artist their code, and from then on the verifier looks for it: the slug's request is filed if
-- it was not, and marked as one a code was given out for, due at once. Asking again changes nothing: not
-- the code, which never changes, and not the request's place in the queue. The answer has the same shape
-- whether the slug is unknown, hidden or visible; only an artist's slug is filed. Service role only: the
-- site's server function calls it once client_wait has let the visitor through, and the verifier calls it
-- for the code to look for.
create or replace function public.start_removal_proof(p_slug text) returns text
language sql security definer set search_path = ''
as $$
  with filed as (
    insert into public.removal_requests as r (artist_slug, code_at, due_at)
    select a.slug, now(), now() from public.artists a where a.slug = p_slug
    on conflict (artist_slug) do update set code_at = now(), due_at = now() where r.code_at is null
  )
  select public.removal_code(p_slug)
$$;

revoke all on function
  public.client_wait(text, text),
  public.start_removal_proof(text)
from public, anon, authenticated, service_role;

grant execute on function
  public.client_wait(text, text),
  public.start_removal_proof(text)
to service_role;

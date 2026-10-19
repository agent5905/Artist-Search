-- What the site's server may ask the database for, and how much. Safe to rerun, and it only adds: two
-- tables and the functions below (a counts table of an earlier shape is brought to this one). db.migrate()
-- runs the files in name order in one transaction, so this one runs after 002_functions.sql, and then sets
-- the site's token from .env (SITE_TOKEN), by its hash.
--
-- The site's server functions (web/functions/api/) call the database as the anon role, with the publishable
-- key, and send the site's token, a secret of the Cloudflare project. The anon role may run the seven
-- functions below and nothing else, and each of them refuses a call without the token. So the publishable
-- key alone opens nothing, and nothing the site can reach deletes or hides an artist.

-- The hash of the site's token (sha256, as key_hash makes it). Readable by the table owner only. Without
-- a row, the site is refused everything.
create table if not exists public.site_secret (
  only_row boolean primary key default true check (only_row),
  token_hash text not null check (token_hash ~ '^[0-9a-f]{64}$')
);
alter table public.site_secret enable row level security;
revoke all on table public.site_secret from public, anon, authenticated, service_role;

-- Raises unless p_token is the site's.
create or replace function public.site_check(p_token text) returns void
language plpgsql stable set search_path = ''
as $$
begin
  if not exists (select 1 from public.site_secret where token_hash = public.key_hash(p_token)) then
    raise exception 'not the site' using errcode = 'insufficient_privilege';
  end if;
end
$$;

-- How much each visitor, each network and everyone have been let in, per budget: one number a row,
-- busy_until (epoch seconds), as the generic cell rate algorithm keeps it. A request let in moves it on
-- by its share of the day, from now or from where it was, whichever is later; a request is let in when
-- that leaves it no more than a day ahead of now. A visitor's and a network's rows are keyed by an HMAC
-- made with the removal secret, so the table holds no address.
create table if not exists public.client_counts (
  client_key text primary key,
  busy_until float8 not null
);
-- A table of an earlier shape (windows of hits, never applied anywhere but possible): it takes this one, and its
-- counts start again from nothing, which is all they are.
alter table public.client_counts
  drop column if exists slot,
  drop column if exists hits,
  drop column if exists hits_before,
  drop column if exists seen_at,
  add column if not exists busy_until float8 not null default 0;
create index if not exists client_counts_busy_until_idx on public.client_counts (busy_until);
alter table public.client_counts enable row level security;
revoke all on table public.client_counts from public, anon, authenticated, service_role;

-- The site's server calls it first, with the visitor as Cloudflare saw them (an IPv4 address, or the /56
-- of an IPv6 one) and their network (its /24, or the /48). It answers how many seconds to wait: 0 means
-- go ahead, and the request is counted. A request told to wait is not counted, so asking again while
-- told to wait costs nothing.
--   read: a page of search results or of similar artists counts 1, a profile 0.1 (its size). 100 a day
--         per visitor, 100 per network, 3,000 for everyone.
--   removal: asking for a code, or asking without one, counts 1; the claim-or-remove page itself (site_claim)
--         0.1. 10 a day per visitor, 20 per network. No total, and apart from the reads: many networks
--         together must not be able to keep an artist from asking, and a day's reads used up must not either.
-- "n a day" means n at once, then one every 1/n of a day; so in any 24 hours one visitor gets at most
-- 2n, as does one network, and day after day n.
-- The total is what the free tier's 5 GB of egress a month allows, and no less: the site is the database's
-- one big reader, and Workers AI's free 10,000 neurons a day are some 150,000 queries of bge-m3 (1,075 neurons
-- a million tokens, a query at most 200 characters), so they are not what runs out. A page of 24 cards is 117
-- kB as the database writes it and 45 kB gzipped, which the site's server asks for (measured on rows of the
-- stored shape, every thumbnail's signed token at 640 characters, the longest seen). At most 96,000 pages a
-- month (the first day's burst, then 31 days of 3,000) is 4.3 GB gzipped, 86% of the allowance; the rest is
-- for the push stage's reads (about 10 MB a run). Change the numbers here if a real page measures larger.
-- A visitor is one address, an office's or a carrier's shared one too, so a visitor may use what a network
-- may. One network is 3.3% of the day: it takes 30 networks a day to use it up, and one person on 20 of them
-- leaves a third. Past that, many networks at once are the edge rule's to stop (docs/edge-rate-limit.md).
-- The rows are locked in one order (visitor, network, everyone), so two calls never wait on each other
-- in a circle. The clock is read once they are all held, and a row only ever moves forward, so the order
-- in which calls arrive cannot undo a count. Anything missing (the token, the removal secret, a known
-- kind) is an error, and nothing is let in. (A refused request still runs the server function, so this
-- does not guard Cloudflare's 100,000 function requests a day: only a rate limiting rule in front of
-- the site can, docs/edge-rate-limit.md.)
create or replace function public.client_wait(p_token text, p_kind text, p_client text, p_network text)
returns int
language plpgsql security definer set search_path = ''
as $$
declare
  budget constant text := case when p_kind in ('search', 'similar', 'profile') then 'read'
                               when p_kind in ('removal', 'claim') then 'removal' end;
  cost constant float8 := case when p_kind in ('profile', 'claim') then 0.1 else 1 end;
  per_day constant float8[] := case budget when 'read' then '{100, 100, 3000}'::float8[] else '{10, 20, null}'::float8[] end;
  secret text;
  keys text[];
  held float8[];
  busy float8;
  now_s float8;
  wait float8 := 0;
begin
  perform public.site_check(p_token);
  select s.secret into secret from public.removal_secret s;
  if budget is null or secret is null then
    raise exception 'cannot count this request' using errcode = 'invalid_parameter_value';
  end if;
  keys := array[
    encode(extensions.hmac(budget || ' visitor ' || coalesce(p_client, ''), secret, 'sha256'), 'hex'),
    encode(extensions.hmac(budget || ' network ' || coalesce(p_network, ''), secret, 'sha256'), 'hex'),
    budget || ' everyone'];
  for i in 1 .. 3 loop
    continue when per_day[i] is null;
    -- The row's lock, the row made if need be, and what it holds.
    insert into public.client_counts as c (client_key, busy_until) values (keys[i], 0)
    on conflict (client_key) do update set busy_until = c.busy_until
    returning c.busy_until into busy;
    held[i] := busy;
  end loop;
  now_s := extract(epoch from clock_timestamp());
  for i in 1 .. 3 loop
    continue when per_day[i] is null;
    wait := greatest(wait, greatest(held[i], now_s) + cost * 86400 / per_day[i] - 86400 - now_s);
  end loop;
  if wait > 0 then
    return ceil(wait)::int;
  end if;
  for i in 1 .. 3 loop
    continue when per_day[i] is null;
    update public.client_counts set busy_until = greatest(held[i], now_s) + cost * 86400 / per_day[i]
    where client_key = keys[i];
  end loop;
  -- A row back to nothing says nothing: it goes, and a row another call holds is skipped, not waited for.
  delete from public.client_counts where client_key in (
    select client_key from public.client_counts where busy_until < now_s limit 100 for update skip locked);
  return 0;
end
$$;

-- What the site's server asks for once client_wait has let the visitor in. Each checks the token first.
-- A page of search results is always 24 cards.
create or replace function public.site_search(
  p_token text, q text, q_embedding real[] default null, p_mediums text[] default null,
  p_specialties text[] default null, p_open_only boolean default false, p_offset int default 0
) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select public.search_artists(q, q_embedding, p_mediums, p_specialties, p_open_only, 24, p_offset);
$$;

create or replace function public.site_artist(p_token text, p_slug text) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select public.get_artist(p_slug);
$$;

create or replace function public.site_similar(p_token text, p_slug text) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select public.similar_artists(p_slug, 24);
$$;

-- What the claim-or-remove page shows of a listed artist, and no more (it is counted as a removal, not a read):
-- the slug, the name, the two places a code can go, and whether the DeviantArt one can prove anything. It can
-- only for an artist whose account the library keeps (artists.da_account): without it, whoever holds the handle
-- now could be somebody else (pipeline/removals.py). And the ticket the page sends back with a request, which
-- binds it to this artist even if a push holds them back meanwhile (claim_ticket). p_slug may be the artist's
-- DeviantArt username instead, so an artist can reach the page with no search, which is a read. Nothing when
-- the artist is not listed.
create or replace function public.site_claim(p_token text, p_slug text) returns setof jsonb
language sql stable security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select jsonb_build_object(
    'slug', a.slug, 'name', a.name, 'site_url', a.site_url,
    'deviantart_url', 'https://www.deviantart.com/' || a.da_username, 'deviantart_proof', a.da_account is not null,
    'ticket', public.claim_ticket(jsonb_build_object('slug', a.slug, 'da_username', a.da_username,
      'site_url', a.site_url, 'site_domain', a.site_domain, 'da_account', a.da_account)))
  from public.artists a
  where (a.slug = p_slug or a.da_key = public.key_hash('da:' || lower(p_slug))) and not a.hidden
    and not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key, a.da_account))
  order by a.slug = p_slug desc
  limit 1
$$;

-- The two a request is filed by: bound to what the page showed (p_ticket), and a code in the turn of the network
-- that asks for it (p_network, as client_wait is given it).
drop function if exists public.site_removal_code(text, text);
drop function if exists public.site_request_removal(text, text, text);

create or replace function public.site_removal_code(
  p_token text, p_slug text, p_ticket text default null, p_network text default null
) returns text
language sql security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select public.start_removal_proof(p_slug, p_ticket, p_network);
$$;

create or replace function public.site_request_removal(p_token text, p_slug text, p_note text, p_ticket text default null)
returns boolean
language sql security definer set search_path = ''
as $$
  select public.site_check(p_token);
  select public.request_removal(p_slug, p_note, p_ticket);
$$;

revoke all on function
  public.site_check(text),
  public.client_wait(text, text, text, text),
  public.site_search(text, text, real[], text[], text[], boolean, int),
  public.site_artist(text, text),
  public.site_similar(text, text),
  public.site_claim(text, text),
  public.site_removal_code(text, text, text, text),
  public.site_request_removal(text, text, text, text)
from public, anon, authenticated, service_role;

grant execute on function
  public.client_wait(text, text, text, text),
  public.site_search(text, text, real[], text[], text[], boolean, int),
  public.site_artist(text, text),
  public.site_similar(text, text),
  public.site_claim(text, text),
  public.site_removal_code(text, text, text, text),
  public.site_request_removal(text, text, text, text)
to anon;

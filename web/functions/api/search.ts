// GET /api/search?q=&specialty=&medium=&open=1&offset=
// Turns the query into its bge-m3 embedding on Workers AI, then asks the database for artists
// (search_artists in sql/002_functions.sql). Without an embedding the search still runs, on keywords alone.
// Each client has a limit, checked before either is asked: past it the answer is a 429 that says how long to wait.
import { rpc } from '../../src/rpc'

export type Env = {
  AI: { run(model: string, input: { text: string[] }): Promise<unknown> }
  VITE_SUPABASE_URL: string
  VITE_SUPABASE_PUBLISHABLE_KEY: string
}

export const MAX_QUERY = 200 // as many characters as search_artists reads
export const PAGE = 24
const EMBED_MS = 5_000
// Searches a client may make in a window. A person searching, ticking filters and loading more makes a few a
// minute and seldom more than 20 in their busiest one: 60 in ten minutes leaves room for that, and for a few people
// behind one address. A loop is held to one search every ten seconds, 8,640 a day from one address: a small share of
// the free tier's 100,000 function requests a day, and nothing the database or the Workers AI allowance notices.
export const LIMIT = 60
export const WINDOW = 600 // seconds
export const TOO_MANY = 'Too many searches from your connection in the last few minutes.'

/** Who is asking, for the limit: their address, and an IPv6 one by its /64, which one subscriber usually has whole. */
export function client(request: Request): string {
  const address = request.headers.get('cf-connecting-ip') ?? ''
  if (!address.includes(':')) return address
  const [head, tail] = address.split('::').map((part) => (part ? part.split(':') : []))
  const groups = tail ? [...head, ...Array(Math.max(8 - head.length - tail.length, 0)).fill('0'), ...tail] : head
  return `${groups.slice(0, 4).map((group) => parseInt(group, 16).toString(16)).join(':')}::/64`
}

/**
 * Counts one search against the client's limit, in the cache of the Cloudflare location that serves them: no
 * binding or setting is needed, on pages.dev or on a domain of the owner's. The seconds until the window ends when
 * they are over it, 0 when they are not.
 * ponytail: a count read and then written, at each location on its own, so searches sent all at once can count as
 * one. Ceiling: a client that sends many in parallel. Then a rate limiting binding, which a Pages project cannot
 * hold (wrangler refuses `ratelimits` there), in a small Worker this function reaches by a service binding.
 */
export async function wait(cache: Cache, origin: string, who: string, now = Date.now()): Promise<number> {
  const window = Math.floor(now / 1000 / WINDOW)
  const key = `${origin}/__search-limit/${encodeURIComponent(who)}/${window}`
  const count = Number((await (await cache.match(key))?.text()) ?? 0) + 1
  await cache.put(key, new Response(String(count), { headers: { 'cache-control': `max-age=${WINDOW}` } }))
  return count > LIMIT ? (window + 1) * WINDOW - Math.floor(now / 1000) : 0
}

/** The query's 1,024 numbers, or null when Workers AI fails, stalls or answers with anything else. */
export async function embed(ai: Env['AI'], q: string): Promise<number[] | null> {
  if (!q) return null
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const late = new Promise<never>((_, reject) => (timer = setTimeout(reject, EMBED_MS)))
    const answer = (await Promise.race([ai.run('@cf/baai/bge-m3', { text: [q] }), late])) as { data?: unknown[] }
    const vector = answer.data?.[0]
    return Array.isArray(vector) && vector.length === 1024 && vector.every(Number.isFinite) ? vector : null
  } catch {
    return null
  } finally {
    clearTimeout(timer)
  }
}

/** `cache` is where the limit is counted; without one there is no limit. A cache that fails lets the search through. */
export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch, cache?: Cache): Promise<Response> {
  const headers = { 'cache-control': 'no-store' } // a removed artist must not linger in a cached answer
  const seconds = cache ? await wait(cache, new URL(request.url).origin, client(request)).catch(() => 0) : 0
  if (seconds) return Response.json({ error: TOO_MANY }, { status: 429, headers: { ...headers, 'retry-after': String(seconds) } })
  const asked = new URL(request.url).searchParams
  const list = (name: string) => {
    const values = asked.getAll(name).filter(Boolean).slice(0, 20) // search_artists reads no more than 20
    return values.length ? values : null
  }
  const q = (asked.get('q') ?? '').trim().slice(0, MAX_QUERY)
  const q_embedding = await embed(env.AI, q)
  const args = {
    q,
    q_embedding,
    p_mediums: list('medium'),
    p_specialties: list('specialty'),
    p_open_only: asked.get('open') === '1',
    p_limit: PAGE,
    p_offset: Math.min(Math.max(Math.trunc(Number(asked.get('offset'))) || 0, 0), 1000),
  }
  try {
    const artists = await rpc(env.VITE_SUPABASE_URL, env.VITE_SUPABASE_PUBLISHABLE_KEY, 'search_artists', args, fetcher)
    return Response.json({ artists, ranked_by: q_embedding ? 'meaning' : q ? 'keywords' : 'name' }, { headers })
  } catch {
    return Response.json({ error: 'The search is not answering right now.' }, { status: 502, headers })
  }
}

export const onRequestGet = async (context: { request: Request; env: Env }) =>
  handle(context.request, context.env, fetch, await caches.open('search-limit').catch(() => undefined))

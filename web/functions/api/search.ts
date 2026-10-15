// GET /api/search?q=&specialty=&medium=&open=1&offset=
// Turns the query into its bge-m3 embedding on Workers AI, then asks the database for artists
// (search_artists in sql/002_functions.sql). Without an embedding the search still runs, on keywords alone.
// Before either is asked, the database counts the visitor (client_wait in sql/003_limits.sql): past their limit,
// or past the day's for everyone, the answer is a 429 that says how long to wait.
// The database is called with SUPABASE_SERVER_KEY: a Supabase secret key made for this site, which the owner sets
// as a secret of the Cloudflare Pages project. It is never sent to the browser, and the publishable key the
// browser holds cannot search at all.
import { rpc } from '../../src/rpc'

export type Env = {
  AI: { run(model: string, input: { text: string[] }): Promise<unknown> }
  VITE_SUPABASE_URL: string
  SUPABASE_SERVER_KEY: string
}

export const MAX_QUERY = 200 // as many characters as search_artists reads
export const PAGE = 24
const EMBED_MS = 5_000
export const TOO_MANY = 'Too many searches for now.'
const NO_STORE = { 'cache-control': 'no-store' } // a removed artist must not linger in a cached answer

/** Who is asking, for the limits: their address, and an IPv6 one by its /56, which one subscriber usually has whole. */
export function client(request: Request): string {
  const address = request.headers.get('cf-connecting-ip') ?? ''
  if (!address.includes(':')) return address
  const [head, tail] = address.split('::').map((part) => (part ? part.split(':') : []))
  const written = tail ? [...head, ...Array(Math.max(8 - head.length - tail.length, 0)).fill('0'), ...tail] : head
  const [a, b, c, d] = written.map((group) => parseInt(group, 16) || 0)
  return `${[a, b, c, d & 0xff00].map((group) => (group ?? 0).toString(16)).join(':')}::/56`
}

/**
 * Counts this request against the visitor's limit of `kind` (client_wait): the seconds they should wait, 0 to go
 * ahead. The database keeps the count, one upsert that returns it, so requests sent all at once are each counted.
 */
export async function wait(request: Request, env: Env, kind: 'search' | 'removal', fetcher: typeof fetch): Promise<number> {
  const args = { p_kind: kind, p_client: client(request) }
  const seconds = Number(await rpc(env.VITE_SUPABASE_URL, env.SUPABASE_SERVER_KEY, 'client_wait', args, fetcher))
  return seconds <= 0 ? 0 : Math.ceil(seconds) || 60 // an answer that is not a number counts as "wait"
}

/** The answer to a visitor told to wait: a 429 that says how many seconds. */
export const refused = (error: string, seconds: number) =>
  Response.json({ error }, { status: 429, headers: { ...NO_STORE, 'retry-after': String(seconds) } })

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

export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const asked = new URL(request.url).searchParams
  const list = (name: string) => {
    const values = asked.getAll(name).filter(Boolean).slice(0, 20) // search_artists reads no more than 20
    return values.length ? values : null
  }
  const q = (asked.get('q') ?? '').trim().slice(0, MAX_QUERY)
  try {
    const seconds = await wait(request, env, 'search', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
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
    const artists = await rpc(env.VITE_SUPABASE_URL, env.SUPABASE_SERVER_KEY, 'search_artists', args, fetcher)
    return Response.json({ artists, ranked_by: q_embedding ? 'meaning' : q ? 'keywords' : 'name' }, { headers: NO_STORE })
  } catch {
    return Response.json({ error: 'The search is not answering right now.' }, { status: 502, headers: NO_STORE })
  }
}

export const onRequestGet = (context: { request: Request; env: Env }) => handle(context.request, context.env)

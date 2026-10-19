// GET /api/search?q=&specialty=&medium=&open=1&offset=
// Turns the query into its bge-m3 embedding on Workers AI, then asks the database for a page of artists
// (site_search, which runs search_artists: sql/003_limits.sql and 002_functions.sql). Without an embedding the search
// still runs, on keywords alone. Before either is asked, the database counts the visitor (client_wait): past their
// limit, their network's or the day's for everyone, the answer is a 429 that says how long to wait. Before even that,
// a call without the header the site's own pages send is refused (crossSite), as every function here refuses it.
// Every function here calls the database the same way: as its public role, with the publishable key, and with the
// site's token (SITE_TOKEN), a secret of the Cloudflare Pages project that never reaches the browser. The public
// role may run the site's seven functions and nothing else, and those only with the token. The browser holds no key.
import { call, OWN_PAGE, rpc } from '../../src/rpc'

export type Env = {
  AI: { run(model: string, input: { text: string[] }): Promise<unknown> }
  VITE_SUPABASE_URL: string
  VITE_SUPABASE_PUBLISHABLE_KEY: string
  SITE_TOKEN: string
}

export const MAX_QUERY = 200 // as many characters as search_artists reads
const EMBED_MS = 5_000
export const TOO_MANY = 'Too many searches for now.'
export const NO_STORE = { 'cache-control': 'no-store' } // a removed artist must not linger in a cached answer

/** The address Cloudflare saw, and nothing a client can set. An IPv4 address written as IPv6 is read as IPv4. */
const address = (request: Request) => (request.headers.get('cf-connecting-ip') ?? '').replace(/^::ffff:(?=[\d.]+$)/i, '')

/** The first four groups of an IPv6 address, however it is written. */
function groups(ipv6: string): number[] {
  const [head, tail] = ipv6.split('::').map((part) => (part ? part.split(':') : []))
  const written = tail ? [...head, ...Array(Math.max(8 - head.length - tail.length, 0)).fill('0'), ...tail] : head
  return [0, 1, 2, 3].map((n) => parseInt(written[n], 16) || 0)
}

/** Who is asking, for the limits: their address, and an IPv6 one by its /56, which one subscriber usually has whole. */
export function client(request: Request): string {
  const ip = address(request)
  if (!ip.includes(':')) return ip
  const [a, b, c, d] = groups(ip)
  return `${[a, b, c, d & 0xff00].map((group) => group.toString(16)).join(':')}::/56`
}

/** The network they ask from, which holds whatever other addresses one person can easily have: an IPv4 /24, an IPv6 /48. */
export function network(request: Request): string {
  const ip = address(request)
  if (!ip.includes(':')) return ip.replace(/\.\d+$/, '.0/24')
  return `${groups(ip).slice(0, 3).map((group) => group.toString(16)).join(':')}::/48`
}

/**
 * Whether another site's page may have made the visitor's browser send this: it lacks the header the site's own pages
 * send (OWN_PAGE). An <img>, a form or a fetch on any page would otherwise spend its visitors' limits here, each from
 * their own network, and the day's reads for everyone. Every function here refuses such a call before it asks the
 * database anything, so it is not counted either.
 */
export const crossSite = (request: Request) => !request.headers.has(OWN_PAGE)
export const notHere = () => Response.json({ error: 'Please use the site’s own pages.' }, { status: 403, headers: NO_STORE })

/** One of the site's database functions (sql/003_limits.sql), with the token. Asked once more if it fails. */
export const site = <T>(env: Env, name: string, args: object, fetcher: typeof fetch) =>
  rpc<T>(env.VITE_SUPABASE_URL, env.VITE_SUPABASE_PUBLISHABLE_KEY, name, { p_token: env.SITE_TOKEN, ...args }, fetcher)

/**
 * Counts this request against the limits of `kind` (client_wait): the seconds the visitor should wait, 0 to go ahead,
 * and then it has been counted. Asked once only, so a lost answer never counts one request twice. Only a number lets a
 * request through: any other answer counts as "wait".
 */
export async function wait(
  request: Request,
  env: Env,
  kind: 'search' | 'similar' | 'profile' | 'removal' | 'claim',
  fetcher: typeof fetch,
): Promise<number> {
  const args = { p_token: env.SITE_TOKEN, p_kind: kind, p_client: client(request), p_network: network(request) }
  const seconds = await call(env.VITE_SUPABASE_URL, env.VITE_SUPABASE_PUBLISHABLE_KEY, 'client_wait', args, fetcher)
  return typeof seconds === 'number' && seconds <= 0 ? 0 : Math.ceil(Number(seconds)) || 60
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
  if (crossSite(request)) return notHere()
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
      p_offset: Math.min(Math.max(Math.trunc(Number(asked.get('offset'))) || 0, 0), 1000),
    }
    const artists = await site(env, 'site_search', args, fetcher)
    return Response.json({ artists, ranked_by: q_embedding ? 'meaning' : q ? 'keywords' : 'name' }, { headers: NO_STORE })
  } catch {
    return Response.json({ error: 'The search is not answering right now.' }, { status: 502, headers: NO_STORE })
  }
}

export const onRequestGet = (context: { request: Request; env: Env }) => handle(context.request, context.env)

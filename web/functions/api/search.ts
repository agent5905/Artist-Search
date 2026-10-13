// GET /api/search?q=&specialty=&medium=&open=1&offset=
// Turns the query into its bge-m3 embedding on Workers AI, then asks the database for artists
// (search_artists in sql/002_functions.sql). Without an embedding the search still runs, on keywords alone.
import { rpc } from '../../src/rpc'

export type Env = {
  AI: { run(model: string, input: { text: string[] }): Promise<unknown> }
  VITE_SUPABASE_URL: string
  VITE_SUPABASE_PUBLISHABLE_KEY: string
}

export const MAX_QUERY = 200 // as many characters as search_artists reads
export const PAGE = 24
const EMBED_MS = 5_000

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
  const headers = { 'cache-control': 'no-store' } // a removed artist must not linger in a cached answer
  try {
    const artists = await rpc(env.VITE_SUPABASE_URL, env.VITE_SUPABASE_PUBLISHABLE_KEY, 'search_artists', args, fetcher)
    return Response.json({ artists, ranked_by: q_embedding ? 'meaning' : q ? 'keywords' : 'name' }, { headers })
  } catch {
    return Response.json({ error: 'The search is not answering right now.' }, { status: 502, headers })
  }
}

export const onRequestGet = (context: { request: Request; env: Env }) => handle(context.request, context.env)

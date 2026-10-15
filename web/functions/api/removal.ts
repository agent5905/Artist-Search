// POST /api/removal, a JSON body:
//   {"slug", "proof": true}  -> {"code"}      start_removal_proof: the artist's code, and from now on the verifier
//                                              looks for it (pipeline/removals.py)
//   {"slug", "note"}         -> {"ok": true}  request_removal: a request without proof, for the site owner
// The database counts the visitor first (client_wait 'removal': 10 a day); past that, a 429 that says how long to
// wait. Called with the site's own key, as search.ts is. Only a JSON body is read, so a form on another site cannot
// post here from a visitor's browser without the browser asking this site first, which it never agrees to.
import { rpc } from '../../src/rpc'
import { refused, wait, type Env } from './search'

export const TOO_MANY = 'Too many removal requests from your connection today.'

export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const headers = { 'cache-control': 'no-store' }
  const json = (request.headers.get('content-type') ?? '').startsWith('application/json')
  const body = ((json ? await request.json().catch(() => null) : null) ?? {}) as { slug?: unknown; note?: unknown; proof?: unknown }
  const slug = typeof body.slug === 'string' ? body.slug.slice(0, 80) : ''
  if (!slug) return Response.json({ error: 'Which profile?' }, { status: 400, headers })
  const call = <T>(name: string, args: object) => rpc<T>(env.VITE_SUPABASE_URL, env.SUPABASE_SERVER_KEY, name, args, fetcher)
  try {
    const seconds = await wait(request, env, 'removal', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
    if (body.proof === true) return Response.json({ code: await call<string>('start_removal_proof', { p_slug: slug }) }, { headers })
    const note = typeof body.note === 'string' ? body.note.trim().slice(0, 300) || null : null
    await call<boolean>('request_removal', { p_slug: slug, p_note: note })
    return Response.json({ ok: true }, { headers })
  } catch {
    return Response.json({ error: 'That did not go through.' }, { status: 502, headers })
  }
}

export const onRequestPost = (context: { request: Request; env: Env }) => handle(context.request, context.env)

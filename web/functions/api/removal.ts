// POST /api/removal, a JSON body:
//   {"slug", "proof": true}  -> {"code"}      site_removal_code: the artist's code, and from now on the verifier
//                                              looks for it (pipeline/removals.py)
//   {"slug", "note"}         -> {"ok": true}  site_request_removal: a request without proof, for the site owner
// The database counts the visitor first (client_wait 'removal': 10 a day, 20 a network); past that, a 429 that says
// how long to wait. Called as search.ts calls it. Only a JSON body is read, so a form on another site cannot post here
// from a visitor's browser without the browser asking this site first, which it never agrees to.
import { refused, site, wait, type Env } from './search'

export const TOO_MANY = 'Too many removal requests from your connection today.'

export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const headers = { 'cache-control': 'no-store' }
  const json = (request.headers.get('content-type') ?? '').startsWith('application/json')
  const body = ((json ? await request.json().catch(() => null) : null) ?? {}) as { slug?: unknown; note?: unknown; proof?: unknown }
  const slug = typeof body.slug === 'string' ? body.slug.slice(0, 80) : ''
  if (!slug) return Response.json({ error: 'Which profile?' }, { status: 400, headers })
  try {
    const seconds = await wait(request, env, 'removal', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
    if (body.proof === true) return Response.json({ code: await site<string>(env, 'site_removal_code', { p_slug: slug }, fetcher) }, { headers })
    const note = typeof body.note === 'string' ? body.note.trim().slice(0, 300) || null : null
    await site<boolean>(env, 'site_request_removal', { p_slug: slug, p_note: note }, fetcher)
    return Response.json({ ok: true }, { headers })
  } catch {
    return Response.json({ error: 'That did not go through.' }, { status: 502, headers })
  }
}

export const onRequestPost = (context: { request: Request; env: Env }) => handle(context.request, context.env)

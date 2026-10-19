// GET  /api/removal?slug=, what the claim-or-remove page shows of the artist (site_claim), or null when they are not listed
// POST /api/removal, a JSON body:
//   {"slug", "ticket", "proof": true}  -> {"code"}      site_removal_code: the artist's code, and from now on the verifier
//                                                        looks for it (pipeline/removals.py)
//   {"slug", "ticket", "note"}         -> {"ok": true}  site_request_removal: a request without proof, for the site owner
// The ticket is what site_claim gave the page: it binds the request to the artist the page showed, even if a push holds
// them back while the page is open (sql/002_functions.sql: request_binding). A code is asked for in the turn of the
// visitor's network (due_removals). The database counts the visitor first (client_wait: 10 removals a day, 20 a network,
// the page a tenth of one), apart from the reads and with no total for everyone, so a day's reads used up cannot keep
// an artist from this page; past that, a 429 that says how long to wait. Called as search.ts calls it. Only a JSON body is
// read, so a form on another site cannot post here from a visitor's browser without the browser asking this site first,
// which it never agrees to.
import { network, NO_STORE, refused, site, wait, type Env } from './search'

export const TOO_MANY = 'Too many removal requests from your connection today.'
const MAX_TICKET = 4096 // a ticket is the artist's slug, handle, site and account, signed: far less than this

export async function claim(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const slug = (new URL(request.url).searchParams.get('slug') ?? '').slice(0, 80) // no slug is longer (sql/001_schema.sql)
  if (!slug) return Response.json({ error: 'Which profile?' }, { status: 400, headers: NO_STORE })
  try {
    const seconds = await wait(request, env, 'claim', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
    const rows = await site<unknown[]>(env, 'site_claim', { p_slug: slug }, fetcher)
    return Response.json(rows[0] ?? null, { headers: NO_STORE })
  } catch {
    return Response.json({ error: 'That did not load.' }, { status: 502, headers: NO_STORE })
  }
}

export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const headers = NO_STORE
  const json = (request.headers.get('content-type') ?? '').startsWith('application/json')
  const body = ((json ? await request.json().catch(() => null) : null) ?? {}) as { slug?: unknown; note?: unknown; proof?: unknown; ticket?: unknown }
  const slug = typeof body.slug === 'string' ? body.slug.slice(0, 80) : ''
  if (!slug) return Response.json({ error: 'Which profile?' }, { status: 400, headers })
  const ticket = typeof body.ticket === 'string' && body.ticket.length <= MAX_TICKET ? body.ticket : null
  try {
    const seconds = await wait(request, env, 'removal', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
    if (body.proof === true) {
      const args = { p_slug: slug, p_ticket: ticket, p_network: network(request) }
      return Response.json({ code: await site<string>(env, 'site_removal_code', args, fetcher) }, { headers })
    }
    const note = typeof body.note === 'string' ? body.note.trim().slice(0, 300) || null : null
    await site<boolean>(env, 'site_request_removal', { p_slug: slug, p_note: note, p_ticket: ticket }, fetcher)
    return Response.json({ ok: true }, { headers })
  } catch {
    return Response.json({ error: 'That did not go through.' }, { status: 502, headers })
  }
}

export const onRequestGet = (context: { request: Request; env: Env }) => claim(context.request, context.env)
export const onRequestPost = (context: { request: Request; env: Env }) => handle(context.request, context.env)

// GET /api/artist?slug=            -> the artist's profile (site_artist), or null when they are not listed
// GET /api/artist?slug=&similar=1  -> the artists nearest them in style (site_similar)
// Counted first, as a search is (client_wait: a profile is a tenth of a page, the similar artists a page).
import { NO_STORE, refused, site, wait, type Env } from './search'

export const TOO_MANY = 'Too many pages for now.'

export async function handle(request: Request, env: Env, fetcher: typeof fetch = fetch): Promise<Response> {
  const asked = new URL(request.url).searchParams
  const slug = (asked.get('slug') ?? '').slice(0, 80) // no slug is longer (sql/001_schema.sql)
  const similar = asked.get('similar') === '1'
  if (!slug) return Response.json({ error: 'Which artist?' }, { status: 400, headers: NO_STORE })
  try {
    const seconds = await wait(request, env, similar ? 'similar' : 'profile', fetcher)
    if (seconds) return refused(TOO_MANY, seconds)
    const rows = await site<unknown[]>(env, similar ? 'site_similar' : 'site_artist', { p_slug: slug }, fetcher)
    return Response.json(similar ? rows : (rows[0] ?? null), { headers: NO_STORE })
  } catch {
    return Response.json({ error: 'That did not load.' }, { status: 502, headers: NO_STORE })
  }
}

export const onRequestGet = (context: { request: Request; env: Env }) => handle(context.request, context.env)

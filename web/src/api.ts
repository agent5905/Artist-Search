// Everything the pages ask for, all of it from the site's own server functions (functions/api/), which count each
// visitor and call the database with a token of their own. The browser holds no database key.
import { twice } from './rpc'

/**
 * A work as the database returns it. The image is never copied: image_url is the source's own thumbnail.
 * width and height are the thumbnail's pixels: not returned yet, used when they are (see Thumb).
 */
export type Work = {
  image_url: string
  page_url: string
  title: string | null
  source: string
  width?: number | null
  height?: number | null
}
export type Artist = {
  slug: string
  name: string
  tagline: string | null
  profile: string | null
  specialties: string[] | null
  mediums: string[] | null
  open_for_work: boolean | null
  works: Work[]
}
export type Profile = Artist & { top_tags: string[] | null; site_url: string | null; deviantart_url: string }
export type Results = { artists: Artist[]; ranked_by: 'meaning' | 'keywords' | 'name' }

export const PAGE = 24 // what /api/search returns at most (functions/api/search.ts)

// Answers are kept for the life of the tab, so going back to a page is instant. A failure is not kept.
const kept = new Map<string, Promise<unknown>>()
function once<T>(key: string, load: () => Promise<T>): Promise<T> {
  if (!kept.has(key)) kept.set(key, load().catch((error) => (kept.delete(key), Promise.reject(error))))
  return kept.get(key) as Promise<T>
}

/** A limit per visitor, or the day's, was reached (functions/api/): it lifts by itself after `seconds`. */
export class Busy extends Error {
  constructor(readonly seconds: number) {
    super('asked to wait')
  }
}

/** One GET of a server function, asked once more if it fails. A 429 is Busy. */
const get = <T>(path: string) =>
  twice(async () => {
    const response = await fetch(path, { signal: AbortSignal.timeout(20_000) })
    if (response.status === 429) return new Busy(Number(response.headers.get('retry-after')) || 60) // not asked again
    if (!response.ok) throw new Error(`${path} answered ${response.status}`)
    return (await response.json()) as T
  }).then((answer) => (answer instanceof Busy ? Promise.reject(answer) : answer))

/** `filters` is the search page's own query string: q, specialty, medium, open. */
export const search = (filters: string, offset = 0) =>
  once(`search ${filters} ${offset}`, () => get<Results>(`/api/search?${filters}&offset=${offset}`))

export const getArtist = (slug: string) =>
  once(`artist ${slug}`, () => get<Profile | null>(`/api/artist?slug=${encodeURIComponent(slug)}`))

export const similarArtists = (slug: string) =>
  once(`similar ${slug}`, () => get<Artist[]>(`/api/artist?slug=${encodeURIComponent(slug)}&similar=1`))

/** One POST to /api/removal (functions/api/removal.ts). Not asked again by itself: each one counts. */
async function removal<T>(body: object): Promise<T> {
  const response = await fetch('/api/removal', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(20_000),
  })
  if (response.status === 429) throw new Busy(Number(response.headers.get('retry-after')) || 60)
  if (!response.ok) throw new Error(`removal answered ${response.status}`)
  return (await response.json()) as T
}

/** The artist's code. Asking for it files their request, and from then on the code is looked for. */
export const removalCode = (slug: string) =>
  once(`code ${slug}`, async () => (await removal<{ code: string }>({ slug, proof: true })).code)

export const requestRemoval = (slug: string, note: string) => removal<{ ok: boolean }>({ slug, note: note.trim() || null })

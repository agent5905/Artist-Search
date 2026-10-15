// Everything the pages ask for. Search goes through the Pages Function (it needs an embedding);
// the rest are public database functions, called with the publishable key.
import { rpc, twice } from './rpc'

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

const call = <T>(name: string, args: object) =>
  rpc<T>(import.meta.env.VITE_SUPABASE_URL, import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY, name, args)

// Answers are kept for the life of the tab, so going back to a page is instant. A failure is not kept.
const kept = new Map<string, Promise<unknown>>()
function once<T>(key: string, load: () => Promise<T>): Promise<T> {
  if (!kept.has(key)) kept.set(key, load().catch((error) => (kept.delete(key), Promise.reject(error))))
  return kept.get(key) as Promise<T>
}

/** The search's limit per visitor was reached (functions/api/search.ts): it lifts by itself after `seconds`. */
export class Busy extends Error {
  constructor(readonly seconds: number) {
    super('too many searches')
  }
}

/** `filters` is the search page's own query string: q, specialty, medium, open. */
export const search = (filters: string, offset = 0) =>
  once(`search ${filters} ${offset}`, () =>
    twice(async () => {
      const response = await fetch(`/api/search?${filters}&offset=${offset}`, { signal: AbortSignal.timeout(20_000) })
      if (response.status === 429) return new Busy(Number(response.headers.get('retry-after')) || 60) // not asked again
      if (!response.ok) throw new Error(`search answered ${response.status}`)
      return (await response.json()) as Results
    }).then((answer) => (answer instanceof Busy ? Promise.reject(answer) : answer)),
  )

export const getArtist = (slug: string) =>
  once(`artist ${slug}`, async () => (await call<Profile[]>('get_artist', { p_slug: slug }))[0] ?? null)

export const similarArtists = (slug: string) => once(`similar ${slug}`, () => call<Artist[]>('similar_artists', { p_slug: slug }))

export const removalCode = (slug: string) => once(`code ${slug}`, () => call<string>('start_removal_proof', { p_slug: slug }))

export const requestRemoval = (slug: string, note: string) =>
  call<boolean>('request_removal', { p_slug: slug, p_note: note.trim() || null })

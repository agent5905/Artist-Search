// The pieces every page shares: the artist card, a work's thumbnail, icons, and the loading, empty and error states.
import { useEffect, useState, type ReactNode } from 'react'
import { Link } from 'react-router'
import type { Artist, Work } from './api'

/** "vehicle or mech" reads "Vehicle or mech"; "3d render" reads "3D render". */
export const label = (value: string) => (value.charAt(0).toUpperCase() + value.slice(1)).replace(/^3d\b/, '3D')

/** Only an http(s) address becomes a link or an image: these come from other people's pages. */
export const web = (url: string | null | undefined) => (url && /^https?:\/\//i.test(url) ? url : undefined)

export const host = (url: string) => {
  try {
    return new URL(url).hostname.replace(/^www\./, '')
  } catch {
    return ''
  }
}

/** Where a work lives, for the credit beside it. */
export const sourceName = (work: Work) => (work.source === 'deviantart' ? 'DeviantArt' : host(work.page_url) || 'the artist’s site')

/** The artist's own page at the source of a work: on DeviantArt a work sits at /<username>/art/<title>. */
export const sourceHome = (work: Work) => {
  try {
    const page = new URL(work.page_url)
    return web(work.source === 'deviantart' ? `${page.origin}/${page.pathname.split('/')[1]}` : page.origin)
  } catch {
    return undefined
  }
}

/**
 * Whether the artist says they take work, in the words their profile line uses. Null when they have not said:
 * that is unknown, never "closed".
 */
export const availability = (artist: Artist) =>
  !artist.open_for_work
    ? null
    : /available for hire/i.test(artist.profile ?? '')
      ? 'Available for hire'
      : /open for commissions/i.test(artist.profile ?? '')
        ? 'Open for commissions'
        : 'Open for work'

const ICONS = {
  search: 'M11 4a7 7 0 1 0 0 14 7 7 0 0 0 0-14Zm9 16-4.1-4.1',
  out: 'M8 16 16 8m-6.5 0H16v6.5',
  next: 'M5 12h14m-6-6 6 6-6 6',
  similar: 'M9 9h11v11H9zM5 15V4h11',
  image: 'M4 5h16v14H4zm0 11 4.5-4.5L13 16l2.5-2.5L20 18M3 3l18 18',
  person: 'M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Zm-8 8a8 8 0 0 1 16 0',
  check: 'm5 12.5 4.5 4.5L19 7.5',
  chevron: 'm9.5 6 6 6-6 6',
}

export function Icon({ name, size = 18 }: { name: keyof typeof ICONS; size?: number }) {
  return (
    <svg
      className="icon"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={ICONS[name]} />
    </svg>
  )
}

export function Logo({ size = 26 }: { size?: number }) {
  return (
    <svg className="logo" width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden="true">
      <circle cx="14" cy="14" r="9.5" stroke="currentColor" strokeWidth="3" />
      <path d="m21.5 21.5 7 7" stroke="currentColor" strokeWidth="3.5" strokeLinecap="round" />
      <circle cx="14" cy="14" r="4.2" fill="var(--accent)" />
    </svg>
  )
}

/**
 * One work: the source's own thumbnail in a plain <img> (never a copy, hard rule 1), linking to the work's page
 * at the source. The tile keeps its shape while the image loads, and when it cannot load.
 *
 * `natural` gives the tile the work's own shape, uncropped. It needs the image's width and height from the data,
 * which the database does not return yet: without both, the tile stays the fixed shape the stylesheet gives it.
 */
export function Thumb({ work, by, eager, natural }: { work: Work; by: string; eager?: boolean; natural?: boolean }) {
  const [failed, setFailed] = useState(false)
  const title = work.title?.trim() || 'Untitled'
  const source = sourceName(work)
  const sized = !!work.width && !!work.height
  return (
    <a
      className="thumb"
      href={web(work.page_url)}
      target="_blank"
      rel="noopener"
      title={`${title}, by ${by}. Opens on ${source}.`}
      style={natural && sized ? { aspectRatio: `${work.width} / ${work.height}` } : undefined}
    >
      {failed ? (
        <span className="thumb-off">
          <Icon name="image" size={22} />
          <span>{title}</span>
        </span>
      ) : (
        <img
          src={web(work.image_url)}
          alt={`${title}, by ${by}`}
          width={sized ? work.width! : undefined}
          height={sized ? work.height! : undefined}
          loading={eager ? 'eager' : 'lazy'}
          decoding="async"
          onError={() => setFailed(true)}
        />
      )}
      <span className="sr-only"> (opens on {source})</span>
    </a>
  )
}

/** Shown only when the artist has said so. An artist who has not said gets no chip on a card. */
export function OpenBadge({ artist }: { artist: Artist }) {
  const status = availability(artist)
  return status ? (
    <span className="open" title="Stated by the artist">
      {status}
    </span>
  ) : null
}

export function Tags({ values }: { values: string[] | null }) {
  if (!values?.length) return null
  return (
    <ul className="tags">
      {values.map((value) => (
        <li key={value}>{label(value)}</li>
      ))}
    </ul>
  )
}

/** The artist card: the same on the search page and the similar-style page. */
export function Card({ artist, eager }: { artist: Artist; eager?: boolean }) {
  const slug = encodeURIComponent(artist.slug)
  const works = artist.works.slice(0, 4)
  return (
    <article className="card">
      {works.length > 0 && (
        <div className="mosaic">
          {works.map((work) => (
            <Thumb key={work.page_url} work={work} by={artist.name} eager={eager} />
          ))}
        </div>
      )}
      <div className="card-body">
        <div className="card-head">
          <h3>
            <Link to={`/artist/${slug}`}>{artist.name}</Link>
          </h3>
          <OpenBadge artist={artist} />
        </div>
        {artist.tagline && <p className="card-tagline">{artist.tagline}</p>}
        {artist.profile && <p className="card-profile">{artist.profile}</p>}
        <Tags values={artist.specialties} />
        <div className="card-foot">
          <Link className="btn quiet" to={`/artist/${slug}`}>
            View profile
          </Link>
          <Link className="btn quiet" to={`/similar/${slug}`} aria-label={`More artists like ${artist.name}`}>
            More like this
          </Link>
        </div>
        {works.length > 0 && (
          <p className="credit">
            {/* The credit is a way through to the artist's own page at the source, and looks like one. */}
            <a href={sourceHome(works[0])} target="_blank" rel="noopener">
              Art © {artist.name} on {sourceName(works[0])}
              <Icon name="out" size={13} />
            </a>
          </p>
        )}
      </div>
    </article>
  )
}

export function Cards({ artists }: { artists: Artist[] }) {
  return (
    <div className="grid">
      {artists.map((artist, i) => (
        <Card key={artist.slug} artist={artist} eager={i < 3} />
      ))}
    </div>
  )
}

/** Grey cards of the real cards' shape, so nothing moves when the answer arrives. */
export function Skeletons({ count = 6 }: { count?: number }) {
  return (
    <div className="grid" aria-hidden="true">
      {Array.from({ length: count }, (_, i) => (
        <div className="card sk" key={i}>
          <div className="mosaic">
            <span className="thumb" />
            <span className="thumb" />
            <span className="thumb" />
            <span className="thumb" />
          </div>
          <div className="card-body">
            <span className="sk-line" style={{ width: '45%', height: 20 }} />
            <span className="sk-line" />
            <span className="sk-line" style={{ width: '80%' }} />
            <span className="sk-line" style={{ width: '55%', height: 36, marginTop: 14 }} />
          </div>
        </div>
      ))}
    </div>
  )
}

/** An empty result, a failure, or a page that is not there. */
export function Notice({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="notice" role="status">
      <h2>{title}</h2>
      {children}
    </div>
  )
}

export function Failed({ retry }: { retry: () => void }) {
  return (
    <Notice title="That didn’t load">
      <p>We asked twice and got no answer. It is usually back within a minute.</p>
      <button className="btn" onClick={retry}>
        Try again
      </button>
    </Notice>
  )
}

/** The search asked to be left alone for a while (Busy): nothing is broken, and it says when to try again. */
export function Slow({ seconds, retry }: { seconds: number; retry: () => void }) {
  const minutes = Math.max(1, Math.ceil(seconds / 60))
  return (
    <Notice title="That’s a lot of searches in a few minutes">
      <p>
        To keep the search free for everyone, each visitor gets a number of searches every ten minutes. Yours come back in about{' '}
        {minutes === 1 ? 'a minute' : `${minutes} minutes`}.
      </p>
      <button className="btn" onClick={retry}>
        Try again
      </button>
    </Notice>
  )
}

/**
 * Loads something for `key`. `data` stays the last answer until the next one arrives, with `stale` saying it
 * belongs to an earlier key, so a page can keep showing what it has instead of flashing empty. `error` is why it
 * failed, when it has.
 */
export function useLoad<T>(key: string, load: () => Promise<T>) {
  const [state, setState] = useState<{ key?: string; data?: T; failed?: boolean; error?: unknown }>({})
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    let current = true
    load().then(
      (data) => current && setState({ key, data }),
      (error) => current && setState({ key, failed: true, error }),
    )
    return () => {
      current = false
    }
  }, [key, attempt])
  const failed = state.key === key && !!state.failed
  return {
    data: state.data,
    stale: state.key !== key,
    failed,
    error: failed ? state.error : undefined,
    retry: () => (setState({}), setAttempt((n) => n + 1)),
  }
}

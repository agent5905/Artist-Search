// The pages: search, similar style, profile, and claim or remove.
import { useRef, useState, type FormEvent, type ReactNode } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router'
import { Busy, getArtist, PAGE, removalCode, requestRemoval, search, similarArtists } from './api'
import facets from './facets.json'
import { about, availability, Cards, Failed, host, Icon, label, Notice, Skeletons, Slow, sourceName, Tags, Thumb, useLoad, web } from './ui'

const TITLE = 'Artist Search'

// ---------------------------------------------------------------------------------------------- search

export function Search() {
  const [params, setParams] = useSearchParams()
  const q = (params.get('q') ?? '').trim()
  const picked = { specialty: params.getAll('specialty'), medium: params.getAll('medium') }
  const open = params.get('open') === '1'
  const filtered = open || picked.specialty.length + picked.medium.length > 0

  // The same search always gets the same string, whatever order the address has it in.
  const filters = new URLSearchParams([
    ['q', q],
    ...picked.specialty.map((value) => ['specialty', value]),
    ...picked.medium.map((value) => ['medium', value]),
    ...(open ? [['open', '1']] : []),
  ]).toString()

  const row = useRef<HTMLDivElement>(null)
  const [rowEnded, setRowEnded] = useState(false)
  const [more, setMore] = useState({ filters, pages: 1 })
  const pages = more.filters === filters ? more.pages : 1
  const { data, stale, failed, error, retry } = useLoad(`${filters} ${pages}`, async () => ({
    filters,
    pages: await Promise.all(Array.from({ length: pages }, (_, i) => search(filters, i * PAGE))),
  }))

  const change = (edit: (next: URLSearchParams) => void) => {
    const next = new URLSearchParams(params)
    edit(next)
    setParams(next, { preventScrollReset: true })
  }
  const toggle = (name: 'specialty' | 'medium', value: string) =>
    change((next) => {
      const now = picked[name].includes(value) ? picked[name].filter((v) => v !== value) : [...picked[name], value]
      next.delete(name)
      now.forEach((v) => next.append(name, v))
    })
  const clear = () => change((next) => ['specialty', 'medium', 'open'].forEach((name) => next.delete(name)))

  const artists = [...new Map((data?.pages ?? []).flatMap((page) => page.artists).map((a) => [a.slug, a])).values()]
  const rankedBy = data?.pages[0]?.ranked_by
  const sameSearch = data?.filters === filters
  const hasMore = sameSearch && data?.pages.at(-1)?.artists.length === PAGE

  return (
    <main id="main" className="wrap">
      <title>{q ? `${q} · ${TITLE}` : `${TITLE}: find game concept and character artists`}</title>

      <div className="filters-wrap">
        {/* On a phone the filters are one row to swipe. The arrow says so, moves the row along, and goes at the end. */}
        {!rowEnded && (
          <button
            className="filters-more"
            type="button"
            tabIndex={-1}
            aria-hidden="true"
            onClick={() => row.current?.scrollBy({ left: 200, behavior: 'smooth' })}
          >
            <Icon name="chevron" size={16} />
          </button>
        )}
        <div
          className="filters"
          role="group"
          aria-label="Filters"
          ref={row}
          onScroll={({ currentTarget: at }) => setRowEnded(at.scrollLeft + at.clientWidth >= at.scrollWidth - 8)}
        >
          <label className="chip chip-open">
            <input type="checkbox" checked={open} onChange={() => change((next) => (open ? next.delete('open') : next.set('open', '1')))} />
            Open for work
          </label>
          {(['specialty', 'medium'] as const).map((name) => (
            <div className="filter-group" role="group" aria-label={label(name)} key={name}>
              <span className="filter-label" aria-hidden="true">
                {label(name)}
              </span>
              {facets[name === 'specialty' ? 'specialties' : 'mediums'].map((value) => (
                <label className="chip" key={value}>
                  <input type="checkbox" checked={picked[name].includes(value)} onChange={() => toggle(name, value)} />
                  {label(value)}
                </label>
              ))}
            </div>
          ))}
          {filtered && (
            <button className="link" type="button" onClick={clear}>
              Clear filters
            </button>
          )}
        </div>
      </div>

      {/* Without a query the page's h1 is the headline above the search box. */}
      {q && <h1 className="sr-only">Search results</h1>}
      <div className="results-head">
        <h2>{q ? <>Artists for “{q}”</> : 'Browse artists'}</h2>
        <p role="status">
          {failed
            ? ''
            : stale && !sameSearch
              ? 'Searching…'
              : artists.length === 0
                ? ''
                : rankedBy === 'meaning'
                  ? 'Best matches first'
                  : rankedBy === 'keywords'
                    ? 'Matched on keywords only: ranking by meaning is unavailable right now'
                    : 'A to Z'}
        </p>
      </div>

      {failed ? (
        error instanceof Busy ? <Slow seconds={error.seconds} retry={retry} /> : <Failed retry={retry} />
      ) : !data ? (
        <Skeletons />
      ) : artists.length === 0 ? (
        <Notice title="No artists match">
          <p>
            {filtered ? 'Nothing fits every filter you picked. ' : 'Nothing matched those words. '}
            Try {filtered ? 'fewer filters, or ' : ''}describing the style or the subject another way.
          </p>
          {filtered && (
            <button className="btn" onClick={clear}>
              Clear filters
            </button>
          )}
        </Notice>
      ) : (
        <div className={sameSearch ? undefined : 'dim'} aria-busy={!sameSearch}>
          <Cards artists={artists} />
        </div>
      )}

      {hasMore && (
        <p className="more">
          <button className="btn quiet" disabled={stale} onClick={() => setMore({ filters, pages: pages + 1 })}>
            {stale ? 'Loading…' : 'Show more artists'}
          </button>
        </p>
      )}
    </main>
  )
}

// ---------------------------------------------------------------------------------------- similar style

export function Similar() {
  const { slug = '' } = useParams()
  const seed = useLoad(`artist ${slug}`, () => getArtist(slug))
  const near = useLoad(`similar ${slug}`, () => similarArtists(slug))

  if (seed.failed) return <Page><Failed retry={seed.retry} /></Page>
  if (seed.stale) return <Page><Skeletons /></Page>
  if (!seed.data) return <Gone />
  const artist = seed.data

  return (
    <Page>
      <title>{`More like ${artist.name} · ${TITLE}`}</title>
      <section className="seed">
        <div className="seed-text">
          <p className="eyebrow">More like this artist</p>
          <h1 className="display">{artist.name}</h1>
          {artist.profile && <p>{artist.profile}</p>}
          <Link className="btn quiet" to={`/artist/${encodeURIComponent(artist.slug)}`}>
            View profile and all {artist.works.length} works
          </Link>
        </div>
        <div className="strip" role="group" aria-label={`Works by ${artist.name}`}>
          {artist.works.map((work) => (
            <Thumb key={work.page_url} work={work} by={artist.name} eager />
          ))}
        </div>
      </section>

      <div className="results-head">
        <h2>Artists with a similar style</h2>
        <p role="status">{near.stale ? 'Looking…' : near.data?.length ? 'Closest first, judged by how their work looks' : ''}</p>
      </div>
      {near.failed ? (
        <Failed retry={near.retry} />
      ) : near.stale || !near.data ? (
        <Skeletons />
      ) : near.data.length === 0 ? (
        <Notice title="No close matches yet">
          <p>Nobody in the index is close enough to {artist.name} in style. A search by subject may do better.</p>
          <Link className="btn" to="/">
            Search artists
          </Link>
        </Notice>
      ) : (
        <Cards artists={near.data} />
      )}
    </Page>
  )
}

// ---------------------------------------------------------------------------------------------- profile

export function ProfilePage() {
  const { slug = '' } = useParams()
  const { data, stale, failed, retry } = useLoad(`artist ${slug}`, () => getArtist(slug))

  if (failed) return <Page><Failed retry={retry} /></Page>
  if (stale)
    return (
      <Page>
        <div className="profile sk" aria-hidden="true">
          <div className="profile-head">
            <div className="profile-about">
              <span className="sk-line" style={{ width: 'min(60%, 300px)', height: 44 }} />
              <span className="sk-line" style={{ width: 'min(100%, 560px)', height: 18 }} />
              <span className="sk-line" style={{ width: 'min(80%, 440px)', height: 18 }} />
            </div>
          </div>
          <div className="works">
            {Array.from({ length: 10 }, (_, i) => (
              <span className="thumb" key={i} />
            ))}
          </div>
        </div>
      </Page>
    )
  if (!data) return <Gone />
  const artist = data
  const source = artist.works[0] ? sourceName(artist.works[0]) : 'DeviantArt'
  const site = web(artist.site_url)
  const deviantart = web(artist.deviantart_url)
  const status = availability(artist)
  // Uncropped tiles need every work's size, which the database does not return yet.
  const natural = artist.works.length > 0 && artist.works.every((work) => work.width && work.height)

  return (
    <Page>
      <title>{`${artist.name} · ${TITLE}`}</title>
      <div className="profile">
        <header className="profile-head">
          <div className="profile-about">
            <h1 className="display">{artist.name}</h1>
            {artist.tagline && <p className="tagline">{artist.tagline}</p>}
            {artist.profile && <p className="lead">{artist.profile}</p>}
            <dl className="facts">
              {!!artist.specialties?.length && (
                <div>
                  <dt>Specialties</dt>
                  <dd>
                    <Tags values={artist.specialties} />
                  </dd>
                </div>
              )}
              {!!artist.mediums?.length && (
                <div>
                  <dt>Mediums</dt>
                  <dd>
                    <Tags values={artist.mediums} />
                  </dd>
                </div>
              )}
            </dl>
          </div>

          {/* What someone hiring needs: is the artist taking work, and where to reach them. No email, rate or
              location is stored, so none is shown: contact goes to the artist's own site, or their DeviantArt page. */}
          <aside className="hire" aria-label={`Hiring ${artist.name}`}>
            <p className={status ? 'avail is-open' : 'avail'}>
              <strong>{status ?? 'Availability not stated'}</strong>
              <span>{status ? 'Stated by the artist' : 'Ask the artist directly'}</span>
            </p>
            <div className="hire-actions">
              <a className="btn wide" href={site ?? deviantart} target="_blank" rel="noopener">
                {site ? `Contact via ${host(site)}` : 'Contact on DeviantArt'} <Icon name="out" size={16} />
              </a>
              <Link className="btn quiet" to={`/similar/${encodeURIComponent(artist.slug)}`} aria-label={`More artists like ${artist.name}`}>
                <Icon name="similar" size={16} /> More like this
              </Link>
              {site && (
                <a className="btn quiet" href={deviantart} target="_blank" rel="noopener">
                  DeviantArt profile <Icon name="out" size={16} />
                </a>
              )}
            </div>
            <p className="fine">Artist Search holds no email, rates or location: you reach {artist.name} on their own pages.</p>
          </aside>
        </header>

        <p className="claim-line">
          <Icon name="person" size={16} />
          <span>
            Is this you? <Link to={`/remove/${encodeURIComponent(artist.slug)}`}>Claim or remove this profile</Link>
          </span>
        </p>

        <section className="profile-works" aria-labelledby="works">
          <div className="results-head">
            <h2 id="works">Works</h2>
            <p>
              {artist.works.length}, shown from {source}
            </p>
          </div>
          <ul className={natural ? 'works natural' : 'works'}>
            {artist.works.map((work) => (
              <li key={work.page_url}>
                <Thumb work={work} by={artist.name} natural={natural} />
                <p>{work.title?.trim() || 'Untitled'}</p>
              </li>
            ))}
          </ul>
          <p className="credit">
            All artwork © {artist.name}. Each thumbnail is served by {source}, where {artist.name} posted it, and opens the work on its own
            page there.
          </p>
          {!!artist.top_tags?.length && (
            <div className="own-tags">
              <h2>The artist’s own tags</h2>
              <ul>
                {artist.top_tags.map((tag) => (
                  <li key={tag}>
                    <Link to={`/?q=${encodeURIComponent(tag)}`}>#{tag}</Link>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </section>
      </div>
    </Page>
  )
}

// ------------------------------------------------------------------------------------- claim or remove

const PROOF = 'artist-search-removal' // the <meta> name and the file the verifier reads (pipeline/removals.py)

export function Remove() {
  const { slug } = useParams()
  return slug ? <RemoveProfile slug={slug} key={slug} /> : <RemoveStart />
}

/** Reached from the footer, where no profile is in view: explain, and help the artist find theirs. */
function RemoveStart() {
  const navigate = useNavigate()
  const find = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const name = String(new FormData(event.currentTarget).get('name') ?? '').trim()
    if (name) navigate(`/?q=${encodeURIComponent(name)}`)
  }
  return (
    <Page narrow>
      <title>{`Claim or remove a profile · ${TITLE}`}</title>
      <h1 className="display">Claim or remove a profile</h1>
      <p className="lead">
        Thank you for getting in touch. Profiles here are put together from work artists posted publicly on DeviantArt. If one is yours
        and you would rather not be listed, it will be deleted, and you will not be added again.
      </p>
      <section className="option">
        <h2>First, find your profile</h2>
        <p>Search for your name or DeviantArt username, open your profile, and follow “Is this you?” on it.</p>
        <form className="inline-form" onSubmit={find}>
          <label className="sr-only" htmlFor="find-name">
            Your name or DeviantArt username
          </label>
          <input id="find-name" name="name" placeholder="Your name or DeviantArt username" autoComplete="off" maxLength={80} />
          <button className="btn">Find my profile</button>
        </form>
      </section>
      <section className="option">
        <h2>Then pick one of two ways</h2>
        <ul className="ways">
          <li>
            <strong>With proof.</strong> You get a short code to put in your DeviantArt profile’s About text or tagline, or on your own
            site. Once the code is found there, the profile is removed for good.
          </li>
          <li>
            <strong>Without proof.</strong> You send a request, with a note if you like. The site owner reads it and decides.
          </li>
        </ul>
        <p className="fine">No account or email is needed for either.</p>
      </section>
    </Page>
  )
}

function RemoveProfile({ slug }: { slug: string }) {
  const artist = useLoad(`artist ${slug}`, () => getArtist(slug))
  const [code, setCode] = useState('')
  const [copied, setCopied] = useState('')
  const [sent, setSent] = useState(false)
  const [busy, setBusy] = useState('')
  const [problem, setProblem] = useState<{ kind: string; error: unknown }>()

  if (artist.failed) return <Page narrow><Failed retry={artist.retry} /></Page>
  if (artist.stale) return <Page narrow><p className="fine" role="status">Loading…</p></Page>
  if (!artist.data) return <Gone />
  const { name, site_url, deviantart_url } = artist.data
  const site = web(site_url)
  const tag = `<meta name="${PROOF}" content="${code}">`
  const file = site ? new URL(`/.well-known/${PROOF}.txt`, site).href : ''

  const ask = async (kind: 'code' | 'request', run: () => Promise<void>) => {
    setBusy(kind)
    setProblem(undefined)
    try {
      await run()
    } catch (error) {
      setProblem({ kind, error })
    } finally {
      setBusy('')
    }
  }
  const copy = (text: string) =>
    navigator.clipboard.writeText(text).then(
      () => setCopied(text),
      () => {},
    )
  const copyRow = (text: string) => (
    <span className="code-row">
      <code>{text}</code>
      <button className="btn quiet" type="button" onClick={() => copy(text)}>
        {copied === text ? 'Copied' : 'Copy'}
      </button>
    </span>
  )
  const failure = (kind: string) =>
    problem?.kind === kind && (
      <p className="problem" role="alert">
        {problem.error instanceof Busy
          ? `That is a lot of requests from your connection today. Please try again in ${about(problem.error.seconds)}.`
          : 'That did not go through. Nothing was sent; please try again in a minute.'}
      </p>
    )

  return (
    <Page narrow>
      <title>{`Claim or remove ${name} · ${TITLE}`}</title>
      <p className="back">
        <Link to={`/artist/${encodeURIComponent(slug)}`}>← Back to {name}’s profile</Link>
      </p>
      <h1 className="display">Claim or remove this profile</h1>
      <p className="lead">
        Thank you for getting in touch. This profile was put together from work <strong>{name}</strong> posted publicly on DeviantArt. If
        it is yours and you would rather not be listed, it will be deleted, and you will not be added again.
      </p>

      <section className="option">
        <h2>
          <span className="num">1</span> Remove it with proof
        </h2>
        <p>Show the profile is yours by putting a short code where only you can write.</p>
        {code ? (
          <ol className="steps">
            <li>
              <span>Your code:</span>
              {copyRow(code)}
            </li>
            <li>
              <span>Put it in one of these places. Capitals and spaces around it do not matter; anywhere else does not count.</span>
              <ul className="ways">
                <li>
                  <strong>On DeviantArt:</strong> paste it into the About text or the tagline of{' '}
                  <a href={deviantart_url}>the DeviantArt profile this one was made from</a>.
                </li>
                {site && (
                  <li>
                    <strong>Or on {host(site)}:</strong> add this line to the head of your front page, between &lt;head&gt; and
                    &lt;/head&gt;:
                    {copyRow(tag)}
                    or put a plain text file that holds only the code at this address:
                    {copyRow(file)}
                    This counts while your DeviantArt profile gives {host(site)} as your website, and unless your robots.txt keeps out our
                    crawler, ArtistSearchBot. A comment or a guestbook entry on your page never counts, so no visitor can remove you.
                  </li>
                )}
              </ul>
            </li>
            <li>
              <span>
                That is all. We look for the code regularly, and as soon as it is found this profile is deleted and you will not be added
                again. You can take the code down after that.
              </span>
            </li>
          </ol>
        ) : (
          <>
            <button className="btn" type="button" disabled={busy === 'code'} onClick={() => ask('code', async () => setCode(await removalCode(slug)))}>
              {busy === 'code' ? 'Getting your code…' : 'Get my code'}
            </button>
            {failure('code')}
          </>
        )}
        <p className="fine">
          If this profile is still here a day after you put the code up, check that it is exactly where step 2 says. If it is, or you cannot
          put it there, send a request below and the site owner will look at it.
        </p>
      </section>

      <section className="option">
        <h2>
          <span className="num">2</span> Ask without proof
        </h2>
        <p>Can’t put the code up? Send a request. The site owner reads every one and decides; nothing changes until then.</p>
        {sent ? (
          <p className="thanks" role="status">
            <Icon name="check" size={20} />
            <span>
              <strong>Thank you.</strong> Your request is with the site owner. If they remove this profile, you will not be added again.
            </span>
          </p>
        ) : (
          <form
            className="stack-form"
            onSubmit={(event) => {
              event.preventDefault()
              const note = String(new FormData(event.currentTarget).get('note') ?? '')
              ask('request', async () => (await requestRemoval(slug, note), setSent(true)))
            }}
          >
            <label htmlFor="note">Anything the owner should know (optional)</label>
            <textarea id="note" name="note" rows={3} maxLength={300} placeholder="For example: this is my work and I’d like it taken down." />
            {failure('request')}
            <button className="btn" disabled={busy === 'request'}>
              {busy === 'request' ? 'Sending…' : 'Send removal request'}
            </button>
          </form>
        )}
      </section>

      <section className="option plain">
        <h2>Want to keep the profile and fix it?</h2>
        <p>
          Editing your own profile is not built yet. Until it is, use the note above to say what is wrong, and the owner will correct it or
          take the profile down.
        </p>
        <p className="fine">No account or email is needed. Only your note, if you write one, is sent.</p>
      </section>
    </Page>
  )
}

// ------------------------------------------------------------------------------------------------ misc

function Page({ children, narrow }: { children: ReactNode; narrow?: boolean }) {
  return (
    <main id="main" className={narrow ? 'wrap narrow' : 'wrap'}>
      {children}
    </main>
  )
}

/** No such artist: never listed, or removed. A removed artist is not named or hinted at. */
function Gone() {
  return (
    <Page>
      <title>{`Not listed · ${TITLE}`}</title>
      <Notice title="This artist isn’t listed">
        <p>There is no profile at this address. Artists who ask to be removed are deleted and never added again.</p>
        <Link className="btn" to="/">
          Search artists
        </Link>
      </Notice>
    </Page>
  )
}

export function NotFound() {
  return (
    <Page>
      <title>{`Page not found · ${TITLE}`}</title>
      <Notice title="Page not found">
        <p>Nothing lives at this address.</p>
        <Link className="btn" to="/">
          Search artists
        </Link>
      </Notice>
    </Page>
  )
}

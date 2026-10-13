// Entry: the routes, and the frame every page sits in (the search box on top, the honest footer below).
import { StrictMode, useEffect, useState, type FormEvent } from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, Link, Outlet, RouterProvider, ScrollRestoration, useLocation, useNavigate, useSearchParams } from 'react-router'
import { NotFound, ProfilePage, Remove, Search, Similar } from './pages'
import { Icon, Logo } from './ui'
import './styles.css'

const REPO = 'https://github.com/agent5905/Artist-Search'

// Each of these was run against the live index and returns artists who fit.
const EXAMPLES = [
  'dark fantasy creature designer',
  'sci-fi environment concept artist',
  'someone who paints dragons',
  'mech and robot concept designer',
  'painterly fantasy character artist, open for commissions',
]

function SearchBox({ big }: { big: boolean }) {
  const { pathname } = useLocation()
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const searching = pathname === '/'
  const q = searching ? (params.get('q') ?? '') : ''
  const [text, setText] = useState(q)
  useEffect(() => setText(q), [q])

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const next = new URLSearchParams(searching ? params : undefined) // a new query keeps the filters in use
    if (text.trim()) next.set('q', text.trim())
    else next.delete('q')
    navigate({ pathname: '/', search: next.toString() })
  }

  return (
    <form className="search" role="search" onSubmit={submit}>
      <Icon name="search" size={20} />
      <label className="sr-only" htmlFor="q">
        Describe the art or the artist you need
      </label>
      <input
        id="q"
        name="q"
        type="search"
        value={text}
        onChange={(event) => setText(event.target.value)}
        placeholder="Describe the art you need"
        maxLength={200}
        autoComplete="off"
        enterKeyHint="search"
        // Ready to type on a desktop; on a phone this would only throw the keyboard over the page.
        autoFocus={big && matchMedia('(pointer: fine)').matches}
      />
      <button type="submit">Search</button>
    </form>
  )
}

function Layout() {
  const { pathname } = useLocation()
  const [params] = useSearchParams()
  const hero = pathname === '/' && !params.get('q')?.trim()
  const profile = /^\/(?:artist|remove)\/([^/]+)/.exec(pathname)?.[1]

  return (
    <>
      <a className="skip" href="#main">
        Skip to content
      </a>
      <header className={hero ? 'top is-hero' : 'top'}>
        <div className="wrap top-in">
          <Link className="brand" to="/" aria-label="Artist Search, home">
            <Logo />
            <span>Artist Search</span>
          </Link>
          <div className="top-search">
            {hero && (
              <>
                <h1>Find the artist your game needs.</h1>
                <p className="sub">Describe the art you’re after and see the artists whose work fits.</p>
              </>
            )}
            <SearchBox big={hero} />
            {hero && (
              <p className="examples">
                <span>Try</span>
                {EXAMPLES.map((example) => (
                  <Link key={example} to={`/?q=${encodeURIComponent(example)}`}>
                    {example}
                  </Link>
                ))}
              </p>
            )}
          </div>
        </div>
      </header>

      <Outlet />

      <footer className="foot">
        <div className="wrap foot-in">
          <p>
            <strong>Artist Search</strong> is a portfolio project: a search engine for game concept and character artists. Artwork is shown
            straight from DeviantArt, where each artist posted it. No image is copied or stored here, and every work links back to its own
            page.
          </p>
          <nav aria-label="About this site">
            <a href={REPO}>Source code on GitHub</a>
            <Link to={profile ? `/remove/${profile}` : '/remove'}>Claim or remove a profile</Link>
          </nav>
        </div>
      </footer>
      {/* A page opened afresh has no history key of its own: go by its address, so a new address starts at the top. */}
      <ScrollRestoration getKey={(location) => (location.key === 'default' ? location.pathname + location.search : location.key)} />
    </>
  )
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <Search /> },
      { path: '/similar/:slug', element: <Similar /> },
      { path: '/artist/:slug', element: <ProfilePage /> },
      { path: '/remove/:slug?', element: <Remove /> },
      { path: '*', element: <NotFound /> },
    ],
  },
])

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
)

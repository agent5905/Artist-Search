// The site's server functions (search, an artist, removal): their requests, their limits, fallback and retry; a card's
// works; and what the built site may contain. No network: Workers AI and the database are stand-ins. The database's
// own count (client_wait in sql/003_limits.sql) is run for real by tests/test_db.py; here a stand-in counts by visitor,
// one call at a time.
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseEnv } from 'node:util'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { handle as artistPage } from '../functions/api/artist'
import { claim as claimPage, handle as removal } from '../functions/api/removal'
import { client, handle, MAX_QUERY, network, onRequestGet, TOO_MANY, type Env } from '../functions/api/search'
import { Busy, claim, getArtist, removalCode, requestRemoval, search, similarArtists } from '../src/api'
import { ProofPlaces } from '../src/pages'
import { call, OWN_PAGE } from '../src/rpc'
import { Cards } from '../src/ui'

const VECTOR = Array.from({ length: 1024 }, (_, i) => i / 1024)
const KEY = 'FAKE-PUBLISHABLE-KEY'
const TOKEN = 'FAKE-SITE-TOKEN-0123456789'
const HEADERS = { apikey: KEY, 'content-type': 'application/json', 'accept-encoding': 'gzip' }
const embeds = () => vi.fn(async () => ({ shape: [1, 1024], data: [VECTOR] }))
const env = (run: Env['AI']['run'] = embeds()): Env => ({
  AI: { run },
  VITE_SUPABASE_URL: 'https://db.test',
  VITE_SUPABASE_PUBLISHABLE_KEY: KEY,
  SITE_TOKEN: TOKEN,
})
const CARDS = [{ slug: 'someone', name: 'Someone' }]

/**
 * A database. client_wait counts each call by kind and visitor and says "wait 120 seconds" past `limit`, as the real
 * one does, and counts nothing it refuses. Every other function gives these answers in turn, then CARDS; an Error
 * stands for a dropped connection.
 */
function database(...answers: (Response | Error)[]) {
  const calls: { name: string; init: RequestInit; args: Record<string, unknown> }[] = []
  const db = {
    limit: Infinity,
    counts: new Map<string, number>(),
    calls,
    called: (name: string) => calls.filter((call) => call.name === name),
    sent: (n = 0) => db.called('site_search')[n].args,
    fetcher: (async (url: string, init: RequestInit) => {
      const name = url.replace('https://db.test/rest/v1/rpc/', '')
      const args = JSON.parse(String(init.body))
      calls.push({ name, init, args })
      if (name === 'client_wait') {
        const key = `${args.p_kind} ${args.p_client}`
        const over = (db.counts.get(key) ?? 0) >= db.limit
        if (!over) db.counts.set(key, (db.counts.get(key) ?? 0) + 1)
        return Response.json(over ? 120 : 0)
      }
      const answer = answers.shift() ?? Response.json(CARDS)
      if (answer instanceof Error) throw answer
      return answer
    }) as typeof fetch,
  }
  return db
}
const PAGE_HEADER = { [OWN_PAGE]: '1' } // what the site's own pages send with every call (src/api.ts)
const ask = (query: string, run: Env['AI']['run'], db = database()) =>
  handle(new Request(`https://artist-search.test/api/search?${query}`, { headers: PAGE_HEADER }), env(run), db.fetcher)

describe('GET /api/search', () => {
  it('counts the visitor, then asks site_search for the query, its embedding and the filters, with the token', async () => {
    const run = embeds()
    const db = database()
    const response = await ask('q=dark+fantasy+creature+designer&specialty=creature&specialty=character&medium=digital+painting&open=1&offset=24', run, db)

    expect(run).toHaveBeenCalledExactlyOnceWith('@cf/baai/bge-m3', { text: ['dark fantasy creature designer'] })
    expect(db.calls.map((call) => call.name)).toEqual(['client_wait', 'site_search'])
    expect(db.calls[0].args).toEqual({ p_token: TOKEN, p_kind: 'search', p_client: '', p_network: '' })
    for (const call of db.calls) {
      expect(call.init.method).toBe('POST')
      expect(call.init.headers).toEqual(HEADERS)
    }
    expect(db.sent()).toEqual({
      p_token: TOKEN,
      q: 'dark fantasy creature designer',
      q_embedding: VECTOR,
      p_mediums: ['digital painting'],
      p_specialties: ['creature', 'character'],
      p_open_only: true,
      p_offset: 24,
    })
    expect(response.status).toBe(200)
    expect(response.headers.get('cache-control')).toBe('no-store')
    expect(await response.json()).toEqual({ artists: CARDS, ranked_by: 'meaning' })
  })

  it('caps the query, and reads no filter and no page it was not given', async () => {
    const run = embeds()
    const db = database()
    await ask(`q=${'x'.repeat(5000)}&specialty=&offset=-3`, run, db)
    await ask('q=robots&offset=99999', run, db)
    await ask('q=robots&offset=soon&open=yes', run, db)

    expect(db.sent(0).q).toHaveLength(MAX_QUERY)
    expect(run.mock.calls[0]).toEqual(['@cf/baai/bge-m3', { text: ['x'.repeat(MAX_QUERY)] }])
    expect(db.sent(0)).toMatchObject({ p_mediums: null, p_specialties: null, p_open_only: false, p_offset: 0 })
    expect(db.sent(1).p_offset).toBe(1000)
    expect(db.sent(2)).toMatchObject({ p_offset: 0, p_open_only: false })
  })

  it('browses without Workers AI when nothing is typed', async () => {
    const run = embeds()
    const db = database()
    const response = await ask('q=++&specialty=environment', run, db)

    expect(run).not.toHaveBeenCalled()
    expect(db.sent()).toMatchObject({ q: '', q_embedding: null, p_specialties: ['environment'] })
    expect(await response.json()).toMatchObject({ ranked_by: 'name' })
  })

  it('falls back to keywords when the embedding fails, in any way', async () => {
    const failures: Env['AI']['run'][] = [
      async () => Promise.reject(new Error('Workers AI is down')),
      async () => ({ data: [[0.1, 0.2]] }), // not 1,024 numbers
      async () => ({ data: [VECTOR.map(String)] }), // not numbers
      async () => ({}),
    ]
    for (const run of failures) {
      const db = database()
      const response = await ask('q=painterly+dragons', run, db)

      expect(db.sent()).toMatchObject({ q: 'painterly dragons', q_embedding: null })
      expect(response.status).toBe(200)
      expect(await response.json()).toEqual({ artists: CARDS, ranked_by: 'keywords' })
    }
  })

  it('asks the database for the page once more when it fails, and only once more', async () => {
    const stalled = () => new Response('{"code":"57014"}', { status: 500 })

    const recovers = database(stalled(), Response.json(CARDS))
    expect((await ask('q=mechs', embeds(), recovers)).status).toBe(200)
    expect(recovers.called('site_search')).toHaveLength(2)
    expect(recovers.sent(1)).toEqual(recovers.sent(0))

    const dropped = database(new TypeError('fetch failed'), Response.json(CARDS))
    expect(await (await ask('q=mechs', embeds(), dropped)).json()).toMatchObject({ artists: CARDS })
    expect(dropped.called('site_search')).toHaveLength(2)

    const down = database(stalled(), stalled(), Response.json(CARDS))
    const response = await ask('q=mechs', embeds(), down)
    expect(response.status).toBe(502)
    expect(await response.json()).toEqual({ error: 'The search is not answering right now.' })
    expect(down.called('site_search')).toHaveLength(2)
  })
})

const from = (address: string, path = '/api/search?q=mechs') =>
  new Request(`https://artist-search.test${path}`, { headers: { 'cf-connecting-ip': address, ...PAGE_HEADER } })

describe('the limits, which the database keeps', () => {
  it('answers 429 when the database says wait, before Workers AI or the search is asked, and holds up nobody else', async () => {
    const run = embeds()
    const db = database()
    db.limit = 3
    for (let n = 0; n < 3; n++) expect((await handle(from('203.0.113.7'), env(run), db.fetcher)).status).toBe(200)

    const over = await handle(from('203.0.113.7'), env(run), db.fetcher)
    expect(over.status).toBe(429)
    expect(over.headers.get('retry-after')).toBe('120')
    expect(over.headers.get('cache-control')).toBe('no-store')
    expect(await over.json()).toEqual({ error: TOO_MANY })
    expect(run).toHaveBeenCalledTimes(3)
    expect(db.called('site_search')).toHaveLength(3)

    expect((await handle(from('198.51.100.1'), env(run), db.fetcher)).status).toBe(200)
  })

  it('lets through only what the database lets through: 600 searches at once from one address', async () => {
    const run = embeds()
    const db = database()
    db.limit = 60
    const answers = await Promise.all(Array.from({ length: 600 }, () => handle(from('203.0.113.7'), env(run), db.fetcher)))

    expect(answers.filter((answer) => answer.status === 200)).toHaveLength(60)
    expect(answers.filter((answer) => answer.status === 429)).toHaveLength(540)
    expect(db.called('client_wait')).toHaveLength(600) // every one asked about
    expect(run).toHaveBeenCalledTimes(60)
    expect(db.called('site_search')).toHaveLength(60)
  })

  it('lets nothing through unless the count answers with a number, and asks the count once only', async () => {
    // The count answers badly; site_search would answer well.
    let counted = 0
    const counting = (wait: () => Response) =>
      (async (url: string) => (url.endsWith('client_wait') ? (counted++, wait()) : Response.json(CARDS))) as unknown as typeof fetch
    const run = embeds()
    expect((await handle(from('203.0.113.7'), env(run), counting(() => new Response('{}', { status: 500 })))).status).toBe(502)
    expect(counted).toBe(1) // a count that may have been made is not made again

    for (const answer of ['"soon"', 'null', '[]', '""', 'false', '"0"']) {
      const told = await handle(from('203.0.113.7'), env(run), counting(() => new Response(answer)))
      expect([answer, told.status, told.headers.get('retry-after')]).toEqual([answer, 429, '60'])
    }
    expect(run).not.toHaveBeenCalled()
  })

  it('counts the live entry point too', async () => {
    const db = database()
    db.limit = 0
    vi.stubGlobal('fetch', db.fetcher)
    try {
      const response = await onRequestGet({ request: from('203.0.113.7'), env: env() })
      expect(response.status).toBe(429)
      expect(db.calls.map((call) => call.name)).toEqual(['client_wait'])
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('counts an IPv6 visitor by their /64 and network by its /48, however the address is written', () => {
    const visitor = (address: string) => client(from(address))
    expect(visitor('2001:db8:aa:bb::1')).toBe('2001:db8:aa:bb::/64')
    expect(visitor('2001:db8:aa:bbcc:ffff:1:2:3')).toBe('2001:db8:aa:bbcc::/64')
    expect(visitor('2001:0db8:00aa:bbff::9')).toBe('2001:db8:aa:bbff::/64')
    expect(visitor('2001:DB8:AA:BBFF:0:0:0:9')).toBe('2001:db8:aa:bbff::/64')
    expect(visitor('2001:db8::1')).toBe('2001:db8:0:0::/64')
    expect(visitor('1::2:3:4:5:6:7')).toBe('1:0:2:3::/64')
    expect(visitor('1:2:3:4567::')).toBe('1:2:3:4567::/64')
    // An IPv4 tail is two groups, so the same address written with one is the same visitor.
    expect(visitor('2001:db8::5:6:7:1.2.3.4')).toBe('2001:db8:0:5::/64')
    expect(visitor('2001:db8::5:6:7:1.2.3.4')).toBe(visitor('2001:db8:0:5:6:7:102:304'))
    // One subscriber's /64: whatever address a device takes in it (a new private one every day or so) is one visitor.
    const in64 = Array.from({ length: 256 }, (_, n) => visitor(`2001:db8:aa:1200:${((n * 0x9e37) & 0xffff).toString(16)}:${n}:${n * 7}:1`))
    expect(new Set(in64)).toEqual(new Set(['2001:db8:aa:1200::/64']))
    // Phones on one carrier: each its own /64, each its own visitor, and the 4,096 of a /48 one network.
    const phones = Array.from({ length: 4096 }, (_, n) => from(`2001:db8:77:${n.toString(16)}::1`))
    expect(new Set(phones.map(client)).size).toBe(4096)
    expect(new Set(phones.map(network))).toEqual(new Set(['2001:db8:77::/48']))
    expect(network(from('2001:db8:78::1'))).toBe('2001:db8:78::/48')
  })

  it('counts an IPv4 visitor by their address and network by its /24, an IPv4 address written as IPv6 too', () => {
    expect([client(from('203.0.113.7')), network(from('203.0.113.7'))]).toEqual(['203.0.113.7', '203.0.113.0/24'])
    expect([client(from('::ffff:203.0.113.7')), network(from('::FFFF:203.0.113.200'))]).toEqual(['203.0.113.7', '203.0.113.0/24'])
    expect(network(from('203.0.114.7'))).not.toBe(network(from('203.0.113.7')))
    // Only the address Cloudflare saw: headers a client can set change nothing.
    const spoofed = { 'cf-connecting-ip': '203.0.113.7', 'x-forwarded-for': '1.2.3.4', 'x-real-ip': '5.6.7.8' }
    const request = new Request('https://x.test/api/search', { headers: spoofed })
    expect([client(request), network(request)]).toEqual(['203.0.113.7', '203.0.113.0/24'])
  })

  it('sends the visitor and their network to the count, for every kind of request', async () => {
    const db = database()
    const site = env()
    await handle(from('2001:db8:77:1200::5'), site, db.fetcher)
    await artistPage(from('2001:db8:77:1200::5', '/api/artist?slug=someone'), site, db.fetcher)
    await artistPage(from('2001:db8:77:1200::5', '/api/artist?slug=someone&similar=1'), site, db.fetcher)
    await claimPage(from('2001:db8:77:1200::5', '/api/removal?slug=someone'), site, db.fetcher)
    for (const body of [{ slug: 'someone', proof: true }, { slug: 'someone' }]) await removal(post(body, '2001:db8:77:1200::5'), site, db.fetcher)

    const asked = { p_token: TOKEN, p_client: '2001:db8:77:1200::/64', p_network: '2001:db8:77::/48' }
    expect(db.called('client_wait').map((call) => call.args)).toEqual(
      ['search', 'profile', 'similar', 'claim', 'removal', 'removal'].map((kind) => ({ ...asked, p_kind: kind })),
    )
  })

  it('never puts the token or the key in an answer', async () => {
    const answers = [
      await ask('q=mechs', embeds()),
      await ask('q=mechs', embeds(), database(new Error(TOKEN), new Error(KEY))),
      await handle(from('203.0.113.7'), env(), Object.assign(database(), { limit: 0 }).fetcher),
      await artistPage(from('203.0.113.7', '/api/artist?slug=x'), env(), database(new Error(TOKEN), new Error(KEY)).fetcher),
      await removal(post({ slug: 'someone', proof: true }), env(), database(new Response(TOKEN, { status: 500 }), new Error(KEY)).fetcher),
      await claimPage(from('203.0.113.7', '/api/removal?slug=x'), env(), database(new Error(TOKEN), new Error(KEY)).fetcher),
      await claimPage(from('203.0.113.7', '/api/removal?slug=x'), env(), database(new Response(TOKEN, { status: 500 }), new Error(KEY)).fetcher),
    ]
    for (const answer of answers) {
      const text = (await answer.text()) + JSON.stringify([...answer.headers])
      expect(text).not.toContain(TOKEN)
      expect(text).not.toContain(KEY)
    }
  })

  it('is not asked again by the page, which is told how long to wait', async () => {
    const asked = vi.fn(async (_url: string) => Response.json({ error: TOO_MANY }, { status: 429, headers: { 'retry-after': '150' } }))
    vi.stubGlobal('fetch', asked)
    try {
      for (const load of [() => search('q=limited'), () => getArtist('limited'), () => similarArtists('limited'), () => claim('limited')]) {
        const error = await load().catch((reason: unknown) => reason)
        expect(error).toBeInstanceOf(Busy)
        expect((error as Busy).seconds).toBe(150)
      }
      expect(asked.mock.calls.map(([url]) => url)).toEqual([
        '/api/search?q=limited&offset=0',
        '/api/artist?slug=limited',
        '/api/artist?slug=limited&similar=1',
        '/api/removal?slug=limited',
      ])
    } finally {
      vi.unstubAllGlobals()
    }
  })
})

describe('a call another site’s page makes the visitor’s browser send', () => {
  // The fifth critic's: an <img src> or a no-cors fetch on any page was counted and served like the visitor's own
  // search, so a hostile page could make its visitors spend the day's reads, each from a network of their own.
  const forced = (path: string, headers: Record<string, string> = {}, init: RequestInit = {}) =>
    new Request(`https://artist-search.test${path}`, { ...init, headers: { 'cf-connecting-ip': '203.0.113.7', ...headers } })
  const asSent = { 'sec-fetch-site': 'cross-site', 'sec-fetch-mode': 'no-cors', referer: 'https://hostile.example/thread' }

  it('is refused by every function that spends a limit, before the database is asked anything', async () => {
    for (const headers of [asSent, {}]) {
      // As a browser marks it, and as an old browser that marks nothing sends it: neither carries the pages' header.
      const db = database()
      const body = JSON.stringify({ slug: 'someone', ticket: 'FAKE-TICKET', proof: true })
      const answers = [
        await handle(forced('/api/search?q=knight', headers), env(), db.fetcher),
        await artistPage(forced('/api/artist?slug=someone', headers), env(), db.fetcher),
        await artistPage(forced('/api/artist?slug=someone&similar=1', headers), env(), db.fetcher),
        await claimPage(forced('/api/removal?slug=someone', headers), env(), db.fetcher),
        await removal(forced('/api/removal', { ...headers, 'content-type': 'application/json' }, { method: 'POST', body }), env(), db.fetcher),
      ]
      expect(answers.map((answer) => [answer.status, answer.headers.get('cache-control')])).toEqual(Array(5).fill([403, 'no-store']))
      expect(db.calls).toEqual([])
    }
  })

  it('is told apart from the site’s own pages, which send their header with every call and are served', async () => {
    const db = database()
    const sent: (string | null)[] = []
    vi.stubGlobal('fetch', async (path: string, init: RequestInit = {}) => {
      const request = forced(path, init.headers as Record<string, string>, { method: init.method, body: init.body })
      sent.push(request.headers.get(OWN_PAGE))
      if (path.startsWith('/api/search')) return handle(request, env(), db.fetcher)
      if (path.startsWith('/api/artist')) return artistPage(request, env(), db.fetcher)
      return init.method === 'POST' ? removal(request, env(), db.fetcher) : claimPage(request, env(), db.fetcher)
    })
    try {
      await search('q=own+pages')
      await getArtist('own-pages')
      await similarArtists('own-pages')
      await claim('own-pages')
      await removalCode('own-pages', 'FAKE-TICKET')
      await requestRemoval('own-pages', 'it is me', 'FAKE-TICKET')
      expect(sent).toEqual(Array(6).fill('1'))
      expect(db.called('client_wait').map((call) => call.args.p_kind)).toEqual(['search', 'profile', 'similar', 'claim', 'removal', 'removal'])
    } finally {
      vi.unstubAllGlobals()
    }
  })
})

describe('GET /api/artist', () => {
  it('counts a profile, then asks site_artist for it with the token: the card, or null when the artist is not listed', async () => {
    const profile = { slug: 'someone', name: 'Someone', works: [] }
    const db = database(Response.json([profile]), Response.json([]))
    const shown = await artistPage(from('203.0.113.7', '/api/artist?slug=someone'), env(), db.fetcher)
    const gone = await artistPage(from('203.0.113.7', '/api/artist?slug=nobody'), env(), db.fetcher)

    expect([await shown.json(), await gone.json()]).toEqual([profile, null])
    expect(shown.headers.get('cache-control')).toBe('no-store')
    expect(db.calls.map((call) => [call.name, call.args])).toEqual([
      ['client_wait', { p_token: TOKEN, p_kind: 'profile', p_client: '203.0.113.7', p_network: '203.0.113.0/24' }],
      ['site_artist', { p_token: TOKEN, p_slug: 'someone' }],
      ['client_wait', { p_token: TOKEN, p_kind: 'profile', p_client: '203.0.113.7', p_network: '203.0.113.0/24' }],
      ['site_artist', { p_token: TOKEN, p_slug: 'nobody' }],
    ])
    expect(db.calls.every((call) => JSON.stringify(call.init.headers) === JSON.stringify(HEADERS))).toBe(true)
  })

  it('counts the similar artists as a page, then asks site_similar for them', async () => {
    const db = database(Response.json(CARDS))
    const response = await artistPage(from('203.0.113.7', '/api/artist?slug=someone&similar=1'), env(), db.fetcher)

    expect(await response.json()).toEqual(CARDS)
    expect(db.calls.map((call) => [call.name, call.args.p_kind ?? call.args.p_slug])).toEqual([
      ['client_wait', 'similar'],
      ['site_similar', 'someone'],
    ])
  })

  it('past the limit: a 429 that says how long, and nothing asked; no slug: nothing counted', async () => {
    const db = database()
    db.limit = 0
    const over = await artistPage(from('203.0.113.7', '/api/artist?slug=someone'), env(), db.fetcher)
    expect([over.status, over.headers.get('retry-after')]).toEqual([429, '120'])
    expect(db.called('site_artist')).toEqual([])

    const blank = database()
    expect((await artistPage(from('203.0.113.7', '/api/artist?slug='), env(), blank.fetcher)).status).toBe(400)
    expect(blank.calls).toEqual([])
  })

  it('cuts the slug to 80 characters, the longest there is', async () => {
    const db = database(Response.json([]))
    await artistPage(from('203.0.113.7', `/api/artist?slug=${'s'.repeat(500)}`), env(), db.fetcher)
    expect(db.called('site_artist')[0].args.p_slug).toBe('s'.repeat(80))
  })
})

const post = (body: unknown, address = '203.0.113.7', type = 'application/json') =>
  new Request('https://artist-search.test/api/removal', {
    method: 'POST',
    headers: { 'content-type': type, 'cf-connecting-ip': address, ...PAGE_HEADER },
    body: JSON.stringify(body),
  })

describe('POST /api/removal', () => {
  it('counts the visitor, then gives the artist their code, which files their request, with the page’s ticket and network', async () => {
    const db = database(Response.json('artist-search-removal-0123456789abcdef0123456789abcdef'))
    const response = await removal(post({ slug: 'someone', ticket: 'FAKE-TICKET', proof: true }), env(), db.fetcher)

    expect(await response.json()).toEqual({ code: 'artist-search-removal-0123456789abcdef0123456789abcdef' })
    expect(db.calls.map((call) => [call.name, call.args])).toEqual([
      ['client_wait', { p_token: TOKEN, p_kind: 'removal', p_client: '203.0.113.7', p_network: '203.0.113.0/24' }],
      ['site_removal_code', { p_token: TOKEN, p_slug: 'someone', p_ticket: 'FAKE-TICKET', p_network: '203.0.113.0/24' }],
    ])
    expect(db.calls.every((call) => (call.init.headers as Record<string, string>).apikey === KEY)).toBe(true)
  })

  it('files a request without proof, with its note trimmed and cut to 300 characters, and the network it came from', async () => {
    // The note is kept in its network's place (request_removal): a stranger elsewhere cannot put the artist's out.
    const db = database(Response.json(true))
    const response = await removal(post({ slug: 'someone', ticket: 'FAKE-TICKET', note: `  ${'n'.repeat(400)} ` }), env(), db.fetcher)

    expect(await response.json()).toEqual({ ok: true })
    expect(db.called('site_request_removal')[0].args).toEqual({
      p_token: TOKEN,
      p_slug: 'someone',
      p_note: 'n'.repeat(300),
      p_ticket: 'FAKE-TICKET',
      p_network: '203.0.113.0/24',
    })
  })

  it('sends a ticket only as the page had it: a text no longer than a ticket can be, or none', async () => {
    for (const ticket of [undefined, 7, ['FAKE-TICKET'], 'x'.repeat(4097)]) {
      const db = database(Response.json('code'), Response.json(true))
      await removal(post({ slug: 'someone', ticket, proof: true }), env(), db.fetcher)
      await removal(post({ slug: 'someone', ticket }), env(), db.fetcher)
      expect([ticket, db.called('site_removal_code')[0].args.p_ticket, db.called('site_request_removal')[0].args.p_ticket]).toEqual([ticket, null, null])
    }
  })

  it('gives a code only when proof is asked for with true: anything else is a request without proof', async () => {
    for (const proof of ['yes', 1, 'true', {}, [true]]) {
      const db = database(Response.json(true))
      expect(await (await removal(post({ slug: 'someone', proof }), env(), db.fetcher)).json()).toEqual({ ok: true })
      expect([proof, db.calls.map((call) => call.name)]).toEqual([proof, ['client_wait', 'site_request_removal']])
    }
  })

  it('cuts the slug to 80 characters, the longest there is', async () => {
    const db = database(Response.json('code'), Response.json(true))
    await removal(post({ slug: 's'.repeat(500), proof: true }), env(), db.fetcher)
    await removal(post({ slug: 's'.repeat(500) }), env(), db.fetcher)
    expect([...db.called('site_removal_code'), ...db.called('site_request_removal')].map((call) => call.args.p_slug)).toEqual([
      's'.repeat(80),
      's'.repeat(80),
    ])
  })

  it('past the visitor’s limit: a 429 that says how long, and nothing filed', async () => {
    const db = database()
    db.limit = 2
    const statuses = []
    for (let n = 0; n < 4; n++) statuses.push((await removal(post({ slug: `someone-${n}`, proof: true }), env(), db.fetcher)).status)

    expect(statuses).toEqual([200, 200, 429, 429])
    expect(db.called('site_removal_code')).toHaveLength(2)
  })

  it('asks the database once more when filing fails, and only once more, and counts the visitor once', async () => {
    const down = () => new Response('{"code":"57014"}', { status: 500 })
    for (const [body, name] of [
      [{ slug: 'someone', ticket: 'FAKE-TICKET', proof: true }, 'site_removal_code'],
      [{ slug: 'someone', ticket: 'FAKE-TICKET', note: 'it is me' }, 'site_request_removal'],
    ] as const) {
      const db = database(down(), down(), down(), down())
      const response = await removal(post(body), env(), db.fetcher)
      expect([response.status, await response.json()]).toEqual([502, { error: 'That did not go through.' }])
      expect(db.calls.map((call) => call.name)).toEqual(['client_wait', name, name])
    }
  })

  it('reads only a JSON body that names a profile, so another site’s form cannot post here', async () => {
    for (const request of [post({ slug: 'someone', proof: true }, '203.0.113.7', 'text/plain'), post({ proof: true }), post([1, 2])]) {
      const db = database()
      expect((await removal(request, env(), db.fetcher)).status).toBe(400)
      expect(db.calls).toEqual([])
    }
  })

  it('is what the page calls, once each time, and a 429 tells the page how long to wait', async () => {
    const asked = vi.fn(async (_url: string, init: RequestInit) =>
      JSON.parse(String(init.body)).proof
        ? Response.json({ error: 'wait' }, { status: 429, headers: { 'retry-after': '7200' } })
        : Response.json({ ok: true }),
    )
    vi.stubGlobal('fetch', asked)
    try {
      const error = await removalCode('someone', 'FAKE-TICKET').catch((reason: unknown) => reason)
      expect(error).toBeInstanceOf(Busy)
      expect((error as Busy).seconds).toBe(7200)
      expect(await requestRemoval('someone', '  it is me  ', 'FAKE-TICKET')).toEqual({ ok: true })
      expect(asked.mock.calls.map(([url, init]) => [url, init.method, JSON.parse(String(init.body))])).toEqual([
        ['/api/removal', 'POST', { slug: 'someone', ticket: 'FAKE-TICKET', proof: true }],
        ['/api/removal', 'POST', { slug: 'someone', ticket: 'FAKE-TICKET', note: 'it is me' }],
      ])
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('files the request again at every press of “Get my code”, which starts its checks over', async () => {
    const asked = vi.fn(async () => Response.json({ code: 'artist-search-removal-0123456789abcdef0123456789abcdef' }))
    vi.stubGlobal('fetch', asked)
    try {
      await removalCode('pressed-twice', 'FAKE-TICKET')
      await removalCode('pressed-twice', 'FAKE-TICKET') // the artist comes back to the page, in the same tab, and presses again
      expect(asked).toHaveBeenCalledTimes(2)
    } finally {
      vi.unstubAllGlobals()
    }
  })
})

describe('GET /api/removal: the claim-or-remove page', () => {
  it('counts the visitor as asking about a removal, then asks site_claim with the token: the artist, or null', async () => {
    const shown = { slug: 'someone', name: 'Someone', site_url: null, deviantart_url: 'https://www.deviantart.com/someone', deviantart_proof: true }
    const db = database(Response.json([shown]), Response.json([]))
    const answers = [
      await claimPage(from('203.0.113.7', '/api/removal?slug=someone'), env(), db.fetcher),
      await claimPage(from('203.0.113.7', `/api/removal?slug=${'s'.repeat(500)}`), env(), db.fetcher),
    ]
    expect([await answers[0].json(), await answers[1].json()]).toEqual([shown, null])
    expect(answers[0].headers.get('cache-control')).toBe('no-store')
    expect(db.calls.map((call) => [call.name, call.args])).toEqual([
      ['client_wait', { p_token: TOKEN, p_kind: 'claim', p_client: '203.0.113.7', p_network: '203.0.113.0/24' }],
      ['site_claim', { p_token: TOKEN, p_slug: 'someone' }],
      ['client_wait', { p_token: TOKEN, p_kind: 'claim', p_client: '203.0.113.7', p_network: '203.0.113.0/24' }],
      ['site_claim', { p_token: TOKEN, p_slug: 's'.repeat(80) }],
    ])

    const blank = database()
    expect((await claimPage(from('203.0.113.7', '/api/removal?slug='), env(), blank.fetcher)).status).toBe(400)
    expect(blank.calls).toEqual([])
    const over = Object.assign(database(), { limit: 0 })
    const refused = await claimPage(from('203.0.113.7', '/api/removal?slug=someone'), env(), over.fetcher)
    expect([refused.status, refused.headers.get('retry-after'), over.called('site_claim')]).toEqual([429, '120', []])
  })

  it('says only that it did not load when the database fails, never what the database said', async () => {
    const said = 'FAKE: relation public.artists, row (zz-fake-someone, FAKE-HANDLE)'
    for (const answer of [new Response(said, { status: 500 }), new Error(said)]) {
      const failed = await claimPage(from('203.0.113.7', '/api/removal?slug=someone'), env(), database(answer, answer).fetcher)
      expect([failed.status, await failed.json()]).toEqual([502, { error: 'That did not load.' }])
    }
  })

  it('loads, and gives the code, while the day’s reads are used up for everyone', async () => {
    // The critic's case: one person spends the shared read total. The page loads the artist through its own count.
    const counted: string[] = []
    const fetcher = (async (url: string, init: RequestInit) => {
      const name = url.replace('https://db.test/rest/v1/rpc/', '')
      const args = JSON.parse(String(init.body))
      if (name === 'client_wait') return (counted.push(args.p_kind), Response.json(['claim', 'removal'].includes(args.p_kind) ? 0 : 87))
      if (name === 'site_claim') return Response.json([{ name: 'Spent', site_url: null, deviantart_url: 'x', deviantart_proof: true }])
      return Response.json(name === 'site_removal_code' ? 'artist-search-removal-0123456789abcdef0123456789abcdef' : [])
    }) as unknown as typeof fetch
    vi.stubGlobal('fetch', async (path: string, init?: RequestInit) =>
      path.startsWith('/api/artist')
        ? artistPage(from('203.0.113.7', path), env(), fetcher)
        : init?.method === 'POST'
          ? removal(post(JSON.parse(String(init.body))), env(), fetcher)
          : claimPage(from('203.0.113.7', path), env(), fetcher),
    )
    try {
      expect(await getArtist('spent-reads').catch((reason: unknown) => reason)).toBeInstanceOf(Busy)
      expect(await claim('spent-reads')).toMatchObject({ name: 'Spent' })
      expect(await removalCode('spent-reads', 'FAKE-TICKET')).toMatch(/^artist-search-removal-/)
      expect(counted).toEqual(['profile', 'claim', 'removal'])
    } finally {
      vi.unstubAllGlobals()
    }
  })
})

describe('the pages’ loader and the database call', () => {
  it('keeps no failure, so “Try again” asks again', async () => {
    let calls = 0
    vi.stubGlobal('fetch', async () => (++calls <= 2 ? new Response('down', { status: 500 }) : Response.json({ slug: 'flaky' })))
    try {
      await expect(getArtist('flaky')).rejects.toThrow() // asked twice, failed twice
      expect(await getArtist('flaky')).toEqual({ slug: 'flaky' })
      expect(calls).toBe(3)
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('gives up on the database after a while, rather than wait for ever', async () => {
    const seen: (AbortSignal | null | undefined)[] = []
    const fetcher = (async (_url: string, init: RequestInit) => (seen.push(init.signal), Response.json(0))) as unknown as typeof fetch
    await call('https://db.test', KEY, 'client_wait', {}, fetcher)
    expect(seen).toHaveLength(1)
    expect(seen[0]).toBeInstanceOf(AbortSignal)
    expect(seen[0]?.aborted).toBe(false)
  })
})

describe('an artist card', () => {
  it('shows the first four works it is given, in the order given', () => {
    // similar_artists puts the works closest to the seed's first: the card must not sort or pick again.
    const works = [7, 0, 10, 3, 1].map((n) => ({
      page_url: `https://www.deviantart.com/someone/art/work-${n}`,
      image_url: `https://images-wixmp-x.wixmp.com/f/${n}.jpg`,
      title: `Work ${10 - n}`,
      source: 'deviantart',
      width: 300 - n,
      height: 200 + n,
    }))
    const artist = { slug: 'someone', name: 'Someone', tagline: null, profile: null, specialties: null, mediums: null, open_for_work: null, works }
    const html = renderToStaticMarkup(createElement(MemoryRouter, null, createElement(Cards, { artists: [artist] })))

    expect([...html.matchAll(/class="thumb" href="[^"]*work-(\d+)"/g)].map((found) => Number(found[1]))).toEqual([7, 0, 10, 3])
  })

  it('shows only the works it is given when there are fewer than four', () => {
    // A similar artist comes with their works close enough to the seed's and no others: two or three make a card.
    const works = [5, 2].map((n) => ({
      page_url: `https://www.deviantart.com/someone/art/work-${n}`,
      image_url: `https://images-wixmp-x.wixmp.com/f/${n}.jpg`,
      title: `Work ${n}`,
      source: 'deviantart',
      width: null,
      height: null,
    }))
    const artist = { slug: 'someone', name: 'Someone', tagline: null, profile: null, specialties: null, mediums: null, open_for_work: null, works }
    const html = renderToStaticMarkup(createElement(MemoryRouter, null, createElement(Cards, { artists: [artist] })))

    expect([...html.matchAll(/class="thumb" href="[^"]*work-(\d+)"/g)].map((found) => Number(found[1]))).toEqual([5, 2])
    expect(html.match(/class="mosaic"/g)).toHaveLength(1)
  })
})

describe('the claim-or-remove page', () => {
  const places = (deviantart_proof: boolean, site_url: string | null, copyRow = (text: string) => text) => {
    const claim = { slug: 'someone', name: 'Someone', site_url, deviantart_url: 'https://www.deviantart.com/someone', deviantart_proof, ticket: 'FAKE-TICKET' }
    return renderToStaticMarkup(createElement(ProofPlaces, { claim, code: 'CODE', copyRow }))
  }

  it('tells the artist the tag and the file the checks read, word for word', () => {
    // pipeline/removals.py reads <meta name="artist-search-removal"> in the head, and /.well-known/artist-search-removal.txt
    // (crawl.read_proof). A property= tag or the file anywhere else is never found, and the artist waits for nothing.
    const rows: string[] = []
    places(true, 'https://someone.art/', (text) => (rows.push(text), text))
    expect(rows).toEqual(['CODE', '<meta name="artist-search-removal" content="CODE">', 'https://someone.art/.well-known/artist-search-removal.txt'])
  })

  it('promises only what the checks keep: usually within a few hours, at most within a day', () => {
    // Hourly for a day, every six hours to two weeks, a run every 15 minutes: tests/test_removals.py holds them to it.
    expect(places(true, 'https://someone.art/')).toContain('Once the code is up, we usually find it within a few hours, and at most within a day.')
  })

  it('offers DeviantArt only for a profile whose account was kept, the site when there is one, and says which', () => {
    expect([places(true, 'https://someone.art/'), places(true, null)].map((page) => page.includes('About text or the tagline'))).toEqual([true, true])
    expect(places(true, 'https://someone.art/')).toContain('Or on someone.art')

    const siteOnly = places(false, 'https://someone.art/')
    expect(siteOnly).not.toContain('About text')
    expect(siteOnly).toContain('Not on DeviantArt for this profile, only on your site')
    expect(siteOnly).toContain('On someone.art')

    const neither = places(false, null)
    expect(neither).toContain('There is no place we can check a code for this profile')
    expect(neither).toContain('send a request below')
    expect(neither).not.toContain('Your code')
  })
})

describe('the built site', () => {
  const web = fileURLToPath(new URL('..', import.meta.url))
  const envFile = join(web, '..', '.env')

  // `npm test` builds first (pretest). Without a .env there is nothing that could leak.
  it.skipIf(!existsSync(envFile))('holds nothing from .env: no database key or address, no token', () => {
    const values = parseEnv(readFileSync(envFile, 'utf8'))
    const files = readdirSync(join(web, 'dist'), { recursive: true, withFileTypes: true }).filter((entry) => entry.isFile())
    const built = files.map((entry) => readFileSync(join(entry.parentPath, entry.name), 'latin1')).join('\n')

    // Names only, here and in any failure message: a value is never printed. The same rule as scripts/secret_scan.py
    // for a secret (16 characters or more), and the database's address and publishable key besides.
    const leaked = Object.entries(values)
      .filter(([name, value]) => (value ?? '').length >= 16 || (name.startsWith('VITE_') && value))
      .filter(([, value]) => built.includes(value!))
      .map(([name]) => name)
    expect(leaked).toEqual([])

    // The check above means something only if these are the pages, which ask the server functions for everything.
    expect(files.length).toBeGreaterThan(2)
    expect(built).toContain('/api/artist?slug=')
    expect(built).not.toMatch(/VITE_SUPABASE|supabase\.co|SITE_TOKEN|SUPABASE_SECRET|CLOUDFLARE_API_TOKEN|GH_TOKEN|DEVIANT_ART_SECRET/)
  })
})

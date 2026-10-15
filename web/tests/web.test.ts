// The search and removal functions' requests, their limits, fallback and retry; a card's works; and what the built
// site may contain. No network: Workers AI and the database are stand-ins. The database's own count (client_wait in
// sql/003_limits.sql) is run for real by tests/test_db.py; here a stand-in counts the same way, one call at a time.
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseEnv } from 'node:util'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { handle as removal } from '../functions/api/removal'
import { client, handle, MAX_QUERY, onRequestGet, PAGE, TOO_MANY, type Env } from '../functions/api/search'
import { Busy, removalCode, requestRemoval, search } from '../src/api'
import { Cards } from '../src/ui'

const VECTOR = Array.from({ length: 1024 }, (_, i) => i / 1024)
const KEY = 'FAKE-SERVER-KEY'
const embeds = () => vi.fn(async () => ({ shape: [1, 1024], data: [VECTOR] }))
const env = (run: Env['AI']['run'] = embeds()): Env => ({ AI: { run }, VITE_SUPABASE_URL: 'https://db.test', SUPABASE_SERVER_KEY: KEY })
const CARDS = [{ slug: 'someone', name: 'Someone' }]

/**
 * A database. client_wait counts each call by kind and client, as the real one does, and says "wait 120 seconds"
 * past `limit`. Every other function gives these answers in turn, then CARDS; an Error stands for a dropped connection.
 */
function database(...answers: (Response | Error)[]) {
  const calls: { name: string; init: RequestInit; args: Record<string, unknown> }[] = []
  const db = {
    limit: Infinity,
    counts: new Map<string, number>(),
    calls,
    called: (name: string) => calls.filter((call) => call.name === name),
    sent: (n = 0) => db.called('search_artists')[n].args,
    fetcher: (async (url: string, init: RequestInit) => {
      const name = url.replace('https://db.test/rest/v1/rpc/', '')
      const args = JSON.parse(String(init.body))
      calls.push({ name, init, args })
      if (name === 'client_wait') {
        const key = `${args.p_kind} ${args.p_client}`
        db.counts.set(key, (db.counts.get(key) ?? 0) + 1)
        return Response.json(db.counts.get(key)! > db.limit ? 120 : 0)
      }
      const answer = answers.shift() ?? Response.json(CARDS)
      if (answer instanceof Error) throw answer
      return answer
    }) as typeof fetch,
  }
  return db
}
const ask = (query: string, run: Env['AI']['run'], db = database()) =>
  handle(new Request(`https://artist-search.test/api/search?${query}`), env(run), db.fetcher)

describe('GET /api/search', () => {
  it('counts the visitor, then asks search_artists for the query, its embedding and the filters, with the server key', async () => {
    const run = embeds()
    const db = database()
    const response = await ask('q=dark+fantasy+creature+designer&specialty=creature&specialty=character&medium=digital+painting&open=1&offset=24', run, db)

    expect(run).toHaveBeenCalledExactlyOnceWith('@cf/baai/bge-m3', { text: ['dark fantasy creature designer'] })
    expect(db.calls.map((call) => call.name)).toEqual(['client_wait', 'search_artists'])
    expect(db.calls[0].args).toEqual({ p_kind: 'search', p_client: '' })
    for (const call of db.calls) {
      expect(call.init.method).toBe('POST')
      expect(call.init.headers).toEqual({ apikey: KEY, 'content-type': 'application/json' })
    }
    expect(db.sent()).toEqual({
      q: 'dark fantasy creature designer',
      q_embedding: VECTOR,
      p_mediums: ['digital painting'],
      p_specialties: ['creature', 'character'],
      p_open_only: true,
      p_limit: PAGE,
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

  it('asks the database once more when it fails, and only once more', async () => {
    const stalled = () => new Response('{"code":"57014"}', { status: 500 })

    const recovers = database(stalled(), Response.json(CARDS))
    expect((await ask('q=mechs', embeds(), recovers)).status).toBe(200)
    expect(recovers.called('search_artists')).toHaveLength(2)
    expect(recovers.sent(1)).toEqual(recovers.sent(0))

    const dropped = database(new TypeError('fetch failed'), Response.json(CARDS))
    expect(await (await ask('q=mechs', embeds(), dropped)).json()).toMatchObject({ artists: CARDS })
    expect(dropped.called('search_artists')).toHaveLength(2)

    const down = database(stalled(), stalled(), Response.json(CARDS))
    const response = await ask('q=mechs', embeds(), down)
    expect(response.status).toBe(502)
    expect(await response.json()).toEqual({ error: 'The search is not answering right now.' })
    expect(down.called('search_artists')).toHaveLength(2)
  })
})

const from = (address: string, origin = 'https://artist-search.test') =>
  new Request(`${origin}/api/search?q=mechs`, { headers: { 'cf-connecting-ip': address } })

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
    expect(db.called('search_artists')).toHaveLength(3)

    expect((await handle(from('198.51.100.1'), env(run), db.fetcher)).status).toBe(200)
  })

  it('lets through only what the database lets through: 600 searches at once from one address', async () => {
    const run = embeds()
    const db = database()
    db.limit = 60
    const answers = await Promise.all(Array.from({ length: 600 }, () => handle(from('203.0.113.7'), env(run), db.fetcher)))

    expect(answers.filter((answer) => answer.status === 200)).toHaveLength(60)
    expect(answers.filter((answer) => answer.status === 429)).toHaveLength(540)
    expect(db.called('client_wait')).toHaveLength(600) // every one counted
    expect(run).toHaveBeenCalledTimes(60)
    expect(db.called('search_artists')).toHaveLength(60)
  })

  it('lets nothing through when the count fails or is not a number', async () => {
    // The count answers badly; search_artists would answer well.
    const counting = (wait: () => Response) =>
      (async (url: string) => (url.endsWith('client_wait') ? wait() : Response.json(CARDS))) as unknown as typeof fetch
    const run = embeds()
    expect((await handle(from('203.0.113.7'), env(run), counting(() => new Response('{}', { status: 500 })))).status).toBe(502)

    const answer = await handle(from('203.0.113.7'), env(run), counting(() => Response.json('soon')))
    expect(answer.status).toBe(429)
    expect(answer.headers.get('retry-after')).toBe('60')
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

  it('counts an IPv6 address by its /56, however it is written', () => {
    const key = (address: string) => client(from(address))
    expect(key('2001:db8:aa:bb::1')).toBe('2001:db8:aa:0::/56')
    expect(key('2001:db8:aa:bbcc:ffff:1:2:3')).toBe('2001:db8:aa:bb00::/56')
    expect(key('2001:0db8:00aa:bbff::9')).toBe('2001:db8:aa:bb00::/56')
    expect(key('2001:db8::1')).toBe('2001:db8:0:0::/56')
    expect(key('1::2:3:4:5:6:7')).toBe('1:0:2:0::/56')
    expect(key('1:2:3:4567::')).toBe('1:2:3:4500::/56')
    // A home's /56 is 256 /64s: one client. The next /56 is another.
    expect(new Set(Array.from({ length: 256 }, (_, n) => key(`2001:db8:aa:${(0x1200 + n).toString(16)}::1`))).size).toBe(1)
    expect(key('2001:db8:aa:1300::1')).not.toBe(key('2001:db8:aa:12ff::1'))
    expect(key('203.0.113.7')).toBe('203.0.113.7')
    // Only the address Cloudflare saw: headers a client can set change nothing.
    const spoofed = { 'cf-connecting-ip': '203.0.113.7', 'x-forwarded-for': '1.2.3.4', 'x-real-ip': '5.6.7.8' }
    expect(client(new Request('https://x.test/api/search', { headers: spoofed }))).toBe('203.0.113.7')
  })

  it('never puts the server key in an answer', async () => {
    const answers = [
      await ask('q=mechs', embeds()),
      await ask('q=mechs', embeds(), database(new Error(KEY), new Error(KEY))),
      await handle(from('203.0.113.7'), env(), Object.assign(database(), { limit: 0 }).fetcher),
      await removal(post({ slug: 'someone', proof: true }), env(), database(new Response(KEY, { status: 500 }), new Error(KEY)).fetcher),
    ]
    for (const answer of answers) expect(await answer.text()).not.toContain(KEY)
  })

  it('is not asked again by the page, which is told how long to wait', async () => {
    const asked = vi.fn(async () => Response.json({ error: TOO_MANY }, { status: 429, headers: { 'retry-after': '150' } }))
    vi.stubGlobal('fetch', asked)
    try {
      const error = await search('q=limited').catch((reason: unknown) => reason)
      expect(error).toBeInstanceOf(Busy)
      expect((error as Busy).seconds).toBe(150)
      expect(asked).toHaveBeenCalledTimes(1)
    } finally {
      vi.unstubAllGlobals()
    }
  })
})

const post = (body: unknown, type = 'application/json') =>
  new Request('https://artist-search.test/api/removal', {
    method: 'POST',
    headers: { 'content-type': type, 'cf-connecting-ip': '203.0.113.7' },
    body: JSON.stringify(body),
  })

describe('POST /api/removal', () => {
  it('counts the visitor, then gives the artist their code, which files their request', async () => {
    const db = database(Response.json('artist-search-removal-0123456789abcdef0123456789abcdef'))
    const response = await removal(post({ slug: 'someone', proof: true }), env(), db.fetcher)

    expect(await response.json()).toEqual({ code: 'artist-search-removal-0123456789abcdef0123456789abcdef' })
    expect(db.calls.map((call) => [call.name, call.args])).toEqual([
      ['client_wait', { p_kind: 'removal', p_client: '203.0.113.7' }],
      ['start_removal_proof', { p_slug: 'someone' }],
    ])
    expect(db.calls.every((call) => (call.init.headers as Record<string, string>).apikey === KEY)).toBe(true)
  })

  it('files a request without proof, with its note trimmed and cut to 300 characters', async () => {
    const db = database(Response.json(true))
    const response = await removal(post({ slug: 'someone', note: `  ${'n'.repeat(400)} ` }), env(), db.fetcher)

    expect(await response.json()).toEqual({ ok: true })
    expect(db.called('request_removal')[0].args).toEqual({ p_slug: 'someone', p_note: 'n'.repeat(300) })
  })

  it('past the visitor’s limit: a 429 that says how long, and nothing filed', async () => {
    const db = database()
    db.limit = 2
    const statuses = []
    for (let n = 0; n < 4; n++) statuses.push((await removal(post({ slug: `someone-${n}`, proof: true }), env(), db.fetcher)).status)

    expect(statuses).toEqual([200, 200, 429, 429])
    expect(db.called('start_removal_proof')).toHaveLength(2)
  })

  it('reads only a JSON body that names a profile, so another site’s form cannot post here', async () => {
    for (const request of [post({ slug: 'someone', proof: true }, 'text/plain'), post({ proof: true }), post([1, 2])]) {
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
      const error = await removalCode('someone').catch((reason: unknown) => reason)
      expect(error).toBeInstanceOf(Busy)
      expect((error as Busy).seconds).toBe(7200)
      expect(await requestRemoval('someone', '  it is me  ')).toEqual({ ok: true })
      expect(asked.mock.calls.map(([url, init]) => [url, init.method, JSON.parse(String(init.body))])).toEqual([
        ['/api/removal', 'POST', { slug: 'someone', proof: true }],
        ['/api/removal', 'POST', { slug: 'someone', note: 'it is me' }],
      ])
    } finally {
      vi.unstubAllGlobals()
    }
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
})

describe('the built site', () => {
  const web = fileURLToPath(new URL('..', import.meta.url))
  const envFile = join(web, '..', '.env')
  const PUBLIC = ['VITE_SUPABASE_URL', 'VITE_SUPABASE_PUBLISHABLE_KEY']

  // `npm test` builds first (pretest). Without a .env there is nothing that could leak.
  it.skipIf(!existsSync(envFile))('holds nothing from .env but the two VITE_ values', () => {
    const values = parseEnv(readFileSync(envFile, 'utf8'))
    const files = readdirSync(join(web, 'dist'), { recursive: true, withFileTypes: true }).filter((entry) => entry.isFile())
    const built = files.map((entry) => readFileSync(join(entry.parentPath, entry.name), 'latin1')).join('\n')
    const open = PUBLIC.map((name) => values[name] ?? '')

    // Names only, here and in any failure message: a value is never printed.
    // The same rule as scripts/secret_scan.py: a value of 16 characters or more is a secret. One that is
    // part of a public value (the project's ref, inside its address) is public already.
    const leaked = Object.entries(values)
      .filter(([name, value]) => !PUBLIC.includes(name) && (value ?? '').length >= 16)
      .filter(([, value]) => !open.some((known) => known.includes(value!)) && built.includes(value!))
      .map(([name]) => name)
    expect(leaked).toEqual([])

    // The check above means something only if these are the files the values would be in.
    expect(files.length).toBeGreaterThan(2)
    expect(PUBLIC.filter((name) => built.includes(values[name] ?? '\0'))).toEqual(PUBLIC)
    expect(built).not.toMatch(/SUPABASE_SECRET|SUPABASE_SERVER_KEY|CLOUDFLARE_API_TOKEN|GH_TOKEN|DEVIANT_ART_SECRET/)
  })
})

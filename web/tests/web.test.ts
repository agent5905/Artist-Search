// The search function's request, its fallback and its retry; a card's works; and what the built site may contain.
// No network: Workers AI and the database are stand-ins.
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseEnv } from 'node:util'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { client, handle, LIMIT, MAX_QUERY, PAGE, TOO_MANY, wait, WINDOW, type Env } from '../functions/api/search'
import { Busy, search } from '../src/api'
import { Cards } from '../src/ui'

const VECTOR = Array.from({ length: 1024 }, (_, i) => i / 1024)
const embeds = () => vi.fn(async () => ({ shape: [1, 1024], data: [VECTOR] }))
const env = (run: Env['AI']['run']): Env => ({
  AI: { run },
  VITE_SUPABASE_URL: 'https://db.test',
  VITE_SUPABASE_PUBLISHABLE_KEY: 'publishable',
})
const CARDS = [{ slug: 'someone', name: 'Someone' }]

/** A database that gives these answers in turn; an Error stands for a dropped connection. */
function database(...answers: (Response | Error)[]) {
  const calls: { url: string; init: RequestInit }[] = []
  const fetcher = (async (url: string, init: RequestInit) => {
    calls.push({ url, init })
    const answer = answers.shift() ?? Response.json(CARDS)
    if (answer instanceof Error) throw answer
    return answer
  }) as typeof fetch
  return { fetcher, calls, sent: (call = 0) => JSON.parse(String(calls[call].init.body)) }
}
const ask = (query: string, run: Env['AI']['run'], db = database()) =>
  handle(new Request(`https://artist-search.test/api/search?${query}`), env(run), db.fetcher)

describe('GET /api/search', () => {
  it('asks search_artists for the query, its embedding and the filters', async () => {
    const run = embeds()
    const db = database()
    const response = await ask('q=dark+fantasy+creature+designer&specialty=creature&specialty=character&medium=digital+painting&open=1&offset=24', run, db)

    expect(run).toHaveBeenCalledExactlyOnceWith('@cf/baai/bge-m3', { text: ['dark fantasy creature designer'] })
    expect(db.calls).toHaveLength(1)
    expect(db.calls[0].url).toBe('https://db.test/rest/v1/rpc/search_artists')
    expect(db.calls[0].init.method).toBe('POST')
    expect(db.calls[0].init.headers).toEqual({ apikey: 'publishable', 'content-type': 'application/json' })
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
    expect(recovers.calls).toHaveLength(2)
    expect(recovers.sent(1)).toEqual(recovers.sent(0))

    const dropped = database(new TypeError('fetch failed'), Response.json(CARDS))
    expect(await (await ask('q=mechs', embeds(), dropped)).json()).toMatchObject({ artists: CARDS })
    expect(dropped.calls).toHaveLength(2)

    const down = database(stalled(), stalled(), Response.json(CARDS))
    const response = await ask('q=mechs', embeds(), down)
    expect(response.status).toBe(502)
    expect(await response.json()).toEqual({ error: 'The search is not answering right now.' })
    expect(down.calls).toHaveLength(2)
  })
})

/** One Cloudflare location's cache, as the limit uses it: what was put, by key. */
function memoryCache() {
  const kept = new Map<string, string>()
  const cache = {
    match: async (key: string) => (kept.has(key) ? new Response(kept.get(key)) : undefined),
    put: async (key: string, response: Response) => void kept.set(key, await response.text()),
  }
  return cache as unknown as Cache
}
const from = (address: string) =>
  new Request('https://artist-search.test/api/search?q=mechs', { headers: { 'cf-connecting-ip': address } })

describe('the limit per client', () => {
  it('answers 429 past the limit, before Workers AI or the database is asked, and holds up nobody else', async () => {
    const cache = memoryCache()
    const run = embeds()
    const db = database()
    for (let n = 0; n < LIMIT; n++) expect((await handle(from('203.0.113.7'), env(run), db.fetcher, cache)).status).toBe(200)

    const over = await handle(from('203.0.113.7'), env(run), db.fetcher, cache)
    expect(over.status).toBe(429)
    expect(Number(over.headers.get('retry-after'))).toBeGreaterThan(0)
    expect(Number(over.headers.get('retry-after'))).toBeLessThanOrEqual(WINDOW)
    expect(over.headers.get('cache-control')).toBe('no-store')
    expect(await over.json()).toEqual({ error: TOO_MANY })
    expect(run).toHaveBeenCalledTimes(LIMIT)
    expect(db.calls).toHaveLength(LIMIT)

    expect((await handle(from('198.51.100.1'), env(run), db.fetcher, cache)).status).toBe(200)
  })

  it('lifts when the window ends, and says how long that is', async () => {
    const cache = memoryCache()
    const start = 1_800_000_000_000 - (1_800_000_000 % WINDOW) * 1000 // the first second of a window
    for (let n = 0; n < LIMIT; n++) expect(await wait(cache, 'https://x.test', 'someone', start)).toBe(0)
    expect(await wait(cache, 'https://x.test', 'someone', start + 1000)).toBe(WINDOW - 1)
    expect(await wait(cache, 'https://x.test', 'someone', start + (WINDOW - 1) * 1000)).toBe(1)
    expect(await wait(cache, 'https://x.test', 'someone', start + WINDOW * 1000)).toBe(0)
  })

  it('counts an IPv6 address by its /64, however it is written', () => {
    const key = (address: string) => client(from(address))
    expect(key('2001:db8:aa:bb::1')).toBe('2001:db8:aa:bb::/64')
    expect(key('2001:db8:aa:bb:ffff:1:2:3')).toBe('2001:db8:aa:bb::/64')
    expect(key('2001:0db8:00aa:00bb::9')).toBe('2001:db8:aa:bb::/64')
    expect(key('2001:db8::1')).toBe('2001:db8:0:0::/64')
    expect(key('1::2:3:4:5:6:7')).toBe('1:0:2:3::/64')
    expect(key('2001:db8:aa:bc::1')).not.toBe(key('2001:db8:aa:bb::1'))
    expect(key('203.0.113.7')).toBe('203.0.113.7')
  })

  it('lets the search through when the cache fails', async () => {
    const broken = { match: async () => Promise.reject(new Error('cache down')), put: async () => {} } as unknown as Cache
    expect((await handle(from('203.0.113.7'), env(embeds()), database().fetcher, broken)).status).toBe(200)
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
    expect(built).not.toMatch(/SUPABASE_SECRET|CLOUDFLARE_API_TOKEN|GH_TOKEN|DEVIANT_ART_SECRET/)
  })
})

// Rewrites src/facets.json: the specialties and mediums the public database holds right now, most used first.
// The search page builds its filters from that file, so they only ever offer values the data has.
// ponytail: a snapshot, taken by reading every public card (100 a call). Rerun after a load; a facets
// function in the database is the upgrade when that gets slow or stale.
// It runs on the owner's machine with the pipeline's secret key: the publishable key cannot search.
import { readFileSync, writeFileSync } from 'node:fs'
import { parseEnv } from 'node:util'

const env = parseEnv(readFileSync(new URL('../../.env', import.meta.url), 'utf8'))
const counts = { specialties: new Map(), mediums: new Map() }
let artists = 0
for (let offset = 0; offset <= 1000; offset += 100) {
  const response = await fetch(`${env.VITE_SUPABASE_URL}/rest/v1/rpc/search_artists`, {
    method: 'POST',
    headers: { apikey: env.SUPABASE_SECRET_KEY, 'content-type': 'application/json' },
    body: JSON.stringify({ q: '', p_limit: 100, p_offset: offset }),
  })
  if (!response.ok) throw new Error(`search_artists answered ${response.status}`)
  const cards = await response.json()
  for (const card of cards)
    for (const [facet, seen] of Object.entries(counts))
      for (const value of card[facet] ?? []) seen.set(value, (seen.get(value) ?? 0) + 1)
  artists += cards.length
  if (cards.length < 100) break
}
const byUse = (seen) => [...seen].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).map(([value]) => value)
const facets = { specialties: byUse(counts.specialties), mediums: byUse(counts.mediums) }
writeFileSync(new URL('../src/facets.json', import.meta.url), JSON.stringify(facets, null, 2) + '\n')
console.log(`${artists} artists: ${facets.specialties.length} specialties, ${facets.mediums.length} mediums`)

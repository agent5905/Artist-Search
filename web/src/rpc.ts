// One call to a public database function through the Supabase Data API.
// Shared by the browser (api.ts) and the Pages Function (functions/api/search.ts): no browser or Vite globals here.

/** Run it; if it fails, run it once more. A call can stall or fail while the owner loads data. */
export const twice = <T>(run: () => Promise<T>): Promise<T> => run().catch(run)

export function rpc<T>(url: string, key: string, name: string, args: object, fetcher: typeof fetch = fetch): Promise<T> {
  return twice(async () => {
    const response = await fetcher(`${url}/rest/v1/rpc/${name}`, {
      method: 'POST',
      headers: { apikey: key, 'content-type': 'application/json' },
      body: JSON.stringify(args),
      signal: AbortSignal.timeout(10_000),
    })
    if (!response.ok) throw new Error(`${name} answered ${response.status}`)
    return (await response.json()) as T
  })
}

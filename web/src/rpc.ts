// One call to a database function through the Supabase Data API, from the site's server functions (functions/api/).
// No browser or Vite globals here: the pages (api.ts) use `twice` too.

/** Run it; if it fails, run it once more. A call can stall or fail while the owner loads data. */
export const twice = <T>(run: () => Promise<T>): Promise<T> => run().catch(run)

/** One call, not asked again. The answer comes gzipped: what the limits are sized by (sql/003_limits.sql). */
export async function call<T>(url: string, key: string, name: string, args: object, fetcher: typeof fetch = fetch): Promise<T> {
  const response = await fetcher(`${url}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: { apikey: key, 'content-type': 'application/json', 'accept-encoding': 'gzip' },
    body: JSON.stringify(args),
    signal: AbortSignal.timeout(10_000),
  })
  if (!response.ok) throw new Error(`${name} answered ${response.status}`)
  return (await response.json()) as T
}

/** The same, asked once more if it fails. */
export const rpc = <T>(url: string, key: string, name: string, args: object, fetcher: typeof fetch = fetch): Promise<T> =>
  twice(() => call<T>(url, key, name, args, fetcher))

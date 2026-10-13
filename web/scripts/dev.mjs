// Local run. Nothing from .env is printed, and nothing is deployed.
//
//   node scripts/dev.mjs           Vite serves the pages on :5173 and proxies /api to wrangler on :8788
//   node scripts/dev.mjs --built   wrangler alone serves ./dist and the function on :8788, as Pages would
//
// Reads the repo's .env in memory and hands each process only what it needs:
//   * web/.dev.vars (git-ignored) gets the two browser-safe values, for the function;
//   * wrangler gets the Cloudflare account id and token in its environment, for the real Workers AI binding;
//   * Vite reads .env itself (envDir) and puts only VITE_ names in the bundle.
import { spawn } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { parseEnv } from 'node:util'

const web = new URL('../', import.meta.url)
const env = parseEnv(readFileSync(new URL('../.env', web), 'utf8'))

const forFunction = ['VITE_SUPABASE_URL', 'VITE_SUPABASE_PUBLISHABLE_KEY']
writeFileSync(new URL('.dev.vars', web), forFunction.map((name) => `${name}=${JSON.stringify(env[name] ?? '')}\n`).join(''))
mkdirSync(new URL('dist', web), { recursive: true }) // wrangler wants the directory even when Vite serves the pages

const run = (script, args, extra = {}) =>
  spawn(process.execPath, [script, ...args], { cwd: web, stdio: 'inherit', env: { ...process.env, ...extra } })

const children = [
  run('node_modules/wrangler/bin/wrangler.js', ['pages', 'dev', 'dist', '--port', '8788', '--show-interactive-dev-session=false'], {
    CLOUDFLARE_ACCOUNT_ID: env.CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN: env.CLOUDFLARE_API_TOKEN,
    WRANGLER_SEND_METRICS: 'false',
  }),
]
if (!process.argv.includes('--built')) children.push(run('node_modules/vite/bin/vite.js', ['--port', '5173', '--strictPort']))

const stop = () => children.forEach((child) => child.kill())
children.forEach((child) => child.on('exit', (code) => (stop(), process.exit(code ?? 0))))
process.on('SIGINT', stop)
process.on('SIGTERM', stop)

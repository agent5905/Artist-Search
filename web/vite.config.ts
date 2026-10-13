import { defineConfig } from 'vitest/config'

export default defineConfig({
  // The repo's .env is the only env file. Vite puts only names starting with VITE_ in the bundle.
  envDir: '..',
  // In development the pages come from Vite and /api from the Pages Function under wrangler (scripts/dev.mjs).
  server: { proxy: { '/api': 'http://127.0.0.1:8788' } },
  build: {
    rolldownOptions: {
      // The router marks its files "use client" for server rendering, which this site does not do.
      onwarn: (warning, warn) => warning.code !== 'MODULE_LEVEL_DIRECTIVE' && warn(warning),
    },
  },
  test: { include: ['tests/*.test.ts'] },
})

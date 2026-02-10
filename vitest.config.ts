import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        miniflare: {
          bindings: {
            R2INDEX_API_TOKEN: 'test-token',
            CACHE_MAX_AGE: '60',
          },
        },
      },
    },
  },
});

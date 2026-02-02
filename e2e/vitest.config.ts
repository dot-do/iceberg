import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // E2E tests run against live service - longer timeout
    testTimeout: 30000,
    hookTimeout: 30000,

    // Run tests sequentially to avoid race conditions
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },

    // Only run E2E tests
    include: ['**/*.test.ts'],

    // Retry failed tests once (network issues)
    retry: 1,

    // Reporter
    reporters: ['verbose'],
  },
});

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    globals: false,
    environment: 'node',
    // Memory management - prevent runaway processes
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    maxWorkers: 1,
    minWorkers: 1,
    isolate: true,
    // Disable watch mode by default (use vitest --watch explicitly)
    watch: false,
    // Timeouts to prevent hanging tests
    testTimeout: 30000,
    hookTimeout: 10000,
  },
});

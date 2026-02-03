import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    include: ['**/*.bench.ts'],
    root: __dirname,
    benchmark: {
      include: ['**/*.bench.ts'],
      outputFile: resolve(__dirname, 'benchmark-results.json'),
      reporters: ['verbose'],
    },
    // Longer timeouts for benchmarks against remote services
    testTimeout: 120000,
    hookTimeout: 60000,
  },
});

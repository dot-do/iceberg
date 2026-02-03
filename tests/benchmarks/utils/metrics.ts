/**
 * Metrics Collection for Benchmarks
 *
 * Utilities for collecting and analyzing benchmark metrics.
 */

// ============================================================================
// Types
// ============================================================================

export interface BenchmarkResult {
  operation: string;
  catalog: string;
  latencyMs: number;
  p50: number;
  p95: number;
  p99: number;
  throughput: number; // ops/sec
  errors: number;
  samples: number;
}

export interface TimingSample {
  durationMs: number;
  timestamp: number;
  error?: string;
}

// ============================================================================
// Metrics Collector
// ============================================================================

export class MetricsCollector {
  private samples: Map<string, TimingSample[]> = new Map();

  /**
   * Record a timing sample for an operation.
   */
  record(operation: string, catalog: string, durationMs: number, error?: string): void {
    const key = `${catalog}:${operation}`;
    if (!this.samples.has(key)) {
      this.samples.set(key, []);
    }
    this.samples.get(key)!.push({
      durationMs,
      timestamp: Date.now(),
      error,
    });
  }

  /**
   * Time an async operation and record the result.
   */
  async time<T>(
    operation: string,
    catalog: string,
    fn: () => Promise<T>
  ): Promise<T> {
    const start = performance.now();
    try {
      const result = await fn();
      this.record(operation, catalog, performance.now() - start);
      return result;
    } catch (error) {
      this.record(
        operation,
        catalog,
        performance.now() - start,
        error instanceof Error ? error.message : String(error)
      );
      throw error;
    }
  }

  /**
   * Get results for all recorded operations.
   */
  getResults(): BenchmarkResult[] {
    const results: BenchmarkResult[] = [];

    for (const [key, samples] of this.samples) {
      const [catalog, operation] = key.split(':');
      const successSamples = samples.filter((s) => !s.error);
      const errorCount = samples.length - successSamples.length;

      if (successSamples.length === 0) {
        results.push({
          operation,
          catalog,
          latencyMs: 0,
          p50: 0,
          p95: 0,
          p99: 0,
          throughput: 0,
          errors: errorCount,
          samples: samples.length,
        });
        continue;
      }

      const durations = successSamples.map((s) => s.durationMs).sort((a, b) => a - b);
      const totalDuration = durations.reduce((a, b) => a + b, 0);

      results.push({
        operation,
        catalog,
        latencyMs: totalDuration / durations.length,
        p50: percentile(durations, 50),
        p95: percentile(durations, 95),
        p99: percentile(durations, 99),
        throughput: successSamples.length / (totalDuration / 1000),
        errors: errorCount,
        samples: samples.length,
      });
    }

    return results;
  }

  /**
   * Reset all collected metrics.
   */
  reset(): void {
    this.samples.clear();
  }

  /**
   * Print results to console in a formatted table.
   */
  printResults(): void {
    const results = this.getResults();

    console.log('\n' + '='.repeat(100));
    console.log('Benchmark Results');
    console.log('='.repeat(100));

    // Group by operation
    const byOperation = new Map<string, BenchmarkResult[]>();
    for (const result of results) {
      if (!byOperation.has(result.operation)) {
        byOperation.set(result.operation, []);
      }
      byOperation.get(result.operation)!.push(result);
    }

    for (const [operation, opResults] of byOperation) {
      console.log(`\n${operation}:`);
      console.log('-'.repeat(80));
      console.log(
        'Catalog'.padEnd(20) +
          'Avg (ms)'.padStart(12) +
          'P50 (ms)'.padStart(12) +
          'P95 (ms)'.padStart(12) +
          'P99 (ms)'.padStart(12) +
          'Ops/sec'.padStart(12) +
          'Errors'.padStart(10)
      );
      console.log('-'.repeat(80));

      for (const result of opResults) {
        console.log(
          result.catalog.padEnd(20) +
            result.latencyMs.toFixed(2).padStart(12) +
            result.p50.toFixed(2).padStart(12) +
            result.p95.toFixed(2).padStart(12) +
            result.p99.toFixed(2).padStart(12) +
            result.throughput.toFixed(2).padStart(12) +
            result.errors.toString().padStart(10)
        );
      }
    }

    console.log('\n' + '='.repeat(100) + '\n');
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Calculate percentile from sorted array.
 */
function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return 0;
  if (sortedValues.length === 1) return sortedValues[0];

  const index = (p / 100) * (sortedValues.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);

  if (lower === upper) return sortedValues[lower];

  const fraction = index - lower;
  return sortedValues[lower] * (1 - fraction) + sortedValues[upper] * fraction;
}

/**
 * Run a function multiple times and collect timing statistics.
 */
export async function benchmark<T>(
  fn: () => Promise<T>,
  iterations: number = 10
): Promise<{ results: T[]; stats: { avg: number; p50: number; p95: number; p99: number } }> {
  const results: T[] = [];
  const durations: number[] = [];

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    results.push(await fn());
    durations.push(performance.now() - start);
  }

  durations.sort((a, b) => a - b);

  return {
    results,
    stats: {
      avg: durations.reduce((a, b) => a + b, 0) / durations.length,
      p50: percentile(durations, 50),
      p95: percentile(durations, 95),
      p99: percentile(durations, 99),
    },
  };
}

/**
 * Run concurrent operations and measure throughput.
 */
export async function benchmarkConcurrent<T>(
  fn: () => Promise<T>,
  concurrency: number,
  totalOperations: number
): Promise<{
  results: PromiseSettledResult<T>[];
  stats: { totalTimeMs: number; throughput: number; successRate: number };
}> {
  const start = performance.now();
  const batches: Promise<PromiseSettledResult<T>[]>[] = [];

  // Run in batches of `concurrency`
  for (let i = 0; i < totalOperations; i += concurrency) {
    const batchSize = Math.min(concurrency, totalOperations - i);
    const batch = Array.from({ length: batchSize }, () => fn());
    batches.push(Promise.allSettled(batch));
  }

  const allResults = (await Promise.all(batches)).flat();
  const totalTimeMs = performance.now() - start;

  const successCount = allResults.filter((r) => r.status === 'fulfilled').length;

  return {
    results: allResults,
    stats: {
      totalTimeMs,
      throughput: (totalOperations / totalTimeMs) * 1000, // ops/sec
      successRate: successCount / totalOperations,
    },
  };
}

/**
 * Create a global metrics collector instance.
 */
export const globalMetrics = new MetricsCollector();

# Performance Benchmarks: iceberg.do vs R2 Data Catalog

Benchmark suite comparing iceberg.do (Durable Objects + SQLite backend) against Cloudflare R2 Data Catalog for Iceberg metadata operations.

## Quick Start

```bash
# Run all benchmarks against iceberg.do
pnpm bench

# Run with R2 Data Catalog (requires credentials)
CF_ACCOUNT_ID=xxx R2_DATA_CATALOG_TOKEN=xxx pnpm bench

# Run specific benchmark
pnpm bench --grep "create table"

# Run only iceberg.do benchmarks
BENCHMARK_CATALOG=iceberg.do pnpm bench

# Run only R2 Data Catalog benchmarks
BENCHMARK_CATALOG=r2 pnpm bench
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ICEBERG_DO_URL` | No | `https://iceberg-do.dotdo.workers.dev` | iceberg.do worker URL |
| `CF_ACCOUNT_ID` | For R2 | - | Cloudflare Account ID |
| `R2_DATA_CATALOG_TOKEN` | For R2 | - | R2 Data Catalog API token |
| `BENCHMARK_CATALOG` | No | all | Filter: `iceberg.do`, `r2`, or `all` |

## Benchmark Operations

### Metadata Operations (Latency Focus)

| Operation | Description |
|-----------|-------------|
| **Create namespace** | Single namespace creation with properties |
| **List namespaces** | List all namespaces in catalog |
| **Create table** | Table with schema + partition spec |
| **Load table** | Read full table metadata |
| **List tables** | Pagination tests (10, 50 tables) |
| **Property commit** | Property update with OCC |

### Throughput Operations

| Operation | Description |
|-----------|-------------|
| **Concurrent loads** | 10/50 parallel loadTable calls |
| **Sequential creates** | 5 tables created in sequence |
| **Parallel creates** | 5 tables created in parallel |
| **Throughput test** | 20 creates at 5 concurrency |

## Datasets

Real-world schemas for realistic benchmarks:

| Dataset | Fields | Complexity | Use Case |
|---------|--------|------------|----------|
| **IMDB** | 15 | Medium | Movies, ratings, time-series |
| **O*NET** | 16 | Medium | Occupations, hierarchical |
| **Wiktionary** | 16 | Complex | Dictionary, variant/JSON |
| **Wikidata** | 12 | Complex | Knowledge graph, semi-structured |

## Output

Benchmarks output:
- Console: Real-time progress and summary
- JSON: `benchmark-results.json` (Vitest benchmark output)

Example output:
```
====================================================================================================
Benchmark Results
====================================================================================================

Create Namespace:
--------------------------------------------------------------------------------
Catalog             Avg (ms)    P50 (ms)    P95 (ms)    P99 (ms)     Ops/sec    Errors
--------------------------------------------------------------------------------
iceberg.do             45.23       42.10       62.30       85.20       22.11         0
r2-data-catalog        89.45       85.20      120.50      145.30       11.18         0

Create Table (IMDB):
--------------------------------------------------------------------------------
Catalog             Avg (ms)    P50 (ms)    P95 (ms)    P99 (ms)     Ops/sec    Errors
--------------------------------------------------------------------------------
iceberg.do             78.34       75.20       95.40      110.20       12.76         0
r2-data-catalog       145.67      140.30      180.50      210.40        6.87         0
```

## Architecture Comparison

### iceberg.do
- **Backend**: Durable Objects with SQLite
- **Location**: Edge (closest Cloudflare PoP)
- **Consistency**: Strong (DO provides single-writer)
- **Latency**: Low (edge-local reads)
- **Throughput**: High (SQLite is fast for metadata)

### R2 Data Catalog
- **Backend**: Cloudflare managed catalog service
- **Location**: Regional (R2 bucket region)
- **Consistency**: Eventual (catalog updates)
- **Latency**: Variable (depends on catalog sync)
- **Throughput**: Managed by Cloudflare

## File Structure

```
tests/benchmarks/
├── README.md                    # This file
├── vitest.config.ts             # Vitest bench mode config
├── catalog-benchmark.bench.ts   # Main benchmark file
├── utils/
│   ├── clients.ts               # iceberg.do + R2 catalog clients
│   ├── metrics.ts               # Timing/stats collection
│   └── cleanup.ts               # Test data cleanup
└── datasets/
    ├── index.ts                 # Dataset exports
    ├── imdb.ts                  # IMDB movies schema
    ├── onet.ts                  # O*NET occupations schema
    ├── wiktionary.ts            # Wiktionary definitions schema
    └── wikidata.ts              # Wikidata entities schema
```

## Adding New Benchmarks

1. Add new bench blocks to `catalog-benchmark.bench.ts`:

```typescript
describe('My New Benchmark', () => {
  if (shouldBenchmarkCatalog('iceberg.do')) {
    bench('iceberg.do - my operation', async () => {
      // benchmark code
    }, { iterations: 10 });
  }
});
```

2. Add new datasets to `datasets/`:

```typescript
export const MY_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'field', required: true, type: 'string' },
  ],
};
```

## Cleanup

Benchmarks automatically clean up test data after each run. Namespaces are prefixed with `bench_` and timestamp to avoid conflicts.

If cleanup fails, manually remove test namespaces:

```bash
# List test namespaces
curl https://iceberg-do.dotdo.workers.dev/v1/namespaces | jq '.namespaces[] | select(.[0] | startswith("bench_"))'

# Delete namespace
curl -X DELETE https://iceberg-do.dotdo.workers.dev/v1/namespaces/bench_xxx
```

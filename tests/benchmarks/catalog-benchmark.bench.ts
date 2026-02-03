/**
 * Catalog Benchmark Suite
 *
 * Compares performance of iceberg.do (DO SQLite backend) against
 * Cloudflare R2 Data Catalog for Iceberg metadata operations.
 */

import { describe, bench, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  createIcebergDoClient,
  createR2CatalogClient,
  shouldBenchmarkCatalog,
  type CatalogClient,
} from './utils/clients.js';
import {
  CleanupContext,
  generateBenchmarkNamespace,
  generateTableNames,
} from './utils/cleanup.js';
import { globalMetrics, benchmarkConcurrent } from './utils/metrics.js';
import { DATASETS, IMDB_MOVIES_SCHEMA, IMDB_PARTITION_BY_YEAR } from './datasets/index.js';

// ============================================================================
// Setup
// ============================================================================

const cleanup = new CleanupContext();
let icebergDoClient: CatalogClient;
let r2Client: CatalogClient | null;
let testNamespace: string[];

// Benchmark iterations
const ITERATIONS = 10;

beforeAll(() => {
  icebergDoClient = createIcebergDoClient();
  r2Client = createR2CatalogClient();

  if (!r2Client) {
    console.log(
      '\n⚠️  R2 Data Catalog not configured (missing CF_ACCOUNT_ID or R2_DATA_CATALOG_TOKEN)\n' +
        '   Only running iceberg.do benchmarks\n'
    );
  }
});

afterAll(async () => {
  // Clean up all test resources
  const result = await cleanup.cleanup();
  if (result.namespacesDropped > 0) {
    console.log(
      `\nCleanup: dropped ${result.namespacesDropped} namespaces, ${result.tablesDropped} tables`
    );
  }

  // Print collected metrics
  globalMetrics.printResults();
});

// ============================================================================
// Namespace Operations
// ============================================================================

describe('Namespace Operations', () => {
  describe('Create Namespace', () => {
    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - create namespace',
        async () => {
          const ns = generateBenchmarkNamespace('ns_create');
          await icebergDoClient.createNamespace(ns, { benchmark: 'true' });
          cleanup.register(icebergDoClient, ns);
        },
        { iterations: ITERATIONS }
      );
    }

    if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
      bench(
        'r2-data-catalog - create namespace',
        async () => {
          const ns = generateBenchmarkNamespace('ns_create');
          await r2Client!.createNamespace(ns, { benchmark: 'true' });
          cleanup.register(r2Client!, ns);
        },
        { iterations: ITERATIONS }
      );
    }
  });

  describe('List Namespaces', () => {
    beforeAll(async () => {
      // Create test namespaces for listing
      testNamespace = generateBenchmarkNamespace('ns_list');
      await icebergDoClient.createNamespace(testNamespace);
      cleanup.register(icebergDoClient, testNamespace);

      if (r2Client) {
        const r2Ns = generateBenchmarkNamespace('ns_list_r2');
        await r2Client.createNamespace(r2Ns);
        cleanup.register(r2Client, r2Ns);
      }
    });

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - list namespaces',
        async () => {
          await icebergDoClient.listNamespaces();
        },
        { iterations: ITERATIONS }
      );
    }

    if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
      bench(
        'r2-data-catalog - list namespaces',
        async () => {
          await r2Client!.listNamespaces();
        },
        { iterations: ITERATIONS }
      );
    }
  });
});

// ============================================================================
// Table Creation
// ============================================================================

describe('Table Creation', () => {
  let createTableNs: string[];

  beforeAll(async () => {
    createTableNs = generateBenchmarkNamespace('tbl_create');
    await icebergDoClient.createNamespace(createTableNs);
    cleanup.register(icebergDoClient, createTableNs);

    if (r2Client) {
      await r2Client.createNamespace(createTableNs);
      cleanup.register(r2Client, createTableNs);
    }
  });

  describe('Simple Schema (5 fields)', () => {
    const simpleSchema = {
      type: 'struct' as const,
      'schema-id': 0,
      fields: [
        { id: 1, name: 'id', required: true, type: 'long' as const },
        { id: 2, name: 'name', required: true, type: 'string' as const },
        { id: 3, name: 'value', required: false, type: 'double' as const },
        { id: 4, name: 'active', required: true, type: 'boolean' as const },
        { id: 5, name: 'created_at', required: true, type: 'timestamptz' as const },
      ],
    };

    let tableCounter = 0;

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - create simple table',
        async () => {
          await icebergDoClient.createTable({
            name: `simple_${tableCounter++}`,
            namespace: createTableNs,
            schema: simpleSchema,
          });
        },
        { iterations: ITERATIONS }
      );
    }

    if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
      bench(
        'r2-data-catalog - create simple table',
        async () => {
          await r2Client!.createTable({
            name: `simple_r2_${tableCounter++}`,
            namespace: createTableNs,
            schema: simpleSchema,
          });
        },
        { iterations: ITERATIONS }
      );
    }
  });

  describe('Complex Schema (IMDB - 15 fields)', () => {
    let tableCounter = 0;

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - create IMDB table',
        async () => {
          await icebergDoClient.createTable({
            name: `imdb_${tableCounter++}`,
            namespace: createTableNs,
            schema: IMDB_MOVIES_SCHEMA,
            partitionSpec: IMDB_PARTITION_BY_YEAR,
          });
        },
        { iterations: ITERATIONS }
      );
    }

    if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
      bench(
        'r2-data-catalog - create IMDB table',
        async () => {
          await r2Client!.createTable({
            name: `imdb_r2_${tableCounter++}`,
            namespace: createTableNs,
            schema: IMDB_MOVIES_SCHEMA,
            partitionSpec: IMDB_PARTITION_BY_YEAR,
          });
        },
        { iterations: ITERATIONS }
      );
    }
  });

  describe('All Datasets', () => {
    for (const dataset of DATASETS) {
      let tableCounter = 0;

      if (shouldBenchmarkCatalog('iceberg.do')) {
        bench(
          `iceberg.do - create ${dataset.name}`,
          async () => {
            await icebergDoClient.createTable({
              name: `${dataset.name}_${tableCounter++}`,
              namespace: createTableNs,
              schema: dataset.schema,
              partitionSpec: dataset.partitionSpec,
              properties: dataset.properties,
            });
          },
          { iterations: Math.min(ITERATIONS, 5) }
        );
      }
    }
  });
});

// ============================================================================
// Table Loading
// ============================================================================

describe('Table Loading', () => {
  let loadTableNs: string[];
  const tableName = 'load_test_table';

  beforeAll(async () => {
    loadTableNs = generateBenchmarkNamespace('tbl_load');
    await icebergDoClient.createNamespace(loadTableNs);
    await icebergDoClient.createTable({
      name: tableName,
      namespace: loadTableNs,
      schema: IMDB_MOVIES_SCHEMA,
      partitionSpec: IMDB_PARTITION_BY_YEAR,
    });
    cleanup.register(icebergDoClient, loadTableNs);

    if (r2Client) {
      await r2Client.createNamespace(loadTableNs);
      await r2Client.createTable({
        name: tableName,
        namespace: loadTableNs,
        schema: IMDB_MOVIES_SCHEMA,
        partitionSpec: IMDB_PARTITION_BY_YEAR,
      });
      cleanup.register(r2Client, loadTableNs);
    }
  });

  if (shouldBenchmarkCatalog('iceberg.do')) {
    bench(
      'iceberg.do - load table',
      async () => {
        await icebergDoClient.loadTable(loadTableNs, tableName);
      },
      { iterations: ITERATIONS * 2 }
    );
  }

  if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
    bench(
      'r2-data-catalog - load table',
      async () => {
        await r2Client!.loadTable(loadTableNs, tableName);
      },
      { iterations: ITERATIONS * 2 }
    );
  }
});

// ============================================================================
// Table Listing
// ============================================================================

describe('Table Listing', () => {
  describe('Small namespace (10 tables)', () => {
    let listNs10: string[];

    beforeAll(async () => {
      listNs10 = generateBenchmarkNamespace('list_10');
      await icebergDoClient.createNamespace(listNs10);

      const simpleSchema = {
        type: 'struct' as const,
        'schema-id': 0,
        fields: [{ id: 1, name: 'id', required: true, type: 'long' as const }],
      };

      for (let i = 0; i < 10; i++) {
        await icebergDoClient.createTable({
          name: `table_${i}`,
          namespace: listNs10,
          schema: simpleSchema,
        });
      }
      cleanup.register(icebergDoClient, listNs10);
    });

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - list 10 tables',
        async () => {
          await icebergDoClient.listTables(listNs10);
        },
        { iterations: ITERATIONS }
      );
    }
  });

  describe('Medium namespace (50 tables)', () => {
    let listNs50: string[];

    beforeAll(async () => {
      listNs50 = generateBenchmarkNamespace('list_50');
      await icebergDoClient.createNamespace(listNs50);

      const simpleSchema = {
        type: 'struct' as const,
        'schema-id': 0,
        fields: [{ id: 1, name: 'id', required: true, type: 'long' as const }],
      };

      // Create tables in parallel batches
      const batchSize = 10;
      for (let batch = 0; batch < 5; batch++) {
        await Promise.all(
          Array.from({ length: batchSize }, (_, i) =>
            icebergDoClient.createTable({
              name: `table_${batch * batchSize + i}`,
              namespace: listNs50,
              schema: simpleSchema,
            })
          )
        );
      }
      cleanup.register(icebergDoClient, listNs50);
    });

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - list 50 tables',
        async () => {
          await icebergDoClient.listTables(listNs50);
        },
        { iterations: ITERATIONS }
      );
    }
  });
});

// ============================================================================
// Commit Operations
// ============================================================================

describe('Commit Operations', () => {
  let commitNs: string[];
  const commitTableName = 'commit_test';

  beforeAll(async () => {
    commitNs = generateBenchmarkNamespace('commit');
    await icebergDoClient.createNamespace(commitNs);
    await icebergDoClient.createTable({
      name: commitTableName,
      namespace: commitNs,
      schema: IMDB_MOVIES_SCHEMA,
    });
    cleanup.register(icebergDoClient, commitNs);

    if (r2Client) {
      await r2Client.createNamespace(commitNs);
      await r2Client.createTable({
        name: commitTableName,
        namespace: commitNs,
        schema: IMDB_MOVIES_SCHEMA,
      });
      cleanup.register(r2Client, commitNs);
    }
  });

  describe('Property Update Commit', () => {
    let propCounter = 0;

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - property update commit',
        async () => {
          await icebergDoClient.commitPropertyUpdate(commitNs, commitTableName, {
            [`benchmark.prop.${propCounter++}`]: `value_${Date.now()}`,
          });
        },
        { iterations: ITERATIONS }
      );
    }

    if (r2Client && shouldBenchmarkCatalog('r2-data-catalog')) {
      bench(
        'r2-data-catalog - property update',
        async () => {
          await r2Client!.commitPropertyUpdate(commitNs, commitTableName, {
            [`benchmark.prop.${propCounter++}`]: `value_${Date.now()}`,
          });
        },
        { iterations: ITERATIONS }
      );
    }
  });
});

// ============================================================================
// Concurrent Operations
// ============================================================================

describe('Concurrent Operations', () => {
  describe('Concurrent Table Loads', () => {
    let concurrentNs: string[];
    const concurrentTable = 'concurrent_load';

    beforeAll(async () => {
      concurrentNs = generateBenchmarkNamespace('concurrent');
      await icebergDoClient.createNamespace(concurrentNs);
      await icebergDoClient.createTable({
        name: concurrentTable,
        namespace: concurrentNs,
        schema: IMDB_MOVIES_SCHEMA,
      });
      cleanup.register(icebergDoClient, concurrentNs);
    });

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - 10 concurrent loads',
        async () => {
          await Promise.all(
            Array.from({ length: 10 }, () =>
              icebergDoClient.loadTable(concurrentNs, concurrentTable)
            )
          );
        },
        { iterations: 5 }
      );

      bench(
        'iceberg.do - 50 concurrent loads',
        async () => {
          await Promise.all(
            Array.from({ length: 50 }, () =>
              icebergDoClient.loadTable(concurrentNs, concurrentTable)
            )
          );
        },
        { iterations: 3 }
      );
    }
  });

  describe('Sequential vs Parallel Creation', () => {
    let seqParNs: string[];

    beforeAll(async () => {
      seqParNs = generateBenchmarkNamespace('seq_par');
      await icebergDoClient.createNamespace(seqParNs);
      cleanup.register(icebergDoClient, seqParNs);
    });

    const simpleSchema = {
      type: 'struct' as const,
      'schema-id': 0,
      fields: [{ id: 1, name: 'id', required: true, type: 'long' as const }],
    };

    let seqCounter = 0;
    let parCounter = 0;

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - 5 sequential creates',
        async () => {
          for (let i = 0; i < 5; i++) {
            await icebergDoClient.createTable({
              name: `seq_${seqCounter++}`,
              namespace: seqParNs,
              schema: simpleSchema,
            });
          }
        },
        { iterations: 3 }
      );

      bench(
        'iceberg.do - 5 parallel creates',
        async () => {
          await Promise.all(
            Array.from({ length: 5 }, (_, i) =>
              icebergDoClient.createTable({
                name: `par_${parCounter++}`,
                namespace: seqParNs,
                schema: simpleSchema,
              })
            )
          );
        },
        { iterations: 3 }
      );
    }
  });
});

// ============================================================================
// Throughput Tests
// ============================================================================

describe('Throughput Tests', () => {
  describe('Table Creation Throughput', () => {
    let throughputNs: string[];

    beforeAll(async () => {
      throughputNs = generateBenchmarkNamespace('throughput');
      await icebergDoClient.createNamespace(throughputNs);
      cleanup.register(icebergDoClient, throughputNs);
    });

    const simpleSchema = {
      type: 'struct' as const,
      'schema-id': 0,
      fields: [{ id: 1, name: 'id', required: true, type: 'long' as const }],
    };

    let throughputCounter = 0;

    if (shouldBenchmarkCatalog('iceberg.do')) {
      bench(
        'iceberg.do - 20 table creates (5 concurrent)',
        async () => {
          const results = await benchmarkConcurrent(
            () =>
              icebergDoClient.createTable({
                name: `tp_${throughputCounter++}`,
                namespace: throughputNs,
                schema: simpleSchema,
              }),
            5, // concurrency
            20 // total operations
          );
          // Log throughput for this run
          globalMetrics.record('throughput-create-20', 'iceberg.do', results.stats.totalTimeMs);
        },
        { iterations: 2 }
      );
    }
  });
});

/**
 * Catalog Benchmark Suite
 *
 * Compares performance of iceberg.do (DO SQLite backend) against
 * Cloudflare R2 Data Catalog for Iceberg metadata operations.
 */

import { describe, bench, beforeAll, afterAll } from 'vitest';
import {
  createIcebergDoClient,
  createR2CatalogClient,
  shouldBenchmarkCatalog,
  type CatalogClient,
} from './utils/clients.js';
import {
  CleanupContext,
  generateBenchmarkNamespace,
} from './utils/cleanup.js';
import { globalMetrics, benchmarkConcurrent } from './utils/metrics.js';
import { DATASETS, IMDB_MOVIES_SCHEMA, IMDB_PARTITION_BY_YEAR } from './datasets/index.js';

// ============================================================================
// Global Setup - All namespaces created before any benchmarks
// ============================================================================

const cleanup = new CleanupContext();
let icebergDoClient: CatalogClient;
let r2Client: CatalogClient | null;

// Pre-allocated namespaces for each benchmark category
const namespaces = {
  nsCreate: null as string[] | null,
  nsList: null as string[] | null,
  tblCreate: null as string[] | null,
  tblLoad: null as string[] | null,
  list10: null as string[] | null,
  list50: null as string[] | null,
  commit: null as string[] | null,
  concurrent: null as string[] | null,
  seqPar: null as string[] | null,
  throughput: null as string[] | null,
};

// Table names and schemas
const SIMPLE_SCHEMA = {
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

const MINIMAL_SCHEMA = {
  type: 'struct' as const,
  'schema-id': 0,
  fields: [{ id: 1, name: 'id', required: true, type: 'long' as const }],
};

// Counters for unique names
let tableCounter = 0;
let nsCounter = 0;

// Benchmark iterations
const ITERATIONS = 10;

beforeAll(async () => {
  icebergDoClient = createIcebergDoClient();
  r2Client = createR2CatalogClient();

  if (!r2Client) {
    console.log(
      '\n⚠️  R2 Data Catalog not configured (missing CF_ACCOUNT_ID or R2_DATA_CATALOG_TOKEN)\n' +
        '   Only running iceberg.do benchmarks\n'
    );
  }

  console.log('\n📦 Setting up benchmark namespaces...\n');

  // Create all namespaces and required tables upfront
  try {
    // 1. Table creation namespace
    namespaces.tblCreate = generateBenchmarkNamespace('tbl_create');
    await icebergDoClient.createNamespace(namespaces.tblCreate);
    cleanup.register(icebergDoClient, namespaces.tblCreate);

    // 2. Table loading namespace with pre-created table
    namespaces.tblLoad = generateBenchmarkNamespace('tbl_load');
    await icebergDoClient.createNamespace(namespaces.tblLoad);
    await icebergDoClient.createTable({
      name: 'load_test_table',
      namespace: namespaces.tblLoad,
      schema: IMDB_MOVIES_SCHEMA,
      partitionSpec: IMDB_PARTITION_BY_YEAR,
    });
    cleanup.register(icebergDoClient, namespaces.tblLoad);

    // 3. List 10 tables namespace
    namespaces.list10 = generateBenchmarkNamespace('list_10');
    await icebergDoClient.createNamespace(namespaces.list10);
    for (let i = 0; i < 10; i++) {
      await icebergDoClient.createTable({
        name: `table_${i}`,
        namespace: namespaces.list10,
        schema: MINIMAL_SCHEMA,
      });
    }
    cleanup.register(icebergDoClient, namespaces.list10);

    // 4. List 50 tables namespace
    namespaces.list50 = generateBenchmarkNamespace('list_50');
    await icebergDoClient.createNamespace(namespaces.list50);
    const batchSize = 10;
    for (let batch = 0; batch < 5; batch++) {
      await Promise.all(
        Array.from({ length: batchSize }, (_, i) =>
          icebergDoClient.createTable({
            name: `table_${batch * batchSize + i}`,
            namespace: namespaces.list50,
            schema: MINIMAL_SCHEMA,
          })
        )
      );
    }
    cleanup.register(icebergDoClient, namespaces.list50);

    // 5. Commit namespace with table
    namespaces.commit = generateBenchmarkNamespace('commit');
    await icebergDoClient.createNamespace(namespaces.commit);
    await icebergDoClient.createTable({
      name: 'commit_test',
      namespace: namespaces.commit,
      schema: IMDB_MOVIES_SCHEMA,
    });
    cleanup.register(icebergDoClient, namespaces.commit);

    // 6. Concurrent load namespace with table
    namespaces.concurrent = generateBenchmarkNamespace('concurrent');
    await icebergDoClient.createNamespace(namespaces.concurrent);
    await icebergDoClient.createTable({
      name: 'concurrent_load',
      namespace: namespaces.concurrent,
      schema: IMDB_MOVIES_SCHEMA,
    });
    cleanup.register(icebergDoClient, namespaces.concurrent);

    // 7. Sequential vs parallel namespace
    namespaces.seqPar = generateBenchmarkNamespace('seq_par');
    await icebergDoClient.createNamespace(namespaces.seqPar);
    cleanup.register(icebergDoClient, namespaces.seqPar);

    // 8. Throughput namespace
    namespaces.throughput = generateBenchmarkNamespace('throughput');
    await icebergDoClient.createNamespace(namespaces.throughput);
    cleanup.register(icebergDoClient, namespaces.throughput);

    console.log('✅ Setup complete\n');
  } catch (error) {
    console.error('❌ Setup failed:', error);
    throw error;
  }
});

afterAll(async () => {
  const result = await cleanup.cleanup();
  if (result.namespacesDropped > 0) {
    console.log(
      `\n🧹 Cleanup: dropped ${result.namespacesDropped} namespaces, ${result.tablesDropped} tables`
    );
  }
  globalMetrics.printResults();
});

// ============================================================================
// Namespace Operations
// ============================================================================

describe('Namespace Operations', () => {
  bench(
    'iceberg.do - create namespace',
    async () => {
      const ns = [`ns_create_${Date.now()}_${nsCounter++}`];
      await icebergDoClient.createNamespace(ns, { benchmark: 'true' });
      cleanup.register(icebergDoClient, ns);
    },
    { iterations: ITERATIONS }
  );

  bench(
    'iceberg.do - list namespaces',
    async () => {
      await icebergDoClient.listNamespaces();
    },
    { iterations: ITERATIONS }
  );
});

// ============================================================================
// Table Creation
// ============================================================================

describe('Table Creation', () => {
  bench(
    'iceberg.do - create simple table (5 fields)',
    async () => {
      await icebergDoClient.createTable({
        name: `simple_${tableCounter++}`,
        namespace: namespaces.tblCreate!,
        schema: SIMPLE_SCHEMA,
      });
    },
    { iterations: ITERATIONS }
  );

  bench(
    'iceberg.do - create IMDB table (15 fields + partition)',
    async () => {
      await icebergDoClient.createTable({
        name: `imdb_${tableCounter++}`,
        namespace: namespaces.tblCreate!,
        schema: IMDB_MOVIES_SCHEMA,
        partitionSpec: IMDB_PARTITION_BY_YEAR,
      });
    },
    { iterations: ITERATIONS }
  );
});

// ============================================================================
// Dataset Schema Benchmarks
// ============================================================================

describe('Create All Dataset Schemas', () => {
  for (const dataset of DATASETS) {
    bench(
      `iceberg.do - create ${dataset.name} (${dataset.fieldCount} fields)`,
      async () => {
        await icebergDoClient.createTable({
          name: `${dataset.name}_${tableCounter++}`,
          namespace: namespaces.tblCreate!,
          schema: dataset.schema,
          partitionSpec: dataset.partitionSpec,
          properties: dataset.properties,
        });
      },
      { iterations: 5 }
    );
  }
});

// ============================================================================
// Table Loading
// ============================================================================

describe('Table Loading', () => {
  bench(
    'iceberg.do - load table',
    async () => {
      await icebergDoClient.loadTable(namespaces.tblLoad!, 'load_test_table');
    },
    { iterations: ITERATIONS * 2 }
  );
});

// ============================================================================
// Table Listing
// ============================================================================

describe('Table Listing', () => {
  bench(
    'iceberg.do - list 10 tables',
    async () => {
      await icebergDoClient.listTables(namespaces.list10!);
    },
    { iterations: ITERATIONS }
  );

  bench(
    'iceberg.do - list 50 tables',
    async () => {
      await icebergDoClient.listTables(namespaces.list50!);
    },
    { iterations: ITERATIONS }
  );
});

// ============================================================================
// Commit Operations
// ============================================================================

describe('Commit Operations', () => {
  let propCounter = 0;

  bench(
    'iceberg.do - property update commit (with OCC)',
    async () => {
      await icebergDoClient.commitPropertyUpdate(namespaces.commit!, 'commit_test', {
        [`benchmark.prop.${propCounter++}`]: `value_${Date.now()}`,
      });
    },
    { iterations: ITERATIONS }
  );
});

// ============================================================================
// Concurrent Operations
// ============================================================================

describe('Concurrent Operations', () => {
  bench(
    'iceberg.do - 10 concurrent loads',
    async () => {
      await Promise.all(
        Array.from({ length: 10 }, () =>
          icebergDoClient.loadTable(namespaces.concurrent!, 'concurrent_load')
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
          icebergDoClient.loadTable(namespaces.concurrent!, 'concurrent_load')
        )
      );
    },
    { iterations: 3 }
  );

  bench(
    'iceberg.do - 5 sequential creates',
    async () => {
      for (let i = 0; i < 5; i++) {
        await icebergDoClient.createTable({
          name: `seq_${tableCounter++}`,
          namespace: namespaces.seqPar!,
          schema: MINIMAL_SCHEMA,
        });
      }
    },
    { iterations: 3 }
  );

  bench(
    'iceberg.do - 5 parallel creates',
    async () => {
      await Promise.all(
        Array.from({ length: 5 }, () =>
          icebergDoClient.createTable({
            name: `par_${tableCounter++}`,
            namespace: namespaces.seqPar!,
            schema: MINIMAL_SCHEMA,
          })
        )
      );
    },
    { iterations: 3 }
  );
});

// ============================================================================
// Throughput Tests
// ============================================================================

describe('Throughput Tests', () => {
  bench(
    'iceberg.do - 20 table creates (5 concurrent batches)',
    async () => {
      const results = await benchmarkConcurrent(
        () =>
          icebergDoClient.createTable({
            name: `tp_${tableCounter++}`,
            namespace: namespaces.throughput!,
            schema: MINIMAL_SCHEMA,
          }),
        5,
        20
      );
      globalMetrics.record('throughput-create-20', 'iceberg.do', results.stats.totalTimeMs);
    },
    { iterations: 2 }
  );
});

/**
 * E2E Tests for iceberg.do REST Catalog
 *
 * These tests run against the LIVE iceberg.do worker - no mocks, no fakes.
 * They verify full Iceberg functionality using both:
 * 1. Our @dotdo/iceberg library
 * 2. Supabase's iceberg-js third-party library
 *
 * @see https://iceberg-do.dotdo.workers.dev
 * @see https://github.com/supabase/iceberg-js
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { IcebergRestCatalog } from 'iceberg-js';

// ============================================================================
// Configuration
// ============================================================================

const ICEBERG_DO_URL = process.env.ICEBERG_DO_URL || 'https://iceberg-do.dotdo.workers.dev';
const BASE_URL = `${ICEBERG_DO_URL}/v1`;  // For direct HTTP tests
const ICEBERG_JS_BASE_URL = ICEBERG_DO_URL;  // iceberg-js adds /v1 prefix automatically

// Test namespace - unique per test run to avoid conflicts
const TEST_NAMESPACE = `e2e_test_${Date.now()}`;

// ============================================================================
// Helper Functions
// ============================================================================

async function fetchJson(path: string, options: RequestInit = {}): Promise<unknown> {
  const url = `${BASE_URL}${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }

  return response.json();
}

async function deleteNamespace(namespace: string): Promise<void> {
  try {
    // First delete all tables in the namespace
    const tables = await fetchJson(`/namespaces/${namespace}/tables`) as { identifiers: Array<{ name: string }> };
    for (const table of tables.identifiers || []) {
      await fetch(`${BASE_URL}/namespaces/${namespace}/tables/${table.name}`, { method: 'DELETE' });
    }
    // Then delete the namespace
    await fetch(`${BASE_URL}/namespaces/${namespace}`, { method: 'DELETE' });
  } catch {
    // Ignore errors during cleanup
  }
}

// ============================================================================
// Test Schema Definitions
// ============================================================================

const USER_SCHEMA = {
  type: 'struct' as const,
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'email', required: true, type: 'string' },
    { id: 3, name: 'name', required: false, type: 'string' },
    { id: 4, name: 'created_at', required: true, type: 'timestamptz' },
    { id: 5, name: 'active', required: true, type: 'boolean' },
  ],
};

const ORDER_SCHEMA = {
  type: 'struct' as const,
  'schema-id': 0,
  fields: [
    { id: 1, name: 'order_id', required: true, type: 'long' },
    { id: 2, name: 'user_id', required: true, type: 'long' },
    { id: 3, name: 'total', required: true, type: 'decimal(10,2)' },
    { id: 4, name: 'status', required: true, type: 'string' },
    { id: 5, name: 'order_date', required: true, type: 'date' },
  ],
};

const PARTITION_SPEC = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 5, // order_date
      'field-id': 1000,
      name: 'order_date_day',
      transform: 'day',
    },
  ],
};

// ============================================================================
// E2E Tests - Direct HTTP (No Library)
// ============================================================================

describe('E2E: iceberg.do Direct HTTP Tests', () => {
  beforeAll(async () => {
    // Clean up any existing test namespace
    await deleteNamespace(TEST_NAMESPACE);
  });

  afterAll(async () => {
    // Clean up test namespace
    await deleteNamespace(TEST_NAMESPACE);
  });

  describe('Service Health', () => {
    it('should return healthy status', async () => {
      const health = await fetchJson('/../health') as { status: string; service: string };
      expect(health.status).toBe('healthy');
      expect(health.service).toBe('iceberg.do');
    });

    it('should return config', async () => {
      const config = await fetchJson('/config') as { defaults: Record<string, string> };
      expect(config.defaults).toBeDefined();
      expect(config.defaults['write.parquet.compression-codec']).toBe('zstd');
    });
  });

  describe('Namespace Operations', () => {
    it('should create a namespace', async () => {
      const result = await fetchJson('/namespaces', {
        method: 'POST',
        body: JSON.stringify({
          namespace: [TEST_NAMESPACE],
          properties: { owner: 'e2e-test', description: 'E2E test namespace' },
        }),
      }) as { namespace: string[]; properties: Record<string, string> };

      expect(result.namespace).toEqual([TEST_NAMESPACE]);
      expect(result.properties.owner).toBe('e2e-test');
    });

    it('should list namespaces', async () => {
      const result = await fetchJson('/namespaces') as { namespaces: string[][] };
      expect(result.namespaces).toBeDefined();
      expect(result.namespaces.some(ns => ns[0] === TEST_NAMESPACE)).toBe(true);
    });

    it('should get namespace metadata', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}`) as { namespace: string[]; properties: Record<string, string> };
      expect(result.namespace).toEqual([TEST_NAMESPACE]);
      expect(result.properties.owner).toBe('e2e-test');
    });

    it('should update namespace properties', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/properties`, {
        method: 'POST',
        body: JSON.stringify({
          updates: { updated: 'true', version: '2' },
          removals: [],
        }),
      }) as { updated: string[]; removed: string[] };

      expect(result.updated).toContain('updated');
      expect(result.updated).toContain('version');
    });

    it('should reject duplicate namespace creation', async () => {
      await expect(
        fetchJson('/namespaces', {
          method: 'POST',
          body: JSON.stringify({ namespace: [TEST_NAMESPACE] }),
        })
      ).rejects.toThrow(/409|already exists/i);
    });
  });

  describe('Table Operations', () => {
    const tableName = 'users';

    it('should create a table', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`, {
        method: 'POST',
        body: JSON.stringify({
          name: tableName,
          schema: USER_SCHEMA,
          properties: { 'write.format.default': 'parquet' },
        }),
      }) as { metadata: TableMetadata; 'metadata-location': string };

      expect(result.metadata).toBeDefined();
      expect(result.metadata['format-version']).toBe(2);
      expect(result.metadata['table-uuid']).toBeDefined();
      expect(result.metadata.schemas[0].fields.length).toBe(5);
      expect(result['metadata-location']).toContain('.metadata.json');
    });

    it('should list tables', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`) as { identifiers: Array<{ namespace: string[]; name: string }> };
      expect(result.identifiers).toBeDefined();
      expect(result.identifiers.some(t => t.name === tableName)).toBe(true);
    });

    it('should load table metadata', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/${tableName}`) as { metadata: TableMetadata };
      expect(result.metadata).toBeDefined();
      expect(result.metadata['format-version']).toBe(2);
      expect(result.metadata.schemas).toHaveLength(1);
    });

    it('should reject duplicate table creation', async () => {
      await expect(
        fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`, {
          method: 'POST',
          body: JSON.stringify({ name: tableName, schema: USER_SCHEMA }),
        })
      ).rejects.toThrow(/409|already exists/i);
    });

    it('should create table with partition spec', async () => {
      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`, {
        method: 'POST',
        body: JSON.stringify({
          name: 'orders',
          schema: ORDER_SCHEMA,
          'partition-spec': PARTITION_SPEC,
        }),
      }) as { metadata: TableMetadata };

      expect(result.metadata['partition-specs']).toHaveLength(1);
      expect(result.metadata['partition-specs'][0].fields).toHaveLength(1);
      expect(result.metadata['partition-specs'][0].fields[0].transform).toBe('day');
    });
  });

  describe('Table Commits', () => {
    it('should commit schema evolution', async () => {
      // Add a new column to users table
      const loadResult = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/users`) as { metadata: TableMetadata };
      const currentMetadata = loadResult.metadata;

      const newSchema = {
        type: 'struct' as const,
        'schema-id': 1,
        fields: [
          ...USER_SCHEMA.fields,
          { id: 6, name: 'updated_at', required: false, type: 'timestamptz' },
        ],
      };

      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/users`, {
        method: 'POST',
        body: JSON.stringify({
          requirements: [
            { type: 'assert-table-uuid', uuid: currentMetadata['table-uuid'] },
          ],
          updates: [
            { action: 'add-schema', schema: newSchema, 'last-column-id': 6 },
            { action: 'set-current-schema', 'schema-id': 1 },
          ],
        }),
      }) as { 'metadata-location': string; metadata: TableMetadata };

      expect(result.metadata.schemas).toHaveLength(2);
      expect(result.metadata['current-schema-id']).toBe(1);
      expect(result.metadata.schemas[1].fields).toHaveLength(6);
    });

    it('should reject commit with wrong table UUID', async () => {
      await expect(
        fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/users`, {
          method: 'POST',
          body: JSON.stringify({
            requirements: [
              { type: 'assert-table-uuid', uuid: '00000000-0000-0000-0000-000000000000' },
            ],
            updates: [
              { action: 'set-properties', updates: { test: 'value' } },
            ],
          }),
        })
      ).rejects.toThrow(/409|uuid|mismatch/i);
    });

    it('should commit property updates', async () => {
      const loadResult = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/users`) as { metadata: TableMetadata };

      const result = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables/users`, {
        method: 'POST',
        body: JSON.stringify({
          requirements: [
            { type: 'assert-table-uuid', uuid: loadResult.metadata['table-uuid'] },
          ],
          updates: [
            { action: 'set-properties', updates: { 'custom.property': 'test-value' } },
          ],
        }),
      }) as { metadata: TableMetadata };

      expect(result.metadata.properties['custom.property']).toBe('test-value');
    });
  });

  describe('Table Rename and Drop', () => {
    it('should rename a table', async () => {
      // Create a table to rename
      await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`, {
        method: 'POST',
        body: JSON.stringify({
          name: 'to_rename',
          schema: { type: 'struct', 'schema-id': 0, fields: [{ id: 1, name: 'id', required: true, type: 'long' }] },
        }),
      });

      // Rename returns 204 No Content, so use fetch directly (not fetchJson)
      const renameResponse = await fetch(`${BASE_URL}/tables/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source: { namespace: [TEST_NAMESPACE], name: 'to_rename' },
          destination: { namespace: [TEST_NAMESPACE], name: 'renamed_table' },
        }),
      });
      expect(renameResponse.ok).toBe(true);

      // Verify rename
      const tables = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`) as { identifiers: Array<{ name: string }> };
      expect(tables.identifiers.some(t => t.name === 'renamed_table')).toBe(true);
      expect(tables.identifiers.some(t => t.name === 'to_rename')).toBe(false);
    });

    it('should drop a table', async () => {
      // Create a table to drop
      await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`, {
        method: 'POST',
        body: JSON.stringify({
          name: 'to_drop',
          schema: { type: 'struct', 'schema-id': 0, fields: [{ id: 1, name: 'id', required: true, type: 'long' }] },
        }),
      });

      // Drop the table
      await fetch(`${BASE_URL}/namespaces/${TEST_NAMESPACE}/tables/to_drop`, { method: 'DELETE' });

      // Verify drop
      const tables = await fetchJson(`/namespaces/${TEST_NAMESPACE}/tables`) as { identifiers: Array<{ name: string }> };
      expect(tables.identifiers.some(t => t.name === 'to_drop')).toBe(false);
    });
  });
});

// ============================================================================
// E2E Tests - Supabase iceberg-js Library
// ============================================================================

describe('E2E: iceberg.do with Supabase iceberg-js', () => {
  let catalog: IcebergRestCatalog;
  const SUPABASE_TEST_NS = `supabase_e2e_${Date.now()}`;

  beforeAll(async () => {
    // Create catalog client
    // Note: iceberg-js automatically adds /v1 prefix, so we use the base URL
    catalog = new IcebergRestCatalog({
      baseUrl: ICEBERG_JS_BASE_URL,
      // No auth needed for dev mode
    });

    // Clean up any existing test namespace
    await deleteNamespace(SUPABASE_TEST_NS);
  });

  afterAll(async () => {
    await deleteNamespace(SUPABASE_TEST_NS);
  });

  describe('Namespace Operations via iceberg-js', () => {
    it('should create namespace', async () => {
      const result = await catalog.createNamespace(
        { namespace: [SUPABASE_TEST_NS] },
        { properties: { source: 'iceberg-js', test: 'true' } }
      );

      expect(result.namespace).toEqual([SUPABASE_TEST_NS]);
    });

    it('should list namespaces', async () => {
      const namespaces = await catalog.listNamespaces();
      // listNamespaces returns NamespaceIdentifier[] directly
      expect(namespaces.some(ns => ns.namespace[0] === SUPABASE_TEST_NS)).toBe(true);
    });

    it('should load namespace metadata', async () => {
      const metadata = await catalog.loadNamespaceMetadata({ namespace: [SUPABASE_TEST_NS] });
      expect(metadata.properties?.source).toBe('iceberg-js');
    });

    it('should check if namespace exists', async () => {
      const exists = await catalog.namespaceExists({ namespace: [SUPABASE_TEST_NS] });
      expect(exists).toBe(true);

      const notExists = await catalog.namespaceExists({ namespace: ['nonexistent_ns'] });
      expect(notExists).toBe(false);
    });
  });

  describe('Table Operations via iceberg-js', () => {
    const tableName = 'events';

    it('should create table', async () => {
      const result = await catalog.createTable(
        { namespace: [SUPABASE_TEST_NS] },
        {
          name: tableName,
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [
              { id: 1, name: 'event_id', required: true, type: 'string' },
              { id: 2, name: 'event_type', required: true, type: 'string' },
              { id: 3, name: 'payload', required: false, type: 'string' },
              { id: 4, name: 'timestamp', required: true, type: 'timestamptz' },
            ],
          },
        }
      );

      // createTable returns TableMetadata directly
      expect(result['format-version']).toBe(2);
      expect(result['table-uuid']).toBeDefined();
    });

    it('should list tables', async () => {
      const tables = await catalog.listTables({ namespace: [SUPABASE_TEST_NS] });
      // listTables returns TableIdentifier[] directly
      expect(tables.some(t => t.name === tableName)).toBe(true);
    });

    it('should load table', async () => {
      const table = await catalog.loadTable({
        namespace: [SUPABASE_TEST_NS],
        name: tableName,
      });

      // loadTable returns TableMetadata directly
      expect(table['format-version']).toBe(2);
      expect(table.schemas).toHaveLength(1);
    });

    it('should check if table exists', async () => {
      const exists = await catalog.tableExists({
        namespace: [SUPABASE_TEST_NS],
        name: tableName,
      });
      expect(exists).toBe(true);

      const notExists = await catalog.tableExists({
        namespace: [SUPABASE_TEST_NS],
        name: 'nonexistent',
      });
      expect(notExists).toBe(false);
    });

    it('should update table properties', async () => {
      // First load the table to get its UUID for the requirement
      const table = await catalog.loadTable({
        namespace: [SUPABASE_TEST_NS],
        name: tableName,
      });

      // Use the proper Iceberg commit format with requirements and updates
      const result = await catalog.updateTable(
        { namespace: [SUPABASE_TEST_NS], name: tableName },
        {
          requirements: [{ type: 'assert-table-uuid', uuid: table['table-uuid'] }],
          updates: [{ action: 'set-properties', updates: { 'iceberg-js.test': 'passed' } }],
        }
      );

      expect(result.metadata.properties['iceberg-js.test']).toBe('passed');
    });
  });

  describe('Error Handling via iceberg-js', () => {
    it('should handle namespace not found', async () => {
      await expect(
        catalog.loadNamespaceMetadata({ namespace: ['nonexistent_namespace'] })
      ).rejects.toThrow();
    });

    it('should handle table not found', async () => {
      await expect(
        catalog.loadTable({ namespace: [SUPABASE_TEST_NS], name: 'nonexistent' })
      ).rejects.toThrow();
    });

    it('should handle duplicate namespace', async () => {
      await expect(
        catalog.createNamespace({ namespace: [SUPABASE_TEST_NS] })
      ).rejects.toThrow();
    });
  });
});

// ============================================================================
// E2E Tests - Interoperability
// ============================================================================

describe('E2E: Interoperability Tests', () => {
  const INTEROP_NS = `interop_${Date.now()}`;
  let catalog: IcebergRestCatalog;

  beforeAll(async () => {
    catalog = new IcebergRestCatalog({ baseUrl: ICEBERG_JS_BASE_URL });
    await deleteNamespace(INTEROP_NS);

    // Create namespace via direct HTTP
    await fetchJson('/namespaces', {
      method: 'POST',
      body: JSON.stringify({ namespace: [INTEROP_NS] }),
    });
  });

  afterAll(async () => {
    await deleteNamespace(INTEROP_NS);
  });

  it('should read namespace created via HTTP with iceberg-js', async () => {
    const metadata = await catalog.loadNamespaceMetadata({ namespace: [INTEROP_NS] });
    expect(metadata.properties).toBeDefined();
  });

  it('should read table created via HTTP with iceberg-js', async () => {
    // Create table via HTTP
    await fetchJson(`/namespaces/${INTEROP_NS}/tables`, {
      method: 'POST',
      body: JSON.stringify({
        name: 'http_created',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
        },
      }),
    });

    // Read via iceberg-js - loadTable returns TableMetadata directly
    const table = await catalog.loadTable({
      namespace: [INTEROP_NS],
      name: 'http_created',
    });

    expect(table['format-version']).toBe(2);
  });

  it('should read table created via iceberg-js with HTTP', async () => {
    // Create table via iceberg-js - takes (namespace, request) as separate params
    await catalog.createTable(
      { namespace: [INTEROP_NS] },
      {
        name: 'js_created',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'value', required: true, type: 'string' }],
        },
      }
    );

    // Read via HTTP
    const result = await fetchJson(`/namespaces/${INTEROP_NS}/tables/js_created`) as { metadata: TableMetadata };
    expect(result.metadata['format-version']).toBe(2);
  });

  it('should handle concurrent updates correctly', async () => {
    // Use unique table name to handle test retries
    const concurrentTable = `concurrent_${Date.now()}`;

    // Create a table via iceberg-js
    await catalog.createTable(
      { namespace: [INTEROP_NS] },
      {
        name: concurrentTable,
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'x', required: true, type: 'int' }],
        },
      }
    );

    // loadTable returns TableMetadata directly
    const table = await catalog.loadTable({
      namespace: [INTEROP_NS],
      name: concurrentTable,
    });

    // Try concurrent updates via iceberg-js (using proper commit format) and HTTP
    const update1 = catalog.updateTable(
      { namespace: [INTEROP_NS], name: concurrentTable },
      {
        requirements: [{ type: 'assert-table-uuid', uuid: table['table-uuid'] }],
        updates: [{ action: 'set-properties', updates: { update: '1' } }],
      }
    );

    const update2 = fetchJson(`/namespaces/${INTEROP_NS}/tables/${concurrentTable}`, {
      method: 'POST',
      body: JSON.stringify({
        requirements: [{ type: 'assert-table-uuid', uuid: table['table-uuid'] }],
        updates: [{ action: 'set-properties', updates: { update: '2' } }],
      }),
    });

    // Both might succeed or one might fail depending on timing
    // What's important is that the final state is consistent
    const results = await Promise.allSettled([update1, update2]);
    const successes = results.filter(r => r.status === 'fulfilled');
    expect(successes.length).toBeGreaterThanOrEqual(1);

    // Verify final state is valid
    const finalTable = await catalog.loadTable({
      namespace: [INTEROP_NS],
      name: concurrentTable,
    });
    expect(finalTable.properties.update).toBeDefined();
  });
});

// ============================================================================
// Types
// ============================================================================

interface TableMetadata {
  'format-version': number;
  'table-uuid': string;
  location: string;
  'last-sequence-number': number;
  'last-updated-ms': number;
  'last-column-id': number;
  'current-schema-id': number;
  schemas: Array<{
    type: 'struct';
    'schema-id': number;
    fields: Array<{ id: number; name: string; required: boolean; type: string }>;
  }>;
  'default-spec-id': number;
  'partition-specs': Array<{
    'spec-id': number;
    fields: Array<{ 'source-id': number; 'field-id': number; name: string; transform: string }>;
  }>;
  'last-partition-id': number;
  'default-sort-order-id': number;
  'sort-orders': Array<{ 'order-id': number; fields: unknown[] }>;
  properties: Record<string, string>;
  snapshots: unknown[];
  'snapshot-log': unknown[];
  'metadata-log': unknown[];
  refs: Record<string, unknown>;
}

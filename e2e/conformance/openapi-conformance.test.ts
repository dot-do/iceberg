/**
 * OpenAPI Conformance Tests for iceberg.do
 *
 * Validates API responses against the official Apache Iceberg REST Catalog
 * OpenAPI specification.
 *
 * @see https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';

// ============================================================================
// Configuration
// ============================================================================

const ICEBERG_DO_URL = process.env.ICEBERG_DO_URL || 'https://iceberg-do.dotdo.workers.dev';
const BASE_URL = `${ICEBERG_DO_URL}/v1`;

// Test namespace - unique per test run
const TEST_NS = `conformance_${Date.now()}`;

// ============================================================================
// Helpers
// ============================================================================

async function api(path: string, options: RequestInit = {}): Promise<Response> {
  return fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });
}

async function cleanup() {
  try {
    // Delete all tables in namespace
    const tablesRes = await api(`/namespaces/${TEST_NS}/tables`);
    if (tablesRes.ok) {
      const data = await tablesRes.json() as { identifiers: Array<{ name: string }> };
      for (const table of data.identifiers || []) {
        await api(`/namespaces/${TEST_NS}/tables/${table.name}`, { method: 'DELETE' });
      }
    }
    // Delete namespace
    await api(`/namespaces/${TEST_NS}`, { method: 'DELETE' });
  } catch {
    // Ignore cleanup errors
  }
}

// ============================================================================
// OpenAPI Schema Definitions (from official spec)
// ============================================================================

interface ErrorModel {
  message: string;
  type: string;
  code: number;
  stack?: string[];
}

interface IcebergErrorResponse {
  error: ErrorModel;
}

interface ConfigResponse {
  defaults?: Record<string, string>;
  overrides?: Record<string, string>;
}

interface ListNamespacesResponse {
  next_page_token?: string;
  namespaces: string[][];
}

interface GetNamespaceResponse {
  namespace: string[];
  properties?: Record<string, string>;
}

interface CreateNamespaceRequest {
  namespace: string[];
  properties?: Record<string, string>;
}

interface CreateNamespaceResponse {
  namespace: string[];
  properties?: Record<string, string>;
}

interface UpdateNamespacePropertiesRequest {
  removals?: string[];
  updates?: Record<string, string>;
}

interface UpdateNamespacePropertiesResponse {
  updated: string[];
  removed: string[];
  missing?: string[];
}

interface ListTablesResponse {
  next_page_token?: string;
  identifiers: Array<{
    namespace: string[];
    name: string;
  }>;
}

interface LoadTableResponse {
  'metadata-location': string;
  metadata: TableMetadata;
  config?: Record<string, string>;
}

interface TableMetadata {
  'format-version': 1 | 2;
  'table-uuid': string;
  location: string;
  'last-updated-ms': number;
  properties?: Record<string, string>;
  schemas: Array<{
    type: 'struct';
    'schema-id': number;
    fields: Array<{
      id: number;
      name: string;
      required: boolean;
      type: string;
    }>;
  }>;
  'current-schema-id': number;
  'partition-specs': Array<{
    'spec-id': number;
    fields: Array<{
      'source-id': number;
      'field-id': number;
      name: string;
      transform: string;
    }>;
  }>;
  'default-spec-id': number;
  'sort-orders': Array<{
    'order-id': number;
    fields: unknown[];
  }>;
  'default-sort-order-id': number;
  snapshots?: unknown[];
  refs?: Record<string, unknown>;
}

// ============================================================================
// Conformance Tests
// ============================================================================

describe('OpenAPI Conformance: Configuration', () => {
  it('GET /v1/config should return ConfigResponse', async () => {
    const res = await api('/config');
    expect(res.status).toBe(200);

    const data = await res.json() as ConfigResponse;

    // Validate structure
    expect(data).toHaveProperty('defaults');
    expect(typeof data.defaults).toBe('object');

    if (data.overrides !== undefined) {
      expect(typeof data.overrides).toBe('object');
    }
  });
});

describe('OpenAPI Conformance: Namespaces', () => {
  beforeAll(async () => {
    await cleanup();
  });

  afterAll(async () => {
    await cleanup();
  });

  it('POST /v1/namespaces should create namespace and return CreateNamespaceResponse', async () => {
    const request: CreateNamespaceRequest = {
      namespace: [TEST_NS],
      properties: { owner: 'conformance-test' },
    };

    const res = await api('/namespaces', {
      method: 'POST',
      body: JSON.stringify(request),
    });

    expect(res.status).toBe(200);

    const data = await res.json() as CreateNamespaceResponse;

    // Validate required fields
    expect(data).toHaveProperty('namespace');
    expect(Array.isArray(data.namespace)).toBe(true);
    expect(data.namespace).toEqual([TEST_NS]);

    // Validate optional fields
    if (data.properties !== undefined) {
      expect(typeof data.properties).toBe('object');
    }
  });

  it('GET /v1/namespaces should return ListNamespacesResponse', async () => {
    const res = await api('/namespaces');
    expect(res.status).toBe(200);

    const data = await res.json() as ListNamespacesResponse;

    // Validate structure
    expect(data).toHaveProperty('namespaces');
    expect(Array.isArray(data.namespaces)).toBe(true);

    // Each namespace should be an array of strings
    for (const ns of data.namespaces) {
      expect(Array.isArray(ns)).toBe(true);
      for (const part of ns) {
        expect(typeof part).toBe('string');
      }
    }

    // Our test namespace should be present
    expect(data.namespaces.some(ns => ns[0] === TEST_NS)).toBe(true);
  });

  it('GET /v1/namespaces/{namespace} should return GetNamespaceResponse', async () => {
    const res = await api(`/namespaces/${TEST_NS}`);
    expect(res.status).toBe(200);

    const data = await res.json() as GetNamespaceResponse;

    // Validate required fields
    expect(data).toHaveProperty('namespace');
    expect(Array.isArray(data.namespace)).toBe(true);
    expect(data.namespace).toEqual([TEST_NS]);

    // Validate optional fields
    if (data.properties !== undefined) {
      expect(typeof data.properties).toBe('object');
    }
  });

  it('HEAD /v1/namespaces/{namespace} should return 204 for existing namespace', async () => {
    const res = await api(`/namespaces/${TEST_NS}`, { method: 'HEAD' });
    expect(res.status).toBe(204);
  });

  it('HEAD /v1/namespaces/{namespace} should return 404 for non-existent namespace', async () => {
    const res = await api('/namespaces/nonexistent_ns_12345', { method: 'HEAD' });
    expect(res.status).toBe(404);
  });

  it('POST /v1/namespaces/{namespace}/properties should return UpdateNamespacePropertiesResponse', async () => {
    const request: UpdateNamespacePropertiesRequest = {
      updates: { updated: 'true' },
      removals: [],
    };

    const res = await api(`/namespaces/${TEST_NS}/properties`, {
      method: 'POST',
      body: JSON.stringify(request),
    });

    expect(res.status).toBe(200);

    const data = await res.json() as UpdateNamespacePropertiesResponse;

    // Validate required fields
    expect(data).toHaveProperty('updated');
    expect(data).toHaveProperty('removed');
    expect(Array.isArray(data.updated)).toBe(true);
    expect(Array.isArray(data.removed)).toBe(true);
  });

  it('POST /v1/namespaces should return 409 for duplicate namespace', async () => {
    const request: CreateNamespaceRequest = {
      namespace: [TEST_NS],
    };

    const res = await api('/namespaces', {
      method: 'POST',
      body: JSON.stringify(request),
    });

    expect(res.status).toBe(409);

    const data = await res.json() as IcebergErrorResponse;
    expect(data).toHaveProperty('error');
    expect(data.error).toHaveProperty('message');
    expect(data.error).toHaveProperty('type');
    expect(data.error).toHaveProperty('code');
    expect(data.error.code).toBe(409);
  });

  it('GET /v1/namespaces/{namespace} should return 404 for non-existent namespace', async () => {
    const res = await api('/namespaces/nonexistent_ns_12345');
    expect(res.status).toBe(404);

    const data = await res.json() as IcebergErrorResponse;
    expect(data).toHaveProperty('error');
    expect(data.error.code).toBe(404);
  });
});

describe('OpenAPI Conformance: Tables', () => {
  const tableName = 'conformance_table';

  beforeAll(async () => {
    // Ensure namespace exists
    await api('/namespaces', {
      method: 'POST',
      body: JSON.stringify({ namespace: [TEST_NS] }),
    });
  });

  afterAll(async () => {
    await cleanup();
  });

  it('POST /v1/namespaces/{ns}/tables should create table and return LoadTableResponse', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables`, {
      method: 'POST',
      body: JSON.stringify({
        name: tableName,
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            { id: 2, name: 'data', required: false, type: 'string' },
          ],
        },
      }),
    });

    expect(res.status).toBe(200);

    const data = await res.json() as LoadTableResponse;

    // Validate required fields
    expect(data).toHaveProperty('metadata-location');
    expect(typeof data['metadata-location']).toBe('string');

    expect(data).toHaveProperty('metadata');
    const metadata = data.metadata;

    // Validate metadata structure
    expect(metadata['format-version']).toBe(2);
    expect(metadata['table-uuid']).toBeDefined();
    expect(typeof metadata['table-uuid']).toBe('string');
    expect(metadata.location).toBeDefined();
    expect(metadata['last-updated-ms']).toBeDefined();
    expect(typeof metadata['last-updated-ms']).toBe('number');

    // Validate schemas
    expect(Array.isArray(metadata.schemas)).toBe(true);
    expect(metadata.schemas.length).toBeGreaterThan(0);
    expect(metadata['current-schema-id']).toBeDefined();

    // Validate partition specs
    expect(Array.isArray(metadata['partition-specs'])).toBe(true);
    expect(metadata['default-spec-id']).toBeDefined();

    // Validate sort orders
    expect(Array.isArray(metadata['sort-orders'])).toBe(true);
    expect(metadata['default-sort-order-id']).toBeDefined();
  });

  it('GET /v1/namespaces/{ns}/tables should return ListTablesResponse', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables`);
    expect(res.status).toBe(200);

    const data = await res.json() as ListTablesResponse;

    // Validate structure
    expect(data).toHaveProperty('identifiers');
    expect(Array.isArray(data.identifiers)).toBe(true);

    // Each identifier should have namespace and name
    for (const id of data.identifiers) {
      expect(id).toHaveProperty('namespace');
      expect(id).toHaveProperty('name');
      expect(Array.isArray(id.namespace)).toBe(true);
      expect(typeof id.name).toBe('string');
    }

    // Our table should be present
    expect(data.identifiers.some(t => t.name === tableName)).toBe(true);
  });

  it('GET /v1/namespaces/{ns}/tables/{table} should return LoadTableResponse', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables/${tableName}`);
    expect(res.status).toBe(200);

    const data = await res.json() as LoadTableResponse;

    // Validate structure matches create response
    expect(data).toHaveProperty('metadata-location');
    expect(data).toHaveProperty('metadata');
    expect(data.metadata['format-version']).toBe(2);
    expect(data.metadata['table-uuid']).toBeDefined();
  });

  it('HEAD /v1/namespaces/{ns}/tables/{table} should return 204 for existing table', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables/${tableName}`, { method: 'HEAD' });
    expect(res.status).toBe(204);
  });

  it('HEAD /v1/namespaces/{ns}/tables/{table} should return 404 for non-existent table', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables/nonexistent_table`, { method: 'HEAD' });
    expect(res.status).toBe(404);
  });

  it('POST /v1/namespaces/{ns}/tables/{table} should commit updates', async () => {
    // First load the table to get UUID
    const loadRes = await api(`/namespaces/${TEST_NS}/tables/${tableName}`);
    const loadData = await loadRes.json() as LoadTableResponse;
    const tableUuid = loadData.metadata['table-uuid'];

    // Commit property update
    const res = await api(`/namespaces/${TEST_NS}/tables/${tableName}`, {
      method: 'POST',
      body: JSON.stringify({
        requirements: [{ type: 'assert-table-uuid', uuid: tableUuid }],
        updates: [{ action: 'set-properties', updates: { conformance: 'passed' } }],
      }),
    });

    expect(res.status).toBe(200);

    const data = await res.json() as LoadTableResponse;
    expect(data.metadata.properties?.conformance).toBe('passed');
  });

  it('POST /v1/namespaces/{ns}/tables/{table} should return 409 for UUID mismatch', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables/${tableName}`, {
      method: 'POST',
      body: JSON.stringify({
        requirements: [{ type: 'assert-table-uuid', uuid: '00000000-0000-0000-0000-000000000000' }],
        updates: [{ action: 'set-properties', updates: { test: 'value' } }],
      }),
    });

    expect(res.status).toBe(409);

    const data = await res.json() as IcebergErrorResponse;
    expect(data.error.code).toBe(409);
  });

  it('POST /v1/tables/rename should rename table', async () => {
    // Use unique names to handle test retries
    const srcName = `to_rename_${Date.now()}`;
    const dstName = `renamed_${Date.now()}`;

    // Create a table to rename
    await api(`/namespaces/${TEST_NS}/tables`, {
      method: 'POST',
      body: JSON.stringify({
        name: srcName,
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'x', required: true, type: 'int' }],
        },
      }),
    });

    const res = await api('/tables/rename', {
      method: 'POST',
      body: JSON.stringify({
        source: { namespace: [TEST_NS], name: srcName },
        destination: { namespace: [TEST_NS], name: dstName },
      }),
    });

    expect(res.status).toBe(204);

    // Verify rename
    const checkOld = await api(`/namespaces/${TEST_NS}/tables/${srcName}`, { method: 'HEAD' });
    expect(checkOld.status).toBe(404);

    const checkNew = await api(`/namespaces/${TEST_NS}/tables/${dstName}`, { method: 'HEAD' });
    expect(checkNew.status).toBe(204);
  });

  it('DELETE /v1/namespaces/{ns}/tables/{table} should drop table', async () => {
    // Create a table to drop
    await api(`/namespaces/${TEST_NS}/tables`, {
      method: 'POST',
      body: JSON.stringify({
        name: 'to_drop',
        schema: {
          type: 'struct',
          'schema-id': 0,
          fields: [{ id: 1, name: 'x', required: true, type: 'int' }],
        },
      }),
    });

    const res = await api(`/namespaces/${TEST_NS}/tables/to_drop`, { method: 'DELETE' });
    expect(res.status).toBe(204);

    // Verify dropped
    const check = await api(`/namespaces/${TEST_NS}/tables/to_drop`, { method: 'HEAD' });
    expect(check.status).toBe(404);
  });

  it('GET /v1/namespaces/{ns}/tables/{table} should return 404 for non-existent table', async () => {
    const res = await api(`/namespaces/${TEST_NS}/tables/nonexistent_table_12345`);
    expect(res.status).toBe(404);

    const data = await res.json() as IcebergErrorResponse;
    expect(data.error.code).toBe(404);
  });
});

describe('OpenAPI Conformance: Namespace Deletion', () => {
  const deleteNs = `delete_test_${Date.now()}`;

  it('DELETE /v1/namespaces/{namespace} should drop empty namespace', async () => {
    // Create namespace
    await api('/namespaces', {
      method: 'POST',
      body: JSON.stringify({ namespace: [deleteNs] }),
    });

    // Delete it
    const res = await api(`/namespaces/${deleteNs}`, { method: 'DELETE' });
    expect(res.status).toBe(204);

    // Verify deleted
    const check = await api(`/namespaces/${deleteNs}`, { method: 'HEAD' });
    expect(check.status).toBe(404);
  });

  it('DELETE /v1/namespaces/{namespace} should return 404 for non-existent namespace', async () => {
    const res = await api('/namespaces/nonexistent_ns_99999', { method: 'DELETE' });
    expect(res.status).toBe(404);
  });
});

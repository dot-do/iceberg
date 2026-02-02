/**
 * Tests for Iceberg REST Catalog Routes
 *
 * Tests the REST API endpoints per the Iceberg REST Catalog specification.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { createIcebergRoutes } from '../src/routes.js';
import type { AuthContext } from '../src/auth/middleware.js';
import type { AuthorizationContext } from '../src/auth/authorization-middleware.js';
import { InMemoryPermissionStore } from '../src/auth/rbac.js';
import { createFGAEngine } from '../src/auth/fga.js';

// ============================================================================
// Mock Environment Setup
// ============================================================================

interface MockEnv {
  CATALOG: {
    idFromName: (name: string) => { toString: () => string };
    get: (id: { toString: () => string }) => MockDurableObjectStub;
  };
  R2_BUCKET?: MockR2Bucket;
  ENVIRONMENT: string;
}

interface MockDurableObjectStub {
  fetch: (request: Request) => Promise<Response>;
}

interface MockR2Bucket {
  put: (key: string, value: string, options?: { httpMetadata?: { contentType: string } }) => Promise<void>;
  get: (key: string) => Promise<{ json: () => Promise<unknown> } | null>;
  list: (options?: { prefix?: string }) => Promise<{ objects: Array<{ key: string }> }>;
  delete: (keys: string[]) => Promise<void>;
}

// In-memory storage for mock DO
const mockNamespaces = new Map<string, Record<string, string>>();
const mockTables = new Map<string, {
  location: string;
  metadataLocation: string;
  metadata?: unknown;
  properties: Record<string, string>;
  version: number;
}>();

// Reset mock storage
function resetMockStorage(): void {
  mockNamespaces.clear();
  mockTables.clear();
}

// Mock DO fetch handler
async function mockDOFetch(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    // GET /namespaces
    if (method === 'GET' && path === '/namespaces') {
      const namespaces = Array.from(mockNamespaces.keys()).map(k => k.split('\x1f'));
      return Response.json({ namespaces });
    }

    // POST /namespaces
    if (method === 'POST' && path === '/namespaces') {
      const body = await request.json() as { namespace: string[]; properties?: Record<string, string> };
      const key = body.namespace.join('\x1f');
      if (mockNamespaces.has(key)) {
        return Response.json({ error: 'UNIQUE constraint failed' }, { status: 409 });
      }
      mockNamespaces.set(key, body.properties ?? {});
      return Response.json({ namespace: body.namespace, properties: body.properties ?? {} });
    }

    // GET /namespaces/{namespace}
    const getNamespaceMatch = path.match(/^\/namespaces\/([^/]+)$/);
    if (method === 'GET' && getNamespaceMatch) {
      const namespaceKey = decodeURIComponent(getNamespaceMatch[1]);
      const properties = mockNamespaces.get(namespaceKey);
      if (!properties) {
        return Response.json({ error: 'Namespace does not exist' }, { status: 404 });
      }
      return Response.json({ properties });
    }

    // DELETE /namespaces/{namespace}
    const deleteNamespaceMatch = path.match(/^\/namespaces\/([^/]+)$/);
    if (method === 'DELETE' && deleteNamespaceMatch) {
      const namespaceKey = decodeURIComponent(deleteNamespaceMatch[1]);
      if (!mockNamespaces.has(namespaceKey)) {
        return Response.json({ error: 'Namespace does not exist' }, { status: 404 });
      }
      // Check if namespace has tables
      for (const tableKey of mockTables.keys()) {
        if (tableKey.startsWith(namespaceKey + '\x1f')) {
          return Response.json({ error: 'Namespace is not empty' }, { status: 409 });
        }
      }
      mockNamespaces.delete(namespaceKey);
      return new Response(null, { status: 204 });
    }

    // POST /namespaces/{namespace}/properties
    const updatePropsMatch = path.match(/^\/namespaces\/([^/]+)\/properties$/);
    if (method === 'POST' && updatePropsMatch) {
      const namespaceKey = decodeURIComponent(updatePropsMatch[1]);
      const properties = mockNamespaces.get(namespaceKey);
      if (!properties) {
        return Response.json({ error: 'Namespace does not exist' }, { status: 404 });
      }
      const body = await request.json() as { updates?: Record<string, string>; removals?: string[] };
      const updated = Object.keys(body.updates ?? {});
      const removed: string[] = [];
      const missing: string[] = [];
      for (const key of body.removals ?? []) {
        if (key in properties) {
          delete properties[key];
          removed.push(key);
        } else {
          missing.push(key);
        }
      }
      for (const [key, value] of Object.entries(body.updates ?? {})) {
        properties[key] = value;
      }
      mockNamespaces.set(namespaceKey, properties);
      return Response.json({ updated, removed, missing });
    }

    // GET /namespaces/{namespace}/tables
    const listTablesMatch = path.match(/^\/namespaces\/([^/]+)\/tables$/);
    if (method === 'GET' && listTablesMatch) {
      const namespaceKey = decodeURIComponent(listTablesMatch[1]);
      if (!mockNamespaces.has(namespaceKey)) {
        return Response.json({ error: 'Namespace does not exist' }, { status: 404 });
      }
      const tables: Array<{ namespace: string[]; name: string }> = [];
      for (const tableKey of mockTables.keys()) {
        const [ns, name] = tableKey.split('\x1f\x00');
        if (ns === namespaceKey) {
          tables.push({ namespace: ns.split('\x1f'), name });
        }
      }
      return Response.json({ tables });
    }

    // POST /namespaces/{namespace}/tables
    const createTableMatch = path.match(/^\/namespaces\/([^/]+)\/tables$/);
    if (method === 'POST' && createTableMatch) {
      const namespaceKey = decodeURIComponent(createTableMatch[1]);
      if (!mockNamespaces.has(namespaceKey)) {
        return Response.json({ error: 'Namespace does not exist' }, { status: 404 });
      }
      const body = await request.json() as {
        name: string;
        location: string;
        metadataLocation: string;
        metadata?: unknown;
        properties?: Record<string, string>;
      };
      const tableKey = namespaceKey + '\x1f\x00' + body.name;
      if (mockTables.has(tableKey)) {
        return Response.json({ error: 'UNIQUE constraint failed' }, { status: 409 });
      }
      mockTables.set(tableKey, {
        location: body.location,
        metadataLocation: body.metadataLocation,
        metadata: body.metadata,
        properties: body.properties ?? {},
        version: 1,
      });
      return Response.json({ created: true });
    }

    // GET /namespaces/{namespace}/tables/{table}
    const getTableMatch = path.match(/^\/namespaces\/([^/]+)\/tables\/([^/]+)$/);
    if (method === 'GET' && getTableMatch) {
      const namespaceKey = decodeURIComponent(getTableMatch[1]);
      const tableName = decodeURIComponent(getTableMatch[2]);
      const tableKey = namespaceKey + '\x1f\x00' + tableName;
      const table = mockTables.get(tableKey);
      if (!table) {
        return Response.json({ error: 'Table does not exist' }, { status: 404 });
      }
      // Include version in response for OCC
      return Response.json({
        location: table.location,
        metadataLocation: table.metadataLocation,
        metadata: table.metadata,
        properties: table.properties,
        version: table.version,
      });
    }

    // DELETE /namespaces/{namespace}/tables/{table}
    const deleteTableMatch = path.match(/^\/namespaces\/([^/]+)\/tables\/([^/]+)$/);
    if (method === 'DELETE' && deleteTableMatch) {
      const namespaceKey = decodeURIComponent(deleteTableMatch[1]);
      const tableName = decodeURIComponent(deleteTableMatch[2]);
      const tableKey = namespaceKey + '\x1f\x00' + tableName;
      if (!mockTables.has(tableKey)) {
        return Response.json({ error: 'Table does not exist' }, { status: 404 });
      }
      mockTables.delete(tableKey);
      return new Response(null, { status: 204 });
    }

    // POST /namespaces/{namespace}/tables/{table}/commit
    const commitTableMatch = path.match(/^\/namespaces\/([^/]+)\/tables\/([^/]+)\/commit$/);
    if (method === 'POST' && commitTableMatch) {
      const namespaceKey = decodeURIComponent(commitTableMatch[1]);
      const tableName = decodeURIComponent(commitTableMatch[2]);
      const tableKey = namespaceKey + '\x1f\x00' + tableName;
      const table = mockTables.get(tableKey);
      if (!table) {
        return Response.json({ error: 'Table does not exist' }, { status: 404 });
      }
      const body = await request.json() as { metadataLocation: string; metadata?: unknown; expectedVersion?: number };
      // Check OCC - if expectedVersion is provided, verify it matches
      if (body.expectedVersion !== undefined && body.expectedVersion !== table.version) {
        return Response.json(
          { error: 'Concurrent modification detected', code: 'CONFLICT' },
          { status: 409 }
        );
      }
      table.metadataLocation = body.metadataLocation;
      if (body.metadata) {
        table.metadata = body.metadata;
      }
      table.version = table.version + 1;
      mockTables.set(tableKey, table);
      return Response.json({ committed: true, version: table.version });
    }

    // POST /tables/rename
    if (method === 'POST' && path === '/tables/rename') {
      const body = await request.json() as {
        fromNamespace: string[];
        fromName: string;
        toNamespace: string[];
        toName: string;
      };
      const fromKey = body.fromNamespace.join('\x1f') + '\x1f\x00' + body.fromName;
      const toKey = body.toNamespace.join('\x1f') + '\x1f\x00' + body.toName;
      const table = mockTables.get(fromKey);
      if (!table) {
        return Response.json({ error: 'Table does not exist' }, { status: 404 });
      }
      if (!mockNamespaces.has(body.toNamespace.join('\x1f'))) {
        return Response.json({ error: 'Destination namespace does not exist' }, { status: 404 });
      }
      if (mockTables.has(toKey)) {
        return Response.json({ error: 'Table already exists' }, { status: 409 });
      }
      mockTables.delete(fromKey);
      mockTables.set(toKey, table);
      return new Response(null, { status: 204 });
    }

    return new Response('Not Found', { status: 404 });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : 'Unknown error' }, { status: 500 });
  }
}

// Mock variables type
interface MockVariables {
  catalogStub: { fetch: (request: Request) => Promise<Response> };
  auth: AuthContext;
  authorization: AuthorizationContext;
}

// Create mock environment
function createMockEnv(): MockEnv {
  return {
    CATALOG: {
      idFromName: (name: string) => ({ toString: () => `mock-id-${name}` }),
      get: (_id: { toString: () => string }) => ({
        fetch: mockDOFetch,
      }),
    },
    ENVIRONMENT: 'test',
  };
}

// Create mock auth context with admin role for tests
function createMockAuthContext(): AuthContext {
  return {
    authenticated: true,
    userId: 'test-user-id',
    email: 'test@example.com',
    roles: ['admin'], // Admin role grants full access
    organizationId: 'test-org',
  };
}

// Create mock authorization context
function createMockAuthorizationContext(): AuthorizationContext {
  const store = new InMemoryPermissionStore();
  const fga = createFGAEngine(store);
  return { fga, store };
}

// Create test app
function createTestApp(): Hono<{ Bindings: MockEnv; Variables: MockVariables }> {
  const app = new Hono<{ Bindings: MockEnv; Variables: MockVariables }>();

  // Middleware to set up the catalog stub and auth context (simulating what index.ts does)
  app.use('/*', async (c, next) => {
    c.set('catalogStub', { fetch: mockDOFetch });
    c.set('auth', createMockAuthContext());
    c.set('authorization', createMockAuthorizationContext());
    return next();
  });

  app.route('/v1', createIcebergRoutes() as Hono<{ Bindings: MockEnv; Variables: MockVariables }>);
  return app;
}

// ============================================================================
// Tests
// ============================================================================

describe('Iceberg REST Catalog Routes', () => {
  let app: Hono<{ Bindings: MockEnv; Variables: MockVariables }>;
  let env: MockEnv;

  beforeEach(() => {
    resetMockStorage();
    app = createTestApp();
    env = createMockEnv();
  });

  // Helper to make requests
  async function request(method: string, path: string, body?: unknown): Promise<Response> {
    const req = new Request(`http://test${path}`, {
      method,
      headers: body ? { 'Content-Type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
    return app.fetch(req, env);
  }

  // =========================================================================
  // GET /v1/config
  // =========================================================================
  describe('GET /v1/config', () => {
    it('should return catalog configuration', async () => {
      const res = await request('GET', '/v1/config');
      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data).toHaveProperty('defaults');
      expect(data).toHaveProperty('overrides');
    });
  });

  // =========================================================================
  // Namespace Operations
  // =========================================================================
  describe('Namespace Operations', () => {
    describe('GET /v1/namespaces', () => {
      it('should return empty list when no namespaces exist', async () => {
        const res = await request('GET', '/v1/namespaces');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toEqual([]);
      });

      it('should list created namespaces', async () => {
        // Create a namespace first
        await request('POST', '/v1/namespaces', {
          namespace: ['test_db'],
          properties: {},
        });

        const res = await request('GET', '/v1/namespaces');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toContainEqual(['test_db']);
      });

      it('should only list top-level namespaces when no parent specified', async () => {
        // Create nested namespaces
        await request('POST', '/v1/namespaces', { namespace: ['db'] });
        await request('POST', '/v1/namespaces', { namespace: ['db', 'schema'] });
        await request('POST', '/v1/namespaces', { namespace: ['db', 'schema', 'subschema'] });
        await request('POST', '/v1/namespaces', { namespace: ['other_db'] });

        // List without parent should return only top-level
        const res = await request('GET', '/v1/namespaces');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toContainEqual(['db']);
        expect(data.namespaces).toContainEqual(['other_db']);
        // Should NOT contain nested namespaces
        expect(data.namespaces).not.toContainEqual(['db', 'schema']);
        expect(data.namespaces).not.toContainEqual(['db', 'schema', 'subschema']);
      });

      it('should list direct children when parent is specified', async () => {
        // Create nested namespaces
        await request('POST', '/v1/namespaces', { namespace: ['parent_ns'] });
        await request('POST', '/v1/namespaces', { namespace: ['parent_ns', 'child1'] });
        await request('POST', '/v1/namespaces', { namespace: ['parent_ns', 'child2'] });
        await request('POST', '/v1/namespaces', { namespace: ['parent_ns', 'child1', 'grandchild'] });

        // List with parent should return only direct children
        const res = await request('GET', '/v1/namespaces?parent=parent_ns');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toContainEqual(['parent_ns', 'child1']);
        expect(data.namespaces).toContainEqual(['parent_ns', 'child2']);
        expect(data.namespaces).toHaveLength(2);
        // Should NOT contain grandchildren
        expect(data.namespaces).not.toContainEqual(['parent_ns', 'child1', 'grandchild']);
      });

      it('should list grandchildren when nested parent is specified', async () => {
        // Create deeply nested namespaces
        await request('POST', '/v1/namespaces', { namespace: ['a'] });
        await request('POST', '/v1/namespaces', { namespace: ['a', 'b'] });
        await request('POST', '/v1/namespaces', { namespace: ['a', 'b', 'c'] });
        await request('POST', '/v1/namespaces', { namespace: ['a', 'b', 'd'] });
        await request('POST', '/v1/namespaces', { namespace: ['a', 'b', 'c', 'e'] });

        // List children of a%1Fb (using unit separator)
        const res = await request('GET', '/v1/namespaces?parent=a%1Fb');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toContainEqual(['a', 'b', 'c']);
        expect(data.namespaces).toContainEqual(['a', 'b', 'd']);
        expect(data.namespaces).toHaveLength(2);
        // Should NOT contain deeper levels
        expect(data.namespaces).not.toContainEqual(['a', 'b', 'c', 'e']);
      });

      it('should return empty list when parent has no children', async () => {
        await request('POST', '/v1/namespaces', { namespace: ['leaf_ns'] });

        const res = await request('GET', '/v1/namespaces?parent=leaf_ns');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespaces).toEqual([]);
      });
    });

    describe('POST /v1/namespaces', () => {
      it('should create a namespace', async () => {
        const res = await request('POST', '/v1/namespaces', {
          namespace: ['my_db'],
          properties: { owner: 'test_user' },
        });
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespace).toEqual(['my_db']);
        expect(data.properties).toEqual({ owner: 'test_user' });
      });

      it('should return 409 for duplicate namespace', async () => {
        await request('POST', '/v1/namespaces', { namespace: ['my_db'] });
        const res = await request('POST', '/v1/namespaces', { namespace: ['my_db'] });
        expect(res.status).toBe(409);
        const data = await res.json();
        expect(data.error.type).toBe('AlreadyExistsException');
      });

      it('should return 400 for missing namespace', async () => {
        const res = await request('POST', '/v1/namespaces', {});
        expect(res.status).toBe(400);
      });
    });

    describe('GET /v1/namespaces/{namespace}', () => {
      it('should get namespace metadata', async () => {
        await request('POST', '/v1/namespaces', {
          namespace: ['test_db'],
          properties: { location: 's3://bucket/test_db' },
        });

        const res = await request('GET', '/v1/namespaces/test_db');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.namespace).toEqual(['test_db']);
        expect(data.properties).toEqual({ location: 's3://bucket/test_db' });
      });

      it('should return 404 for non-existent namespace', async () => {
        const res = await request('GET', '/v1/namespaces/nonexistent');
        expect(res.status).toBe(404);
        const data = await res.json();
        expect(data.error.type).toBe('NoSuchNamespaceException');
      });
    });

    describe('DELETE /v1/namespaces/{namespace}', () => {
      it('should delete an empty namespace', async () => {
        await request('POST', '/v1/namespaces', { namespace: ['to_delete'] });
        const res = await request('DELETE', '/v1/namespaces/to_delete');
        expect(res.status).toBe(204);
      });

      it('should return 404 for non-existent namespace', async () => {
        const res = await request('DELETE', '/v1/namespaces/nonexistent');
        expect(res.status).toBe(404);
      });

      it('should return 409 for non-empty namespace', async () => {
        // Create namespace and table
        await request('POST', '/v1/namespaces', { namespace: ['non_empty'] });
        await request('POST', '/v1/namespaces/non_empty/tables', {
          name: 'test_table',
          schema: { type: 'struct', fields: [] },
        });

        const res = await request('DELETE', '/v1/namespaces/non_empty');
        expect(res.status).toBe(409);
        const data = await res.json();
        expect(data.error.type).toBe('NamespaceNotEmptyException');
      });
    });
  });

  // =========================================================================
  // Table Operations
  // =========================================================================
  describe('Table Operations', () => {
    beforeEach(async () => {
      // Create a namespace for table tests
      await request('POST', '/v1/namespaces', { namespace: ['test_db'] });
    });

    describe('GET /v1/namespaces/{namespace}/tables', () => {
      it('should return empty list when no tables exist', async () => {
        const res = await request('GET', '/v1/namespaces/test_db/tables');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.identifiers).toEqual([]);
      });

      it('should return 404 for non-existent namespace', async () => {
        const res = await request('GET', '/v1/namespaces/nonexistent/tables');
        expect(res.status).toBe(404);
      });
    });

    describe('POST /v1/namespaces/{namespace}/tables', () => {
      it('should create a table', async () => {
        const res = await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'users',
          schema: {
            type: 'struct',
            fields: [
              { id: 1, name: 'id', required: true, type: 'long' },
              { id: 2, name: 'name', required: true, type: 'string' },
            ],
          },
        });
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data['metadata-location']).toContain('users');
        expect(data.metadata['format-version']).toBe(2);
        expect(data.metadata['table-uuid']).toBeDefined();
      });

      it('should return 400 for missing name', async () => {
        const res = await request('POST', '/v1/namespaces/test_db/tables', {
          schema: { type: 'struct', fields: [] },
        });
        expect(res.status).toBe(400);
      });

      it('should return 400 for missing schema', async () => {
        const res = await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'test_table',
        });
        expect(res.status).toBe(400);
      });

      it('should return 404 for non-existent namespace', async () => {
        const res = await request('POST', '/v1/namespaces/nonexistent/tables', {
          name: 'test_table',
          schema: { type: 'struct', fields: [] },
        });
        expect(res.status).toBe(404);
      });
    });

    describe('GET /v1/namespaces/{namespace}/tables/{table}', () => {
      it('should load a table', async () => {
        // Create table first
        await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'users',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
          },
        });

        const res = await request('GET', '/v1/namespaces/test_db/tables/users');
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data['metadata-location']).toContain('users');
        expect(data.metadata).toBeDefined();
      });

      it('should return 404 for non-existent table', async () => {
        const res = await request('GET', '/v1/namespaces/test_db/tables/nonexistent');
        expect(res.status).toBe(404);
        const data = await res.json();
        expect(data.error.type).toBe('NoSuchTableException');
      });
    });

    describe('DELETE /v1/namespaces/{namespace}/tables/{table}', () => {
      it('should drop a table', async () => {
        // Create table first
        await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'to_drop',
          schema: { type: 'struct', fields: [] },
        });

        const res = await request('DELETE', '/v1/namespaces/test_db/tables/to_drop');
        expect(res.status).toBe(204);

        // Verify table is gone
        const getRes = await request('GET', '/v1/namespaces/test_db/tables/to_drop');
        expect(getRes.status).toBe(404);
      });

      it('should return 404 for non-existent table', async () => {
        const res = await request('DELETE', '/v1/namespaces/test_db/tables/nonexistent');
        expect(res.status).toBe(404);
      });
    });

    describe('POST /v1/namespaces/{namespace}/tables/{table} (commit)', () => {
      it('should commit table changes', async () => {
        // Create table first
        const createRes = await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'events',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
          },
        });
        const createData = await createRes.json();

        // Commit changes (add a snapshot)
        const res = await request('POST', '/v1/namespaces/test_db/tables/events', {
          requirements: [
            { type: 'assert-table-uuid', uuid: createData.metadata['table-uuid'] },
          ],
          updates: [
            {
              action: 'add-snapshot',
              snapshot: {
                'snapshot-id': Date.now(),
                'sequence-number': 1,
                'timestamp-ms': Date.now(),
                'manifest-list': 's3://bucket/test_db/events/metadata/snap-1.avro',
                summary: { operation: 'append' },
              },
            },
          ],
        });
        expect(res.status).toBe(200);
        const data = await res.json();
        expect(data.metadata.snapshots).toHaveLength(1);
      });

      it('should handle full append flow with set-snapshot-ref', async () => {
        // This test mirrors what RCK testAppend does:
        // 1. Create table
        // 2. Assert main branch is null (no snapshot)
        // 3. Add snapshot
        // 4. Set main branch to new snapshot

        // Create table
        const createRes = await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'append_test',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
          },
        });
        const createData = await createRes.json();
        const tableUuid = createData.metadata['table-uuid'];

        // First append: assert main is null, add snapshot, set main ref
        const snapshotId1 = Date.now();
        const res1 = await request('POST', '/v1/namespaces/test_db/tables/append_test', {
          requirements: [
            { type: 'assert-table-uuid', uuid: tableUuid },
            { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': null },
          ],
          updates: [
            {
              action: 'add-snapshot',
              snapshot: {
                'snapshot-id': snapshotId1,
                'sequence-number': 1,
                'timestamp-ms': snapshotId1,
                'manifest-list': 's3://bucket/test_db/append_test/metadata/snap-1.avro',
                summary: { operation: 'append' },
              },
            },
            {
              action: 'set-snapshot-ref',
              'ref-name': 'main',
              type: 'branch',
              'snapshot-id': snapshotId1,
            },
          ],
        });
        expect(res1.status).toBe(200);
        const data1 = await res1.json();
        expect(data1.metadata.snapshots).toHaveLength(1);
        expect(data1.metadata['current-snapshot-id']).toBe(snapshotId1);
        expect(data1.metadata.refs?.main?.['snapshot-id']).toBe(snapshotId1);
        expect(data1.metadata['snapshot-log']).toHaveLength(1);

        // Second append: assert main equals first snapshot, add new snapshot, update main
        const snapshotId2 = Date.now() + 1;
        const res2 = await request('POST', '/v1/namespaces/test_db/tables/append_test', {
          requirements: [
            { type: 'assert-table-uuid', uuid: tableUuid },
            { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': snapshotId1 },
          ],
          updates: [
            {
              action: 'add-snapshot',
              snapshot: {
                'snapshot-id': snapshotId2,
                'parent-snapshot-id': snapshotId1,
                'sequence-number': 2,
                'timestamp-ms': snapshotId2,
                'manifest-list': 's3://bucket/test_db/append_test/metadata/snap-2.avro',
                summary: { operation: 'append' },
              },
            },
            {
              action: 'set-snapshot-ref',
              'ref-name': 'main',
              type: 'branch',
              'snapshot-id': snapshotId2,
            },
          ],
        });
        expect(res2.status).toBe(200);
        const data2 = await res2.json();
        expect(data2.metadata.snapshots).toHaveLength(2);
        expect(data2.metadata['current-snapshot-id']).toBe(snapshotId2);
        expect(data2.metadata.refs?.main?.['snapshot-id']).toBe(snapshotId2);
        expect(data2.metadata['snapshot-log']).toHaveLength(2);
        expect(data2.metadata['last-sequence-number']).toBe(2);
      });

      it('should return 404 for non-existent table', async () => {
        const res = await request('POST', '/v1/namespaces/test_db/tables/nonexistent', {
          requirements: [],
          updates: [],
        });
        expect(res.status).toBe(404);
      });

      it('should return 409 when concurrent modification occurs (OCC)', async () => {
        // Create table - starts at version 1
        await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'concurrent_test',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
          },
        });

        // Get the initial version from the mock directly (routes don't return version)
        const tableKey = 'test_db\x1f\x00concurrent_test';
        const tableBefore = mockTables.get(tableKey);
        const staleVersion = tableBefore?.version ?? 1; // Should be 1

        // Simulate concurrent modification - another process commits first
        // This increments the version in the mock storage
        const table = mockTables.get(tableKey);
        if (table) {
          table.version = table.version + 1; // Now version 2
          mockTables.set(tableKey, table);
        }

        // Our commit should fail because we're trying to commit with stale version
        // but the table has been modified by another concurrent operation
        // Test directly against mock DO to verify OCC works
        const commitRes = await mockDOFetch(new Request('http://internal/namespaces/test_db/tables/concurrent_test/commit', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            metadataLocation: 's3://bucket/test_db/concurrent_test/metadata/new.json',
            metadata: {},
            expectedVersion: staleVersion, // Stale version (1, but table is now at 2)
          }),
        }));

        expect(commitRes.status).toBe(409);
        const data = await commitRes.json() as { error: string; code?: string };
        expect(data.code).toBe('CONFLICT');
      });

      it('should succeed with server-side retry when requirements are stale but updates can be rebased', async () => {
        // Create table with initial schema
        const createRes = await request('POST', '/v1/namespaces/test_db/tables', {
          name: 'retry_test',
          schema: {
            type: 'struct',
            'schema-id': 0,
            fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
          },
        });
        const createData = await createRes.json();
        const originalSchemaId = createData.metadata['current-schema-id'];

        // First client adds a new sort order (simulating concurrent modification)
        const firstUpdateRes = await request('POST', '/v1/namespaces/test_db/tables/retry_test', {
          requirements: [
            { type: 'assert-table-uuid', uuid: createData.metadata['table-uuid'] },
            { type: 'assert-current-schema-id', 'current-schema-id': originalSchemaId },
          ],
          updates: [
            {
              action: 'add-sort-order',
              'sort-order': {
                'order-id': 1,
                fields: [],
              },
            },
            { action: 'set-default-sort-order', 'sort-order-id': 1 },
          ],
        });
        expect(firstUpdateRes.status).toBe(200);

        // Get the updated table state
        const loadRes = await request('GET', '/v1/namespaces/test_db/tables/retry_test');
        const loadData = await loadRes.json();
        const newSortOrderId = loadData.metadata['default-sort-order-id'];
        expect(newSortOrderId).toBe(1);

        // Second client tries to add a schema with stale requirements
        // (requirements based on original state, but table has been modified)
        // Server-side retry should detect the stale requirements and rebase the update
        const retryRes = await request('POST', '/v1/namespaces/test_db/tables/retry_test', {
          requirements: [
            { type: 'assert-table-uuid', uuid: createData.metadata['table-uuid'] },
            // This is stale - sort order was 0 when we read, but now it's 1
            { type: 'assert-default-sort-order-id', 'default-sort-order-id': 0 },
          ],
          updates: [
            {
              action: 'add-schema',
              schema: {
                type: 'struct',
                'schema-id': -1, // Auto-assign
                fields: [
                  { id: 1, name: 'id', required: true, type: 'long' },
                  { id: 2, name: 'name', required: false, type: 'string' },
                ],
              },
            },
          ],
        });

        // Server-side retry should succeed by updating requirements and rebasing the schema update
        expect(retryRes.status).toBe(200);
        const retryData = await retryRes.json();
        // The new schema should have been added
        expect(retryData.metadata.schemas.length).toBe(2);
      });
    });
  });

  // =========================================================================
  // Table Rename
  // =========================================================================
  describe('POST /v1/tables/rename', () => {
    beforeEach(async () => {
      await request('POST', '/v1/namespaces', { namespace: ['source_db'] });
      await request('POST', '/v1/namespaces', { namespace: ['dest_db'] });
      await request('POST', '/v1/namespaces/source_db/tables', {
        name: 'old_table',
        schema: { type: 'struct', fields: [] },
      });
    });

    it('should rename a table within the same namespace', async () => {
      const res = await request('POST', '/v1/tables/rename', {
        source: { namespace: ['source_db'], name: 'old_table' },
        destination: { namespace: ['source_db'], name: 'new_table' },
      });
      expect(res.status).toBe(204);

      // Verify old table is gone and new exists
      const oldRes = await request('GET', '/v1/namespaces/source_db/tables/old_table');
      expect(oldRes.status).toBe(404);
      const newRes = await request('GET', '/v1/namespaces/source_db/tables/new_table');
      expect(newRes.status).toBe(200);
    });

    it('should rename a table across namespaces', async () => {
      const res = await request('POST', '/v1/tables/rename', {
        source: { namespace: ['source_db'], name: 'old_table' },
        destination: { namespace: ['dest_db'], name: 'moved_table' },
      });
      expect(res.status).toBe(204);
    });

    it('should return 404 for non-existent source table', async () => {
      const res = await request('POST', '/v1/tables/rename', {
        source: { namespace: ['source_db'], name: 'nonexistent' },
        destination: { namespace: ['dest_db'], name: 'new_table' },
      });
      expect(res.status).toBe(404);
    });

    it('should return 409 when destination table already exists', async () => {
      // Create destination table
      await request('POST', '/v1/namespaces/dest_db/tables', {
        name: 'existing_table',
        schema: { type: 'struct', fields: [] },
      });

      const res = await request('POST', '/v1/tables/rename', {
        source: { namespace: ['source_db'], name: 'old_table' },
        destination: { namespace: ['dest_db'], name: 'existing_table' },
      });
      expect(res.status).toBe(409);
    });
  });

  // =========================================================================
  // Table Registration
  // =========================================================================
  describe('POST /v1/namespaces/{namespace}/register', () => {
    let appWithR2: Hono<{ Bindings: MockEnv; Variables: MockVariables }>;
    let envWithR2: MockEnv;

    // Mock R2 storage for metadata files
    const mockR2Storage = new Map<string, unknown>();

    function createMockR2Bucket(): MockR2Bucket {
      return {
        put: async (key: string, value: string) => {
          mockR2Storage.set(key, JSON.parse(value));
        },
        get: async (key: string) => {
          const data = mockR2Storage.get(key);
          if (data) {
            return { json: async () => data };
          }
          return null;
        },
        list: async (options?: { prefix?: string }) => {
          const objects: Array<{ key: string }> = [];
          for (const key of mockR2Storage.keys()) {
            if (!options?.prefix || key.startsWith(options.prefix)) {
              objects.push({ key });
            }
          }
          return { objects };
        },
        delete: async (keys: string[]) => {
          for (const key of keys) {
            mockR2Storage.delete(key);
          }
        },
      };
    }

    beforeEach(async () => {
      resetMockStorage();
      mockR2Storage.clear();

      envWithR2 = {
        ...createMockEnv(),
        R2_BUCKET: createMockR2Bucket(),
      };

      appWithR2 = new Hono<{ Bindings: MockEnv; Variables: MockVariables }>();
      appWithR2.use('/*', async (c, next) => {
        c.set('catalogStub', { fetch: mockDOFetch });
        c.set('auth', createMockAuthContext());
        c.set('authorization', createMockAuthorizationContext());
        return next();
      });
      appWithR2.route('/v1', createIcebergRoutes() as Hono<{ Bindings: MockEnv; Variables: MockVariables }>);

      // Create a namespace for register tests
      await appWithR2.fetch(
        new Request('http://test/v1/namespaces', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ namespace: ['test_db'] }),
        }),
        envWithR2
      );
    });

    async function requestWithR2(method: string, path: string, body?: unknown): Promise<Response> {
      const req = new Request(`http://test${path}`, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      });
      return appWithR2.fetch(req, envWithR2);
    }

    it('should register an existing table', async () => {
      // First, create metadata in R2 to simulate an existing table
      const metadataLocation = 's3://iceberg-tables/test_db/existing_table/metadata/00000-abc123.metadata.json';
      const metadata = {
        'format-version': 2,
        'table-uuid': 'abc123-uuid',
        location: 's3://iceberg-tables/test_db/existing_table',
        'last-updated-ms': Date.now(),
        'last-column-id': 2,
        'current-schema-id': 0,
        schemas: [{
          type: 'struct',
          'schema-id': 0,
          fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            { id: 2, name: 'name', required: false, type: 'string' },
          ],
        }],
        'default-spec-id': 0,
        'partition-specs': [{ 'spec-id': 0, fields: [] }],
        'last-partition-id': 999,
        'default-sort-order-id': 0,
        'sort-orders': [{ 'order-id': 0, fields: [] }],
        properties: { owner: 'test-user' },
      };

      // Store metadata in mock R2
      mockR2Storage.set('test_db/existing_table/metadata/00000-abc123.metadata.json', metadata);

      // Register the table
      const res = await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        name: 'existing_table',
        'metadata-location': metadataLocation,
      });

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data['metadata-location']).toBe(metadataLocation);
      expect(data.metadata['table-uuid']).toBe('abc123-uuid');
      expect(data.metadata.location).toBe('s3://iceberg-tables/test_db/existing_table');

      // Verify table is now accessible via normal load
      const loadRes = await requestWithR2('GET', '/v1/namespaces/test_db/tables/existing_table');
      expect(loadRes.status).toBe(200);
    });

    it('should return 400 for missing name', async () => {
      const res = await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        'metadata-location': 's3://bucket/path/to/metadata.json',
      });
      expect(res.status).toBe(400);
      const data = await res.json();
      expect(data.error.message).toContain('name');
    });

    it('should return 400 for missing metadata-location', async () => {
      const res = await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        name: 'test_table',
      });
      expect(res.status).toBe(400);
      const data = await res.json();
      expect(data.error.message).toContain('Metadata location');
    });

    it('should return 400 when metadata cannot be loaded', async () => {
      const res = await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        name: 'nonexistent_table',
        'metadata-location': 's3://iceberg-tables/path/to/nonexistent/metadata.json',
      });
      expect(res.status).toBe(400);
      const data = await res.json();
      expect(data.error.message).toContain('Unable to load metadata');
    });

    it('should return 409 when table already exists', async () => {
      // Create metadata in R2
      const metadata = {
        'format-version': 2,
        'table-uuid': 'duplicate-uuid',
        location: 's3://iceberg-tables/test_db/duplicate_table',
        'last-updated-ms': Date.now(),
        'last-column-id': 1,
        'current-schema-id': 0,
        schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
        'default-spec-id': 0,
        'partition-specs': [{ 'spec-id': 0, fields: [] }],
        'last-partition-id': 999,
        'default-sort-order-id': 0,
        'sort-orders': [{ 'order-id': 0, fields: [] }],
      };
      mockR2Storage.set('test_db/duplicate_table/metadata/00000.metadata.json', metadata);

      // Register the table first time
      await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        name: 'duplicate_table',
        'metadata-location': 's3://iceberg-tables/test_db/duplicate_table/metadata/00000.metadata.json',
      });

      // Try to register again
      const res = await requestWithR2('POST', '/v1/namespaces/test_db/register', {
        name: 'duplicate_table',
        'metadata-location': 's3://iceberg-tables/test_db/duplicate_table/metadata/00000.metadata.json',
      });
      expect(res.status).toBe(409);
      const data = await res.json();
      expect(data.error.type).toBe('AlreadyExistsException');
    });

    it('should return 404 for non-existent namespace', async () => {
      // Create metadata in R2
      const metadata = {
        'format-version': 2,
        'table-uuid': 'orphan-uuid',
        location: 's3://iceberg-tables/nonexistent_ns/orphan_table',
        'last-updated-ms': Date.now(),
        'last-column-id': 1,
        'current-schema-id': 0,
        schemas: [{ type: 'struct', 'schema-id': 0, fields: [] }],
        'default-spec-id': 0,
        'partition-specs': [{ 'spec-id': 0, fields: [] }],
        'last-partition-id': 999,
        'default-sort-order-id': 0,
        'sort-orders': [{ 'order-id': 0, fields: [] }],
      };
      mockR2Storage.set('nonexistent_ns/orphan_table/metadata/00000.metadata.json', metadata);

      const res = await requestWithR2('POST', '/v1/namespaces/nonexistent_ns/register', {
        name: 'orphan_table',
        'metadata-location': 's3://iceberg-tables/nonexistent_ns/orphan_table/metadata/00000.metadata.json',
      });
      expect(res.status).toBe(404);
      const data = await res.json();
      expect(data.error.type).toBe('NoSuchNamespaceException');
    });
  });
});

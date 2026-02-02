import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  FileSystemCatalog,
  MemoryCatalog,
  createCatalog,
  type CatalogConfig,
  type TableIdentifier,
  type CreateTableRequest,
  type NamespaceProperties,
} from '../src/catalog/filesystem.js';
import {
  R2DataCatalogClient,
  R2DataCatalogError,
  createCatalogClient,
  type R2DataCatalogConfig,
  type RegisterTableRequest,
  type CatalogTable,
  type ListTablesResponse,
  type ListNamespacesResponse,
} from '../src/catalog/r2-client.js';
import {
  createDefaultSchema,
  createUnpartitionedSpec,
  createBucketPartitionSpec,
} from '../src/metadata/schema.js';
import type { StorageBackend, IcebergSchema } from '../src/metadata/types.js';

// ============================================================================
// Mock Storage Backend
// ============================================================================

/**
 * Create an in-memory storage backend for testing.
 */
function createMockStorage(): StorageBackend & {
  data: Map<string, Uint8Array>;
  clear: () => void;
} {
  const data = new Map<string, Uint8Array>();

  /**
   * Helper to compare Uint8Array values.
   */
  function areEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  return {
    data,

    async get(key: string): Promise<Uint8Array | null> {
      return data.get(key) ?? null;
    },

    async put(key: string, value: Uint8Array): Promise<void> {
      data.set(key, value);
    },

    async delete(key: string): Promise<void> {
      data.delete(key);
    },

    async list(prefix: string): Promise<string[]> {
      const results: string[] = [];
      for (const key of data.keys()) {
        if (key.startsWith(prefix)) {
          results.push(key);
        }
      }
      return results.sort();
    },

    async exists(key: string): Promise<boolean> {
      return data.has(key);
    },

    async putIfAbsent(key: string, value: Uint8Array): Promise<boolean> {
      if (data.has(key)) {
        return false;
      }
      data.set(key, value);
      return true;
    },

    async compareAndSwap(key: string, expected: Uint8Array | null, value: Uint8Array): Promise<boolean> {
      const current = data.get(key) ?? null;
      if (expected === null) {
        // Key must not exist
        if (current !== null) {
          return false;
        }
      } else {
        // Key must exist and match expected value
        if (current === null || !areEqual(current, expected)) {
          return false;
        }
      }
      data.set(key, value);
      return true;
    },

    clear(): void {
      data.clear();
    },
  };
}

/**
 * Create a simple test schema.
 */
function createTestSchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'name', required: false, type: 'string' },
      { id: 3, name: 'created_at', required: true, type: 'timestamp' },
    ],
  };
}

// ============================================================================
// FileSystemCatalog Tests
// ============================================================================

describe('FileSystemCatalog', () => {
  let storage: ReturnType<typeof createMockStorage>;
  let catalog: FileSystemCatalog;
  const warehouse = 's3://bucket/warehouse';

  beforeEach(() => {
    storage = createMockStorage();
    catalog = new FileSystemCatalog({
      name: 'test-catalog',
      warehouse,
      storage,
      defaultProperties: { 'default.prop': 'value' },
    });
  });

  describe('name()', () => {
    it('should return the catalog name', () => {
      expect(catalog.name()).toBe('test-catalog');
    });
  });

  describe('Namespace Operations', () => {
    describe('createNamespace', () => {
      it('should create a namespace with properties', async () => {
        const namespace = ['db1'];
        const props: NamespaceProperties = { location: '/custom/location', owner: 'admin' };

        await catalog.createNamespace(namespace, props);

        const exists = await catalog.namespaceExists(namespace);
        expect(exists).toBe(true);

        const storedProps = await catalog.getNamespaceProperties(namespace);
        expect(storedProps.location).toBe('/custom/location');
        expect(storedProps.owner).toBe('admin');
      });

      it('should create a namespace without properties', async () => {
        const namespace = ['db1'];

        await catalog.createNamespace(namespace);

        const exists = await catalog.namespaceExists(namespace);
        expect(exists).toBe(true);
      });

      it('should create nested namespaces', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createNamespace(['db1', 'schema1']);

        expect(await catalog.namespaceExists(['db1'])).toBe(true);
        expect(await catalog.namespaceExists(['db1', 'schema1'])).toBe(true);
      });

      it('should throw when namespace already exists', async () => {
        const namespace = ['db1'];
        await catalog.createNamespace(namespace);

        await expect(catalog.createNamespace(namespace)).rejects.toThrow(
          'Namespace db1 already exists'
        );
      });
    });

    describe('listNamespaces', () => {
      it('should list top-level namespaces', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createNamespace(['db2']);
        await catalog.createNamespace(['db3']);

        const namespaces = await catalog.listNamespaces();

        expect(namespaces).toHaveLength(3);
        expect(namespaces).toContainEqual(['db1']);
        expect(namespaces).toContainEqual(['db2']);
        expect(namespaces).toContainEqual(['db3']);
      });

      it('should list child namespaces of a parent', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createNamespace(['db1', 'schema1']);
        await catalog.createNamespace(['db1', 'schema2']);

        const childNamespaces = await catalog.listNamespaces(['db1']);

        expect(childNamespaces).toHaveLength(2);
        expect(childNamespaces).toContainEqual(['db1', 'schema1']);
        expect(childNamespaces).toContainEqual(['db1', 'schema2']);
      });

      it('should return empty array when no namespaces exist', async () => {
        const namespaces = await catalog.listNamespaces();
        expect(namespaces).toHaveLength(0);
      });
    });

    describe('dropNamespace', () => {
      it('should drop an empty namespace', async () => {
        await catalog.createNamespace(['db1']);

        const result = await catalog.dropNamespace(['db1']);

        expect(result).toBe(true);
        expect(await catalog.namespaceExists(['db1'])).toBe(false);
      });

      it('should return false when namespace does not exist', async () => {
        const result = await catalog.dropNamespace(['nonexistent']);
        expect(result).toBe(false);
      });

      it('should throw when namespace is not empty', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createTable(['db1'], {
          name: 'table1',
          schema: createTestSchema(),
        });

        await expect(catalog.dropNamespace(['db1'])).rejects.toThrow(
          'Namespace db1 is not empty'
        );
      });
    });

    describe('namespaceExists', () => {
      it('should return true for existing namespace', async () => {
        await catalog.createNamespace(['db1']);
        expect(await catalog.namespaceExists(['db1'])).toBe(true);
      });

      it('should return false for non-existing namespace', async () => {
        expect(await catalog.namespaceExists(['nonexistent'])).toBe(false);
      });
    });

    describe('getNamespaceProperties', () => {
      it('should return namespace properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin', comment: 'Test DB' });

        const props = await catalog.getNamespaceProperties(['db1']);

        expect(props.owner).toBe('admin');
        expect(props.comment).toBe('Test DB');
      });

      it('should throw for non-existing namespace', async () => {
        await expect(catalog.getNamespaceProperties(['nonexistent'])).rejects.toThrow(
          'Namespace nonexistent does not exist'
        );
      });
    });

    describe('updateNamespaceProperties', () => {
      it('should update namespace properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin', comment: 'Old' });

        await catalog.updateNamespaceProperties(['db1'], { comment: 'New', version: '1' }, []);

        const props = await catalog.getNamespaceProperties(['db1']);
        expect(props.owner).toBe('admin');
        expect(props.comment).toBe('New');
        expect(props.version).toBe('1');
      });

      it('should remove namespace properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin', comment: 'Test' });

        await catalog.updateNamespaceProperties(['db1'], {}, ['comment']);

        const props = await catalog.getNamespaceProperties(['db1']);
        expect(props.owner).toBe('admin');
        expect(props.comment).toBeUndefined();
      });

      it('should throw for non-existing namespace', async () => {
        await expect(
          catalog.updateNamespaceProperties(['nonexistent'], { key: 'value' }, [])
        ).rejects.toThrow('Namespace nonexistent does not exist');
      });
    });
  });

  describe('Table Operations', () => {
    beforeEach(async () => {
      await catalog.createNamespace(['db1']);
    });

    describe('createTable', () => {
      it('should create a table with schema', async () => {
        const request: CreateTableRequest = {
          name: 'users',
          schema: createTestSchema(),
        };

        const metadata = await catalog.createTable(['db1'], request);

        expect(metadata['format-version']).toBe(2);
        expect(metadata.location).toBe(`${warehouse}/db1/users`);
        expect(metadata.schemas).toHaveLength(1);
        expect(metadata.schemas[0].fields).toHaveLength(3);
      });

      it('should create a table with custom location', async () => {
        const customLocation = 's3://other-bucket/tables/users';
        const request: CreateTableRequest = {
          name: 'users',
          schema: createTestSchema(),
          location: customLocation,
        };

        const metadata = await catalog.createTable(['db1'], request);

        expect(metadata.location).toBe(customLocation);
      });

      it('should create a table with partition spec', async () => {
        const request: CreateTableRequest = {
          name: 'events',
          schema: createTestSchema(),
          partitionSpec: createBucketPartitionSpec(1, 'id_bucket', 16),
        };

        const metadata = await catalog.createTable(['db1'], request);

        expect(metadata['partition-specs']).toHaveLength(1);
        expect(metadata['partition-specs'][0].fields).toHaveLength(1);
        expect(metadata['partition-specs'][0].fields[0].transform).toBe('bucket[16]');
      });

      it('should create a table with properties', async () => {
        const request: CreateTableRequest = {
          name: 'users',
          schema: createTestSchema(),
          properties: { 'app.version': '1.0' },
        };

        const metadata = await catalog.createTable(['db1'], request);

        expect(metadata.properties['app.version']).toBe('1.0');
        expect(metadata.properties['default.prop']).toBe('value');
      });

      it('should throw when table already exists', async () => {
        const request: CreateTableRequest = {
          name: 'users',
          schema: createTestSchema(),
        };

        await catalog.createTable(['db1'], request);

        await expect(catalog.createTable(['db1'], request)).rejects.toThrow(
          'Table db1.users already exists'
        );
      });
    });

    describe('listTables', () => {
      it('should list tables in a namespace', async () => {
        await catalog.createTable(['db1'], { name: 'table1', schema: createTestSchema() });
        await catalog.createTable(['db1'], { name: 'table2', schema: createTestSchema() });

        const tables = await catalog.listTables(['db1']);

        expect(tables).toHaveLength(2);
        expect(tables).toContainEqual({ namespace: ['db1'], name: 'table1' });
        expect(tables).toContainEqual({ namespace: ['db1'], name: 'table2' });
      });

      it('should return empty array when no tables exist', async () => {
        const tables = await catalog.listTables(['db1']);
        expect(tables).toHaveLength(0);
      });
    });

    describe('loadTable', () => {
      it('should load table metadata', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const metadata = await catalog.loadTable({ namespace: ['db1'], name: 'users' });

        expect(metadata['format-version']).toBe(2);
        expect(metadata.schemas).toHaveLength(1);
      });

      it('should throw when table does not exist', async () => {
        await expect(
          catalog.loadTable({ namespace: ['db1'], name: 'nonexistent' })
        ).rejects.toThrow('Table db1.nonexistent does not exist');
      });
    });

    describe('tableExists', () => {
      it('should return true for existing table', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const exists = await catalog.tableExists({ namespace: ['db1'], name: 'users' });

        expect(exists).toBe(true);
      });

      it('should return false for non-existing table', async () => {
        const exists = await catalog.tableExists({ namespace: ['db1'], name: 'nonexistent' });

        expect(exists).toBe(false);
      });
    });

    describe('dropTable', () => {
      it('should drop a table (metadata only)', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const result = await catalog.dropTable({ namespace: ['db1'], name: 'users' }, false);

        expect(result).toBe(true);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(false);
      });

      it('should drop a table with purge', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const result = await catalog.dropTable({ namespace: ['db1'], name: 'users' }, true);

        expect(result).toBe(true);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(false);
      });

      it('should return false when table does not exist', async () => {
        const result = await catalog.dropTable({ namespace: ['db1'], name: 'nonexistent' });

        expect(result).toBe(false);
      });
    });

    describe('renameTable', () => {
      it('should rename a table within the same namespace', async () => {
        await catalog.createTable(['db1'], { name: 'old_name', schema: createTestSchema() });

        await catalog.renameTable(
          { namespace: ['db1'], name: 'old_name' },
          { namespace: ['db1'], name: 'new_name' }
        );

        expect(await catalog.tableExists({ namespace: ['db1'], name: 'old_name' })).toBe(false);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'new_name' })).toBe(true);
      });

      it('should rename a table to a different namespace', async () => {
        await catalog.createNamespace(['db2']);
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        await catalog.renameTable(
          { namespace: ['db1'], name: 'users' },
          { namespace: ['db2'], name: 'customers' }
        );

        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(false);
        expect(await catalog.tableExists({ namespace: ['db2'], name: 'customers' })).toBe(true);
      });

      it('should preserve table properties after rename', async () => {
        await catalog.createTable(['db1'], {
          name: 'users',
          schema: createTestSchema(),
          properties: { 'custom.key': 'value' },
        });

        await catalog.renameTable(
          { namespace: ['db1'], name: 'users' },
          { namespace: ['db1'], name: 'renamed' }
        );

        const metadata = await catalog.loadTable({ namespace: ['db1'], name: 'renamed' });
        expect(metadata.properties['custom.key']).toBe('value');
      });
    });

    describe('commitTable', () => {
      it('should commit table updates', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const identifier: TableIdentifier = { namespace: ['db1'], name: 'users' };
        const result = await catalog.commitTable({
          identifier,
          requirements: [],
          updates: [
            { action: 'set-properties', updates: { 'new.key': 'new.value' } },
          ],
        });

        expect(result.metadata.properties['new.key']).toBe('new.value');
        expect(result['metadata-location']).toContain('v2.metadata.json');
      });

      it('should validate table UUID requirement', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const identifier: TableIdentifier = { namespace: ['db1'], name: 'users' };
        const metadata = await catalog.loadTable(identifier);

        await expect(
          catalog.commitTable({
            identifier,
            requirements: [
              { type: 'assert-table-uuid', uuid: 'wrong-uuid' },
            ],
            updates: [],
          })
        ).rejects.toThrow('Table UUID mismatch');
      });

      it('should add and set current schema', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const identifier: TableIdentifier = { namespace: ['db1'], name: 'users' };
        const newSchema: IcebergSchema = {
          'schema-id': 1,
          type: 'struct',
          fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            { id: 2, name: 'name', required: false, type: 'string' },
            { id: 3, name: 'created_at', required: true, type: 'timestamp' },
            { id: 4, name: 'email', required: false, type: 'string' },
          ],
        };

        const result = await catalog.commitTable({
          identifier,
          requirements: [],
          updates: [
            { action: 'add-schema', schema: newSchema, 'last-column-id': 4 },
            { action: 'set-current-schema', 'schema-id': 1 },
          ],
        });

        expect(result.metadata.schemas).toHaveLength(2);
        expect(result.metadata['current-schema-id']).toBe(1);
      });

      it('should remove properties', async () => {
        await catalog.createTable(['db1'], {
          name: 'users',
          schema: createTestSchema(),
          properties: { 'to.remove': 'value' },
        });

        const identifier: TableIdentifier = { namespace: ['db1'], name: 'users' };
        const result = await catalog.commitTable({
          identifier,
          requirements: [],
          updates: [
            { action: 'remove-properties', removals: ['to.remove'] },
          ],
        });

        expect(result.metadata.properties['to.remove']).toBeUndefined();
      });
    });
  });
});

// ============================================================================
// MemoryCatalog Tests
// ============================================================================

describe('MemoryCatalog', () => {
  let catalog: MemoryCatalog;

  beforeEach(() => {
    catalog = new MemoryCatalog({ name: 'memory-catalog' });
  });

  describe('name()', () => {
    it('should return the catalog name', () => {
      expect(catalog.name()).toBe('memory-catalog');
    });
  });

  describe('Namespace Operations', () => {
    describe('createNamespace', () => {
      it('should create a namespace', async () => {
        await catalog.createNamespace(['db1']);

        expect(await catalog.namespaceExists(['db1'])).toBe(true);
      });

      it('should create a namespace with properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin' });

        const props = await catalog.getNamespaceProperties(['db1']);
        expect(props.owner).toBe('admin');
      });

      it('should throw when namespace already exists', async () => {
        await catalog.createNamespace(['db1']);

        await expect(catalog.createNamespace(['db1'])).rejects.toThrow(
          'Namespace db1 already exists'
        );
      });
    });

    describe('listNamespaces', () => {
      it('should list top-level namespaces', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createNamespace(['db2']);

        const namespaces = await catalog.listNamespaces();

        expect(namespaces).toHaveLength(2);
        expect(namespaces).toContainEqual(['db1']);
        expect(namespaces).toContainEqual(['db2']);
      });

      it('should list child namespaces', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createNamespace(['db1.schema1']);

        const children = await catalog.listNamespaces(['db1']);

        expect(children).toContainEqual(['db1', 'schema1']);
      });

      it('should return empty array when no namespaces exist', async () => {
        const namespaces = await catalog.listNamespaces();
        expect(namespaces).toHaveLength(0);
      });
    });

    describe('dropNamespace', () => {
      it('should drop an empty namespace', async () => {
        await catalog.createNamespace(['db1']);

        const result = await catalog.dropNamespace(['db1']);

        expect(result).toBe(true);
        expect(await catalog.namespaceExists(['db1'])).toBe(false);
      });

      it('should return false when namespace does not exist', async () => {
        const result = await catalog.dropNamespace(['nonexistent']);
        expect(result).toBe(false);
      });

      it('should throw when namespace is not empty', async () => {
        await catalog.createNamespace(['db1']);
        await catalog.createTable(['db1'], { name: 'table1', schema: createTestSchema() });

        await expect(catalog.dropNamespace(['db1'])).rejects.toThrow(
          'Namespace db1 is not empty'
        );
      });
    });

    describe('namespaceExists', () => {
      it('should return true for existing namespace', async () => {
        await catalog.createNamespace(['db1']);
        expect(await catalog.namespaceExists(['db1'])).toBe(true);
      });

      it('should return false for non-existing namespace', async () => {
        expect(await catalog.namespaceExists(['nonexistent'])).toBe(false);
      });
    });

    describe('updateNamespaceProperties', () => {
      it('should update namespace properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin' });

        await catalog.updateNamespaceProperties(['db1'], { version: '2.0' }, []);

        const props = await catalog.getNamespaceProperties(['db1']);
        expect(props.owner).toBe('admin');
        expect(props.version).toBe('2.0');
      });

      it('should remove namespace properties', async () => {
        await catalog.createNamespace(['db1'], { owner: 'admin', version: '1.0' });

        await catalog.updateNamespaceProperties(['db1'], {}, ['version']);

        const props = await catalog.getNamespaceProperties(['db1']);
        expect(props.owner).toBe('admin');
        expect(props.version).toBeUndefined();
      });
    });
  });

  describe('Table Operations', () => {
    beforeEach(async () => {
      await catalog.createNamespace(['db1']);
    });

    describe('createTable', () => {
      it('should create a table', async () => {
        const metadata = await catalog.createTable(['db1'], {
          name: 'users',
          schema: createTestSchema(),
        });

        expect(metadata['format-version']).toBe(2);
        expect(metadata.location).toBe('memory://db1.users');
      });

      it('should create a table with custom location', async () => {
        const metadata = await catalog.createTable(['db1'], {
          name: 'users',
          schema: createTestSchema(),
          location: 'custom://path/to/table',
        });

        expect(metadata.location).toBe('custom://path/to/table');
      });

      it('should throw when table already exists', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        await expect(
          catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() })
        ).rejects.toThrow('Table db1.users already exists');
      });
    });

    describe('listTables', () => {
      it('should list tables in a namespace', async () => {
        await catalog.createTable(['db1'], { name: 'table1', schema: createTestSchema() });
        await catalog.createTable(['db1'], { name: 'table2', schema: createTestSchema() });

        const tables = await catalog.listTables(['db1']);

        expect(tables).toHaveLength(2);
        expect(tables).toContainEqual({ namespace: ['db1'], name: 'table1' });
        expect(tables).toContainEqual({ namespace: ['db1'], name: 'table2' });
      });
    });

    describe('loadTable', () => {
      it('should load table metadata', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const metadata = await catalog.loadTable({ namespace: ['db1'], name: 'users' });

        expect(metadata['format-version']).toBe(2);
      });

      it('should throw when table does not exist', async () => {
        await expect(
          catalog.loadTable({ namespace: ['db1'], name: 'nonexistent' })
        ).rejects.toThrow('Table db1.nonexistent does not exist');
      });
    });

    describe('tableExists', () => {
      it('should return true for existing table', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(true);
      });

      it('should return false for non-existing table', async () => {
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'nonexistent' })).toBe(false);
      });
    });

    describe('dropTable', () => {
      it('should drop a table', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const result = await catalog.dropTable({ namespace: ['db1'], name: 'users' });

        expect(result).toBe(true);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(false);
      });

      it('should return false when table does not exist', async () => {
        const result = await catalog.dropTable({ namespace: ['db1'], name: 'nonexistent' });
        expect(result).toBe(false);
      });
    });

    describe('renameTable', () => {
      it('should rename a table', async () => {
        await catalog.createTable(['db1'], { name: 'old_name', schema: createTestSchema() });

        await catalog.renameTable(
          { namespace: ['db1'], name: 'old_name' },
          { namespace: ['db1'], name: 'new_name' }
        );

        expect(await catalog.tableExists({ namespace: ['db1'], name: 'old_name' })).toBe(false);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'new_name' })).toBe(true);
      });

      it('should throw when source table does not exist', async () => {
        await expect(
          catalog.renameTable(
            { namespace: ['db1'], name: 'nonexistent' },
            { namespace: ['db1'], name: 'new_name' }
          )
        ).rejects.toThrow('Table db1.nonexistent does not exist');
      });

      it('should throw when target table already exists', async () => {
        await catalog.createTable(['db1'], { name: 'table1', schema: createTestSchema() });
        await catalog.createTable(['db1'], { name: 'table2', schema: createTestSchema() });

        await expect(
          catalog.renameTable(
            { namespace: ['db1'], name: 'table1' },
            { namespace: ['db1'], name: 'table2' }
          )
        ).rejects.toThrow('Table db1.table2 already exists');
      });
    });

    describe('commitTable', () => {
      it('should commit table updates', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        const result = await catalog.commitTable({
          identifier: { namespace: ['db1'], name: 'users' },
          requirements: [],
          updates: [
            { action: 'set-properties', updates: { 'new.key': 'new.value' } },
          ],
        });

        expect(result.metadata.properties['new.key']).toBe('new.value');
      });

      it('should throw when table does not exist', async () => {
        await expect(
          catalog.commitTable({
            identifier: { namespace: ['db1'], name: 'nonexistent' },
            requirements: [],
            updates: [],
          })
        ).rejects.toThrow('Table db1.nonexistent does not exist');
      });
    });

    describe('clear()', () => {
      it('should clear all data', async () => {
        await catalog.createTable(['db1'], { name: 'users', schema: createTestSchema() });

        catalog.clear();

        expect(await catalog.namespaceExists(['db1'])).toBe(false);
        expect(await catalog.tableExists({ namespace: ['db1'], name: 'users' })).toBe(false);
      });
    });
  });

  describe('Fast In-Memory Operations', () => {
    it('should perform operations quickly without I/O', async () => {
      const start = Date.now();

      // Create multiple namespaces and tables
      for (let i = 0; i < 100; i++) {
        await catalog.createNamespace([`ns${i}`]);
        await catalog.createTable([`ns${i}`], { name: 'table', schema: createTestSchema() });
      }

      const elapsed = Date.now() - start;

      // Should complete in under 100ms (memory operations are fast)
      expect(elapsed).toBeLessThan(1000);
      expect((await catalog.listNamespaces()).length).toBe(100);
    });
  });
});

// ============================================================================
// createCatalog Factory Tests
// ============================================================================

describe('createCatalog', () => {
  it('should create a MemoryCatalog', () => {
    const config: CatalogConfig = {
      type: 'memory',
      name: 'test-memory',
    };

    const catalog = createCatalog(config);

    expect(catalog).toBeInstanceOf(MemoryCatalog);
    expect(catalog.name()).toBe('test-memory');
  });

  it('should create a FileSystemCatalog', () => {
    const storage = createMockStorage();
    const config: CatalogConfig = {
      type: 'filesystem',
      name: 'test-fs',
      warehouse: 's3://bucket/warehouse',
      storage,
    };

    const catalog = createCatalog(config);

    expect(catalog).toBeInstanceOf(FileSystemCatalog);
    expect(catalog.name()).toBe('test-fs');
  });

  it('should throw when filesystem catalog is missing required config', () => {
    const config: CatalogConfig = {
      type: 'filesystem',
      name: 'test-fs',
    };

    expect(() => createCatalog(config)).toThrow('Filesystem catalog requires warehouse and storage');
  });

  it('should throw for REST catalog (not implemented)', () => {
    const config: CatalogConfig = {
      type: 'rest',
      name: 'test-rest',
      uri: 'http://localhost:8080',
    };

    expect(() => createCatalog(config)).toThrow('REST catalog not yet implemented');
  });
});

// ============================================================================
// R2DataCatalogClient Tests
// ============================================================================

describe('R2DataCatalogClient', () => {
  let client: R2DataCatalogClient;
  let mockFetch: ReturnType<typeof vi.fn>;
  const accountId = 'test-account-id';
  const token = 'test-token';

  beforeEach(() => {
    mockFetch = vi.fn();
    global.fetch = mockFetch;

    client = new R2DataCatalogClient({
      accountId,
      token,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * Helper to mock successful API response.
   */
  function mockSuccessResponse<T>(result: T): void {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ success: true, result }),
    });
  }

  /**
   * Helper to mock error API response.
   */
  function mockErrorResponse(status: number, code: string, message: string): void {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status,
      statusText: 'Error',
      json: async () => ({ error: { code, message } }),
    });
  }

  describe('Authentication', () => {
    it('should include bearer token in requests', async () => {
      mockSuccessResponse({ namespaces: [] });

      await client.listNamespaces();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('/namespaces'),
        expect.objectContaining({
          headers: expect.objectContaining({
            'Authorization': `Bearer ${token}`,
          }),
        })
      );
    });

    it('should use correct base URL', async () => {
      mockSuccessResponse({ namespaces: [] });

      await client.listNamespaces();

      expect(mockFetch).toHaveBeenCalledWith(
        `https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/catalog/namespaces`,
        expect.anything()
      );
    });

    it('should use custom base URL when provided', async () => {
      const customClient = new R2DataCatalogClient({
        accountId,
        token,
        baseUrl: 'https://custom.api.com',
      });

      mockSuccessResponse({ namespaces: [] });
      await customClient.listNamespaces();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('https://custom.api.com'),
        expect.anything()
      );
    });
  });

  describe('Namespace Operations', () => {
    describe('listNamespaces', () => {
      it('should list namespaces', async () => {
        const mockResponse: ListNamespacesResponse = {
          namespaces: [['db1'], ['db2']],
        };
        mockSuccessResponse(mockResponse);

        const result = await client.listNamespaces();

        expect(result.namespaces).toHaveLength(2);
        expect(result.namespaces).toContainEqual(['db1']);
      });

      it('should handle pagination', async () => {
        const mockResponse: ListNamespacesResponse = {
          namespaces: [['db1']],
          nextPageToken: 'next-page',
        };
        mockSuccessResponse(mockResponse);

        const result = await client.listNamespaces(undefined, 'prev-page');

        expect(result.nextPageToken).toBe('next-page');
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('pageToken=prev-page'),
          expect.anything()
        );
      });

      it('should list child namespaces', async () => {
        mockSuccessResponse({ namespaces: [['parent', 'child']] });

        await client.listNamespaces(['parent']);

        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('parent=parent'),
          expect.anything()
        );
      });
    });

    describe('createNamespace', () => {
      it('should create a namespace', async () => {
        mockSuccessResponse({ namespace: ['db1'], properties: {} });

        const result = await client.createNamespace(['db1']);

        expect(result.namespace).toEqual(['db1']);
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/namespaces'),
          expect.objectContaining({
            method: 'POST',
            body: JSON.stringify({ namespace: ['db1'], properties: {} }),
          })
        );
      });

      it('should create a namespace with properties', async () => {
        mockSuccessResponse({ namespace: ['db1'], properties: { owner: 'admin' } });

        const result = await client.createNamespace(['db1'], { owner: 'admin' });

        expect(result.properties).toEqual({ owner: 'admin' });
      });
    });

    describe('getNamespace', () => {
      it('should get namespace metadata', async () => {
        mockSuccessResponse({ namespace: ['db1'], properties: { owner: 'admin' } });

        const result = await client.getNamespace(['db1']);

        expect(result.namespace).toEqual(['db1']);
        expect(result.properties).toEqual({ owner: 'admin' });
      });
    });

    describe('namespaceExists', () => {
      it('should return true when namespace exists', async () => {
        mockSuccessResponse({ namespace: ['db1'], properties: {} });

        const exists = await client.namespaceExists(['db1']);

        expect(exists).toBe(true);
      });

      it('should return false when namespace does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');

        const exists = await client.namespaceExists(['nonexistent']);

        expect(exists).toBe(false);
      });
    });

    describe('updateNamespaceProperties', () => {
      it('should update namespace properties', async () => {
        mockSuccessResponse({ namespace: ['db1'], properties: { key: 'value' } });

        const result = await client.updateNamespaceProperties(['db1'], { key: 'value' }, ['old']);

        expect(result.properties).toEqual({ key: 'value' });
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/properties'),
          expect.objectContaining({
            method: 'POST',
            body: JSON.stringify({ updates: { key: 'value' }, removals: ['old'] }),
          })
        );
      });
    });

    describe('dropNamespace', () => {
      it('should drop a namespace', async () => {
        mockSuccessResponse(undefined);

        const result = await client.dropNamespace(['db1']);

        expect(result).toBe(true);
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/namespaces/db1'),
          expect.objectContaining({ method: 'DELETE' })
        );
      });

      it('should return false when namespace does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');

        const result = await client.dropNamespace(['nonexistent']);

        expect(result).toBe(false);
      });
    });
  });

  describe('Table Operations', () => {
    describe('listTables', () => {
      it('should list tables in a namespace', async () => {
        const mockResponse: ListTablesResponse = {
          identifiers: [
            { namespace: ['db1'], name: 'table1' },
            { namespace: ['db1'], name: 'table2' },
          ],
        };
        mockSuccessResponse(mockResponse);

        const result = await client.listTables(['db1']);

        expect(result.identifiers).toHaveLength(2);
      });

      it('should handle pagination', async () => {
        mockSuccessResponse({ identifiers: [], nextPageToken: 'next' });

        const result = await client.listTables(['db1'], 'prev');

        expect(result.nextPageToken).toBe('next');
      });
    });

    describe('createTable', () => {
      it('should create a table', async () => {
        const mockTable: CatalogTable = {
          identifier: { namespace: ['db1'], name: 'users' },
          location: 's3://bucket/db1/users',
          metadataLocation: 's3://bucket/db1/users/metadata/v1.metadata.json',
          properties: {},
        };
        mockSuccessResponse(mockTable);

        const request: RegisterTableRequest = {
          name: 'users',
          namespace: ['db1'],
          location: 's3://bucket/db1/users',
        };

        const result = await client.createTable(request);

        expect(result.identifier.name).toBe('users');
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/namespaces/db1/tables'),
          expect.objectContaining({ method: 'POST' })
        );
      });

      it('should create a table with schema and partition spec', async () => {
        const mockTable: CatalogTable = {
          identifier: { namespace: ['db1'], name: 'events' },
          location: 's3://bucket/db1/events',
          metadataLocation: 's3://bucket/db1/events/metadata/v1.metadata.json',
          properties: {},
        };
        mockSuccessResponse(mockTable);

        const request: RegisterTableRequest = {
          name: 'events',
          namespace: ['db1'],
          location: 's3://bucket/db1/events',
          schema: createTestSchema(),
          partitionSpec: createBucketPartitionSpec(1, 'id_bucket', 16),
        };

        await client.createTable(request);

        expect(mockFetch).toHaveBeenCalledWith(
          expect.anything(),
          expect.objectContaining({
            body: expect.stringContaining('"partition-spec"'),
          })
        );
      });
    });

    describe('loadTable', () => {
      it('should load table metadata', async () => {
        const mockTable: CatalogTable = {
          identifier: { namespace: ['db1'], name: 'users' },
          location: 's3://bucket/db1/users',
          metadataLocation: 's3://bucket/db1/users/metadata/v1.metadata.json',
          properties: { key: 'value' },
        };
        mockSuccessResponse(mockTable);

        const result = await client.loadTable(['db1'], 'users');

        expect(result.identifier.name).toBe('users');
        expect(result.properties.key).toBe('value');
      });

      it('should throw when table does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Table not found');

        await expect(client.loadTable(['db1'], 'nonexistent')).rejects.toThrow(
          R2DataCatalogError
        );
      });
    });

    describe('tableExists', () => {
      it('should return true when table exists', async () => {
        mockSuccessResponse({
          identifier: { namespace: ['db1'], name: 'users' },
          location: 's3://bucket/db1/users',
          metadataLocation: '',
          properties: {},
        });

        const exists = await client.tableExists(['db1'], 'users');

        expect(exists).toBe(true);
      });

      it('should return false when table does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Table not found');

        const exists = await client.tableExists(['db1'], 'nonexistent');

        expect(exists).toBe(false);
      });
    });

    describe('updateTableLocation', () => {
      it('should update table location', async () => {
        mockSuccessResponse({
          identifier: { namespace: ['db1'], name: 'users' },
          location: 's3://new-bucket/users',
          metadataLocation: 's3://new-bucket/users/metadata/v2.metadata.json',
          properties: {},
        });

        const result = await client.updateTableLocation({
          name: 'users',
          namespace: ['db1'],
          location: 's3://new-bucket/users',
          metadataLocation: 's3://new-bucket/users/metadata/v2.metadata.json',
        });

        expect(result.location).toBe('s3://new-bucket/users');
        expect(mockFetch).toHaveBeenCalledWith(
          expect.anything(),
          expect.objectContaining({ method: 'PATCH' })
        );
      });
    });

    describe('dropTable', () => {
      it('should drop a table', async () => {
        mockSuccessResponse(undefined);

        const result = await client.dropTable(['db1'], 'users');

        expect(result).toBe(true);
        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/tables/users'),
          expect.objectContaining({ method: 'DELETE' })
        );
      });

      it('should return false when table does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Table not found');

        const result = await client.dropTable(['db1'], 'nonexistent');

        expect(result).toBe(false);
      });
    });

    describe('renameTable', () => {
      it('should rename a table', async () => {
        mockSuccessResponse(undefined);

        await client.renameTable(['db1'], 'old_name', ['db2'], 'new_name');

        expect(mockFetch).toHaveBeenCalledWith(
          expect.stringContaining('/tables/rename'),
          expect.objectContaining({
            method: 'POST',
            body: JSON.stringify({
              source: { namespace: ['db1'], name: 'old_name' },
              destination: { namespace: ['db2'], name: 'new_name' },
            }),
          })
        );
      });
    });
  });

  describe('High-Level Operations', () => {
    describe('registerCollection', () => {
      it('should register a collection with namespace creation', async () => {
        // 1. First check ['app', 'mydb'] namespace - doesn't exist
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');
        // 2. Then check ['app'] namespace - doesn't exist
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');
        // 3. Create ['app'] namespace
        mockSuccessResponse({ namespace: ['app'], properties: {} });
        // 4. Create ['app', 'mydb'] namespace
        mockSuccessResponse({ namespace: ['app', 'mydb'], properties: {} });
        // 5. Create table
        mockSuccessResponse({
          identifier: { namespace: ['app', 'mydb'], name: 'users' },
          location: 's3://bucket/users',
          metadataLocation: '',
          properties: {},
        });

        const result = await client.registerCollection('mydb', 'users', 's3://bucket/users');

        expect(result.identifier.name).toBe('users');
      });

      it('should register a collection when namespace exists', async () => {
        // 1. Check ['app', 'mydb'] namespace - exists
        mockSuccessResponse({ namespace: ['app', 'mydb'], properties: {} });
        // 2. Create table (namespace checks pass, no need to create namespaces)
        mockSuccessResponse({
          identifier: { namespace: ['app', 'mydb'], name: 'users' },
          location: 's3://bucket/users',
          metadataLocation: '',
          properties: {},
        });

        const result = await client.registerCollection('mydb', 'users', 's3://bucket/users');

        expect(result.identifier.name).toBe('users');
      });
    });

    describe('unregisterCollection', () => {
      it('should unregister a collection', async () => {
        mockSuccessResponse(undefined);

        const result = await client.unregisterCollection('mydb', 'users');

        expect(result).toBe(true);
      });
    });

    describe('listCollections', () => {
      it('should list collections in a database', async () => {
        mockSuccessResponse({
          identifiers: [
            { namespace: ['app', 'mydb'], name: 'users' },
            { namespace: ['app', 'mydb'], name: 'orders' },
          ],
        });

        const collections = await client.listCollections('mydb');

        expect(collections).toEqual(['users', 'orders']);
      });

      it('should return empty array when database does not exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');

        const collections = await client.listCollections('nonexistent');

        expect(collections).toEqual([]);
      });
    });

    describe('listDatabases', () => {
      it('should list databases', async () => {
        mockSuccessResponse({
          namespaces: [['app', 'db1'], ['app', 'db2']],
        });

        const databases = await client.listDatabases();

        expect(databases).toEqual(['db1', 'db2']);
      });

      it('should return empty array when no databases exist', async () => {
        mockErrorResponse(404, 'NOT_FOUND', 'Namespace not found');

        const databases = await client.listDatabases();

        expect(databases).toEqual([]);
      });
    });

    describe('refreshTable', () => {
      it('should refresh table metadata', async () => {
        mockSuccessResponse({
          identifier: { namespace: ['app', 'mydb'], name: 'users' },
          location: 's3://bucket/users',
          metadataLocation: 's3://bucket/users/metadata/v2.metadata.json',
          properties: {},
        });

        const result = await client.refreshTable(
          'mydb',
          'users',
          's3://bucket/users/metadata/v2.metadata.json'
        );

        expect(result.metadataLocation).toBe('s3://bucket/users/metadata/v2.metadata.json');
      });
    });
  });

  describe('Error Handling', () => {
    it('should throw R2DataCatalogError with status code', async () => {
      mockErrorResponse(409, 'CONFLICT', 'Resource already exists');

      try {
        await client.createNamespace(['db1']);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(R2DataCatalogError);
        expect((error as R2DataCatalogError).statusCode).toBe(409);
        expect((error as R2DataCatalogError).message).toContain('CONFLICT');
      }
    });

    it('should handle non-JSON error responses', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: async () => {
          throw new Error('Invalid JSON');
        },
      });

      try {
        await client.listNamespaces();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(R2DataCatalogError);
        expect((error as R2DataCatalogError).statusCode).toBe(500);
      }
    });

    it('should handle API response with success: false', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          success: false,
          error: { code: 'VALIDATION_ERROR', message: 'Invalid input' },
        }),
      });

      await expect(client.listNamespaces()).rejects.toThrow('VALIDATION_ERROR: Invalid input');
    });
  });

  describe('Namespace Encoding', () => {
    it('should encode namespace with special characters', async () => {
      mockSuccessResponse({ namespace: ['my-db', 'my.schema'], properties: {} });

      await client.getNamespace(['my-db', 'my.schema']);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('my-db%1Fmy.schema'),
        expect.anything()
      );
    });
  });
});

// ============================================================================
// createCatalogClient Factory Tests
// ============================================================================

describe('createCatalogClient', () => {
  it('should create an R2DataCatalogClient', () => {
    const config: R2DataCatalogConfig = {
      accountId: 'test-account',
      token: 'test-token',
    };

    const client = createCatalogClient(config);

    expect(client).toBeInstanceOf(R2DataCatalogClient);
  });

  it('should use custom base URL', () => {
    const config: R2DataCatalogConfig = {
      accountId: 'test-account',
      token: 'test-token',
      baseUrl: 'https://custom.api.com',
    };

    const client = createCatalogClient(config);

    expect(client).toBeInstanceOf(R2DataCatalogClient);
  });
});

// ============================================================================
// R2DataCatalogError Tests
// ============================================================================

describe('R2DataCatalogError', () => {
  it('should have correct name and properties', () => {
    const error = new R2DataCatalogError('Test error', 404);

    expect(error.name).toBe('R2DataCatalogError');
    expect(error.message).toBe('Test error');
    expect(error.statusCode).toBe(404);
  });

  it('should be an instance of Error', () => {
    const error = new R2DataCatalogError('Test error', 500);

    expect(error).toBeInstanceOf(Error);
  });
});

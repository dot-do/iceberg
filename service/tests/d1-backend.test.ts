/**
 * Tests for D1 Catalog Backend
 *
 * Tests the D1CatalogBackend class and createD1CatalogHandler function.
 * Uses a mock D1 database that simulates the D1 API.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { D1CatalogBackend, createD1CatalogHandler, D1_SCHEMA } from '../src/catalog/d1.js';
import {
  NamespaceNotFoundError,
  NamespaceAlreadyExistsError,
  NamespaceNotEmptyError,
  TableNotFoundError,
  TableAlreadyExistsError,
  ConcurrencyConflictError,
} from '../src/catalog/types.js';

// ============================================================================
// Mock D1 Database
// ============================================================================

interface MockRow {
  [key: string]: string | number | null;
}

interface MockTable {
  rows: MockRow[];
  columns: string[];
}

/**
 * Mock D1Database implementation for testing.
 * Simulates basic SQLite behavior with in-memory storage.
 */
class MockD1Database implements D1Database {
  private tables: Map<string, MockTable> = new Map();
  private initialized = false;

  /**
   * Execute raw SQL (used for schema creation).
   */
  async exec(query: string): Promise<D1ExecResult> {
    // Parse and execute CREATE TABLE statements
    const createTableMatches = query.matchAll(/CREATE TABLE IF NOT EXISTS (\w+)\s*\(([\s\S]*?)\);/gi);
    for (const match of createTableMatches) {
      const tableName = match[1];
      if (!this.tables.has(tableName)) {
        this.tables.set(tableName, { rows: [], columns: [] });
      }
    }
    this.initialized = true;
    return { count: 0, duration: 0 };
  }

  /**
   * Prepare a SQL statement.
   */
  prepare(query: string): D1PreparedStatement {
    return new MockD1PreparedStatement(this, query);
  }

  /**
   * Batch execute statements.
   */
  async batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]> {
    const results: D1Result<T>[] = [];
    for (const stmt of statements) {
      results.push(await stmt.all<T>());
    }
    return results;
  }

  /**
   * Dump the database (not implemented).
   */
  dump(): Promise<ArrayBuffer> {
    throw new Error('Not implemented');
  }

  // Internal methods for mock implementation

  _getTable(name: string): MockTable {
    if (!this.tables.has(name)) {
      this.tables.set(name, { rows: [], columns: [] });
    }
    return this.tables.get(name)!;
  }

  _insert(tableName: string, row: MockRow): number {
    const table = this._getTable(tableName);
    const id = table.rows.length + 1;
    table.rows.push({ ...row, id });
    return 1;
  }

  _select(tableName: string, where?: (row: MockRow) => boolean): MockRow[] {
    const table = this._getTable(tableName);
    if (where) {
      return table.rows.filter(where);
    }
    return [...table.rows];
  }

  _update(tableName: string, updates: Partial<MockRow>, where: (row: MockRow) => boolean): number {
    const table = this._getTable(tableName);
    let count = 0;
    for (let i = 0; i < table.rows.length; i++) {
      if (where(table.rows[i])) {
        table.rows[i] = { ...table.rows[i], ...updates };
        count++;
      }
    }
    return count;
  }

  _delete(tableName: string, where: (row: MockRow) => boolean): number {
    const table = this._getTable(tableName);
    const initialLength = table.rows.length;
    table.rows = table.rows.filter((row) => !where(row));
    return initialLength - table.rows.length;
  }

  _reset(): void {
    this.tables.clear();
    this.initialized = false;
  }
}

/**
 * Mock D1PreparedStatement implementation.
 */
class MockD1PreparedStatement implements D1PreparedStatement {
  private db: MockD1Database;
  private query: string;
  private bindings: (string | number | null)[] = [];

  constructor(db: MockD1Database, query: string) {
    this.db = db;
    this.query = query;
  }

  bind(...values: (string | number | null | ArrayBuffer)[]): D1PreparedStatement {
    this.bindings = values.map((v) =>
      v instanceof ArrayBuffer ? null : (v as string | number | null)
    );
    return this;
  }

  async first<T = unknown>(colName?: string): Promise<T | null> {
    const result = await this.all<T>();
    if (result.results.length === 0) return null;
    const row = result.results[0];
    if (colName && typeof row === 'object' && row !== null) {
      return (row as Record<string, unknown>)[colName] as T;
    }
    return row;
  }

  async run(): Promise<D1Result<unknown>> {
    return this._execute();
  }

  async all<T = unknown>(): Promise<D1Result<T>> {
    return this._execute<T>();
  }

  async raw<T = unknown[]>(): Promise<T[]> {
    const result = await this.all();
    return result.results.map((row) => Object.values(row as object)) as T[];
  }

  private async _execute<T = unknown>(): Promise<D1Result<T>> {
    const query = this.query.trim().toUpperCase();

    try {
      // INSERT
      if (query.startsWith('INSERT INTO')) {
        return this._executeInsert<T>();
      }

      // SELECT
      if (query.startsWith('SELECT')) {
        return this._executeSelect<T>();
      }

      // UPDATE
      if (query.startsWith('UPDATE')) {
        return this._executeUpdate<T>();
      }

      // DELETE
      if (query.startsWith('DELETE')) {
        return this._executeDelete<T>();
      }

      return {
        success: true,
        results: [] as T[],
        meta: { duration: 0, changes: 0, last_row_id: 0, changed_db: false, size_after: 0, rows_read: 0, rows_written: 0 },
      };
    } catch (error) {
      throw error;
    }
  }

  private _executeInsert<T>(): D1Result<T> {
    // Parse: INSERT INTO table (cols) VALUES (vals)
    const match = this.query.match(/INSERT INTO (\w+)/i);
    if (!match) throw new Error('Invalid INSERT query');

    const tableName = match[1];
    const table = this.db._getTable(tableName);

    // Check for UNIQUE constraint violations
    if (tableName === 'namespaces') {
      const namespace = this.bindings[0] as string;
      const existing = this.db._select('namespaces', (r) => r.namespace === namespace);
      if (existing.length > 0) {
        throw new Error('UNIQUE constraint failed: namespaces.namespace');
      }
    }

    if (tableName === 'tables') {
      const ns = this.bindings[0] as string;
      const name = this.bindings[1] as string;
      const existing = this.db._select('tables', (r) => r.namespace === ns && r.name === name);
      if (existing.length > 0) {
        throw new Error('UNIQUE constraint failed: tables.namespace, tables.name');
      }
    }

    // Build row from bindings based on table
    let row: MockRow;
    if (tableName === 'namespaces') {
      row = {
        namespace: this.bindings[0] as string,
        properties: this.bindings[1] as string,
        created_at: this.bindings[2] as number,
        updated_at: this.bindings[3] as number,
      };
    } else if (tableName === 'tables') {
      row = {
        namespace: this.bindings[0] as string,
        name: this.bindings[1] as string,
        location: this.bindings[2] as string,
        metadata_location: this.bindings[3] as string,
        metadata: this.bindings[4] as string | null,
        properties: this.bindings[5] as string,
        version: 1,
        created_at: this.bindings[6] as number,
        updated_at: this.bindings[7] as number,
      };
    } else {
      row = {};
    }

    this.db._insert(tableName, row);

    return {
      success: true,
      results: [] as T[],
      meta: { duration: 0, changes: 1, last_row_id: table.rows.length, changed_db: true, size_after: 0, rows_read: 0, rows_written: 1 },
    };
  }

  private _executeSelect<T>(): D1Result<T> {
    // Parse: SELECT cols FROM table WHERE ...
    const fromMatch = this.query.match(/FROM (\w+)/i);
    if (!fromMatch) throw new Error('Invalid SELECT query');

    const tableName = fromMatch[1];
    let rows = this.db._select(tableName);

    // Apply WHERE clause
    if (this.query.toUpperCase().includes('WHERE')) {
      rows = this._applyWhere(rows);
    }

    // Apply ORDER BY
    if (this.query.toUpperCase().includes('ORDER BY')) {
      const orderMatch = this.query.match(/ORDER BY (\w+)/i);
      if (orderMatch) {
        const col = orderMatch[1];
        rows.sort((a, b) => {
          const aVal = a[col];
          const bVal = b[col];
          if (aVal === null) return 1;
          if (bVal === null) return -1;
          return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
        });
      }
    }

    // Select specific columns or COUNT
    if (this.query.toUpperCase().includes('COUNT(*)')) {
      return {
        success: true,
        results: [{ count: rows.length }] as T[],
        meta: { duration: 0, changes: 0, last_row_id: 0, changed_db: false, size_after: 0, rows_read: rows.length, rows_written: 0 },
      };
    }

    return {
      success: true,
      results: rows as T[],
      meta: { duration: 0, changes: 0, last_row_id: 0, changed_db: false, size_after: 0, rows_read: rows.length, rows_written: 0 },
    };
  }

  private _executeUpdate<T>(): D1Result<T> {
    // Parse: UPDATE table SET ... WHERE ...
    const match = this.query.match(/UPDATE (\w+)/i);
    if (!match) throw new Error('Invalid UPDATE query');

    const tableName = match[1];

    // Build updates from SET clause and bindings
    const updates: Partial<MockRow> = {};

    if (tableName === 'namespaces') {
      // UPDATE namespaces SET properties = ?, updated_at = ? WHERE namespace = ?
      updates.properties = this.bindings[0] as string;
      updates.updated_at = this.bindings[1] as number;
    } else if (tableName === 'tables') {
      // Various UPDATE patterns for tables
      if (this.query.toUpperCase().includes('METADATA_LOCATION')) {
        updates.metadata_location = this.bindings[0] as string;
        updates.metadata = this.bindings[1] as string | null;
        updates.version = 'version + 1' as unknown as number; // Will be handled specially
        updates.updated_at = this.bindings[2] as number;
      } else if (this.query.toUpperCase().includes('NAMESPACE = ?,')) {
        // Rename: SET namespace = ?, name = ?, updated_at = ?
        updates.namespace = this.bindings[0] as string;
        updates.name = this.bindings[1] as string;
        updates.updated_at = this.bindings[2] as number;
      }
    }

    // Apply WHERE and update
    const rows = this.db._select(tableName);
    const whereRows = this._applyWhere(rows);

    let changes = 0;
    for (const row of whereRows) {
      const actualUpdates = { ...updates };
      if (actualUpdates.version === ('version + 1' as unknown as number)) {
        actualUpdates.version = ((row.version as number) || 0) + 1;
      }
      changes += this.db._update(tableName, actualUpdates, (r) => r.id === row.id);
    }

    return {
      success: true,
      results: [] as T[],
      meta: { duration: 0, changes, last_row_id: 0, changed_db: changes > 0, size_after: 0, rows_read: rows.length, rows_written: changes },
    };
  }

  private _executeDelete<T>(): D1Result<T> {
    // Parse: DELETE FROM table WHERE ...
    const match = this.query.match(/DELETE FROM (\w+)/i);
    if (!match) throw new Error('Invalid DELETE query');

    const tableName = match[1];
    const rows = this.db._select(tableName);
    const whereRows = this._applyWhere(rows);

    let changes = 0;
    for (const row of whereRows) {
      changes += this.db._delete(tableName, (r) => r.id === row.id);
    }

    return {
      success: true,
      results: [] as T[],
      meta: { duration: 0, changes, last_row_id: 0, changed_db: changes > 0, size_after: 0, rows_read: rows.length, rows_written: changes },
    };
  }

  private _applyWhere(rows: MockRow[]): MockRow[] {
    const queryUpper = this.query.toUpperCase();
    const whereIndex = queryUpper.indexOf('WHERE');
    if (whereIndex === -1) return rows;

    // Simple WHERE clause parsing
    // Supports: col = ?, col LIKE ?, AND conditions
    const wherePart = this.query.substring(whereIndex + 5).trim();

    // Count placeholders before WHERE
    const beforeWhere = this.query.substring(0, whereIndex);
    const bindingOffset = (beforeWhere.match(/\?/g) || []).length;

    return rows.filter((row) => {
      let bindingIndex = bindingOffset;

      // Parse conditions
      const conditions = wherePart.split(/\s+AND\s+/i);

      for (const condition of conditions) {
        const likeMatch = condition.match(/(\w+)\s+LIKE\s+\?/i);
        if (likeMatch) {
          const col = likeMatch[1];
          const pattern = this.bindings[bindingIndex++] as string;
          const value = row[col] as string;
          // Convert SQL LIKE to regex (simplified)
          if (pattern.endsWith('%')) {
            const prefix = pattern.slice(0, -1);
            if (!value?.startsWith(prefix)) return false;
          }
          continue;
        }

        const eqMatch = condition.match(/(\w+)\s*=\s*\?/i);
        if (eqMatch) {
          const col = eqMatch[1];
          const expected = this.bindings[bindingIndex++];
          if (row[col] !== expected) return false;
          continue;
        }
      }

      return true;
    });
  }
}

// ============================================================================
// D1CatalogBackend Tests
// ============================================================================

describe('D1CatalogBackend', () => {
  let mockDb: MockD1Database;
  let backend: D1CatalogBackend;

  beforeEach(() => {
    mockDb = new MockD1Database();
    backend = new D1CatalogBackend(mockDb);
  });

  describe('Namespace Operations', () => {
    describe('createNamespace', () => {
      it('should create a namespace', async () => {
        const result = await backend.createNamespace(['test_db'], { owner: 'test_user' });
        expect(result.namespace).toEqual(['test_db']);
        expect(result.properties).toEqual({ owner: 'test_user' });
        expect(result.createdAt).toBeDefined();
        expect(result.updatedAt).toBeDefined();
      });

      it('should create a multi-level namespace', async () => {
        const result = await backend.createNamespace(['prod', 'warehouse'], {});
        expect(result.namespace).toEqual(['prod', 'warehouse']);
      });

      it('should throw NamespaceAlreadyExistsError for duplicate', async () => {
        await backend.createNamespace(['test_db'], {});
        await expect(backend.createNamespace(['test_db'], {})).rejects.toThrow(
          NamespaceAlreadyExistsError
        );
      });
    });

    describe('namespaceExists', () => {
      it('should return true for existing namespace', async () => {
        await backend.createNamespace(['test_db'], {});
        const exists = await backend.namespaceExists(['test_db']);
        expect(exists).toBe(true);
      });

      it('should return false for non-existent namespace', async () => {
        const exists = await backend.namespaceExists(['nonexistent']);
        expect(exists).toBe(false);
      });
    });

    describe('getNamespace', () => {
      it('should return namespace properties', async () => {
        await backend.createNamespace(['test_db'], { owner: 'user1' });
        const props = await backend.getNamespace(['test_db']);
        expect(props).toEqual({ owner: 'user1' });
      });

      it('should throw NamespaceNotFoundError for non-existent', async () => {
        await expect(backend.getNamespace(['nonexistent'])).rejects.toThrow(
          NamespaceNotFoundError
        );
      });
    });

    describe('getNamespaceData', () => {
      it('should return full namespace data', async () => {
        await backend.createNamespace(['test_db'], { owner: 'user1' });
        const data = await backend.getNamespaceData(['test_db']);
        expect(data).not.toBeNull();
        expect(data!.namespace).toEqual(['test_db']);
        expect(data!.properties).toEqual({ owner: 'user1' });
        expect(data!.createdAt).toBeDefined();
      });

      it('should return null for non-existent namespace', async () => {
        const data = await backend.getNamespaceData(['nonexistent']);
        expect(data).toBeNull();
      });
    });

    describe('listNamespaces', () => {
      it('should list all namespaces', async () => {
        await backend.createNamespace(['db1'], {});
        await backend.createNamespace(['db2'], {});
        const namespaces = await backend.listNamespaces();
        expect(namespaces).toHaveLength(2);
        expect(namespaces).toContainEqual(['db1']);
        expect(namespaces).toContainEqual(['db2']);
      });

      it('should return empty array when no namespaces', async () => {
        const namespaces = await backend.listNamespaces();
        expect(namespaces).toEqual([]);
      });
    });

    describe('updateNamespaceProperties', () => {
      it('should update properties', async () => {
        await backend.createNamespace(['test_db'], { owner: 'user1' });
        const result = await backend.updateNamespaceProperties(
          ['test_db'],
          { location: 's3://bucket' },
          []
        );
        expect(result.updated).toContain('location');

        const props = await backend.getNamespace(['test_db']);
        expect(props.location).toBe('s3://bucket');
        expect(props.owner).toBe('user1');
      });

      it('should remove properties', async () => {
        await backend.createNamespace(['test_db'], { owner: 'user1', temp: 'value' });
        const result = await backend.updateNamespaceProperties(
          ['test_db'],
          {},
          ['temp']
        );
        expect(result.removed).toContain('temp');

        const props = await backend.getNamespace(['test_db']);
        expect(props.temp).toBeUndefined();
        expect(props.owner).toBe('user1');
      });

      it('should track missing properties in removals', async () => {
        await backend.createNamespace(['test_db'], { owner: 'user1' });
        const result = await backend.updateNamespaceProperties(
          ['test_db'],
          {},
          ['nonexistent']
        );
        expect(result.missing).toContain('nonexistent');
      });

      it('should throw NamespaceNotFoundError for non-existent', async () => {
        await expect(
          backend.updateNamespaceProperties(['nonexistent'], {}, [])
        ).rejects.toThrow(NamespaceNotFoundError);
      });
    });

    describe('dropNamespace', () => {
      it('should drop an empty namespace', async () => {
        await backend.createNamespace(['test_db'], {});
        const result = await backend.dropNamespace(['test_db']);
        expect(result).toBe(true);

        const exists = await backend.namespaceExists(['test_db']);
        expect(exists).toBe(false);
      });

      it('should throw NamespaceNotFoundError for non-existent', async () => {
        await expect(backend.dropNamespace(['nonexistent'])).rejects.toThrow(
          NamespaceNotFoundError
        );
      });

      it('should throw NamespaceNotEmptyError when tables exist', async () => {
        await backend.createNamespace(['test_db'], {});
        await backend.createTable(
          ['test_db'],
          'test_table',
          's3://bucket/test_table',
          's3://bucket/test_table/metadata/v1.json'
        );
        await expect(backend.dropNamespace(['test_db'])).rejects.toThrow(
          NamespaceNotEmptyError
        );
      });
    });
  });

  describe('Table Operations', () => {
    beforeEach(async () => {
      await backend.createNamespace(['test_db'], {});
    });

    describe('createTable', () => {
      it('should create a table', async () => {
        const result = await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json',
          { 'format-version': 2 },
          { owner: 'user1' }
        );
        expect(result.location).toBe('s3://bucket/users');
        expect(result.metadataLocation).toBe('s3://bucket/users/metadata/v1.json');
        expect(result.metadata).toEqual({ 'format-version': 2 });
        expect(result.properties).toEqual({ owner: 'user1' });
        expect(result.version).toBe(1);
      });

      it('should throw NamespaceNotFoundError for non-existent namespace', async () => {
        await expect(
          backend.createTable(
            ['nonexistent'],
            'users',
            's3://bucket/users',
            's3://bucket/users/metadata/v1.json'
          )
        ).rejects.toThrow(NamespaceNotFoundError);
      });

      it('should throw TableAlreadyExistsError for duplicate', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );
        await expect(
          backend.createTable(
            ['test_db'],
            'users',
            's3://bucket/users2',
            's3://bucket/users2/metadata/v1.json'
          )
        ).rejects.toThrow(TableAlreadyExistsError);
      });
    });

    describe('tableExists', () => {
      it('should return true for existing table', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );
        const exists = await backend.tableExists(['test_db'], 'users');
        expect(exists).toBe(true);
      });

      it('should return false for non-existent table', async () => {
        const exists = await backend.tableExists(['test_db'], 'nonexistent');
        expect(exists).toBe(false);
      });
    });

    describe('getTable', () => {
      it('should return table data', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json',
          { 'format-version': 2 }
        );
        const table = await backend.getTable(['test_db'], 'users');
        expect(table).not.toBeNull();
        expect(table!.location).toBe('s3://bucket/users');
        expect(table!.metadataLocation).toBe('s3://bucket/users/metadata/v1.json');
        expect(table!.metadata).toEqual({ 'format-version': 2 });
      });

      it('should return null for non-existent table', async () => {
        const table = await backend.getTable(['test_db'], 'nonexistent');
        expect(table).toBeNull();
      });
    });

    describe('listTables', () => {
      it('should list tables in namespace', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );
        await backend.createTable(
          ['test_db'],
          'orders',
          's3://bucket/orders',
          's3://bucket/orders/metadata/v1.json'
        );
        const tables = await backend.listTables(['test_db']);
        expect(tables).toHaveLength(2);
        expect(tables.map((t) => t.name)).toContain('users');
        expect(tables.map((t) => t.name)).toContain('orders');
      });

      it('should return empty array for namespace without tables', async () => {
        const tables = await backend.listTables(['test_db']);
        expect(tables).toEqual([]);
      });
    });

    describe('updateTableMetadata', () => {
      it('should update metadata location', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );
        const result = await backend.updateTableMetadata(
          ['test_db'],
          'users',
          's3://bucket/users/metadata/v2.json',
          { 'format-version': 2, 'last-sequence-number': 1 }
        );
        expect(result.success).toBe(true);
        expect(result.newVersion).toBe(2);

        const table = await backend.getTable(['test_db'], 'users');
        expect(table!.metadataLocation).toBe('s3://bucket/users/metadata/v2.json');
      });

      it('should throw TableNotFoundError for non-existent table', async () => {
        await expect(
          backend.updateTableMetadata(
            ['test_db'],
            'nonexistent',
            's3://bucket/metadata.json'
          )
        ).rejects.toThrow(TableNotFoundError);
      });

      it('should support optimistic concurrency control', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );

        // Update with correct version
        const result = await backend.updateTableMetadata(
          ['test_db'],
          'users',
          's3://bucket/users/metadata/v2.json',
          undefined,
          1
        );
        expect(result.success).toBe(true);
        expect(result.newVersion).toBe(2);
      });

      it('should throw ConcurrencyConflictError on version mismatch', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );

        await expect(
          backend.updateTableMetadata(
            ['test_db'],
            'users',
            's3://bucket/users/metadata/v2.json',
            undefined,
            99 // Wrong version
          )
        ).rejects.toThrow(ConcurrencyConflictError);
      });
    });

    describe('dropTable', () => {
      it('should drop a table', async () => {
        await backend.createTable(
          ['test_db'],
          'users',
          's3://bucket/users',
          's3://bucket/users/metadata/v1.json'
        );
        const result = await backend.dropTable(['test_db'], 'users');
        expect(result).toBe(true);

        const exists = await backend.tableExists(['test_db'], 'users');
        expect(exists).toBe(false);
      });

      it('should throw TableNotFoundError for non-existent table', async () => {
        await expect(backend.dropTable(['test_db'], 'nonexistent')).rejects.toThrow(
          TableNotFoundError
        );
      });
    });

    describe('renameTable', () => {
      beforeEach(async () => {
        await backend.createNamespace(['dest_db'], {});
        await backend.createTable(
          ['test_db'],
          'old_table',
          's3://bucket/old_table',
          's3://bucket/old_table/metadata/v1.json'
        );
      });

      it('should rename a table within the same namespace', async () => {
        const result = await backend.renameTable(
          ['test_db'],
          'old_table',
          ['test_db'],
          'new_table'
        );
        expect(result).toBe(true);

        const oldExists = await backend.tableExists(['test_db'], 'old_table');
        expect(oldExists).toBe(false);

        const newExists = await backend.tableExists(['test_db'], 'new_table');
        expect(newExists).toBe(true);
      });

      it('should rename a table across namespaces', async () => {
        const result = await backend.renameTable(
          ['test_db'],
          'old_table',
          ['dest_db'],
          'moved_table'
        );
        expect(result).toBe(true);

        const oldExists = await backend.tableExists(['test_db'], 'old_table');
        expect(oldExists).toBe(false);

        const newExists = await backend.tableExists(['dest_db'], 'moved_table');
        expect(newExists).toBe(true);
      });

      it('should throw TableNotFoundError for non-existent source', async () => {
        await expect(
          backend.renameTable(['test_db'], 'nonexistent', ['dest_db'], 'new_table')
        ).rejects.toThrow(TableNotFoundError);
      });

      it('should throw NamespaceNotFoundError for non-existent destination namespace', async () => {
        await expect(
          backend.renameTable(['test_db'], 'old_table', ['nonexistent'], 'new_table')
        ).rejects.toThrow(NamespaceNotFoundError);
      });

      it('should throw TableAlreadyExistsError when destination exists', async () => {
        await backend.createTable(
          ['dest_db'],
          'existing_table',
          's3://bucket/existing',
          's3://bucket/existing/metadata/v1.json'
        );
        await expect(
          backend.renameTable(['test_db'], 'old_table', ['dest_db'], 'existing_table')
        ).rejects.toThrow(TableAlreadyExistsError);
      });
    });
  });
});

// ============================================================================
// D1 HTTP Handler Tests
// ============================================================================

describe('createD1CatalogHandler', () => {
  let mockDb: MockD1Database;
  let handler: { fetch: (request: Request) => Promise<Response> };

  beforeEach(() => {
    mockDb = new MockD1Database();
    handler = createD1CatalogHandler(mockDb);
  });

  it('should handle GET /namespaces', async () => {
    // Create a namespace first
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces', { method: 'GET' })
    );
    expect(response.status).toBe(200);
    const data = await response.json() as { namespaces: string[][] };
    expect(data.namespaces).toContainEqual(['test_db']);
  });

  it('should handle POST /namespaces', async () => {
    const response = await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: { owner: 'user1' } }),
      })
    );
    expect(response.status).toBe(200);
    const data = await response.json() as { namespace: string[]; properties: Record<string, string> };
    expect(data.namespace).toEqual(['test_db']);
    expect(data.properties).toEqual({ owner: 'user1' });
  });

  it('should handle GET /namespaces/{namespace}', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: { owner: 'user1' } }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces/test_db', { method: 'GET' })
    );
    expect(response.status).toBe(200);
    const data = await response.json() as { properties: Record<string, string> };
    expect(data.properties).toEqual({ owner: 'user1' });
  });

  it('should handle DELETE /namespaces/{namespace}', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces/test_db', { method: 'DELETE' })
    );
    expect(response.status).toBe(204);
  });

  it('should handle POST /namespaces/{namespace}/tables', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces/test_db/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'users',
          location: 's3://bucket/users',
          metadataLocation: 's3://bucket/users/metadata/v1.json',
        }),
      })
    );
    expect(response.status).toBe(200);
    const data = await response.json() as { created: boolean };
    expect(data.created).toBe(true);
  });

  it('should handle GET /namespaces/{namespace}/tables/{table}', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    await handler.fetch(
      new Request('http://internal/namespaces/test_db/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'users',
          location: 's3://bucket/users',
          metadataLocation: 's3://bucket/users/metadata/v1.json',
          metadata: { 'format-version': 2 },
        }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces/test_db/tables/users', { method: 'GET' })
    );
    expect(response.status).toBe(200);
    const data = await response.json() as { location: string; metadata: unknown };
    expect(data.location).toBe('s3://bucket/users');
    expect(data.metadata).toEqual({ 'format-version': 2 });
  });

  it('should return 404 for non-existent table', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces/test_db/tables/nonexistent', { method: 'GET' })
    );
    expect(response.status).toBe(404);
  });

  it('should return 409 for duplicate namespace', async () => {
    await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );

    const response = await handler.fetch(
      new Request('http://internal/namespaces', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ namespace: ['test_db'], properties: {} }),
      })
    );
    expect(response.status).toBe(409);
  });
});

// ============================================================================
// D1 Schema Tests
// ============================================================================

describe('D1_SCHEMA', () => {
  it('should contain namespaces table', () => {
    expect(D1_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS namespaces');
    expect(D1_SCHEMA).toContain('namespace TEXT NOT NULL UNIQUE');
    expect(D1_SCHEMA).toContain('properties TEXT DEFAULT');
  });

  it('should contain tables table', () => {
    expect(D1_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS tables');
    expect(D1_SCHEMA).toContain('metadata_location TEXT NOT NULL');
    expect(D1_SCHEMA).toContain('version INTEGER DEFAULT 1');
    expect(D1_SCHEMA).toContain('UNIQUE(namespace, name)');
  });

  it('should contain indexes', () => {
    expect(D1_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_tables_namespace');
    expect(D1_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_tables_ns_name');
    expect(D1_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_namespaces_prefix');
  });
});

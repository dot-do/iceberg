/**
 * D1 Catalog Backend
 *
 * Implements the catalog storage using Cloudflare D1 (SQLite).
 * Provides the same operations as the Durable Object backend but uses
 * a global D1 database for storage.
 *
 * Benefits of D1:
 * - Global access without geographic pinning
 * - SQL queries across all data
 * - Lower cost for read-heavy workloads
 * - Built-in backup and restore
 *
 * Trade-offs vs Durable Objects:
 * - No guaranteed single-threaded execution
 * - Higher latency for writes
 * - Need to handle concurrent access explicitly
 */

import type {
  CatalogBackend,
  TableData,
  NamespaceData,
} from './types.js';
import {
  NamespaceNotFoundError,
  NamespaceAlreadyExistsError,
  NamespaceNotEmptyError,
  TableNotFoundError,
  TableAlreadyExistsError,
  ConcurrencyConflictError,
  encodeNamespace,
  decodeNamespace,
} from './types.js';

// ============================================================================
// D1 Schema
// ============================================================================

/**
 * SQL schema for the D1 catalog database.
 * This is the same schema used by the Durable Object backend,
 * ensuring compatibility between the two.
 */
export const D1_SCHEMA = `
-- Namespaces table
CREATE TABLE IF NOT EXISTS namespaces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace TEXT NOT NULL UNIQUE,
  properties TEXT DEFAULT '{}',
  created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Tables table with metadata and version for OCC
CREATE TABLE IF NOT EXISTS tables (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  location TEXT NOT NULL,
  metadata_location TEXT NOT NULL,
  metadata TEXT,
  properties TEXT DEFAULT '{}',
  version INTEGER DEFAULT 1,
  created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  UNIQUE(namespace, name)
);

-- Index on namespace for listing tables in a namespace
CREATE INDEX IF NOT EXISTS idx_tables_namespace ON tables(namespace);

-- Index on name for table lookups by name
CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name);

-- Composite index for namespace + name lookups (most common query)
CREATE INDEX IF NOT EXISTS idx_tables_ns_name ON tables(namespace, name);

-- Index for namespace prefix queries (hierarchical namespaces)
CREATE INDEX IF NOT EXISTS idx_namespaces_prefix ON namespaces(namespace);
`;

// ============================================================================
// D1 Catalog Backend
// ============================================================================

/**
 * D1CatalogBackend - Catalog backend using Cloudflare D1.
 *
 * Uses D1 (SQLite) for storing:
 * - Namespaces and their properties
 * - Table metadata locations
 * - Table properties
 * - Full table metadata (optional, for faster loads)
 *
 * Features:
 * - Global access to catalog data
 * - SQL queries for filtering/searching
 * - Optimistic concurrency control (OCC) via version column
 * - Compatible schema with Durable Object backend
 */
export class D1CatalogBackend implements CatalogBackend {
  private db: D1Database;
  private initialized: boolean = false;

  constructor(db: D1Database) {
    this.db = db;
  }

  // ==========================================================================
  // Schema Initialization
  // ==========================================================================

  /**
   * Initialize the D1 schema.
   * This should be called once during deployment via migration,
   * but we also check at runtime for development convenience.
   */
  async init(): Promise<void> {
    if (this.initialized) return;

    // Run schema creation (idempotent with IF NOT EXISTS)
    await this.db.exec(D1_SCHEMA);
    this.initialized = true;
  }

  // ==========================================================================
  // Namespace Operations
  // ==========================================================================

  /**
   * List all namespaces, optionally filtered by parent.
   */
  async listNamespaces(parent?: string[]): Promise<string[][]> {
    await this.init();

    const parentPrefix = parent ? encodeNamespace(parent) + '\x1f' : '';
    const results = await this.db
      .prepare(
        `SELECT namespace FROM namespaces WHERE namespace LIKE ? || '%' ORDER BY namespace`
      )
      .bind(parentPrefix)
      .all<{ namespace: string }>();

    return results.results.map((row) => decodeNamespace(row.namespace));
  }

  /**
   * Create a namespace.
   * @throws NamespaceAlreadyExistsError if namespace already exists
   */
  async createNamespace(
    namespace: string[],
    properties: Record<string, string> = {}
  ): Promise<NamespaceData> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const now = Date.now();

    try {
      await this.db
        .prepare(
          `INSERT INTO namespaces (namespace, properties, created_at, updated_at) VALUES (?, ?, ?, ?)`
        )
        .bind(namespaceKey, JSON.stringify(properties), now, now)
        .run();
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.includes('UNIQUE constraint')
      ) {
        throw new NamespaceAlreadyExistsError(namespace);
      }
      throw error;
    }

    return {
      namespace,
      properties,
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * Check if a namespace exists.
   */
  async namespaceExists(namespace: string[]): Promise<boolean> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(`SELECT COUNT(*) as count FROM namespaces WHERE namespace = ?`)
      .bind(namespaceKey)
      .first<{ count: number }>();

    return (result?.count ?? 0) > 0;
  }

  /**
   * Get namespace properties.
   * @throws NamespaceNotFoundError if namespace doesn't exist
   */
  async getNamespace(namespace: string[]): Promise<Record<string, string>> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(
        `SELECT properties, created_at, updated_at FROM namespaces WHERE namespace = ?`
      )
      .bind(namespaceKey)
      .first<{ properties: string; created_at: number; updated_at: number }>();

    if (!result) {
      throw new NamespaceNotFoundError(namespace);
    }

    return JSON.parse(result.properties);
  }

  /**
   * Get namespace data including timestamps.
   * Returns null if namespace doesn't exist.
   */
  async getNamespaceData(namespace: string[]): Promise<NamespaceData | null> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(
        `SELECT properties, created_at, updated_at FROM namespaces WHERE namespace = ?`
      )
      .bind(namespaceKey)
      .first<{ properties: string; created_at: number; updated_at: number }>();

    if (!result) {
      return null;
    }

    return {
      namespace,
      properties: JSON.parse(result.properties),
      createdAt: result.created_at,
      updatedAt: result.updated_at,
    };
  }

  /**
   * Update namespace properties.
   * @throws NamespaceNotFoundError if namespace doesn't exist
   */
  async updateNamespaceProperties(
    namespace: string[],
    updates: Record<string, string>,
    removals: string[]
  ): Promise<{ updated: string[]; removed: string[]; missing: string[] }> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);

    // Get current properties (throws if not found)
    const current = await this.getNamespace(namespace);

    // Track which removals were actually present
    const actuallyRemoved: string[] = [];
    const missing: string[] = [];

    // Apply removals
    for (const key of removals) {
      if (key in current) {
        delete current[key];
        actuallyRemoved.push(key);
      } else {
        missing.push(key);
      }
    }

    // Apply updates
    for (const [key, value] of Object.entries(updates)) {
      current[key] = value;
    }

    const now = Date.now();

    // Save updated properties
    await this.db
      .prepare(
        `UPDATE namespaces SET properties = ?, updated_at = ? WHERE namespace = ?`
      )
      .bind(JSON.stringify(current), now, namespaceKey)
      .run();

    return {
      updated: Object.keys(updates),
      removed: actuallyRemoved,
      missing,
    };
  }

  /**
   * Drop a namespace.
   * @throws NamespaceNotFoundError if namespace doesn't exist
   * @throws NamespaceNotEmptyError if namespace contains tables
   */
  async dropNamespace(namespace: string[]): Promise<boolean> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);

    // Check if namespace exists
    const exists = await this.namespaceExists(namespace);
    if (!exists) {
      throw new NamespaceNotFoundError(namespace);
    }

    // Check if namespace has tables
    const tableCount = await this.db
      .prepare(`SELECT COUNT(*) as count FROM tables WHERE namespace = ?`)
      .bind(namespaceKey)
      .first<{ count: number }>();

    if ((tableCount?.count ?? 0) > 0) {
      throw new NamespaceNotEmptyError(namespace);
    }

    const result = await this.db
      .prepare(`DELETE FROM namespaces WHERE namespace = ?`)
      .bind(namespaceKey)
      .run();

    return result.meta.changes > 0;
  }

  // ==========================================================================
  // Table Operations
  // ==========================================================================

  /**
   * List tables in a namespace.
   */
  async listTables(
    namespace: string[]
  ): Promise<Array<{ namespace: string[]; name: string }>> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const results = await this.db
      .prepare(
        `SELECT namespace, name FROM tables WHERE namespace = ? ORDER BY name`
      )
      .bind(namespaceKey)
      .all<{ namespace: string; name: string }>();

    return results.results.map((row) => ({
      namespace: decodeNamespace(row.namespace),
      name: row.name,
    }));
  }

  /**
   * Create a table entry.
   * @throws NamespaceNotFoundError if namespace doesn't exist
   * @throws TableAlreadyExistsError if table already exists
   */
  async createTable(
    namespace: string[],
    name: string,
    location: string,
    metadataLocation: string,
    metadata?: unknown,
    properties: Record<string, string> = {}
  ): Promise<TableData> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);

    // Check namespace exists
    const nsExists = await this.namespaceExists(namespace);
    if (!nsExists) {
      throw new NamespaceNotFoundError(namespace);
    }

    const now = Date.now();

    try {
      await this.db
        .prepare(
          `INSERT INTO tables (namespace, name, location, metadata_location, metadata, properties, version, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`
        )
        .bind(
          namespaceKey,
          name,
          location,
          metadataLocation,
          metadata ? JSON.stringify(metadata) : null,
          JSON.stringify(properties),
          now,
          now
        )
        .run();
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.includes('UNIQUE constraint')
      ) {
        throw new TableAlreadyExistsError(namespace, name);
      }
      throw error;
    }

    return {
      location,
      metadataLocation,
      metadata,
      properties,
      version: 1,
    };
  }

  /**
   * Check if a table exists.
   */
  async tableExists(namespace: string[], name: string): Promise<boolean> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(
        `SELECT COUNT(*) as count FROM tables WHERE namespace = ? AND name = ?`
      )
      .bind(namespaceKey, name)
      .first<{ count: number }>();

    return (result?.count ?? 0) > 0;
  }

  /**
   * Get table metadata.
   * Returns null if table doesn't exist.
   */
  async getTable(namespace: string[], name: string): Promise<TableData | null> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(
        `SELECT location, metadata_location, metadata, properties, version FROM tables WHERE namespace = ? AND name = ?`
      )
      .bind(namespaceKey, name)
      .first<{
        location: string;
        metadata_location: string;
        metadata: string | null;
        properties: string;
        version: number;
      }>();

    if (!result) return null;

    return {
      location: result.location,
      metadataLocation: result.metadata_location,
      metadata: result.metadata ? JSON.parse(result.metadata) : undefined,
      properties: JSON.parse(result.properties),
      version: result.version,
    };
  }

  /**
   * Update table metadata location and optionally the full metadata.
   * Supports optimistic concurrency control (OCC) via version checking.
   *
   * @param expectedVersion - If provided, update will only succeed if current version matches
   * @throws TableNotFoundError if table doesn't exist
   * @throws ConcurrencyConflictError if version mismatch (OCC failure)
   */
  async updateTableMetadata(
    namespace: string[],
    name: string,
    metadataLocation: string,
    metadata?: unknown,
    expectedVersion?: number
  ): Promise<{ success: boolean; newVersion: number }> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const now = Date.now();

    // If OCC is requested, check version first
    if (expectedVersion !== undefined) {
      const current = await this.getTable(namespace, name);
      if (!current) {
        throw new TableNotFoundError(namespace, name);
      }
      if (current.version !== expectedVersion) {
        throw new ConcurrencyConflictError(
          `Version mismatch: expected ${expectedVersion}, got ${current.version}`
        );
      }
    }

    // Build the update query
    let query: string;
    let bindings: (string | number | null)[];

    if (expectedVersion !== undefined) {
      query = `UPDATE tables SET
        metadata_location = ?,
        metadata = ?,
        version = version + 1,
        updated_at = ?
       WHERE namespace = ? AND name = ? AND version = ?`;
      bindings = [
        metadataLocation,
        metadata ? JSON.stringify(metadata) : null,
        now,
        namespaceKey,
        name,
        expectedVersion,
      ];
    } else {
      query = `UPDATE tables SET
        metadata_location = ?,
        metadata = ?,
        version = version + 1,
        updated_at = ?
       WHERE namespace = ? AND name = ?`;
      bindings = [
        metadataLocation,
        metadata ? JSON.stringify(metadata) : null,
        now,
        namespaceKey,
        name,
      ];
    }

    const result = await this.db.prepare(query).bind(...bindings).run();

    if (result.meta.changes === 0) {
      if (expectedVersion !== undefined) {
        throw new ConcurrencyConflictError('Concurrent modification detected');
      }
      throw new TableNotFoundError(namespace, name);
    }

    // Get new version
    const versionResult = await this.db
      .prepare(`SELECT version FROM tables WHERE namespace = ? AND name = ?`)
      .bind(namespaceKey, name)
      .first<{ version: number }>();

    return { success: true, newVersion: versionResult?.version ?? 1 };
  }

  /**
   * Drop a table.
   * @throws TableNotFoundError if table doesn't exist
   */
  async dropTable(namespace: string[], name: string): Promise<boolean> {
    await this.init();

    const namespaceKey = encodeNamespace(namespace);
    const result = await this.db
      .prepare(`DELETE FROM tables WHERE namespace = ? AND name = ?`)
      .bind(namespaceKey, name)
      .run();

    if (result.meta.changes === 0) {
      throw new TableNotFoundError(namespace, name);
    }

    return true;
  }

  /**
   * Rename a table.
   * @throws TableNotFoundError if source table doesn't exist
   * @throws NamespaceNotFoundError if destination namespace doesn't exist
   * @throws TableAlreadyExistsError if destination table already exists
   */
  async renameTable(
    fromNamespace: string[],
    fromName: string,
    toNamespace: string[],
    toName: string
  ): Promise<boolean> {
    await this.init();

    const fromKey = encodeNamespace(fromNamespace);
    const toKey = encodeNamespace(toNamespace);

    // Use a batch for atomicity
    // Check source table exists
    const source = await this.getTable(fromNamespace, fromName);
    if (!source) {
      throw new TableNotFoundError(fromNamespace, fromName);
    }

    // Check destination namespace exists
    const toNsExists = await this.namespaceExists(toNamespace);
    if (!toNsExists) {
      throw new NamespaceNotFoundError(toNamespace);
    }

    // Check destination table doesn't exist
    const destExists = await this.tableExists(toNamespace, toName);
    if (destExists) {
      throw new TableAlreadyExistsError(toNamespace, toName);
    }

    const now = Date.now();

    const result = await this.db
      .prepare(
        `UPDATE tables SET namespace = ?, name = ?, updated_at = ?
         WHERE namespace = ? AND name = ?`
      )
      .bind(toKey, toName, now, fromKey, fromName)
      .run();

    return result.meta.changes > 0;
  }
}

// ============================================================================
// HTTP Handler for D1 Backend
// ============================================================================

/**
 * Create a fetch handler for the D1 catalog backend.
 * This provides the same HTTP API as the Durable Object backend,
 * allowing the routes.ts to work with either backend.
 */
export function createD1CatalogHandler(db: D1Database): {
  fetch: (request: Request) => Promise<Response>;
} {
  const backend = new D1CatalogBackend(db);

  return {
    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);
      const path = url.pathname;

      try {
        // =====================================================================
        // Namespace operations
        // =====================================================================

        // GET /namespaces - List all namespaces
        if (request.method === 'GET' && path === '/namespaces') {
          const namespaces = await backend.listNamespaces();
          return Response.json({ namespaces });
        }

        // POST /namespaces - Create namespace
        if (request.method === 'POST' && path === '/namespaces') {
          const body = (await request.json()) as {
            namespace: string[];
            properties?: Record<string, string>;
          };
          const result = await backend.createNamespace(
            body.namespace,
            body.properties
          );
          return Response.json({
            namespace: body.namespace,
            properties: result.properties,
          });
        }

        // GET /namespaces/{namespace} - Get namespace
        const getNamespaceMatch = path.match(/^\/namespaces\/([^/]+)$/);
        if (request.method === 'GET' && getNamespaceMatch) {
          const namespaceKey = decodeURIComponent(getNamespaceMatch[1]);
          const namespace = decodeNamespace(namespaceKey);
          const properties = await backend.getNamespace(namespace);
          return Response.json({ properties });
        }

        // DELETE /namespaces/{namespace} - Drop namespace
        const deleteNamespaceMatch = path.match(/^\/namespaces\/([^/]+)$/);
        if (request.method === 'DELETE' && deleteNamespaceMatch) {
          const namespaceKey = decodeURIComponent(deleteNamespaceMatch[1]);
          const namespace = decodeNamespace(namespaceKey);
          await backend.dropNamespace(namespace);
          return new Response(null, { status: 204 });
        }

        // POST /namespaces/{namespace}/properties - Update namespace properties
        const updatePropsMatch = path.match(
          /^\/namespaces\/([^/]+)\/properties$/
        );
        if (request.method === 'POST' && updatePropsMatch) {
          const namespaceKey = decodeURIComponent(updatePropsMatch[1]);
          const namespace = decodeNamespace(namespaceKey);
          const body = (await request.json()) as {
            updates?: Record<string, string>;
            removals?: string[];
          };
          const result = await backend.updateNamespaceProperties(
            namespace,
            body.updates ?? {},
            body.removals ?? []
          );
          return Response.json(result);
        }

        // =====================================================================
        // Table operations
        // =====================================================================

        // GET /namespaces/{namespace}/tables - List tables
        const listTablesMatch = path.match(/^\/namespaces\/([^/]+)\/tables$/);
        if (request.method === 'GET' && listTablesMatch) {
          const namespaceKey = decodeURIComponent(listTablesMatch[1]);
          const namespace = decodeNamespace(namespaceKey);

          // Check namespace exists
          const exists = await backend.namespaceExists(namespace);
          if (!exists) {
            throw new NamespaceNotFoundError(namespace);
          }

          const tables = await backend.listTables(namespace);
          return Response.json({ tables });
        }

        // POST /namespaces/{namespace}/tables - Create table
        const createTableMatch = path.match(/^\/namespaces\/([^/]+)\/tables$/);
        if (request.method === 'POST' && createTableMatch) {
          const namespaceKey = decodeURIComponent(createTableMatch[1]);
          const namespace = decodeNamespace(namespaceKey);
          const body = (await request.json()) as {
            name: string;
            location: string;
            metadataLocation: string;
            metadata?: unknown;
            properties?: Record<string, string>;
          };
          await backend.createTable(
            namespace,
            body.name,
            body.location,
            body.metadataLocation,
            body.metadata,
            body.properties
          );
          return Response.json({ created: true });
        }

        // GET /namespaces/{namespace}/tables/{table} - Get table
        const getTableMatch = path.match(
          /^\/namespaces\/([^/]+)\/tables\/([^/]+)$/
        );
        if (request.method === 'GET' && getTableMatch) {
          const namespaceKey = decodeURIComponent(getTableMatch[1]);
          const tableName = decodeURIComponent(getTableMatch[2]);
          const namespace = decodeNamespace(namespaceKey);
          const table = await backend.getTable(namespace, tableName);
          if (!table) {
            throw new TableNotFoundError(namespace, tableName);
          }
          return Response.json({
            location: table.location,
            metadataLocation: table.metadataLocation,
            metadata: table.metadata,
            properties: table.properties,
            version: table.version,
          });
        }

        // DELETE /namespaces/{namespace}/tables/{table} - Drop table
        const deleteTableMatch = path.match(
          /^\/namespaces\/([^/]+)\/tables\/([^/]+)$/
        );
        if (request.method === 'DELETE' && deleteTableMatch) {
          const namespaceKey = decodeURIComponent(deleteTableMatch[1]);
          const tableName = decodeURIComponent(deleteTableMatch[2]);
          const namespace = decodeNamespace(namespaceKey);
          await backend.dropTable(namespace, tableName);
          return new Response(null, { status: 204 });
        }

        // POST /namespaces/{namespace}/tables/{table}/commit - Commit table changes
        const commitTableMatch = path.match(
          /^\/namespaces\/([^/]+)\/tables\/([^/]+)\/commit$/
        );
        if (request.method === 'POST' && commitTableMatch) {
          const namespaceKey = decodeURIComponent(commitTableMatch[1]);
          const tableName = decodeURIComponent(commitTableMatch[2]);
          const namespace = decodeNamespace(namespaceKey);
          const body = (await request.json()) as {
            metadataLocation: string;
            metadata?: unknown;
            expectedVersion?: number;
          };
          const result = await backend.updateTableMetadata(
            namespace,
            tableName,
            body.metadataLocation,
            body.metadata,
            body.expectedVersion
          );
          return Response.json({ committed: true, version: result.newVersion });
        }

        // =====================================================================
        // Cross-namespace operations
        // =====================================================================

        // POST /tables/rename - Rename table
        if (request.method === 'POST' && path === '/tables/rename') {
          const body = (await request.json()) as {
            fromNamespace: string[];
            fromName: string;
            toNamespace: string[];
            toName: string;
          };
          await backend.renameTable(
            body.fromNamespace,
            body.fromName,
            body.toNamespace,
            body.toName
          );
          return new Response(null, { status: 204 });
        }

        return new Response('Not Found', { status: 404 });
      } catch (error) {
        // Handle typed errors
        if (error instanceof NamespaceNotFoundError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 404 }
          );
        }
        if (error instanceof NamespaceAlreadyExistsError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 409 }
          );
        }
        if (error instanceof NamespaceNotEmptyError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 409 }
          );
        }
        if (error instanceof TableNotFoundError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 404 }
          );
        }
        if (error instanceof TableAlreadyExistsError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 409 }
          );
        }
        if (error instanceof ConcurrencyConflictError) {
          return Response.json(
            { error: error.message, code: error.code },
            { status: 409 }
          );
        }

        // Handle unknown errors
        const message =
          error instanceof Error ? error.message : 'Unknown error';
        const status = message.includes('UNIQUE constraint') ? 409 : 500;
        return Response.json({ error: message }, { status });
      }
    },
  };
}

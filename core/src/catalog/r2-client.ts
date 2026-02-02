/**
 * R2 Data Catalog Client
 *
 * Integration with Cloudflare R2 Data Catalog API for Iceberg tables.
 * Provides methods to register, update, and manage Iceberg tables
 * in the R2 Data Catalog.
 *
 * @see https://developers.cloudflare.com/r2/data-catalog/
 */

import type {
  IcebergSchema,
  PartitionSpec,
  TableMetadata,
} from '../metadata/types.js';
import {
  createDefaultSchema,
  createUnpartitionedSpec,
} from '../metadata/schema.js';

// ============================================================================
// Type Definitions
// ============================================================================

/** R2 Data Catalog configuration */
export interface R2DataCatalogConfig {
  /** Cloudflare Account ID */
  accountId: string;
  /** R2 Data Catalog API token (R2_DATA_CATALOG_TOKEN) */
  token: string;
  /** Optional base URL for the API (defaults to Cloudflare API) */
  baseUrl?: string;
}

/** Namespace in the R2 Data Catalog */
export interface CatalogNamespace {
  /** Namespace identifier (array of names, e.g., ['db', 'schema']) */
  namespace: string[];
  /** Namespace properties */
  properties?: Record<string, string>;
}

/** Table registration request */
export interface RegisterTableRequest {
  /** Table name */
  name: string;
  /** Namespace (array of names) */
  namespace: string[];
  /** Table location in R2 (e.g., 's3://bucket/warehouse/db/table') */
  location: string;
  /** Iceberg schema */
  schema?: IcebergSchema;
  /** Partition specification */
  partitionSpec?: PartitionSpec;
  /** Table properties */
  properties?: Record<string, string>;
}

/** Table update request */
export interface UpdateTableRequest {
  /** Table name */
  name: string;
  /** Namespace */
  namespace: string[];
  /** New table location */
  location?: string;
  /** Metadata location (path to metadata.json) */
  metadataLocation?: string;
  /** Updated properties */
  properties?: Record<string, string>;
}

/** Table information from the catalog */
export interface CatalogTable {
  /** Table identifier */
  identifier: {
    namespace: string[];
    name: string;
  };
  /** Table location */
  location: string;
  /** Metadata location */
  metadataLocation: string;
  /** Table properties */
  properties: Record<string, string>;
  /** Table metadata (if loaded) */
  metadata?: TableMetadata;
}

/** List tables response */
export interface ListTablesResponse {
  /** Array of table identifiers */
  identifiers: Array<{
    namespace: string[];
    name: string;
  }>;
  /** Pagination token for next page */
  nextPageToken?: string;
}

/** List namespaces response */
export interface ListNamespacesResponse {
  /** Array of namespaces */
  namespaces: string[][];
  /** Pagination token for next page */
  nextPageToken?: string;
}

/** Error response from R2 Data Catalog API */
export interface CatalogErrorResponse {
  error: {
    code: string;
    message: string;
    type?: string;
  };
}

/** API response wrapper */
export type CatalogApiResponse<T> =
  | { success: true; result: T }
  | { success: false; error: CatalogErrorResponse['error'] };

// ============================================================================
// R2 Data Catalog Client
// ============================================================================

/**
 * Client for interacting with the Cloudflare R2 Data Catalog API.
 *
 * The R2 Data Catalog provides a REST API for managing Iceberg table
 * metadata, enabling query engines like DuckDB, Spark, and Trino to
 * discover and query Iceberg tables stored in R2.
 *
 * @example
 * ```ts
 * const client = new R2DataCatalogClient({
 *   accountId: process.env.CF_ACCOUNT_ID!,
 *   token: process.env.R2_DATA_CATALOG_TOKEN!,
 * });
 *
 * // Create a namespace
 * await client.createNamespace(['myapp', 'production']);
 *
 * // Register a table
 * await client.createTable({
 *   name: 'users',
 *   namespace: ['myapp', 'production'],
 *   location: 's3://my-bucket/warehouse/myapp/production/users',
 * });
 *
 * // List tables
 * const tables = await client.listTables(['myapp', 'production']);
 * ```
 */
export class R2DataCatalogClient {
  private readonly config: Required<R2DataCatalogConfig>;

  constructor(config: R2DataCatalogConfig) {
    this.config = {
      ...config,
      baseUrl: config.baseUrl ?? 'https://api.cloudflare.com/client/v4',
    };
  }

  // ==========================================================================
  // Private Helpers
  // ==========================================================================

  /**
   * Get the base API URL for the R2 Data Catalog.
   */
  private getApiUrl(): string {
    return `${this.config.baseUrl}/accounts/${this.config.accountId}/r2/catalog`;
  }

  /**
   * Make an authenticated request to the R2 Data Catalog API.
   */
  private async request<T>(
    method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH',
    path: string,
    body?: unknown
  ): Promise<T> {
    const url = `${this.getApiUrl()}${path}`;

    const headers: Record<string, string> = {
      'Authorization': `Bearer ${this.config.token}`,
      'Content-Type': 'application/json',
    };

    const response = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!response.ok) {
      let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
      try {
        const errorBody = await response.json() as CatalogErrorResponse;
        if (errorBody.error) {
          errorMessage = `${errorBody.error.code}: ${errorBody.error.message}`;
        }
      } catch {
        // Use default error message
      }
      throw new R2DataCatalogError(errorMessage, response.status);
    }

    const result = await response.json() as CatalogApiResponse<T>;

    if (!result.success) {
      throw new R2DataCatalogError(
        `${result.error.code}: ${result.error.message}`,
        response.status
      );
    }

    return result.result;
  }

  /**
   * Encode namespace as URL path segment.
   */
  private encodeNamespace(namespace: string[]): string {
    return namespace.map(encodeURIComponent).join('%1F');
  }

  // ==========================================================================
  // Namespace Operations
  // ==========================================================================

  /**
   * List all namespaces in the catalog.
   *
   * @param parent - Optional parent namespace to list children of
   * @param pageToken - Optional pagination token
   * @returns List of namespaces
   */
  async listNamespaces(
    parent?: string[],
    pageToken?: string
  ): Promise<ListNamespacesResponse> {
    const params = new URLSearchParams();
    if (parent && parent.length > 0) {
      params.set('parent', this.encodeNamespace(parent));
    }
    if (pageToken) {
      params.set('pageToken', pageToken);
    }

    const query = params.toString();
    const path = `/namespaces${query ? `?${query}` : ''}`;

    return this.request<ListNamespacesResponse>('GET', path);
  }

  /**
   * Create a new namespace.
   *
   * @param namespace - Namespace path as array of names
   * @param properties - Optional namespace properties
   * @throws R2DataCatalogError if namespace already exists
   */
  async createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<CatalogNamespace> {
    return this.request<CatalogNamespace>('POST', '/namespaces', {
      namespace,
      properties: properties ?? {},
    });
  }

  /**
   * Get namespace metadata.
   *
   * @param namespace - Namespace path as array of names
   * @returns Namespace metadata
   * @throws R2DataCatalogError if namespace does not exist
   */
  async getNamespace(namespace: string[]): Promise<CatalogNamespace> {
    const path = `/namespaces/${this.encodeNamespace(namespace)}`;
    return this.request<CatalogNamespace>('GET', path);
  }

  /**
   * Check if a namespace exists.
   *
   * @param namespace - Namespace path as array of names
   * @returns True if namespace exists
   */
  async namespaceExists(namespace: string[]): Promise<boolean> {
    try {
      await this.getNamespace(namespace);
      return true;
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update namespace properties.
   *
   * @param namespace - Namespace path as array of names
   * @param updates - Properties to add or update
   * @param removals - Property keys to remove
   */
  async updateNamespaceProperties(
    namespace: string[],
    updates: Record<string, string>,
    removals: string[] = []
  ): Promise<CatalogNamespace> {
    const path = `/namespaces/${this.encodeNamespace(namespace)}/properties`;
    return this.request<CatalogNamespace>('POST', path, {
      updates,
      removals,
    });
  }

  /**
   * Drop a namespace.
   *
   * @param namespace - Namespace path as array of names
   * @returns True if namespace was dropped, false if it didn't exist
   * @throws R2DataCatalogError if namespace is not empty
   */
  async dropNamespace(namespace: string[]): Promise<boolean> {
    try {
      const path = `/namespaces/${this.encodeNamespace(namespace)}`;
      await this.request<void>('DELETE', path);
      return true;
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return false;
      }
      throw error;
    }
  }

  // ==========================================================================
  // Table Operations
  // ==========================================================================

  /**
   * List all tables in a namespace.
   *
   * @param namespace - Namespace path as array of names
   * @param pageToken - Optional pagination token
   * @returns List of table identifiers
   */
  async listTables(
    namespace: string[],
    pageToken?: string
  ): Promise<ListTablesResponse> {
    const params = new URLSearchParams();
    if (pageToken) {
      params.set('pageToken', pageToken);
    }

    const query = params.toString();
    const path = `/namespaces/${this.encodeNamespace(namespace)}/tables${query ? `?${query}` : ''}`;

    return this.request<ListTablesResponse>('GET', path);
  }

  /**
   * Create (register) a new table in the catalog.
   *
   * @param request - Table registration request
   * @returns Created table metadata
   * @throws R2DataCatalogError if table already exists
   */
  async createTable(request: RegisterTableRequest): Promise<CatalogTable> {
    const path = `/namespaces/${this.encodeNamespace(request.namespace)}/tables`;

    const schema = request.schema ?? createDefaultSchema();
    const partitionSpec = request.partitionSpec ?? createUnpartitionedSpec();

    return this.request<CatalogTable>('POST', path, {
      name: request.name,
      location: request.location,
      schema,
      'partition-spec': partitionSpec,
      properties: {
        'format-version': '2',
        ...request.properties,
      },
    });
  }

  /**
   * Load table metadata from the catalog.
   *
   * @param namespace - Namespace path as array of names
   * @param name - Table name
   * @returns Table metadata
   * @throws R2DataCatalogError if table does not exist
   */
  async loadTable(namespace: string[], name: string): Promise<CatalogTable> {
    const path = `/namespaces/${this.encodeNamespace(namespace)}/tables/${encodeURIComponent(name)}`;
    return this.request<CatalogTable>('GET', path);
  }

  /**
   * Check if a table exists.
   *
   * @param namespace - Namespace path as array of names
   * @param name - Table name
   * @returns True if table exists
   */
  async tableExists(namespace: string[], name: string): Promise<boolean> {
    try {
      await this.loadTable(namespace, name);
      return true;
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Update table location in the catalog.
   *
   * This is useful when moving table data to a new location or
   * when the metadata file location has changed.
   *
   * @param request - Table update request
   * @returns Updated table metadata
   */
  async updateTableLocation(request: UpdateTableRequest): Promise<CatalogTable> {
    const path = `/namespaces/${this.encodeNamespace(request.namespace)}/tables/${encodeURIComponent(request.name)}`;

    const updates: Record<string, unknown> = {};
    if (request.location) {
      updates.location = request.location;
    }
    if (request.metadataLocation) {
      updates['metadata-location'] = request.metadataLocation;
    }
    if (request.properties) {
      updates.properties = request.properties;
    }

    return this.request<CatalogTable>('PATCH', path, updates);
  }

  /**
   * Drop (unregister) a table from the catalog.
   *
   * Note: This only removes the table from the catalog. It does not
   * delete the actual table data or metadata files in R2.
   *
   * @param namespace - Namespace path as array of names
   * @param name - Table name
   * @param purge - If true, also delete the table data (not implemented)
   * @returns True if table was dropped, false if it didn't exist
   */
  async dropTable(
    namespace: string[],
    name: string,
    _purge: boolean = false
  ): Promise<boolean> {
    try {
      const path = `/namespaces/${this.encodeNamespace(namespace)}/tables/${encodeURIComponent(name)}`;
      await this.request<void>('DELETE', path);
      return true;
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Rename a table within the same namespace or move to a different namespace.
   *
   * @param fromNamespace - Source namespace
   * @param fromName - Source table name
   * @param toNamespace - Destination namespace
   * @param toName - Destination table name
   */
  async renameTable(
    fromNamespace: string[],
    fromName: string,
    toNamespace: string[],
    toName: string
  ): Promise<void> {
    await this.request<void>('POST', '/tables/rename', {
      source: {
        namespace: fromNamespace,
        name: fromName,
      },
      destination: {
        namespace: toNamespace,
        name: toName,
      },
    });
  }

  // ==========================================================================
  // High-Level Operations
  // ==========================================================================

  /**
   * Register an application collection as an Iceberg table in the catalog.
   *
   * This is a convenience method that creates the namespace if needed
   * and registers the table with application-specific properties.
   *
   * @param database - Database name
   * @param collection - Collection name
   * @param location - Table location in R2
   * @param options - Additional options
   * @returns Registered table metadata
   */
  async registerCollection(
    database: string,
    collection: string,
    location: string,
    options?: {
      schema?: IcebergSchema;
      partitionSpec?: PartitionSpec;
      properties?: Record<string, string>;
    }
  ): Promise<CatalogTable> {
    const namespace = ['app', database];

    // Ensure namespace exists
    if (!(await this.namespaceExists(namespace))) {
      // Create parent namespace if needed
      if (!(await this.namespaceExists(['app']))) {
        await this.createNamespace(['app'], {
          description: 'Application managed tables',
        });
      }
      await this.createNamespace(namespace, {
        description: `Database: ${database}`,
      });
    }

    return this.createTable({
      name: collection,
      namespace,
      location,
      schema: options?.schema,
      partitionSpec: options?.partitionSpec,
      properties: {
        'app.database': database,
        'app.collection': collection,
        ...options?.properties,
      },
    });
  }

  /**
   * Unregister a collection from the catalog.
   *
   * @param database - Database name
   * @param collection - Collection name
   * @returns True if table was dropped
   */
  async unregisterCollection(
    database: string,
    collection: string
  ): Promise<boolean> {
    const namespace = ['app', database];
    return this.dropTable(namespace, collection);
  }

  /**
   * List all collections in a database.
   *
   * @param database - Database name
   * @returns Array of collection names
   */
  async listCollections(database: string): Promise<string[]> {
    const namespace = ['app', database];

    try {
      const response = await this.listTables(namespace);
      return response.identifiers.map((id) => id.name);
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return [];
      }
      throw error;
    }
  }

  /**
   * List all databases.
   *
   * @returns Array of database names
   */
  async listDatabases(): Promise<string[]> {
    try {
      const response = await this.listNamespaces(['app']);
      return response.namespaces.map((ns) => ns[ns.length - 1]);
    } catch (error) {
      if (error instanceof R2DataCatalogError && error.statusCode === 404) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Refresh table metadata in the catalog.
   *
   * Call this after writing new snapshots to update the catalog
   * with the latest metadata location.
   *
   * @param database - Database name
   * @param collection - Collection name
   * @param metadataLocation - New metadata file location
   */
  async refreshTable(
    database: string,
    collection: string,
    metadataLocation: string
  ): Promise<CatalogTable> {
    return this.updateTableLocation({
      name: collection,
      namespace: ['app', database],
      metadataLocation,
    });
  }
}

// ============================================================================
// Error Class
// ============================================================================

/**
 * Error thrown by R2 Data Catalog operations.
 */
export class R2DataCatalogError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number
  ) {
    super(message);
    this.name = 'R2DataCatalogError';
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create an R2 Data Catalog client from environment variables.
 *
 * Required environment variables:
 * - CF_ACCOUNT_ID: Cloudflare Account ID
 * - R2_DATA_CATALOG_TOKEN: R2 Data Catalog API token
 *
 * @returns R2DataCatalogClient instance
 * @throws Error if required environment variables are missing
 */
export function createR2DataCatalogClient(): R2DataCatalogClient {
  const accountId = process.env.CF_ACCOUNT_ID;
  const token = process.env.R2_DATA_CATALOG_TOKEN;

  if (!accountId) {
    throw new Error('CF_ACCOUNT_ID environment variable is required');
  }
  if (!token) {
    throw new Error('R2_DATA_CATALOG_TOKEN environment variable is required');
  }

  return new R2DataCatalogClient({
    accountId,
    token,
  });
}

/**
 * Create an R2 Data Catalog client from configuration.
 *
 * @param config - Client configuration
 * @returns R2DataCatalogClient instance
 */
export function createCatalogClient(config: R2DataCatalogConfig): R2DataCatalogClient {
  return new R2DataCatalogClient(config);
}

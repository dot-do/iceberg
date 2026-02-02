/**
 * R2 Data Catalog Client
 *
 * Integration with Cloudflare R2 Data Catalog API for Iceberg tables.
 * Provides methods to register, update, and manage Iceberg tables
 * in the R2 Data Catalog.
 *
 * @see https://developers.cloudflare.com/r2/data-catalog/
 */
import type { IcebergSchema, PartitionSpec, TableMetadata } from '../metadata/types.js';
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
export type CatalogApiResponse<T> = {
    success: true;
    result: T;
} | {
    success: false;
    error: CatalogErrorResponse['error'];
};
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
export declare class R2DataCatalogClient {
    private readonly config;
    constructor(config: R2DataCatalogConfig);
    /**
     * Get the base API URL for the R2 Data Catalog.
     */
    private getApiUrl;
    /**
     * Make an authenticated request to the R2 Data Catalog API.
     */
    private request;
    /**
     * Encode namespace as URL path segment.
     */
    private encodeNamespace;
    /**
     * List all namespaces in the catalog.
     *
     * @param parent - Optional parent namespace to list children of
     * @param pageToken - Optional pagination token
     * @returns List of namespaces
     */
    listNamespaces(parent?: string[], pageToken?: string): Promise<ListNamespacesResponse>;
    /**
     * Create a new namespace.
     *
     * @param namespace - Namespace path as array of names
     * @param properties - Optional namespace properties
     * @throws R2DataCatalogError if namespace already exists
     */
    createNamespace(namespace: string[], properties?: Record<string, string>): Promise<CatalogNamespace>;
    /**
     * Get namespace metadata.
     *
     * @param namespace - Namespace path as array of names
     * @returns Namespace metadata
     * @throws R2DataCatalogError if namespace does not exist
     */
    getNamespace(namespace: string[]): Promise<CatalogNamespace>;
    /**
     * Check if a namespace exists.
     *
     * @param namespace - Namespace path as array of names
     * @returns True if namespace exists
     */
    namespaceExists(namespace: string[]): Promise<boolean>;
    /**
     * Update namespace properties.
     *
     * @param namespace - Namespace path as array of names
     * @param updates - Properties to add or update
     * @param removals - Property keys to remove
     */
    updateNamespaceProperties(namespace: string[], updates: Record<string, string>, removals?: string[]): Promise<CatalogNamespace>;
    /**
     * Drop a namespace.
     *
     * @param namespace - Namespace path as array of names
     * @returns True if namespace was dropped, false if it didn't exist
     * @throws R2DataCatalogError if namespace is not empty
     */
    dropNamespace(namespace: string[]): Promise<boolean>;
    /**
     * List all tables in a namespace.
     *
     * @param namespace - Namespace path as array of names
     * @param pageToken - Optional pagination token
     * @returns List of table identifiers
     */
    listTables(namespace: string[], pageToken?: string): Promise<ListTablesResponse>;
    /**
     * Create (register) a new table in the catalog.
     *
     * @param request - Table registration request
     * @returns Created table metadata
     * @throws R2DataCatalogError if table already exists
     */
    createTable(request: RegisterTableRequest): Promise<CatalogTable>;
    /**
     * Load table metadata from the catalog.
     *
     * @param namespace - Namespace path as array of names
     * @param name - Table name
     * @returns Table metadata
     * @throws R2DataCatalogError if table does not exist
     */
    loadTable(namespace: string[], name: string): Promise<CatalogTable>;
    /**
     * Check if a table exists.
     *
     * @param namespace - Namespace path as array of names
     * @param name - Table name
     * @returns True if table exists
     */
    tableExists(namespace: string[], name: string): Promise<boolean>;
    /**
     * Update table location in the catalog.
     *
     * This is useful when moving table data to a new location or
     * when the metadata file location has changed.
     *
     * @param request - Table update request
     * @returns Updated table metadata
     */
    updateTableLocation(request: UpdateTableRequest): Promise<CatalogTable>;
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
    dropTable(namespace: string[], name: string, _purge?: boolean): Promise<boolean>;
    /**
     * Rename a table within the same namespace or move to a different namespace.
     *
     * @param fromNamespace - Source namespace
     * @param fromName - Source table name
     * @param toNamespace - Destination namespace
     * @param toName - Destination table name
     */
    renameTable(fromNamespace: string[], fromName: string, toNamespace: string[], toName: string): Promise<void>;
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
    registerCollection(database: string, collection: string, location: string, options?: {
        schema?: IcebergSchema;
        partitionSpec?: PartitionSpec;
        properties?: Record<string, string>;
    }): Promise<CatalogTable>;
    /**
     * Unregister a collection from the catalog.
     *
     * @param database - Database name
     * @param collection - Collection name
     * @returns True if table was dropped
     */
    unregisterCollection(database: string, collection: string): Promise<boolean>;
    /**
     * List all collections in a database.
     *
     * @param database - Database name
     * @returns Array of collection names
     */
    listCollections(database: string): Promise<string[]>;
    /**
     * List all databases.
     *
     * @returns Array of database names
     */
    listDatabases(): Promise<string[]>;
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
    refreshTable(database: string, collection: string, metadataLocation: string): Promise<CatalogTable>;
}
/**
 * Error thrown by R2 Data Catalog operations.
 */
export declare class R2DataCatalogError extends Error {
    readonly statusCode: number;
    constructor(message: string, statusCode: number);
}
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
export declare function createR2DataCatalogClient(): R2DataCatalogClient;
/**
 * Create an R2 Data Catalog client from configuration.
 *
 * @param config - Client configuration
 * @returns R2DataCatalogClient instance
 */
export declare function createCatalogClient(config: R2DataCatalogConfig): R2DataCatalogClient;
//# sourceMappingURL=r2-client.d.ts.map
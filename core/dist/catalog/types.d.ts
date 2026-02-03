/**
 * Catalog Abstraction Layer
 *
 * Defines the core interfaces and types for all Iceberg catalog implementations.
 * This abstraction allows for pluggable catalog backends including:
 * - Filesystem (local, S3, R2, GCS)
 * - REST catalogs (Iceberg REST Catalog spec)
 * - Hive Metastore
 * - AWS Glue
 * - Nessie
 * - Custom implementations
 *
 * @see https://iceberg.apache.org/spec/#catalog-api
 */
import type { IcebergSchema, PartitionSpec, SortOrder, TableMetadata, Snapshot } from '../metadata/types.js';
/**
 * Unique identifier for a table within a catalog.
 *
 * A table identifier consists of a namespace (hierarchical path) and a table name.
 * For example: namespace = ['database', 'schema'], name = 'users'
 *
 * @example
 * ```ts
 * const identifier: TableIdentifier = {
 *   namespace: ['production', 'analytics'],
 *   name: 'events',
 * };
 *
 * // Convert to string representation
 * const fullName = tableIdentifierToString(identifier); // 'production.analytics.events'
 * ```
 */
export interface TableIdentifier {
    /** Hierarchical namespace path (e.g., ['database', 'schema']) */
    readonly namespace: readonly string[];
    /** Table name within the namespace */
    readonly name: string;
}
/**
 * Convert a TableIdentifier to a dot-separated string.
 */
export declare function tableIdentifierToString(identifier: TableIdentifier): string;
/**
 * Parse a dot-separated string into a TableIdentifier.
 * The last segment becomes the table name, rest become the namespace.
 */
export declare function parseTableIdentifier(fullName: string): TableIdentifier;
/**
 * Check if two TableIdentifiers are equal.
 */
export declare function tableIdentifiersEqual(a: TableIdentifier, b: TableIdentifier): boolean;
/**
 * Properties associated with a namespace.
 *
 * Namespaces can have arbitrary key-value properties for metadata.
 * Common properties include:
 * - location: Default storage location for tables in this namespace
 * - owner: Owner of the namespace
 * - description: Human-readable description
 * - comment: Additional comments
 *
 * @example
 * ```ts
 * const props: NamespaceProperties = {
 *   location: 's3://warehouse/production',
 *   owner: 'analytics-team',
 *   description: 'Production analytics tables',
 * };
 * ```
 */
export interface NamespaceProperties {
    /** Default storage location for tables in this namespace */
    location?: string;
    /** Owner of the namespace */
    owner?: string;
    /** Human-readable description */
    description?: string;
    /** Additional arbitrary properties */
    [key: string]: string | undefined;
}
/**
 * Result of a namespace properties update operation.
 */
export interface UpdateNamespacePropertiesResponse {
    /** Properties that were successfully updated */
    updated: string[];
    /** Properties that were successfully removed */
    removed: string[];
    /** Properties that were missing (removal requested but didn't exist) */
    missing: string[];
}
/**
 * Configuration for creating a catalog instance.
 *
 * @example
 * ```ts
 * // Filesystem catalog
 * const fsConfig: CatalogConfig = {
 *   type: 'filesystem',
 *   name: 'local',
 *   warehouse: '/data/warehouse',
 *   properties: {
 *     'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
 *   },
 * };
 *
 * // REST catalog
 * const restConfig: CatalogConfig = {
 *   type: 'rest',
 *   name: 'catalog',
 *   uri: 'https://catalog.example.com',
 *   credential: 'client_id:client_secret',
 * };
 * ```
 */
export interface CatalogConfig {
    /** Catalog implementation type */
    type: 'rest' | 'filesystem' | 'memory' | 'hive' | 'glue' | 'nessie' | 'r2';
    /** Catalog name used for identification */
    name: string;
    /** Base URI for REST-based catalogs */
    uri?: string;
    /** Warehouse location (base path for table data) */
    warehouse?: string;
    /** Authentication credential (format depends on catalog type) */
    credential?: string;
    /** OAuth2 token for authentication */
    token?: string;
    /** OAuth2 scope for token requests */
    scope?: string;
    /** Default table properties applied to all new tables */
    defaultProperties?: Record<string, string>;
    /** Catalog-specific properties */
    properties?: Record<string, string>;
}
/**
 * Request to create a new table.
 *
 * @example
 * ```ts
 * const request: CreateTableRequest = {
 *   name: 'users',
 *   schema: {
 *     'schema-id': 0,
 *     type: 'struct',
 *     fields: [
 *       { id: 1, name: 'id', type: 'long', required: true },
 *       { id: 2, name: 'name', type: 'string', required: false },
 *     ],
 *   },
 *   location: 's3://bucket/warehouse/db/users',
 *   properties: { 'write.format.default': 'parquet' },
 * };
 * ```
 */
export interface CreateTableRequest {
    /** Table name (without namespace) */
    name: string;
    /** Table schema defining columns and types */
    schema: IcebergSchema;
    /** Optional partition specification */
    partitionSpec?: PartitionSpec;
    /** Optional sort order specification */
    sortOrder?: SortOrder;
    /** Optional explicit table location (defaults to warehouse/namespace/name) */
    location?: string;
    /** Optional table properties */
    properties?: Record<string, string>;
    /** Whether to stage the table creation without committing */
    stageCreate?: boolean;
}
/**
 * Response from creating a table.
 */
export interface CreateTableResponse {
    /** Path to the metadata file */
    metadataLocation: string;
    /** Complete table metadata */
    metadata: TableMetadata;
}
/**
 * Request to load a table, optionally at a specific snapshot.
 */
export interface LoadTableRequest {
    /** Table identifier */
    identifier: TableIdentifier;
    /** Optional snapshot ID to load */
    snapshotId?: number;
    /** Optional ref name (branch or tag) to load */
    ref?: string;
}
/**
 * Response from loading a table.
 */
export interface LoadTableResponse {
    /** Path to the metadata file */
    metadataLocation: string;
    /** Complete table metadata */
    metadata: TableMetadata;
    /** Catalog-specific configuration for the table */
    config?: Record<string, string>;
}
/**
 * Requirements that must be satisfied for a commit to succeed.
 * These enable optimistic concurrency control.
 */
export type TableRequirement = {
    type: 'assert-create';
} | {
    type: 'assert-table-uuid';
    uuid: string;
} | {
    type: 'assert-ref-snapshot-id';
    ref: string;
    'snapshot-id': number | null;
} | {
    type: 'assert-last-assigned-field-id';
    'last-assigned-field-id': number;
} | {
    type: 'assert-current-schema-id';
    'current-schema-id': number;
} | {
    type: 'assert-last-assigned-partition-id';
    'last-assigned-partition-id': number;
} | {
    type: 'assert-default-spec-id';
    'default-spec-id': number;
} | {
    type: 'assert-default-sort-order-id';
    'default-sort-order-id': number;
};
/**
 * Updates to apply to a table atomically.
 */
export type TableUpdate = {
    action: 'assign-uuid';
    uuid: string;
} | {
    action: 'upgrade-format-version';
    'format-version': number;
} | {
    action: 'add-schema';
    schema: IcebergSchema;
    'last-column-id': number;
} | {
    action: 'set-current-schema';
    'schema-id': number;
} | {
    action: 'add-partition-spec';
    spec: PartitionSpec;
} | {
    action: 'set-default-spec';
    'spec-id': number;
} | {
    action: 'add-sort-order';
    'sort-order': SortOrder;
} | {
    action: 'set-default-sort-order';
    'sort-order-id': number;
} | {
    action: 'add-snapshot';
    snapshot: Snapshot;
} | {
    action: 'remove-snapshots';
    'snapshot-ids': number[];
} | {
    action: 'remove-snapshot-ref';
    'ref-name': string;
} | {
    action: 'set-snapshot-ref';
    'ref-name': string;
    type: 'branch' | 'tag';
    'snapshot-id': number;
    'max-ref-age-ms'?: number;
    'max-snapshot-age-ms'?: number;
    'min-snapshots-to-keep'?: number;
} | {
    action: 'set-properties';
    updates: Record<string, string>;
} | {
    action: 'remove-properties';
    removals: string[];
} | {
    action: 'set-location';
    location: string;
};
/**
 * Request to commit changes to a table atomically.
 *
 * @example
 * ```ts
 * const commitRequest: CommitTableRequest = {
 *   identifier: { namespace: ['db'], name: 'users' },
 *   requirements: [
 *     { type: 'assert-ref-snapshot-id', ref: 'main', 'snapshot-id': 123456789 },
 *   ],
 *   updates: [
 *     { action: 'add-snapshot', snapshot: newSnapshot },
 *     { action: 'set-snapshot-ref', 'ref-name': 'main', type: 'branch', 'snapshot-id': newSnapshot['snapshot-id'] },
 *   ],
 * };
 * ```
 */
export interface CommitTableRequest {
    /** Table identifier */
    identifier: TableIdentifier;
    /** Requirements that must be satisfied */
    requirements: TableRequirement[];
    /** Updates to apply atomically */
    updates: TableUpdate[];
}
/**
 * Response from committing changes to a table.
 */
export interface CommitTableResponse {
    /** Path to the new metadata file */
    'metadata-location': string;
    /** Updated table metadata */
    metadata: TableMetadata;
}
/**
 * Base error for all catalog operations.
 */
export declare class CatalogError extends Error {
    readonly code: string;
    readonly cause?: Error | undefined;
    constructor(message: string, code: string, cause?: Error | undefined);
}
/**
 * Error thrown when a namespace does not exist.
 *
 * @example
 * ```ts
 * try {
 *   await catalog.loadTable({ namespace: ['nonexistent'], name: 'table' });
 * } catch (error) {
 *   if (error instanceof NamespaceNotFoundError) {
 *     console.log(`Namespace not found: ${error.namespace.join('.')}`);
 *   }
 * }
 * ```
 */
export declare class NamespaceNotFoundError extends CatalogError {
    readonly namespace: string[];
    constructor(namespace: string[]);
}
/**
 * Error thrown when attempting to create a namespace that already exists.
 */
export declare class NamespaceAlreadyExistsError extends CatalogError {
    readonly namespace: string[];
    constructor(namespace: string[]);
}
/**
 * Error thrown when attempting to drop a non-empty namespace.
 */
export declare class NamespaceNotEmptyError extends CatalogError {
    readonly namespace: string[];
    constructor(namespace: string[]);
}
/**
 * Error thrown when a table does not exist.
 *
 * @example
 * ```ts
 * try {
 *   await catalog.loadTable({ namespace: ['db'], name: 'nonexistent' });
 * } catch (error) {
 *   if (error instanceof TableNotFoundError) {
 *     console.log(`Table not found: ${tableIdentifierToString(error.identifier)}`);
 *   }
 * }
 * ```
 */
export declare class TableNotFoundError extends CatalogError {
    readonly identifier: TableIdentifier;
    constructor(identifier: TableIdentifier);
}
/**
 * Error thrown when attempting to create a table that already exists.
 */
export declare class TableAlreadyExistsError extends CatalogError {
    readonly identifier: TableIdentifier;
    constructor(identifier: TableIdentifier);
}
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export declare class CommitFailedError extends CatalogError {
    readonly identifier: TableIdentifier;
    readonly requirement: TableRequirement;
    constructor(identifier: TableIdentifier, requirement: TableRequirement, message?: string);
}
/**
 * Error thrown when a commit conflicts with another concurrent commit.
 */
export declare class CommitConflictError extends CatalogError {
    readonly identifier: TableIdentifier;
    constructor(identifier: TableIdentifier, message?: string);
}
/**
 * Error thrown when catalog authentication fails.
 */
export declare class AuthenticationError extends CatalogError {
    constructor(message?: string);
}
/**
 * Error thrown when an operation is not permitted.
 */
export declare class AuthorizationError extends CatalogError {
    readonly operation: string;
    readonly resource: string;
    constructor(operation: string, resource: string, message?: string);
}
/**
 * Error thrown when the catalog service is unavailable.
 */
export declare class ServiceUnavailableError extends CatalogError {
    constructor(message?: string);
}
/**
 * Error thrown when an invalid argument is provided.
 */
export declare class InvalidArgumentError extends CatalogError {
    readonly argument: string;
    constructor(argument: string, message: string);
}
/**
 * Core interface for Iceberg catalog implementations.
 *
 * A catalog is responsible for:
 * - Managing namespaces (hierarchical organization)
 * - Managing tables (CRUD operations)
 * - Providing atomic commits for table updates
 * - Tracking table metadata locations
 *
 * All methods should throw appropriate CatalogError subclasses on failure.
 *
 * @example
 * ```ts
 * // Create a catalog instance
 * const catalog = createCatalog({
 *   type: 'filesystem',
 *   name: 'local',
 *   warehouse: '/data/warehouse',
 * });
 *
 * // Namespace operations
 * await catalog.createNamespace(['production', 'analytics']);
 * const namespaces = await catalog.listNamespaces(['production']);
 *
 * // Table operations
 * await catalog.createTable(['production', 'analytics'], {
 *   name: 'events',
 *   schema: mySchema,
 * });
 *
 * const table = await catalog.loadTable({
 *   namespace: ['production', 'analytics'],
 *   name: 'events',
 * });
 *
 * // Atomic commit
 * await catalog.commitTable({
 *   identifier: { namespace: ['production', 'analytics'], name: 'events' },
 *   requirements: [...],
 *   updates: [...],
 * });
 * ```
 */
export interface Catalog {
    /**
     * Get the catalog name.
     */
    name(): string;
    /**
     * Get catalog properties.
     */
    properties?(): Record<string, string>;
    /**
     * Initialize the catalog (connect, authenticate, etc.).
     * Called automatically by factory functions.
     */
    initialize?(): Promise<void>;
    /**
     * Close the catalog and release resources.
     */
    close?(): Promise<void>;
    /**
     * List namespaces in the catalog.
     *
     * @param parent - Optional parent namespace to list children of
     * @returns Array of namespace paths
     * @throws NamespaceNotFoundError if parent namespace does not exist
     */
    listNamespaces(parent?: string[]): Promise<string[][]>;
    /**
     * Create a new namespace.
     *
     * @param namespace - Namespace path to create
     * @param properties - Optional namespace properties
     * @throws NamespaceAlreadyExistsError if namespace already exists
     * @throws NamespaceNotFoundError if parent namespace does not exist
     */
    createNamespace(namespace: string[], properties?: NamespaceProperties): Promise<void>;
    /**
     * Drop a namespace.
     *
     * @param namespace - Namespace path to drop
     * @returns true if namespace was dropped, false if it didn't exist
     * @throws NamespaceNotEmptyError if namespace contains tables
     */
    dropNamespace(namespace: string[]): Promise<boolean>;
    /**
     * Check if a namespace exists.
     *
     * @param namespace - Namespace path to check
     * @returns true if namespace exists
     */
    namespaceExists(namespace: string[]): Promise<boolean>;
    /**
     * Get namespace properties.
     *
     * @param namespace - Namespace path
     * @returns Namespace properties
     * @throws NamespaceNotFoundError if namespace does not exist
     */
    getNamespaceProperties(namespace: string[]): Promise<NamespaceProperties>;
    /**
     * Update namespace properties.
     *
     * @param namespace - Namespace path
     * @param updates - Properties to add or update
     * @param removals - Property keys to remove
     * @returns Update result
     * @throws NamespaceNotFoundError if namespace does not exist
     */
    updateNamespaceProperties(namespace: string[], updates: Record<string, string>, removals: string[]): Promise<UpdateNamespacePropertiesResponse | void>;
    /**
     * List tables in a namespace.
     *
     * @param namespace - Namespace path
     * @returns Array of table identifiers
     * @throws NamespaceNotFoundError if namespace does not exist
     */
    listTables(namespace: string[]): Promise<TableIdentifier[]>;
    /**
     * Create a new table.
     *
     * @param namespace - Namespace path for the table
     * @param request - Table creation request
     * @returns Created table metadata
     * @throws NamespaceNotFoundError if namespace does not exist
     * @throws TableAlreadyExistsError if table already exists
     */
    createTable(namespace: string[], request: CreateTableRequest): Promise<TableMetadata>;
    /**
     * Load a table's metadata.
     *
     * @param identifier - Table identifier
     * @returns Table metadata
     * @throws TableNotFoundError if table does not exist
     */
    loadTable(identifier: TableIdentifier): Promise<TableMetadata>;
    /**
     * Check if a table exists.
     *
     * @param identifier - Table identifier
     * @returns true if table exists
     */
    tableExists(identifier: TableIdentifier): Promise<boolean>;
    /**
     * Drop a table.
     *
     * @param identifier - Table identifier
     * @param purge - If true, also delete data files
     * @returns true if table was dropped, false if it didn't exist
     */
    dropTable(identifier: TableIdentifier, purge?: boolean): Promise<boolean>;
    /**
     * Rename a table.
     *
     * @param from - Current table identifier
     * @param to - New table identifier
     * @throws TableNotFoundError if source table does not exist
     * @throws TableAlreadyExistsError if destination table already exists
     * @throws NamespaceNotFoundError if destination namespace does not exist
     */
    renameTable(from: TableIdentifier, to: TableIdentifier): Promise<void>;
    /**
     * Commit changes to a table atomically.
     *
     * This is the core operation for making changes to Iceberg tables.
     * It validates requirements and applies updates in a single atomic operation.
     *
     * @param request - Commit request with requirements and updates
     * @returns Commit response with new metadata location
     * @throws TableNotFoundError if table does not exist
     * @throws CommitFailedError if requirements are not satisfied
     * @throws CommitConflictError if there was a concurrent modification
     */
    commitTable(request: CommitTableRequest): Promise<CommitTableResponse>;
    /**
     * Register an existing table in the catalog.
     * The table metadata must already exist at the specified location.
     *
     * @param namespace - Namespace path for the table
     * @param name - Table name
     * @param metadataLocation - Path to existing metadata file
     * @returns Registered table metadata
     */
    registerTable?(namespace: string[], name: string, metadataLocation: string): Promise<TableMetadata>;
    /**
     * Invalidate any cached metadata for a table.
     *
     * @param identifier - Table identifier
     */
    invalidateTable?(identifier: TableIdentifier): Promise<void>;
    /**
     * Get the metadata location for a table without loading full metadata.
     *
     * @param identifier - Table identifier
     * @returns Metadata file location
     */
    getTableMetadataLocation?(identifier: TableIdentifier): Promise<string>;
}
/**
 * Check if an error is a CatalogError.
 */
export declare function isCatalogError(error: unknown): error is CatalogError;
/**
 * Check if an error is a NamespaceNotFoundError.
 */
export declare function isNamespaceNotFoundError(error: unknown): error is NamespaceNotFoundError;
/**
 * Check if an error is a TableNotFoundError.
 */
export declare function isTableNotFoundError(error: unknown): error is TableNotFoundError;
/**
 * Check if an error is a CommitFailedError.
 */
export declare function isCommitFailedError(error: unknown): error is CommitFailedError;
/**
 * Check if an error is a CommitConflictError.
 */
export declare function isCommitConflictError(error: unknown): error is CommitConflictError;
/**
 * Factory function type for creating catalog instances.
 */
export type CatalogFactory = (config: CatalogConfig) => Catalog | Promise<Catalog>;
//# sourceMappingURL=types.d.ts.map
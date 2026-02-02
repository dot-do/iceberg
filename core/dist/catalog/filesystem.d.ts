/**
 * Filesystem Catalog Implementation
 *
 * Provides catalog management for Iceberg tables using a filesystem backend.
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { StorageBackend, IcebergSchema, PartitionSpec, TableMetadata, Snapshot } from '../metadata/types.js';
/** Catalog configuration */
export interface CatalogConfig {
    /** Catalog type */
    type: 'rest' | 'filesystem' | 'memory';
    /** Catalog name */
    name: string;
    /** Base URI for REST catalog or base path for filesystem catalog */
    uri?: string;
    /** Warehouse location for filesystem catalog */
    warehouse?: string;
    /** Storage backend for filesystem catalog */
    storage?: StorageBackend;
    /** Default properties for new tables */
    defaultProperties?: Record<string, string>;
}
/** Table identifier */
export interface TableIdentifier {
    namespace: string[];
    name: string;
}
/** Namespace properties */
export interface NamespaceProperties {
    location?: string;
    [key: string]: string | undefined;
}
/** Table creation request */
export interface CreateTableRequest {
    name: string;
    schema: IcebergSchema;
    partitionSpec?: PartitionSpec;
    location?: string;
    properties?: Record<string, string>;
}
/** Table commit request */
export interface CommitTableRequest {
    identifier: TableIdentifier;
    requirements: TableRequirement[];
    updates: TableUpdate[];
}
/** Table requirement for commit validation */
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
/** Table update operation */
export type TableUpdate = {
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
    action: 'add-snapshot';
    snapshot: Snapshot;
} | {
    action: 'set-snapshot-ref';
    'ref-name': string;
    type: 'branch' | 'tag';
    'snapshot-id': number;
} | {
    action: 'remove-snapshot-ref';
    'ref-name': string;
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
/** Commit response */
export interface CommitTableResponse {
    'metadata-location': string;
    metadata: TableMetadata;
}
/**
 * Iceberg catalog interface.
 *
 * A catalog is responsible for managing table metadata and providing
 * atomic operations for creating, updating, and dropping tables.
 */
export interface IcebergCatalog {
    /** Get the catalog name */
    name(): string;
    /** List all namespaces */
    listNamespaces(parent?: string[]): Promise<string[][]>;
    /** Create a namespace */
    createNamespace(namespace: string[], properties?: NamespaceProperties): Promise<void>;
    /** Drop a namespace (must be empty) */
    dropNamespace(namespace: string[]): Promise<boolean>;
    /** Check if namespace exists */
    namespaceExists(namespace: string[]): Promise<boolean>;
    /** Get namespace properties */
    getNamespaceProperties(namespace: string[]): Promise<NamespaceProperties>;
    /** Update namespace properties */
    updateNamespaceProperties(namespace: string[], updates: Record<string, string>, removals: string[]): Promise<void>;
    /** List tables in a namespace */
    listTables(namespace: string[]): Promise<TableIdentifier[]>;
    /** Create a new table */
    createTable(namespace: string[], request: CreateTableRequest): Promise<TableMetadata>;
    /** Load table metadata */
    loadTable(identifier: TableIdentifier): Promise<TableMetadata>;
    /** Check if table exists */
    tableExists(identifier: TableIdentifier): Promise<boolean>;
    /** Drop a table */
    dropTable(identifier: TableIdentifier, purge?: boolean): Promise<boolean>;
    /** Rename a table */
    renameTable(from: TableIdentifier, to: TableIdentifier): Promise<void>;
    /** Commit changes to a table (atomic update) */
    commitTable(request: CommitTableRequest): Promise<CommitTableResponse>;
}
/**
 * File system based Iceberg catalog.
 *
 * Stores table metadata in JSON files on the underlying storage backend.
 * Uses atomic writes for catalog operations.
 *
 * Directory structure:
 * ```
 * warehouse/
 *   namespace1/
 *     table1/
 *       metadata/
 *         v1.metadata.json
 *         v2.metadata.json
 *         ...
 *       data/
 *         00000-0-<uuid>.parquet
 *         ...
 * ```
 */
export declare class FileSystemCatalog implements IcebergCatalog {
    private readonly catalogName;
    private readonly warehouse;
    private readonly storage;
    private readonly defaultProperties;
    constructor(config: {
        name: string;
        warehouse: string;
        storage: StorageBackend;
        defaultProperties?: Record<string, string>;
    });
    name(): string;
    private namespacePath;
    private tablePath;
    private metadataPath;
    listNamespaces(parent?: string[]): Promise<string[][]>;
    createNamespace(namespace: string[], properties?: NamespaceProperties): Promise<void>;
    dropNamespace(namespace: string[]): Promise<boolean>;
    namespaceExists(namespace: string[]): Promise<boolean>;
    getNamespaceProperties(namespace: string[]): Promise<NamespaceProperties>;
    updateNamespaceProperties(namespace: string[], updates: Record<string, string>, removals: string[]): Promise<void>;
    listTables(namespace: string[]): Promise<TableIdentifier[]>;
    createTable(namespace: string[], request: CreateTableRequest): Promise<TableMetadata>;
    loadTable(identifier: TableIdentifier): Promise<TableMetadata>;
    tableExists(identifier: TableIdentifier): Promise<boolean>;
    dropTable(identifier: TableIdentifier, purge?: boolean): Promise<boolean>;
    renameTable(from: TableIdentifier, to: TableIdentifier): Promise<void>;
    commitTable(request: CommitTableRequest): Promise<CommitTableResponse>;
}
/**
 * In-memory Iceberg catalog for testing.
 */
export declare class MemoryCatalog implements IcebergCatalog {
    private readonly catalogName;
    private readonly namespaces;
    private readonly tables;
    constructor(config: {
        name: string;
    });
    private namespaceKey;
    private tableKey;
    name(): string;
    listNamespaces(parent?: string[]): Promise<string[][]>;
    createNamespace(namespace: string[], properties?: NamespaceProperties): Promise<void>;
    dropNamespace(namespace: string[]): Promise<boolean>;
    namespaceExists(namespace: string[]): Promise<boolean>;
    getNamespaceProperties(namespace: string[]): Promise<NamespaceProperties>;
    updateNamespaceProperties(namespace: string[], updates: Record<string, string>, removals: string[]): Promise<void>;
    listTables(namespace: string[]): Promise<TableIdentifier[]>;
    createTable(namespace: string[], request: CreateTableRequest): Promise<TableMetadata>;
    loadTable(identifier: TableIdentifier): Promise<TableMetadata>;
    tableExists(identifier: TableIdentifier): Promise<boolean>;
    dropTable(identifier: TableIdentifier, _purge?: boolean): Promise<boolean>;
    renameTable(from: TableIdentifier, to: TableIdentifier): Promise<void>;
    commitTable(request: CommitTableRequest): Promise<CommitTableResponse>;
    /**
     * Clear all data (for testing).
     */
    clear(): void;
}
/**
 * Create an Iceberg catalog from configuration.
 */
export declare function createCatalog(config: CatalogConfig): IcebergCatalog;
//# sourceMappingURL=filesystem.d.ts.map
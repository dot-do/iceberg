/**
 * Iceberg Type Definitions
 *
 * Core type definitions for Apache Iceberg format.
 *
 * @see https://iceberg.apache.org/spec/
 */
/** Iceberg primitive types */
export type IcebergPrimitiveType = 'boolean' | 'int' | 'long' | 'float' | 'double' | 'decimal' | 'date' | 'time' | 'timestamp' | 'timestamptz' | 'string' | 'uuid' | 'fixed' | 'binary';
/** Iceberg list type */
export interface IcebergListType {
    readonly type: 'list';
    readonly 'element-id': number;
    readonly element: IcebergType;
    readonly 'element-required': boolean;
}
/** Iceberg map type */
export interface IcebergMapType {
    readonly type: 'map';
    readonly 'key-id': number;
    readonly 'value-id': number;
    readonly key: IcebergType;
    readonly value: IcebergType;
    readonly 'value-required': boolean;
}
/** Iceberg struct field */
export interface IcebergStructField {
    readonly id: number;
    readonly name: string;
    readonly required: boolean;
    readonly type: IcebergType;
    readonly doc?: string;
}
/** Iceberg struct type */
export interface IcebergStructType {
    readonly type: 'struct';
    readonly fields: readonly IcebergStructField[];
}
/** Combined Iceberg type */
export type IcebergType = IcebergPrimitiveType | IcebergListType | IcebergMapType | IcebergStructType;
/** Iceberg schema definition */
export interface IcebergSchema {
    readonly 'schema-id': number;
    readonly type: 'struct';
    readonly fields: readonly IcebergStructField[];
}
/** Partition transform types */
export type PartitionTransform = 'identity' | 'bucket' | 'truncate' | 'year' | 'month' | 'day' | 'hour' | 'void';
/** Partition field specification */
export interface PartitionField {
    readonly 'source-id': number;
    readonly 'field-id': number;
    readonly name: string;
    readonly transform: PartitionTransform | string;
}
/** Partition specification */
export interface PartitionSpec {
    readonly 'spec-id': number;
    readonly fields: readonly PartitionField[];
}
/** Sort field definition */
export interface SortField {
    readonly 'source-id': number;
    readonly transform: string;
    readonly direction: 'asc' | 'desc';
    readonly 'null-order': 'nulls-first' | 'nulls-last';
}
/** Sort order specification */
export interface SortOrder {
    readonly 'order-id': number;
    readonly fields: readonly SortField[];
}
/** Data file content type */
export type ContentType = 'data' | 'position-deletes' | 'equality-deletes';
/** File format type */
export type FileFormat = 'parquet' | 'avro' | 'orc';
/** Column statistics for a data file */
export interface ColumnMetrics {
    readonly 'column-sizes'?: Record<number, number>;
    readonly 'value-counts'?: Record<number, number>;
    readonly 'null-value-counts'?: Record<number, number>;
    readonly 'nan-value-counts'?: Record<number, number>;
    readonly 'lower-bounds'?: Record<number, Uint8Array | string>;
    readonly 'upper-bounds'?: Record<number, Uint8Array | string>;
}
/**
 * Bloom filter metadata for a data file column.
 * References the bloom filter file and which columns it covers.
 */
export interface BloomFilterRef {
    /** Path to the bloom filter file */
    readonly 'bloom-filter-path'?: string;
    /** Size of the bloom filter file in bytes */
    readonly 'bloom-filter-size-in-bytes'?: number;
    /** Field IDs that have bloom filters in this file */
    readonly 'bloom-filter-columns'?: readonly number[];
}
/** Data file entry in a manifest */
export interface DataFile extends BloomFilterRef {
    readonly content: number;
    readonly 'file-path': string;
    readonly 'file-format': FileFormat;
    readonly 'partition': Record<string, unknown>;
    readonly 'record-count': number;
    readonly 'file-size-in-bytes': number;
    readonly 'column-sizes'?: Record<number, number>;
    readonly 'value-counts'?: Record<number, number>;
    readonly 'null-value-counts'?: Record<number, number>;
    readonly 'nan-value-counts'?: Record<number, number>;
    readonly 'lower-bounds'?: Record<number, Uint8Array | string>;
    readonly 'upper-bounds'?: Record<number, Uint8Array | string>;
    readonly 'key-metadata'?: Uint8Array;
    readonly 'split-offsets'?: readonly number[];
    readonly 'equality-ids'?: readonly number[];
    readonly 'sort-order-id'?: number;
}
/** Manifest entry status */
export type ManifestEntryStatus = 0 | 1 | 2;
/** Manifest entry (row in manifest file) */
export interface ManifestEntry {
    readonly status: ManifestEntryStatus;
    readonly 'snapshot-id': number;
    readonly 'sequence-number': number;
    readonly 'file-sequence-number': number;
    readonly 'data-file': DataFile;
}
/** Partition field summary in manifest list entries */
export interface PartitionFieldSummary {
    /** Whether any value in the partition field is null */
    readonly 'contains-null': boolean;
    /** Whether any value is NaN (for floating point fields) */
    readonly 'contains-nan'?: boolean;
    /** Lower bound of partition values (binary encoded) */
    readonly 'lower-bound'?: Uint8Array | unknown;
    /** Upper bound of partition values (binary encoded) */
    readonly 'upper-bound'?: Uint8Array | unknown;
}
/** Manifest file metadata (entry in manifest list) */
export interface ManifestFile {
    readonly 'manifest-path': string;
    readonly 'manifest-length': number;
    readonly 'partition-spec-id': number;
    readonly content: number;
    readonly 'sequence-number': number;
    readonly 'min-sequence-number': number;
    readonly 'added-snapshot-id': number;
    readonly 'added-files-count': number;
    readonly 'existing-files-count': number;
    readonly 'deleted-files-count': number;
    readonly 'added-rows-count': number;
    readonly 'existing-rows-count': number;
    readonly 'deleted-rows-count': number;
    readonly 'partitions'?: readonly PartitionFieldSummary[];
}
/** Snapshot summary statistics */
export interface SnapshotSummary {
    readonly operation: 'append' | 'replace' | 'overwrite' | 'delete';
    readonly 'added-data-files'?: string;
    readonly 'deleted-data-files'?: string;
    readonly 'added-records'?: string;
    readonly 'deleted-records'?: string;
    readonly 'added-files-size'?: string;
    readonly 'removed-files-size'?: string;
    readonly 'total-records'?: string;
    readonly 'total-files-size'?: string;
    readonly 'total-data-files'?: string;
    readonly [key: string]: string | undefined;
}
/** Snapshot reference (branch or tag) */
export interface SnapshotRef {
    readonly 'snapshot-id': number;
    readonly type: 'branch' | 'tag';
    readonly 'max-ref-age-ms'?: number;
    readonly 'max-snapshot-age-ms'?: number;
    readonly 'min-snapshots-to-keep'?: number;
}
/** Snapshot log entry */
export interface SnapshotLogEntry {
    readonly 'timestamp-ms': number;
    readonly 'snapshot-id': number;
}
/** Metadata log entry */
export interface MetadataLogEntry {
    readonly 'timestamp-ms': number;
    readonly 'metadata-file': string;
}
/** Snapshot definition */
export interface Snapshot {
    readonly 'snapshot-id': number;
    readonly 'parent-snapshot-id'?: number;
    readonly 'sequence-number': number;
    readonly 'timestamp-ms': number;
    readonly 'manifest-list': string;
    readonly summary: SnapshotSummary;
    readonly 'schema-id': number;
}
/** Iceberg table metadata (v2 format) */
export interface TableMetadata {
    readonly 'format-version': 2;
    readonly 'table-uuid': string;
    readonly location: string;
    readonly 'last-sequence-number': number;
    readonly 'last-updated-ms': number;
    readonly 'last-column-id': number;
    readonly 'current-schema-id': number;
    readonly schemas: readonly IcebergSchema[];
    readonly 'default-spec-id': number;
    readonly 'partition-specs': readonly PartitionSpec[];
    readonly 'last-partition-id': number;
    readonly 'default-sort-order-id': number;
    readonly 'sort-orders': readonly SortOrder[];
    readonly properties: Readonly<Record<string, string>>;
    readonly 'current-snapshot-id': number | null;
    readonly snapshots: readonly Snapshot[];
    readonly 'snapshot-log': readonly SnapshotLogEntry[];
    readonly 'metadata-log': readonly MetadataLogEntry[];
    readonly refs: Readonly<Record<string, SnapshotRef>>;
}
/**
 * Storage backend interface for Iceberg operations.
 * This is a simplified interface - implementations can use
 * various backends (filesystem, R2, S3, etc.)
 */
export interface StorageBackend {
    /** Get object by key */
    get(key: string): Promise<Uint8Array | null>;
    /** Put object */
    put(key: string, data: Uint8Array): Promise<void>;
    /** Delete object */
    delete(key: string): Promise<void>;
    /** List objects by prefix */
    list(prefix: string): Promise<string[]>;
    /** Check if object exists */
    exists(key: string): Promise<boolean>;
    /**
     * Put data only if the key doesn't exist.
     * Returns true if successful, false if key already exists.
     */
    putIfAbsent?(key: string, data: Uint8Array): Promise<boolean>;
    /**
     * Compare and swap - atomically update if current value matches expected.
     * Returns true if successful, false if value has changed.
     * If expected is null, the key must not exist for the operation to succeed.
     */
    compareAndSwap?(key: string, expected: Uint8Array | null, data: Uint8Array): Promise<boolean>;
}
//# sourceMappingURL=types.d.ts.map
/**
 * Iceberg Type Definitions
 *
 * Core type definitions for Apache Iceberg format.
 *
 * @see https://iceberg.apache.org/spec/
 */
/** Iceberg primitive types */
export type IcebergPrimitiveType = 'boolean' | 'int' | 'long' | 'float' | 'double' | 'decimal' | 'date' | 'time' | 'timestamp' | 'timestamptz' | 'timestamp_ns' | 'timestamptz_ns' | 'string' | 'uuid' | 'fixed' | 'binary' | 'variant' | 'unknown';
/**
 * Valid edge-interpolation algorithms for geography types.
 * These determine how edges between coordinates are calculated on the sphere.
 */
export type EdgeInterpolationAlgorithm = 'spherical' | 'vincenty' | 'thomas' | 'andoyer' | 'karney';
/** Array of valid edge-interpolation algorithms */
export declare const VALID_EDGE_INTERPOLATION_ALGORITHMS: readonly EdgeInterpolationAlgorithm[];
/** Default CRS for geospatial types */
export declare const GEOSPATIAL_DEFAULT_CRS = "OGC:CRS84";
/** Default edge-interpolation algorithm for geography types */
export declare const GEOSPATIAL_DEFAULT_ALGORITHM: EdgeInterpolationAlgorithm;
/**
 * Parsed information for a geometry type.
 * geometry(C) where C is the CRS parameter.
 */
export interface GeometryTypeInfo {
    readonly crs: string;
}
/**
 * Parsed information for a geography type.
 * geography(C, A) where C is the CRS and A is the edge-interpolation algorithm.
 */
export interface GeographyTypeInfo {
    readonly crs: string;
    readonly algorithm: EdgeInterpolationAlgorithm;
}
/**
 * Check if a type string represents a geospatial type (geometry or geography).
 */
export declare function isGeospatialType(type: string): boolean;
/**
 * Check if an algorithm name is a valid edge-interpolation algorithm.
 */
export declare function isValidEdgeInterpolationAlgorithm(algorithm: string): algorithm is EdgeInterpolationAlgorithm;
/**
 * Parse a geometry type string and extract its CRS parameter.
 * Returns null if the type is not a geometry type.
 *
 * @example
 * parseGeometryType('geometry') // { crs: 'OGC:CRS84' }
 * parseGeometryType('geometry(EPSG:4326)') // { crs: 'EPSG:4326' }
 */
export declare function parseGeometryType(type: string): GeometryTypeInfo | null;
/**
 * Parse a geography type string and extract its CRS and algorithm parameters.
 * Returns null if the type is not a geography type.
 *
 * @example
 * parseGeographyType('geography') // { crs: 'OGC:CRS84', algorithm: 'spherical' }
 * parseGeographyType('geography(EPSG:4326, vincenty)') // { crs: 'EPSG:4326', algorithm: 'vincenty' }
 */
export declare function parseGeographyType(type: string): GeographyTypeInfo | null;
/**
 * Serialize a GeometryTypeInfo back to its string representation.
 * Uses the compact form if using default CRS.
 *
 * @example
 * serializeGeometryType({ crs: 'OGC:CRS84' }) // 'geometry'
 * serializeGeometryType({ crs: 'EPSG:4326' }) // 'geometry(EPSG:4326)'
 */
export declare function serializeGeometryType(info: GeometryTypeInfo): string;
/**
 * Serialize a GeographyTypeInfo back to its string representation.
 * Uses the compact form if using default CRS and algorithm.
 *
 * @example
 * serializeGeographyType({ crs: 'OGC:CRS84', algorithm: 'spherical' }) // 'geography'
 * serializeGeographyType({ crs: 'EPSG:4326', algorithm: 'karney' }) // 'geography(EPSG:4326, karney)'
 */
export declare function serializeGeographyType(info: GeographyTypeInfo): string;
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
    /**
     * Default value for reading rows written before this field was added.
     * Cannot be changed once set. Required for adding new required fields.
     * @see https://iceberg.apache.org/spec/#default-values
     */
    readonly 'initial-default'?: unknown;
    /**
     * Default value for writing new rows that don't specify a value.
     * Can be changed through schema evolution.
     * @see https://iceberg.apache.org/spec/#default-values
     */
    readonly 'write-default'?: unknown;
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
/**
 * Partition field specification.
 *
 * For single-argument transforms (most transforms), use 'source-id'.
 * For multi-argument transforms (future use case), use 'source-ids' array.
 *
 * A partition field must have either 'source-id' OR 'source-ids', but not both.
 *
 * @see https://iceberg.apache.org/spec/#partitioning
 */
export interface PartitionField {
    /** Source field ID for single-argument transforms */
    readonly 'source-id'?: number;
    /** Source field IDs for multi-argument transforms (Iceberg v3) */
    readonly 'source-ids'?: readonly number[];
    readonly 'field-id': number;
    readonly name: string;
    readonly transform: PartitionTransform | string;
}
/** Partition specification */
export interface PartitionSpec {
    readonly 'spec-id': number;
    readonly fields: readonly PartitionField[];
}
/**
 * Sort field definition.
 *
 * For single-argument transforms (most transforms), use 'source-id'.
 * For multi-argument transforms (future use case), use 'source-ids' array.
 *
 * A sort field must have either 'source-id' OR 'source-ids', but not both.
 *
 * @see https://iceberg.apache.org/spec/#sorting
 */
export interface SortField {
    /** Source field ID for single-argument transforms */
    readonly 'source-id'?: number;
    /** Source field IDs for multi-argument transforms (Iceberg v3) */
    readonly 'source-ids'?: readonly number[];
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
    /**
     * Byte offset of the deletion vector blob within a Puffin file.
     * Required for deletion vector entries (together with content-size-in-bytes).
     * @see https://iceberg.apache.org/spec/#deletion-vectors
     */
    readonly 'content-offset'?: number;
    /**
     * Size of the deletion vector blob in bytes within a Puffin file.
     * Required for deletion vector entries (together with content-offset).
     * @see https://iceberg.apache.org/spec/#deletion-vectors
     */
    readonly 'content-size-in-bytes'?: number;
    /**
     * Path to the data file that this deletion vector references.
     * Required for deletion vector entries - identifies which data file's rows are deleted.
     * @see https://iceberg.apache.org/spec/#deletion-vectors
     */
    readonly 'referenced-data-file'?: string;
    /**
     * First row ID assigned to rows in this data file (Iceberg v3).
     *
     * For ADDED files (status=1), this can be null to indicate the value should
     * be inherited from the manifest's first-row-id plus the cumulative record
     * counts of preceding files in the manifest.
     *
     * For EXISTING files (status=0), this must be an explicit non-null value.
     *
     * The row ID for any row in this file is: first-row-id + row_position
     * where row_position is the 0-based position of the row within the file.
     *
     * @see https://iceberg.apache.org/spec/#row-lineage
     */
    readonly 'first-row-id'?: number | null;
}
/** Result of validating deletion vector fields */
export interface DeletionVectorValidationResult {
    /** Whether the validation passed */
    readonly valid: boolean;
    /** List of validation error messages */
    readonly errors: readonly string[];
}
/**
 * Check if a DataFile represents a deletion vector.
 *
 * A deletion vector is a position delete file (content=1) that has all three
 * deletion vector fields: content-offset, content-size-in-bytes, and referenced-data-file.
 *
 * @param dataFile - The DataFile to check
 * @returns true if the DataFile is a deletion vector
 */
export declare function isDeletionVector(dataFile: DataFile): boolean;
/**
 * Validate deletion vector fields on a DataFile.
 *
 * Validates that:
 * - content-offset and content-size-in-bytes are provided together
 * - referenced-data-file is required when DV fields are present
 *
 * @param dataFile - The DataFile to validate
 * @returns Validation result with any errors
 */
export declare function validateDeletionVectorFields(dataFile: DataFile): DeletionVectorValidationResult;
/**
 * Calculate the row ID for a specific row in a data file.
 *
 * The row ID is calculated as: first_row_id + row_position
 * where row_position is the 0-based position of the row within the file.
 *
 * @param firstRowId - The first row ID assigned to this data file (or null/undefined)
 * @param rowPosition - The 0-based position of the row within the data file
 * @returns The unique row ID, or null if first_row_id is null or undefined
 *
 * @example
 * ```ts
 * // Data file with first-row-id of 5000
 * const rowId = calculateRowId(5000, 42);
 * console.log(rowId); // 5042
 *
 * // Data file with null first-row-id (inherits from manifest)
 * const rowId = calculateRowId(null, 42);
 * console.log(rowId); // null
 * ```
 *
 * @see https://iceberg.apache.org/spec/#row-lineage
 */
export declare function calculateRowId(firstRowId: number | null | undefined, rowPosition: number): number | null;
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
    /**
     * First row ID assigned to data files in this manifest (v3 only).
     * Used for row lineage tracking.
     * - number: explicit first row ID for this manifest
     * - null: inherit from manifest list context (based on cumulative row counts)
     * - undefined: field not present (v2 compatibility)
     * @see https://iceberg.apache.org/spec/#manifest-lists
     */
    readonly 'first-row-id'?: number | null;
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
    /**
     * The first row ID assigned to rows added by this snapshot (v3 only).
     * Equals the table's next-row-id at snapshot creation time.
     * Required for format-version 3, must be non-negative.
     * @see https://iceberg.apache.org/spec/#snapshots
     */
    readonly 'first-row-id'?: number;
    /**
     * Total number of rows added by this snapshot (v3 only).
     * Required for format-version 3, must be non-negative.
     * @see https://iceberg.apache.org/spec/#snapshots
     */
    readonly 'added-rows'?: number;
    /**
     * ID of the encryption key that encrypts the manifest list key metadata.
     * Optional - only present when table encryption is enabled.
     * References an encryption key from the table metadata's encryption-keys list.
     * @see https://iceberg.apache.org/docs/nightly/encryption/
     */
    readonly 'key-id'?: number;
}
/**
 * Encryption key metadata for table encryption.
 * Used to store encryption key references in table metadata.
 *
 * @see https://iceberg.apache.org/spec/#table-metadata
 */
export interface EncryptionKey {
    /**
     * Unique identifier for the encryption key.
     * Must be unique within the table's encryption-keys list.
     */
    readonly 'key-id': number;
    /**
     * Base64-encoded key metadata containing encrypted key material.
     * The actual encryption key is wrapped/encrypted and stored here.
     */
    readonly 'key-metadata': string;
}
/** Iceberg table metadata (v2/v3 format) */
export interface TableMetadata {
    readonly 'format-version': 2 | 3;
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
    /**
     * Next row ID to be assigned (v3 only).
     * Required for format-version 3, must be non-negative.
     * Used for row lineage tracking.
     */
    readonly 'next-row-id'?: number;
    /**
     * Optional list of encryption keys for table encryption.
     * Each key has a unique key-id and base64-encoded key-metadata.
     * @see https://iceberg.apache.org/spec/#table-metadata
     */
    readonly 'encryption-keys'?: readonly EncryptionKey[];
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
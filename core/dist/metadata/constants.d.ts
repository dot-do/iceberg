/**
 * Iceberg Metadata Constants
 *
 * Centralized constants for the Apache Iceberg TypeScript implementation.
 * These values are defined by the Iceberg specification and should not be changed.
 *
 * @see https://iceberg.apache.org/spec/
 */
/**
 * The Iceberg format version supported by this implementation.
 * This implementation supports Iceberg v2 format.
 * Default to v2 for backward compatibility.
 */
export declare const FORMAT_VERSION = 2;
/**
 * Iceberg format version 3.
 * v3 adds additional features while maintaining backward compatibility with v2.
 */
export declare const FORMAT_VERSION_3 = 3;
/**
 * The standard metadata directory name within a table location.
 * Metadata files are stored at: {table-location}/metadata/
 */
export declare const METADATA_DIR = "metadata";
/**
 * The version hint filename.
 * This file contains the current metadata version number or path.
 */
export declare const VERSION_HINT_FILENAME = "version-hint.text";
/**
 * The starting field ID for partition fields.
 * Partition field IDs start at 1000 per the Iceberg specification.
 * Schema field IDs use lower numbers (typically starting at 1).
 */
export declare const PARTITION_FIELD_ID_START = 1000;
/**
 * The initial value for last-partition-id when a table has no partition fields.
 * Set to 999 so the first partition field will have ID 1000.
 */
export declare const INITIAL_PARTITION_ID = 999;
/**
 * Reserved field ID for file_path in position delete schema.
 * Uses high field IDs to avoid conflicts with user schema field IDs.
 */
export declare const POSITION_DELETE_FILE_PATH_FIELD_ID = 2147483546;
/**
 * Reserved field ID for pos (position) in position delete schema.
 */
export declare const POSITION_DELETE_POS_FIELD_ID = 2147483545;
/**
 * Content type value for data files.
 */
export declare const CONTENT_DATA = 0;
/**
 * Content type value for position delete files.
 */
export declare const CONTENT_POSITION_DELETES = 1;
/**
 * Content type value for equality delete files.
 */
export declare const CONTENT_EQUALITY_DELETES = 2;
/**
 * Manifest content type for data manifests (containing data file references).
 */
export declare const MANIFEST_CONTENT_DATA = 0;
/**
 * Manifest content type for delete manifests (containing delete file references).
 */
export declare const MANIFEST_CONTENT_DELETES = 1;
/**
 * Manifest entry status indicating an existing entry (carried forward from previous snapshot).
 */
export declare const MANIFEST_ENTRY_STATUS_EXISTING = 0;
/**
 * Manifest entry status indicating a newly added entry.
 */
export declare const MANIFEST_ENTRY_STATUS_ADDED = 1;
/**
 * Manifest entry status indicating a deleted entry.
 */
export declare const MANIFEST_ENTRY_STATUS_DELETED = 2;
/**
 * Default schema ID for the initial schema.
 */
export declare const DEFAULT_SCHEMA_ID = 0;
/**
 * Default partition spec ID for the initial partition spec.
 */
export declare const DEFAULT_SPEC_ID = 0;
/**
 * Default sort order ID for an unsorted table.
 */
export declare const DEFAULT_SORT_ORDER_ID = 0;
/**
 * Internal schema ID for the position delete schema.
 */
export declare const POSITION_DELETE_SCHEMA_ID = -1;
/**
 * Internal schema ID for equality delete schemas.
 */
export declare const EQUALITY_DELETE_SCHEMA_ID = -2;
/** Default maximum number of commit retries */
export declare const COMMIT_MAX_RETRIES = 5;
/** Default base delay for exponential backoff in milliseconds */
export declare const DEFAULT_BASE_RETRY_DELAY_MS = 100;
/** Default maximum delay for exponential backoff in milliseconds */
export declare const DEFAULT_MAX_RETRY_DELAY_MS = 5000;
/** Default jitter factor for retry delays (0-1) */
export declare const DEFAULT_RETRY_JITTER = 0.2;
/** Default number of metadata versions to retain */
export declare const METADATA_RETAIN_VERSIONS = 10;
/** Default maximum age of metadata files to retain (7 days in milliseconds) */
export declare const METADATA_MAX_AGE_MS: number;
/**
 * Milliseconds per day. Used for day partition transform calculations.
 */
export declare const MS_PER_DAY: number;
/**
 * Milliseconds per hour. Used for hour partition transform calculations.
 */
export declare const MS_PER_HOUR: number;
/**
 * The Unix epoch year (1970). Used for temporal partition transforms.
 */
export declare const EPOCH_YEAR = 1970;
//# sourceMappingURL=constants.d.ts.map
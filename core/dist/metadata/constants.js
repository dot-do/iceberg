/**
 * Iceberg Metadata Constants
 *
 * Centralized constants for the Apache Iceberg TypeScript implementation.
 * These values are defined by the Iceberg specification and should not be changed.
 *
 * @see https://iceberg.apache.org/spec/
 */
// ============================================================================
// Format Version
// ============================================================================
/**
 * The Iceberg format version supported by this implementation.
 * This implementation supports Iceberg v2 format.
 * Default to v2 for backward compatibility.
 */
export const FORMAT_VERSION = 2;
/**
 * Iceberg format version 3.
 * v3 adds additional features while maintaining backward compatibility with v2.
 */
export const FORMAT_VERSION_3 = 3;
// ============================================================================
// Metadata Directory
// ============================================================================
/**
 * The standard metadata directory name within a table location.
 * Metadata files are stored at: {table-location}/metadata/
 */
export const METADATA_DIR = 'metadata';
/**
 * The version hint filename.
 * This file contains the current metadata version number or path.
 */
export const VERSION_HINT_FILENAME = 'version-hint.text';
// ============================================================================
// Field IDs
// ============================================================================
/**
 * The starting field ID for partition fields.
 * Partition field IDs start at 1000 per the Iceberg specification.
 * Schema field IDs use lower numbers (typically starting at 1).
 */
export const PARTITION_FIELD_ID_START = 1000;
/**
 * The initial value for last-partition-id when a table has no partition fields.
 * Set to 999 so the first partition field will have ID 1000.
 */
export const INITIAL_PARTITION_ID = 999;
// ============================================================================
// Reserved Field IDs (Position Delete Schema)
// ============================================================================
/**
 * Reserved field ID for file_path in position delete schema.
 * Uses high field IDs to avoid conflicts with user schema field IDs.
 */
export const POSITION_DELETE_FILE_PATH_FIELD_ID = 2147483546;
/**
 * Reserved field ID for pos (position) in position delete schema.
 */
export const POSITION_DELETE_POS_FIELD_ID = 2147483545;
// ============================================================================
// Content Types
// ============================================================================
/**
 * Content type value for data files.
 */
export const CONTENT_DATA = 0;
/**
 * Content type value for position delete files.
 */
export const CONTENT_POSITION_DELETES = 1;
/**
 * Content type value for equality delete files.
 */
export const CONTENT_EQUALITY_DELETES = 2;
// ============================================================================
// Manifest Content Types
// ============================================================================
/**
 * Manifest content type for data manifests (containing data file references).
 */
export const MANIFEST_CONTENT_DATA = 0;
/**
 * Manifest content type for delete manifests (containing delete file references).
 */
export const MANIFEST_CONTENT_DELETES = 1;
// ============================================================================
// Manifest Entry Status
// ============================================================================
/**
 * Manifest entry status indicating an existing entry (carried forward from previous snapshot).
 */
export const MANIFEST_ENTRY_STATUS_EXISTING = 0;
/**
 * Manifest entry status indicating a newly added entry.
 */
export const MANIFEST_ENTRY_STATUS_ADDED = 1;
/**
 * Manifest entry status indicating a deleted entry.
 */
export const MANIFEST_ENTRY_STATUS_DELETED = 2;
// ============================================================================
// Default Schema and Spec IDs
// ============================================================================
/**
 * Default schema ID for the initial schema.
 */
export const DEFAULT_SCHEMA_ID = 0;
/**
 * Default partition spec ID for the initial partition spec.
 */
export const DEFAULT_SPEC_ID = 0;
/**
 * Default sort order ID for an unsorted table.
 */
export const DEFAULT_SORT_ORDER_ID = 0;
// ============================================================================
// Internal Schema IDs
// ============================================================================
/**
 * Internal schema ID for the position delete schema.
 */
export const POSITION_DELETE_SCHEMA_ID = -1;
/**
 * Internal schema ID for equality delete schemas.
 */
export const EQUALITY_DELETE_SCHEMA_ID = -2;
// ============================================================================
// Commit Constants (re-exported from commit.ts for convenience)
// ============================================================================
/** Default maximum number of commit retries */
export const COMMIT_MAX_RETRIES = 5;
/** Default base delay for exponential backoff in milliseconds */
export const DEFAULT_BASE_RETRY_DELAY_MS = 100;
/** Default maximum delay for exponential backoff in milliseconds */
export const DEFAULT_MAX_RETRY_DELAY_MS = 5000;
/** Default jitter factor for retry delays (0-1) */
export const DEFAULT_RETRY_JITTER = 0.2;
/** Default number of metadata versions to retain */
export const METADATA_RETAIN_VERSIONS = 10;
/** Default maximum age of metadata files to retain (7 days in milliseconds) */
export const METADATA_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
// ============================================================================
// Time Constants
// ============================================================================
/**
 * Milliseconds per day. Used for day partition transform calculations.
 */
export const MS_PER_DAY = 24 * 60 * 60 * 1000;
/**
 * Milliseconds per hour. Used for hour partition transform calculations.
 */
export const MS_PER_HOUR = 60 * 60 * 1000;
/**
 * The Unix epoch year (1970). Used for temporal partition transforms.
 */
export const EPOCH_YEAR = 1970;
//# sourceMappingURL=constants.js.map
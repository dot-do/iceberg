/**
 * @dot-do/iceberg
 *
 * Apache Iceberg implementation for TypeScript.
 * Provides metadata, manifest, and catalog operations for Iceberg tables.
 *
 * @example
 * ```ts
 * import {
 *   MetadataWriter,
 *   ManifestGenerator,
 *   SnapshotBuilder,
 *   createDefaultSchema,
 * } from '@dot-do/iceberg';
 *
 * // Create table metadata
 * const writer = new MetadataWriter(storage);
 * const result = await writer.writeNewTable({
 *   location: 's3://bucket/warehouse/db/table',
 * });
 *
 * // Create a manifest
 * const manifest = new ManifestGenerator({
 *   sequenceNumber: 1,
 *   snapshotId: Date.now(),
 * });
 * manifest.addDataFile({
 *   'file-path': 's3://bucket/data/file.parquet',
 *   'file-format': 'parquet',
 *   'record-count': 1000,
 *   'file-size-in-bytes': 4096,
 *   partition: {},
 * });
 * ```
 *
 * @see https://iceberg.apache.org/spec/
 */

// ============================================================================
// Core Metadata Exports (Tier 1)
// ============================================================================

// Types
export type {
  // Primitive and complex types
  IcebergPrimitiveType,
  IcebergListType,
  IcebergMapType,
  IcebergStructField,
  IcebergStructType,
  IcebergType,
  IcebergSchema,
  // Partition and sort
  PartitionTransform,
  PartitionField,
  PartitionSpec,
  SortField,
  SortOrder,
  // Data files
  ContentType,
  FileFormat,
  ColumnMetrics,
  BloomFilterRef,
  DataFile,
  // Manifest
  ManifestEntryStatus,
  ManifestEntry,
  ManifestFile,
  PartitionFieldSummary,
  // Snapshot
  SnapshotSummary,
  SnapshotRef,
  SnapshotLogEntry,
  MetadataLogEntry,
  Snapshot,
  // Table metadata
  TableMetadata,
  // Storage
  StorageBackend,
} from './metadata/index.js';

// Reader
export {
  readTableMetadata,
  readMetadataFromPath,
  parseTableMetadata,
  getCurrentVersion,
  getSnapshotAtTimestamp,
  getSnapshotByRef,
  getSnapshotById,
  getCurrentSnapshot,
  listMetadataFiles,
} from './metadata/index.js';

// Writer
export {
  MetadataWriter,
  writeNewTableMetadata,
  writeMetadataIfMissing,
  type MetadataWriterOptions,
  type MetadataWriteResult,
} from './metadata/index.js';

// Manifest
export {
  ManifestGenerator,
  ManifestListGenerator,
  createDataFileStats,
  type ManifestWriterOptions,
  type ManifestListWriterOptions,
} from './metadata/index.js';

// Snapshot
export {
  SnapshotBuilder,
  TableMetadataBuilder,
  SnapshotManager,
  generateUUID,
  createTableWithSnapshot,
  type CreateSnapshotOptions,
  type CreateTableOptions,
  type SnapshotRetentionPolicy,
  type ExpireSnapshotsResult,
} from './metadata/index.js';

// Schema
export {
  // Schema creation
  createDefaultSchema,
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createBucketPartitionSpec,
  createTimePartitionSpec,
  createUnsortedOrder,
  createSortOrder,
  // Schema conversion
  parquetToIcebergType,
  parquetSchemaToIceberg,
  type ParquetTypeName,
  type ParquetConvertedType,
  type ParquetSchemaElement,
  // Schema evolution (legacy)
  validateSchemaEvolution,
  generateSchemaId,
  findMaxFieldId,
  type SchemaChangeType,
  type SchemaChange,
  type SchemaComparisonResult,
} from './metadata/index.js';

// Schema Evolution (advanced)
export {
  // Type compatibility
  isTypePromotionAllowed,
  areTypesCompatible,
  type TypeCompatibilityResult,
  // Error handling
  SchemaEvolutionError,
  type SchemaEvolutionErrorCode,
  // Operation types
  type SchemaEvolutionOperation,
  type AddColumnOperation,
  type DropColumnOperation,
  type RenameColumnOperation,
  type UpdateColumnTypeOperation,
  type MakeColumnOptionalOperation,
  type MakeColumnRequiredOperation,
  type UpdateColumnDocOperation,
  type MoveColumnOperation,
  type ColumnPosition,
  // Builder
  SchemaEvolutionBuilder,
  type SchemaValidationResult,
  type SchemaEvolutionResult,
  // Schema comparison
  compareSchemas,
  type SchemaChangeKind,
  type SchemaChangeSummary,
  // Compatibility checking
  isBackwardCompatible,
  isForwardCompatible,
  isFullyCompatible,
  type CompatibilityResult,
  // Schema history
  getSchemaHistory,
  getSchemaForSnapshot,
  getSchemaChangesBetween,
  type SchemaHistoryEntry,
  // Field utilities
  findFieldByName,
  findFieldById,
  getAllFieldIds,
  // Helper functions
  evolveSchema,
  applySchemaEvolution,
  evolveNestedStruct,
  // Field ID management
  FieldIdManager,
} from './metadata/index.js';

// Column Statistics
export {
  // Types
  type ColumnStatistics,
  type ColumnStatsCollector,
  type ComputeStatsOptions,
  type ComputedFileStats,
  type ColumnPartitionFieldSummary,
  type ZoneMap,
  // Collectors
  createColumnStatsCollector,
  FileStatsCollector,
  // Encoding
  encodeFileStats,
  applyStatsToDataFile,
  // Aggregation
  aggregateColumnStats,
  computePartitionSummaries,
  // Zone map helpers
  createZoneMapFromStats,
  canPruneZoneMap,
  // Utilities
  getPrimitiveType,
  getComparator,
  estimateValueSize,
  truncateUpperBound,
} from './metadata/index.js';

// Partition Transforms
export {
  // Transform functions
  applyTransform,
  parseTransform,
  formatTransform,
  getTransformResultType,
  // Partition data
  getPartitionData,
  getPartitionPath,
  parsePartitionPath,
  // Builder
  PartitionSpecBuilder,
  createPartitionSpecBuilder,
  createPartitionSpecFromDefinitions,
  // Statistics
  PartitionStatsCollector,
  createPartitionStatsCollector,
  // Evolution
  comparePartitionSpecs,
  findMaxPartitionFieldId,
  generatePartitionSpecId,
  // Types
  type ParsedTransform,
  type PartitionFieldDefinition,
  type CreatePartitionSpecOptions,
  type PartitionValue,
  type PartitionedFile,
  type PartitionStats,
  type PartitionStatsAggregate,
  type PartitionSpecChangeType,
  type PartitionSpecChange,
  type PartitionSpecComparisonResult,
} from './metadata/index.js';

// Row-Level Deletes
export {
  // Constants
  CONTENT_DATA,
  CONTENT_POSITION_DELETES,
  CONTENT_EQUALITY_DELETES,
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
  // Position delete schema
  POSITION_DELETE_SCHEMA,
  // Builders
  PositionDeleteBuilder,
  EqualityDeleteBuilder,
  DeleteManifestGenerator,
  // Lookups for read-side application
  PositionDeleteLookup,
  EqualityDeleteLookup,
  // Merger for compaction
  DeleteMerger,
  // Parsers
  parsePositionDeleteFile,
  parseEqualityDeleteFile,
  // Application
  applyDeletes,
  // Type guards
  isDeleteFile,
  isPositionDeleteFile,
  isEqualityDeleteFile,
  // Utilities
  getDeleteContentTypeName,
  createEqualityDeleteSchema,
  // Types
  type PositionDelete,
  type EqualityDelete,
  type DeleteFile,
  type PositionDeleteFile,
  type EqualityDeleteFile,
  type PositionDeleteBuilderOptions,
  type EqualityDeleteBuilderOptions,
  type DeleteFileResult,
  type PositionDeleteStatistics,
  type EqualityDeleteStatistics,
  type DeleteApplicationResult,
  type DeleteMergerOptions,
} from './metadata/index.js';

// Atomic Commits
export {
  // Classes
  AtomicCommitter,
  // Errors
  CommitConflictError,
  CommitRetryExhaustedError,
  CommitTransactionError,
  // Functions
  createAtomicCommitter,
  commitWithCleanup,
  generateVersionedMetadataPath,
  parseMetadataVersion,
  getVersionHintPath,
  getMetadataVersion,
  // Constants
  COMMIT_MAX_RETRIES,
  DEFAULT_BASE_RETRY_DELAY_MS,
  DEFAULT_MAX_RETRY_DELAY_MS,
  DEFAULT_RETRY_JITTER,
  METADATA_RETAIN_VERSIONS,
  METADATA_MAX_AGE_MS,
  // Types
  type CommitOptions,
  type MetadataCleanupOptions,
  type CleanupFailureEvent,
  type ConflictResolutionStrategy,
  type PendingCommit,
  type CommitResult,
} from './metadata/index.js';

// ============================================================================
// Avro Exports
// ============================================================================

export {
  // Types
  type AvroPrimitive,
  type AvroArray,
  type AvroMap,
  type AvroFixed,
  type AvroEnum,
  type AvroRecordField,
  type AvroRecord,
  type AvroUnion,
  type AvroType,
  // Classes
  AvroEncoder,
  AvroFileWriter,
  // Schema builders
  createManifestEntrySchema,
  createManifestListSchema,
  // Utilities
  encodeStatValue,
  truncateString,
} from './avro/index.js';

// ============================================================================
// Bloom Filter Exports
// ============================================================================

export {
  // Core bloom filter
  BloomFilter,
  BloomFilterWriter,
  // Types
  type BloomFilterOptions,
  type BloomFilterMetadata,
  type SerializedBloomFilter,
  type BloomFilterFileRef,
  type BloomFilterWriterOptions,
  type ParsedBloomFilterEntry,
  // Hash functions
  xxh64,
  xxh64String,
  xxh64Number,
  xxh64BigInt,
  // Utility functions
  calculateOptimalBlocks,
  estimateFalsePositiveRate,
  parseBloomFilterFile,
  createBloomFilterMap,
  shouldReadFile,
  shouldReadFileForAny,
  getBloomFilterPath,
} from './bloom/index.js';

// ============================================================================
// Constants Exports
// ============================================================================

export {
  // Format version
  FORMAT_VERSION,
  // Metadata directory
  METADATA_DIR,
  VERSION_HINT_FILENAME,
  // Field IDs
  PARTITION_FIELD_ID_START,
  INITIAL_PARTITION_ID,
  POSITION_DELETE_FILE_PATH_FIELD_ID,
  POSITION_DELETE_POS_FIELD_ID,
  // Content types (also exported from metadata for backward compatibility)
  // CONTENT_DATA,
  // CONTENT_POSITION_DELETES,
  // CONTENT_EQUALITY_DELETES,
  // Manifest content types (also exported from metadata for backward compatibility)
  // MANIFEST_CONTENT_DATA,
  // MANIFEST_CONTENT_DELETES,
  // Manifest entry status
  MANIFEST_ENTRY_STATUS_EXISTING,
  MANIFEST_ENTRY_STATUS_ADDED,
  MANIFEST_ENTRY_STATUS_DELETED,
  // Default IDs
  DEFAULT_SCHEMA_ID,
  DEFAULT_SPEC_ID,
  DEFAULT_SORT_ORDER_ID,
  // Internal schema IDs
  POSITION_DELETE_SCHEMA_ID,
  EQUALITY_DELETE_SCHEMA_ID,
  // Time constants
  MS_PER_DAY,
  MS_PER_HOUR,
  EPOCH_YEAR,
} from './metadata/constants.js';

// ============================================================================
// Error Classes Exports
// ============================================================================

export {
  // Base error
  IcebergError,
  // Metadata errors
  MetadataError,
  type MetadataErrorCode,
  // Catalog errors
  CatalogError,
  type CatalogErrorCode,
  // Storage errors
  StorageError,
  type StorageErrorCode,
  // Validation errors
  ValidationError,
  type ValidationErrorCode,
  // Commit errors (also exported from metadata for backward compatibility)
  // CommitConflictError,
  // CommitRetryExhaustedError,
  // CommitTransactionError,
  // Schema evolution errors (also exported from metadata for backward compatibility)
  // SchemaEvolutionError,
  // type SchemaEvolutionErrorCode,
  // Transform errors
  TransformError,
  // Type guards
  isIcebergError,
  isMetadataError,
  isCatalogError,
  isStorageError,
  isValidationError,
  isCommitConflictError,
  isSchemaEvolutionError,
  wrapError,
} from './errors.js';

// ============================================================================
// Utility Exports
// ============================================================================

export {
  // Path validation utilities
  validatePath,
  sanitizePath,
  isAbsolutePath,
  joinPaths,
  getParentPath,
  getBasename,
} from './utils/index.js';

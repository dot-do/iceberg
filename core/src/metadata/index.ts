/**
 * Iceberg Metadata Module
 *
 * Core metadata operations for Apache Iceberg tables.
 *
 * @see https://iceberg.apache.org/spec/
 */

// Constants
export {
  // Format version
  FORMAT_VERSION,
  FORMAT_VERSION_3,
  // Metadata directory
  METADATA_DIR,
  VERSION_HINT_FILENAME,
  // Field IDs
  PARTITION_FIELD_ID_START,
  INITIAL_PARTITION_ID,
  POSITION_DELETE_FILE_PATH_FIELD_ID,
  POSITION_DELETE_POS_FIELD_ID,
  // Content types
  CONTENT_DATA,
  CONTENT_POSITION_DELETES,
  CONTENT_EQUALITY_DELETES,
  // Manifest content types
  MANIFEST_CONTENT_DATA,
  MANIFEST_CONTENT_DELETES,
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
  // Commit constants
  COMMIT_MAX_RETRIES,
  DEFAULT_BASE_RETRY_DELAY_MS,
  DEFAULT_MAX_RETRY_DELAY_MS,
  DEFAULT_RETRY_JITTER,
  METADATA_RETAIN_VERSIONS,
  METADATA_MAX_AGE_MS,
  // Time constants
  MS_PER_DAY,
  MS_PER_HOUR,
  EPOCH_YEAR,
} from './constants.js';

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
  // Geospatial types (v3)
  EdgeInterpolationAlgorithm,
  GeometryTypeInfo,
  GeographyTypeInfo,
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
  // Deletion vector validation
  DeletionVectorValidationResult,
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
  // Encryption
  EncryptionKey,
  // Storage
  StorageBackend,
} from './types.js';

// Geospatial type utilities (v3)
export {
  // Constants
  VALID_EDGE_INTERPOLATION_ALGORITHMS,
  GEOSPATIAL_DEFAULT_CRS,
  GEOSPATIAL_DEFAULT_ALGORITHM,
  // Functions
  isGeospatialType,
  isValidEdgeInterpolationAlgorithm,
  parseGeometryType,
  parseGeographyType,
  serializeGeometryType,
  serializeGeographyType,
  // Deletion vector utilities
  isDeletionVector,
  validateDeletionVectorFields,
  // Row lineage utilities (v3)
  calculateRowId,
} from './types.js';

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
} from './reader.js';

// Writer
export {
  MetadataWriter,
  writeNewTableMetadata,
  writeMetadataIfMissing,
  type MetadataWriterOptions,
  type MetadataWriteResult,
} from './writer.js';

// Manifest
export {
  ManifestGenerator,
  ManifestListGenerator,
  createDataFileStats,
  type ManifestWriterOptions,
  type ManifestListWriterOptions,
} from './manifest.js';

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
} from './snapshot.js';

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
  // Unknown type validation (v3)
  validateUnknownTypeField,
  validateSchema,
  type FieldValidationResult,
  // Default values validation (v3)
  validateFieldDefault,
  canChangeInitialDefault,
  canChangeWriteDefault,
  type FieldDefaultValidationOptions,
  type DefaultChangeResult,
} from './schema.js';

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
} from './schema-evolution.js';

// Column Statistics
export {
  // Types
  type ColumnStatistics,
  type ColumnStatsCollector,
  type ComputeStatsOptions,
  type ComputedFileStats,
  type PartitionFieldSummary as ColumnPartitionFieldSummary,
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
} from './column-stats.js';

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
} from './partition.js';

// Row-Level Deletes
export {
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
} from './deletes.js';

// Commit (Atomic Commits with Optimistic Concurrency Control)
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
  // Types
  type CommitOptions,
  type MetadataCleanupOptions,
  type CleanupFailureEvent,
  type ConflictResolutionStrategy,
  type PendingCommit,
  type CommitResult,
} from './commit.js';

// Deletion Vectors (v3 feature)
export {
  // Entry creation
  createDeletionVectorEntry,
  // Scan planning
  findDeletionVectorsForFile,
  shouldIgnorePositionDeletes,
  // V3 validation
  validateV3DeletionVectorRules,
  countDeletionVectorsPerDataFile,
  // Types
  type CreateDeletionVectorOptions,
  type V3DeletionVectorValidationResult,
} from './deletion-vectors.js';

// Version Upgrade (v2 to v3)
export {
  // Upgrade functions
  upgradeTableToV3,
  upgradeTableToV3WithOptions,
  canUpgradeToV3,
  // Error handling
  VersionUpgradeError,
  // Types
  type VersionUpgradeErrorCode,
  type UpgradeOptions,
} from './upgrade.js';

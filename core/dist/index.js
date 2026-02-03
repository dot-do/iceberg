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
// Deletion Vector Helpers
export { isDeletionVector, validateDeletionVectorFields, } from './metadata/index.js';
// Reader
export { readTableMetadata, readMetadataFromPath, parseTableMetadata, getCurrentVersion, getSnapshotAtTimestamp, getSnapshotByRef, getSnapshotById, getCurrentSnapshot, listMetadataFiles, } from './metadata/index.js';
// Writer
export { MetadataWriter, writeNewTableMetadata, writeMetadataIfMissing, } from './metadata/index.js';
// Manifest
export { ManifestGenerator, ManifestListGenerator, createDataFileStats, } from './metadata/index.js';
// Snapshot
export { SnapshotBuilder, TableMetadataBuilder, SnapshotManager, generateUUID, createTableWithSnapshot, } from './metadata/index.js';
// Schema
export { 
// Schema creation
createDefaultSchema, createUnpartitionedSpec, createIdentityPartitionSpec, createBucketPartitionSpec, createTimePartitionSpec, createUnsortedOrder, createSortOrder, 
// Schema conversion
parquetToIcebergType, parquetSchemaToIceberg, 
// Schema evolution (legacy)
validateSchemaEvolution, generateSchemaId, findMaxFieldId, } from './metadata/index.js';
// Schema Evolution (advanced)
export { 
// Type compatibility
isTypePromotionAllowed, areTypesCompatible, 
// Error handling
SchemaEvolutionError, 
// Builder
SchemaEvolutionBuilder, 
// Schema comparison
compareSchemas, 
// Compatibility checking
isBackwardCompatible, isForwardCompatible, isFullyCompatible, 
// Schema history
getSchemaHistory, getSchemaForSnapshot, getSchemaChangesBetween, 
// Field utilities
findFieldByName, findFieldById, getAllFieldIds, 
// Helper functions
evolveSchema, applySchemaEvolution, evolveNestedStruct, 
// Field ID management
FieldIdManager, } from './metadata/index.js';
// Column Statistics
export { 
// Collectors
createColumnStatsCollector, FileStatsCollector, 
// Encoding
encodeFileStats, applyStatsToDataFile, 
// Aggregation
aggregateColumnStats, computePartitionSummaries, 
// Zone map helpers
createZoneMapFromStats, canPruneZoneMap, 
// Utilities
getPrimitiveType, getComparator, estimateValueSize, truncateUpperBound, } from './metadata/index.js';
// Partition Transforms
export { 
// Transform functions
applyTransform, parseTransform, formatTransform, getTransformResultType, 
// Partition data
getPartitionData, getPartitionPath, parsePartitionPath, 
// Builder
PartitionSpecBuilder, createPartitionSpecBuilder, createPartitionSpecFromDefinitions, 
// Statistics
PartitionStatsCollector, createPartitionStatsCollector, 
// Evolution
comparePartitionSpecs, findMaxPartitionFieldId, generatePartitionSpecId, } from './metadata/index.js';
// Row-Level Deletes
export { 
// Constants
CONTENT_DATA, CONTENT_POSITION_DELETES, CONTENT_EQUALITY_DELETES, MANIFEST_CONTENT_DATA, MANIFEST_CONTENT_DELETES, 
// Position delete schema
POSITION_DELETE_SCHEMA, 
// Builders
PositionDeleteBuilder, EqualityDeleteBuilder, DeleteManifestGenerator, 
// Lookups for read-side application
PositionDeleteLookup, EqualityDeleteLookup, 
// Merger for compaction
DeleteMerger, 
// Parsers
parsePositionDeleteFile, parseEqualityDeleteFile, 
// Application
applyDeletes, 
// Type guards
isDeleteFile, isPositionDeleteFile, isEqualityDeleteFile, 
// Utilities
getDeleteContentTypeName, createEqualityDeleteSchema, } from './metadata/index.js';
// Atomic Commits
export { 
// Classes
AtomicCommitter, 
// Errors
CommitConflictError, CommitRetryExhaustedError, CommitTransactionError, 
// Functions
createAtomicCommitter, commitWithCleanup, generateVersionedMetadataPath, parseMetadataVersion, getVersionHintPath, getMetadataVersion, 
// Constants
COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS, } from './metadata/index.js';
// ============================================================================
// Avro Exports
// ============================================================================
export { 
// Classes
AvroEncoder, AvroFileWriter, 
// Schema builders
createManifestEntrySchema, createManifestListSchema, 
// Utilities
encodeStatValue, truncateString, } from './avro/index.js';
// ============================================================================
// Bloom Filter Exports
// ============================================================================
export { 
// Core bloom filter
BloomFilter, BloomFilterWriter, 
// Hash functions
xxh64, xxh64String, xxh64Number, xxh64BigInt, 
// Utility functions
calculateOptimalBlocks, estimateFalsePositiveRate, parseBloomFilterFile, createBloomFilterMap, shouldReadFile, shouldReadFileForAny, getBloomFilterPath, } from './bloom/index.js';
// ============================================================================
// Constants Exports
// ============================================================================
export { 
// Format version
FORMAT_VERSION, FORMAT_VERSION_3, 
// Metadata directory
METADATA_DIR, VERSION_HINT_FILENAME, 
// Field IDs
PARTITION_FIELD_ID_START, INITIAL_PARTITION_ID, POSITION_DELETE_FILE_PATH_FIELD_ID, POSITION_DELETE_POS_FIELD_ID, 
// Content types (also exported from metadata for backward compatibility)
// CONTENT_DATA,
// CONTENT_POSITION_DELETES,
// CONTENT_EQUALITY_DELETES,
// Manifest content types (also exported from metadata for backward compatibility)
// MANIFEST_CONTENT_DATA,
// MANIFEST_CONTENT_DELETES,
// Manifest entry status
MANIFEST_ENTRY_STATUS_EXISTING, MANIFEST_ENTRY_STATUS_ADDED, MANIFEST_ENTRY_STATUS_DELETED, 
// Default IDs
DEFAULT_SCHEMA_ID, DEFAULT_SPEC_ID, DEFAULT_SORT_ORDER_ID, 
// Internal schema IDs
POSITION_DELETE_SCHEMA_ID, EQUALITY_DELETE_SCHEMA_ID, 
// Time constants
MS_PER_DAY, MS_PER_HOUR, EPOCH_YEAR, } from './metadata/constants.js';
// ============================================================================
// Error Classes Exports
// ============================================================================
export { 
// Base error
IcebergError, 
// Metadata errors
MetadataError, 
// Catalog errors
CatalogError, 
// Storage errors
StorageError, 
// Validation errors
ValidationError, 
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
isIcebergError, isMetadataError, isCatalogError, isStorageError, isValidationError, isCommitConflictError, isSchemaEvolutionError, wrapError, } from './errors.js';
// ============================================================================
// Deletion Vector Exports
// ============================================================================
export { 
// Core class
DeletionVector, 
// Serialization functions
serializeDeletionVector, deserializeDeletionVector, 
// Puffin blob helpers
createDeletionVectorBlob, 
// Merge utilities
mergeDeletionVectors, 
// Constants
DELETION_VECTOR_V1_BLOB_TYPE, } from './deletes/deletion-vector.js';
// ============================================================================
// Version Upgrade Exports
// ============================================================================
export { 
// Upgrade functions
upgradeTableToV3, upgradeTableToV3WithOptions, canUpgradeToV3, 
// Error handling
VersionUpgradeError, } from './metadata/index.js';
// ============================================================================
// Utility Exports
// ============================================================================
export { 
// Path validation utilities
validatePath, sanitizePath, isAbsolutePath, joinPaths, getParentPath, getBasename, } from './utils/index.js';
export { 
// Shredding helper functions
createShreddedFieldInfo, createVariantColumnSchema, validateVariantShredConfig, 
// Path generation functions
getMetadataPath, getValuePath, getTypedValuePath, 
// Property key constants
VARIANT_SHRED_COLUMNS_KEY, VARIANT_SHRED_FIELDS_KEY_PREFIX, VARIANT_SHRED_FIELDS_KEY_SUFFIX, VARIANT_FIELD_TYPES_KEY_SUFFIX, 
// Property key generation functions
getShredFieldsKey, getFieldTypesKey, 
// Property parsing functions
parseShredColumnsProperty, parseShredFieldsProperty, parseFieldTypesProperty, extractVariantShredConfig, 
// Property serialization functions
formatShredColumnsProperty, formatShredFieldsProperty, formatFieldTypesProperty, toTableProperties, 
// Property validation
validateShredConfig, 
// Statistics path functions
getStatisticsPaths, getColumnForFilterPath, mapFilterPathToStats, isVariantFilterPath, extractVariantFilterColumns, 
// Filter transformation functions
transformVariantFilter, isComparisonOperator, isLogicalOperator, 
// Manifest stats functions
getShreddedStatisticsPaths, assignShreddedFieldIds, serializeShreddedBound, deserializeShreddedBound, createShreddedColumnStats, applyShreddedStatsToDataFile, mergeShreddedStats, 
// Stats collection functions
collectShreddedColumnStats, computeStringBounds, computeNumericBounds, computeTimestampBounds, computeBooleanBounds, addShreddedStatsToDataFile, 
// Row group filter functions
createRangePredicate, evaluateRangePredicate, combinePredicatesAnd, combinePredicatesOr, filterDataFiles, filterDataFilesWithStats, 
// Predicate pushdown functions
shouldSkipDataFile, boundsOverlapValue, evaluateInPredicate, 
// ParqueDB/Hyparquet integration functions
parseShredConfig, formatShredConfig, createVariantSchemaFields, getFieldIdForShreddedPath, validateConfigWithSchema, setupVariantShredding, } from './variant/index.js';
//# sourceMappingURL=index.js.map
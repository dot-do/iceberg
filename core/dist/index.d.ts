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
export type { IcebergPrimitiveType, IcebergListType, IcebergMapType, IcebergStructField, IcebergStructType, IcebergType, IcebergSchema, PartitionTransform, PartitionField, PartitionSpec, SortField, SortOrder, ContentType, FileFormat, ColumnMetrics, BloomFilterRef, DataFile, ManifestEntryStatus, ManifestEntry, ManifestFile, PartitionFieldSummary, SnapshotSummary, SnapshotRef, SnapshotLogEntry, MetadataLogEntry, Snapshot, TableMetadata, StorageBackend, } from './metadata/index.js';
export { readTableMetadata, readMetadataFromPath, parseTableMetadata, getCurrentVersion, getSnapshotAtTimestamp, getSnapshotByRef, getSnapshotById, getCurrentSnapshot, listMetadataFiles, } from './metadata/index.js';
export { MetadataWriter, writeNewTableMetadata, writeMetadataIfMissing, type MetadataWriterOptions, type MetadataWriteResult, } from './metadata/index.js';
export { ManifestGenerator, ManifestListGenerator, createDataFileStats, type ManifestWriterOptions, type ManifestListWriterOptions, } from './metadata/index.js';
export { SnapshotBuilder, TableMetadataBuilder, SnapshotManager, generateUUID, createTableWithSnapshot, type CreateSnapshotOptions, type CreateTableOptions, type SnapshotRetentionPolicy, type ExpireSnapshotsResult, } from './metadata/index.js';
export { createDefaultSchema, createUnpartitionedSpec, createIdentityPartitionSpec, createBucketPartitionSpec, createTimePartitionSpec, createUnsortedOrder, createSortOrder, parquetToIcebergType, parquetSchemaToIceberg, type ParquetTypeName, type ParquetConvertedType, type ParquetSchemaElement, validateSchemaEvolution, generateSchemaId, findMaxFieldId, type SchemaChangeType, type SchemaChange, type SchemaComparisonResult, } from './metadata/index.js';
export { isTypePromotionAllowed, areTypesCompatible, type TypeCompatibilityResult, SchemaEvolutionError, type SchemaEvolutionErrorCode, type SchemaEvolutionOperation, type AddColumnOperation, type DropColumnOperation, type RenameColumnOperation, type UpdateColumnTypeOperation, type MakeColumnOptionalOperation, type MakeColumnRequiredOperation, type UpdateColumnDocOperation, type MoveColumnOperation, type ColumnPosition, SchemaEvolutionBuilder, type SchemaValidationResult, type SchemaEvolutionResult, compareSchemas, type SchemaChangeKind, type SchemaChangeSummary, isBackwardCompatible, isForwardCompatible, isFullyCompatible, type CompatibilityResult, getSchemaHistory, getSchemaForSnapshot, getSchemaChangesBetween, type SchemaHistoryEntry, findFieldByName, findFieldById, getAllFieldIds, evolveSchema, applySchemaEvolution, evolveNestedStruct, FieldIdManager, } from './metadata/index.js';
export { type ColumnStatistics, type ColumnStatsCollector, type ComputeStatsOptions, type ComputedFileStats, type ColumnPartitionFieldSummary, type ZoneMap, createColumnStatsCollector, FileStatsCollector, encodeFileStats, applyStatsToDataFile, aggregateColumnStats, computePartitionSummaries, createZoneMapFromStats, canPruneZoneMap, getPrimitiveType, getComparator, estimateValueSize, truncateUpperBound, } from './metadata/index.js';
export { applyTransform, parseTransform, formatTransform, getTransformResultType, getPartitionData, getPartitionPath, parsePartitionPath, PartitionSpecBuilder, createPartitionSpecBuilder, createPartitionSpecFromDefinitions, PartitionStatsCollector, createPartitionStatsCollector, comparePartitionSpecs, findMaxPartitionFieldId, generatePartitionSpecId, type ParsedTransform, type PartitionFieldDefinition, type CreatePartitionSpecOptions, type PartitionValue, type PartitionedFile, type PartitionStats, type PartitionStatsAggregate, type PartitionSpecChangeType, type PartitionSpecChange, type PartitionSpecComparisonResult, } from './metadata/index.js';
export { CONTENT_DATA, CONTENT_POSITION_DELETES, CONTENT_EQUALITY_DELETES, MANIFEST_CONTENT_DATA, MANIFEST_CONTENT_DELETES, POSITION_DELETE_SCHEMA, PositionDeleteBuilder, EqualityDeleteBuilder, DeleteManifestGenerator, PositionDeleteLookup, EqualityDeleteLookup, DeleteMerger, parsePositionDeleteFile, parseEqualityDeleteFile, applyDeletes, isDeleteFile, isPositionDeleteFile, isEqualityDeleteFile, getDeleteContentTypeName, createEqualityDeleteSchema, type PositionDelete, type EqualityDelete, type DeleteFile, type PositionDeleteFile, type EqualityDeleteFile, type PositionDeleteBuilderOptions, type EqualityDeleteBuilderOptions, type DeleteFileResult, type PositionDeleteStatistics, type EqualityDeleteStatistics, type DeleteApplicationResult, type DeleteMergerOptions, } from './metadata/index.js';
export { AtomicCommitter, CommitConflictError, CommitRetryExhaustedError, CommitTransactionError, createAtomicCommitter, commitWithCleanup, generateVersionedMetadataPath, parseMetadataVersion, getVersionHintPath, getMetadataVersion, COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS, type CommitOptions, type MetadataCleanupOptions, type CleanupFailureEvent, type ConflictResolutionStrategy, type PendingCommit, type CommitResult, } from './metadata/index.js';
export { type AvroPrimitive, type AvroArray, type AvroMap, type AvroFixed, type AvroEnum, type AvroRecordField, type AvroRecord, type AvroUnion, type AvroType, AvroEncoder, AvroFileWriter, createManifestEntrySchema, createManifestListSchema, encodeStatValue, truncateString, } from './avro/index.js';
export { BloomFilter, BloomFilterWriter, type BloomFilterOptions, type BloomFilterMetadata, type SerializedBloomFilter, type BloomFilterFileRef, type BloomFilterWriterOptions, type ParsedBloomFilterEntry, xxh64, xxh64String, xxh64Number, xxh64BigInt, calculateOptimalBlocks, estimateFalsePositiveRate, parseBloomFilterFile, createBloomFilterMap, shouldReadFile, shouldReadFileForAny, getBloomFilterPath, } from './bloom/index.js';
export { FORMAT_VERSION, METADATA_DIR, VERSION_HINT_FILENAME, PARTITION_FIELD_ID_START, INITIAL_PARTITION_ID, POSITION_DELETE_FILE_PATH_FIELD_ID, POSITION_DELETE_POS_FIELD_ID, MANIFEST_ENTRY_STATUS_EXISTING, MANIFEST_ENTRY_STATUS_ADDED, MANIFEST_ENTRY_STATUS_DELETED, DEFAULT_SCHEMA_ID, DEFAULT_SPEC_ID, DEFAULT_SORT_ORDER_ID, POSITION_DELETE_SCHEMA_ID, EQUALITY_DELETE_SCHEMA_ID, MS_PER_DAY, MS_PER_HOUR, EPOCH_YEAR, } from './metadata/constants.js';
export { IcebergError, MetadataError, type MetadataErrorCode, CatalogError, type CatalogErrorCode, StorageError, type StorageErrorCode, ValidationError, type ValidationErrorCode, TransformError, isIcebergError, isMetadataError, isCatalogError, isStorageError, isValidationError, isCommitConflictError, isSchemaEvolutionError, wrapError, } from './errors.js';
export { validatePath, sanitizePath, isAbsolutePath, joinPaths, getParentPath, getBasename, } from './utils/index.js';
//# sourceMappingURL=index.d.ts.map
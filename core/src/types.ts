/**
 * Core Types for @dotdo/iceberg
 *
 * Re-exports all types from metadata/types.ts for convenience.
 */

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
  DataFile,
  // Manifest
  ManifestEntryStatus,
  ManifestEntry,
  ManifestFile,
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
} from './metadata/types.js';

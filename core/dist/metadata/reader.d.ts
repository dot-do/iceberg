/**
 * Iceberg Metadata Reader
 *
 * Reads and parses Iceberg metadata.json files.
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { TableMetadata, StorageBackend, Snapshot } from './types.js';
/**
 * Read table metadata from storage.
 *
 * @param storage - Storage backend
 * @param location - Table location (base path)
 * @returns The table metadata, or null if not found
 */
export declare function readTableMetadata(storage: StorageBackend, location: string): Promise<TableMetadata | null>;
/**
 * Read table metadata from a specific path.
 *
 * @param storage - Storage backend
 * @param metadataPath - Full path to the metadata.json file
 * @returns The table metadata
 * @throws Error if metadata file not found or invalid
 */
export declare function readMetadataFromPath(storage: StorageBackend, metadataPath: string): Promise<TableMetadata>;
/**
 * Parse table metadata from JSON string.
 *
 * @param json - JSON string containing table metadata
 * @returns Parsed table metadata
 * @throws Error if JSON is invalid or format version is unsupported
 */
export declare function parseTableMetadata(json: string): TableMetadata;
/**
 * Get the current version number from the version hint file.
 *
 * @param storage - Storage backend
 * @param location - Table location
 * @returns The current version number, or null if not found
 */
export declare function getCurrentVersion(storage: StorageBackend, location: string): Promise<number | null>;
/**
 * Get the snapshot for a specific timestamp (time-travel).
 *
 * @param metadata - Table metadata
 * @param timestampMs - Target timestamp in milliseconds
 * @returns The most recent snapshot at or before the given timestamp
 */
export declare function getSnapshotAtTimestamp(metadata: TableMetadata, timestampMs: number): Snapshot | undefined;
/**
 * Get snapshot by reference name (branch or tag).
 *
 * @param metadata - Table metadata
 * @param refName - Reference name (e.g., 'main')
 * @returns The snapshot referenced by the given name
 */
export declare function getSnapshotByRef(metadata: TableMetadata, refName: string): Snapshot | undefined;
/**
 * Get snapshot by ID.
 *
 * @param metadata - Table metadata
 * @param snapshotId - Snapshot ID
 * @returns The snapshot with the given ID
 */
export declare function getSnapshotById(metadata: TableMetadata, snapshotId: number): Snapshot | undefined;
/**
 * Get the current snapshot from table metadata.
 *
 * @param metadata - Table metadata
 * @returns The current snapshot, or undefined if no snapshots exist
 */
export declare function getCurrentSnapshot(metadata: TableMetadata): Snapshot | undefined;
/**
 * List all metadata files in a table.
 *
 * @param storage - Storage backend
 * @param location - Table location
 * @returns Array of metadata file paths
 */
export declare function listMetadataFiles(storage: StorageBackend, location: string): Promise<string[]>;
//# sourceMappingURL=reader.d.ts.map
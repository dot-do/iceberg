/**
 * Iceberg Metadata Reader
 *
 * Reads and parses Iceberg metadata.json files.
 *
 * @see https://iceberg.apache.org/spec/
 */

import type {
  TableMetadata,
  StorageBackend,
  Snapshot,
} from './types.js';
import {
  FORMAT_VERSION,
  METADATA_DIR,
  VERSION_HINT_FILENAME,
} from './constants.js';
import { validatePath } from '../utils/path-validation.js';

// ============================================================================
// Metadata Reader
// ============================================================================

/**
 * Read table metadata from storage.
 *
 * @param storage - Storage backend
 * @param location - Table location (base path)
 * @returns The table metadata, or null if not found
 */
export async function readTableMetadata(
  storage: StorageBackend,
  location: string
): Promise<TableMetadata | null> {
  // Validate location is a non-empty string
  if (typeof location !== 'string' || location.trim() === '') {
    throw new Error('Location must be a non-empty string');
  }

  const metadataDir = `${location}/${METADATA_DIR}`;
  const versionHintPath = `${metadataDir}/${VERSION_HINT_FILENAME}`;

  // Try to read version hint
  const versionHintData = await storage.get(versionHintPath);
  if (!versionHintData) {
    // Try to find latest metadata file directly
    const files = await storage.list(metadataDir);
    const metadataFiles = files
      .filter((f) => f.endsWith('.metadata.json'))
      .sort()
      .reverse();

    if (metadataFiles.length === 0) {
      return null;
    }

    const metadataData = await storage.get(metadataFiles[0]);
    if (!metadataData) {
      return null;
    }

    return parseTableMetadata(new TextDecoder().decode(metadataData));
  }

  // Read metadata from version hint (supports both version number and full path)
  const versionHintContent = new TextDecoder().decode(versionHintData).trim();

  // Validate version hint content is not empty
  if (versionHintContent === '') {
    throw new Error('Version hint file is empty');
  }

  let metadataPath: string;

  if (versionHintContent.includes('/')) {
    // Full path stored in version hint
    // Check for path traversal attempts
    validatePath(versionHintContent);
    metadataPath = versionHintContent;
  } else {
    // Version number stored in version hint
    const version = parseInt(versionHintContent, 10);
    // Handle NaN from parseInt
    if (Number.isNaN(version)) {
      throw new Error(`Invalid version hint: "${versionHintContent}" is not a valid version number`);
    }
    metadataPath = `${metadataDir}/v${version}.metadata.json`;
  }

  const metadataData = await storage.get(metadataPath);

  if (!metadataData) {
    return null;
  }

  return parseTableMetadata(new TextDecoder().decode(metadataData));
}

/**
 * Read table metadata from a specific path.
 *
 * @param storage - Storage backend
 * @param metadataPath - Full path to the metadata.json file
 * @returns The table metadata
 * @throws Error if metadata file not found or invalid
 */
export async function readMetadataFromPath(
  storage: StorageBackend,
  metadataPath: string
): Promise<TableMetadata> {
  // Validate metadataPath is a non-empty string
  if (typeof metadataPath !== 'string' || metadataPath.trim() === '') {
    throw new Error('Metadata path must be a non-empty string');
  }

  const data = await storage.get(metadataPath);
  if (!data) {
    throw new Error(`Metadata file not found: ${metadataPath}`);
  }

  return parseTableMetadata(new TextDecoder().decode(data));
}

/**
 * Parse table metadata from JSON string.
 *
 * @param json - JSON string containing table metadata
 * @returns Parsed table metadata
 * @throws Error if JSON is invalid or format version is unsupported
 */
export function parseTableMetadata(json: string): TableMetadata {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch (error) {
    throw new Error(`Failed to parse table metadata JSON: ${error instanceof Error ? error.message : 'Invalid JSON'}`);
  }

  // Validate parsed result is an object before accessing properties
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error('Table metadata must be a JSON object');
  }

  const metadata = parsed as Record<string, unknown>;

  // Validate required fields
  if (metadata['format-version'] !== FORMAT_VERSION) {
    throw new Error(`Unsupported format version: ${metadata['format-version']}`);
  }

  if (!metadata['table-uuid']) {
    throw new Error('Missing required field: table-uuid');
  }

  if (!metadata.location) {
    throw new Error('Missing required field: location');
  }

  return parsed as unknown as TableMetadata;
}

/**
 * Get the current version number from the version hint file.
 *
 * @param storage - Storage backend
 * @param location - Table location
 * @returns The current version number, or null if not found
 */
export async function getCurrentVersion(
  storage: StorageBackend,
  location: string
): Promise<number | null> {
  // Validate location is a non-empty string
  if (typeof location !== 'string' || location.trim() === '') {
    throw new Error('Location must be a non-empty string');
  }

  const versionHintPath = `${location}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
  const data = await storage.get(versionHintPath);

  if (!data) {
    return null;
  }

  const versionStr = new TextDecoder().decode(data).trim();
  const version = parseInt(versionStr, 10);

  // Handle NaN from parseInt
  if (Number.isNaN(version)) {
    return null;
  }

  return version;
}

/**
 * Get the snapshot for a specific timestamp (time-travel).
 *
 * @param metadata - Table metadata
 * @param timestampMs - Target timestamp in milliseconds
 * @returns The most recent snapshot at or before the given timestamp
 */
export function getSnapshotAtTimestamp(
  metadata: TableMetadata,
  timestampMs: number
): Snapshot | undefined {
  // Validate timestampMs is a valid number
  if (typeof timestampMs !== 'number' || Number.isNaN(timestampMs)) {
    throw new Error('Timestamp must be a valid number');
  }

  // Find the most recent snapshot at or before the given timestamp
  const validSnapshots = metadata.snapshots
    .filter((s) => s['timestamp-ms'] <= timestampMs)
    .sort((a, b) => b['timestamp-ms'] - a['timestamp-ms']);

  return validSnapshots[0];
}

/**
 * Get snapshot by reference name (branch or tag).
 *
 * @param metadata - Table metadata
 * @param refName - Reference name (e.g., 'main')
 * @returns The snapshot referenced by the given name
 */
export function getSnapshotByRef(
  metadata: TableMetadata,
  refName: string
): Snapshot | undefined {
  // Validate refName is a non-empty string
  if (typeof refName !== 'string' || refName.trim() === '') {
    throw new Error('Reference name must be a non-empty string');
  }

  const ref = metadata.refs[refName];
  if (!ref) {
    return undefined;
  }

  return metadata.snapshots.find((s) => s['snapshot-id'] === ref['snapshot-id']);
}

/**
 * Get snapshot by ID.
 *
 * @param metadata - Table metadata
 * @param snapshotId - Snapshot ID
 * @returns The snapshot with the given ID
 */
export function getSnapshotById(
  metadata: TableMetadata,
  snapshotId: number
): Snapshot | undefined {
  // Validate snapshotId is a valid number
  if (typeof snapshotId !== 'number' || Number.isNaN(snapshotId)) {
    throw new Error('Snapshot ID must be a valid number');
  }

  return metadata.snapshots.find((s) => s['snapshot-id'] === snapshotId);
}

/**
 * Get the current snapshot from table metadata.
 *
 * @param metadata - Table metadata
 * @returns The current snapshot, or undefined if no snapshots exist
 */
export function getCurrentSnapshot(metadata: TableMetadata): Snapshot | undefined {
  if (metadata['current-snapshot-id'] === null) {
    return undefined;
  }
  return getSnapshotById(metadata, metadata['current-snapshot-id']);
}

/**
 * List all metadata files in a table.
 *
 * @param storage - Storage backend
 * @param location - Table location
 * @returns Array of metadata file paths
 */
export async function listMetadataFiles(
  storage: StorageBackend,
  location: string
): Promise<string[]> {
  // Validate location is a non-empty string
  if (typeof location !== 'string' || location.trim() === '') {
    throw new Error('Location must be a non-empty string');
  }

  const metadataDir = `${location}/${METADATA_DIR}`;
  const files = await storage.list(metadataDir);
  return files.filter((f) => f.endsWith('.metadata.json')).sort();
}

/**
 * Iceberg Table Version Upgrade
 *
 * Functions for upgrading Iceberg tables between format versions.
 *
 * @see https://iceberg.apache.org/spec/#format-versioning
 */

import type { TableMetadata } from './types.js';

/**
 * Error codes for version upgrade operations.
 */
export type VersionUpgradeErrorCode =
  | 'INVALID_SOURCE_VERSION'
  | 'ALREADY_TARGET_VERSION'
  | 'DOWNGRADE_NOT_ALLOWED';

/**
 * Error thrown when a version upgrade operation fails.
 */
export class VersionUpgradeError extends Error {
  readonly code: VersionUpgradeErrorCode;

  constructor(code: VersionUpgradeErrorCode, message: string) {
    super(message);
    this.name = 'VersionUpgradeError';
    this.code = code;
  }
}

/**
 * Upgrade table metadata from v2 to v3.
 *
 * This function performs a table format version upgrade with the following changes:
 * - format-version is changed from 2 to 3
 * - next-row-id is initialized to 0
 * - All existing fields are preserved
 * - Existing snapshots are preserved without adding row lineage fields retroactively
 *
 * Key constraints:
 * - Cannot upgrade v1 directly to v3 (must upgrade to v2 first)
 * - Cannot downgrade (v3 to v2 or lower is not supported)
 * - If already v3, throws an error (upgrade is not needed)
 *
 * @param metadata - The v2 table metadata to upgrade
 * @returns A new TableMetadata object with format-version 3
 * @throws {VersionUpgradeError} If the source version is not v2 or the metadata is already v3
 *
 * @example
 * ```ts
 * const v2Metadata = await readTableMetadata(storage, location);
 * const v3Metadata = upgradeTableToV3(v2Metadata);
 * await writeMetadata(storage, v3Metadata);
 * ```
 */
export function upgradeTableToV3(metadata: TableMetadata): TableMetadata {
  // Validate source version
  if (metadata['format-version'] === 3) {
    throw new VersionUpgradeError(
      'ALREADY_TARGET_VERSION',
      'Table is already at format-version 3. Upgrade is not needed.'
    );
  }

  if (metadata['format-version'] !== 2) {
    throw new VersionUpgradeError(
      'INVALID_SOURCE_VERSION',
      `Cannot upgrade from format-version ${metadata['format-version']} to v3. ` +
        'Only v2 tables can be upgraded to v3. ' +
        'If upgrading from v1, upgrade to v2 first.'
    );
  }

  // Create upgraded metadata
  // Note: We preserve existing snapshots exactly as they are without adding
  // row lineage fields retroactively, since we cannot determine accurate
  // row IDs for pre-existing data.
  const upgradedMetadata: TableMetadata = {
    // Change format version to 3
    'format-version': 3,

    // Preserve all existing fields
    'table-uuid': metadata['table-uuid'],
    location: metadata.location,
    'last-sequence-number': metadata['last-sequence-number'],
    'last-updated-ms': Date.now(), // Update timestamp to reflect the upgrade
    'last-column-id': metadata['last-column-id'],
    'current-schema-id': metadata['current-schema-id'],
    schemas: metadata.schemas,
    'default-spec-id': metadata['default-spec-id'],
    'partition-specs': metadata['partition-specs'],
    'last-partition-id': metadata['last-partition-id'],
    'default-sort-order-id': metadata['default-sort-order-id'],
    'sort-orders': metadata['sort-orders'],
    properties: metadata.properties,
    'current-snapshot-id': metadata['current-snapshot-id'],
    snapshots: metadata.snapshots, // Preserved without row lineage fields
    'snapshot-log': metadata['snapshot-log'],
    'metadata-log': metadata['metadata-log'],
    refs: metadata.refs,

    // Initialize row lineage tracking for v3
    'next-row-id': 0,
  };

  return upgradedMetadata;
}

/**
 * Check if a table can be upgraded to v3.
 *
 * @param metadata - The table metadata to check
 * @returns true if the table can be upgraded to v3, false otherwise
 */
export function canUpgradeToV3(metadata: TableMetadata): boolean {
  return metadata['format-version'] === 2;
}

/**
 * Options for upgrading table metadata.
 */
export interface UpgradeOptions {
  /**
   * Whether to preserve the original last-updated-ms timestamp.
   * If false (default), the timestamp is updated to the current time.
   */
  preserveTimestamp?: boolean;
}

/**
 * Upgrade table metadata from v2 to v3 with options.
 *
 * This is an alternative to upgradeTableToV3 that allows for additional options.
 *
 * @param metadata - The v2 table metadata to upgrade
 * @param options - Upgrade options
 * @returns A new TableMetadata object with format-version 3
 * @throws {VersionUpgradeError} If the source version is not v2 or the metadata is already v3
 */
export function upgradeTableToV3WithOptions(
  metadata: TableMetadata,
  options: UpgradeOptions = {}
): TableMetadata {
  const upgraded = upgradeTableToV3(metadata);

  if (options.preserveTimestamp) {
    return {
      ...upgraded,
      'last-updated-ms': metadata['last-updated-ms'],
    };
  }

  return upgraded;
}

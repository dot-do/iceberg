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
export type VersionUpgradeErrorCode = 'INVALID_SOURCE_VERSION' | 'ALREADY_TARGET_VERSION' | 'DOWNGRADE_NOT_ALLOWED';
/**
 * Error thrown when a version upgrade operation fails.
 */
export declare class VersionUpgradeError extends Error {
    readonly code: VersionUpgradeErrorCode;
    constructor(code: VersionUpgradeErrorCode, message: string);
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
export declare function upgradeTableToV3(metadata: TableMetadata): TableMetadata;
/**
 * Check if a table can be upgraded to v3.
 *
 * @param metadata - The table metadata to check
 * @returns true if the table can be upgraded to v3, false otherwise
 */
export declare function canUpgradeToV3(metadata: TableMetadata): boolean;
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
export declare function upgradeTableToV3WithOptions(metadata: TableMetadata, options?: UpgradeOptions): TableMetadata;
//# sourceMappingURL=upgrade.d.ts.map
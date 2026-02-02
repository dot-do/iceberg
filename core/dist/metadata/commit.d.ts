/**
 * Iceberg Atomic Commit
 *
 * Provides atomic snapshot commit functionality for Iceberg tables with:
 * - Optimistic concurrency control using version numbers
 * - Conflict detection on concurrent writes
 * - Automatic retry with exponential backoff
 * - Transaction cleanup on failure
 * - Version-hint.text file management for quick version lookup
 *
 * @see https://iceberg.apache.org/spec/
 */
import type { StorageBackend, TableMetadata, Snapshot, SnapshotSummary } from './types.js';
import { COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS } from './constants.js';
export { COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS, };
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export declare class CommitConflictError extends Error {
    /** The version we expected the table to be at */
    readonly expectedVersion: number;
    /** The actual version we found */
    readonly actualVersion: number;
    constructor(message: string, expectedVersion: number, actualVersion: number);
}
/**
 * Error thrown when max retries are exhausted.
 */
export declare class CommitRetryExhaustedError extends Error {
    /** Number of attempts made before giving up */
    readonly attempts: number;
    /** The last error that occurred */
    readonly lastError: Error;
    constructor(message: string, attempts: number, lastError: Error);
}
/**
 * Error thrown when a commit transaction fails and needs cleanup.
 */
export declare class CommitTransactionError extends Error {
    /** Files that were written and need cleanup */
    readonly writtenFiles: string[];
    /** Whether cleanup was successful */
    readonly cleanupSuccessful: boolean;
    constructor(message: string, writtenFiles: string[], cleanupSuccessful: boolean);
}
/**
 * Options for snapshot commit operations.
 */
export interface CommitOptions {
    /** Maximum number of retries for concurrent commits (default: 5) */
    maxRetries?: number;
    /** Base delay for exponential backoff in milliseconds (default: 100) */
    baseRetryDelayMs?: number;
    /** Maximum delay for exponential backoff in milliseconds (default: 5000) */
    maxRetryDelayMs?: number;
    /** Jitter factor for retry delays (0-1, default: 0.2) */
    retryJitter?: number;
    /** Whether to clean up written files on failure (default: true) */
    cleanupOnFailure?: boolean;
}
/**
 * Options for metadata cleanup.
 */
export interface MetadataCleanupOptions {
    /** Number of metadata versions to retain (default: 10) */
    retainVersions?: number;
    /** Maximum age of metadata files to retain in milliseconds (default: 7 days) */
    maxAgeMs?: number;
    /** Whether to perform cleanup (default: true) */
    enabled?: boolean;
    /** Callback invoked when cleanup fails */
    onCleanupFailure?: (event: CleanupFailureEvent) => void;
}
/**
 * Event emitted when cleanup fails.
 */
export interface CleanupFailureEvent {
    /** Table location where cleanup failed */
    tableLocation: string;
    /** The error that occurred during cleanup */
    error: Error;
    /** Timestamp when the failure occurred */
    timestamp: number;
    /** Metadata version that was committed */
    metadataVersion: number;
    /** Cleanup options that were used */
    cleanupOptions?: Omit<MetadataCleanupOptions, 'onCleanupFailure'>;
}
/**
 * Conflict resolution strategy.
 */
export type ConflictResolutionStrategy = 'fail' | 'retry' | 'rebase';
/**
 * Pending commit operation for building a commit.
 */
export interface PendingCommit {
    /** Current metadata at time of building commit */
    baseMetadata: TableMetadata;
    /** New snapshot to commit */
    snapshot: Snapshot;
    /** Optional previous metadata path for metadata-log */
    previousMetadataPath?: string;
}
/**
 * Result of a snapshot commit operation.
 */
export interface CommitResult {
    /** The committed snapshot */
    snapshot: Snapshot;
    /** The new metadata version */
    metadataVersion: number;
    /** Path to the new metadata file */
    metadataPath: string;
    /** Number of attempts made */
    attempts: number;
    /** Whether conflict occurred and was resolved */
    conflictResolved: boolean;
    /** Files cleaned up (if cleanup was enabled) */
    cleanedUpFiles?: string[];
}
/**
 * Generate a versioned metadata file path.
 * Format: {tableLocation}/metadata/{version}-{uuid}.metadata.json
 */
export declare function generateVersionedMetadataPath(tableLocation: string, version: number): string;
/**
 * Parse version number from a metadata file path.
 * Supports both formats:
 * - {location}/metadata/v{version}.metadata.json
 * - {location}/metadata/{version}-{uuid}.metadata.json
 */
export declare function parseMetadataVersion(metadataPath: string): number | null;
/**
 * Get the version hint file path.
 */
export declare function getVersionHintPath(tableLocation: string): string;
/**
 * Extract the current version from table metadata.
 * Uses last-sequence-number as the version indicator.
 */
export declare function getMetadataVersion(metadata: TableMetadata): number;
/**
 * AtomicCommitter handles atomic snapshot commits with optimistic locking.
 *
 * This class implements the Iceberg commit protocol:
 * 1. Read current metadata and version
 * 2. Build new metadata with snapshot
 * 3. Write new metadata file with versioned name
 * 4. Atomically update version-hint.text
 * 5. On conflict, retry with exponential backoff
 *
 * @example
 * ```ts
 * const committer = new AtomicCommitter(storage, 's3://bucket/table');
 *
 * // Commit a new snapshot
 * const result = await committer.commit(async (metadata) => ({
 *   baseMetadata: metadata!,
 *   snapshot: buildSnapshot(metadata),
 * }));
 *
 * console.log(`Committed version ${result.metadataVersion}`);
 * ```
 */
export declare class AtomicCommitter {
    private readonly storage;
    private readonly tableLocation;
    constructor(storage: StorageBackend, tableLocation: string);
    /**
     * Get the table location.
     */
    getTableLocation(): string;
    /**
     * Get the version hint file path.
     */
    private getVersionHintPath;
    /**
     * Read the current version from version-hint.text.
     */
    getCurrentVersion(): Promise<number | null>;
    /**
     * Read the current metadata path from version-hint.text.
     */
    getCurrentMetadataPath(): Promise<string | null>;
    /**
     * Load the current table metadata.
     */
    loadMetadata(): Promise<TableMetadata | null>;
    /**
     * Write the version hint file atomically.
     * Uses compareAndSwap if available for true atomic updates.
     */
    private writeVersionHint;
    /**
     * Write metadata file to storage.
     * Uses putIfAbsent if available for atomic writes.
     * @returns true if written successfully, false if file already exists (when using putIfAbsent)
     */
    private writeMetadata;
    /**
     * Clean up written files on failure.
     */
    private cleanupFiles;
    /**
     * Attempt to atomically commit a new snapshot.
     *
     * This performs the following steps:
     * 1. Verify the base metadata version matches the current version
     * 2. Build new metadata with the snapshot
     * 3. Write the new metadata file with a versioned name
     * 4. Atomically update the version hint
     *
     * @throws CommitConflictError if version mismatch detected
     */
    private attemptCommit;
    /**
     * Commit a new snapshot with retry logic for concurrent commits.
     *
     * @param buildCommit - Function that builds the pending commit from current metadata
     * @param options - Commit options
     * @returns The commit result
     * @throws CommitRetryExhaustedError if all retries fail
     */
    commit(buildCommit: (metadata: TableMetadata | null) => Promise<PendingCommit>, options?: CommitOptions): Promise<CommitResult>;
    /**
     * Commit a snapshot using the simple interface.
     *
     * @param manifestListPath - Path to the manifest list for the new snapshot
     * @param summary - Summary for the new snapshot
     * @param options - Commit options
     * @returns The commit result
     */
    commitSnapshot(manifestListPath: string, summary: SnapshotSummary, options?: CommitOptions & {
        schemaId?: number;
        parentSnapshotId?: number;
    }): Promise<CommitResult>;
    /**
     * Clean up old metadata files based on retention policy.
     *
     * @param options - Cleanup options
     * @returns List of deleted file paths
     */
    cleanupOldMetadata(options?: MetadataCleanupOptions): Promise<string[]>;
}
/**
 * Create an AtomicCommitter instance.
 */
export declare function createAtomicCommitter(storage: StorageBackend, tableLocation: string): AtomicCommitter;
/**
 * Commit with cleanup - performs a commit and then cleans up old metadata files.
 *
 * Cleanup errors are logged and optionally reported via callback,
 * but never block the main commit operation.
 */
export declare function commitWithCleanup(committer: AtomicCommitter, buildCommit: (metadata: TableMetadata | null) => Promise<PendingCommit>, commitOptions?: CommitOptions, cleanupOptions?: MetadataCleanupOptions): Promise<CommitResult>;
//# sourceMappingURL=commit.d.ts.map
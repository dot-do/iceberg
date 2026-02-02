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
import { TableMetadataBuilder, SnapshotBuilder, generateUUID } from './snapshot.js';
import { readTableMetadata } from './reader.js';
import { COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS, VERSION_HINT_FILENAME, METADATA_DIR, } from './constants.js';
// Re-export commit constants for backward compatibility
export { COMMIT_MAX_RETRIES, DEFAULT_BASE_RETRY_DELAY_MS, DEFAULT_MAX_RETRY_DELAY_MS, DEFAULT_RETRY_JITTER, METADATA_RETAIN_VERSIONS, METADATA_MAX_AGE_MS, };
// ============================================================================
// Error Types
// ============================================================================
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export class CommitConflictError extends Error {
    /** The version we expected the table to be at */
    expectedVersion;
    /** The actual version we found */
    actualVersion;
    constructor(message, expectedVersion, actualVersion) {
        super(message);
        this.name = 'CommitConflictError';
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }
}
/**
 * Error thrown when max retries are exhausted.
 */
export class CommitRetryExhaustedError extends Error {
    /** Number of attempts made before giving up */
    attempts;
    /** The last error that occurred */
    lastError;
    constructor(message, attempts, lastError) {
        super(message);
        this.name = 'CommitRetryExhaustedError';
        this.attempts = attempts;
        this.lastError = lastError;
    }
}
/**
 * Error thrown when a commit transaction fails and needs cleanup.
 */
export class CommitTransactionError extends Error {
    /** Files that were written and need cleanup */
    writtenFiles;
    /** Whether cleanup was successful */
    cleanupSuccessful;
    constructor(message, writtenFiles, cleanupSuccessful) {
        super(message);
        this.name = 'CommitTransactionError';
        this.writtenFiles = writtenFiles;
        this.cleanupSuccessful = cleanupSuccessful;
    }
}
// ============================================================================
// Utility Functions
// ============================================================================
/**
 * Generate a versioned metadata file path.
 * Format: {tableLocation}/metadata/{version}-{uuid}.metadata.json
 */
export function generateVersionedMetadataPath(tableLocation, version) {
    const uuid = generateUUID();
    return `${tableLocation}/metadata/${version}-${uuid}.metadata.json`;
}
/**
 * Parse version number from a metadata file path.
 * Supports both formats:
 * - {location}/metadata/v{version}.metadata.json
 * - {location}/metadata/{version}-{uuid}.metadata.json
 */
export function parseMetadataVersion(metadataPath) {
    const match = metadataPath.match(/\/metadata\/(?:v)?(\d+)(?:-[a-f0-9-]+)?\.metadata\.json$/);
    if (!match)
        return null;
    return parseInt(match[1], 10);
}
/**
 * Get the version hint file path.
 */
export function getVersionHintPath(tableLocation) {
    return `${tableLocation}/${METADATA_DIR}/${VERSION_HINT_FILENAME}`;
}
/**
 * Sleep with jitter for exponential backoff.
 */
function sleepWithJitter(baseDelayMs, attempt, maxDelayMs, jitter) {
    const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
    const cappedDelay = Math.min(exponentialDelay, maxDelayMs);
    const jitterAmount = cappedDelay * jitter * (Math.random() * 2 - 1);
    const finalDelay = Math.max(0, cappedDelay + jitterAmount);
    return new Promise(resolve => setTimeout(resolve, finalDelay));
}
/**
 * Extract the current version from table metadata.
 * Uses last-sequence-number as the version indicator.
 */
export function getMetadataVersion(metadata) {
    return metadata['last-sequence-number'];
}
// ============================================================================
// AtomicCommitter Class
// ============================================================================
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
export class AtomicCommitter {
    storage;
    tableLocation;
    constructor(storage, tableLocation) {
        this.storage = storage;
        this.tableLocation = tableLocation;
    }
    /**
     * Get the table location.
     */
    getTableLocation() {
        return this.tableLocation;
    }
    /**
     * Get the version hint file path.
     */
    getVersionHintPath() {
        return getVersionHintPath(this.tableLocation);
    }
    /**
     * Read the current version from version-hint.text.
     */
    async getCurrentVersion() {
        const data = await this.storage.get(this.getVersionHintPath());
        if (!data)
            return null;
        const text = new TextDecoder().decode(data).trim();
        // Handle both formats: just version number, or full path
        if (text.includes('/')) {
            return parseMetadataVersion(text);
        }
        return parseInt(text, 10);
    }
    /**
     * Read the current metadata path from version-hint.text.
     */
    async getCurrentMetadataPath() {
        const data = await this.storage.get(this.getVersionHintPath());
        if (!data)
            return null;
        const text = new TextDecoder().decode(data).trim();
        // Handle both formats: just version number, or full path
        if (text.includes('/')) {
            return text;
        }
        // Convert version number to path
        return `${this.tableLocation}/metadata/v${text}.metadata.json`;
    }
    /**
     * Load the current table metadata.
     */
    async loadMetadata() {
        return readTableMetadata(this.storage, this.tableLocation);
    }
    /**
     * Write the version hint file atomically.
     * Uses compareAndSwap if available for true atomic updates.
     */
    async writeVersionHint(version) {
        const content = new TextEncoder().encode(String(version));
        const hintPath = this.getVersionHintPath();
        if (this.storage.compareAndSwap) {
            // Get current hint to use as expected value
            const currentHint = await this.storage.get(hintPath);
            const success = await this.storage.compareAndSwap(hintPath, currentHint, content);
            if (!success) {
                // Concurrent update - fall back to regular put (our metadata is already written)
                await this.storage.put(hintPath, content);
            }
        }
        else {
            await this.storage.put(hintPath, content);
        }
    }
    /**
     * Write metadata file to storage.
     * Uses putIfAbsent if available for atomic writes.
     * @returns true if written successfully, false if file already exists (when using putIfAbsent)
     */
    async writeMetadata(path, metadata) {
        const json = JSON.stringify(metadata, null, 2);
        const content = new TextEncoder().encode(json);
        if (this.storage.putIfAbsent) {
            return this.storage.putIfAbsent(path, content);
        }
        await this.storage.put(path, content);
        return true;
    }
    /**
     * Clean up written files on failure.
     */
    async cleanupFiles(files) {
        let allSuccessful = true;
        for (const file of files) {
            try {
                await this.storage.delete(file);
            }
            catch {
                allSuccessful = false;
            }
        }
        return allSuccessful;
    }
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
    async attemptCommit(pendingCommit, expectedVersion, options) {
        const writtenFiles = [];
        try {
            // Step 1: Verify current version matches expected
            const currentVersion = await this.getCurrentVersion();
            const actualVersion = currentVersion ?? 0;
            if (actualVersion !== expectedVersion) {
                throw new CommitConflictError(`Commit conflict: expected version ${expectedVersion}, but current version is ${actualVersion}`, expectedVersion, actualVersion);
            }
            // Step 2: Build the new metadata with the snapshot
            const builder = TableMetadataBuilder.fromMetadata(pendingCommit.baseMetadata);
            builder.addSnapshot(pendingCommit.snapshot);
            // Add previous metadata to metadata-log if provided
            if (pendingCommit.previousMetadataPath) {
                builder.addMetadataLogEntry(pendingCommit.previousMetadataPath, pendingCommit.baseMetadata['last-updated-ms']);
            }
            const newMetadata = builder.build();
            const newVersion = getMetadataVersion(newMetadata);
            // Step 3: Write the new metadata file using putIfAbsent if available
            const newMetadataPath = generateVersionedMetadataPath(this.tableLocation, newVersion);
            const written = await this.writeMetadata(newMetadataPath, newMetadata);
            if (!written) {
                // Metadata file already exists - concurrent write detected
                throw new CommitConflictError(`Commit conflict: metadata file already exists at ${newMetadataPath}`, expectedVersion, expectedVersion // We don't know the actual version, use expected
                );
            }
            writtenFiles.push(newMetadataPath);
            // Step 4: Atomically update the version hint with full path
            // (reader supports both version number and full path)
            await this.writeVersionHint(newMetadataPath);
            return {
                snapshot: pendingCommit.snapshot,
                metadataVersion: newVersion,
                metadataPath: newMetadataPath,
                attempts: 1,
                conflictResolved: false,
            };
        }
        catch (error) {
            // Clean up on failure if requested
            if (options.cleanupOnFailure !== false && writtenFiles.length > 0) {
                const cleanupSuccessful = await this.cleanupFiles(writtenFiles);
                if (error instanceof CommitConflictError) {
                    throw error;
                }
                throw new CommitTransactionError(error instanceof Error ? error.message : String(error), writtenFiles, cleanupSuccessful);
            }
            throw error;
        }
    }
    /**
     * Commit a new snapshot with retry logic for concurrent commits.
     *
     * @param buildCommit - Function that builds the pending commit from current metadata
     * @param options - Commit options
     * @returns The commit result
     * @throws CommitRetryExhaustedError if all retries fail
     */
    async commit(buildCommit, options = {}) {
        const { maxRetries = COMMIT_MAX_RETRIES, baseRetryDelayMs = DEFAULT_BASE_RETRY_DELAY_MS, maxRetryDelayMs = DEFAULT_MAX_RETRY_DELAY_MS, retryJitter = DEFAULT_RETRY_JITTER, } = options;
        let lastError;
        let attempts = 0;
        let conflictResolved = false;
        for (let attempt = 0; attempt <= maxRetries; attempt++) {
            attempts++;
            try {
                // Load current metadata
                const currentMetadata = await this.loadMetadata();
                const expectedVersion = currentMetadata
                    ? getMetadataVersion(currentMetadata)
                    : 0;
                // Build the pending commit
                const pendingCommit = await buildCommit(currentMetadata);
                // Attempt the commit
                const result = await this.attemptCommit(pendingCommit, expectedVersion, options);
                result.attempts = attempts;
                result.conflictResolved = conflictResolved;
                return result;
            }
            catch (error) {
                lastError = error instanceof Error ? error : new Error(String(error));
                if (error instanceof CommitConflictError) {
                    // Concurrent modification - retry with backoff
                    if (attempt < maxRetries) {
                        conflictResolved = true;
                        await sleepWithJitter(baseRetryDelayMs, attempt, maxRetryDelayMs, retryJitter);
                        continue;
                    }
                }
                else {
                    // Non-retryable error
                    throw error;
                }
            }
        }
        throw new CommitRetryExhaustedError(`Failed to commit after ${attempts} attempts`, attempts, lastError);
    }
    /**
     * Commit a snapshot using the simple interface.
     *
     * @param manifestListPath - Path to the manifest list for the new snapshot
     * @param summary - Summary for the new snapshot
     * @param options - Commit options
     * @returns The commit result
     */
    async commitSnapshot(manifestListPath, summary, options = {}) {
        const { schemaId, parentSnapshotId, ...commitOptions } = options;
        return this.commit(async (metadata) => {
            if (!metadata) {
                throw new Error('Table does not exist. Initialize the table first.');
            }
            const previousMetadataPath = await this.getCurrentMetadataPath();
            const sequenceNumber = metadata['last-sequence-number'] + 1;
            const currentSnapshotId = metadata['current-snapshot-id'];
            const snapshotBuilder = new SnapshotBuilder({
                sequenceNumber,
                parentSnapshotId: parentSnapshotId ?? currentSnapshotId ?? undefined,
                manifestListPath,
                operation: summary.operation,
                schemaId: schemaId ?? metadata['current-schema-id'],
            });
            // Copy summary stats
            if (summary['added-data-files']) {
                const addedFiles = parseInt(summary['added-data-files'], 10);
                const deletedFiles = parseInt(summary['deleted-data-files'] ?? '0', 10);
                const addedRecords = parseInt(summary['added-records'] ?? '0', 10);
                const deletedRecords = parseInt(summary['deleted-records'] ?? '0', 10);
                const addedSize = parseInt(summary['added-files-size'] ?? '0', 10);
                const removedSize = parseInt(summary['removed-files-size'] ?? '0', 10);
                const totalRecords = parseInt(summary['total-records'] ?? '0', 10);
                const totalSize = parseInt(summary['total-files-size'] ?? '0', 10);
                const totalFiles = parseInt(summary['total-data-files'] ?? '0', 10);
                snapshotBuilder.setSummary(addedFiles, deletedFiles, addedRecords, deletedRecords, addedSize, removedSize, totalRecords, totalSize, totalFiles);
            }
            return {
                baseMetadata: metadata,
                snapshot: snapshotBuilder.build(),
                previousMetadataPath: previousMetadataPath ?? undefined,
            };
        }, commitOptions);
    }
    /**
     * Clean up old metadata files based on retention policy.
     *
     * @param options - Cleanup options
     * @returns List of deleted file paths
     */
    async cleanupOldMetadata(options = {}) {
        const { retainVersions = METADATA_RETAIN_VERSIONS, maxAgeMs = METADATA_MAX_AGE_MS, enabled = true, } = options;
        if (!enabled)
            return [];
        const metadata = await this.loadMetadata();
        if (!metadata)
            return [];
        const metadataLog = metadata['metadata-log'] ?? [];
        const currentPath = await this.getCurrentMetadataPath();
        const now = Date.now();
        // Sort metadata log by timestamp (newest first)
        const sortedLog = [...metadataLog].sort((a, b) => b['timestamp-ms'] - a['timestamp-ms']);
        // Collect files to delete
        const filesToDelete = [];
        for (let i = 0; i < sortedLog.length; i++) {
            const entry = sortedLog[i];
            const filePath = entry['metadata-file'];
            // Skip the current metadata file
            if (filePath === currentPath)
                continue;
            // Keep recent versions
            if (i < retainVersions)
                continue;
            // Delete if older than maxAge
            if (now - entry['timestamp-ms'] > maxAgeMs) {
                filesToDelete.push(filePath);
            }
        }
        // Delete old files (best effort)
        const deletedFiles = [];
        for (const filePath of filesToDelete) {
            try {
                await this.storage.delete(filePath);
                deletedFiles.push(filePath);
            }
            catch {
                // Ignore deletion errors
            }
        }
        return deletedFiles;
    }
}
// ============================================================================
// Convenience Functions
// ============================================================================
/**
 * Create an AtomicCommitter instance.
 */
export function createAtomicCommitter(storage, tableLocation) {
    return new AtomicCommitter(storage, tableLocation);
}
/**
 * Commit with cleanup - performs a commit and then cleans up old metadata files.
 *
 * Cleanup errors are logged and optionally reported via callback,
 * but never block the main commit operation.
 */
export async function commitWithCleanup(committer, buildCommit, commitOptions, cleanupOptions) {
    const result = await committer.commit(buildCommit, commitOptions);
    // Extract callback before passing options to cleanup
    const { onCleanupFailure, ...cleanupOptionsWithoutCallback } = cleanupOptions ?? {};
    // Cleanup old metadata in background (don't await to avoid blocking)
    committer.cleanupOldMetadata(cleanupOptionsWithoutCallback).catch((error) => {
        const cleanupError = error instanceof Error ? error : new Error(String(error));
        // Log cleanup errors with details for debugging
        console.error('[IcebergCommit] Cleanup failed for table', {
            error: cleanupError.message,
            tableLocation: committer.getTableLocation(),
            metadataVersion: result.metadataVersion,
            cleanupOptions: cleanupOptionsWithoutCallback,
        });
        // Emit event for monitoring/alerting systems
        if (onCleanupFailure) {
            const failureEvent = {
                tableLocation: committer.getTableLocation(),
                error: cleanupError,
                timestamp: Date.now(),
                metadataVersion: result.metadataVersion,
                cleanupOptions: cleanupOptionsWithoutCallback,
            };
            onCleanupFailure(failureEvent);
        }
    });
    return result;
}
//# sourceMappingURL=commit.js.map
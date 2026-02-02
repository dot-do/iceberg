/**
 * Iceberg Error Classes
 *
 * Centralized error definitions for the Apache Iceberg TypeScript implementation.
 * All custom errors extend the base IcebergError class for consistent error handling.
 *
 * @example
 * ```ts
 * import { MetadataError, ValidationError } from '@dot-do/iceberg';
 *
 * try {
 *   // ... operation
 * } catch (error) {
 *   if (error instanceof MetadataError) {
 *     console.log(`Metadata error (${error.code}): ${error.message}`);
 *   }
 * }
 * ```
 */
// ============================================================================
// Base Error
// ============================================================================
/**
 * Base error class for all Iceberg-related errors.
 * Provides a consistent structure with error codes for programmatic handling.
 */
export class IcebergError extends Error {
    /** Error code for programmatic handling */
    code;
    constructor(message, code = 'ICEBERG_ERROR') {
        super(message);
        this.name = 'IcebergError';
        this.code = code;
        // Maintain proper prototype chain for instanceof checks
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
/**
 * Error thrown during metadata operations (read, write, parse).
 */
export class MetadataError extends IcebergError {
    /** Specific metadata error code */
    code;
    /** Path to the metadata file (if applicable) */
    metadataPath;
    constructor(message, code, metadataPath) {
        super(message, code);
        this.name = 'MetadataError';
        this.code = code;
        this.metadataPath = metadataPath;
    }
}
/**
 * Error thrown during catalog operations.
 */
export class CatalogError extends IcebergError {
    /** Specific catalog error code */
    code;
    /** Table identifier (if applicable) */
    tableIdentifier;
    /** Namespace (if applicable) */
    namespace;
    constructor(message, code, options) {
        super(message, code);
        this.name = 'CatalogError';
        this.code = code;
        this.tableIdentifier = options?.tableIdentifier;
        this.namespace = options?.namespace;
    }
}
/**
 * Error thrown during storage operations (read, write, delete, list).
 */
export class StorageError extends IcebergError {
    /** Specific storage error code */
    code;
    /** Path that caused the error (if applicable) */
    path;
    /** Original error from the storage backend */
    cause;
    constructor(message, code, options) {
        super(message, code);
        this.name = 'StorageError';
        this.code = code;
        this.path = options?.path;
        this.cause = options?.cause;
    }
}
/**
 * Error thrown during validation operations.
 */
export class ValidationError extends IcebergError {
    /** Specific validation error code */
    code;
    /** Field or property that failed validation (if applicable) */
    field;
    /** Expected value or type (if applicable) */
    expected;
    /** Actual value or type received (if applicable) */
    actual;
    constructor(message, code, options) {
        super(message, code);
        this.name = 'ValidationError';
        this.code = code;
        this.field = options?.field;
        this.expected = options?.expected;
        this.actual = options?.actual;
    }
}
// ============================================================================
// Commit Errors (re-exported from commit.ts for centralization)
// ============================================================================
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export class CommitConflictError extends IcebergError {
    /** The version we expected the table to be at */
    expectedVersion;
    /** The actual version we found */
    actualVersion;
    constructor(message, expectedVersion, actualVersion) {
        super(message, 'COMMIT_CONFLICT');
        this.name = 'CommitConflictError';
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }
}
/**
 * Error thrown when max retries are exhausted.
 */
export class CommitRetryExhaustedError extends IcebergError {
    /** Number of attempts made before giving up */
    attempts;
    /** The last error that occurred */
    lastError;
    constructor(message, attempts, lastError) {
        super(message, 'COMMIT_RETRY_EXHAUSTED');
        this.name = 'CommitRetryExhaustedError';
        this.attempts = attempts;
        this.lastError = lastError;
    }
}
/**
 * Error thrown when a commit transaction fails and needs cleanup.
 */
export class CommitTransactionError extends IcebergError {
    /** Files that were written and need cleanup */
    writtenFiles;
    /** Whether cleanup was successful */
    cleanupSuccessful;
    constructor(message, writtenFiles, cleanupSuccessful) {
        super(message, 'COMMIT_TRANSACTION_ERROR');
        this.name = 'CommitTransactionError';
        this.writtenFiles = writtenFiles;
        this.cleanupSuccessful = cleanupSuccessful;
    }
}
/**
 * Error thrown during schema evolution operations.
 */
export class SchemaEvolutionError extends IcebergError {
    /** Specific schema evolution error code */
    code;
    constructor(message, code) {
        super(message, code);
        this.name = 'SchemaEvolutionError';
        this.code = code;
    }
}
// ============================================================================
// Transform Errors
// ============================================================================
/**
 * Error thrown during partition transform operations.
 */
export class TransformError extends IcebergError {
    /** The transform that failed */
    transform;
    /** The value that caused the error (if applicable) */
    value;
    constructor(message, transform, value) {
        super(message, 'TRANSFORM_ERROR');
        this.name = 'TransformError';
        this.transform = transform;
        this.value = value;
    }
}
// ============================================================================
// Utility Functions
// ============================================================================
/**
 * Type guard to check if an error is an IcebergError.
 */
export function isIcebergError(error) {
    return error instanceof IcebergError;
}
/**
 * Type guard to check if an error is a MetadataError.
 */
export function isMetadataError(error) {
    return error instanceof MetadataError;
}
/**
 * Type guard to check if an error is a CatalogError.
 */
export function isCatalogError(error) {
    return error instanceof CatalogError;
}
/**
 * Type guard to check if an error is a StorageError.
 */
export function isStorageError(error) {
    return error instanceof StorageError;
}
/**
 * Type guard to check if an error is a ValidationError.
 */
export function isValidationError(error) {
    return error instanceof ValidationError;
}
/**
 * Type guard to check if an error is a CommitConflictError.
 */
export function isCommitConflictError(error) {
    return error instanceof CommitConflictError;
}
/**
 * Type guard to check if an error is a SchemaEvolutionError.
 */
export function isSchemaEvolutionError(error) {
    return error instanceof SchemaEvolutionError;
}
/**
 * Wrap an unknown error in an IcebergError if it isn't already one.
 */
export function wrapError(error, defaultMessage = 'Unknown error') {
    if (error instanceof IcebergError) {
        return error;
    }
    if (error instanceof Error) {
        const wrapped = new IcebergError(error.message, 'WRAPPED_ERROR');
        wrapped.stack = error.stack;
        return wrapped;
    }
    return new IcebergError(String(error) || defaultMessage, 'UNKNOWN_ERROR');
}
//# sourceMappingURL=errors.js.map
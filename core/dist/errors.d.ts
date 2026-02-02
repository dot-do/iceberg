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
/**
 * Base error class for all Iceberg-related errors.
 * Provides a consistent structure with error codes for programmatic handling.
 */
export declare class IcebergError extends Error {
    /** Error code for programmatic handling */
    readonly code: string;
    constructor(message: string, code?: string);
}
/**
 * Error codes for metadata operations.
 */
export type MetadataErrorCode = 'METADATA_NOT_FOUND' | 'METADATA_PARSE_ERROR' | 'METADATA_WRITE_ERROR' | 'METADATA_VERSION_MISMATCH' | 'INVALID_FORMAT_VERSION' | 'MISSING_REQUIRED_FIELD' | 'INVALID_METADATA_STRUCTURE' | 'SCHEMA_NOT_FOUND' | 'PARTITION_SPEC_NOT_FOUND' | 'SORT_ORDER_NOT_FOUND' | 'SNAPSHOT_NOT_FOUND';
/**
 * Error thrown during metadata operations (read, write, parse).
 */
export declare class MetadataError extends IcebergError {
    /** Specific metadata error code */
    readonly code: MetadataErrorCode;
    /** Path to the metadata file (if applicable) */
    readonly metadataPath?: string;
    constructor(message: string, code: MetadataErrorCode, metadataPath?: string);
}
/**
 * Error codes for catalog operations.
 */
export type CatalogErrorCode = 'TABLE_NOT_FOUND' | 'TABLE_ALREADY_EXISTS' | 'NAMESPACE_NOT_FOUND' | 'NAMESPACE_ALREADY_EXISTS' | 'CATALOG_CONNECTION_ERROR' | 'CATALOG_PERMISSION_DENIED' | 'INVALID_TABLE_IDENTIFIER' | 'INVALID_NAMESPACE';
/**
 * Error thrown during catalog operations.
 */
export declare class CatalogError extends IcebergError {
    /** Specific catalog error code */
    readonly code: CatalogErrorCode;
    /** Table identifier (if applicable) */
    readonly tableIdentifier?: string;
    /** Namespace (if applicable) */
    readonly namespace?: string;
    constructor(message: string, code: CatalogErrorCode, options?: {
        tableIdentifier?: string;
        namespace?: string;
    });
}
/**
 * Error codes for storage operations.
 */
export type StorageErrorCode = 'STORAGE_READ_ERROR' | 'STORAGE_WRITE_ERROR' | 'STORAGE_DELETE_ERROR' | 'STORAGE_LIST_ERROR' | 'FILE_NOT_FOUND' | 'FILE_ALREADY_EXISTS' | 'PERMISSION_DENIED' | 'STORAGE_CONNECTION_ERROR' | 'INVALID_PATH';
/**
 * Error thrown during storage operations (read, write, delete, list).
 */
export declare class StorageError extends IcebergError {
    /** Specific storage error code */
    readonly code: StorageErrorCode;
    /** Path that caused the error (if applicable) */
    readonly path?: string;
    /** Original error from the storage backend */
    readonly cause?: Error;
    constructor(message: string, code: StorageErrorCode, options?: {
        path?: string;
        cause?: Error;
    });
}
/**
 * Error codes for validation operations.
 */
export type ValidationErrorCode = 'INVALID_SCHEMA' | 'INVALID_PARTITION_SPEC' | 'INVALID_SORT_ORDER' | 'INVALID_SNAPSHOT' | 'INVALID_MANIFEST' | 'INVALID_DATA_FILE' | 'TYPE_MISMATCH' | 'CONSTRAINT_VIOLATION' | 'DUPLICATE_FIELD_ID' | 'DUPLICATE_FIELD_NAME' | 'INVALID_TRANSFORM';
/**
 * Error thrown during validation operations.
 */
export declare class ValidationError extends IcebergError {
    /** Specific validation error code */
    readonly code: ValidationErrorCode;
    /** Field or property that failed validation (if applicable) */
    readonly field?: string;
    /** Expected value or type (if applicable) */
    readonly expected?: string;
    /** Actual value or type received (if applicable) */
    readonly actual?: string;
    constructor(message: string, code: ValidationErrorCode, options?: {
        field?: string;
        expected?: string;
        actual?: string;
    });
}
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export declare class CommitConflictError extends IcebergError {
    /** The version we expected the table to be at */
    readonly expectedVersion: number;
    /** The actual version we found */
    readonly actualVersion: number;
    constructor(message: string, expectedVersion: number, actualVersion: number);
}
/**
 * Error thrown when max retries are exhausted.
 */
export declare class CommitRetryExhaustedError extends IcebergError {
    /** Number of attempts made before giving up */
    readonly attempts: number;
    /** The last error that occurred */
    readonly lastError: Error;
    constructor(message: string, attempts: number, lastError: Error);
}
/**
 * Error thrown when a commit transaction fails and needs cleanup.
 */
export declare class CommitTransactionError extends IcebergError {
    /** Files that were written and need cleanup */
    readonly writtenFiles: string[];
    /** Whether cleanup was successful */
    readonly cleanupSuccessful: boolean;
    constructor(message: string, writtenFiles: string[], cleanupSuccessful: boolean);
}
/** Error codes for schema evolution operations */
export type SchemaEvolutionErrorCode = 'FIELD_NOT_FOUND' | 'FIELD_EXISTS' | 'INCOMPATIBLE_TYPE' | 'REQUIRED_FIELD_NO_DEFAULT' | 'INVALID_OPERATION' | 'INVALID_POSITION' | 'IDENTIFIER_FIELD';
/**
 * Error thrown during schema evolution operations.
 */
export declare class SchemaEvolutionError extends IcebergError {
    /** Specific schema evolution error code */
    readonly code: SchemaEvolutionErrorCode;
    constructor(message: string, code: SchemaEvolutionErrorCode);
}
/**
 * Error thrown during partition transform operations.
 */
export declare class TransformError extends IcebergError {
    /** The transform that failed */
    readonly transform: string;
    /** The value that caused the error (if applicable) */
    readonly value?: unknown;
    constructor(message: string, transform: string, value?: unknown);
}
/**
 * Type guard to check if an error is an IcebergError.
 */
export declare function isIcebergError(error: unknown): error is IcebergError;
/**
 * Type guard to check if an error is a MetadataError.
 */
export declare function isMetadataError(error: unknown): error is MetadataError;
/**
 * Type guard to check if an error is a CatalogError.
 */
export declare function isCatalogError(error: unknown): error is CatalogError;
/**
 * Type guard to check if an error is a StorageError.
 */
export declare function isStorageError(error: unknown): error is StorageError;
/**
 * Type guard to check if an error is a ValidationError.
 */
export declare function isValidationError(error: unknown): error is ValidationError;
/**
 * Type guard to check if an error is a CommitConflictError.
 */
export declare function isCommitConflictError(error: unknown): error is CommitConflictError;
/**
 * Type guard to check if an error is a SchemaEvolutionError.
 */
export declare function isSchemaEvolutionError(error: unknown): error is SchemaEvolutionError;
/**
 * Wrap an unknown error in an IcebergError if it isn't already one.
 */
export declare function wrapError(error: unknown, defaultMessage?: string): IcebergError;
//# sourceMappingURL=errors.d.ts.map
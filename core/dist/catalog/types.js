/**
 * Catalog Abstraction Layer
 *
 * Defines the core interfaces and types for all Iceberg catalog implementations.
 * This abstraction allows for pluggable catalog backends including:
 * - Filesystem (local, S3, R2, GCS)
 * - REST catalogs (Iceberg REST Catalog spec)
 * - Hive Metastore
 * - AWS Glue
 * - Nessie
 * - Custom implementations
 *
 * @see https://iceberg.apache.org/spec/#catalog-api
 */
/**
 * Convert a TableIdentifier to a dot-separated string.
 */
export function tableIdentifierToString(identifier) {
    return [...identifier.namespace, identifier.name].join('.');
}
/**
 * Parse a dot-separated string into a TableIdentifier.
 * The last segment becomes the table name, rest become the namespace.
 */
export function parseTableIdentifier(fullName) {
    const parts = fullName.split('.');
    if (parts.length < 1) {
        throw new Error('Invalid table identifier: must have at least a name');
    }
    const name = parts.pop();
    return { namespace: parts, name };
}
/**
 * Check if two TableIdentifiers are equal.
 */
export function tableIdentifiersEqual(a, b) {
    if (a.name !== b.name)
        return false;
    if (a.namespace.length !== b.namespace.length)
        return false;
    return a.namespace.every((n, i) => n === b.namespace[i]);
}
// ============================================================================
// Catalog Errors
// ============================================================================
/**
 * Base error for all catalog operations.
 */
export class CatalogError extends Error {
    code;
    cause;
    constructor(message, code, cause) {
        super(message);
        this.code = code;
        this.cause = cause;
        this.name = 'CatalogError';
        Error.captureStackTrace?.(this, this.constructor);
    }
}
/**
 * Error thrown when a namespace does not exist.
 *
 * @example
 * ```ts
 * try {
 *   await catalog.loadTable({ namespace: ['nonexistent'], name: 'table' });
 * } catch (error) {
 *   if (error instanceof NamespaceNotFoundError) {
 *     console.log(`Namespace not found: ${error.namespace.join('.')}`);
 *   }
 * }
 * ```
 */
export class NamespaceNotFoundError extends CatalogError {
    namespace;
    constructor(namespace) {
        super(`Namespace does not exist: ${namespace.join('.')}`, 'NAMESPACE_NOT_FOUND');
        this.namespace = namespace;
        this.name = 'NamespaceNotFoundError';
    }
}
/**
 * Error thrown when attempting to create a namespace that already exists.
 */
export class NamespaceAlreadyExistsError extends CatalogError {
    namespace;
    constructor(namespace) {
        super(`Namespace already exists: ${namespace.join('.')}`, 'NAMESPACE_ALREADY_EXISTS');
        this.namespace = namespace;
        this.name = 'NamespaceAlreadyExistsError';
    }
}
/**
 * Error thrown when attempting to drop a non-empty namespace.
 */
export class NamespaceNotEmptyError extends CatalogError {
    namespace;
    constructor(namespace) {
        super(`Namespace is not empty: ${namespace.join('.')}`, 'NAMESPACE_NOT_EMPTY');
        this.namespace = namespace;
        this.name = 'NamespaceNotEmptyError';
    }
}
/**
 * Error thrown when a table does not exist.
 *
 * @example
 * ```ts
 * try {
 *   await catalog.loadTable({ namespace: ['db'], name: 'nonexistent' });
 * } catch (error) {
 *   if (error instanceof TableNotFoundError) {
 *     console.log(`Table not found: ${tableIdentifierToString(error.identifier)}`);
 *   }
 * }
 * ```
 */
export class TableNotFoundError extends CatalogError {
    identifier;
    constructor(identifier) {
        super(`Table does not exist: ${tableIdentifierToString(identifier)}`, 'TABLE_NOT_FOUND');
        this.identifier = identifier;
        this.name = 'TableNotFoundError';
    }
}
/**
 * Error thrown when attempting to create a table that already exists.
 */
export class TableAlreadyExistsError extends CatalogError {
    identifier;
    constructor(identifier) {
        super(`Table already exists: ${tableIdentifierToString(identifier)}`, 'TABLE_ALREADY_EXISTS');
        this.identifier = identifier;
        this.name = 'TableAlreadyExistsError';
    }
}
/**
 * Error thrown when a commit fails due to concurrent modification.
 */
export class CommitFailedError extends CatalogError {
    identifier;
    requirement;
    constructor(identifier, requirement, message) {
        super(message ?? `Commit failed for ${tableIdentifierToString(identifier)}: requirement not satisfied`, 'COMMIT_FAILED');
        this.identifier = identifier;
        this.requirement = requirement;
        this.name = 'CommitFailedError';
    }
}
/**
 * Error thrown when a commit conflicts with another concurrent commit.
 */
export class CommitConflictError extends CatalogError {
    identifier;
    constructor(identifier, message) {
        super(message ?? `Commit conflict for ${tableIdentifierToString(identifier)}`, 'COMMIT_CONFLICT');
        this.identifier = identifier;
        this.name = 'CommitConflictError';
    }
}
/**
 * Error thrown when catalog authentication fails.
 */
export class AuthenticationError extends CatalogError {
    constructor(message = 'Authentication failed') {
        super(message, 'AUTHENTICATION_FAILED');
        this.name = 'AuthenticationError';
    }
}
/**
 * Error thrown when an operation is not permitted.
 */
export class AuthorizationError extends CatalogError {
    operation;
    resource;
    constructor(operation, resource, message) {
        super(message ?? `Not authorized to ${operation} ${resource}`, 'AUTHORIZATION_FAILED');
        this.operation = operation;
        this.resource = resource;
        this.name = 'AuthorizationError';
    }
}
/**
 * Error thrown when the catalog service is unavailable.
 */
export class ServiceUnavailableError extends CatalogError {
    constructor(message = 'Catalog service is unavailable') {
        super(message, 'SERVICE_UNAVAILABLE');
        this.name = 'ServiceUnavailableError';
    }
}
/**
 * Error thrown when an invalid argument is provided.
 */
export class InvalidArgumentError extends CatalogError {
    argument;
    constructor(argument, message) {
        super(message, 'INVALID_ARGUMENT');
        this.argument = argument;
        this.name = 'InvalidArgumentError';
    }
}
// ============================================================================
// Type Guards
// ============================================================================
/**
 * Check if an error is a CatalogError.
 */
export function isCatalogError(error) {
    return error instanceof CatalogError;
}
/**
 * Check if an error is a NamespaceNotFoundError.
 */
export function isNamespaceNotFoundError(error) {
    return error instanceof NamespaceNotFoundError;
}
/**
 * Check if an error is a TableNotFoundError.
 */
export function isTableNotFoundError(error) {
    return error instanceof TableNotFoundError;
}
/**
 * Check if an error is a CommitFailedError.
 */
export function isCommitFailedError(error) {
    return error instanceof CommitFailedError;
}
/**
 * Check if an error is a CommitConflictError.
 */
export function isCommitConflictError(error) {
    return error instanceof CommitConflictError;
}
//# sourceMappingURL=types.js.map
/**
 * Iceberg Catalog Module
 *
 * Provides catalog management for Iceberg tables.
 * Supports multiple catalog implementations including filesystem and R2.
 *
 * @see https://iceberg.apache.org/spec/
 */
// ============================================================================
// Catalog Abstraction Layer (types.ts)
// ============================================================================
export { tableIdentifierToString, parseTableIdentifier, tableIdentifiersEqual, 
// Error types
CatalogError, NamespaceNotFoundError, NamespaceAlreadyExistsError, NamespaceNotEmptyError, TableNotFoundError, TableAlreadyExistsError, CommitFailedError, CommitConflictError, AuthenticationError, AuthorizationError, ServiceUnavailableError, InvalidArgumentError, 
// Type guards
isCatalogError, isNamespaceNotFoundError, isTableNotFoundError, isCommitFailedError, isCommitConflictError, } from './types.js';
// ============================================================================
// Filesystem Catalog Implementation
// ============================================================================
export { 
// Classes
FileSystemCatalog, MemoryCatalog, 
// Factory
createCatalog, } from './filesystem.js';
export { 
// R2 Data Catalog client
R2DataCatalogClient, R2DataCatalogError, createR2DataCatalogClient, createCatalogClient, } from './r2-client.js';
//# sourceMappingURL=index.js.map
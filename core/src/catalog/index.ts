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

export {
  // Core interfaces
  type Catalog,
  type CatalogConfig,
  type CatalogFactory,

  // Table identifier
  type TableIdentifier,
  tableIdentifierToString,
  parseTableIdentifier,
  tableIdentifiersEqual,

  // Namespace properties
  type NamespaceProperties,
  type UpdateNamespacePropertiesResponse,

  // Table operations
  type CreateTableRequest,
  type CreateTableResponse,
  type LoadTableRequest,
  type LoadTableResponse,

  // Commit operations
  type TableRequirement,
  type TableUpdate,
  type CommitTableRequest,
  type CommitTableResponse,

  // Error types
  CatalogError,
  NamespaceNotFoundError,
  NamespaceAlreadyExistsError,
  NamespaceNotEmptyError,
  TableNotFoundError,
  TableAlreadyExistsError,
  CommitFailedError,
  CommitConflictError,
  AuthenticationError,
  AuthorizationError,
  ServiceUnavailableError,
  InvalidArgumentError,

  // Type guards
  isCatalogError,
  isNamespaceNotFoundError,
  isTableNotFoundError,
  isCommitFailedError,
  isCommitConflictError,
} from './types.js';

// ============================================================================
// Filesystem Catalog Implementation
// ============================================================================

export {
  // Legacy interface (deprecated, use Catalog from types.ts)
  type IcebergCatalog,
  // Classes
  FileSystemCatalog,
  MemoryCatalog,
  // Factory
  createCatalog,
} from './filesystem.js';

export {
  // R2 Data Catalog types
  type R2DataCatalogConfig,
  type CatalogNamespace,
  type RegisterTableRequest,
  type UpdateTableRequest,
  type CatalogTable,
  type ListTablesResponse,
  type ListNamespacesResponse,
  type CatalogErrorResponse,
  type CatalogApiResponse,
  // R2 Data Catalog client
  R2DataCatalogClient,
  R2DataCatalogError,
  createR2DataCatalogClient,
  createCatalogClient,
} from './r2-client.js';

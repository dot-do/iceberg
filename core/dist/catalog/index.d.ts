/**
 * Iceberg Catalog Module
 *
 * Provides catalog management for Iceberg tables.
 * Supports multiple catalog implementations including filesystem and R2.
 *
 * @see https://iceberg.apache.org/spec/
 */
export { type Catalog, type CatalogConfig, type CatalogFactory, type TableIdentifier, tableIdentifierToString, parseTableIdentifier, tableIdentifiersEqual, type NamespaceProperties, type UpdateNamespacePropertiesResponse, type CreateTableRequest, type CreateTableResponse, type LoadTableRequest, type LoadTableResponse, type TableRequirement, type TableUpdate, type CommitTableRequest, type CommitTableResponse, CatalogError, NamespaceNotFoundError, NamespaceAlreadyExistsError, NamespaceNotEmptyError, TableNotFoundError, TableAlreadyExistsError, CommitFailedError, CommitConflictError, AuthenticationError, AuthorizationError, ServiceUnavailableError, InvalidArgumentError, isCatalogError, isNamespaceNotFoundError, isTableNotFoundError, isCommitFailedError, isCommitConflictError, } from './types.js';
export { type IcebergCatalog, FileSystemCatalog, MemoryCatalog, createCatalog, } from './filesystem.js';
export { type R2DataCatalogConfig, type CatalogNamespace, type RegisterTableRequest, type UpdateTableRequest, type CatalogTable, type ListTablesResponse, type ListNamespacesResponse, type CatalogErrorResponse, type CatalogApiResponse, R2DataCatalogClient, R2DataCatalogError, createR2DataCatalogClient, createCatalogClient, } from './r2-client.js';
//# sourceMappingURL=index.d.ts.map
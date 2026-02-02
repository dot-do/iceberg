/**
 * Catalog Module
 *
 * Exports catalog backend implementations and types.
 */

// Types and errors
export {
  CatalogError,
  CatalogErrorCode,
  NamespaceNotFoundError,
  NamespaceAlreadyExistsError,
  NamespaceNotEmptyError,
  TableNotFoundError,
  TableAlreadyExistsError,
  ConcurrencyConflictError,
  encodeNamespace,
  decodeNamespace,
} from './types.js';

export type {
  CatalogBackend,
  TableData,
  NamespaceData,
} from './types.js';

// Durable Object backend
export { CatalogDO } from './durable-object.js';

// D1 backend
export { D1CatalogBackend, D1_SCHEMA, createD1CatalogHandler } from './d1.js';

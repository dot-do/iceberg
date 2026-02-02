/**
 * Catalog Backend Types and Interfaces
 *
 * Common types and interfaces for catalog backend implementations.
 * Both Durable Object and D1 backends implement these interfaces.
 */

// ============================================================================
// Error Types
// ============================================================================

/** Catalog error codes for typed error handling */
export enum CatalogErrorCode {
  NOT_FOUND = 'NOT_FOUND',
  ALREADY_EXISTS = 'ALREADY_EXISTS',
  NOT_EMPTY = 'NOT_EMPTY',
  CONFLICT = 'CONFLICT',
  INVALID_INPUT = 'INVALID_INPUT',
  INTERNAL_ERROR = 'INTERNAL_ERROR',
  TRANSACTION_FAILED = 'TRANSACTION_FAILED',
}

/** Base catalog error with type information */
export class CatalogError extends Error {
  constructor(
    message: string,
    public readonly code: CatalogErrorCode,
    public readonly statusCode: number = 500
  ) {
    super(message);
    this.name = 'CatalogError';
  }
}

/** Namespace not found error */
export class NamespaceNotFoundError extends CatalogError {
  constructor(namespace: string[]) {
    super(
      `Namespace does not exist: ${namespace.join('.')}`,
      CatalogErrorCode.NOT_FOUND,
      404
    );
    this.name = 'NamespaceNotFoundError';
  }
}

/** Namespace already exists error */
export class NamespaceAlreadyExistsError extends CatalogError {
  constructor(namespace: string[]) {
    super(
      `Namespace already exists: ${namespace.join('.')}`,
      CatalogErrorCode.ALREADY_EXISTS,
      409
    );
    this.name = 'NamespaceAlreadyExistsError';
  }
}

/** Namespace not empty error */
export class NamespaceNotEmptyError extends CatalogError {
  constructor(namespace: string[]) {
    super(
      `Namespace is not empty: ${namespace.join('.')}`,
      CatalogErrorCode.NOT_EMPTY,
      409
    );
    this.name = 'NamespaceNotEmptyError';
  }
}

/** Table not found error */
export class TableNotFoundError extends CatalogError {
  constructor(namespace: string[], name: string) {
    super(
      `Table does not exist: ${namespace.join('.')}.${name}`,
      CatalogErrorCode.NOT_FOUND,
      404
    );
    this.name = 'TableNotFoundError';
  }
}

/** Table already exists error */
export class TableAlreadyExistsError extends CatalogError {
  constructor(namespace: string[], name: string) {
    super(
      `Table already exists: ${namespace.join('.')}.${name}`,
      CatalogErrorCode.ALREADY_EXISTS,
      409
    );
    this.name = 'TableAlreadyExistsError';
  }
}

/** Concurrency conflict error (OCC) */
export class ConcurrencyConflictError extends CatalogError {
  constructor(message: string) {
    super(message, CatalogErrorCode.CONFLICT, 409);
    this.name = 'ConcurrencyConflictError';
  }
}

// ============================================================================
// Data Types
// ============================================================================

/** Table data stored in catalog */
export interface TableData {
  location: string;
  metadataLocation: string;
  metadata?: unknown;
  properties: Record<string, string>;
  version: number;
}

/** Namespace data stored in catalog */
export interface NamespaceData {
  namespace: string[];
  properties: Record<string, string>;
  createdAt: number;
  updatedAt: number;
}

// ============================================================================
// Catalog Backend Interface
// ============================================================================

/**
 * Interface for catalog backend implementations.
 * Both Durable Object and D1 backends implement this interface.
 */
export interface CatalogBackend {
  // Namespace Operations
  listNamespaces(parent?: string[]): Promise<string[][]>;
  createNamespace(
    namespace: string[],
    properties?: Record<string, string>
  ): Promise<NamespaceData>;
  namespaceExists(namespace: string[]): Promise<boolean>;
  getNamespace(namespace: string[]): Promise<Record<string, string>>;
  getNamespaceData(namespace: string[]): Promise<NamespaceData | null>;
  updateNamespaceProperties(
    namespace: string[],
    updates: Record<string, string>,
    removals: string[]
  ): Promise<{ updated: string[]; removed: string[]; missing: string[] }>;
  dropNamespace(namespace: string[]): Promise<boolean>;

  // Table Operations
  listTables(
    namespace: string[]
  ): Promise<Array<{ namespace: string[]; name: string }>>;
  createTable(
    namespace: string[],
    name: string,
    location: string,
    metadataLocation: string,
    metadata?: unknown,
    properties?: Record<string, string>
  ): Promise<TableData>;
  tableExists(namespace: string[], name: string): Promise<boolean>;
  getTable(namespace: string[], name: string): Promise<TableData | null>;
  updateTableMetadata(
    namespace: string[],
    name: string,
    metadataLocation: string,
    metadata?: unknown,
    expectedVersion?: number
  ): Promise<{ success: boolean; newVersion: number }>;
  dropTable(namespace: string[], name: string): Promise<boolean>;
  renameTable(
    fromNamespace: string[],
    fromName: string,
    toNamespace: string[],
    toName: string
  ): Promise<boolean>;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Encode namespace array to string key using unit separator.
 */
export function encodeNamespace(namespace: string[]): string {
  return namespace.join('\x1f');
}

/**
 * Decode namespace string key to array.
 */
export function decodeNamespace(key: string): string[] {
  return key.split('\x1f');
}

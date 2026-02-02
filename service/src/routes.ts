/**
 * Iceberg REST Catalog Routes
 *
 * Implements the Iceberg REST Catalog API specification.
 *
 * @see https://iceberg.apache.org/spec/#iceberg-rest-catalog
 */

import { Hono } from 'hono';
import type { Context } from 'hono';
import type { Env } from './index.js';
import {
  requireNamespacePermission,
  requireTablePermission,
  type AuthorizationVariables,
} from './auth/authorization-middleware.js';

// ============================================================================
// Types
// ============================================================================

/** Iceberg error response per REST spec */
interface IcebergErrorResponse {
  error: {
    message: string;
    type: string;
    code: number;
    stack?: string[];
  };
}

/** Request body for creating a namespace */
interface CreateNamespaceRequest {
  namespace: string[];
  properties?: Record<string, string>;
}

/** Request body for creating a table */
interface CreateTableRequest {
  name: string;
  location?: string;
  schema: IcebergSchema;
  'partition-spec'?: PartitionSpec;
  'write-order'?: SortOrder;
  'stage-create'?: boolean;
  properties?: Record<string, string>;
}

/** Request body for table commit */
interface CommitTableRequest {
  identifier?: { namespace: string[]; name: string };
  requirements: TableRequirement[];
  updates: TableUpdate[];
}

/** Request body for rename table */
interface RenameTableRequest {
  source: { namespace: string[]; name: string };
  destination: { namespace: string[]; name: string };
}

/** Request body for registering an existing table */
interface RegisterTableRequest {
  name: string;
  'metadata-location': string;
}

/** Request body for updating namespace properties */
interface UpdateNamespacePropertiesRequest {
  removals?: string[];
  updates?: Record<string, string>;
}

/** Table requirement for optimistic locking */
type TableRequirement =
  | { type: 'assert-create' }
  | { type: 'assert-table-uuid'; uuid: string }
  | { type: 'assert-ref-snapshot-id'; ref: string; 'snapshot-id': number | null }
  | { type: 'assert-last-assigned-field-id'; 'last-assigned-field-id': number }
  | { type: 'assert-current-schema-id'; 'current-schema-id': number }
  | { type: 'assert-last-assigned-partition-id'; 'last-assigned-partition-id': number }
  | { type: 'assert-default-spec-id'; 'default-spec-id': number }
  | { type: 'assert-default-sort-order-id'; 'default-sort-order-id': number };

/** Table update operations */
type TableUpdate =
  | { action: 'assign-uuid'; uuid: string }
  | { action: 'upgrade-format-version'; 'format-version': number }
  | { action: 'add-schema'; schema: IcebergSchema; 'last-column-id'?: number }
  | { action: 'set-current-schema'; 'schema-id': number }
  | { action: 'add-spec'; spec: PartitionSpec }
  | { action: 'set-default-spec'; 'spec-id': number }
  | { action: 'add-sort-order'; 'sort-order': SortOrder }
  | { action: 'set-default-sort-order'; 'sort-order-id': number }
  | { action: 'add-snapshot'; snapshot: Snapshot }
  | { action: 'remove-snapshots'; 'snapshot-ids': number[] }
  | { action: 'remove-snapshot-ref'; 'ref-name': string }
  | { action: 'set-snapshot-ref'; 'ref-name': string; type: 'branch' | 'tag'; 'snapshot-id': number; 'max-ref-age-ms'?: number; 'max-snapshot-age-ms'?: number; 'min-snapshots-to-keep'?: number }
  | { action: 'set-properties'; updates: Record<string, string> }
  | { action: 'remove-properties'; removals: string[] }
  | { action: 'set-location'; location: string };

// Simplified types for table metadata
interface IcebergSchema {
  type: 'struct';
  'schema-id'?: number;
  'identifier-field-ids'?: number[];
  fields: IcebergField[];
}

interface IcebergField {
  id: number;
  name: string;
  required: boolean;
  type: string | IcebergSchema | IcebergListType | IcebergMapType;
  doc?: string;
}

interface IcebergListType {
  type: 'list';
  'element-id': number;
  element: string | IcebergSchema;
  'element-required': boolean;
}

interface IcebergMapType {
  type: 'map';
  'key-id': number;
  key: string | IcebergSchema;
  'value-id': number;
  value: string | IcebergSchema;
  'value-required': boolean;
}

interface PartitionSpec {
  'spec-id': number;
  fields: PartitionField[];
}

interface PartitionField {
  'field-id': number;
  'source-id': number;
  name: string;
  transform: string;
}

interface SortOrder {
  'order-id': number;
  fields: SortField[];
}

interface SortField {
  'source-id': number;
  transform: string;
  direction: 'asc' | 'desc';
  'null-order': 'nulls-first' | 'nulls-last';
}

interface Snapshot {
  'snapshot-id': number;
  'parent-snapshot-id'?: number;
  'sequence-number': number;
  'timestamp-ms': number;
  'manifest-list': string;
  summary: Record<string, string>;
  'schema-id'?: number;
}

interface TableMetadata {
  'format-version': number;
  'table-uuid': string;
  location: string;
  'last-sequence-number'?: number;
  'last-updated-ms': number;
  'last-column-id': number;
  'current-schema-id': number;
  schemas: IcebergSchema[];
  'default-spec-id': number;
  'partition-specs': PartitionSpec[];
  'last-partition-id': number;
  'default-sort-order-id': number;
  'sort-orders': SortOrder[];
  properties?: Record<string, string>;
  'current-snapshot-id'?: number;
  snapshots?: Snapshot[];
  'snapshot-log'?: Array<{ 'timestamp-ms': number; 'snapshot-id': number }>;
  'metadata-log'?: Array<{ 'timestamp-ms': number; 'metadata-file': string }>;
  refs?: Record<string, { 'snapshot-id': number; type: 'branch' | 'tag' }>;
}

// ============================================================================
// Types for Context Variables
// ============================================================================

interface ContextVariables extends AuthorizationVariables {
  catalogStub: { fetch: (request: Request) => Promise<Response> };
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get the catalog stub for a given context.
 * Works with both Durable Object and D1 backends.
 */
function getCatalogStub(c: Context<{ Bindings: Env; Variables: ContextVariables }>): { fetch: (request: Request) => Promise<Response> } {
  return c.get('catalogStub');
}

/**
 * Parse namespace from URL parameter.
 * Namespaces can be multi-level, encoded with URL encoding.
 * Examples: "db" -> ["db"], "db%1Fschema" -> ["db", "schema"]
 */
function parseNamespace(namespaceParam: string): string[] {
  // Decode URL encoding and split by unit separator or dot
  const decoded = decodeURIComponent(namespaceParam);
  // Check for unit separator (common in Iceberg) or fall back to dot notation
  if (decoded.includes('\x1f')) {
    return decoded.split('\x1f');
  }
  // Support dot notation for convenience
  if (decoded.includes('.')) {
    return decoded.split('.');
  }
  return [decoded];
}

/**
 * Create an Iceberg error response.
 */
function icebergError(
  c: Context,
  message: string,
  type: string,
  code: number
): Response {
  const error: IcebergErrorResponse = {
    error: {
      message,
      type,
      code,
    },
  };
  return c.json(error, code as 400 | 404 | 409 | 500);
}

/**
 * Deep copy a schema field to preserve field IDs exactly.
 */
function deepCopyField(field: IcebergField): IcebergField {
  const copy: IcebergField = {
    id: field.id,
    name: field.name,
    required: field.required,
    type: deepCopyType(field.type),
  };
  if (field.doc !== undefined) {
    copy.doc = field.doc;
  }
  return copy;
}

/**
 * Deep copy a type (handles nested structs, lists, maps).
 */
function deepCopyType(type: string | IcebergSchema | IcebergListType | IcebergMapType): string | IcebergSchema | IcebergListType | IcebergMapType {
  if (typeof type === 'string') {
    return type;
  }

  if ('fields' in type && type.type === 'struct') {
    // It's a struct type
    const result: IcebergSchema = {
      type: 'struct',
      fields: type.fields.map(deepCopyField),
    };
    if (type['schema-id'] !== undefined) {
      result['schema-id'] = type['schema-id'];
    }
    if (type['identifier-field-ids'] !== undefined) {
      result['identifier-field-ids'] = [...type['identifier-field-ids']];
    }
    return result;
  }

  if ('element-id' in type) {
    // It's a list type
    return {
      type: 'list',
      'element-id': type['element-id'],
      element: deepCopyType(type.element),
      'element-required': type['element-required'],
    } as IcebergListType;
  }

  if ('key-id' in type) {
    // It's a map type
    return {
      type: 'map',
      'key-id': type['key-id'],
      key: deepCopyType(type.key),
      'value-id': type['value-id'],
      value: deepCopyType(type.value),
      'value-required': type['value-required'],
    } as IcebergMapType;
  }

  // Fallback - return as is (should not happen with valid schemas)
  return type;
}

/**
 * Create default table metadata.
 *
 * IMPORTANT: This function preserves the exact field IDs from the input schema.
 * Field IDs are never reassigned - they are used exactly as provided in the request.
 */
function createDefaultMetadata(
  tableUuid: string,
  location: string,
  schema: IcebergSchema,
  partitionSpec?: PartitionSpec,
  sortOrder?: SortOrder,
  properties?: Record<string, string>
): TableMetadata {
  const now = Date.now();

  // Deep copy the schema to ensure field IDs are preserved exactly as provided.
  // This prevents any potential mutation of the original schema from affecting
  // the stored metadata, and ensures field IDs are never reassigned.
  const normalizedSchema: IcebergSchema = {
    type: 'struct',
    'schema-id': schema['schema-id'] ?? 0,
    fields: schema.fields.map(deepCopyField),
  };

  // Copy identifier-field-ids if present
  if (schema['identifier-field-ids']) {
    normalizedSchema['identifier-field-ids'] = [...schema['identifier-field-ids']];
  }

  // Find max field ID in schema (used for last-column-id tracking)
  const maxFieldId = findMaxFieldId(normalizedSchema);

  // Default partition spec (unpartitioned)
  const defaultPartitionSpec: PartitionSpec = partitionSpec ?? {
    'spec-id': 0,
    fields: [],
  };

  // Default sort order (unsorted)
  const defaultSortOrder: SortOrder = sortOrder ?? {
    'order-id': 0,
    fields: [],
  };

  return {
    'format-version': 2,
    'table-uuid': tableUuid,
    location,
    'last-sequence-number': 0,
    'last-updated-ms': now,
    'last-column-id': maxFieldId,
    'current-schema-id': normalizedSchema['schema-id']!,
    schemas: [normalizedSchema],
    'default-spec-id': defaultPartitionSpec['spec-id'],
    'partition-specs': [defaultPartitionSpec],
    'last-partition-id': findMaxPartitionFieldId(defaultPartitionSpec),
    'default-sort-order-id': defaultSortOrder['order-id'],
    'sort-orders': [defaultSortOrder],
    properties: properties ?? {},
    snapshots: [],
    'snapshot-log': [],
    'metadata-log': [],
    refs: {},
  };
}

/**
 * Find the maximum field ID in a schema.
 */
function findMaxFieldId(schema: IcebergSchema): number {
  let maxId = 0;

  function traverse(type: string | IcebergSchema | IcebergListType | IcebergMapType): void {
    if (typeof type === 'string') return;

    if ('fields' in type && Array.isArray(type.fields)) {
      for (const field of type.fields) {
        maxId = Math.max(maxId, field.id);
        traverse(field.type);
      }
    }

    if ('element-id' in type) {
      maxId = Math.max(maxId, type['element-id']);
      traverse(type.element);
    }

    if ('key-id' in type) {
      maxId = Math.max(maxId, type['key-id'], type['value-id']);
      traverse(type.key);
      traverse(type.value);
    }
  }

  traverse(schema);
  return maxId;
}

/**
 * Find the maximum partition field ID in a spec.
 */
function findMaxPartitionFieldId(spec: PartitionSpec): number {
  if (spec.fields.length === 0) return 999; // Iceberg default for empty spec
  return Math.max(...spec.fields.map(f => f['field-id']));
}

/**
 * Apply table updates to metadata.
 */
function applyUpdates(
  metadata: TableMetadata,
  updates: TableUpdate[]
): TableMetadata {
  let result = { ...metadata };

  for (const update of updates) {
    switch (update.action) {
      case 'assign-uuid':
        result['table-uuid'] = update.uuid;
        break;

      case 'upgrade-format-version':
        result['format-version'] = update['format-version'];
        break;

      case 'add-schema': {
        // Ensure schema-id is a valid non-negative integer
        const providedSchemaId = update.schema['schema-id'];
        const schemaId = (typeof providedSchemaId === 'number' && providedSchemaId >= 0)
          ? providedSchemaId
          : result.schemas.length;
        const newSchema = {
          ...update.schema,
          'schema-id': schemaId,
        };
        result.schemas = [...result.schemas, newSchema];
        if (update['last-column-id'] !== undefined) {
          result['last-column-id'] = update['last-column-id'];
        }
        break;
      }

      case 'set-current-schema': {
        const schemaId = update['schema-id'];
        // Validate schema-id is non-negative and exists in schemas
        if (typeof schemaId !== 'number' || schemaId < 0) {
          throw new Error(`Invalid schema-id: ${schemaId}. Must be a non-negative integer.`);
        }
        const schemaExists = result.schemas.some(s => s['schema-id'] === schemaId);
        if (!schemaExists) {
          throw new Error(`Cannot find schema with current-schema-id=${schemaId} from schemas`);
        }
        result['current-schema-id'] = schemaId;
        break;
      }

      case 'add-spec':
        result['partition-specs'] = [...result['partition-specs'], update.spec];
        break;

      case 'set-default-spec':
        result['default-spec-id'] = update['spec-id'];
        break;

      case 'add-sort-order':
        result['sort-orders'] = [...result['sort-orders'], update['sort-order']];
        break;

      case 'set-default-sort-order':
        result['default-sort-order-id'] = update['sort-order-id'];
        break;

      case 'add-snapshot': {
        const snapshot = update.snapshot;
        result.snapshots = [...(result.snapshots ?? []), snapshot];
        result['snapshot-log'] = [
          ...(result['snapshot-log'] ?? []),
          { 'timestamp-ms': snapshot['timestamp-ms'], 'snapshot-id': snapshot['snapshot-id'] },
        ];
        result['current-snapshot-id'] = snapshot['snapshot-id'];
        result['last-sequence-number'] = snapshot['sequence-number'];
        break;
      }

      case 'remove-snapshots':
        result.snapshots = (result.snapshots ?? []).filter(
          s => !update['snapshot-ids'].includes(s['snapshot-id'])
        );
        break;

      case 'set-snapshot-ref':
        result.refs = {
          ...(result.refs ?? {}),
          [update['ref-name']]: {
            'snapshot-id': update['snapshot-id'],
            type: update.type,
          },
        };
        if (update['ref-name'] === 'main') {
          result['current-snapshot-id'] = update['snapshot-id'];
        }
        break;

      case 'remove-snapshot-ref': {
        const { [update['ref-name']]: _, ...remainingRefs } = result.refs ?? {};
        result.refs = remainingRefs;
        break;
      }

      case 'set-properties':
        result.properties = { ...(result.properties ?? {}), ...update.updates };
        break;

      case 'remove-properties':
        result.properties = Object.fromEntries(
          Object.entries(result.properties ?? {}).filter(
            ([key]) => !update.removals.includes(key)
          )
        );
        break;

      case 'set-location':
        result.location = update.location;
        break;
    }
  }

  result['last-updated-ms'] = Date.now();
  return result;
}

/**
 * Validate table requirements against current metadata.
 */
function validateRequirements(
  metadata: TableMetadata | null,
  requirements: TableRequirement[]
): { valid: boolean; message?: string } {
  for (const req of requirements) {
    switch (req.type) {
      case 'assert-create':
        if (metadata !== null) {
          return { valid: false, message: 'Cannot commit: table already exists' };
        }
        break;

      case 'assert-table-uuid':
        if (!metadata || metadata['table-uuid'] !== req.uuid) {
          return { valid: false, message: `Cannot commit: table UUID mismatch expected ${req.uuid}, got ${metadata?.['table-uuid'] ?? 'null'}` };
        }
        break;

      case 'assert-ref-snapshot-id': {
        if (!metadata) {
          return { valid: false, message: 'Cannot commit: table does not exist' };
        }
        const refSnapshotId = metadata.refs?.[req.ref]?.['snapshot-id'] ?? null;
        if (refSnapshotId !== req['snapshot-id']) {
          return {
            valid: false,
            message: `Cannot commit: snapshot ID mismatch for ref ${req.ref} expected ${req['snapshot-id']}, got ${refSnapshotId}`,
          };
        }
        break;
      }

      case 'assert-last-assigned-field-id':
        if (!metadata || metadata['last-column-id'] !== req['last-assigned-field-id']) {
          return {
            valid: false,
            message: `Cannot commit: last assigned field ID mismatch expected ${req['last-assigned-field-id']}, got ${metadata?.['last-column-id'] ?? 'null'}`,
          };
        }
        break;

      case 'assert-current-schema-id':
        if (!metadata || metadata['current-schema-id'] !== req['current-schema-id']) {
          return {
            valid: false,
            message: `Cannot commit: current schema ID mismatch expected ${req['current-schema-id']}, got ${metadata?.['current-schema-id'] ?? 'null'}`,
          };
        }
        break;

      case 'assert-last-assigned-partition-id':
        if (!metadata || metadata['last-partition-id'] !== req['last-assigned-partition-id']) {
          return {
            valid: false,
            message: `Cannot commit: last assigned partition ID mismatch expected ${req['last-assigned-partition-id']}, got ${metadata?.['last-partition-id'] ?? 'null'}`,
          };
        }
        break;

      case 'assert-default-spec-id':
        if (!metadata || metadata['default-spec-id'] !== req['default-spec-id']) {
          return {
            valid: false,
            message: `Cannot commit: default spec ID mismatch expected ${req['default-spec-id']}, got ${metadata?.['default-spec-id'] ?? 'null'}`,
          };
        }
        break;

      case 'assert-default-sort-order-id':
        if (!metadata || metadata['default-sort-order-id'] !== req['default-sort-order-id']) {
          return {
            valid: false,
            message: `Cannot commit: default sort order ID mismatch expected ${req['default-sort-order-id']}, got ${metadata?.['default-sort-order-id'] ?? 'null'}`,
          };
        }
        break;
    }
  }

  return { valid: true };
}

// ============================================================================
// Route Handler
// ============================================================================

/**
 * Create Iceberg REST Catalog routes.
 *
 * Endpoints:
 * - GET /config - Get catalog configuration
 * - GET /namespaces - List namespaces
 * - POST /namespaces - Create namespace
 * - GET /namespaces/{namespace} - Get namespace metadata
 * - DELETE /namespaces/{namespace} - Drop namespace
 * - POST /namespaces/{namespace}/properties - Update namespace properties
 * - GET /namespaces/{namespace}/tables - List tables
 * - POST /namespaces/{namespace}/tables - Create table
 * - POST /namespaces/{namespace}/register - Register existing table
 * - GET /namespaces/{namespace}/tables/{table} - Load table
 * - DELETE /namespaces/{namespace}/tables/{table} - Drop table
 * - POST /namespaces/{namespace}/tables/{table} - Commit table changes
 * - POST /tables/rename - Rename table
 */
export function createIcebergRoutes(): Hono<{ Bindings: Env; Variables: ContextVariables }> {
  const api = new Hono<{ Bindings: Env; Variables: ContextVariables }>();

  // -------------------------------------------------------------------------
  // GET /config - Catalog configuration
  // -------------------------------------------------------------------------
  api.get('/config', (c) => {
    return c.json({
      defaults: {
        // Default properties for new tables
        'write.parquet.compression-codec': 'zstd',
      },
      overrides: {
        // Properties that cannot be overridden by clients
      },
    });
  });

  // -------------------------------------------------------------------------
  // GET /namespaces - List all namespaces
  // -------------------------------------------------------------------------
  api.get('/namespaces', requireNamespacePermission('namespace:list'), async (c) => {
    try {
      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(new Request('http://internal/namespaces'));
      const data = await response.json() as { namespaces: string[][] };

      // Support parent query parameter for hierarchical namespaces
      const parentParam = c.req.query('parent');
      let namespaces = data.namespaces;

      if (parentParam) {
        const parent = parseNamespace(parentParam);
        namespaces = namespaces.filter(ns => {
          // Return namespaces that are direct children of parent
          if (ns.length !== parent.length + 1) return false;
          for (let i = 0; i < parent.length; i++) {
            if (ns[i] !== parent[i]) return false;
          }
          return true;
        });
      }

      return c.json({ namespaces });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to list namespaces',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces - Create a namespace
  // -------------------------------------------------------------------------
  api.post('/namespaces', requireNamespacePermission('namespace:create'), async (c) => {
    try {
      const body = await c.req.json() as CreateNamespaceRequest;

      if (!body.namespace || !Array.isArray(body.namespace) || body.namespace.length === 0) {
        return icebergError(c, 'Namespace is required and must be a non-empty array', 'BadRequest', 400);
      }

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request('http://internal/namespaces', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            namespace: body.namespace,
            properties: body.properties ?? {},
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint')) {
          return icebergError(c, `Namespace already exists: ${body.namespace.join('.')}`, 'AlreadyExists', 409);
        }
        throw new Error(error.error || 'Failed to create namespace');
      }

      return c.json({
        namespace: body.namespace,
        properties: body.properties ?? {},
      }, 200);
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to create namespace',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // GET/HEAD /namespaces/{namespace} - Get namespace metadata / Check exists
  // -------------------------------------------------------------------------
  api.on(['GET', 'HEAD'], '/namespaces/:namespace', requireNamespacePermission('namespace:read'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`)
      );

      if (!response.ok) {
        // HEAD returns 404, GET returns error JSON
        if (c.req.method === 'HEAD') {
          return c.body(null, 404);
        }
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
      }

      // HEAD returns 204 No Content, GET returns full response
      if (c.req.method === 'HEAD') {
        return c.body(null, 204);
      }

      const data = await response.json() as { properties: Record<string, string> };

      return c.json({
        namespace,
        properties: data.properties ?? {},
      });
    } catch (error) {
      if (c.req.method === 'HEAD') {
        return c.body(null, 500);
      }
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to get namespace',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // DELETE /namespaces/{namespace} - Drop namespace
  // -------------------------------------------------------------------------
  api.delete('/namespaces/:namespace', requireNamespacePermission('namespace:drop'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`, {
          method: 'DELETE',
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (error.error?.includes('not empty')) {
          return icebergError(c, `Namespace is not empty: ${namespace.join('.')}`, 'NamespaceNotEmpty', 409);
        }
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to drop namespace',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/properties - Update namespace properties
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/properties', requireNamespacePermission('namespace:update'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as UpdateNamespacePropertiesRequest;

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/properties`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            updates: body.updates ?? {},
            removals: body.removals ?? [],
          }),
        })
      );

      if (!response.ok) {
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
      }

      const data = await response.json() as { updated: string[]; removed: string[]; missing: string[] };

      return c.json({
        updated: data.updated ?? Object.keys(body.updates ?? {}),
        removed: data.removed ?? body.removals ?? [],
        missing: data.missing ?? [],
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to update namespace properties',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // GET /namespaces/{namespace}/tables - List tables in namespace
  // -------------------------------------------------------------------------
  api.get('/namespaces/:namespace/tables', requireTablePermission('table:list'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables`)
      );

      if (!response.ok) {
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
      }

      const data = await response.json() as { tables: Array<{ namespace: string[]; name: string }> };

      return c.json({
        identifiers: (data.tables ?? []).map(t => ({
          namespace: t.namespace,
          name: t.name,
        })),
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to list tables',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/tables - Create table
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/tables', requireTablePermission('table:create'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as CreateTableRequest;

      if (!body.name) {
        return icebergError(c, 'Table name is required', 'BadRequest', 400);
      }

      if (!body.schema) {
        return icebergError(c, 'Schema is required', 'BadRequest', 400);
      }

      // Generate table UUID
      const tableUuid = crypto.randomUUID();

      // Determine table location
      const warehousePrefix = c.env.R2_BUCKET ? 's3://iceberg-tables' : 'file:///warehouse';
      const tableLocation = body.location ?? `${warehousePrefix}/${namespace.join('/')}/${body.name}`;

      // Create initial metadata
      const metadata = createDefaultMetadata(
        tableUuid,
        tableLocation,
        body.schema,
        body['partition-spec'],
        body['write-order'],
        body.properties
      );

      // Determine metadata location
      const metadataLocation = `${tableLocation}/metadata/00000-${tableUuid}.metadata.json`;

      // Store table in catalog
      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: body.name,
            location: tableLocation,
            metadataLocation,
            metadata,
            properties: body.properties ?? {},
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint')) {
          return icebergError(c, `Table already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExists', 409);
        }
        if (response.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
        }
        throw new Error(error.error || 'Failed to create table');
      }

      // If stage-create is true, don't write metadata to storage yet
      if (body['stage-create']) {
        return c.json({
          'metadata-location': metadataLocation,
          metadata,
        });
      }

      // Write metadata to R2 if available
      if (c.env.R2_BUCKET) {
        const metadataPath = metadataLocation.replace('s3://iceberg-tables/', '');
        await c.env.R2_BUCKET.put(metadataPath, JSON.stringify(metadata, null, 2), {
          httpMetadata: { contentType: 'application/json' },
        });
      }

      return c.json({
        'metadata-location': metadataLocation,
        metadata,
      });
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to create table',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // GET/HEAD /namespaces/{namespace}/tables/{table} - Load table / Check exists
  // -------------------------------------------------------------------------
  api.on(['GET', 'HEAD'], '/namespaces/:namespace/tables/:table', requireTablePermission('table:read'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const tableName = c.req.param('table');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}`)
      );

      if (!response.ok) {
        // HEAD returns 404, GET returns error JSON
        if (c.req.method === 'HEAD') {
          return c.body(null, 404);
        }
        return icebergError(
          c,
          `Table does not exist: ${namespace.join('.')}.${tableName}`,
          'NoSuchTable',
          404
        );
      }

      // HEAD returns 204 No Content
      if (c.req.method === 'HEAD') {
        return c.body(null, 204);
      }

      const data = await response.json() as {
        location: string;
        metadataLocation: string;
        metadata?: TableMetadata;
      };

      // If we have metadata stored, return it
      if (data.metadata) {
        return c.json({
          'metadata-location': data.metadataLocation,
          metadata: data.metadata,
        });
      }

      // Otherwise try to read from R2
      if (c.env.R2_BUCKET) {
        const metadataPath = data.metadataLocation.replace('s3://iceberg-tables/', '');
        const object = await c.env.R2_BUCKET.get(metadataPath);

        if (object) {
          const metadata = await object.json() as TableMetadata;
          return c.json({
            'metadata-location': data.metadataLocation,
            metadata,
          });
        }
      }

      // Return minimal metadata if we can't find the full metadata
      return c.json({
        'metadata-location': data.metadataLocation,
        metadata: {
          'format-version': 2,
          'table-uuid': crypto.randomUUID(),
          location: data.location,
          'last-updated-ms': Date.now(),
          'last-column-id': 0,
          'current-schema-id': 0,
          schemas: [],
          'default-spec-id': 0,
          'partition-specs': [],
          'last-partition-id': 999,
          'default-sort-order-id': 0,
          'sort-orders': [],
        },
      });
    } catch (error) {
      if (c.req.method === 'HEAD') {
        return c.body(null, 500);
      }
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to load table',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // DELETE /namespaces/{namespace}/tables/{table} - Drop table
  // -------------------------------------------------------------------------
  api.delete('/namespaces/:namespace/tables/:table', requireTablePermission('table:drop'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const tableName = c.req.param('table');
      const namespace = parseNamespace(namespaceParam);

      // Check purge query parameter
      const purge = c.req.query('purgeRequested') === 'true';

      const catalog = getCatalogStub(c);

      // Get table info before deleting (for purge)
      let tableInfo: { location: string } | null = null;
      if (purge && c.env.R2_BUCKET) {
        const response = await catalog.fetch(
          new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}`)
        );
        if (response.ok) {
          tableInfo = await response.json() as { location: string };
        }
      }

      // Delete from catalog
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}`, {
          method: 'DELETE',
        })
      );

      if (!response.ok) {
        return icebergError(
          c,
          `Table does not exist: ${namespace.join('.')}.${tableName}`,
          'NoSuchTable',
          404
        );
      }

      // If purge requested and we have R2, delete the data
      if (purge && tableInfo && c.env.R2_BUCKET) {
        const prefix = tableInfo.location.replace('s3://iceberg-tables/', '');
        // List and delete all objects with this prefix
        const listed = await c.env.R2_BUCKET.list({ prefix });
        if (listed.objects.length > 0) {
          await c.env.R2_BUCKET.delete(listed.objects.map(o => o.key));
        }
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to drop table',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/tables/{table} - Commit table changes
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/tables/:table', requireTablePermission('table:commit'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const tableName = c.req.param('table');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as CommitTableRequest;

      const catalog = getCatalogStub(c);

      // Load current table metadata
      const loadResponse = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}`)
      );

      if (!loadResponse.ok) {
        return icebergError(
          c,
          `Table does not exist: ${namespace.join('.')}.${tableName}`,
          'NoSuchTable',
          404
        );
      }

      const tableData = await loadResponse.json() as {
        location: string;
        metadataLocation: string;
        metadata?: TableMetadata;
      };

      let currentMetadata = tableData.metadata;

      // Try to load from R2 if not in catalog
      if (!currentMetadata && c.env.R2_BUCKET) {
        const metadataPath = tableData.metadataLocation.replace('s3://iceberg-tables/', '');
        const object = await c.env.R2_BUCKET.get(metadataPath);
        if (object) {
          currentMetadata = await object.json() as TableMetadata;
        }
      }

      // Validate requirements
      const validation = validateRequirements(currentMetadata ?? null, body.requirements ?? []);
      if (!validation.valid) {
        return icebergError(c, validation.message!, 'CommitFailed', 409);
      }

      // Apply updates
      const newMetadata = applyUpdates(currentMetadata!, body.updates ?? []);

      // Generate new metadata location
      const seqNum = (newMetadata['last-sequence-number'] ?? 0).toString().padStart(5, '0');
      const newMetadataLocation = `${newMetadata.location}/metadata/${seqNum}-${newMetadata['table-uuid']}.metadata.json`;

      // Write new metadata to R2
      if (c.env.R2_BUCKET) {
        const metadataPath = newMetadataLocation.replace('s3://iceberg-tables/', '');
        await c.env.R2_BUCKET.put(metadataPath, JSON.stringify(newMetadata, null, 2), {
          httpMetadata: { contentType: 'application/json' },
        });
      }

      // Update catalog with new metadata location
      await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}/commit`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            metadataLocation: newMetadataLocation,
            metadata: newMetadata,
          }),
        })
      );

      return c.json({
        'metadata-location': newMetadataLocation,
        metadata: newMetadata,
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to commit table changes',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /tables/rename - Rename table
  // -------------------------------------------------------------------------
  api.post('/tables/rename', requireTablePermission('table:rename'), async (c) => {
    try {
      const body = await c.req.json() as RenameTableRequest;

      if (!body.source || !body.destination) {
        return icebergError(c, 'Source and destination are required', 'BadRequest', 400);
      }

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request('http://internal/tables/rename', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            fromNamespace: body.source.namespace,
            fromName: body.source.name,
            toNamespace: body.destination.namespace,
            toName: body.destination.name,
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (error.error?.includes('not found') || error.error?.includes('does not exist')) {
          return icebergError(
            c,
            `Table does not exist: ${body.source.namespace.join('.')}.${body.source.name}`,
            'NoSuchTable',
            404
          );
        }
        if (error.error?.includes('already exists')) {
          return icebergError(
            c,
            `Table already exists: ${body.destination.namespace.join('.')}.${body.destination.name}`,
            'AlreadyExists',
            409
          );
        }
        throw new Error(error.error || 'Failed to rename table');
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to rename table',
        'InternalServerError',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/register - Register existing table
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/register', requireTablePermission('table:create'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as RegisterTableRequest;

      if (!body.name) {
        return icebergError(c, 'Table name is required', 'BadRequest', 400);
      }

      if (!body['metadata-location']) {
        return icebergError(c, 'Metadata location is required', 'BadRequest', 400);
      }

      const metadataLocation = body['metadata-location'];

      // Load metadata from the provided location (R2)
      let metadata: TableMetadata | null = null;
      if (c.env.R2_BUCKET) {
        const metadataPath = metadataLocation.replace('s3://iceberg-tables/', '');
        const object = await c.env.R2_BUCKET.get(metadataPath);

        if (object) {
          metadata = await object.json() as TableMetadata;
        }
      }

      if (!metadata) {
        return icebergError(
          c,
          `Unable to load metadata from location: ${metadataLocation}`,
          'BadRequest',
          400
        );
      }

      // Extract table location from metadata
      const tableLocation = metadata.location;

      // Store table in catalog
      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: body.name,
            location: tableLocation,
            metadataLocation,
            metadata,
            properties: metadata.properties ?? {},
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint')) {
          return icebergError(c, `Table already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExists', 409);
        }
        if (response.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespace', 404);
        }
        throw new Error(error.error || 'Failed to register table');
      }

      // Return LoadTableResponse format
      return c.json({
        'metadata-location': metadataLocation,
        metadata,
      });
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to register table',
        'InternalServerError',
        500
      );
    }
  });

  return api;
}

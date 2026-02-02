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

/** Request body for rename view */
interface RenameViewRequest {
  source: { namespace: string[]; name: string };
  destination: { namespace: string[]; name: string };
}

/** View representation (SQL query with dialect) */
interface ViewRepresentation {
  type: 'sql';
  sql: string;
  dialect: string;
}

/** View version */
interface ViewVersion {
  'version-id': number;
  'schema-id': number;
  'timestamp-ms': number;
  summary: Record<string, string>;
  representations: ViewRepresentation[];
  'default-catalog'?: string;
  'default-namespace'?: string[];
}

/** Request body for creating a view */
interface CreateViewRequest {
  name: string;
  location?: string;
  schema: IcebergSchema;
  'view-version': ViewVersion;
  properties?: Record<string, string>;
}

/** View metadata per Iceberg View spec */
interface ViewMetadata {
  'view-uuid': string;
  'format-version': 1;
  location: string;
  'current-version-id': number;
  versions: ViewVersion[];
  'version-log': Array<{ 'timestamp-ms': number; 'version-id': number }>;
  schemas: IcebergSchema[];
  properties?: Record<string, string>;
}

/** Request body for replacing a view */
interface ReplaceViewRequest {
  identifier?: { namespace: string[]; name: string };
  requirements?: ViewRequirement[];
  updates?: ViewUpdate[];
}

/** View requirement for optimistic locking */
type ViewRequirement =
  | { type: 'assert-view-uuid'; uuid: string };

/** View update operations */
type ViewUpdate =
  | { action: 'assign-uuid'; uuid: string }
  | { action: 'set-location'; location: string }
  | { action: 'add-schema'; schema: IcebergSchema }
  | { action: 'add-view-version'; 'view-version': ViewVersion }
  | { action: 'set-current-view-version'; 'view-version-id': number }
  | { action: 'set-properties'; updates: Record<string, string> }
  | { action: 'remove-properties'; removals: string[] };

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
 *
 * Per the Iceberg REST spec, namespace components are separated by the unit
 * separator character (0x1F) when encoded in a URL path. A dot in the namespace
 * is NOT a hierarchy separator - it's a literal character in the namespace name.
 *
 * Examples:
 * - "db" -> ["db"] (single component)
 * - "new.db" -> ["new.db"] (single component with literal dot)
 * - "db%1Fschema" -> ["db", "schema"] (two components using unit separator)
 */
function parseNamespace(namespaceParam: string): string[] {
  // Decode URL encoding first
  const decoded = decodeURIComponent(namespaceParam);
  // Only split on unit separator (0x1F) for multi-level namespaces
  // Do NOT split on dots - they are valid characters in namespace names
  if (decoded.includes('\x1f')) {
    return decoded.split('\x1f');
  }
  // Single namespace component (may contain dots)
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
 * Check if a field ID is valid (positive integer).
 * Per Iceberg spec, field IDs must be positive integers.
 */
function isValidFieldId(id: unknown): id is number {
  return typeof id === 'number' && id > 0 && Number.isInteger(id);
}

/**
 * Find the maximum valid field ID in a schema (first pass).
 * Returns 0 if no valid field IDs are found.
 */
function findMaxValidFieldId(schema: IcebergSchema): number {
  let maxId = 0;

  function traverse(type: string | IcebergSchema | IcebergListType | IcebergMapType): void {
    if (typeof type === 'string') return;

    if ('fields' in type && Array.isArray(type.fields)) {
      for (const field of type.fields) {
        if (isValidFieldId(field.id)) {
          maxId = Math.max(maxId, field.id);
        }
        traverse(field.type);
      }
    }

    if ('element-id' in type) {
      if (isValidFieldId(type['element-id'])) {
        maxId = Math.max(maxId, type['element-id']);
      }
      traverse(type.element);
    }

    if ('key-id' in type) {
      if (isValidFieldId(type['key-id'])) {
        maxId = Math.max(maxId, type['key-id']);
      }
      if (isValidFieldId(type['value-id'])) {
        maxId = Math.max(maxId, type['value-id']);
      }
      traverse(type.key);
      traverse(type.value);
    }
  }

  traverse(schema);
  return maxId;
}

/**
 * Context for assigning field IDs during schema normalization.
 */
interface FieldIdContext {
  nextId: number;
}

/**
 * Deep copy and normalize a schema field, assigning IDs if missing.
 * If field.id is missing, 0, or negative, assigns a new sequential ID.
 * If field.id is valid (positive), preserves it exactly.
 */
function normalizeField(field: IcebergField, ctx: FieldIdContext): IcebergField {
  // Assign ID if missing or invalid, otherwise preserve
  const id = isValidFieldId(field.id) ? field.id : ctx.nextId++;

  const copy: IcebergField = {
    id,
    name: field.name,
    required: field.required,
    type: normalizeType(field.type, ctx),
  };
  if (field.doc !== undefined) {
    copy.doc = field.doc;
  }
  return copy;
}

/**
 * Deep copy and normalize a type, assigning IDs to nested elements if missing.
 */
function normalizeType(type: string | IcebergSchema | IcebergListType | IcebergMapType, ctx: FieldIdContext): string | IcebergSchema | IcebergListType | IcebergMapType {
  if (typeof type === 'string') {
    return type;
  }

  if ('fields' in type && type.type === 'struct') {
    // It's a struct type
    const result: IcebergSchema = {
      type: 'struct',
      fields: type.fields.map(f => normalizeField(f, ctx)),
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
    // It's a list type - assign element-id if missing
    const elementId = isValidFieldId(type['element-id']) ? type['element-id'] : ctx.nextId++;
    return {
      type: 'list',
      'element-id': elementId,
      element: normalizeType(type.element, ctx),
      'element-required': type['element-required'],
    } as IcebergListType;
  }

  if ('key-id' in type) {
    // It's a map type - assign key-id and value-id if missing
    const keyId = isValidFieldId(type['key-id']) ? type['key-id'] : ctx.nextId++;
    const valueId = isValidFieldId(type['value-id']) ? type['value-id'] : ctx.nextId++;
    return {
      type: 'map',
      'key-id': keyId,
      key: normalizeType(type.key, ctx),
      'value-id': valueId,
      value: normalizeType(type.value, ctx),
      'value-required': type['value-required'],
    } as IcebergMapType;
  }

  // Fallback - return as is (should not happen with valid schemas)
  return type;
}

/**
 * Normalize a schema by REASSIGNING all field IDs sequentially starting from 1.
 * Per Iceberg spec, when a table is created, all IDs in the schema are re-assigned
 * to ensure uniqueness and sequential assignment.
 *
 * This means:
 * - Input schema with IDs 3, 4 → output schema with IDs 1, 2
 * - Input schema with IDs 100, 200 → output schema with IDs 1, 2
 * - Input schema with no IDs → output schema with IDs 1, 2
 *
 * Returns both the normalized schema and a mapping from old field IDs to new field IDs,
 * which is used to update partition specs and sort orders.
 */
function normalizeSchemaWithMapping(schema: IcebergSchema): { schema: IcebergSchema; idMapping: Map<number, number> } {
  // Always start field ID assignment from 1 for new tables
  const ctx: FieldIdContext = { nextId: 1 };
  const idMapping = new Map<number, number>();

  const result: IcebergSchema = {
    type: 'struct',
    fields: schema.fields.map(f => reassignFieldWithMapping(f, ctx, idMapping)),
  };

  if (schema['schema-id'] !== undefined) {
    result['schema-id'] = schema['schema-id'];
  }
  if (schema['identifier-field-ids'] !== undefined) {
    // Map old identifier-field-ids to new IDs
    result['identifier-field-ids'] = schema['identifier-field-ids']
      .map(id => idMapping.get(id) ?? id);
  }

  return { schema: result, idMapping };
}

/**
 * Legacy function that returns only the normalized schema (for backward compatibility).
 */
function normalizeSchema(schema: IcebergSchema): IcebergSchema {
  return normalizeSchemaWithMapping(schema).schema;
}

/**
 * Reassign a field with a fresh ID, tracking the mapping from old ID to new ID.
 */
function reassignFieldWithMapping(field: IcebergField, ctx: FieldIdContext, idMapping: Map<number, number>): IcebergField {
  const newId = ctx.nextId++;
  idMapping.set(field.id, newId);

  const copy: IcebergField = {
    id: newId,
    name: field.name,
    required: field.required,
    type: reassignTypeWithMapping(field.type, ctx, idMapping),
  };
  if (field.doc !== undefined) {
    copy.doc = field.doc;
  }
  return copy;
}

/**
 * Reassign IDs in a nested type, tracking the mapping.
 */
function reassignTypeWithMapping(type: string | IcebergSchema | IcebergListType | IcebergMapType, ctx: FieldIdContext, idMapping: Map<number, number>): string | IcebergSchema | IcebergListType | IcebergMapType {
  if (typeof type === 'string') {
    return type;
  }

  if ('fields' in type && type.type === 'struct') {
    // It's a struct type
    const result: IcebergSchema = {
      type: 'struct',
      fields: type.fields.map(f => reassignFieldWithMapping(f, ctx, idMapping)),
    };
    if (type['schema-id'] !== undefined) {
      result['schema-id'] = type['schema-id'];
    }
    if (type['identifier-field-ids'] !== undefined) {
      result['identifier-field-ids'] = type['identifier-field-ids']
        .map(id => idMapping.get(id) ?? id);
    }
    return result;
  }

  if ('element-id' in type) {
    // It's a list type
    const newElementId = ctx.nextId++;
    idMapping.set(type['element-id'], newElementId);
    return {
      type: 'list',
      'element-id': newElementId,
      element: reassignTypeWithMapping(type.element, ctx, idMapping),
      'element-required': type['element-required'],
    } as IcebergListType;
  }

  if ('key-id' in type) {
    // It's a map type
    const newKeyId = ctx.nextId++;
    const newValueId = ctx.nextId++;
    idMapping.set(type['key-id'], newKeyId);
    idMapping.set(type['value-id'], newValueId);
    return {
      type: 'map',
      'key-id': newKeyId,
      key: reassignTypeWithMapping(type.key, ctx, idMapping),
      'value-id': newValueId,
      value: reassignTypeWithMapping(type.value, ctx, idMapping),
      'value-required': type['value-required'],
    } as IcebergMapType;
  }

  return type;
}

/**
 * Reassign a field with a fresh ID (always assigns a new ID, ignoring any existing ID).
 */
function reassignField(field: IcebergField, ctx: FieldIdContext): IcebergField {
  const id = ctx.nextId++;

  const copy: IcebergField = {
    id,
    name: field.name,
    required: field.required,
    type: reassignType(field.type, ctx),
  };
  if (field.doc !== undefined) {
    copy.doc = field.doc;
  }
  return copy;
}

/**
 * Reassign IDs in a nested type.
 */
function reassignType(type: string | IcebergSchema | IcebergListType | IcebergMapType, ctx: FieldIdContext): string | IcebergSchema | IcebergListType | IcebergMapType {
  if (typeof type === 'string') {
    return type;
  }

  if ('fields' in type && type.type === 'struct') {
    // It's a struct type
    const result: IcebergSchema = {
      type: 'struct',
      fields: type.fields.map(f => reassignField(f, ctx)),
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
    const elementId = ctx.nextId++;
    return {
      type: 'list',
      'element-id': elementId,
      element: reassignType(type.element, ctx),
      'element-required': type['element-required'],
    } as IcebergListType;
  }

  if ('key-id' in type) {
    // It's a map type
    const keyId = ctx.nextId++;
    const valueId = ctx.nextId++;
    return {
      type: 'map',
      'key-id': keyId,
      key: reassignType(type.key, ctx),
      'value-id': valueId,
      value: reassignType(type.value, ctx),
      'value-required': type['value-required'],
    } as IcebergMapType;
  }

  return type;
}

/**
 * Deep copy a schema field preserving field IDs exactly (legacy function).
 */
function deepCopyField(field: IcebergField): IcebergField {
  const ctx: FieldIdContext = { nextId: 1 };
  return normalizeField(field, ctx);
}

/**
 * Deep copy a type preserving IDs (legacy function).
 */
function deepCopyType(type: string | IcebergSchema | IcebergListType | IcebergMapType): string | IcebergSchema | IcebergListType | IcebergMapType {
  const ctx: FieldIdContext = { nextId: 1 };
  return normalizeType(type, ctx);
}

/**
 * Create default table metadata.
 *
 * This function normalizes the schema by reassigning all field IDs sequentially
 * starting from 1. The partition spec and sort order source-ids are updated
 * to reference the new field IDs using the mapping from the normalization.
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

  // Normalize the schema and get the ID mapping
  const { schema: normalizedSchema, idMapping } = normalizeSchemaWithMapping(schema);

  // Ensure schema-id is set (default to 0)
  if (normalizedSchema['schema-id'] === undefined) {
    normalizedSchema['schema-id'] = schema['schema-id'] ?? 0;
  }

  // Find max field ID in schema (used for last-column-id tracking)
  const maxFieldId = findMaxFieldId(normalizedSchema);

  // Remap partition spec source-ids using the ID mapping
  let remappedPartitionSpec: PartitionSpec;
  if (partitionSpec && partitionSpec.fields.length > 0) {
    const remappedFields = partitionSpec.fields.map(pf => {
      const newSourceId = idMapping.get(pf['source-id']);
      if (newSourceId === undefined) {
        // Source ID not found in mapping - this partition field references an invalid column
        console.warn(`Partition field references unknown source-id: ${pf['source-id']}`);
        return null;
      }
      return {
        ...pf,
        'source-id': newSourceId,
      };
    }).filter((f): f is PartitionField => f !== null);

    if (remappedFields.length > 0) {
      remappedPartitionSpec = {
        'spec-id': partitionSpec['spec-id'],
        fields: remappedFields,
      };
    } else {
      // All fields were invalid, fall back to unpartitioned
      remappedPartitionSpec = { 'spec-id': 0, fields: [] };
    }
  } else {
    remappedPartitionSpec = { 'spec-id': 0, fields: [] };
  }

  // Remap sort order source-ids using the ID mapping
  let remappedSortOrder: SortOrder;
  if (sortOrder && sortOrder.fields.length > 0) {
    const remappedFields = sortOrder.fields.map(sf => {
      const newSourceId = idMapping.get(sf['source-id']);
      if (newSourceId === undefined) {
        // Source ID not found in mapping - this sort field references an invalid column
        console.warn(`Sort field references unknown source-id: ${sf['source-id']}`);
        return null;
      }
      return {
        ...sf,
        'source-id': newSourceId,
      };
    }).filter((f): f is SortField => f !== null);

    if (remappedFields.length > 0) {
      remappedSortOrder = {
        'order-id': sortOrder['order-id'],
        fields: remappedFields,
      };
    } else {
      // All fields were invalid, fall back to unsorted
      remappedSortOrder = { 'order-id': 0, fields: [] };
    }
  } else {
    remappedSortOrder = { 'order-id': 0, fields: [] };
  }

  return {
    'format-version': 2,
    'table-uuid': tableUuid,
    location,
    'last-sequence-number': 0,
    'last-updated-ms': now,
    'last-column-id': maxFieldId,
    'current-schema-id': normalizedSchema['schema-id']!,
    schemas: [normalizedSchema],
    'default-spec-id': remappedPartitionSpec['spec-id'],
    'partition-specs': [remappedPartitionSpec],
    'last-partition-id': findMaxPartitionFieldId(remappedPartitionSpec),
    'default-sort-order-id': remappedSortOrder['order-id'],
    'sort-orders': [remappedSortOrder],
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
 * Normalize table metadata to ensure required fields have valid values.
 * Per Iceberg spec, sort-orders must always be an array with at least the default unsorted order.
 */
function normalizeTableMetadata(metadata: TableMetadata): TableMetadata {
  // Ensure sort-orders is always an array with at least the default unsorted order
  if (!metadata['sort-orders'] || !Array.isArray(metadata['sort-orders']) || metadata['sort-orders'].length === 0) {
    metadata['sort-orders'] = [{ 'order-id': 0, fields: [] }];
  }

  // Ensure partition-specs is always an array with at least the default unpartitioned spec
  if (!metadata['partition-specs'] || !Array.isArray(metadata['partition-specs']) || metadata['partition-specs'].length === 0) {
    metadata['partition-specs'] = [{ 'spec-id': 0, fields: [] }];
  }

  return metadata;
}

/**
 * Apply table updates to metadata.
 */
function applyUpdates(
  metadata: TableMetadata,
  updates: TableUpdate[]
): TableMetadata {
  let result = { ...metadata };

  // Track the IDs of the most recently added entities in this update sequence.
  // Per Iceberg spec, id=-1 means "use the entity just added in this same request".
  let lastAddedSchemaId: number | null = null;
  let lastAddedSpecId: number | null = null;
  let lastAddedSortOrderId: number | null = null;

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
        // Track this as the most recently added schema for potential -1 reference
        lastAddedSchemaId = schemaId;

        // Update last-column-id:
        // - If explicitly provided in the update, use that value
        // - Otherwise, calculate from the new schema's max field ID
        if (update['last-column-id'] !== undefined) {
          result['last-column-id'] = update['last-column-id'];
        } else {
          // Calculate max field ID from the added schema
          const maxFieldIdInNewSchema = findMaxFieldId(newSchema as IcebergSchema);
          result['last-column-id'] = Math.max(result['last-column-id'], maxFieldIdInNewSchema);
        }
        break;
      }

      case 'set-current-schema': {
        let schemaId = update['schema-id'];
        // Per Iceberg spec, -1 means "use the schema just added in this same request"
        if (schemaId === -1) {
          if (lastAddedSchemaId === null) {
            throw new Error('Cannot set current schema to -1: no schema was added in this update request');
          }
          schemaId = lastAddedSchemaId;
        }
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

      case 'add-spec': {
        const specId = update.spec['spec-id'];
        result['partition-specs'] = [...(result['partition-specs'] ?? []), update.spec];
        // Track this as the most recently added spec for potential -1 reference
        lastAddedSpecId = specId;
        // Update last-partition-id to track the max partition field ID
        const maxPartitionId = findMaxPartitionFieldId(update.spec);
        result['last-partition-id'] = Math.max(result['last-partition-id'] ?? 999, maxPartitionId);
        break;
      }

      case 'set-default-spec': {
        let specId = update['spec-id'];
        // Per Iceberg spec, -1 means "use the spec just added in this same request"
        if (specId === -1) {
          if (lastAddedSpecId === null) {
            throw new Error('Cannot set default spec to -1: no spec was added in this update request');
          }
          specId = lastAddedSpecId;
        }
        result['default-spec-id'] = specId;
        break;
      }

      case 'add-sort-order': {
        const sortOrderId = update['sort-order']['order-id'];
        result['sort-orders'] = [...(result['sort-orders'] ?? []), update['sort-order']];
        // Track this as the most recently added sort order for potential -1 reference
        lastAddedSortOrderId = sortOrderId;
        break;
      }

      case 'set-default-sort-order': {
        let sortOrderId = update['sort-order-id'];
        // Per Iceberg spec, -1 means "use the sort order just added in this same request"
        if (sortOrderId === -1) {
          if (lastAddedSortOrderId === null) {
            throw new Error('Cannot set default sort order to -1: no sort order was added in this update request');
          }
          sortOrderId = lastAddedSortOrderId;
        }
        result['default-sort-order-id'] = sortOrderId;
        break;
      }

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
  return normalizeTableMetadata(result);
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
 * - HEAD /namespaces/{namespace}/tables/{table} - Check table exists
 * - DELETE /namespaces/{namespace}/tables/{table} - Drop table
 * - POST /namespaces/{namespace}/tables/{table} - Commit table changes
 * - POST /tables/rename - Rename table
 *
 * View Endpoints:
 * - GET /namespaces/{namespace}/views - List views
 * - POST /namespaces/{namespace}/views - Create view
 * - GET /namespaces/{namespace}/views/{view} - Load view
 * - HEAD /namespaces/{namespace}/views/{view} - Check view exists
 * - POST /namespaces/{namespace}/views/{view} - Replace view
 * - DELETE /namespaces/{namespace}/views/{view} - Drop view
 * - POST /views/rename - Rename view
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
      // Advertise supported endpoints so clients know what operations are available
      endpoints: [
        'GET /v1/{prefix}/namespaces',
        'POST /v1/{prefix}/namespaces',
        'GET /v1/{prefix}/namespaces/{namespace}',
        'HEAD /v1/{prefix}/namespaces/{namespace}',
        'DELETE /v1/{prefix}/namespaces/{namespace}',
        'POST /v1/{prefix}/namespaces/{namespace}/properties',
        'GET /v1/{prefix}/namespaces/{namespace}/tables',
        'POST /v1/{prefix}/namespaces/{namespace}/tables',
        'GET /v1/{prefix}/namespaces/{namespace}/tables/{table}',
        'HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}',
        'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}',
        'DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}',
        'POST /v1/{prefix}/tables/rename',
        'POST /v1/{prefix}/namespaces/{namespace}/register',
        // View endpoints
        'GET /v1/{prefix}/namespaces/{namespace}/views',
        'POST /v1/{prefix}/namespaces/{namespace}/views',
        'GET /v1/{prefix}/namespaces/{namespace}/views/{view}',
        'HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}',
        'POST /v1/{prefix}/namespaces/{namespace}/views/{view}',
        'DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}',
        'POST /v1/{prefix}/views/rename',
      ],
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
        'ServiceFailureException',
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
        return icebergError(c, 'Namespace is required and must be a non-empty array', 'BadRequestException', 400);
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
          return icebergError(c, `Namespace already exists: ${body.namespace.join('.')}`, 'AlreadyExistsException', 409);
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
        'ServiceFailureException',
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
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
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
        'ServiceFailureException',
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
          return icebergError(c, `Namespace is not empty: ${namespace.join('.')}`, 'NamespaceNotEmptyException', 409);
        }
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to drop namespace',
        'ServiceFailureException',
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
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
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
        'ServiceFailureException',
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
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
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
        'ServiceFailureException',
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
        return icebergError(c, 'Table name is required', 'BadRequestException', 400);
      }

      if (!body.schema) {
        return icebergError(c, 'Schema is required', 'BadRequestException', 400);
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

      // Normalize metadata to ensure required fields have valid values
      const normalizedMetadata = normalizeTableMetadata(metadata);

      const catalog = getCatalogStub(c);

      // If stage-create is true, don't create the table in the catalog yet.
      // The table will be created when the transaction is committed.
      // We still need to verify the namespace exists.
      if (body['stage-create']) {
        // Verify namespace exists
        const nsResponse = await catalog.fetch(
          new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}`)
        );

        if (!nsResponse.ok) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
        }

        // Check if table already exists (should fail if it does)
        const tableResponse = await catalog.fetch(
          new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(body.name)}`)
        );

        if (tableResponse.ok) {
          return icebergError(c, `Table already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExistsException', 409);
        }

        // Return staged metadata without creating the table
        return c.json({
          'metadata-location': metadataLocation,
          metadata: normalizedMetadata,
        });
      }

      // Store table in catalog
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: body.name,
            location: tableLocation,
            metadataLocation,
            metadata: normalizedMetadata,
            properties: body.properties ?? {},
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint')) {
          return icebergError(c, `Table already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExistsException', 409);
        }
        if (response.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
        }
        throw new Error(error.error || 'Failed to create table');
      }

      // Write metadata to R2 if available
      if (c.env.R2_BUCKET) {
        const metadataPath = metadataLocation.replace('s3://iceberg-tables/', '');
        await c.env.R2_BUCKET.put(metadataPath, JSON.stringify(normalizedMetadata, null, 2), {
          httpMetadata: { contentType: 'application/json' },
        });
      }

      return c.json({
        'metadata-location': metadataLocation,
        metadata: normalizedMetadata,
      });
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to create table',
        'ServiceFailureException',
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
          'NoSuchTableException',
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

      // If we have metadata stored, return it (with normalization)
      if (data.metadata) {
        return c.json({
          'metadata-location': data.metadataLocation,
          metadata: normalizeTableMetadata(data.metadata),
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
            metadata: normalizeTableMetadata(metadata),
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
          'partition-specs': [{ 'spec-id': 0, fields: [] }],
          'last-partition-id': 999,
          'default-sort-order-id': 0,
          'sort-orders': [{ 'order-id': 0, fields: [] }],
        },
      });
    } catch (error) {
      if (c.req.method === 'HEAD') {
        return c.body(null, 500);
      }
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to load table',
        'ServiceFailureException',
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
          'NoSuchTableException',
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
        'ServiceFailureException',
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

      // Check if this is a create transaction (assert-create requirement)
      const hasAssertCreate = (body.requirements ?? []).some(r => r.type === 'assert-create');

      // Load current table metadata
      const loadResponse = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables/${encodeURIComponent(tableName)}`)
      );

      let currentMetadata: TableMetadata | null = null;
      let tableData: { location: string; metadataLocation: string; metadata?: TableMetadata } | null = null;

      if (loadResponse.ok) {
        tableData = await loadResponse.json() as {
          location: string;
          metadataLocation: string;
          metadata?: TableMetadata;
        };
        currentMetadata = tableData.metadata ?? null;

        // Try to load from R2 if not in catalog
        if (!currentMetadata && c.env.R2_BUCKET) {
          const metadataPath = tableData.metadataLocation.replace('s3://iceberg-tables/', '');
          const object = await c.env.R2_BUCKET.get(metadataPath);
          if (object) {
            currentMetadata = await object.json() as TableMetadata;
          }
        }
      } else if (!hasAssertCreate) {
        // Table doesn't exist and this isn't a create transaction
        return icebergError(
          c,
          `Table does not exist: ${namespace.join('.')}.${tableName}`,
          'NoSuchTableException',
          404
        );
      }

      // Validate requirements
      const validation = validateRequirements(currentMetadata, body.requirements ?? []);
      if (!validation.valid) {
        return icebergError(c, validation.message!, 'CommitFailedException', 409);
      }

      // For create transactions, we need to build the initial metadata from the updates
      let newMetadata: TableMetadata;
      if (hasAssertCreate && currentMetadata === null) {
        // This is a create transaction - build initial metadata from updates
        // First, find the assign-uuid and set-location updates to get the basics
        let tableUuid = crypto.randomUUID();
        let tableLocation = '';
        let initialSchema: IcebergSchema | null = null;

        for (const update of body.updates ?? []) {
          if (update.action === 'assign-uuid') {
            tableUuid = update.uuid;
          } else if (update.action === 'set-location') {
            tableLocation = update.location;
          } else if (update.action === 'add-schema') {
            initialSchema = update.schema;
          }
        }

        if (!tableLocation) {
          // Default location if not provided
          const warehousePrefix = c.env.R2_BUCKET ? 's3://iceberg-tables' : 'file:///warehouse';
          tableLocation = `${warehousePrefix}/${namespace.join('/')}/${tableName}`;
        }

        if (!initialSchema) {
          return icebergError(c, 'Cannot create table: no schema provided in updates', 'BadRequestException', 400);
        }

        // Create base metadata
        const baseMetadata = createDefaultMetadata(
          tableUuid,
          tableLocation,
          initialSchema,
          undefined,
          undefined,
          {}
        );

        // Apply all updates to the base metadata
        newMetadata = applyUpdates(baseMetadata, body.updates ?? []);
      } else {
        // Apply updates to existing metadata
        newMetadata = applyUpdates(currentMetadata!, body.updates ?? []);
      }

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

      if (hasAssertCreate && currentMetadata === null) {
        // Create the table in the catalog
        const createResponse = await catalog.fetch(
          new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/tables`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              name: tableName,
              location: newMetadata.location,
              metadataLocation: newMetadataLocation,
              metadata: newMetadata,
              properties: newMetadata.properties ?? {},
            }),
          })
        );

        if (!createResponse.ok) {
          const error = await createResponse.json() as { error: string };
          if (createResponse.status === 409 || error.error?.includes('UNIQUE constraint') || error.error?.includes('already exists')) {
            return icebergError(c, `Table already exists: ${namespace.join('.')}.${tableName}`, 'AlreadyExistsException', 409);
          }
          if (createResponse.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
            return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
          }
          throw new Error(error.error || 'Failed to create table');
        }
      } else {
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
      }

      return c.json({
        'metadata-location': newMetadataLocation,
        metadata: newMetadata,
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to commit table changes',
        'ServiceFailureException',
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
        return icebergError(c, 'Source and destination are required', 'BadRequestException', 400);
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
        const error = await response.json() as { error: string; code?: string };
        // Check for namespace not found (destination namespace missing)
        // The DO returns "Namespace does not exist:" in the error message
        if (error.error?.includes('Namespace does not exist')) {
          return icebergError(
            c,
            `Namespace does not exist: ${body.destination.namespace.join('.')}`,
            'NoSuchNamespaceException',
            404
          );
        }
        // Check for table not found (source table missing)
        if (error.error?.includes('Table does not exist')) {
          return icebergError(
            c,
            `Table does not exist: ${body.source.namespace.join('.')}.${body.source.name}`,
            'NoSuchTableException',
            404
          );
        }
        if (error.error?.includes('already exists')) {
          return icebergError(
            c,
            `Table already exists: ${body.destination.namespace.join('.')}.${body.destination.name}`,
            'AlreadyExistsException',
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
        'ServiceFailureException',
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
        return icebergError(c, 'Table name is required', 'BadRequestException', 400);
      }

      if (!body['metadata-location']) {
        return icebergError(c, 'Metadata location is required', 'BadRequestException', 400);
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
          'BadRequestException',
          400
        );
      }

      // Extract table location from metadata
      const tableLocation = metadata.location;

      // Normalize metadata to ensure required fields have valid values
      const normalizedMetadata = normalizeTableMetadata(metadata);

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
            metadata: normalizedMetadata,
            properties: normalizedMetadata.properties ?? {},
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint')) {
          return icebergError(c, `Table already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExistsException', 409);
        }
        if (response.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
        }
        throw new Error(error.error || 'Failed to register table');
      }

      // Return LoadTableResponse format with normalized metadata
      return c.json({
        'metadata-location': metadataLocation,
        metadata: normalizedMetadata,
      });
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to register table',
        'ServiceFailureException',
        500
      );
    }
  });

  // ===========================================================================
  // View Routes
  // ===========================================================================

  // -------------------------------------------------------------------------
  // GET /namespaces/{namespace}/views - List views in namespace
  // -------------------------------------------------------------------------
  api.get('/namespaces/:namespace/views', requireTablePermission('table:list'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views`)
      );

      if (!response.ok) {
        return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
      }

      const data = await response.json() as { identifiers: Array<{ namespace: string[]; name: string }> };

      return c.json({
        identifiers: (data.identifiers ?? []).map(v => ({
          namespace: v.namespace,
          name: v.name,
        })),
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to list views',
        'ServiceFailureException',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/views - Create view
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/views', requireTablePermission('table:create'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as CreateViewRequest;

      if (!body.name) {
        return icebergError(c, 'View name is required', 'BadRequestException', 400);
      }

      if (!body.schema) {
        return icebergError(c, 'Schema is required', 'BadRequestException', 400);
      }

      if (!body['view-version']) {
        return icebergError(c, 'View version is required', 'BadRequestException', 400);
      }

      // Generate view UUID
      const viewUuid = crypto.randomUUID();

      // Determine view location
      const warehousePrefix = c.env.R2_BUCKET ? 's3://iceberg-tables' : 'file:///warehouse';
      const viewLocation = body.location ?? `${warehousePrefix}/${namespace.join('/')}/views/${body.name}`;

      // Normalize the schema
      const normalizedSchema = normalizeSchema(body.schema);
      if (normalizedSchema['schema-id'] === undefined) {
        normalizedSchema['schema-id'] = body.schema['schema-id'] ?? 0;
      }

      // Create initial view metadata
      const now = Date.now();
      const viewVersion: ViewVersion = {
        ...body['view-version'],
        'version-id': body['view-version']['version-id'] ?? 1,
        'timestamp-ms': body['view-version']['timestamp-ms'] ?? now,
        'schema-id': normalizedSchema['schema-id']!,
      };

      const viewMetadata: ViewMetadata = {
        'view-uuid': viewUuid,
        'format-version': 1,
        location: viewLocation,
        'current-version-id': viewVersion['version-id'],
        versions: [viewVersion],
        'version-log': [
          {
            'timestamp-ms': viewVersion['timestamp-ms'],
            'version-id': viewVersion['version-id'],
          },
        ],
        schemas: [normalizedSchema],
        properties: body.properties ?? {},
      };

      // Store view in catalog
      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: body.name,
            metadata: viewMetadata,
          }),
        })
      );

      if (!response.ok) {
        const error = await response.json() as { error: string };
        if (response.status === 409 || error.error?.includes('UNIQUE constraint') || error.error?.includes('already exists')) {
          return icebergError(c, `View already exists: ${namespace.join('.')}.${body.name}`, 'AlreadyExistsException', 409);
        }
        if (response.status === 404 || error.error?.includes('Namespace does not exist') || error.error?.toLowerCase().includes('namespace')) {
          return icebergError(c, `Namespace does not exist: ${namespace.join('.')}`, 'NoSuchNamespaceException', 404);
        }
        throw new Error(error.error || 'Failed to create view');
      }

      // Generate metadata location for the view
      const metadataLocation = `${viewMetadata.location}/metadata/v${viewMetadata['current-version-id']}.metadata.json`;

      return c.json({
        'metadata-location': metadataLocation,
        metadata: viewMetadata,
      }, 200);
    } catch (error) {
      if (error instanceof Response) throw error;
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to create view',
        'ServiceFailureException',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // GET/HEAD /namespaces/{namespace}/views/{view} - Load view / Check exists
  // -------------------------------------------------------------------------
  api.on(['GET', 'HEAD'], '/namespaces/:namespace/views/:view', requireTablePermission('table:read'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const viewName = c.req.param('view');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views/${encodeURIComponent(viewName)}`)
      );

      if (!response.ok) {
        // HEAD returns 404, GET returns error JSON
        if (c.req.method === 'HEAD') {
          return c.body(null, 404);
        }
        return icebergError(
          c,
          `View does not exist: ${namespace.join('.')}.${viewName}`,
          'NoSuchViewException',
          404
        );
      }

      // HEAD returns 204 No Content
      if (c.req.method === 'HEAD') {
        return c.body(null, 204);
      }

      const data = await response.json() as { metadata: ViewMetadata };

      // Generate metadata location for the view
      const viewLocation = data.metadata.location || `s3://iceberg-tables/${namespace.join('/')}/views/${viewName}`;
      const metadataLocation = `${viewLocation}/metadata/v${data.metadata['current-version-id']}.metadata.json`;

      return c.json({
        'metadata-location': metadataLocation,
        metadata: data.metadata,
      });
    } catch (error) {
      if (c.req.method === 'HEAD') {
        return c.body(null, 500);
      }
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to load view',
        'ServiceFailureException',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /namespaces/{namespace}/views/{view} - Replace view
  // -------------------------------------------------------------------------
  api.post('/namespaces/:namespace/views/:view', requireTablePermission('table:commit'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const viewName = c.req.param('view');
      const namespace = parseNamespace(namespaceParam);
      const body = await c.req.json() as ReplaceViewRequest;

      const catalog = getCatalogStub(c);

      // Load current view metadata
      const loadResponse = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views/${encodeURIComponent(viewName)}`)
      );

      if (!loadResponse.ok) {
        return icebergError(
          c,
          `View does not exist: ${namespace.join('.')}.${viewName}`,
          'NoSuchViewException',
          404
        );
      }

      const viewData = await loadResponse.json() as { metadata: ViewMetadata };
      let currentMetadata = viewData.metadata;

      // Validate requirements
      if (body.requirements) {
        for (const req of body.requirements) {
          if (req.type === 'assert-view-uuid' && currentMetadata['view-uuid'] !== req.uuid) {
            return icebergError(
              c,
              `View UUID mismatch: expected ${req.uuid}, got ${currentMetadata['view-uuid']}`,
              'CommitFailedException',
              409
            );
          }
        }
      }

      // Apply updates
      let newMetadata: ViewMetadata = { ...currentMetadata };
      if (body.updates) {
        for (const update of body.updates) {
          switch (update.action) {
            case 'assign-uuid':
              newMetadata['view-uuid'] = update.uuid;
              break;

            case 'set-location':
              newMetadata.location = update.location;
              break;

            case 'add-schema': {
              const schemaId = update.schema['schema-id'] ?? newMetadata.schemas.length;
              const newSchema = { ...update.schema, 'schema-id': schemaId };
              newMetadata.schemas = [...newMetadata.schemas, newSchema];
              break;
            }

            case 'add-view-version': {
              const newVersion = update['view-version'];
              newMetadata.versions = [...newMetadata.versions, newVersion];
              newMetadata['version-log'] = [
                ...newMetadata['version-log'],
                {
                  'timestamp-ms': newVersion['timestamp-ms'],
                  'version-id': newVersion['version-id'],
                },
              ];
              break;
            }

            case 'set-current-view-version':
              newMetadata['current-version-id'] = update['view-version-id'];
              break;

            case 'set-properties':
              newMetadata.properties = { ...(newMetadata.properties ?? {}), ...update.updates };
              break;

            case 'remove-properties':
              newMetadata.properties = Object.fromEntries(
                Object.entries(newMetadata.properties ?? {}).filter(
                  ([key]) => !update.removals.includes(key)
                )
              );
              break;
          }
        }
      }

      // Update view in catalog
      const updateResponse = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views/${encodeURIComponent(viewName)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            metadata: newMetadata,
          }),
        })
      );

      if (!updateResponse.ok) {
        const error = await updateResponse.json() as { error: string };
        throw new Error(error.error || 'Failed to replace view');
      }

      // Generate metadata location for the view
      const metadataLocation = `${newMetadata.location}/metadata/v${newMetadata['current-version-id']}.metadata.json`;

      return c.json({
        'metadata-location': metadataLocation,
        metadata: newMetadata,
      });
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to replace view',
        'ServiceFailureException',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // DELETE /namespaces/{namespace}/views/{view} - Drop view
  // -------------------------------------------------------------------------
  api.delete('/namespaces/:namespace/views/:view', requireTablePermission('table:drop'), async (c) => {
    try {
      const namespaceParam = c.req.param('namespace');
      const viewName = c.req.param('view');
      const namespace = parseNamespace(namespaceParam);

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request(`http://internal/namespaces/${encodeURIComponent(namespace.join('\x1f'))}/views/${encodeURIComponent(viewName)}`, {
          method: 'DELETE',
        })
      );

      if (!response.ok) {
        return icebergError(
          c,
          `View does not exist: ${namespace.join('.')}.${viewName}`,
          'NoSuchViewException',
          404
        );
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to drop view',
        'ServiceFailureException',
        500
      );
    }
  });

  // -------------------------------------------------------------------------
  // POST /views/rename - Rename view
  // -------------------------------------------------------------------------
  api.post('/views/rename', requireTablePermission('table:rename'), async (c) => {
    try {
      const body = await c.req.json() as RenameViewRequest;

      if (!body.source || !body.destination) {
        return icebergError(c, 'Source and destination are required', 'BadRequestException', 400);
      }

      const catalog = getCatalogStub(c);
      const response = await catalog.fetch(
        new Request('http://internal/views/rename', {
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
            `View does not exist: ${body.source.namespace.join('.')}.${body.source.name}`,
            'NoSuchViewException',
            404
          );
        }
        if (error.error?.includes('already exists')) {
          return icebergError(
            c,
            `View already exists: ${body.destination.namespace.join('.')}.${body.destination.name}`,
            'AlreadyExistsException',
            409
          );
        }
        throw new Error(error.error || 'Failed to rename view');
      }

      return c.body(null, 204);
    } catch (error) {
      return icebergError(
        c,
        error instanceof Error ? error.message : 'Failed to rename view',
        'ServiceFailureException',
        500
      );
    }
  });

  return api;
}

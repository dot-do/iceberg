/**
 * Iceberg Schema Evolution
 *
 * Implements schema evolution for Iceberg tables following the Apache Iceberg
 * specification. Schema evolution allows modifying table schemas while maintaining
 * backward compatibility with existing Parquet files.
 *
 * Key features:
 * - Add/drop/rename/reorder columns
 * - Type widening (compatible type promotions)
 * - Required/optional changes
 * - Nested field modifications
 * - Schema versioning and history tracking
 * - Field ID management (monotonically increasing)
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */
import type { IcebergSchema, IcebergStructField, IcebergType, IcebergPrimitiveType, TableMetadata } from './types.js';
/**
 * Check if a type promotion is allowed.
 *
 * Valid promotions:
 * - int -> long
 * - float -> double
 * - decimal(P, S) -> decimal(P', S) where P' > P (same scale)
 * - fixed(L) -> binary
 */
export declare function isTypePromotionAllowed(fromType: IcebergPrimitiveType, toType: IcebergPrimitiveType): boolean;
/**
 * Check if two types are compatible for evolution.
 * Returns a compatibility result with details about any incompatibility.
 */
export declare function areTypesCompatible(oldType: IcebergType, newType: IcebergType): TypeCompatibilityResult;
/** Result of type compatibility check */
export interface TypeCompatibilityResult {
    compatible: boolean;
    reason?: string;
}
/** Error codes for schema evolution operations */
export type SchemaEvolutionErrorCode = 'FIELD_NOT_FOUND' | 'FIELD_EXISTS' | 'INCOMPATIBLE_TYPE' | 'REQUIRED_FIELD_NO_DEFAULT' | 'INVALID_OPERATION' | 'INVALID_POSITION' | 'IDENTIFIER_FIELD';
/**
 * Error thrown during schema evolution operations.
 */
export declare class SchemaEvolutionError extends Error {
    readonly code: SchemaEvolutionErrorCode;
    constructor(message: string, code: SchemaEvolutionErrorCode);
}
/** Schema evolution operation types */
export type SchemaEvolutionOperation = AddColumnOperation | DropColumnOperation | RenameColumnOperation | UpdateColumnTypeOperation | MakeColumnOptionalOperation | MakeColumnRequiredOperation | UpdateColumnDocOperation | MoveColumnOperation;
/** Add a new column to the schema */
export interface AddColumnOperation {
    type: 'add-column';
    /** Column name (use dot notation for nested fields, e.g., "address.city") */
    name: string;
    /** The Iceberg type for the new column */
    fieldType: IcebergType;
    /** Whether the column is required (default: false) */
    required: boolean;
    /** Optional documentation for the column */
    doc?: string;
    /** Optional position specification */
    position?: ColumnPosition;
}
/** Drop a column from the schema */
export interface DropColumnOperation {
    type: 'drop-column';
    /** Column name to drop */
    name: string;
}
/** Rename a column */
export interface RenameColumnOperation {
    type: 'rename-column';
    /** Current column name */
    oldName: string;
    /** New column name */
    newName: string;
}
/** Update a column's type (only compatible promotions allowed) */
export interface UpdateColumnTypeOperation {
    type: 'update-column-type';
    /** Column name */
    name: string;
    /** New type (must be compatible with current type) */
    newType: IcebergType;
}
/** Make a required column optional */
export interface MakeColumnOptionalOperation {
    type: 'make-column-optional';
    /** Column name */
    name: string;
}
/** Make an optional column required */
export interface MakeColumnRequiredOperation {
    type: 'make-column-required';
    /** Column name */
    name: string;
}
/** Update a column's documentation */
export interface UpdateColumnDocOperation {
    type: 'update-column-doc';
    /** Column name */
    name: string;
    /** New documentation (undefined to remove) */
    doc: string | undefined;
}
/** Move a column to a new position */
export interface MoveColumnOperation {
    type: 'move-column';
    /** Column name */
    name: string;
    /** New position */
    position: ColumnPosition;
}
/**
 * Column position specification.
 * Used when adding or moving columns.
 */
export type ColumnPosition = {
    type: 'first';
} | {
    type: 'last';
} | {
    type: 'after';
    column: string;
} | {
    type: 'before';
    column: string;
};
/**
 * Builder for schema evolution operations.
 * Provides a fluent API for modifying Iceberg schemas.
 *
 * @example
 * ```ts
 * const builder = new SchemaEvolutionBuilder(currentSchema, metadata);
 *
 * builder
 *   .addColumn('email', 'string', { required: false })
 *   .renameColumn('user_name', 'username')
 *   .dropColumn('deprecated_field')
 *   .updateColumnType('count', 'long')
 *   .moveColumn('email', { type: 'after', column: 'name' });
 *
 * const { valid, errors } = builder.validate();
 * if (valid) {
 *   const newSchema = builder.build();
 * }
 * ```
 */
export declare class SchemaEvolutionBuilder {
    private operations;
    private currentSchema;
    private metadata;
    /**
     * Create a new schema evolution builder.
     *
     * @param schema - The current schema to evolve
     * @param metadata - Optional table metadata for field ID tracking
     */
    constructor(schema: IcebergSchema, metadata?: TableMetadata);
    /**
     * Add a new column to the schema.
     *
     * @param name - Column name (use dot notation for nested fields)
     * @param type - Iceberg type for the column
     * @param options - Optional settings (required, doc, position)
     */
    addColumn(name: string, type: IcebergType, options?: {
        required?: boolean;
        doc?: string;
        position?: ColumnPosition;
    }): this;
    /**
     * Drop a column from the schema.
     *
     * Note: The field ID is preserved for historical data compatibility.
     * Dropping identifier fields is not allowed.
     *
     * @param name - Column name to drop
     */
    dropColumn(name: string): this;
    /**
     * Rename a column.
     *
     * Note: Renaming preserves the field ID, so this is always compatible
     * with existing data files.
     *
     * @param oldName - Current column name
     * @param newName - New column name
     */
    renameColumn(oldName: string, newName: string): this;
    /**
     * Update a column's type.
     *
     * Only compatible type promotions are allowed:
     * - int -> long
     * - float -> double
     * - decimal(P, S) -> decimal(P', S) where P' > P
     * - fixed(L) -> binary
     *
     * @param name - Column name
     * @param newType - New type (must be compatible)
     */
    updateColumnType(name: string, newType: IcebergType): this;
    /**
     * Make a required column optional.
     *
     * This is always a safe operation for backward compatibility.
     *
     * @param name - Column name
     */
    makeColumnOptional(name: string): this;
    /**
     * Make an optional column required.
     *
     * Warning: This is only safe if all existing data has non-null values
     * for this column. Otherwise, reads of historical data may fail.
     *
     * @param name - Column name
     */
    makeColumnRequired(name: string): this;
    /**
     * Update a column's documentation.
     *
     * @param name - Column name
     * @param doc - New documentation (undefined to remove)
     */
    updateColumnDoc(name: string, doc: string | undefined): this;
    /**
     * Move a column to a new position.
     *
     * Note: Column ordering is metadata-only; it does not affect data files.
     *
     * @param name - Column name
     * @param position - New position specification
     */
    moveColumn(name: string, position: ColumnPosition): this;
    /**
     * Get the list of pending operations.
     */
    getOperations(): SchemaEvolutionOperation[];
    /**
     * Clear all pending operations.
     */
    clear(): this;
    /**
     * Validate all pending operations without applying them.
     *
     * @returns Validation result with any errors
     */
    validate(): SchemaValidationResult;
    /**
     * Build the new schema with all operations applied.
     *
     * @throws SchemaEvolutionError if any operation fails
     * @returns The evolved schema with incremented schema-id
     */
    build(): IcebergSchema;
    /**
     * Build the evolved schema and update table metadata.
     *
     * This method:
     * 1. Applies all operations to create a new schema
     * 2. Assigns monotonically increasing field IDs
     * 3. Updates the table metadata with the new schema
     * 4. Preserves schema history
     *
     * @throws Error if no metadata was provided to the builder
     * @returns Object containing the new schema, updated metadata, and next column ID
     */
    buildWithMetadata(): SchemaEvolutionResult;
    /**
     * Apply a single operation to a schema.
     * Returns a new schema with the operation applied.
     */
    private applyOperationToSchema;
    private applyAddColumn;
    private applyDropColumn;
    private applyRenameColumn;
    private applyUpdateColumnType;
    private applyMakeColumnOptional;
    private applyMakeColumnRequired;
    private applyUpdateColumnDoc;
    private applyMoveColumn;
    /**
     * Count add-column operations before the given operation.
     */
    private countPreviousAddOperations;
    /**
     * Resolve a position specification to an array index.
     */
    private resolvePosition;
    /**
     * Find the maximum field ID in a list of fields (recursive for nested types).
     */
    private findMaxFieldIdInFields;
}
/** Result of schema validation */
export interface SchemaValidationResult {
    valid: boolean;
    errors: string[];
}
/** Result of schema evolution with metadata */
export interface SchemaEvolutionResult {
    schema: IcebergSchema;
    metadata: TableMetadata;
    nextColumnId: number;
}
/** Types of schema changes detected in comparison */
export type SchemaChangeKind = 'added' | 'removed' | 'renamed' | 'type-changed' | 'nullability-changed' | 'doc-changed' | 'reordered';
/** A single change detected between two schemas */
export interface SchemaChangeSummary {
    /** Type of change */
    type: SchemaChangeKind;
    /** Field ID affected */
    fieldId: number;
    /** Current field name */
    fieldName: string;
    /** Previous value (varies by change type) */
    oldValue?: unknown;
    /** New value (varies by change type) */
    newValue?: unknown;
}
/**
 * Compare two schemas and return the differences.
 *
 * @param oldSchema - The original schema
 * @param newSchema - The new/evolved schema
 * @returns Array of changes detected between schemas
 */
export declare function compareSchemas(oldSchema: IcebergSchema, newSchema: IcebergSchema): SchemaChangeSummary[];
/** Result of backward compatibility check */
export interface CompatibilityResult {
    /** Whether the evolution is backward compatible */
    compatible: boolean;
    /** List of incompatible changes */
    incompatibleChanges: SchemaChangeSummary[];
}
/**
 * Check if a schema change is backward compatible.
 *
 * Backward compatibility means new readers can read data written with the old schema.
 *
 * The following changes are backward compatible:
 * - Adding optional fields
 * - Removing fields (readers ignore unknown fields)
 * - Renaming fields (field ID is preserved)
 * - Making required fields optional
 * - Type promotions (int->long, float->double)
 * - Documentation changes
 *
 * The following changes are NOT backward compatible:
 * - Adding required fields (old data won't have values)
 * - Making optional fields required (old data may have nulls)
 * - Incompatible type changes
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result with list of incompatible changes
 */
export declare function isBackwardCompatible(changes: SchemaChangeSummary[]): CompatibilityResult;
/**
 * Check if a schema change is forward compatible.
 *
 * Forward compatibility means old readers can read data written with the new schema.
 *
 * The following changes are forward compatible:
 * - Removing fields (old readers may expect them but can handle defaults)
 * - Adding optional fields (old readers will ignore)
 * - Renaming fields (field ID is preserved)
 * - Making required fields optional
 * - Documentation changes
 *
 * The following changes are NOT forward compatible:
 * - Adding required fields that old readers expect to be optional
 * - Making optional fields required
 * - Type changes (old readers may not handle new types)
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result with list of incompatible changes
 */
export declare function isForwardCompatible(changes: SchemaChangeSummary[]): CompatibilityResult;
/**
 * Check if a schema change is fully compatible (both backward and forward).
 *
 * Full compatibility means:
 * - New readers can read old data (backward compatible)
 * - Old readers can read new data (forward compatible)
 *
 * @param changes - Array of schema changes to check
 * @returns Compatibility result
 */
export declare function isFullyCompatible(changes: SchemaChangeSummary[]): CompatibilityResult;
/** Schema history entry */
export interface SchemaHistoryEntry {
    schemaId: number;
    fields: readonly IcebergStructField[];
    createdAt?: number;
}
/**
 * Get the schema history for a table.
 *
 * @param metadata - Table metadata containing schema history
 * @returns Array of schema history entries, ordered by schema ID
 */
export declare function getSchemaHistory(metadata: TableMetadata): SchemaHistoryEntry[];
/**
 * Get the schema for a specific snapshot.
 *
 * @param metadata - Table metadata
 * @param snapshotId - The snapshot ID to get schema for
 * @returns The schema for the snapshot, or null if not found
 */
export declare function getSchemaForSnapshot(metadata: TableMetadata, snapshotId: number): IcebergSchema | null;
/**
 * Get the changes between two schema versions.
 *
 * @param metadata - Table metadata
 * @param fromSchemaId - Starting schema ID
 * @param toSchemaId - Ending schema ID
 * @returns Array of changes between the schemas
 */
export declare function getSchemaChangesBetween(metadata: TableMetadata, fromSchemaId: number, toSchemaId: number): SchemaChangeSummary[];
/**
 * Find a field by name in a schema.
 *
 * @param schema - The schema to search
 * @param name - Field name to find
 * @returns The field or undefined if not found
 */
export declare function findFieldByName(schema: IcebergSchema, name: string): IcebergStructField | undefined;
/**
 * Find a field by ID in a schema.
 *
 * @param schema - The schema to search
 * @param id - Field ID to find
 * @returns The field or undefined if not found
 */
export declare function findFieldById(schema: IcebergSchema, id: number): IcebergStructField | undefined;
/**
 * Get all field IDs in a schema (including nested fields).
 *
 * @param schema - The schema to traverse
 * @returns Set of all field IDs
 */
export declare function getAllFieldIds(schema: IcebergSchema): Set<number>;
/**
 * Create a schema evolution builder for a table's current schema.
 *
 * @param metadata - Table metadata
 * @returns A new SchemaEvolutionBuilder
 */
export declare function evolveSchema(metadata: TableMetadata): SchemaEvolutionBuilder;
/**
 * Apply schema evolution operations to metadata.
 *
 * @param metadata - Table metadata
 * @param builder - Schema evolution builder with pending operations
 * @returns Updated table metadata
 */
export declare function applySchemaEvolution(_metadata: TableMetadata, builder: SchemaEvolutionBuilder): TableMetadata;
/**
 * Create a builder for evolving nested struct fields.
 *
 * @param schema - The parent schema
 * @param path - Path to the nested struct (array of field names)
 * @returns A SchemaEvolutionBuilder for the nested struct
 */
export declare function evolveNestedStruct(schema: IcebergSchema, path: string[]): SchemaEvolutionBuilder;
/**
 * Manages monotonically increasing field IDs for a table.
 */
export declare class FieldIdManager {
    private nextId;
    /**
     * Create a new field ID manager.
     *
     * @param startingId - The starting ID (typically last-column-id from metadata)
     */
    constructor(startingId: number);
    /**
     * Create from table metadata.
     */
    static fromMetadata(metadata: TableMetadata): FieldIdManager;
    /**
     * Get the next field ID and increment the counter.
     */
    getNextId(): number;
    /**
     * Get the current next ID without incrementing.
     */
    peekNextId(): number;
    /**
     * Get the last assigned ID.
     */
    getLastId(): number;
    /**
     * Reserve multiple IDs at once (for complex types with multiple field IDs).
     *
     * @param count - Number of IDs to reserve
     * @returns Array of reserved IDs
     */
    reserveIds(count: number): number[];
}
//# sourceMappingURL=schema-evolution.d.ts.map
/**
 * Iceberg Schema Utilities
 *
 * Schema creation, conversion, and evolution utilities.
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */
import type { IcebergSchema, IcebergStructField, IcebergType, PartitionSpec, SortOrder } from './types.js';
/**
 * Create the default Iceberg schema with _id, _seq, _op, _data columns.
 * This is a generic schema suitable for document-style data.
 */
export declare function createDefaultSchema(): IcebergSchema;
/**
 * Create a default (unpartitioned) partition spec.
 */
export declare function createUnpartitionedSpec(): PartitionSpec;
/**
 * Create a partition spec with a single identity transform.
 */
export declare function createIdentityPartitionSpec(sourceFieldId: number, fieldName: string, specId?: number): PartitionSpec;
/**
 * Create a bucket partition spec.
 */
export declare function createBucketPartitionSpec(sourceFieldId: number, fieldName: string, numBuckets: number, specId?: number): PartitionSpec;
/**
 * Create a time-based partition spec (year, month, day, hour).
 */
export declare function createTimePartitionSpec(sourceFieldId: number, fieldName: string, transform: 'year' | 'month' | 'day' | 'hour', specId?: number): PartitionSpec;
/**
 * Create an unsorted sort order.
 */
export declare function createUnsortedOrder(): SortOrder;
/**
 * Create a sort order on a single field.
 */
export declare function createSortOrder(sourceFieldId: number, direction?: 'asc' | 'desc', nullOrder?: 'nulls-first' | 'nulls-last', orderId?: number): SortOrder;
/** Parquet type names (for conversion) */
export type ParquetTypeName = 'BOOLEAN' | 'INT32' | 'INT64' | 'INT96' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'FIXED_LEN_BYTE_ARRAY';
/** Parquet converted type names */
export type ParquetConvertedType = 'UTF8' | 'DATE' | 'TIMESTAMP_MILLIS' | 'TIMESTAMP_MICROS' | 'INT_8' | 'INT_16' | 'INT_32' | 'UINT_8' | 'UINT_16' | 'UINT_32' | 'INT_64' | 'UINT_64' | 'DECIMAL' | 'JSON' | 'BSON';
/** Parquet schema element (simplified) */
export interface ParquetSchemaElement {
    name: string;
    type?: ParquetTypeName;
    convertedType?: ParquetConvertedType;
    repetitionType?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
    children?: ParquetSchemaElement[];
}
/**
 * Convert Parquet schema to Iceberg schema format.
 */
export declare function parquetToIcebergType(parquetType: ParquetTypeName, convertedType?: ParquetConvertedType): IcebergType;
/**
 * Convert a Parquet SchemaElement array to Iceberg schema.
 */
export declare function parquetSchemaToIceberg(parquetSchema: ParquetSchemaElement[], startFieldId?: number): IcebergSchema;
/** Types of schema changes that can occur */
export type SchemaChangeType = 'add-field' | 'remove-field' | 'make-optional' | 'rename-field' | 'update-doc' | 'widen-type';
/** A single schema change */
export interface SchemaChange {
    /** The type of change */
    type: SchemaChangeType;
    /** Field ID affected */
    fieldId: number;
    /** Field name (for add/rename operations) */
    fieldName?: string;
    /** Previous field name (for rename operations) */
    previousName?: string;
    /** Parent field ID (for nested fields, -1 for root) */
    parentFieldId: number;
    /** The new type (for add/widen operations) */
    newType?: IcebergType;
    /** The previous type (for widen operations) */
    previousType?: IcebergType;
    /** Whether the field is required */
    required?: boolean;
    /** Documentation string */
    doc?: string;
    /** Timestamp when the change occurred */
    timestampMs: number;
    /** Snapshot ID where this change was introduced */
    snapshotId?: number;
}
/** Result of schema comparison */
export interface SchemaComparisonResult {
    /** Whether the schemas are compatible (new schema can read old data) */
    compatible: boolean;
    /** List of changes between schemas */
    changes: SchemaChange[];
    /** Breaking changes that prevent compatibility */
    breakingChanges: SchemaChange[];
}
/**
 * Validate that a schema evolution is backwards compatible.
 */
export declare function validateSchemaEvolution(oldSchema: IcebergSchema, newSchema: IcebergSchema): SchemaComparisonResult;
/**
 * Generate a new schema ID based on existing schemas.
 */
export declare function generateSchemaId(existingSchemas: IcebergSchema[]): number;
/**
 * Find the maximum field ID in a schema.
 */
export declare function findMaxFieldId(schema: IcebergSchema): number;
/** Validation result for field or schema validation */
export interface FieldValidationResult {
    /** Whether the validation passed */
    valid: boolean;
    /** List of validation errors */
    errors: string[];
}
/**
 * Validate a field with 'unknown' type.
 *
 * Per the Iceberg v3 spec, unknown type fields:
 * - MUST be optional (required: false)
 * - Always have null values (not stored in data files)
 *
 * @param field - The struct field to validate
 * @returns Validation result with any errors
 */
export declare function validateUnknownTypeField(field: IcebergStructField): FieldValidationResult;
/**
 * Validate all fields in a schema, including nested struct fields.
 *
 * Validates:
 * - Unknown type fields must be optional
 * - Default values for special types (unknown, variant, geometry, geography)
 * - Nested struct fields are validated recursively
 *
 * @param schema - The schema to validate
 * @returns Validation result with any errors
 */
export declare function validateSchema(schema: IcebergSchema): FieldValidationResult;
/** Options for default value validation */
export interface FieldDefaultValidationOptions {
    /**
     * Whether this field is being added to an existing table.
     * Required fields being added must have an initial-default.
     */
    isNewField?: boolean;
}
/**
 * Validate default value for a field.
 *
 * Per the Iceberg spec:
 * - Fields with type unknown, variant, geometry, or geography MUST have null defaults
 * - Struct defaults must be empty {} or null (sub-field defaults tracked separately)
 * - Required fields being added to an existing table must have initial-default
 *
 * @param field - The struct field to validate
 * @param options - Validation options
 * @returns Validation result with any errors
 */
export declare function validateFieldDefault(field: IcebergStructField, options?: FieldDefaultValidationOptions): FieldValidationResult;
/** Result of checking if a default value can be changed */
export interface DefaultChangeResult {
    /** Whether the change is allowed */
    allowed: boolean;
    /** Reason if not allowed */
    reason?: string;
}
/**
 * Check if initial-default can be changed between schema versions.
 *
 * Per the Iceberg spec, initial-default cannot be changed once set.
 *
 * @param oldField - The old field definition
 * @param newField - The new field definition
 * @returns Whether the change is allowed
 */
export declare function canChangeInitialDefault(oldField: IcebergStructField, newField: IcebergStructField): DefaultChangeResult;
/**
 * Check if write-default can be changed between schema versions.
 *
 * Per the Iceberg spec, write-default can be changed through schema evolution.
 *
 * @param _oldField - The old field definition (unused, write-default can always change)
 * @param _newField - The new field definition (unused)
 * @returns Whether the change is allowed (always true)
 */
export declare function canChangeWriteDefault(_oldField: IcebergStructField, _newField: IcebergStructField): DefaultChangeResult;
//# sourceMappingURL=schema.d.ts.map
/**
 * Iceberg Schema Utilities
 *
 * Schema creation, conversion, and evolution utilities.
 *
 * @see https://iceberg.apache.org/spec/#schema-evolution
 */
import { PARTITION_FIELD_ID_START } from './constants.js';
// ============================================================================
// Default Schema Creation
// ============================================================================
/**
 * Create the default Iceberg schema with _id, _seq, _op, _data columns.
 * This is a generic schema suitable for document-style data.
 */
export function createDefaultSchema() {
    return {
        'schema-id': 0,
        type: 'struct',
        fields: [
            {
                id: 1,
                name: '_id',
                required: true,
                type: 'string',
            },
            {
                id: 2,
                name: '_seq',
                required: true,
                type: 'long',
            },
            {
                id: 3,
                name: '_op',
                required: true,
                type: 'string',
            },
            {
                id: 4,
                name: '_data',
                required: true,
                type: 'binary', // Variant-encoded document
            },
        ],
    };
}
// ============================================================================
// Partition Spec Creation
// ============================================================================
/**
 * Create a default (unpartitioned) partition spec.
 */
export function createUnpartitionedSpec() {
    return {
        'spec-id': 0,
        fields: [],
    };
}
/**
 * Create a partition spec with a single identity transform.
 */
export function createIdentityPartitionSpec(sourceFieldId, fieldName, specId = 0) {
    return {
        'spec-id': specId,
        fields: [
            {
                'source-id': sourceFieldId,
                'field-id': PARTITION_FIELD_ID_START + sourceFieldId, // Partition field IDs start at PARTITION_FIELD_ID_START
                name: fieldName,
                transform: 'identity',
            },
        ],
    };
}
/**
 * Create a bucket partition spec.
 */
export function createBucketPartitionSpec(sourceFieldId, fieldName, numBuckets, specId = 0) {
    return {
        'spec-id': specId,
        fields: [
            {
                'source-id': sourceFieldId,
                'field-id': PARTITION_FIELD_ID_START + sourceFieldId,
                name: fieldName,
                transform: `bucket[${numBuckets}]`,
            },
        ],
    };
}
/**
 * Create a time-based partition spec (year, month, day, hour).
 */
export function createTimePartitionSpec(sourceFieldId, fieldName, transform, specId = 0) {
    return {
        'spec-id': specId,
        fields: [
            {
                'source-id': sourceFieldId,
                'field-id': PARTITION_FIELD_ID_START + sourceFieldId,
                name: fieldName,
                transform,
            },
        ],
    };
}
// ============================================================================
// Sort Order Creation
// ============================================================================
/**
 * Create an unsorted sort order.
 */
export function createUnsortedOrder() {
    return {
        'order-id': 0,
        fields: [],
    };
}
/**
 * Create a sort order on a single field.
 */
export function createSortOrder(sourceFieldId, direction = 'asc', nullOrder = 'nulls-first', orderId = 1) {
    return {
        'order-id': orderId,
        fields: [
            {
                'source-id': sourceFieldId,
                transform: 'identity',
                direction,
                'null-order': nullOrder,
            },
        ],
    };
}
/**
 * Convert Parquet schema to Iceberg schema format.
 */
export function parquetToIcebergType(parquetType, convertedType) {
    // Handle converted types first
    if (convertedType) {
        switch (convertedType) {
            case 'UTF8':
                return 'string';
            case 'DATE':
                return 'date';
            case 'TIMESTAMP_MILLIS':
            case 'TIMESTAMP_MICROS':
                return 'timestamp';
            case 'INT_8':
            case 'INT_16':
            case 'INT_32':
            case 'UINT_8':
            case 'UINT_16':
            case 'UINT_32':
                return 'int';
            case 'INT_64':
            case 'UINT_64':
                return 'long';
            case 'DECIMAL':
                return 'decimal';
            case 'JSON':
            case 'BSON':
                return 'string'; // Store as string for JSON/BSON
        }
    }
    // Handle physical types
    switch (parquetType) {
        case 'BOOLEAN':
            return 'boolean';
        case 'INT32':
            return 'int';
        case 'INT64':
            return 'long';
        case 'INT96':
            return 'timestamp'; // INT96 is typically timestamp
        case 'FLOAT':
            return 'float';
        case 'DOUBLE':
            return 'double';
        case 'BYTE_ARRAY':
            return 'binary';
        case 'FIXED_LEN_BYTE_ARRAY':
            return 'fixed';
        default:
            return 'binary'; // Fallback
    }
}
/**
 * Convert a Parquet SchemaElement array to Iceberg schema.
 */
export function parquetSchemaToIceberg(parquetSchema, startFieldId = 1) {
    let nextFieldId = startFieldId;
    const fields = [];
    // Skip the root "schema" element if present
    const elements = parquetSchema[0]?.name === 'schema' ? parquetSchema.slice(1) : parquetSchema;
    function convertElement(element) {
        const fieldId = nextFieldId++;
        const required = element.repetitionType === 'REQUIRED';
        // Handle nested structures
        if (element.children && element.children.length > 0) {
            const childFields = element.children.map(convertElement);
            return {
                id: fieldId,
                name: element.name,
                required,
                type: {
                    type: 'struct',
                    fields: childFields,
                },
            };
        }
        // Handle primitive types
        const icebergType = parquetToIcebergType(element.type || 'BYTE_ARRAY', element.convertedType);
        return {
            id: fieldId,
            name: element.name,
            required,
            type: icebergType,
        };
    }
    for (const element of elements) {
        fields.push(convertElement(element));
    }
    return {
        'schema-id': 0,
        type: 'struct',
        fields,
    };
}
/**
 * Validate that a schema evolution is backwards compatible.
 */
export function validateSchemaEvolution(oldSchema, newSchema) {
    const changes = [];
    const breakingChanges = [];
    const oldFields = flattenFields(oldSchema.fields);
    const newFields = flattenFields(newSchema.fields);
    const oldFieldIds = new Set(oldFields.map((f) => f.field.id));
    const newFieldIds = new Set(newFields.map((f) => f.field.id));
    // Check for removed fields
    for (const { field, parentId } of oldFields) {
        if (!newFieldIds.has(field.id)) {
            const change = {
                type: 'remove-field',
                fieldId: field.id,
                fieldName: field.name,
                parentFieldId: parentId,
                previousType: field.type,
                timestampMs: Date.now(),
            };
            changes.push(change);
            // Removing a required field is a breaking change
            if (field.required) {
                breakingChanges.push(change);
            }
        }
    }
    // Check for added fields
    for (const { field, parentId } of newFields) {
        if (!oldFieldIds.has(field.id)) {
            const change = {
                type: 'add-field',
                fieldId: field.id,
                fieldName: field.name,
                parentFieldId: parentId,
                newType: field.type,
                required: field.required,
                doc: field.doc,
                timestampMs: Date.now(),
            };
            changes.push(change);
            // Adding a required field is a breaking change
            if (field.required) {
                breakingChanges.push(change);
            }
        }
    }
    // Check for modified fields
    for (const { field: oldField, parentId } of oldFields) {
        const newFieldEntry = newFields.find((f) => f.field.id === oldField.id);
        if (!newFieldEntry)
            continue;
        const newField = newFieldEntry.field;
        // Check for name change
        if (oldField.name !== newField.name) {
            changes.push({
                type: 'rename-field',
                fieldId: oldField.id,
                fieldName: newField.name,
                previousName: oldField.name,
                parentFieldId: parentId,
                timestampMs: Date.now(),
            });
        }
        // Check for required -> optional change
        if (oldField.required && !newField.required) {
            changes.push({
                type: 'make-optional',
                fieldId: oldField.id,
                fieldName: newField.name,
                parentFieldId: parentId,
                required: false,
                timestampMs: Date.now(),
            });
        }
        // Check for optional -> required change (breaking)
        if (!oldField.required && newField.required) {
            const change = {
                type: 'add-field', // Treated as adding a required constraint
                fieldId: oldField.id,
                fieldName: newField.name,
                parentFieldId: parentId,
                required: true,
                timestampMs: Date.now(),
            };
            changes.push(change);
            breakingChanges.push(change);
        }
        // Check for type widening
        if (isTypeWidened(oldField.type, newField.type)) {
            changes.push({
                type: 'widen-type',
                fieldId: oldField.id,
                fieldName: newField.name,
                parentFieldId: parentId,
                previousType: oldField.type,
                newType: newField.type,
                timestampMs: Date.now(),
            });
        }
    }
    return {
        compatible: breakingChanges.length === 0,
        changes,
        breakingChanges,
    };
}
/**
 * Generate a new schema ID based on existing schemas.
 */
export function generateSchemaId(existingSchemas) {
    if (existingSchemas.length === 0) {
        return 0;
    }
    const maxId = Math.max(...existingSchemas.map((s) => s['schema-id']));
    return maxId + 1;
}
/**
 * Find the maximum field ID in a schema.
 */
export function findMaxFieldId(schema) {
    let maxId = 0;
    function traverse(fields) {
        for (const field of fields) {
            maxId = Math.max(maxId, field.id);
            if (typeof field.type === 'object' && field.type !== null) {
                const complexType = field.type;
                if (complexType.type === 'struct' && complexType.fields) {
                    traverse(complexType.fields);
                }
                if (complexType['element-id']) {
                    maxId = Math.max(maxId, complexType['element-id']);
                }
                if (complexType['key-id']) {
                    maxId = Math.max(maxId, complexType['key-id']);
                }
                if (complexType['value-id']) {
                    maxId = Math.max(maxId, complexType['value-id']);
                }
            }
        }
    }
    traverse(schema.fields);
    return maxId;
}
// ============================================================================
// Helper Functions
// ============================================================================
/**
 * Flatten a nested field structure with parent tracking.
 */
function flattenFields(fields, parentId = -1) {
    const result = [];
    for (const field of fields) {
        result.push({ field, parentId });
        if (typeof field.type === 'object' && field.type.type === 'struct') {
            const nested = flattenFields(field.type.fields, field.id);
            result.push(...nested);
        }
    }
    return result;
}
/**
 * Check if a type change represents type widening.
 */
function isTypeWidened(oldType, newType) {
    // Only primitive types can be widened
    if (typeof oldType !== 'string' || typeof newType !== 'string') {
        return false;
    }
    // int -> long is allowed
    if (oldType === 'int' && newType === 'long') {
        return true;
    }
    // float -> double is allowed
    if (oldType === 'float' && newType === 'double') {
        return true;
    }
    return false;
}
//# sourceMappingURL=schema.js.map
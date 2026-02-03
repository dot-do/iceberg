/**
 * Wikidata Dataset Schema
 *
 * Schema for entity/property graph data from Wikidata.
 * Large schemas with semi-structured data, good for testing
 * schema evolution and variant handling at scale.
 */

import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

/**
 * Wikidata Entities schema - main entity records.
 */
export const WIKIDATA_ENTITIES_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'entity_id', required: true, type: 'string' }, // Q12345
    { id: 2, name: 'entity_type', required: true, type: 'string' }, // item, property, lexeme
    { id: 3, name: 'labels', required: false, type: 'string' }, // JSON object (lang -> label)
    { id: 4, name: 'descriptions', required: false, type: 'string' }, // JSON object
    { id: 5, name: 'aliases', required: false, type: 'string' }, // JSON object (lang -> [aliases])
    { id: 6, name: 'claims', required: false, type: 'string' }, // JSON object (property -> values)
    { id: 7, name: 'sitelinks', required: false, type: 'string' }, // JSON object (wiki -> title)
    { id: 8, name: 'instance_of', required: false, type: { type: 'list', 'element-id': 101, element: 'string', 'element-required': false } },
    { id: 9, name: 'subclass_of', required: false, type: { type: 'list', 'element-id': 102, element: 'string', 'element-required': false } },
    { id: 10, name: 'part_of', required: false, type: { type: 'list', 'element-id': 103, element: 'string', 'element-required': false } },
    { id: 11, name: 'last_modified', required: true, type: 'timestamptz' },
    { id: 12, name: 'revision_id', required: false, type: 'long' },
  ],
};

/**
 * Wikidata Claims schema - normalized property values.
 */
export const WIKIDATA_CLAIMS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'claim_id', required: true, type: 'string' },
    { id: 2, name: 'entity_id', required: true, type: 'string' },
    { id: 3, name: 'property_id', required: true, type: 'string' }, // P31, P279, etc.
    { id: 4, name: 'value_type', required: true, type: 'string' }, // wikibase-entityid, string, time, quantity
    { id: 5, name: 'value_string', required: false, type: 'string' },
    { id: 6, name: 'value_entity_id', required: false, type: 'string' },
    { id: 7, name: 'value_time', required: false, type: 'string' }, // ISO 8601 with precision
    { id: 8, name: 'value_quantity', required: false, type: 'decimal(20,6)' },
    { id: 9, name: 'value_unit', required: false, type: 'string' },
    { id: 10, name: 'qualifiers', required: false, type: 'string' }, // JSON object
    { id: 11, name: 'references', required: false, type: 'string' }, // JSON array
    { id: 12, name: 'rank', required: false, type: 'string' }, // preferred, normal, deprecated
  ],
};

/**
 * Wikidata Properties schema - property definitions.
 */
export const WIKIDATA_PROPERTIES_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'property_id', required: true, type: 'string' }, // P31
    { id: 2, name: 'label', required: true, type: 'string' },
    { id: 3, name: 'description', required: false, type: 'string' },
    { id: 4, name: 'data_type', required: true, type: 'string' },
    { id: 5, name: 'subject_type', required: false, type: 'string' },
    { id: 6, name: 'value_type_constraint', required: false, type: 'string' },
    { id: 7, name: 'allowed_qualifiers', required: false, type: { type: 'list', 'element-id': 101, element: 'string', 'element-required': false } },
    { id: 8, name: 'inverse_property', required: false, type: 'string' },
    { id: 9, name: 'formatter_url', required: false, type: 'string' },
  ],
};

/**
 * Wikidata Labels schema - flattened labels for search.
 */
export const WIKIDATA_LABELS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'entity_id', required: true, type: 'string' },
    { id: 2, name: 'language', required: true, type: 'string' },
    { id: 3, name: 'label', required: true, type: 'string' },
    { id: 4, name: 'label_type', required: true, type: 'string' }, // label, alias, description
  ],
};

/**
 * Partition spec by entity type.
 */
export const WIKIDATA_PARTITION_BY_TYPE: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 2, // entity_type
      'field-id': 1000,
      name: 'entity_type',
      transform: 'identity',
    },
  ],
};

/**
 * Partition spec by entity ID bucket (for distributed processing).
 */
export const WIKIDATA_PARTITION_BY_BUCKET: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 1, // entity_id
      'field-id': 1000,
      name: 'entity_bucket',
      transform: 'bucket[256]',
    },
  ],
};

/**
 * Partition spec by property for claims table.
 */
export const WIKIDATA_CLAIMS_PARTITION_BY_PROPERTY: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 3, // property_id
      'field-id': 1000,
      name: 'property_id',
      transform: 'identity',
    },
  ],
};

/**
 * Default properties for Wikidata tables.
 */
export const WIKIDATA_TABLE_PROPERTIES: Record<string, string> = {
  'write.format.default': 'parquet',
  'write.parquet.compression-codec': 'zstd',
  'write.parquet.dict-size-bytes': '2097152', // 2MB dict for repeated strings
  'catalog.source': 'Wikidata',
  'variant.shred.columns': 'labels,descriptions,claims,sitelinks',
};

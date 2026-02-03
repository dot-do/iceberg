/**
 * Wiktionary Dataset Schema
 *
 * Schema for dictionary/lexical data from Wiktionary.
 * Heavy on variant/JSON fields for definitions, examples, etymology.
 * Good for testing variant shredding benchmarks.
 */

import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

/**
 * Wiktionary Entries schema - main word entries with variant data.
 */
export const WIKTIONARY_ENTRIES_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'entry_id', required: true, type: 'string' },
    { id: 2, name: 'word', required: true, type: 'string' },
    { id: 3, name: 'language', required: true, type: 'string' },
    { id: 4, name: 'part_of_speech', required: true, type: 'string' },
    { id: 5, name: 'pronunciation_ipa', required: false, type: 'string' },
    { id: 6, name: 'pronunciation_audio', required: false, type: 'string' },
    // Variant/JSON fields for complex nested data
    { id: 7, name: 'definitions', required: false, type: 'string' }, // JSON array
    { id: 8, name: 'etymology', required: false, type: 'string' }, // JSON object
    { id: 9, name: 'synonyms', required: false, type: { type: 'list', 'element-id': 101, element: 'string', 'element-required': false } },
    { id: 10, name: 'antonyms', required: false, type: { type: 'list', 'element-id': 102, element: 'string', 'element-required': false } },
    { id: 11, name: 'related_terms', required: false, type: { type: 'list', 'element-id': 103, element: 'string', 'element-required': false } },
    { id: 12, name: 'usage_notes', required: false, type: 'string' },
    { id: 13, name: 'example_sentences', required: false, type: 'string' }, // JSON array
    { id: 14, name: 'translations', required: false, type: 'string' }, // JSON object (lang -> translation)
    { id: 15, name: 'categories', required: false, type: { type: 'list', 'element-id': 104, element: 'string', 'element-required': false } },
    { id: 16, name: 'revision_timestamp', required: true, type: 'timestamptz' },
  ],
};

/**
 * Wiktionary Definitions schema - normalized definitions table.
 */
export const WIKTIONARY_DEFINITIONS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'definition_id', required: true, type: 'string' },
    { id: 2, name: 'entry_id', required: true, type: 'string' },
    { id: 3, name: 'word', required: true, type: 'string' },
    { id: 4, name: 'language', required: true, type: 'string' },
    { id: 5, name: 'part_of_speech', required: true, type: 'string' },
    { id: 6, name: 'definition_order', required: true, type: 'int' },
    { id: 7, name: 'definition_text', required: true, type: 'string' },
    { id: 8, name: 'glosses', required: false, type: { type: 'list', 'element-id': 101, element: 'string', 'element-required': false } },
    { id: 9, name: 'examples', required: false, type: 'string' }, // JSON array
    { id: 10, name: 'labels', required: false, type: { type: 'list', 'element-id': 102, element: 'string', 'element-required': false } },
    { id: 11, name: 'sense_id', required: false, type: 'string' },
  ],
};

/**
 * Wiktionary Etymology schema - word origins and derivations.
 */
export const WIKTIONARY_ETYMOLOGY_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'etymology_id', required: true, type: 'string' },
    { id: 2, name: 'entry_id', required: true, type: 'string' },
    { id: 3, name: 'word', required: true, type: 'string' },
    { id: 4, name: 'language', required: true, type: 'string' },
    { id: 5, name: 'etymology_text', required: false, type: 'string' },
    { id: 6, name: 'derived_from', required: false, type: 'string' }, // JSON object
    { id: 7, name: 'cognates', required: false, type: 'string' }, // JSON array
    { id: 8, name: 'root_word', required: false, type: 'string' },
    { id: 9, name: 'source_language', required: false, type: 'string' },
  ],
};

/**
 * Partition spec by language (English, German, French, etc.).
 */
export const WIKTIONARY_PARTITION_BY_LANGUAGE: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 3, // language
      'field-id': 1000,
      name: 'language',
      transform: 'identity',
    },
  ],
};

/**
 * Partition spec by first letter (for alphabetical queries).
 */
export const WIKTIONARY_PARTITION_BY_LETTER: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 2, // word
      'field-id': 1000,
      name: 'word_bucket',
      transform: 'truncate[1]',
    },
  ],
};

/**
 * Default properties for Wiktionary tables.
 */
export const WIKTIONARY_TABLE_PROPERTIES: Record<string, string> = {
  'write.format.default': 'parquet',
  'write.parquet.compression-codec': 'zstd',
  'catalog.source': 'Wiktionary',
  'variant.shred.columns': 'definitions,etymology,translations',
};

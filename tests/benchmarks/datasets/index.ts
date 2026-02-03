/**
 * Dataset Exports
 *
 * Re-exports all dataset schemas for use in benchmarks.
 */

// IMDB Movies
export {
  IMDB_MOVIES_SCHEMA,
  IMDB_RATINGS_SCHEMA,
  IMDB_CREW_SCHEMA,
  IMDB_PARTITION_BY_YEAR,
  IMDB_PARTITION_BY_DATE,
  IMDB_TABLE_PROPERTIES,
} from './imdb.js';

// O*NET Occupations
export {
  ONET_OCCUPATIONS_SCHEMA,
  ONET_SKILLS_SCHEMA,
  ONET_ABILITIES_SCHEMA,
  ONET_KNOWLEDGE_SCHEMA,
  ONET_TASKS_SCHEMA,
  ONET_PARTITION_BY_JOB_ZONE,
  ONET_TABLE_PROPERTIES,
} from './onet.js';

// Wiktionary
export {
  WIKTIONARY_ENTRIES_SCHEMA,
  WIKTIONARY_DEFINITIONS_SCHEMA,
  WIKTIONARY_ETYMOLOGY_SCHEMA,
  WIKTIONARY_PARTITION_BY_LANGUAGE,
  WIKTIONARY_PARTITION_BY_LETTER,
  WIKTIONARY_TABLE_PROPERTIES,
} from './wiktionary.js';

// Wikidata
export {
  WIKIDATA_ENTITIES_SCHEMA,
  WIKIDATA_CLAIMS_SCHEMA,
  WIKIDATA_PROPERTIES_SCHEMA,
  WIKIDATA_LABELS_SCHEMA,
  WIKIDATA_PARTITION_BY_TYPE,
  WIKIDATA_PARTITION_BY_BUCKET,
  WIKIDATA_CLAIMS_PARTITION_BY_PROPERTY,
  WIKIDATA_TABLE_PROPERTIES,
} from './wikidata.js';

// ============================================================================
// Dataset Registry
// ============================================================================

import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

import {
  IMDB_MOVIES_SCHEMA,
  IMDB_PARTITION_BY_YEAR,
  IMDB_TABLE_PROPERTIES,
} from './imdb.js';

import {
  ONET_OCCUPATIONS_SCHEMA,
  ONET_PARTITION_BY_JOB_ZONE,
  ONET_TABLE_PROPERTIES,
} from './onet.js';

import {
  WIKTIONARY_ENTRIES_SCHEMA,
  WIKTIONARY_PARTITION_BY_LANGUAGE,
  WIKTIONARY_TABLE_PROPERTIES,
} from './wiktionary.js';

import {
  WIKIDATA_ENTITIES_SCHEMA,
  WIKIDATA_PARTITION_BY_TYPE,
  WIKIDATA_TABLE_PROPERTIES,
} from './wikidata.js';

export interface DatasetDefinition {
  name: string;
  description: string;
  schema: IcebergSchema;
  partitionSpec?: PartitionSpec;
  properties: Record<string, string>;
  fieldCount: number;
  complexity: 'simple' | 'medium' | 'complex';
}

/**
 * Registry of all available datasets for benchmarking.
 */
export const DATASETS: DatasetDefinition[] = [
  {
    name: 'imdb_movies',
    description: 'IMDB movies with ratings and metadata',
    schema: IMDB_MOVIES_SCHEMA,
    partitionSpec: IMDB_PARTITION_BY_YEAR,
    properties: IMDB_TABLE_PROPERTIES,
    fieldCount: 15,
    complexity: 'medium',
  },
  {
    name: 'onet_occupations',
    description: 'O*NET occupation data with skills and abilities',
    schema: ONET_OCCUPATIONS_SCHEMA,
    partitionSpec: ONET_PARTITION_BY_JOB_ZONE,
    properties: ONET_TABLE_PROPERTIES,
    fieldCount: 16,
    complexity: 'medium',
  },
  {
    name: 'wiktionary_entries',
    description: 'Wiktionary dictionary entries with variant/JSON data',
    schema: WIKTIONARY_ENTRIES_SCHEMA,
    partitionSpec: WIKTIONARY_PARTITION_BY_LANGUAGE,
    properties: WIKTIONARY_TABLE_PROPERTIES,
    fieldCount: 16,
    complexity: 'complex',
  },
  {
    name: 'wikidata_entities',
    description: 'Wikidata knowledge graph entities',
    schema: WIKIDATA_ENTITIES_SCHEMA,
    partitionSpec: WIKIDATA_PARTITION_BY_TYPE,
    properties: WIKIDATA_TABLE_PROPERTIES,
    fieldCount: 12,
    complexity: 'complex',
  },
];

/**
 * Get a dataset by name.
 */
export function getDataset(name: string): DatasetDefinition | undefined {
  return DATASETS.find((d) => d.name === name);
}

/**
 * Get datasets filtered by complexity.
 */
export function getDatasetsByComplexity(
  complexity: 'simple' | 'medium' | 'complex'
): DatasetDefinition[] {
  return DATASETS.filter((d) => d.complexity === complexity);
}

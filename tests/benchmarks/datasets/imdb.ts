/**
 * IMDB Movies Dataset Schema
 *
 * Realistic schema for movie data (~7M records in full dataset).
 * Good for time-series queries, ratings aggregation, and nested data.
 */

import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

/**
 * IMDB Movies schema - 15 fields including nested types.
 */
export const IMDB_MOVIES_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'movie_id', required: true, type: 'string' },
    { id: 2, name: 'title', required: true, type: 'string' },
    { id: 3, name: 'original_title', required: false, type: 'string' },
    { id: 4, name: 'release_year', required: false, type: 'int' },
    { id: 5, name: 'release_date', required: false, type: 'date' },
    { id: 6, name: 'runtime_minutes', required: false, type: 'int' },
    { id: 7, name: 'rating', required: false, type: 'double' },
    { id: 8, name: 'num_votes', required: false, type: 'long' },
    { id: 9, name: 'budget', required: false, type: 'decimal(15,2)' },
    { id: 10, name: 'revenue', required: false, type: 'decimal(15,2)' },
    { id: 11, name: 'genres', required: false, type: { type: 'list', 'element-id': 101, element: 'string', 'element-required': false } },
    { id: 12, name: 'director', required: false, type: 'string' },
    { id: 13, name: 'language', required: false, type: 'string' },
    { id: 14, name: 'country', required: false, type: 'string' },
    { id: 15, name: 'last_updated', required: true, type: 'timestamptz' },
  ],
};

/**
 * Partition spec by release year (common for historical queries).
 */
export const IMDB_PARTITION_BY_YEAR: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 4, // release_year
      'field-id': 1000,
      name: 'release_year',
      transform: 'identity',
    },
  ],
};

/**
 * Partition spec by release date (day granularity).
 */
export const IMDB_PARTITION_BY_DATE: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 5, // release_date
      'field-id': 1000,
      name: 'release_date_day',
      transform: 'day',
    },
  ],
};

/**
 * IMDB Ratings schema - for separate ratings table.
 */
export const IMDB_RATINGS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'movie_id', required: true, type: 'string' },
    { id: 2, name: 'user_id', required: true, type: 'string' },
    { id: 3, name: 'rating', required: true, type: 'float' },
    { id: 4, name: 'timestamp', required: true, type: 'timestamptz' },
  ],
};

/**
 * IMDB Crew schema - for credits/crew data.
 */
export const IMDB_CREW_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'movie_id', required: true, type: 'string' },
    { id: 2, name: 'person_id', required: true, type: 'string' },
    { id: 3, name: 'name', required: true, type: 'string' },
    { id: 4, name: 'role', required: true, type: 'string' },
    { id: 5, name: 'character', required: false, type: 'string' },
    { id: 6, name: 'order', required: false, type: 'int' },
  ],
};

/**
 * Default properties for IMDB tables.
 */
export const IMDB_TABLE_PROPERTIES: Record<string, string> = {
  'write.format.default': 'parquet',
  'write.parquet.compression-codec': 'zstd',
  'write.metadata.compression-codec': 'gzip',
};

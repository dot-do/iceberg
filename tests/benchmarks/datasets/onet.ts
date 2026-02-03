/**
 * O*NET Occupations Dataset Schema
 *
 * Schema for occupational data from O*NET (Occupational Information Network).
 * Contains 20+ fields across multiple related tables for hierarchical data.
 */

import type { IcebergSchema, PartitionSpec } from '../../../core/src/metadata/types.js';

/**
 * O*NET Occupations schema - main occupation data.
 */
export const ONET_OCCUPATIONS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'onet_soc_code', required: true, type: 'string' },
    { id: 2, name: 'title', required: true, type: 'string' },
    { id: 3, name: 'description', required: false, type: 'string' },
    { id: 4, name: 'job_zone', required: false, type: 'int' },
    { id: 5, name: 'job_family', required: false, type: 'string' },
    { id: 6, name: 'median_wage', required: false, type: 'decimal(10,2)' },
    { id: 7, name: 'employment_count', required: false, type: 'long' },
    { id: 8, name: 'projected_growth', required: false, type: 'double' },
    { id: 9, name: 'education_level', required: false, type: 'string' },
    { id: 10, name: 'experience_level', required: false, type: 'string' },
    { id: 11, name: 'on_the_job_training', required: false, type: 'string' },
    { id: 12, name: 'bright_outlook', required: false, type: 'boolean' },
    { id: 13, name: 'green_occupation', required: false, type: 'boolean' },
    { id: 14, name: 'apprenticeship', required: false, type: 'boolean' },
    { id: 15, name: 'data_updated_date', required: true, type: 'date' },
    { id: 16, name: 'last_modified', required: true, type: 'timestamptz' },
  ],
};

/**
 * O*NET Skills schema - skills associated with occupations.
 */
export const ONET_SKILLS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'onet_soc_code', required: true, type: 'string' },
    { id: 2, name: 'element_id', required: true, type: 'string' },
    { id: 3, name: 'element_name', required: true, type: 'string' },
    { id: 4, name: 'scale_id', required: true, type: 'string' },
    { id: 5, name: 'scale_name', required: false, type: 'string' },
    { id: 6, name: 'data_value', required: false, type: 'double' },
    { id: 7, name: 'standard_error', required: false, type: 'double' },
    { id: 8, name: 'lower_ci', required: false, type: 'double' },
    { id: 9, name: 'upper_ci', required: false, type: 'double' },
    { id: 10, name: 'recommend_suppress', required: false, type: 'boolean' },
    { id: 11, name: 'not_relevant', required: false, type: 'boolean' },
    { id: 12, name: 'category', required: false, type: 'string' },
  ],
};

/**
 * O*NET Abilities schema - cognitive and physical abilities.
 */
export const ONET_ABILITIES_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'onet_soc_code', required: true, type: 'string' },
    { id: 2, name: 'element_id', required: true, type: 'string' },
    { id: 3, name: 'element_name', required: true, type: 'string' },
    { id: 4, name: 'scale_id', required: true, type: 'string' },
    { id: 5, name: 'data_value', required: false, type: 'double' },
    { id: 6, name: 'standard_error', required: false, type: 'double' },
    { id: 7, name: 'category', required: false, type: 'string' },
  ],
};

/**
 * O*NET Knowledge schema - knowledge areas required.
 */
export const ONET_KNOWLEDGE_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'onet_soc_code', required: true, type: 'string' },
    { id: 2, name: 'element_id', required: true, type: 'string' },
    { id: 3, name: 'element_name', required: true, type: 'string' },
    { id: 4, name: 'scale_id', required: true, type: 'string' },
    { id: 5, name: 'data_value', required: false, type: 'double' },
    { id: 6, name: 'standard_error', required: false, type: 'double' },
    { id: 7, name: 'category', required: false, type: 'string' },
  ],
};

/**
 * O*NET Tasks schema - work activities and tasks.
 */
export const ONET_TASKS_SCHEMA: IcebergSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'onet_soc_code', required: true, type: 'string' },
    { id: 2, name: 'task_id', required: true, type: 'long' },
    { id: 3, name: 'task', required: true, type: 'string' },
    { id: 4, name: 'task_type', required: false, type: 'string' },
    { id: 5, name: 'incumbents_responding', required: false, type: 'int' },
    { id: 6, name: 'date_updated', required: false, type: 'date' },
    { id: 7, name: 'domain_source', required: false, type: 'string' },
  ],
};

/**
 * Partition spec by job zone (1-5 education levels).
 */
export const ONET_PARTITION_BY_JOB_ZONE: PartitionSpec = {
  'spec-id': 0,
  fields: [
    {
      'source-id': 4, // job_zone
      'field-id': 1000,
      name: 'job_zone',
      transform: 'identity',
    },
  ],
};

/**
 * Default properties for O*NET tables.
 */
export const ONET_TABLE_PROPERTIES: Record<string, string> = {
  'write.format.default': 'parquet',
  'write.parquet.compression-codec': 'zstd',
  'catalog.source': 'O*NET',
  'catalog.version': '28.0',
};

/**
 * Iceberg Partition Transforms
 *
 * Implements partition transforms per the Apache Iceberg v2 specification.
 * Supports identity, bucket, truncate, temporal, and void transforms.
 *
 * @see https://iceberg.apache.org/spec/#partitioning
 * @see https://iceberg.apache.org/spec/#partition-transforms
 */

import type {
  IcebergSchema,
  IcebergStructField,
  PartitionField,
  PartitionSpec,
  PartitionTransform,
} from './types.js';
import {
  PARTITION_FIELD_ID_START,
  INITIAL_PARTITION_ID,
  MS_PER_DAY,
  MS_PER_HOUR,
} from './constants.js';

// ============================================================================
// Types
// ============================================================================

/** Parsed transform with optional argument */
export interface ParsedTransform {
  /** Transform type */
  type: PartitionTransform;
  /** Transform argument (for bucket and truncate) */
  arg?: number;
}

/** Definition for creating a partition field */
export interface PartitionFieldDefinition {
  /** Source field name in the schema */
  sourceField: string;
  /** Transform to apply */
  transform: PartitionTransform;
  /** Optional name for the partition field (defaults to generated name) */
  name?: string;
  /** Transform argument (required for bucket and truncate) */
  transformArg?: number;
}

/** Options for creating a partition spec */
export interface CreatePartitionSpecOptions {
  /** Partition spec ID (defaults to 0) */
  specId?: number;
  /** Starting field ID for partition fields (defaults to PARTITION_FIELD_ID_START) */
  startingFieldId?: number;
}

/** A partition value with metadata */
export interface PartitionValue {
  /** Partition field name */
  fieldName: string;
  /** Partition value (transformed) */
  value: unknown;
  /** Transform used */
  transform: PartitionTransform;
  /** Transform argument (if applicable) */
  transformArg?: number;
}

/** Data file partition information */
export interface PartitionedFile {
  /** File path */
  filePath: string;
  /** Partition data (field name -> transformed value) */
  partitionData: Record<string, unknown>;
  /** Record count */
  recordCount: number;
  /** File size in bytes */
  fileSizeBytes: number;
}

/** Partition statistics for a single partition */
export interface PartitionStats {
  /** Partition values (field name -> value) */
  partitionValues: Record<string, unknown>;
  /** Number of data files in this partition */
  fileCount: number;
  /** Total row count in this partition */
  rowCount: number;
  /** Total size in bytes */
  sizeBytes: number;
  /** Last modified timestamp */
  lastModified: number;
}

/** Aggregate statistics across all partitions */
export interface PartitionStatsAggregate {
  /** Total number of partitions */
  partitionCount: number;
  /** Total number of files */
  totalFileCount: number;
  /** Total row count */
  totalRowCount: number;
  /** Total size in bytes */
  totalSizeBytes: number;
  /** Per-partition statistics */
  partitions: PartitionStats[];
  /** Statistics grouped by partition field */
  byField: Record<
    string,
    {
      distinctValues: number;
      minValue?: unknown;
      maxValue?: unknown;
    }
  >;
}

// ============================================================================
// Transform Parsing
// ============================================================================

/**
 * Parse a transform string (e.g., "bucket[16]", "truncate[5]", "identity")
 * into a ParsedTransform object.
 */
export function parseTransform(transform: string): ParsedTransform {
  // Check for parameterized transforms
  const bucketMatch = transform.match(/^bucket\[(\d+)\]$/);
  if (bucketMatch) {
    return { type: 'bucket', arg: parseInt(bucketMatch[1], 10) };
  }

  const truncateMatch = transform.match(/^truncate\[(\d+)\]$/);
  if (truncateMatch) {
    return { type: 'truncate', arg: parseInt(truncateMatch[1], 10) };
  }

  // Simple transforms
  const simpleTransforms: PartitionTransform[] = [
    'identity',
    'year',
    'month',
    'day',
    'hour',
    'void',
  ];

  if (simpleTransforms.includes(transform as PartitionTransform)) {
    return { type: transform as PartitionTransform };
  }

  throw new Error(`Unknown partition transform: ${transform}`);
}

/**
 * Format a transform for serialization (e.g., { type: 'bucket', arg: 16 } -> "bucket[16]")
 */
export function formatTransform(parsed: ParsedTransform): string {
  if (parsed.arg !== undefined) {
    return `${parsed.type}[${parsed.arg}]`;
  }
  return parsed.type;
}

// ============================================================================
// Transform Implementations
// ============================================================================

/**
 * Apply a partition transform to a value.
 * Returns the transformed partition value.
 */
export function applyTransform(
  value: unknown,
  transform: PartitionTransform | string,
  transformArg?: number
): unknown {
  // Handle null/undefined
  if (value === null || value === undefined) {
    return null;
  }

  // Parse transform if it's a string like "bucket[16]"
  let transformType: PartitionTransform;
  let arg = transformArg;

  if (typeof transform === 'string' && transform.includes('[')) {
    // This is a parameterized transform string like "bucket[16]"
    const parsed = parseTransform(transform);
    transformType = parsed.type;
    arg = parsed.arg ?? arg;
  } else if (typeof transform === 'string' && isRecognizedTransform(transform)) {
    // This is a simple transform type name
    transformType = transform as PartitionTransform;
  } else {
    throw new Error(`Unknown partition transform: ${transform}`);
  }

  switch (transformType) {
    case 'identity':
      return identityTransform(value);

    case 'bucket':
      if (arg === undefined) {
        throw new Error('Bucket transform requires number of buckets');
      }
      return bucketTransform(value, arg);

    case 'truncate':
      if (arg === undefined) {
        throw new Error('Truncate transform requires width');
      }
      return truncateTransform(value, arg);

    case 'year':
      return yearTransform(value);

    case 'month':
      return monthTransform(value);

    case 'day':
      return dayTransform(value);

    case 'hour':
      return hourTransform(value);

    case 'void':
      return voidTransform();

    default:
      throw new Error(`Unknown transform: ${transformType}`);
  }
}

/**
 * Check if a transform is a recognized transform type (simple or parameterized base name).
 */
function isRecognizedTransform(transform: string): boolean {
  return ['identity', 'year', 'month', 'day', 'hour', 'void', 'bucket', 'truncate'].includes(
    transform
  );
}

/**
 * Identity transform - returns value unchanged.
 * Per spec: identity(v) returns v
 */
function identityTransform(value: unknown): unknown {
  return value;
}

/**
 * Bucket transform - hash partitioning into N buckets.
 * Per spec: bucket[N](v) = murmur3_32(v) % N
 *
 * We use a 32-bit MurmurHash3-like algorithm for deterministic bucketing.
 */
function bucketTransform(value: unknown, numBuckets: number): number {
  if (numBuckets <= 0) {
    throw new Error('Number of buckets must be positive');
  }

  const hash = murmur3Hash32(value);
  // Ensure non-negative result
  return ((hash % numBuckets) + numBuckets) % numBuckets;
}

/**
 * MurmurHash3-inspired 32-bit hash for deterministic bucketing.
 * Based on the Iceberg spec's use of MurmurHash3.
 */
function murmur3Hash32(value: unknown): number {
  let data: Uint8Array;

  if (typeof value === 'string') {
    data = new TextEncoder().encode(value);
  } else if (typeof value === 'number') {
    // Handle integers and floats
    if (Number.isInteger(value)) {
      // For integers, use 8 bytes (long representation)
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setBigInt64(0, BigInt(value), true); // little-endian
      data = new Uint8Array(buffer);
    } else {
      // For floats, use IEEE 754 representation
      const buffer = new ArrayBuffer(8);
      const view = new DataView(buffer);
      view.setFloat64(0, value, true);
      data = new Uint8Array(buffer);
    }
  } else if (typeof value === 'bigint') {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, value, true);
    data = new Uint8Array(buffer);
  } else if (typeof value === 'boolean') {
    data = new Uint8Array([value ? 1 : 0]);
  } else if (value instanceof Uint8Array) {
    data = value;
  } else if (value instanceof Date) {
    const buffer = new ArrayBuffer(8);
    const view = new DataView(buffer);
    view.setBigInt64(0, BigInt(value.getTime()), true);
    data = new Uint8Array(buffer);
  } else {
    // Fallback: convert to string
    data = new TextEncoder().encode(String(value));
  }

  // MurmurHash3 32-bit implementation
  const c1 = 0xcc9e2d51;
  const c2 = 0x1b873593;
  const seed = 0;

  let h1 = seed;
  const len = data.length;
  const nblocks = Math.floor(len / 4);

  // Body
  for (let i = 0; i < nblocks; i++) {
    let k1 =
      (data[i * 4] | (data[i * 4 + 1] << 8) | (data[i * 4 + 2] << 16) | (data[i * 4 + 3] << 24)) >>>
      0;

    k1 = Math.imul(k1, c1);
    k1 = ((k1 << 15) | (k1 >>> 17)) >>> 0;
    k1 = Math.imul(k1, c2);

    h1 ^= k1;
    h1 = ((h1 << 13) | (h1 >>> 19)) >>> 0;
    h1 = (Math.imul(h1, 5) + 0xe6546b64) >>> 0;
  }

  // Tail - intentional fallthrough behavior for MurmurHash3
  const tail = data.slice(nblocks * 4);
  let k1 = 0;

  if (tail.length >= 3) {
    k1 ^= tail[2] << 16;
  }
  if (tail.length >= 2) {
    k1 ^= tail[1] << 8;
  }
  if (tail.length >= 1) {
    k1 ^= tail[0];
    k1 = Math.imul(k1, c1);
    k1 = ((k1 << 15) | (k1 >>> 17)) >>> 0;
    k1 = Math.imul(k1, c2);
    h1 ^= k1;
  }

  // Finalization
  h1 ^= len;
  h1 ^= h1 >>> 16;
  h1 = Math.imul(h1, 0x85ebca6b);
  h1 ^= h1 >>> 13;
  h1 = Math.imul(h1, 0xc2b2ae35);
  h1 ^= h1 >>> 16;

  return h1 >>> 0; // Ensure unsigned
}

/**
 * Truncate transform - truncate values to width W.
 * Per spec:
 *   - For strings: truncate to first W characters
 *   - For integers: truncate to multiple of W (floor(v / W) * W)
 *   - For decimals: similar to integer truncation
 */
function truncateTransform(value: unknown, width: number): unknown {
  if (width <= 0) {
    throw new Error('Truncate width must be positive');
  }

  if (typeof value === 'string') {
    return value.substring(0, width);
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      // Integer truncation: floor division
      return Math.floor(value / width) * width;
    }
    // For floats, convert to integer representation first
    return Math.floor(value / width) * width;
  }

  if (typeof value === 'bigint') {
    return (value / BigInt(width)) * BigInt(width);
  }

  // For other types, return as-is
  return value;
}

/**
 * Year transform - extracts year from timestamp/date.
 * Per spec: years from 1970-01-01 (years since epoch)
 * Returns integer: year - 1970
 */
function yearTransform(value: unknown): number {
  const date = toDate(value);
  return date.getUTCFullYear() - 1970;
}

/**
 * Month transform - extracts month from timestamp/date.
 * Per spec: months from 1970-01-01 (months since epoch)
 * Returns integer: (year - 1970) * 12 + month
 */
function monthTransform(value: unknown): number {
  const date = toDate(value);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth(); // 0-indexed
  return (year - 1970) * 12 + month;
}

/**
 * Day transform - extracts day from timestamp/date.
 * Per spec: days from 1970-01-01 (days since epoch)
 */
function dayTransform(value: unknown): number {
  const date = toDate(value);
  // Days since Unix epoch
  return Math.floor(date.getTime() / MS_PER_DAY);
}

/**
 * Hour transform - extracts hour from timestamp.
 * Per spec: hours from 1970-01-01 00:00:00 (hours since epoch)
 */
function hourTransform(value: unknown): number {
  const date = toDate(value);
  // Hours since Unix epoch
  return Math.floor(date.getTime() / MS_PER_HOUR);
}

/**
 * Void transform - always produces null.
 * Used for partition evolution when removing a partition field.
 */
function voidTransform(): null {
  return null;
}

/**
 * Convert a value to a Date object.
 */
function toDate(value: unknown): Date {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'number') {
    // Assume milliseconds since epoch
    return new Date(value);
  }
  if (typeof value === 'bigint') {
    return new Date(Number(value));
  }
  if (typeof value === 'string') {
    const parsed = new Date(value);
    if (isNaN(parsed.getTime())) {
      throw new Error(`Cannot parse date string: ${value}`);
    }
    return parsed;
  }
  throw new Error(`Cannot convert value to date: ${typeof value}`);
}

// ============================================================================
// Transform Result Types
// ============================================================================

/**
 * Get the result type of a transform applied to a source type.
 * Per Iceberg spec, transforms produce specific result types.
 */
export function getTransformResultType(
  _sourceType: string,
  transform: PartitionTransform | string
): string {
  // Parse transform if it contains brackets (e.g., "bucket[16]")
  let transformType: PartitionTransform;
  if (typeof transform === 'string' && transform.includes('[')) {
    const parsed = parseTransform(transform);
    transformType = parsed.type;
  } else {
    transformType = transform as PartitionTransform;
  }

  switch (transformType) {
    case 'identity':
      // Identity preserves the source type
      return _sourceType;
    case 'bucket':
      // Bucket always produces int
      return 'int';
    case 'truncate':
      // Truncate preserves the source type
      return _sourceType;
    case 'year':
    case 'month':
    case 'day':
    case 'hour':
      // Temporal transforms produce int
      return 'int';
    case 'void':
      // Void can be any type (always null)
      return _sourceType;
    default:
      return _sourceType;
  }
}

// ============================================================================
// PartitionSpecBuilder
// ============================================================================

/**
 * Builder class for creating partition specifications.
 *
 * @example
 * ```typescript
 * const spec = new PartitionSpecBuilder(schema)
 *   .identity('region')
 *   .day('created_at')
 *   .bucket('user_id', 16)
 *   .build();
 * ```
 */
export class PartitionSpecBuilder {
  private readonly schema: IcebergSchema;
  private readonly fields: PartitionField[] = [];
  private readonly specId: number;
  private nextFieldId: number;

  constructor(schema: IcebergSchema, options?: CreatePartitionSpecOptions) {
    this.schema = schema;
    this.specId = options?.specId ?? 0;
    this.nextFieldId = options?.startingFieldId ?? PARTITION_FIELD_ID_START;
  }

  /**
   * Add an identity partition field.
   * Values are partitioned exactly as they appear.
   */
  identity(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'identity', partitionName);
  }

  /**
   * Add a bucket partition field.
   * Values are hashed into N buckets.
   */
  bucket(sourceFieldName: string, numBuckets: number, partitionName?: string): this {
    return this.addField(sourceFieldName, 'bucket', partitionName, numBuckets);
  }

  /**
   * Add a truncate partition field.
   * Values are truncated to width W.
   */
  truncate(sourceFieldName: string, width: number, partitionName?: string): this {
    return this.addField(sourceFieldName, 'truncate', partitionName, width);
  }

  /**
   * Add a year partition field.
   * Extracts years since epoch from timestamp/date.
   */
  year(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'year', partitionName);
  }

  /**
   * Add a month partition field.
   * Extracts months since epoch from timestamp/date.
   */
  month(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'month', partitionName);
  }

  /**
   * Add a day partition field.
   * Extracts days since epoch from timestamp/date.
   */
  day(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'day', partitionName);
  }

  /**
   * Add an hour partition field.
   * Extracts hours since epoch from timestamp.
   */
  hour(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'hour', partitionName);
  }

  /**
   * Add a void partition field.
   * Always produces null (useful for partition evolution).
   */
  void(sourceFieldName: string, partitionName?: string): this {
    return this.addField(sourceFieldName, 'void', partitionName);
  }

  /**
   * Add a partition field from a definition object.
   */
  addFieldFromDefinition(definition: PartitionFieldDefinition): this {
    return this.addField(
      definition.sourceField,
      definition.transform,
      definition.name,
      definition.transformArg
    );
  }

  /**
   * Get the current number of fields.
   */
  get fieldCount(): number {
    return this.fields.length;
  }

  /**
   * Build the partition specification.
   */
  build(): PartitionSpec {
    return {
      'spec-id': this.specId,
      fields: [...this.fields],
    };
  }

  /**
   * Internal method to add a partition field.
   */
  private addField(
    sourceFieldName: string,
    transform: PartitionTransform,
    partitionName?: string,
    transformArg?: number
  ): this {
    const sourceField = this.findSchemaField(sourceFieldName);
    if (!sourceField) {
      throw new Error(`Source field '${sourceFieldName}' not found in schema`);
    }

    // Validate transform argument requirements
    if ((transform === 'bucket' || transform === 'truncate') && transformArg === undefined) {
      throw new Error(`Transform '${transform}' requires a transform argument`);
    }

    const fieldId = this.nextFieldId++;
    const name = partitionName || this.generatePartitionName(sourceFieldName, transform);

    // Format the transform string
    let transformStr: string = transform;
    if (transformArg !== undefined) {
      transformStr = `${transform}[${transformArg}]`;
    }

    const field: PartitionField = {
      'source-id': sourceField.id,
      'field-id': fieldId,
      name,
      transform: transformStr,
    };

    this.fields.push(field);
    return this;
  }

  /**
   * Find a field in the schema by name.
   */
  private findSchemaField(name: string): IcebergStructField | undefined {
    return this.schema.fields.find((f) => f.name === name);
  }

  /**
   * Generate a default partition field name.
   */
  private generatePartitionName(sourceFieldName: string, transform: PartitionTransform): string {
    switch (transform) {
      case 'identity':
        return sourceFieldName;
      case 'bucket':
        return `${sourceFieldName}_bucket`;
      case 'truncate':
        return `${sourceFieldName}_trunc`;
      case 'year':
        return `${sourceFieldName}_year`;
      case 'month':
        return `${sourceFieldName}_month`;
      case 'day':
        return `${sourceFieldName}_day`;
      case 'hour':
        return `${sourceFieldName}_hour`;
      case 'void':
        return `${sourceFieldName}_void`;
      default:
        return `${sourceFieldName}_${transform}`;
    }
  }
}

// ============================================================================
// Partition Data Extraction
// ============================================================================

/**
 * Get partition data for a record based on a partition spec.
 */
export function getPartitionData(
  record: Record<string, unknown>,
  spec: PartitionSpec,
  schema: IcebergSchema
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  // Build field ID to name map
  const fieldIdToName = new Map<number, string>();
  for (const field of schema.fields) {
    fieldIdToName.set(field.id, field.name);
  }

  // Apply transforms for each partition field
  for (const partitionField of spec.fields) {
    const sourceFieldName = fieldIdToName.get(partitionField['source-id']);
    if (!sourceFieldName) {
      throw new Error(`Source field ID ${partitionField['source-id']} not found in schema`);
    }

    const sourceValue = record[sourceFieldName];
    const partitionValue = applyTransform(sourceValue, partitionField.transform);

    result[partitionField.name] = partitionValue;
  }

  return result;
}

/**
 * Generate the partition path for partition data (e.g., "year=54/month=653/day=19750").
 * Uses Iceberg's Hive-style partition path format.
 */
export function getPartitionPath(
  partitionData: Record<string, unknown>,
  spec: PartitionSpec
): string {
  const parts: string[] = [];

  for (const field of spec.fields) {
    const value = partitionData[field.name];
    // Per Iceberg spec, null values use __HIVE_DEFAULT_PARTITION__
    const strValue =
      value === null || value === undefined ? '__HIVE_DEFAULT_PARTITION__' : String(value);
    parts.push(`${field.name}=${strValue}`);
  }

  return parts.join('/');
}

/**
 * Parse a partition path back to partition data.
 */
export function parsePartitionPath(path: string): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const parts = path.split('/');

  for (const part of parts) {
    const [name, value] = part.split('=');
    if (name && value !== undefined) {
      if (value === '__HIVE_DEFAULT_PARTITION__') {
        result[name] = null;
      } else if (!isNaN(Number(value))) {
        result[name] = Number(value);
      } else {
        result[name] = value;
      }
    }
  }

  return result;
}

// ============================================================================
// Partition Statistics Collector
// ============================================================================

/**
 * Collects and aggregates statistics across partitions.
 */
export class PartitionStatsCollector {
  private readonly spec: PartitionSpec;
  private readonly partitionMap: Map<string, PartitionStats> = new Map();

  constructor(spec: PartitionSpec) {
    this.spec = spec;
  }

  /**
   * Add a data file to the statistics.
   */
  addFile(file: PartitionedFile): void {
    const key = this.getPartitionKey(file.partitionData);

    const existing = this.partitionMap.get(key);
    if (existing) {
      existing.fileCount++;
      existing.rowCount += file.recordCount;
      existing.sizeBytes += file.fileSizeBytes;
      existing.lastModified = Math.max(existing.lastModified, Date.now());
    } else {
      this.partitionMap.set(key, {
        partitionValues: { ...file.partitionData },
        fileCount: 1,
        rowCount: file.recordCount,
        sizeBytes: file.fileSizeBytes,
        lastModified: Date.now(),
      });
    }
  }

  /**
   * Remove a data file from the statistics.
   */
  removeFile(file: PartitionedFile): void {
    const key = this.getPartitionKey(file.partitionData);

    const existing = this.partitionMap.get(key);
    if (existing) {
      existing.fileCount--;
      existing.rowCount -= file.recordCount;
      existing.sizeBytes -= file.fileSizeBytes;

      if (existing.fileCount <= 0) {
        this.partitionMap.delete(key);
      }
    }
  }

  /**
   * Get aggregate statistics.
   */
  getStats(): PartitionStatsAggregate {
    const partitions = Array.from(this.partitionMap.values());

    let totalFileCount = 0;
    let totalRowCount = 0;
    let totalSizeBytes = 0;

    const fieldValues: Map<string, Set<string>> = new Map();
    const fieldBounds: Map<string, { min: unknown; max: unknown }> = new Map();

    for (const partition of partitions) {
      totalFileCount += partition.fileCount;
      totalRowCount += partition.rowCount;
      totalSizeBytes += partition.sizeBytes;

      for (const [fieldName, value] of Object.entries(partition.partitionValues)) {
        if (!fieldValues.has(fieldName)) {
          fieldValues.set(fieldName, new Set());
        }
        fieldValues.get(fieldName)!.add(String(value));

        if (!fieldBounds.has(fieldName)) {
          fieldBounds.set(fieldName, { min: value, max: value });
        } else {
          const bounds = fieldBounds.get(fieldName)!;
          if (this.isLessThan(value, bounds.min)) {
            bounds.min = value;
          }
          if (this.isLessThan(bounds.max, value)) {
            bounds.max = value;
          }
        }
      }
    }

    const byField: PartitionStatsAggregate['byField'] = {};
    for (const [fieldName, values] of fieldValues) {
      const bounds = fieldBounds.get(fieldName);
      byField[fieldName] = {
        distinctValues: values.size,
        minValue: bounds?.min,
        maxValue: bounds?.max,
      };
    }

    return {
      partitionCount: partitions.length,
      totalFileCount,
      totalRowCount,
      totalSizeBytes,
      partitions,
      byField,
    };
  }

  /**
   * Get statistics for a specific partition.
   */
  getPartitionStats(partitionData: Record<string, unknown>): PartitionStats | undefined {
    const key = this.getPartitionKey(partitionData);
    return this.partitionMap.get(key);
  }

  /**
   * Get all partition keys.
   */
  getPartitionKeys(): string[] {
    return Array.from(this.partitionMap.keys());
  }

  /**
   * Clear all statistics.
   */
  clear(): void {
    this.partitionMap.clear();
  }

  /**
   * Generate a stable key for partition values.
   */
  private getPartitionKey(partitionData: Record<string, unknown>): string {
    const sortedFields = this.spec.fields.map((f) => f.name).sort();

    const parts = sortedFields.map((name) => {
      const value = partitionData[name];
      return `${name}=${value === null || value === undefined ? 'null' : String(value)}`;
    });

    return parts.join('/');
  }

  /**
   * Compare two values for ordering.
   */
  private isLessThan(a: unknown, b: unknown): boolean {
    if (a === null || a === undefined) return true;
    if (b === null || b === undefined) return false;

    if (typeof a === 'number' && typeof b === 'number') {
      return a < b;
    }
    if (typeof a === 'string' && typeof b === 'string') {
      return a < b;
    }

    return false;
  }
}

// ============================================================================
// Partition Spec Evolution
// ============================================================================

/** Types of partition spec changes */
export type PartitionSpecChangeType =
  | 'add-field'
  | 'remove-field'
  | 'rename-field'
  | 'change-transform';

/** A single partition spec change */
export interface PartitionSpecChange {
  /** Type of change */
  type: PartitionSpecChangeType;
  /** Field ID affected */
  fieldId: number;
  /** Field name */
  fieldName?: string;
  /** Previous field name (for rename) */
  previousName?: string;
  /** New transform (for change-transform) */
  newTransform?: string;
  /** Previous transform (for change-transform) */
  previousTransform?: string;
}

/** Result of partition spec comparison */
export interface PartitionSpecComparisonResult {
  /** Whether the specs are compatible */
  compatible: boolean;
  /** List of changes between specs */
  changes: PartitionSpecChange[];
}

/**
 * Compare two partition specs to identify changes.
 */
export function comparePartitionSpecs(
  oldSpec: PartitionSpec,
  newSpec: PartitionSpec
): PartitionSpecComparisonResult {
  const changes: PartitionSpecChange[] = [];

  const oldFieldIds = new Set(oldSpec.fields.map((f) => f['field-id']));
  const newFieldIds = new Set(newSpec.fields.map((f) => f['field-id']));

  // Check for removed fields
  for (const field of oldSpec.fields) {
    if (!newFieldIds.has(field['field-id'])) {
      changes.push({
        type: 'remove-field',
        fieldId: field['field-id'],
        fieldName: field.name,
        previousTransform: field.transform,
      });
    }
  }

  // Check for added fields
  for (const field of newSpec.fields) {
    if (!oldFieldIds.has(field['field-id'])) {
      changes.push({
        type: 'add-field',
        fieldId: field['field-id'],
        fieldName: field.name,
        newTransform: field.transform,
      });
    }
  }

  // Check for modified fields
  for (const oldField of oldSpec.fields) {
    const newField = newSpec.fields.find((f) => f['field-id'] === oldField['field-id']);
    if (!newField) continue;

    if (oldField.name !== newField.name) {
      changes.push({
        type: 'rename-field',
        fieldId: oldField['field-id'],
        fieldName: newField.name,
        previousName: oldField.name,
      });
    }

    if (oldField.transform !== newField.transform) {
      changes.push({
        type: 'change-transform',
        fieldId: oldField['field-id'],
        fieldName: newField.name,
        previousTransform: oldField.transform,
        newTransform: newField.transform,
      });
    }
  }

  return {
    compatible: true, // Partition evolution is always "compatible" in Iceberg v2
    changes,
  };
}

/**
 * Find the maximum partition field ID in a spec.
 */
export function findMaxPartitionFieldId(spec: PartitionSpec): number {
  if (spec.fields.length === 0) {
    return INITIAL_PARTITION_ID; // Partition field IDs start at PARTITION_FIELD_ID_START
  }
  return Math.max(...spec.fields.map((f) => f['field-id']));
}

/**
 * Generate a new partition spec ID based on existing specs.
 */
export function generatePartitionSpecId(existingSpecs: PartitionSpec[]): number {
  if (existingSpecs.length === 0) {
    return 0;
  }
  return Math.max(...existingSpecs.map((s) => s['spec-id'])) + 1;
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a partition spec builder.
 */
export function createPartitionSpecBuilder(
  schema: IcebergSchema,
  options?: CreatePartitionSpecOptions
): PartitionSpecBuilder {
  return new PartitionSpecBuilder(schema, options);
}

/**
 * Create a partition spec from field definitions.
 */
export function createPartitionSpecFromDefinitions(
  schema: IcebergSchema,
  fields: PartitionFieldDefinition[],
  options?: CreatePartitionSpecOptions
): PartitionSpec {
  const builder = new PartitionSpecBuilder(schema, options);
  for (const field of fields) {
    builder.addFieldFromDefinition(field);
  }
  return builder.build();
}

/**
 * Create a partition stats collector.
 */
export function createPartitionStatsCollector(spec: PartitionSpec): PartitionStatsCollector {
  return new PartitionStatsCollector(spec);
}

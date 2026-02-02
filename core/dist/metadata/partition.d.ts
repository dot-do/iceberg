/**
 * Iceberg Partition Transforms
 *
 * Implements partition transforms per the Apache Iceberg v2 specification.
 * Supports identity, bucket, truncate, temporal, and void transforms.
 *
 * @see https://iceberg.apache.org/spec/#partitioning
 * @see https://iceberg.apache.org/spec/#partition-transforms
 */
import type { IcebergSchema, PartitionSpec, PartitionTransform } from './types.js';
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
    byField: Record<string, {
        distinctValues: number;
        minValue?: unknown;
        maxValue?: unknown;
    }>;
}
/**
 * Parse a transform string (e.g., "bucket[16]", "truncate[5]", "identity")
 * into a ParsedTransform object.
 */
export declare function parseTransform(transform: string): ParsedTransform;
/**
 * Format a transform for serialization (e.g., { type: 'bucket', arg: 16 } -> "bucket[16]")
 */
export declare function formatTransform(parsed: ParsedTransform): string;
/**
 * Apply a partition transform to a value.
 * Returns the transformed partition value.
 */
export declare function applyTransform(value: unknown, transform: PartitionTransform | string, transformArg?: number): unknown;
/**
 * Get the result type of a transform applied to a source type.
 * Per Iceberg spec, transforms produce specific result types.
 */
export declare function getTransformResultType(_sourceType: string, transform: PartitionTransform | string): string;
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
export declare class PartitionSpecBuilder {
    private readonly schema;
    private readonly fields;
    private readonly specId;
    private nextFieldId;
    constructor(schema: IcebergSchema, options?: CreatePartitionSpecOptions);
    /**
     * Add an identity partition field.
     * Values are partitioned exactly as they appear.
     */
    identity(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add a bucket partition field.
     * Values are hashed into N buckets.
     */
    bucket(sourceFieldName: string, numBuckets: number, partitionName?: string): this;
    /**
     * Add a truncate partition field.
     * Values are truncated to width W.
     */
    truncate(sourceFieldName: string, width: number, partitionName?: string): this;
    /**
     * Add a year partition field.
     * Extracts years since epoch from timestamp/date.
     */
    year(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add a month partition field.
     * Extracts months since epoch from timestamp/date.
     */
    month(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add a day partition field.
     * Extracts days since epoch from timestamp/date.
     */
    day(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add an hour partition field.
     * Extracts hours since epoch from timestamp.
     */
    hour(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add a void partition field.
     * Always produces null (useful for partition evolution).
     */
    void(sourceFieldName: string, partitionName?: string): this;
    /**
     * Add a partition field from a definition object.
     */
    addFieldFromDefinition(definition: PartitionFieldDefinition): this;
    /**
     * Get the current number of fields.
     */
    get fieldCount(): number;
    /**
     * Build the partition specification.
     */
    build(): PartitionSpec;
    /**
     * Internal method to add a partition field.
     */
    private addField;
    /**
     * Find a field in the schema by name.
     */
    private findSchemaField;
    /**
     * Generate a default partition field name.
     */
    private generatePartitionName;
}
/**
 * Get partition data for a record based on a partition spec.
 */
export declare function getPartitionData(record: Record<string, unknown>, spec: PartitionSpec, schema: IcebergSchema): Record<string, unknown>;
/**
 * Generate the partition path for partition data (e.g., "year=54/month=653/day=19750").
 * Uses Iceberg's Hive-style partition path format.
 */
export declare function getPartitionPath(partitionData: Record<string, unknown>, spec: PartitionSpec): string;
/**
 * Parse a partition path back to partition data.
 */
export declare function parsePartitionPath(path: string): Record<string, unknown>;
/**
 * Collects and aggregates statistics across partitions.
 */
export declare class PartitionStatsCollector {
    private readonly spec;
    private readonly partitionMap;
    constructor(spec: PartitionSpec);
    /**
     * Add a data file to the statistics.
     */
    addFile(file: PartitionedFile): void;
    /**
     * Remove a data file from the statistics.
     */
    removeFile(file: PartitionedFile): void;
    /**
     * Get aggregate statistics.
     */
    getStats(): PartitionStatsAggregate;
    /**
     * Get statistics for a specific partition.
     */
    getPartitionStats(partitionData: Record<string, unknown>): PartitionStats | undefined;
    /**
     * Get all partition keys.
     */
    getPartitionKeys(): string[];
    /**
     * Clear all statistics.
     */
    clear(): void;
    /**
     * Generate a stable key for partition values.
     */
    private getPartitionKey;
    /**
     * Compare two values for ordering.
     */
    private isLessThan;
}
/** Types of partition spec changes */
export type PartitionSpecChangeType = 'add-field' | 'remove-field' | 'rename-field' | 'change-transform';
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
export declare function comparePartitionSpecs(oldSpec: PartitionSpec, newSpec: PartitionSpec): PartitionSpecComparisonResult;
/**
 * Find the maximum partition field ID in a spec.
 */
export declare function findMaxPartitionFieldId(spec: PartitionSpec): number;
/**
 * Generate a new partition spec ID based on existing specs.
 */
export declare function generatePartitionSpecId(existingSpecs: PartitionSpec[]): number;
/**
 * Create a partition spec builder.
 */
export declare function createPartitionSpecBuilder(schema: IcebergSchema, options?: CreatePartitionSpecOptions): PartitionSpecBuilder;
/**
 * Create a partition spec from field definitions.
 */
export declare function createPartitionSpecFromDefinitions(schema: IcebergSchema, fields: PartitionFieldDefinition[], options?: CreatePartitionSpecOptions): PartitionSpec;
/**
 * Create a partition stats collector.
 */
export declare function createPartitionStatsCollector(spec: PartitionSpec): PartitionStatsCollector;
//# sourceMappingURL=partition.d.ts.map
/**
 * Iceberg Metadata Writer
 *
 * Generates and writes Iceberg metadata.json files to storage.
 * Follows the Apache Iceberg v2 specification.
 *
 * @see https://iceberg.apache.org/spec/
 */

import type {
  StorageBackend,
  IcebergSchema,
  PartitionSpec,
  SortOrder,
  TableMetadata,
  Snapshot,
  EncryptionKey,
} from './types.js';
import {
  createDefaultSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
} from './schema.js';
import { generateUUID } from './snapshot.js';
import {
  FORMAT_VERSION,
  METADATA_DIR,
  VERSION_HINT_FILENAME,
  INITIAL_PARTITION_ID,
} from './constants.js';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Options for creating table metadata.
 */
export interface MetadataWriterOptions {
  /** Table location (base path for data and metadata files) */
  location: string;
  /** Table UUID (auto-generated if not provided) */
  tableUuid?: string;
  /** Table schema (uses default if not provided) */
  schema?: IcebergSchema;
  /** Partition specification (unpartitioned if not provided) */
  partitionSpec?: PartitionSpec;
  /** Sort order (unsorted if not provided) */
  sortOrder?: SortOrder;
  /** Table properties */
  properties?: Record<string, string>;
  /** Format version (2 or 3). Defaults to 2 for backward compatibility. */
  formatVersion?: 2 | 3;
  /** Initial next-row-id value (v3 only). Defaults to 0 for v3 tables. */
  nextRowId?: number;
  /** Optional encryption keys for table encryption. */
  encryptionKeys?: EncryptionKey[];
}

/**
 * Result of a metadata write operation.
 */
export interface MetadataWriteResult {
  /** Path where the metadata file was written */
  metadataLocation: string;
  /** Version number of the metadata file */
  version: number;
  /** The complete table metadata that was written */
  metadata: TableMetadata;
}

// ============================================================================
// MetadataWriter Class
// ============================================================================

/**
 * Writes Iceberg metadata.json files to storage.
 *
 * The MetadataWriter generates complete Iceberg v2 metadata files including:
 * - format-version
 * - table-uuid
 * - location
 * - last-sequence-number
 * - last-updated-ms
 * - last-column-id
 * - schemas
 * - current-schema-id
 * - partition-specs
 * - default-spec-id
 * - last-partition-id
 * - properties
 * - snapshots
 * - current-snapshot-id
 * - snapshot-log
 *
 * @example
 * ```ts
 * const storage = createStorage({ type: 'filesystem', basePath: '.data' });
 * const writer = new MetadataWriter(storage);
 *
 * // Create new table metadata
 * const result = await writer.writeNewTable({
 *   location: 's3://bucket/warehouse/db/table',
 *   properties: { 'app.collection': 'users' },
 * });
 *
 * // Update existing metadata with a new snapshot
 * const updated = await writer.writeWithSnapshot(
 *   result.metadata,
 *   snapshot,
 *   previousMetadataLocation
 * );
 * ```
 */
export class MetadataWriter {
  private readonly storage: StorageBackend;

  constructor(storage: StorageBackend) {
    this.storage = storage;
  }

  /**
   * Create a new, empty table metadata object.
   *
   * This creates valid Iceberg v2 metadata with:
   * - format-version: 2
   * - A unique table-uuid
   * - Default schema (if not provided)
   * - Unpartitioned partition spec
   * - Unsorted sort order
   * - Empty snapshots list
   * - null current-snapshot-id
   */
  createTableMetadata(options: MetadataWriterOptions): TableMetadata {
    const schema = options.schema ?? createDefaultSchema();
    const partitionSpec = options.partitionSpec ?? createUnpartitionedSpec();
    const sortOrder = options.sortOrder ?? createUnsortedOrder();
    const formatVersion = options.formatVersion ?? FORMAT_VERSION;

    // Find the highest field ID in the schema
    const lastColumnId = this.findMaxFieldId(schema);
    // Find the highest partition field ID
    const lastPartitionId = this.findMaxPartitionFieldId(partitionSpec);

    // Build base metadata
    const baseMetadata = {
      'format-version': formatVersion as 2 | 3,
      'table-uuid': options.tableUuid ?? generateUUID(),
      location: options.location,
      'last-sequence-number': 0,
      'last-updated-ms': Date.now(),
      'last-column-id': lastColumnId,
      'current-schema-id': schema['schema-id'],
      schemas: [schema],
      'default-spec-id': partitionSpec['spec-id'],
      'partition-specs': [partitionSpec],
      'last-partition-id': lastPartitionId,
      'default-sort-order-id': sortOrder['order-id'],
      'sort-orders': [sortOrder],
      properties: options.properties ?? {},
      'current-snapshot-id': null,
      snapshots: [],
      'snapshot-log': [],
      'metadata-log': [],
      refs: {},
      // Add encryption keys if provided
      ...(options.encryptionKeys && options.encryptionKeys.length > 0
        ? { 'encryption-keys': options.encryptionKeys }
        : {}),
    };

    // Add next-row-id for v3 tables (required for row lineage)
    if (formatVersion === 3) {
      return {
        ...baseMetadata,
        'next-row-id': options.nextRowId ?? 0,
      } as TableMetadata;
    }

    return baseMetadata as TableMetadata;
  }

  /**
   * Write new table metadata to storage.
   *
   * Creates a new metadata file at `{location}/metadata/v1.metadata.json`
   * and a version hint file at `{location}/metadata/version-hint.text`.
   *
   * @throws Error if metadata already exists at the location
   */
  async writeNewTable(options: MetadataWriterOptions): Promise<MetadataWriteResult> {
    const metadataDir = `${options.location}/${METADATA_DIR}`;
    const version = 1;
    const metadataPath = `${metadataDir}/v${version}.metadata.json`;

    // Check if table already exists
    const exists = await this.storage.exists(metadataPath);
    if (exists) {
      throw new Error(`Table already exists at ${options.location}`);
    }

    // Create metadata
    const metadata = this.createTableMetadata(options);

    // Write metadata file
    await this.storage.put(
      metadataPath,
      new TextEncoder().encode(JSON.stringify(metadata, null, 2))
    );

    // Write version hint
    await this.storage.put(
      `${metadataDir}/${VERSION_HINT_FILENAME}`,
      new TextEncoder().encode(String(version))
    );

    return {
      metadataLocation: metadataPath,
      version,
      metadata,
    };
  }

  /**
   * Write table metadata with an updated snapshot.
   *
   * This:
   * 1. Adds the snapshot to the snapshots list
   * 2. Updates current-snapshot-id
   * 3. Updates last-sequence-number
   * 4. Updates last-updated-ms
   * 5. Updates snapshot-log
   * 6. Updates refs (main branch)
   * 7. Optionally adds previous metadata to metadata-log
   * 8. Writes a new versioned metadata file
   *
   * @param currentMetadata - The current table metadata
   * @param snapshot - The new snapshot to add
   * @param previousMetadataLocation - Optional path to previous metadata file (for metadata-log)
   */
  async writeWithSnapshot(
    currentMetadata: TableMetadata,
    snapshot: Snapshot,
    previousMetadataLocation?: string
  ): Promise<MetadataWriteResult> {
    // Create updated metadata
    const updatedMetadata = this.addSnapshotToMetadata(
      currentMetadata,
      snapshot,
      previousMetadataLocation
    );

    // Determine next version number
    const metadataDir = `${currentMetadata.location}/${METADATA_DIR}`;
    const version = await this.getNextVersion(metadataDir);
    const metadataPath = `${metadataDir}/v${version}.metadata.json`;

    // Write metadata file
    await this.storage.put(
      metadataPath,
      new TextEncoder().encode(JSON.stringify(updatedMetadata, null, 2))
    );

    // Update version hint
    await this.storage.put(
      `${metadataDir}/${VERSION_HINT_FILENAME}`,
      new TextEncoder().encode(String(version))
    );

    return {
      metadataLocation: metadataPath,
      version,
      metadata: updatedMetadata,
    };
  }

  /**
   * Write metadata if it doesn't already exist.
   *
   * This is an idempotent operation - if metadata already exists,
   * it returns the existing metadata without modification.
   *
   * @returns The existing or newly created metadata
   */
  async writeIfMissing(options: MetadataWriterOptions): Promise<MetadataWriteResult> {
    const metadataDir = `${options.location}/${METADATA_DIR}`;
    const versionHintPath = `${metadataDir}/${VERSION_HINT_FILENAME}`;

    // Check for existing metadata
    const versionHintData = await this.storage.get(versionHintPath);

    if (versionHintData) {
      // Metadata exists, load and return it
      const version = parseInt(new TextDecoder().decode(versionHintData).trim(), 10);
      const metadataPath = `${metadataDir}/v${version}.metadata.json`;
      const metadataData = await this.storage.get(metadataPath);

      if (metadataData) {
        const metadata = JSON.parse(new TextDecoder().decode(metadataData)) as TableMetadata;
        return {
          metadataLocation: metadataPath,
          version,
          metadata,
        };
      }
    }

    // Metadata doesn't exist, create it
    return this.writeNewTable(options);
  }

  /**
   * Serialize table metadata to JSON string.
   *
   * This produces a properly formatted metadata.json file content
   * that can be read by any Iceberg-compatible query engine.
   */
  serializeMetadata(metadata: TableMetadata): string {
    return JSON.stringify(metadata, null, 2);
  }

  /**
   * Validate that metadata contains all required fields.
   *
   * @throws Error if validation fails
   */
  validateMetadata(metadata: TableMetadata): void {
    // Check format version (accept 2 or 3)
    if (metadata['format-version'] !== 2 && metadata['format-version'] !== 3) {
      throw new Error(`Invalid format-version: expected 2 or 3, got ${metadata['format-version']}`);
    }

    // Check required string fields
    if (!metadata['table-uuid']) {
      throw new Error('Missing required field: table-uuid');
    }
    if (!metadata.location) {
      throw new Error('Missing required field: location');
    }

    // Check required number fields
    if (typeof metadata['last-sequence-number'] !== 'number') {
      throw new Error('Missing or invalid field: last-sequence-number');
    }
    if (typeof metadata['last-updated-ms'] !== 'number') {
      throw new Error('Missing or invalid field: last-updated-ms');
    }
    if (typeof metadata['last-column-id'] !== 'number') {
      throw new Error('Missing or invalid field: last-column-id');
    }
    if (typeof metadata['current-schema-id'] !== 'number') {
      throw new Error('Missing or invalid field: current-schema-id');
    }
    if (typeof metadata['default-spec-id'] !== 'number') {
      throw new Error('Missing or invalid field: default-spec-id');
    }
    if (typeof metadata['last-partition-id'] !== 'number') {
      throw new Error('Missing or invalid field: last-partition-id');
    }

    // Check required arrays
    if (!Array.isArray(metadata.schemas) || metadata.schemas.length === 0) {
      throw new Error('Missing or empty field: schemas');
    }
    if (!Array.isArray(metadata['partition-specs']) || metadata['partition-specs'].length === 0) {
      throw new Error('Missing or empty field: partition-specs');
    }
    if (!Array.isArray(metadata['sort-orders']) || metadata['sort-orders'].length === 0) {
      throw new Error('Missing or empty field: sort-orders');
    }
    if (!Array.isArray(metadata.snapshots)) {
      throw new Error('Missing field: snapshots');
    }
    if (!Array.isArray(metadata['snapshot-log'])) {
      throw new Error('Missing field: snapshot-log');
    }

    // Check required objects
    if (typeof metadata.properties !== 'object' || metadata.properties === null) {
      throw new Error('Missing or invalid field: properties');
    }

    // Validate current-snapshot-id consistency
    if (metadata['current-snapshot-id'] !== null) {
      const currentSnapshot = metadata.snapshots.find(
        (s) => s['snapshot-id'] === metadata['current-snapshot-id']
      );
      if (!currentSnapshot) {
        throw new Error(
          `current-snapshot-id ${metadata['current-snapshot-id']} not found in snapshots`
        );
      }
    }

    // Validate schema references
    const schemaIds = new Set(metadata.schemas.map((s) => s['schema-id']));
    if (!schemaIds.has(metadata['current-schema-id'])) {
      throw new Error(
        `current-schema-id ${metadata['current-schema-id']} not found in schemas`
      );
    }

    // Validate next-row-id for v3 tables (row lineage)
    if (metadata['format-version'] === 3) {
      if (typeof metadata['next-row-id'] !== 'number') {
        throw new Error('Missing or invalid field: next-row-id (required for format-version 3)');
      }
      if (metadata['next-row-id'] < 0) {
        throw new Error('Invalid next-row-id: must be non-negative');
      }

      // Validate snapshot row lineage fields for v3 tables
      // Note: When a table is upgraded from v2 to v3, existing snapshots may not have
      // row lineage fields (first-row-id, added-rows). These fields are optional for
      // backward compatibility with upgraded tables. However, if present, they must be
      // valid (non-negative numbers).
      for (const snapshot of metadata.snapshots) {
        // first-row-id is optional for backward compatibility with upgraded v2 snapshots
        if (snapshot['first-row-id'] !== undefined) {
          if (typeof snapshot['first-row-id'] !== 'number') {
            throw new Error(
              `Invalid field: first-row-id in snapshot ${snapshot['snapshot-id']} must be a number`
            );
          }
          if (snapshot['first-row-id'] < 0) {
            throw new Error(
              `Invalid first-row-id in snapshot ${snapshot['snapshot-id']}: must be non-negative`
            );
          }
        }

        // added-rows is optional for backward compatibility with upgraded v2 snapshots
        if (snapshot['added-rows'] !== undefined) {
          if (typeof snapshot['added-rows'] !== 'number') {
            throw new Error(
              `Invalid field: added-rows in snapshot ${snapshot['snapshot-id']} must be a number`
            );
          }
          if (snapshot['added-rows'] < 0) {
            throw new Error(
              `Invalid added-rows in snapshot ${snapshot['snapshot-id']}: must be non-negative`
            );
          }
        }
      }
    }

    // Validate partition spec references
    const specIds = new Set(metadata['partition-specs'].map((s) => s['spec-id']));
    if (!specIds.has(metadata['default-spec-id'])) {
      throw new Error(
        `default-spec-id ${metadata['default-spec-id']} not found in partition-specs`
      );
    }

    // Validate encryption keys if present
    if (metadata['encryption-keys']) {
      const keyIds = new Set<number>();
      for (const key of metadata['encryption-keys']) {
        // Validate key-id is a number
        if (typeof key['key-id'] !== 'number') {
          throw new Error('Invalid encryption key: key-id must be a number');
        }
        // Validate key-metadata is a string
        if (typeof key['key-metadata'] !== 'string') {
          throw new Error('Invalid encryption key: key-metadata must be a string');
        }
        // Check for duplicate key-ids
        if (keyIds.has(key['key-id'])) {
          throw new Error(`Duplicate encryption key-id: ${key['key-id']}. Each key-id must be unique.`);
        }
        keyIds.add(key['key-id']);
      }
    }
  }

  // ============================================================================
  // Private Helper Methods
  // ============================================================================

  /**
   * Find the maximum field ID in a schema.
   */
  private findMaxFieldId(schema: IcebergSchema): number {
    let maxId = 0;

    const traverse = (fields: readonly { id: number; type: unknown }[]): void => {
      for (const field of fields) {
        maxId = Math.max(maxId, field.id);
        if (typeof field.type === 'object' && field.type !== null) {
          const complexType = field.type as {
            type: string;
            fields?: readonly { id: number; type: unknown }[];
            'element-id'?: number;
            'key-id'?: number;
            'value-id'?: number;
          };
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
    };

    traverse(schema.fields);
    return maxId;
  }

  /**
   * Find the maximum partition field ID in a partition spec.
   */
  private findMaxPartitionFieldId(spec: PartitionSpec): number {
    if (spec.fields.length === 0) {
      return INITIAL_PARTITION_ID; // Partition field IDs start at PARTITION_FIELD_ID_START
    }
    return Math.max(...spec.fields.map((f) => f['field-id']));
  }

  /**
   * Get the next version number for metadata files.
   */
  private async getNextVersion(metadataDir: string): Promise<number> {
    const files = await this.storage.list(metadataDir);
    const versions = files
      .filter((f) => f.match(/v\d+\.metadata\.json$/))
      .map((f) => {
        const match = f.match(/v(\d+)\.metadata\.json$/);
        return match ? parseInt(match[1], 10) : 0;
      });

    return Math.max(0, ...versions) + 1;
  }

  /**
   * Add a snapshot to existing metadata.
   */
  private addSnapshotToMetadata(
    currentMetadata: TableMetadata,
    snapshot: Snapshot,
    previousMetadataLocation?: string
  ): TableMetadata {
    const now = Date.now();

    // Create updated metadata with all fields set immutably
    return {
      ...currentMetadata,
      'last-sequence-number': snapshot['sequence-number'],
      'last-updated-ms': now,
      'current-snapshot-id': snapshot['snapshot-id'],
      snapshots: [...currentMetadata.snapshots, snapshot],
      'snapshot-log': [
        ...currentMetadata['snapshot-log'],
        {
          'timestamp-ms': snapshot['timestamp-ms'],
          'snapshot-id': snapshot['snapshot-id'],
        },
      ],
      refs: {
        ...currentMetadata.refs,
        main: {
          'snapshot-id': snapshot['snapshot-id'],
          type: 'branch',
        },
      },
      // Add previous metadata to metadata-log if provided
      'metadata-log': previousMetadataLocation
        ? [
            ...currentMetadata['metadata-log'],
            {
              'timestamp-ms': currentMetadata['last-updated-ms'],
              'metadata-file': previousMetadataLocation,
            },
          ]
        : currentMetadata['metadata-log'],
    };
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Create and write new table metadata in one call.
 *
 * @example
 * ```ts
 * const storage = createStorage({ type: 'filesystem', basePath: '.data' });
 * const result = await writeNewTableMetadata(storage, {
 *   location: 's3://bucket/warehouse/db/table',
 * });
 * ```
 */
export async function writeNewTableMetadata(
  storage: StorageBackend,
  options: MetadataWriterOptions
): Promise<MetadataWriteResult> {
  const writer = new MetadataWriter(storage);
  return writer.writeNewTable(options);
}

/**
 * Write metadata if it doesn't already exist.
 *
 * This is an idempotent operation useful for ensuring a table exists
 * before writing data to it.
 */
export async function writeMetadataIfMissing(
  storage: StorageBackend,
  options: MetadataWriterOptions
): Promise<MetadataWriteResult> {
  const writer = new MetadataWriter(storage);
  return writer.writeIfMissing(options);
}

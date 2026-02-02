/**
 * Avro Binary Encoding for Iceberg Manifest Files
 *
 * Implements Avro binary encoding according to the Apache Avro specification.
 * Used for serializing Iceberg manifest files and manifest lists.
 *
 * @see https://avro.apache.org/docs/current/specification/
 * @see https://iceberg.apache.org/spec/
 */

// ============================================================================
// Avro Schema Types
// ============================================================================

export type AvroPrimitive = 'null' | 'boolean' | 'int' | 'long' | 'float' | 'double' | 'bytes' | 'string';

export interface AvroArray {
  type: 'array';
  items: AvroType;
}

export interface AvroMap {
  type: 'map';
  values: AvroType;
}

export interface AvroFixed {
  type: 'fixed';
  name: string;
  size: number;
}

export interface AvroEnum {
  type: 'enum';
  name: string;
  symbols: string[];
}

export interface AvroRecordField {
  name: string;
  type: AvroType;
  default?: unknown;
  doc?: string;
  'field-id'?: number;
}

export interface AvroRecord {
  type: 'record';
  name: string;
  namespace?: string;
  doc?: string;
  fields: AvroRecordField[];
}

export type AvroUnion = AvroType[];

export type AvroType =
  | AvroPrimitive
  | AvroArray
  | AvroMap
  | AvroFixed
  | AvroEnum
  | AvroRecord
  | AvroUnion;

// ============================================================================
// Avro Binary Encoder
// ============================================================================

/**
 * Avro binary encoder.
 * Encodes values according to the Avro binary encoding specification.
 */
export class AvroEncoder {
  private buffer: number[] = [];

  /**
   * Write a null value (no bytes written).
   */
  writeNull(): void {
    // Null is encoded as zero bytes
  }

  /**
   * Write a boolean value.
   */
  writeBoolean(value: boolean): void {
    this.buffer.push(value ? 1 : 0);
  }

  /**
   * Write an int (32-bit signed) using variable-length zig-zag encoding.
   */
  writeInt(value: number): void {
    this.writeVarInt(this.zigZagEncode32(value));
  }

  /**
   * Write a long (64-bit signed) using variable-length zig-zag encoding.
   */
  writeLong(value: number | bigint): void {
    const n = typeof value === 'bigint' ? value : BigInt(value);
    this.writeVarLong(this.zigZagEncode64(n));
  }

  /**
   * Write a float (32-bit IEEE 754).
   */
  writeFloat(value: number): void {
    const view = new DataView(new ArrayBuffer(4));
    view.setFloat32(0, value, true); // little-endian
    for (let i = 0; i < 4; i++) {
      this.buffer.push(view.getUint8(i));
    }
  }

  /**
   * Write a double (64-bit IEEE 754).
   */
  writeDouble(value: number): void {
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, value, true); // little-endian
    for (let i = 0; i < 8; i++) {
      this.buffer.push(view.getUint8(i));
    }
  }

  /**
   * Write bytes (length-prefixed).
   */
  writeBytes(value: Uint8Array): void {
    this.writeLong(value.length);
    for (const b of value) {
      this.buffer.push(b);
    }
  }

  /**
   * Write a string (UTF-8 encoded, length-prefixed).
   */
  writeString(value: string): void {
    const encoded = new TextEncoder().encode(value);
    this.writeLong(encoded.length);
    for (const b of encoded) {
      this.buffer.push(b);
    }
  }

  /**
   * Write a fixed-length byte array.
   */
  writeFixed(value: Uint8Array, size: number): void {
    if (value.length !== size) {
      throw new Error(`Fixed value must be exactly ${size} bytes, got ${value.length}`);
    }
    for (const b of value) {
      this.buffer.push(b);
    }
  }

  /**
   * Write an enum value (as its ordinal index).
   */
  writeEnum(index: number): void {
    this.writeInt(index);
  }

  /**
   * Write the union index for a union type.
   */
  writeUnionIndex(index: number): void {
    this.writeLong(index);
  }

  /**
   * Write an array of values.
   * Arrays are encoded as a series of blocks, each with count and values.
   */
  writeArray<T>(values: T[], writeElement: (value: T) => void): void {
    if (values.length > 0) {
      this.writeLong(values.length);
      for (const value of values) {
        writeElement(value);
      }
    }
    // Write terminating zero block
    this.writeLong(0);
  }

  /**
   * Write a map of key-value pairs.
   */
  writeMap<V>(map: Map<string, V> | Record<string, V>, writeValue: (value: V) => void): void {
    const entries = map instanceof Map ? Array.from(map.entries()) : Object.entries(map);
    if (entries.length > 0) {
      this.writeLong(entries.length);
      for (const [key, value] of entries) {
        this.writeString(key);
        writeValue(value);
      }
    }
    // Write terminating zero block
    this.writeLong(0);
  }

  /**
   * Get the encoded bytes.
   */
  toBuffer(): Uint8Array {
    return new Uint8Array(this.buffer);
  }

  /**
   * Get the current size of the buffer.
   */
  get size(): number {
    return this.buffer.length;
  }

  /**
   * Append raw bytes to the buffer.
   */
  appendRaw(bytes: Uint8Array): void {
    for (const b of bytes) {
      this.buffer.push(b);
    }
  }

  // Private helpers

  private zigZagEncode32(n: number): number {
    return (n << 1) ^ (n >> 31);
  }

  private zigZagEncode64(n: bigint): bigint {
    return (n << 1n) ^ (n >> 63n);
  }

  private writeVarInt(n: number): void {
    while ((n & ~0x7f) !== 0) {
      this.buffer.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    this.buffer.push(n);
  }

  private writeVarLong(n: bigint): void {
    while ((n & ~0x7fn) !== 0n) {
      this.buffer.push(Number(n & 0x7fn) | 0x80);
      n >>= 7n;
    }
    this.buffer.push(Number(n));
  }
}

// ============================================================================
// Avro Object Container File Writer
// ============================================================================

const AVRO_MAGIC = new Uint8Array([0x4f, 0x62, 0x6a, 0x01]); // "Obj" + version 1
const AVRO_SYNC_SIZE = 16;

/**
 * Avro Object Container File writer.
 * Writes Avro data in the standard container file format with header and sync markers.
 */
export class AvroFileWriter {
  private readonly schema: AvroRecord;
  private readonly metadata: Map<string, string>;
  private readonly syncMarker: Uint8Array;
  private blocks: { count: number; data: Uint8Array }[] = [];

  constructor(schema: AvroRecord, metadata?: Map<string, string> | Record<string, string>) {
    this.schema = schema;
    this.metadata = metadata instanceof Map
      ? metadata
      : new Map(Object.entries(metadata ?? {}));

    // Generate random 16-byte sync marker
    this.syncMarker = crypto.getRandomValues(new Uint8Array(AVRO_SYNC_SIZE));
  }

  /**
   * Add a block of records.
   */
  addBlock(count: number, data: Uint8Array): void {
    this.blocks.push({ count, data });
  }

  /**
   * Generate the complete Avro container file.
   */
  toBuffer(): Uint8Array {
    const encoder = new AvroEncoder();

    // Write magic bytes
    encoder.appendRaw(AVRO_MAGIC);

    // Write file header metadata as a map
    const headerMeta = new Map<string, Uint8Array>();

    // Add schema (required)
    headerMeta.set('avro.schema', new TextEncoder().encode(JSON.stringify(this.schema)));

    // Add codec (required, using null = no compression)
    headerMeta.set('avro.codec', new TextEncoder().encode('null'));

    // Add custom metadata
    for (const [key, value] of this.metadata) {
      headerMeta.set(key, new TextEncoder().encode(value));
    }

    // Write metadata map
    if (headerMeta.size > 0) {
      encoder.writeLong(headerMeta.size);
      for (const [key, value] of headerMeta) {
        encoder.writeString(key);
        encoder.writeBytes(value);
      }
    }
    encoder.writeLong(0); // End of map

    // Write sync marker
    encoder.appendRaw(this.syncMarker);

    // Write data blocks
    for (const block of this.blocks) {
      encoder.writeLong(block.count); // Object count
      encoder.writeLong(block.data.length); // Block byte size
      encoder.appendRaw(block.data); // Block data
      encoder.appendRaw(this.syncMarker); // Sync marker after each block
    }

    return encoder.toBuffer();
  }
}

// ============================================================================
// Iceberg-specific Avro Schemas
// ============================================================================

/**
 * Create the Avro schema for Iceberg manifest entries (v2).
 * The data_file schema is customized based on the partition spec.
 */
export function createManifestEntrySchema(
  partitionFields: Array<{ name: string; type: AvroType; 'field-id': number }>,
  _schemaId: number
): AvroRecord {
  // Create the partition struct schema
  const partitionSchema: AvroRecord = {
    type: 'record',
    name: 'r102',
    fields: partitionFields.map((field) => ({
      name: field.name,
      type: ['null', field.type] as AvroUnion,
      default: null,
      'field-id': field['field-id'],
    })),
  };

  // Create the data_file schema
  const dataFileSchema: AvroRecord = {
    type: 'record',
    name: 'r2',
    fields: [
      { name: 'content', type: 'int', 'field-id': 134, doc: '0: data, 1: position deletes, 2: equality deletes' },
      { name: 'file_path', type: 'string', 'field-id': 100, doc: 'Location URI with FS scheme' },
      { name: 'file_format', type: 'string', 'field-id': 101, doc: 'File format name: avro, orc, or parquet' },
      { name: 'partition', type: partitionSchema, 'field-id': 102 },
      { name: 'record_count', type: 'long', 'field-id': 103, doc: 'Number of records in this file' },
      { name: 'file_size_in_bytes', type: 'long', 'field-id': 104, doc: 'Total file size in bytes' },
      // Column sizes map (field-id -> size in bytes)
      {
        name: 'column_sizes',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k117_v118', fields: [
          { name: 'key', type: 'int', 'field-id': 117 },
          { name: 'value', type: 'long', 'field-id': 118 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 108,
      },
      // Value counts map
      {
        name: 'value_counts',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k119_v120', fields: [
          { name: 'key', type: 'int', 'field-id': 119 },
          { name: 'value', type: 'long', 'field-id': 120 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 109,
      },
      // Null value counts map
      {
        name: 'null_value_counts',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k121_v122', fields: [
          { name: 'key', type: 'int', 'field-id': 121 },
          { name: 'value', type: 'long', 'field-id': 122 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 110,
      },
      // NaN value counts map
      {
        name: 'nan_value_counts',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k138_v139', fields: [
          { name: 'key', type: 'int', 'field-id': 138 },
          { name: 'value', type: 'long', 'field-id': 139 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 137,
      },
      // Lower bounds map (field-id -> binary value)
      {
        name: 'lower_bounds',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k126_v127', fields: [
          { name: 'key', type: 'int', 'field-id': 126 },
          { name: 'value', type: 'bytes', 'field-id': 127 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 125,
      },
      // Upper bounds map (field-id -> binary value)
      {
        name: 'upper_bounds',
        type: ['null', { type: 'array', items: { type: 'record', name: 'k129_v130', fields: [
          { name: 'key', type: 'int', 'field-id': 129 },
          { name: 'value', type: 'bytes', 'field-id': 130 },
        ]}}] as AvroUnion,
        default: null,
        'field-id': 128,
      },
      // Key metadata (encryption)
      {
        name: 'key_metadata',
        type: ['null', 'bytes'] as AvroUnion,
        default: null,
        'field-id': 131,
      },
      // Split offsets (for splitting files)
      {
        name: 'split_offsets',
        type: ['null', { type: 'array', items: 'long' }] as AvroUnion,
        default: null,
        'field-id': 132,
      },
      // Equality field IDs (for equality deletes)
      {
        name: 'equality_ids',
        type: ['null', { type: 'array', items: 'int' }] as AvroUnion,
        default: null,
        'field-id': 135,
      },
      // Sort order ID
      {
        name: 'sort_order_id',
        type: ['null', 'int'] as AvroUnion,
        default: null,
        'field-id': 140,
      },
    ],
  };

  // Create the manifest entry schema
  return {
    type: 'record',
    name: 'manifest_entry',
    fields: [
      { name: 'status', type: 'int', 'field-id': 0, doc: '0: existing, 1: added, 2: deleted' },
      { name: 'snapshot_id', type: ['null', 'long'] as AvroUnion, default: null, 'field-id': 1 },
      { name: 'sequence_number', type: ['null', 'long'] as AvroUnion, default: null, 'field-id': 3 },
      { name: 'file_sequence_number', type: ['null', 'long'] as AvroUnion, default: null, 'field-id': 4 },
      { name: 'data_file', type: dataFileSchema, 'field-id': 2 },
    ],
  };
}

/**
 * Create the Avro schema for manifest list entries.
 */
export function createManifestListSchema(): AvroRecord {
  return {
    type: 'record',
    name: 'manifest_file',
    fields: [
      { name: 'manifest_path', type: 'string', 'field-id': 500, doc: 'Location of the manifest file' },
      { name: 'manifest_length', type: 'long', 'field-id': 501, doc: 'Length of the manifest file in bytes' },
      { name: 'partition_spec_id', type: 'int', 'field-id': 502, doc: 'ID of partition spec used to write the manifest' },
      { name: 'content', type: 'int', 'field-id': 517, doc: '0: data, 1: deletes' },
      { name: 'sequence_number', type: 'long', 'field-id': 515, doc: 'Sequence number when manifest was added' },
      { name: 'min_sequence_number', type: 'long', 'field-id': 516, doc: 'Minimum sequence number of entries in manifest' },
      { name: 'added_snapshot_id', type: 'long', 'field-id': 503, doc: 'Snapshot ID that added the manifest' },
      { name: 'added_files_count', type: 'int', 'field-id': 504, doc: 'Number of entries with status 1' },
      { name: 'existing_files_count', type: 'int', 'field-id': 505, doc: 'Number of entries with status 0' },
      { name: 'deleted_files_count', type: 'int', 'field-id': 506, doc: 'Number of entries with status 2' },
      { name: 'added_rows_count', type: 'long', 'field-id': 512, doc: 'Total rows in entries with status 1' },
      { name: 'existing_rows_count', type: 'long', 'field-id': 513, doc: 'Total rows in entries with status 0' },
      { name: 'deleted_rows_count', type: 'long', 'field-id': 514, doc: 'Total rows in entries with status 2' },
      // Partition summaries (optional)
      {
        name: 'partitions',
        type: ['null', { type: 'array', items: {
          type: 'record',
          name: 'r508',
          fields: [
            { name: 'contains_null', type: 'boolean', 'field-id': 509 },
            { name: 'contains_nan', type: ['null', 'boolean'] as AvroUnion, default: null, 'field-id': 518 },
            { name: 'lower_bound', type: ['null', 'bytes'] as AvroUnion, default: null, 'field-id': 510 },
            { name: 'upper_bound', type: ['null', 'bytes'] as AvroUnion, default: null, 'field-id': 511 },
          ],
        }}] as AvroUnion,
        default: null,
        'field-id': 507,
      },
    ],
  };
}

// ============================================================================
// Statistics Encoding Helpers
// ============================================================================

/**
 * Encode a value to binary format for use in lower_bounds/upper_bounds.
 * Follows Iceberg's single-value serialization spec.
 */
export function encodeStatValue(value: unknown, type: string): Uint8Array {
  if (value === null || value === undefined) {
    return new Uint8Array(0);
  }

  switch (type) {
    case 'boolean': {
      return new Uint8Array([value ? 1 : 0]);
    }
    case 'int': {
      const view = new DataView(new ArrayBuffer(4));
      view.setInt32(0, value as number, true); // little-endian
      return new Uint8Array(view.buffer);
    }
    case 'long':
    case 'timestamp':
    case 'timestamptz': {
      const view = new DataView(new ArrayBuffer(8));
      const n = typeof value === 'bigint' ? value : BigInt(value as number);
      view.setBigInt64(0, n, true); // little-endian
      return new Uint8Array(view.buffer);
    }
    case 'float': {
      const view = new DataView(new ArrayBuffer(4));
      view.setFloat32(0, value as number, true);
      return new Uint8Array(view.buffer);
    }
    case 'double': {
      const view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, value as number, true);
      return new Uint8Array(view.buffer);
    }
    case 'string': {
      return new TextEncoder().encode(value as string);
    }
    case 'binary': {
      return value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer);
    }
    case 'date': {
      // Days since epoch
      const view = new DataView(new ArrayBuffer(4));
      view.setInt32(0, value as number, true);
      return new Uint8Array(view.buffer);
    }
    default: {
      // For complex types, encode as string
      return new TextEncoder().encode(String(value));
    }
  }
}

/**
 * Truncate a string value for statistics.
 * Iceberg uses truncated strings for min/max bounds to save space.
 */
export function truncateString(value: string, length: number = 16): string {
  if (value.length <= length) {
    return value;
  }
  return value.slice(0, length);
}

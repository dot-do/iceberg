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
      // v3 fields
      // First row ID (v3 row lineage)
      {
        name: 'first_row_id',
        type: ['null', 'long'] as AvroUnion,
        default: null,
        'field-id': 142,
        doc: 'First row ID assigned to rows in this data file (v3)',
      },
      // Referenced data file (v3 deletion vectors)
      {
        name: 'referenced_data_file',
        type: ['null', 'string'] as AvroUnion,
        default: null,
        'field-id': 143,
        doc: 'Path to referenced data file for deletion vectors (v3)',
      },
      // Content offset (v3 deletion vectors)
      {
        name: 'content_offset',
        type: ['null', 'long'] as AvroUnion,
        default: null,
        'field-id': 144,
        doc: 'Byte offset in Puffin file for deletion vector blob (v3)',
      },
      // Content size in bytes (v3 deletion vectors)
      {
        name: 'content_size_in_bytes',
        type: ['null', 'long'] as AvroUnion,
        default: null,
        'field-id': 145,
        doc: 'Size of deletion vector blob in bytes (v3)',
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
      // First row ID (v3 row lineage)
      {
        name: 'first_row_id',
        type: ['null', 'long'] as AvroUnion,
        default: null,
        'field-id': 519,
        doc: 'First row ID assigned to data files in this manifest (v3)',
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

// ============================================================================
// Avro Binary Decoder
// ============================================================================

/**
 * Avro binary decoder.
 * Decodes values according to the Avro binary encoding specification.
 */
export class AvroDecoder {
  private buffer: Uint8Array;
  private pos: number = 0;

  constructor(buffer: Uint8Array) {
    this.buffer = buffer;
  }

  /**
   * Read a null value (no bytes read).
   */
  readNull(): null {
    return null;
  }

  /**
   * Read a boolean value.
   */
  readBoolean(): boolean {
    return this.buffer[this.pos++] !== 0;
  }

  /**
   * Read an int (32-bit signed) using variable-length zig-zag encoding.
   */
  readInt(): number {
    const n = this.readVarInt();
    return this.zigZagDecode32(n);
  }

  /**
   * Read a long (64-bit signed) using variable-length zig-zag encoding.
   */
  readLong(): number {
    const n = this.readVarLong();
    return Number(this.zigZagDecode64(n));
  }

  /**
   * Read a float (32-bit IEEE 754).
   */
  readFloat(): number {
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.pos, 4);
    this.pos += 4;
    return view.getFloat32(0, true); // little-endian
  }

  /**
   * Read a double (64-bit IEEE 754).
   */
  readDouble(): number {
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.pos, 8);
    this.pos += 8;
    return view.getFloat64(0, true); // little-endian
  }

  /**
   * Read bytes (length-prefixed).
   */
  readBytes(): Uint8Array {
    const length = this.readLong();
    const bytes = this.buffer.slice(this.pos, this.pos + length);
    this.pos += length;
    return bytes;
  }

  /**
   * Read a string (UTF-8 encoded, length-prefixed).
   */
  readString(): string {
    const bytes = this.readBytes();
    return new TextDecoder().decode(bytes);
  }

  /**
   * Read a fixed-length byte array.
   */
  readFixed(size: number): Uint8Array {
    const bytes = this.buffer.slice(this.pos, this.pos + size);
    this.pos += size;
    return bytes;
  }

  /**
   * Read an enum value (as its ordinal index).
   */
  readEnum(): number {
    return this.readInt();
  }

  /**
   * Read the union index for a union type.
   */
  readUnionIndex(): number {
    return this.readLong();
  }

  /**
   * Read an array of values.
   */
  readArray<T>(readElement: () => T): T[] {
    const result: T[] = [];
    let blockCount = this.readLong();
    while (blockCount !== 0) {
      if (blockCount < 0) {
        // Negative count means block has size prefix (skip it)
        blockCount = -blockCount;
        this.readLong(); // skip block size
      }
      for (let i = 0; i < blockCount; i++) {
        result.push(readElement());
      }
      blockCount = this.readLong();
    }
    return result;
  }

  /**
   * Read a map of key-value pairs.
   */
  readMap<V>(readValue: () => V): Map<string, V> {
    const result = new Map<string, V>();
    let blockCount = this.readLong();
    while (blockCount !== 0) {
      if (blockCount < 0) {
        blockCount = -blockCount;
        this.readLong(); // skip block size
      }
      for (let i = 0; i < blockCount; i++) {
        const key = this.readString();
        const value = readValue();
        result.set(key, value);
      }
      blockCount = this.readLong();
    }
    return result;
  }

  /**
   * Get current position in buffer.
   */
  get position(): number {
    return this.pos;
  }

  /**
   * Check if there are more bytes to read.
   */
  hasMore(): boolean {
    return this.pos < this.buffer.length;
  }

  // Private helpers

  private zigZagDecode32(n: number): number {
    return (n >>> 1) ^ -(n & 1);
  }

  private zigZagDecode64(n: bigint): bigint {
    return (n >> 1n) ^ -(n & 1n);
  }

  private readVarInt(): number {
    let n = 0;
    let shift = 0;
    let b: number;
    do {
      b = this.buffer[this.pos++];
      n |= (b & 0x7f) << shift;
      shift += 7;
    } while ((b & 0x80) !== 0);
    return n;
  }

  private readVarLong(): bigint {
    let n = 0n;
    let shift = 0n;
    let b: number;
    do {
      b = this.buffer[this.pos++];
      n |= BigInt(b & 0x7f) << shift;
      shift += 7n;
    } while ((b & 0x80) !== 0);
    return n;
  }
}

// ============================================================================
// Data File Encoding/Decoding
// ============================================================================

/** Data file structure for encoding/decoding */
export interface EncodableDataFile {
  content: number;
  file_path: string;
  file_format: string;
  partition: Record<string, unknown>;
  record_count: number;
  file_size_in_bytes: number;
  column_sizes?: Array<{ key: number; value: number }> | null;
  value_counts?: Array<{ key: number; value: number }> | null;
  null_value_counts?: Array<{ key: number; value: number }> | null;
  nan_value_counts?: Array<{ key: number; value: number }> | null;
  lower_bounds?: Array<{ key: number; value: Uint8Array }> | null;
  upper_bounds?: Array<{ key: number; value: Uint8Array }> | null;
  key_metadata?: Uint8Array | null;
  split_offsets?: number[] | null;
  equality_ids?: number[] | null;
  sort_order_id?: number | null;
  // v3 fields
  first_row_id?: number | null;
  referenced_data_file?: string | null;
  content_offset?: number | null;
  content_size_in_bytes?: number | null;
}

/** Partition field definition */
export interface PartitionFieldDef {
  name: string;
  type: string;
}

/**
 * Encode a data file to Avro binary format.
 */
export function encodeDataFile(
  encoder: AvroEncoder,
  dataFile: EncodableDataFile,
  partitionFields: PartitionFieldDef[]
): void {
  // content (int)
  encoder.writeInt(dataFile.content);
  // file_path (string)
  encoder.writeString(dataFile.file_path);
  // file_format (string)
  encoder.writeString(dataFile.file_format);
  // partition (record)
  for (const field of partitionFields) {
    const value = dataFile.partition[field.name];
    if (value === null || value === undefined) {
      encoder.writeUnionIndex(0); // null
    } else {
      encoder.writeUnionIndex(1); // non-null
      writePartitionValue(encoder, value, field.type);
    }
  }
  // record_count (long)
  encoder.writeLong(dataFile.record_count);
  // file_size_in_bytes (long)
  encoder.writeLong(dataFile.file_size_in_bytes);
  // column_sizes (optional map as array)
  writeOptionalKeyValueArray(encoder, dataFile.column_sizes, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeLong(kv.value);
  });
  // value_counts
  writeOptionalKeyValueArray(encoder, dataFile.value_counts, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeLong(kv.value);
  });
  // null_value_counts
  writeOptionalKeyValueArray(encoder, dataFile.null_value_counts, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeLong(kv.value);
  });
  // nan_value_counts
  writeOptionalKeyValueArray(encoder, dataFile.nan_value_counts, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeLong(kv.value);
  });
  // lower_bounds
  writeOptionalKeyValueArray(encoder, dataFile.lower_bounds, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeBytes(kv.value);
  });
  // upper_bounds
  writeOptionalKeyValueArray(encoder, dataFile.upper_bounds, (kv) => {
    encoder.writeInt(kv.key);
    encoder.writeBytes(kv.value);
  });
  // key_metadata
  writeOptionalBytes(encoder, dataFile.key_metadata);
  // split_offsets
  writeOptionalLongArray(encoder, dataFile.split_offsets);
  // equality_ids
  writeOptionalIntArray(encoder, dataFile.equality_ids);
  // sort_order_id
  writeOptionalInt(encoder, dataFile.sort_order_id);
  // v3 fields
  // first_row_id
  writeOptionalLong(encoder, dataFile.first_row_id);
  // referenced_data_file
  writeOptionalString(encoder, dataFile.referenced_data_file);
  // content_offset
  writeOptionalLong(encoder, dataFile.content_offset);
  // content_size_in_bytes
  writeOptionalLong(encoder, dataFile.content_size_in_bytes);
}

/**
 * Decode a data file from Avro binary format.
 */
export function decodeDataFile(
  decoder: AvroDecoder,
  partitionFields: PartitionFieldDef[]
): EncodableDataFile {
  // content (int)
  const content = decoder.readInt();
  // file_path (string)
  const file_path = decoder.readString();
  // file_format (string)
  const file_format = decoder.readString();
  // partition (record)
  const partition: Record<string, unknown> = {};
  for (const field of partitionFields) {
    const unionIndex = decoder.readUnionIndex();
    if (unionIndex === 0) {
      partition[field.name] = null;
    } else {
      partition[field.name] = readPartitionValue(decoder, field.type);
    }
  }
  // record_count (long)
  const record_count = decoder.readLong();
  // file_size_in_bytes (long)
  const file_size_in_bytes = decoder.readLong();
  // column_sizes
  const column_sizes = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readLong(),
  }));
  // value_counts
  const value_counts = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readLong(),
  }));
  // null_value_counts
  const null_value_counts = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readLong(),
  }));
  // nan_value_counts
  const nan_value_counts = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readLong(),
  }));
  // lower_bounds
  const lower_bounds = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readBytes(),
  }));
  // upper_bounds
  const upper_bounds = readOptionalKeyValueArray(decoder, () => ({
    key: decoder.readInt(),
    value: decoder.readBytes(),
  }));
  // key_metadata
  const key_metadata = readOptionalBytes(decoder);
  // split_offsets
  const split_offsets = readOptionalLongArray(decoder);
  // equality_ids
  const equality_ids = readOptionalIntArray(decoder);
  // sort_order_id
  const sort_order_id = readOptionalInt(decoder);
  // v3 fields
  // first_row_id
  const first_row_id = readOptionalLong(decoder);
  // referenced_data_file
  const referenced_data_file = readOptionalString(decoder);
  // content_offset
  const content_offset = readOptionalLong(decoder);
  // content_size_in_bytes
  const content_size_in_bytes = readOptionalLong(decoder);

  return {
    content,
    file_path,
    file_format,
    partition,
    record_count,
    file_size_in_bytes,
    column_sizes,
    value_counts,
    null_value_counts,
    nan_value_counts,
    lower_bounds,
    upper_bounds,
    key_metadata,
    split_offsets,
    equality_ids,
    sort_order_id,
    first_row_id,
    referenced_data_file,
    content_offset,
    content_size_in_bytes,
  };
}

// ============================================================================
// Manifest Entry Encoding/Decoding
// ============================================================================

/** Manifest entry structure for encoding/decoding */
export interface EncodableManifestEntry {
  status: number;
  snapshot_id: number | null;
  sequence_number: number | null;
  file_sequence_number: number | null;
  data_file: EncodableDataFile;
}

/**
 * Encode a manifest entry to Avro binary format.
 */
export function encodeManifestEntry(
  encoder: AvroEncoder,
  entry: EncodableManifestEntry,
  partitionFields: PartitionFieldDef[]
): void {
  // status (int)
  encoder.writeInt(entry.status);
  // snapshot_id (optional long)
  writeOptionalLong(encoder, entry.snapshot_id);
  // sequence_number (optional long)
  writeOptionalLong(encoder, entry.sequence_number);
  // file_sequence_number (optional long)
  writeOptionalLong(encoder, entry.file_sequence_number);
  // data_file (record)
  encodeDataFile(encoder, entry.data_file, partitionFields);
}

/**
 * Decode a manifest entry from Avro binary format.
 */
export function decodeManifestEntry(
  decoder: AvroDecoder,
  partitionFields: PartitionFieldDef[]
): EncodableManifestEntry {
  // status (int)
  const status = decoder.readInt();
  // snapshot_id (optional long)
  const snapshot_id = readOptionalLong(decoder);
  // sequence_number (optional long)
  const sequence_number = readOptionalLong(decoder);
  // file_sequence_number (optional long)
  const file_sequence_number = readOptionalLong(decoder);
  // data_file (record)
  const data_file = decodeDataFile(decoder, partitionFields);

  return {
    status,
    snapshot_id,
    sequence_number,
    file_sequence_number,
    data_file,
  };
}

// ============================================================================
// Manifest List Entry Encoding/Decoding
// ============================================================================

/** Manifest list entry structure for encoding/decoding */
export interface EncodableManifestListEntry {
  manifest_path: string;
  manifest_length: number;
  partition_spec_id: number;
  content: number;
  sequence_number: number;
  min_sequence_number: number;
  added_snapshot_id: number;
  added_files_count: number;
  existing_files_count: number;
  deleted_files_count: number;
  added_rows_count: number;
  existing_rows_count: number;
  deleted_rows_count: number;
  partitions?: Array<{
    contains_null: boolean;
    contains_nan?: boolean | null;
    lower_bound?: Uint8Array | null;
    upper_bound?: Uint8Array | null;
  }> | null;
  // v3 field
  first_row_id?: number | null;
}

/**
 * Encode a manifest list entry to Avro binary format.
 */
export function encodeManifestListEntry(
  encoder: AvroEncoder,
  entry: EncodableManifestListEntry
): void {
  // manifest_path (string)
  encoder.writeString(entry.manifest_path);
  // manifest_length (long)
  encoder.writeLong(entry.manifest_length);
  // partition_spec_id (int)
  encoder.writeInt(entry.partition_spec_id);
  // content (int)
  encoder.writeInt(entry.content);
  // sequence_number (long)
  encoder.writeLong(entry.sequence_number);
  // min_sequence_number (long)
  encoder.writeLong(entry.min_sequence_number);
  // added_snapshot_id (long)
  encoder.writeLong(entry.added_snapshot_id);
  // added_files_count (int)
  encoder.writeInt(entry.added_files_count);
  // existing_files_count (int)
  encoder.writeInt(entry.existing_files_count);
  // deleted_files_count (int)
  encoder.writeInt(entry.deleted_files_count);
  // added_rows_count (long)
  encoder.writeLong(entry.added_rows_count);
  // existing_rows_count (long)
  encoder.writeLong(entry.existing_rows_count);
  // deleted_rows_count (long)
  encoder.writeLong(entry.deleted_rows_count);
  // partitions (optional array)
  if (entry.partitions === null || entry.partitions === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeArray(entry.partitions, (p) => {
      encoder.writeBoolean(p.contains_null);
      writeOptionalBoolean(encoder, p.contains_nan);
      writeOptionalBytes(encoder, p.lower_bound);
      writeOptionalBytes(encoder, p.upper_bound);
    });
  }
  // first_row_id (v3)
  writeOptionalLong(encoder, entry.first_row_id);
}

/**
 * Decode a manifest list entry from Avro binary format.
 */
export function decodeManifestListEntry(
  decoder: AvroDecoder
): EncodableManifestListEntry {
  // manifest_path (string)
  const manifest_path = decoder.readString();
  // manifest_length (long)
  const manifest_length = decoder.readLong();
  // partition_spec_id (int)
  const partition_spec_id = decoder.readInt();
  // content (int)
  const content = decoder.readInt();
  // sequence_number (long)
  const sequence_number = decoder.readLong();
  // min_sequence_number (long)
  const min_sequence_number = decoder.readLong();
  // added_snapshot_id (long)
  const added_snapshot_id = decoder.readLong();
  // added_files_count (int)
  const added_files_count = decoder.readInt();
  // existing_files_count (int)
  const existing_files_count = decoder.readInt();
  // deleted_files_count (int)
  const deleted_files_count = decoder.readInt();
  // added_rows_count (long)
  const added_rows_count = decoder.readLong();
  // existing_rows_count (long)
  const existing_rows_count = decoder.readLong();
  // deleted_rows_count (long)
  const deleted_rows_count = decoder.readLong();
  // partitions (optional array)
  const partitionsUnionIndex = decoder.readUnionIndex();
  let partitions: EncodableManifestListEntry['partitions'] = null;
  if (partitionsUnionIndex === 1) {
    partitions = decoder.readArray(() => ({
      contains_null: decoder.readBoolean(),
      contains_nan: readOptionalBoolean(decoder),
      lower_bound: readOptionalBytes(decoder),
      upper_bound: readOptionalBytes(decoder),
    }));
  }
  // first_row_id (v3)
  const first_row_id = readOptionalLong(decoder);

  return {
    manifest_path,
    manifest_length,
    partition_spec_id,
    content,
    sequence_number,
    min_sequence_number,
    added_snapshot_id,
    added_files_count,
    existing_files_count,
    deleted_files_count,
    added_rows_count,
    existing_rows_count,
    deleted_rows_count,
    partitions,
    first_row_id,
  };
}

// ============================================================================
// Helper Functions for Optional Types
// ============================================================================

function writeOptionalLong(encoder: AvroEncoder, value: number | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeLong(value);
  }
}

function readOptionalLong(decoder: AvroDecoder): number | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readLong();
}

function writeOptionalInt(encoder: AvroEncoder, value: number | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeInt(value);
  }
}

function readOptionalInt(decoder: AvroDecoder): number | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readInt();
}

function writeOptionalString(encoder: AvroEncoder, value: string | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeString(value);
  }
}

function readOptionalString(decoder: AvroDecoder): string | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readString();
}

function writeOptionalBoolean(encoder: AvroEncoder, value: boolean | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeBoolean(value);
  }
}

function readOptionalBoolean(decoder: AvroDecoder): boolean | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readBoolean();
}

function writeOptionalBytes(encoder: AvroEncoder, value: Uint8Array | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeBytes(value);
  }
}

function readOptionalBytes(decoder: AvroDecoder): Uint8Array | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readBytes();
}

function writeOptionalKeyValueArray<T>(
  encoder: AvroEncoder,
  value: T[] | null | undefined,
  writeElement: (element: T) => void
): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeArray(value, writeElement);
  }
}

function readOptionalKeyValueArray<T>(
  decoder: AvroDecoder,
  readElement: () => T
): T[] | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readArray(readElement);
}

function writeOptionalLongArray(encoder: AvroEncoder, value: number[] | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeArray(value, (v) => encoder.writeLong(v));
  }
}

function readOptionalLongArray(decoder: AvroDecoder): number[] | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readArray(() => decoder.readLong());
}

function writeOptionalIntArray(encoder: AvroEncoder, value: number[] | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0); // null
  } else {
    encoder.writeUnionIndex(1); // non-null
    encoder.writeArray(value, (v) => encoder.writeInt(v));
  }
}

function readOptionalIntArray(decoder: AvroDecoder): number[] | null {
  const unionIndex = decoder.readUnionIndex();
  if (unionIndex === 0) {
    return null;
  }
  return decoder.readArray(() => decoder.readInt());
}

function writePartitionValue(encoder: AvroEncoder, value: unknown, type: string): void {
  switch (type) {
    case 'int':
      encoder.writeInt(value as number);
      break;
    case 'long':
      encoder.writeLong(value as number);
      break;
    case 'string':
      encoder.writeString(value as string);
      break;
    case 'boolean':
      encoder.writeBoolean(value as boolean);
      break;
    case 'float':
      encoder.writeFloat(value as number);
      break;
    case 'double':
      encoder.writeDouble(value as number);
      break;
    case 'bytes':
      encoder.writeBytes(value as Uint8Array);
      break;
    default:
      encoder.writeString(String(value));
  }
}

function readPartitionValue(decoder: AvroDecoder, type: string): unknown {
  switch (type) {
    case 'int':
      return decoder.readInt();
    case 'long':
      return decoder.readLong();
    case 'string':
      return decoder.readString();
    case 'boolean':
      return decoder.readBoolean();
    case 'float':
      return decoder.readFloat();
    case 'double':
      return decoder.readDouble();
    case 'bytes':
      return decoder.readBytes();
    default:
      return decoder.readString();
  }
}

/**
 * Avro Binary Encoding for Iceberg Manifest Files
 *
 * Implements Avro binary encoding according to the Apache Avro specification.
 * Used for serializing Iceberg manifest files and manifest lists.
 *
 * @see https://avro.apache.org/docs/current/specification/
 * @see https://iceberg.apache.org/spec/
 */
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
export type AvroType = AvroPrimitive | AvroArray | AvroMap | AvroFixed | AvroEnum | AvroRecord | AvroUnion;
/**
 * Avro binary encoder.
 * Encodes values according to the Avro binary encoding specification.
 */
export declare class AvroEncoder {
    private buffer;
    /**
     * Write a null value (no bytes written).
     */
    writeNull(): void;
    /**
     * Write a boolean value.
     */
    writeBoolean(value: boolean): void;
    /**
     * Write an int (32-bit signed) using variable-length zig-zag encoding.
     */
    writeInt(value: number): void;
    /**
     * Write a long (64-bit signed) using variable-length zig-zag encoding.
     */
    writeLong(value: number | bigint): void;
    /**
     * Write a float (32-bit IEEE 754).
     */
    writeFloat(value: number): void;
    /**
     * Write a double (64-bit IEEE 754).
     */
    writeDouble(value: number): void;
    /**
     * Write bytes (length-prefixed).
     */
    writeBytes(value: Uint8Array): void;
    /**
     * Write a string (UTF-8 encoded, length-prefixed).
     */
    writeString(value: string): void;
    /**
     * Write a fixed-length byte array.
     */
    writeFixed(value: Uint8Array, size: number): void;
    /**
     * Write an enum value (as its ordinal index).
     */
    writeEnum(index: number): void;
    /**
     * Write the union index for a union type.
     */
    writeUnionIndex(index: number): void;
    /**
     * Write an array of values.
     * Arrays are encoded as a series of blocks, each with count and values.
     */
    writeArray<T>(values: T[], writeElement: (value: T) => void): void;
    /**
     * Write a map of key-value pairs.
     */
    writeMap<V>(map: Map<string, V> | Record<string, V>, writeValue: (value: V) => void): void;
    /**
     * Get the encoded bytes.
     */
    toBuffer(): Uint8Array;
    /**
     * Get the current size of the buffer.
     */
    get size(): number;
    /**
     * Append raw bytes to the buffer.
     */
    appendRaw(bytes: Uint8Array): void;
    private zigZagEncode32;
    private zigZagEncode64;
    private writeVarInt;
    private writeVarLong;
}
/**
 * Avro Object Container File writer.
 * Writes Avro data in the standard container file format with header and sync markers.
 */
export declare class AvroFileWriter {
    private readonly schema;
    private readonly metadata;
    private readonly syncMarker;
    private blocks;
    constructor(schema: AvroRecord, metadata?: Map<string, string> | Record<string, string>);
    /**
     * Add a block of records.
     */
    addBlock(count: number, data: Uint8Array): void;
    /**
     * Generate the complete Avro container file.
     */
    toBuffer(): Uint8Array;
}
/**
 * Create the Avro schema for Iceberg manifest entries (v2).
 * The data_file schema is customized based on the partition spec.
 */
export declare function createManifestEntrySchema(partitionFields: Array<{
    name: string;
    type: AvroType;
    'field-id': number;
}>, _schemaId: number): AvroRecord;
/**
 * Create the Avro schema for manifest list entries.
 */
export declare function createManifestListSchema(): AvroRecord;
/**
 * Encode a value to binary format for use in lower_bounds/upper_bounds.
 * Follows Iceberg's single-value serialization spec.
 */
export declare function encodeStatValue(value: unknown, type: string): Uint8Array;
/**
 * Truncate a string value for statistics.
 * Iceberg uses truncated strings for min/max bounds to save space.
 */
export declare function truncateString(value: string, length?: number): string;
/**
 * Avro binary decoder.
 * Decodes values according to the Avro binary encoding specification.
 */
export declare class AvroDecoder {
    private buffer;
    private pos;
    constructor(buffer: Uint8Array);
    /**
     * Read a null value (no bytes read).
     */
    readNull(): null;
    /**
     * Read a boolean value.
     */
    readBoolean(): boolean;
    /**
     * Read an int (32-bit signed) using variable-length zig-zag encoding.
     */
    readInt(): number;
    /**
     * Read a long (64-bit signed) using variable-length zig-zag encoding.
     */
    readLong(): number;
    /**
     * Read a float (32-bit IEEE 754).
     */
    readFloat(): number;
    /**
     * Read a double (64-bit IEEE 754).
     */
    readDouble(): number;
    /**
     * Read bytes (length-prefixed).
     */
    readBytes(): Uint8Array;
    /**
     * Read a string (UTF-8 encoded, length-prefixed).
     */
    readString(): string;
    /**
     * Read a fixed-length byte array.
     */
    readFixed(size: number): Uint8Array;
    /**
     * Read an enum value (as its ordinal index).
     */
    readEnum(): number;
    /**
     * Read the union index for a union type.
     */
    readUnionIndex(): number;
    /**
     * Read an array of values.
     */
    readArray<T>(readElement: () => T): T[];
    /**
     * Read a map of key-value pairs.
     */
    readMap<V>(readValue: () => V): Map<string, V>;
    /**
     * Get current position in buffer.
     */
    get position(): number;
    /**
     * Check if there are more bytes to read.
     */
    hasMore(): boolean;
    private zigZagDecode32;
    private zigZagDecode64;
    private readVarInt;
    private readVarLong;
}
/** Data file structure for encoding/decoding */
export interface EncodableDataFile {
    content: number;
    file_path: string;
    file_format: string;
    partition: Record<string, unknown>;
    record_count: number;
    file_size_in_bytes: number;
    column_sizes?: Array<{
        key: number;
        value: number;
    }> | null;
    value_counts?: Array<{
        key: number;
        value: number;
    }> | null;
    null_value_counts?: Array<{
        key: number;
        value: number;
    }> | null;
    nan_value_counts?: Array<{
        key: number;
        value: number;
    }> | null;
    lower_bounds?: Array<{
        key: number;
        value: Uint8Array;
    }> | null;
    upper_bounds?: Array<{
        key: number;
        value: Uint8Array;
    }> | null;
    key_metadata?: Uint8Array | null;
    split_offsets?: number[] | null;
    equality_ids?: number[] | null;
    sort_order_id?: number | null;
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
export declare function encodeDataFile(encoder: AvroEncoder, dataFile: EncodableDataFile, partitionFields: PartitionFieldDef[]): void;
/**
 * Decode a data file from Avro binary format.
 */
export declare function decodeDataFile(decoder: AvroDecoder, partitionFields: PartitionFieldDef[]): EncodableDataFile;
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
export declare function encodeManifestEntry(encoder: AvroEncoder, entry: EncodableManifestEntry, partitionFields: PartitionFieldDef[]): void;
/**
 * Decode a manifest entry from Avro binary format.
 */
export declare function decodeManifestEntry(decoder: AvroDecoder, partitionFields: PartitionFieldDef[]): EncodableManifestEntry;
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
    first_row_id?: number | null;
}
/**
 * Encode a manifest list entry to Avro binary format.
 */
export declare function encodeManifestListEntry(encoder: AvroEncoder, entry: EncodableManifestListEntry): void;
/**
 * Decode a manifest list entry from Avro binary format.
 */
export declare function decodeManifestListEntry(decoder: AvroDecoder): EncodableManifestListEntry;
//# sourceMappingURL=index.d.ts.map
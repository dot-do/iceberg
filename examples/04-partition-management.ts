/**
 * Example 04: Partition Management
 *
 * This example demonstrates how to work with Iceberg partition transforms.
 * Partitioning is key for efficient data organization and query performance.
 * Iceberg supports these transforms:
 * - identity: Partition by exact value
 * - bucket[N]: Hash partition into N buckets
 * - truncate[W]: Truncate strings to W characters or integers to multiples of W
 * - year/month/day/hour: Temporal partitioning
 * - void: Always null (for partition evolution)
 *
 * Run with: npx tsx examples/04-partition-management.ts
 */

import {
  TableMetadataBuilder,
  PartitionSpecBuilder,
  applyTransform,
  getPartitionData,
  getPartitionPath,
  parsePartitionPath,
  createIdentityPartitionSpec,
  createBucketPartitionSpec,
  createTimePartitionSpec,
  type IcebergSchema,
} from '../core/src/index.js';

// ============================================================================
// Main Example
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('Example 04: Partition Management');
  console.log('='.repeat(60));
  console.log();

  // Step 1: Define a schema for our events table
  const schema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'event_id', required: true, type: 'string' },
      { id: 2, name: 'user_id', required: true, type: 'long' },
      { id: 3, name: 'event_type', required: true, type: 'string' },
      { id: 4, name: 'event_time', required: true, type: 'timestamptz' },
      { id: 5, name: 'country', required: false, type: 'string' },
      { id: 6, name: 'payload', required: false, type: 'binary' },
    ],
  };

  console.log('1. Schema for events table:');
  for (const field of schema.fields) {
    console.log(`   ${field.id}: ${field.name} (${field.type})`);
  }
  console.log();

  // Step 2: Demonstrate different transforms
  console.log('2. Partition transforms demonstration:');
  console.log();

  // Identity transform
  console.log('   a) Identity transform:');
  console.log(`      identity("USA") = ${applyTransform('USA', 'identity')}`);
  console.log(`      identity(42) = ${applyTransform(42, 'identity')}`);
  console.log();

  // Bucket transform
  console.log('   b) Bucket transform (hash into N buckets):');
  console.log(`      bucket[16]("user-123") = ${applyTransform('user-123', 'bucket[16]')}`);
  console.log(`      bucket[16]("user-456") = ${applyTransform('user-456', 'bucket[16]')}`);
  console.log(`      bucket[16](12345) = ${applyTransform(12345, 'bucket[16]')}`);
  console.log();

  // Truncate transform
  console.log('   c) Truncate transform:');
  console.log(`      truncate[3]("hello") = ${applyTransform('hello', 'truncate[3]')}`);
  console.log(`      truncate[10](12345) = ${applyTransform(12345, 'truncate[10]')}`);
  console.log(`      truncate[100](12345) = ${applyTransform(12345, 'truncate[100]')}`);
  console.log();

  // Temporal transforms
  const sampleDate = new Date('2024-03-15T14:30:00Z');
  console.log('   d) Temporal transforms:');
  console.log(`      Source: ${sampleDate.toISOString()}`);
  console.log(`      year() = ${applyTransform(sampleDate, 'year')} (years since 1970)`);
  console.log(`      month() = ${applyTransform(sampleDate, 'month')} (months since 1970-01)`);
  console.log(`      day() = ${applyTransform(sampleDate, 'day')} (days since 1970-01-01)`);
  console.log(`      hour() = ${applyTransform(sampleDate, 'hour')} (hours since epoch)`);
  console.log();

  // Step 3: Build a partition spec using the builder
  console.log('3. Building partition spec with PartitionSpecBuilder:');

  const partitionSpec = new PartitionSpecBuilder(schema)
    .day('event_time', 'event_day')
    .identity('country')
    .bucket('user_id', 16, 'user_bucket')
    .build();

  console.log(`   Spec ID: ${partitionSpec['spec-id']}`);
  for (const field of partitionSpec.fields) {
    console.log(`   - ${field.name}: ${field.transform} (source-id: ${field['source-id']})`);
  }
  console.log();

  // Step 4: Create table with partition spec
  console.log('4. Creating table with partition spec:');

  const builder = new TableMetadataBuilder({
    location: 'warehouse/db/events',
    schema: schema,
    partitionSpec: partitionSpec,
  });

  const metadata = builder.build();
  console.log(`   Table location: ${metadata.location}`);
  console.log(`   Default spec ID: ${metadata['default-spec-id']}`);
  console.log();

  // Step 5: Generate partition data for records
  console.log('5. Generating partition data for sample records:');

  const sampleRecords = [
    { event_id: 'e1', user_id: 100, event_type: 'click', event_time: new Date('2024-03-15T10:00:00Z'), country: 'USA' },
    { event_id: 'e2', user_id: 200, event_type: 'view', event_time: new Date('2024-03-15T14:30:00Z'), country: 'UK' },
    { event_id: 'e3', user_id: 100, event_type: 'purchase', event_time: new Date('2024-03-16T08:00:00Z'), country: 'USA' },
    { event_id: 'e4', user_id: 300, event_type: 'click', event_time: new Date('2024-03-16T12:00:00Z'), country: null },
  ];

  for (const record of sampleRecords) {
    const partData = getPartitionData(record, partitionSpec, schema);
    const partPath = getPartitionPath(partData, partitionSpec);
    console.log(`   Record: ${record.event_id} (user: ${record.user_id}, ${record.event_time.toISOString()})`);
    console.log(`   Partition: ${partPath}`);
    console.log();
  }

  // Step 6: Parse partition paths
  console.log('6. Parsing partition paths:');

  const paths = [
    'event_day=19797/country=USA/user_bucket=4',
    'event_day=19798/country=__HIVE_DEFAULT_PARTITION__/user_bucket=12',
  ];

  for (const path of paths) {
    const parsed = parsePartitionPath(path);
    console.log(`   Path: ${path}`);
    console.log(`   Parsed: ${JSON.stringify(parsed)}`);
    console.log();
  }

  // Step 7: Show helper functions for common partition specs
  console.log('7. Convenience functions for common patterns:');

  // Identity partition
  const identitySpec = createIdentityPartitionSpec(5, 'country', 1);
  console.log('   a) createIdentityPartitionSpec(5, "country"):');
  console.log(`      ${JSON.stringify(identitySpec.fields[0])}`);
  console.log();

  // Bucket partition
  const bucketSpec = createBucketPartitionSpec(2, 'user_bucket', 32, 2);
  console.log('   b) createBucketPartitionSpec(2, "user_bucket", 32):');
  console.log(`      ${JSON.stringify(bucketSpec.fields[0])}`);
  console.log();

  // Time partition
  const timeSpec = createTimePartitionSpec(4, 'event_day', 'day', 3);
  console.log('   c) createTimePartitionSpec(4, "event_day", "day"):');
  console.log(`      ${JSON.stringify(timeSpec.fields[0])}`);
  console.log();

  // Step 8: Multi-level partitioning example
  console.log('8. Multi-level partitioning strategy:');
  console.log('   Recommendation for high-volume event data:');
  console.log();
  console.log('   Level 1: day(event_time)');
  console.log('     - Enables efficient time-range queries');
  console.log('     - Creates ~365 partitions per year');
  console.log();
  console.log('   Level 2: bucket[N](user_id)');
  console.log('     - Distributes data evenly across N buckets');
  console.log('     - Choose N based on file size targets');
  console.log('     - Example: N=64 with 100M rows/day = ~1.5M rows/partition/day');
  console.log();
  console.log('   Combined path: data/event_day=19797/user_bucket=42/00001.parquet');

  console.log();
  console.log('='.repeat(60));
  console.log('Partition management example completed!');
  console.log('='.repeat(60));
}

main().catch(console.error);

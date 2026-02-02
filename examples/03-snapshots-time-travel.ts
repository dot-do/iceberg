/**
 * Example 03: Snapshots and Time Travel
 *
 * This example demonstrates how to work with Iceberg snapshots for time travel queries.
 * Snapshots are immutable views of a table at a point in time, enabling:
 * - Time travel queries (query data as it was at a specific time)
 * - Snapshot references (branches and tags)
 * - Snapshot history tracking
 *
 * Run with: npx tsx examples/03-snapshots-time-travel.ts
 */

import {
  TableMetadataBuilder,
  SnapshotBuilder,
  SnapshotManager,
  getSnapshotAtTimestamp,
  getSnapshotById,
  getSnapshotByRef,
  getCurrentSnapshot,
  type IcebergSchema,
  type Snapshot,
} from '../core/src/index.js';

// ============================================================================
// Helper Functions
// ============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatTimestamp(ms: number): string {
  return new Date(ms).toISOString();
}

// ============================================================================
// Main Example
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('Example 03: Snapshots and Time Travel');
  console.log('='.repeat(60));
  console.log();

  // Step 1: Create initial table
  const schema: IcebergSchema = {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: true, type: 'long' },
      { id: 2, name: 'data', required: true, type: 'string' },
    ],
  };

  const builder = new TableMetadataBuilder({
    location: 'warehouse/db/events',
    schema: schema,
  });

  console.log('1. Created empty table (no snapshots yet)');
  console.log();

  // Step 2: Add first snapshot (simulating data append)
  console.log('2. Adding first snapshot (initial data load)...');

  const snapshot1 = new SnapshotBuilder({
    sequenceNumber: 1,
    snapshotId: Date.now(),
    manifestListPath: 'warehouse/db/events/metadata/snap-1-manifest-list.avro',
    operation: 'append',
  })
    .setSummary(
      5,    // added files
      0,    // deleted files
      1000, // added records
      0,    // deleted records
      5000, // added size
      0,    // removed size
      1000, // total records
      5000, // total size
      5     // total files
    )
    .build();

  builder.addSnapshot(snapshot1);
  console.log(`   Snapshot ID: ${snapshot1['snapshot-id']}`);
  console.log(`   Timestamp: ${formatTimestamp(snapshot1['timestamp-ms'])}`);
  console.log(`   Records: ${snapshot1.summary['total-records']}`);
  console.log();

  // Small delay to ensure distinct timestamps
  await sleep(100);

  // Step 3: Add second snapshot
  console.log('3. Adding second snapshot (incremental append)...');

  const snapshot2 = new SnapshotBuilder({
    sequenceNumber: 2,
    snapshotId: Date.now(),
    parentSnapshotId: snapshot1['snapshot-id'],
    manifestListPath: 'warehouse/db/events/metadata/snap-2-manifest-list.avro',
    operation: 'append',
  })
    .setSummary(
      2,    // added files
      0,    // deleted files
      500,  // added records
      0,    // deleted records
      2500, // added size
      0,    // removed size
      1500, // total records
      7500, // total size
      7     // total files
    )
    .build();

  builder.addSnapshot(snapshot2);
  console.log(`   Snapshot ID: ${snapshot2['snapshot-id']}`);
  console.log(`   Timestamp: ${formatTimestamp(snapshot2['timestamp-ms'])}`);
  console.log(`   Records: ${snapshot2.summary['total-records']}`);
  console.log();

  await sleep(100);

  // Step 4: Add third snapshot with deletion
  console.log('4. Adding third snapshot (with deletions)...');

  const snapshot3 = new SnapshotBuilder({
    sequenceNumber: 3,
    snapshotId: Date.now(),
    parentSnapshotId: snapshot2['snapshot-id'],
    manifestListPath: 'warehouse/db/events/metadata/snap-3-manifest-list.avro',
    operation: 'overwrite',
  })
    .setSummary(
      1,    // added files
      2,    // deleted files
      100,  // added records
      300,  // deleted records
      500,  // added size
      1500, // removed size
      1300, // total records
      6500, // total size
      6     // total files
    )
    .build();

  builder.addSnapshot(snapshot3);

  // Create a tag for the current state
  builder.createTag('v1.0', snapshot3['snapshot-id']);

  console.log(`   Snapshot ID: ${snapshot3['snapshot-id']}`);
  console.log(`   Timestamp: ${formatTimestamp(snapshot3['timestamp-ms'])}`);
  console.log(`   Records: ${snapshot3.summary['total-records']}`);
  console.log(`   Created tag: v1.0`);
  console.log();

  // Build the final metadata
  const metadata = builder.build();

  // Step 5: Demonstrate time travel
  console.log('5. Time Travel Queries:');
  console.log();

  // Get current snapshot
  const current = getCurrentSnapshot(metadata);
  console.log('   Current snapshot:');
  if (current) {
    console.log(`     - ID: ${current['snapshot-id']}`);
    console.log(`     - Time: ${formatTimestamp(current['timestamp-ms'])}`);
    console.log(`     - Records: ${current.summary['total-records']}`);
  }
  console.log();

  // Query at different points in time
  const timePoints = [
    snapshot1['timestamp-ms'],
    snapshot2['timestamp-ms'],
    snapshot3['timestamp-ms'],
  ];

  console.log('   Snapshots at each point in time:');
  for (let i = 0; i < timePoints.length; i++) {
    const snap = getSnapshotAtTimestamp(metadata, timePoints[i]);
    if (snap) {
      console.log(`     Time ${i + 1} (${formatTimestamp(timePoints[i])}):`);
      console.log(`       - Snapshot ID: ${snap['snapshot-id']}`);
      console.log(`       - Records: ${snap.summary['total-records']}`);
    }
  }
  console.log();

  // Step 6: Query by reference
  console.log('6. Query by reference:');

  const mainBranch = getSnapshotByRef(metadata, 'main');
  if (mainBranch) {
    console.log(`   main branch -> Snapshot ${mainBranch['snapshot-id']}`);
  }

  const v1Tag = getSnapshotByRef(metadata, 'v1.0');
  if (v1Tag) {
    console.log(`   v1.0 tag -> Snapshot ${v1Tag['snapshot-id']}`);
  }
  console.log();

  // Step 7: Query by snapshot ID
  console.log('7. Query by snapshot ID:');
  const snap = getSnapshotById(metadata, snapshot2['snapshot-id']);
  if (snap) {
    console.log(`   Snapshot ${snap['snapshot-id']}:`);
    console.log(`     - Operation: ${snap.summary.operation}`);
    console.log(`     - Added files: ${snap.summary['added-data-files']}`);
    console.log(`     - Total records: ${snap.summary['total-records']}`);
  }
  console.log();

  // Step 8: Show snapshot history
  console.log('8. Snapshot history (chronological):');
  const history = metadata['snapshot-log'];
  for (const entry of history) {
    const snap = getSnapshotById(metadata, entry['snapshot-id']);
    console.log(`   ${formatTimestamp(entry['timestamp-ms'])} - Snapshot ${entry['snapshot-id']}`);
    if (snap) {
      console.log(`     Operation: ${snap.summary.operation}, Records: ${snap.summary['total-records']}`);
    }
  }
  console.log();

  // Step 9: Use SnapshotManager for advanced operations
  console.log('9. Using SnapshotManager:');

  const manager = SnapshotManager.fromMetadata(metadata);
  const stats = manager.getStats();

  console.log('   Snapshot statistics:');
  console.log(`     - Total snapshots: ${stats.totalSnapshots}`);
  console.log(`     - Current snapshot ID: ${stats.currentSnapshotId}`);
  console.log(`     - Oldest snapshot: ${stats.oldestSnapshotTimestamp ? formatTimestamp(stats.oldestSnapshotTimestamp) : 'N/A'}`);
  console.log(`     - Newest snapshot: ${stats.newestSnapshotTimestamp ? formatTimestamp(stats.newestSnapshotTimestamp) : 'N/A'}`);
  console.log(`     - Branch count: ${stats.branchCount}`);
  console.log(`     - Tag count: ${stats.tagCount}`);
  console.log();

  // Get ancestor chain
  const chain = manager.getAncestorChain(snapshot3['snapshot-id']);
  console.log('   Ancestor chain for current snapshot:');
  for (const s of chain) {
    console.log(`     ${s['snapshot-id']} (seq: ${s['sequence-number']})`);
  }

  console.log();
  console.log('='.repeat(60));
  console.log('Snapshot and time travel example completed!');
  console.log('='.repeat(60));
}

main().catch(console.error);

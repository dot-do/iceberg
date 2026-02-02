# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-02-01

### Added

- **Apache Iceberg v2 Table Format Support**
  - Full implementation of the Apache Iceberg v2 specification
  - Support for metadata versioning and atomic updates

- **TableMetadataBuilder**
  - Create and update table metadata programmatically
  - Manage table properties and configuration
  - Track metadata history and snapshots

- **SnapshotBuilder**
  - Create new snapshots with data files
  - Support for append, overwrite, and delete operations
  - Manifest file management

- **MetadataWriter**
  - Atomic metadata writes with optimistic concurrency control
  - Support for multiple storage backends
  - Safe concurrent access patterns

- **Schema Evolution Support**
  - Add, rename, and drop columns
  - Change column types (with compatible type promotions)
  - Reorder columns
  - Full schema history tracking

- **Partition Spec Evolution**
  - Define and evolve partition specifications
  - Support for identity, bucket, truncate, year, month, day, and hour transforms
  - Partition spec history tracking

- **Sort Order Support**
  - Define sort orders for optimized query performance
  - Multiple sort fields with ascending/descending order
  - Nulls first/last configuration

- **Bloom Filter Support**
  - Column-level bloom filters for efficient data skipping
  - Configurable false positive probability
  - Reduce I/O for selective queries

- **Row-Level Deletes**
  - Position deletes for precise row removal
  - Equality deletes for predicate-based deletion
  - Delete file management and compaction

- **Column Statistics**
  - Min/max values for data skipping
  - Null count and value count tracking
  - NaN count for floating-point columns
  - Bounds tracking for efficient pruning

- **Time Travel Queries**
  - Query data at specific snapshots
  - Query data at specific timestamps
  - Access historical versions of tables

- **Branch and Tag References**
  - Create named branches for isolated development
  - Create tags for marking specific snapshots
  - Reference management and lifecycle

- **Multiple Storage Backends**
  - Memory storage for testing and development
  - File system storage for local development
  - Cloudflare R2 storage for production deployments

- **Catalog Implementations**
  - Catalog interface for table management
  - Namespace support for table organization
  - Table listing and discovery

## Future

### Planned Features

- REST Catalog implementation (Iceberg REST Catalog spec)
- AWS S3 storage backend
- Google Cloud Storage backend
- Azure Blob Storage backend
- Hive Metastore catalog
- AWS Glue catalog
- Parquet file reader/writer integration
- ORC file format support
- Query engine integrations
- Table maintenance operations (compaction, expiration)
- Incremental append support
- Table migration utilities

### Known Limitations

- Currently focused on metadata management; data file reading/writing requires external Parquet libraries
- No built-in query execution engine
- Limited to JavaScript/TypeScript runtime environments

[Unreleased]: https://github.com/dot-do/iceberg/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dot-do/iceberg/releases/tag/v0.1.0

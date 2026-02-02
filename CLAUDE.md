# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Unified Apache Iceberg implementation** in TypeScript intended to be the single source of truth for Iceberg operations across the dotdo ecosystem. Two packages in a pnpm monorepo:

- **@dotdo/iceberg** (`core/`) - Core library for Iceberg metadata, manifests, snapshots, schema evolution, and Avro encoding
- **iceberg.do** (`service/`) - REST Catalog service on Cloudflare Workers with Durable Objects and R2

## Ecosystem Integration

This repo consolidates Iceberg implementations from multiple projects:

| Project | Current State | Integration Path |
|---------|--------------|------------------|
| **MongoLake** | Has `/src/iceberg/` with v2 metadata, manifests, R2 catalog | Replace with `@dotdo/iceberg` |
| **KafkaLake** | Uses Delta Lake, designed for Iceberg | Add Iceberg catalog option |
| **SQLake** | Most complete (~22K lines) - schema evolution, Avro, stats | Extract best patterns here |
| **GitX** | Functional v2 in `/src/iceberg/` | Replace with `@dotdo/iceberg` |
| **ParqueDB** | IceType integration, custom manifests | Adopt native Iceberg metadata |
| **db4** | `@db4/iceberg` package - CDC, partitions, bloom filters | Make spec-compliant via this lib |

**Goal**: Single `@dotdo/iceberg` dependency across all projects for:
- Spec-compliant Iceberg v2 metadata
- Unified schema evolution
- Consistent Avro manifest encoding
- Shared R2/S3 storage patterns
- REST Catalog interoperability (Spark, DuckDB, Trino)

## Commands

```bash
# Workspace (from root)
pnpm install              # Install dependencies
pnpm build                # Build all packages
pnpm test                 # Run all tests
pnpm lint                 # Lint all packages

# Core package
cd core
pnpm test                 # Run tests once
pnpm test:watch           # Run tests in watch mode
vitest run tests/metadata.test.ts  # Run single test file

# Service package
cd service
pnpm dev                  # Start local dev server (wrangler)
pnpm deploy               # Deploy to Cloudflare Workers
```

## Architecture

### Three-Tier Catalog Access Model

1. **Tier 1: Direct Metadata** - Read/write metadata files directly to S3/R2/filesystem
2. **Tier 2: R2 Data Catalog Client** - Cloudflare's managed R2 Data Catalog API
3. **Tier 3: iceberg.do REST Server** - Full REST Catalog with OAuth user-level auth on Workers

### Core Library Structure

- `metadata/types.ts` - Iceberg type definitions (schemas, snapshots, manifests, data files)
- `metadata/schema.ts` - Schema creation, Parquet conversion, schema evolution
- `metadata/reader.ts` - Read table metadata from storage
- `metadata/writer.ts` - Write metadata.json files (MetadataWriter class)
- `metadata/manifest.ts` - ManifestGenerator, ManifestListGenerator
- `metadata/snapshot.ts` - SnapshotBuilder, TableMetadataBuilder, SnapshotManager
- `avro/index.ts` - Avro binary encoding for manifest files per Iceberg spec
- `catalog/filesystem.ts` - FileSystemCatalog, MemoryCatalog implementations
- `catalog/r2-client.ts` - R2DataCatalogClient for Cloudflare R2 integration

### Service Structure

- `routes.ts` - Iceberg REST Catalog API endpoints (v1 spec)
- `catalog/durable-object.ts` - CatalogDO class using SQLite in Durable Objects
- `auth/middleware.ts` - Authentication middleware for oauth.do integration
- `auth/permissions.ts` - RBAC/FGA permission checking

### Key Patterns

- Schema IDs and field IDs are monotonically increasing integers
- Snapshot IDs use millisecond timestamps
- Manifest files use Avro binary encoding per Iceberg spec
- REST API follows the [Iceberg REST Catalog specification](https://iceberg.apache.org/spec/#iceberg-rest-catalog)

### Features to Consolidate (from ecosystem projects)

From **SQLake**: Schema evolution validation, stats integration, atomic commits with retry
From **db4**: Bloom filters, row-level deletes, partition transforms, compaction
From **MongoLake**: R2 Data Catalog client, schema tracker
From **GitX**: Column statistics in manifests, zone map conversion

## Testing

Uses Vitest with Node environment. Tests are in `tests/` directories within each package.

## Issue Tracking

This project uses **bd** (beads) for issue tracking:

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync --flush-only  # Export to JSONL
```

## Service Configuration

Cloudflare Worker bindings (see `service/wrangler.toml`):
- Durable Objects: `CatalogDO` for metadata storage
- R2 bucket: `iceberg-tables` for table data
- Optional D1 database for metadata

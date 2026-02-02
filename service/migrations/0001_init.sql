-- Migration: 0001_init.sql
-- Description: Initialize the D1 catalog schema for iceberg.do
-- Created: 2024-02-01

-- =============================================================================
-- Namespaces Table
-- =============================================================================

-- Namespaces are hierarchical identifiers for organizing tables.
-- Examples: ["production"], ["production", "warehouse"], ["analytics", "staging"]
-- The namespace column stores the namespace as a unit-separator (\x1f) delimited string
-- for efficient prefix queries while maintaining uniqueness.

CREATE TABLE IF NOT EXISTS namespaces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  -- Namespace stored as unit-separator delimited string
  -- e.g., "production" or "production\x1fwarehouse"
  namespace TEXT NOT NULL UNIQUE,
  -- JSON object of namespace properties
  -- Common properties: location, owner, description
  properties TEXT DEFAULT '{}',
  -- Timestamps in milliseconds since Unix epoch
  created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000)
);

-- Index for prefix queries on hierarchical namespaces
-- Enables efficient queries like "list all namespaces under 'production'"
CREATE INDEX IF NOT EXISTS idx_namespaces_prefix ON namespaces(namespace);

-- =============================================================================
-- Tables Table
-- =============================================================================

-- Tables store Iceberg table metadata locations and cached metadata.
-- The actual data files are stored in R2 or other object storage.

CREATE TABLE IF NOT EXISTS tables (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  -- Namespace this table belongs to (unit-separator delimited)
  namespace TEXT NOT NULL,
  -- Table name (unique within namespace)
  name TEXT NOT NULL,
  -- Base location for table data files (e.g., s3://bucket/path)
  location TEXT NOT NULL,
  -- Location of the current metadata JSON file
  metadata_location TEXT NOT NULL,
  -- Cached full metadata JSON (optional, for faster loads)
  -- This is the complete TableMetadata object per Iceberg spec
  metadata TEXT,
  -- JSON object of table-level properties
  -- Separate from metadata.properties for catalog-level config
  properties TEXT DEFAULT '{}',
  -- Version number for optimistic concurrency control (OCC)
  -- Incremented on each metadata update
  version INTEGER DEFAULT 1,
  -- Timestamps in milliseconds since Unix epoch
  created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
  -- Unique constraint on (namespace, name)
  UNIQUE(namespace, name)
);

-- Index on namespace for listing tables in a namespace
CREATE INDEX IF NOT EXISTS idx_tables_namespace ON tables(namespace);

-- Index on name for table lookups by name across namespaces
CREATE INDEX IF NOT EXISTS idx_tables_name ON tables(name);

-- Composite index for the most common query: get table by namespace + name
CREATE INDEX IF NOT EXISTS idx_tables_ns_name ON tables(namespace, name);

-- =============================================================================
-- Views Table (Optional, for future Iceberg view support)
-- =============================================================================

-- Uncomment when implementing Iceberg view support
-- CREATE TABLE IF NOT EXISTS views (
--   id INTEGER PRIMARY KEY AUTOINCREMENT,
--   namespace TEXT NOT NULL,
--   name TEXT NOT NULL,
--   location TEXT NOT NULL,
--   metadata_location TEXT NOT NULL,
--   metadata TEXT,
--   properties TEXT DEFAULT '{}',
--   version INTEGER DEFAULT 1,
--   created_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
--   updated_at INTEGER DEFAULT (strftime('%s', 'now') * 1000),
--   UNIQUE(namespace, name)
-- );
-- CREATE INDEX IF NOT EXISTS idx_views_namespace ON views(namespace);
-- CREATE INDEX IF NOT EXISTS idx_views_ns_name ON views(namespace, name);

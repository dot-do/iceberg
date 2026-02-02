#!/usr/bin/env python3
"""
Test PyIceberg connection to iceberg.do with R2 storage.

This verifies:
1. REST catalog connection works
2. S3 credentials from /v1/config are used
3. Data files can be written to R2
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
import uuid

# Catalog configuration
CATALOG_URI = "https://iceberg-do.dotdo.workers.dev"
WAREHOUSE = f"s3://iceberg-tables/pyiceberg-test-{uuid.uuid4().hex[:8]}"

def main():
    print("=" * 60)
    print("PyIceberg + iceberg.do + R2 Integration Test")
    print("=" * 60)
    print(f"\nCatalog: {CATALOG_URI}")
    print(f"Warehouse: {WAREHOUSE}\n")

    # Load catalog - credentials come from /v1/config
    print("1. Connecting to catalog...")
    catalog = load_catalog(
        "iceberg_do",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
    )
    print("   ✓ Connected to catalog")

    # Create namespace
    namespace = "pyiceberg_test"
    print(f"\n2. Creating namespace '{namespace}'...")
    try:
        catalog.create_namespace(namespace)
        print(f"   ✓ Created namespace")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ✓ Namespace already exists")
        else:
            raise

    # Define schema - use optional fields to match PyArrow's default behavior
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "email", StringType(), required=False),
    )

    # Create table
    table_name = f"users_{uuid.uuid4().hex[:8]}"
    print(f"\n3. Creating table '{namespace}.{table_name}'...")
    table = catalog.create_table(
        f"{namespace}.{table_name}",
        schema=schema,
    )
    print(f"   ✓ Created table at {table.location()}")

    # Write data
    print("\n4. Writing data to R2...")
    data = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", None],
    })
    table.append(data)
    print(f"   ✓ Wrote {len(data)} rows")

    # Read data back
    print("\n5. Reading data back...")
    result = table.scan().to_arrow()
    print(f"   ✓ Read {result.num_rows} rows")
    print(result.to_pydict())

    # Cleanup
    print(f"\n6. Cleaning up...")
    catalog.drop_table(f"{namespace}.{table_name}")
    print(f"   ✓ Dropped table")

    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)

if __name__ == "__main__":
    main()

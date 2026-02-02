# Apache Iceberg REST Catalog Conformance Testing

This directory contains conformance tests to validate iceberg.do against the official Apache Iceberg REST Catalog specification.

## Testing Methods

### 1. REST Compatibility Kit (RCK) - Official Java Tests

The [REST Compatibility Kit](https://github.com/apache/iceberg/tree/main/open-api#rest-compatibility-kit-rck) is the official Apache Iceberg conformance test suite.

```bash
# Run RCK against iceberg.do
./run-rck.sh
```

### 2. OpenAPI Spec Validation

Validates our API responses against the official OpenAPI schema.

```bash
pnpm test:conformance
```

### 3. PyIceberg Integration

Tests using the official Python Iceberg library.

```bash
./run-pyiceberg-tests.sh
```

## Coverage

| Endpoint | RCK | OpenAPI | PyIceberg |
|----------|-----|---------|-----------|
| GET /v1/config | ✓ | ✓ | ✓ |
| GET /v1/namespaces | ✓ | ✓ | ✓ |
| POST /v1/namespaces | ✓ | ✓ | ✓ |
| GET /v1/namespaces/{ns} | ✓ | ✓ | ✓ |
| HEAD /v1/namespaces/{ns} | ✓ | ✓ | ✓ |
| DELETE /v1/namespaces/{ns} | ✓ | ✓ | ✓ |
| POST /v1/namespaces/{ns}/properties | ✓ | ✓ | ✓ |
| GET /v1/namespaces/{ns}/tables | ✓ | ✓ | ✓ |
| POST /v1/namespaces/{ns}/tables | ✓ | ✓ | ✓ |
| GET /v1/namespaces/{ns}/tables/{t} | ✓ | ✓ | ✓ |
| HEAD /v1/namespaces/{ns}/tables/{t} | ✓ | ✓ | ✓ |
| POST /v1/namespaces/{ns}/tables/{t} | ✓ | ✓ | ✓ |
| DELETE /v1/namespaces/{ns}/tables/{t} | ✓ | ✓ | ✓ |
| POST /v1/tables/rename | ✓ | ✓ | ✓ |

## Requirements

- Docker (for RCK)
- Python 3.10+ (for PyIceberg)
- Node.js 18+ (for OpenAPI validation)

## References

- [Iceberg REST Catalog Spec](https://iceberg.apache.org/spec/#iceberg-rest-catalog)
- [OpenAPI Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [REST Compatibility Kit PR](https://github.com/apache/iceberg/pull/10908)

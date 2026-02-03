#!/bin/bash
#
# Run Apache Iceberg REST Compatibility Kit (RCK) against iceberg.do
#
# This uses the official Java-based conformance tests from Apache Iceberg.
# See: https://github.com/apache/iceberg/tree/main/open-api#rest-compatibility-kit-rck
#
# Usage:
#   ./run-rck.sh                    # Run against remote iceberg.do
#   ./run-rck.sh --with-minio       # Start MinIO for local S3-compatible storage
#   ./run-rck.sh --help             # Show help
#
# Environment variables:
#   ICEBERG_DO_URL    - Target server URL (default: https://iceberg-do.dotdo.workers.dev)
#   ICEBERG_VERSION   - Apache Iceberg version to test with (default: main)
#   WAREHOUSE         - Warehouse name (default: rck-<timestamp>)
#
# When using --with-minio:
#   - MinIO is started via docker-compose on ports 9000 (API) and 9001 (console)
#   - The 'iceberg-tables' bucket is created automatically
#   - MinIO credentials are passed to the test environment
#   - MinIO is cleaned up after tests complete
#

set -e

# Script directory (for docker-compose.yml location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
ICEBERG_DO_URL="${ICEBERG_DO_URL:-https://iceberg-do.dotdo.workers.dev}"
# Use main branch by default - it has better prefix/warehouse isolation support
ICEBERG_VERSION="${ICEBERG_VERSION:-main}"
WAREHOUSE="${WAREHOUSE:-rck-$(date +%s)}"

# MinIO configuration
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_BUCKET="iceberg-tables"
USE_MINIO=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --with-minio)
            USE_MINIO=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Run Apache Iceberg REST Compatibility Kit (RCK) tests."
            echo ""
            echo "Options:"
            echo "  --with-minio    Start MinIO via docker-compose for local S3-compatible storage"
            echo "  --help, -h      Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  ICEBERG_DO_URL    Target server URL (default: https://iceberg-do.dotdo.workers.dev)"
            echo "  ICEBERG_VERSION   Apache Iceberg version to test with (default: main)"
            echo "  WAREHOUSE         Warehouse name (default: rck-<timestamp>)"
            echo ""
            echo "Examples:"
            echo "  $0                           # Run against remote iceberg.do"
            echo "  $0 --with-minio              # Start MinIO and run tests"
            echo "  ICEBERG_DO_URL=http://localhost:8787 $0 --with-minio"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Function to start MinIO via docker-compose
start_minio() {
    echo "Starting MinIO via docker-compose..."

    # Check if docker-compose or docker compose is available
    if command -v docker-compose &>/dev/null; then
        COMPOSE_CMD="docker-compose"
    elif docker compose version &>/dev/null 2>&1; then
        COMPOSE_CMD="docker compose"
    else
        echo "Error: Neither 'docker-compose' nor 'docker compose' is available."
        echo "Please install Docker Compose:"
        echo "  brew install docker-compose"
        echo "Or use Docker Desktop which includes Compose."
        exit 1
    fi

    # Start MinIO
    cd "$SCRIPT_DIR"
    $COMPOSE_CMD up -d

    # Wait for MinIO to be healthy
    echo "Waiting for MinIO to be ready..."
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -sf "$MINIO_ENDPOINT/minio/health/live" > /dev/null 2>&1; then
            echo "MinIO is ready!"
            break
        fi
        retries=$((retries - 1))
        sleep 1
    done

    if [ $retries -eq 0 ]; then
        echo "Error: MinIO failed to start within 30 seconds"
        stop_minio
        exit 1
    fi

    # Create the iceberg-tables bucket using MinIO client (mc) via Docker
    echo "Creating bucket '$MINIO_BUCKET'..."
    docker run --rm --network host \
        -e MC_HOST_minio="http://$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY@localhost:9000" \
        quay.io/minio/mc:latest \
        mb --ignore-existing "minio/$MINIO_BUCKET" 2>/dev/null || true

    echo "MinIO setup complete!"
    echo "  API endpoint: $MINIO_ENDPOINT"
    echo "  Console:      http://localhost:9001"
    echo "  Bucket:       $MINIO_BUCKET"
    echo ""
}

# Function to stop MinIO
stop_minio() {
    echo ""
    echo "Stopping MinIO..."
    cd "$SCRIPT_DIR"

    if command -v docker-compose &>/dev/null; then
        docker-compose down
    elif docker compose version &>/dev/null 2>&1; then
        docker compose down
    fi
}

# Set up cleanup trap
cleanup() {
    local exit_code=$?
    if [ "$USE_MINIO" = true ]; then
        stop_minio
    fi
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
    exit $exit_code
}
trap cleanup EXIT

# Start MinIO if requested
if [ "$USE_MINIO" = true ]; then
    start_minio

    # Export MinIO credentials for the test environment
    export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"
    export AWS_REGION="us-east-1"
    export S3_ENDPOINT="$MINIO_ENDPOINT"
    export S3_PATH_STYLE_ACCESS="true"
fi

echo "=============================================="
echo "Apache Iceberg REST Compatibility Kit (RCK)"
echo "=============================================="
echo ""
echo "Target:    $ICEBERG_DO_URL"
echo "Iceberg:   $ICEBERG_VERSION"
echo "Warehouse: $WAREHOUSE"
if [ "$USE_MINIO" = true ]; then
echo "Storage:   MinIO ($MINIO_ENDPOINT)"
fi
echo ""

# Clean up the catalog before running tests
# The RCK tests expect an empty catalog
echo "Cleaning up catalog..."
if command -v jq &>/dev/null; then
    # Use jq for robust JSON parsing
    # Delete all namespaces (which deletes their contents too, if cascade is supported)
    # First, delete all tables and views, then namespaces
    namespaces=$(curl -s "$ICEBERG_DO_URL/v1/namespaces" | jq -r '.namespaces[][] // empty' 2>/dev/null || true)
    for ns in $namespaces; do
        if [ -n "$ns" ]; then
            echo "  Cleaning namespace: $ns"
            # Delete tables
            tables=$(curl -s "$ICEBERG_DO_URL/v1/namespaces/$ns/tables" | jq -r '.identifiers[].name // empty' 2>/dev/null || true)
            for table in $tables; do
                [ -n "$table" ] && curl -s -X DELETE "$ICEBERG_DO_URL/v1/namespaces/$ns/tables/$table?purgeRequested=true" > /dev/null || true
            done
            # Delete views
            views=$(curl -s "$ICEBERG_DO_URL/v1/namespaces/$ns/views" | jq -r '.identifiers[].name // empty' 2>/dev/null || true)
            for view in $views; do
                [ -n "$view" ] && curl -s -X DELETE "$ICEBERG_DO_URL/v1/namespaces/$ns/views/$view" > /dev/null || true
            done
            # Delete namespace
            curl -s -X DELETE "$ICEBERG_DO_URL/v1/namespaces/$ns" > /dev/null || true
        fi
    done
    echo "  Cleanup complete"
else
    echo "  Warning: jq not available, skipping cleanup (install jq for clean catalog)"
fi
echo ""

# Create a temporary directory for the test
TEMP_DIR=$(mktemp -d)

# Clone iceberg repo (shallow)
clone_iceberg_repo() {
    echo "Cloning Apache Iceberg (shallow) - branch: $ICEBERG_VERSION..."
    if [ "$ICEBERG_VERSION" = "main" ]; then
        git clone --depth 1 https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg"
    else
        git clone --depth 1 --branch apache-iceberg-${ICEBERG_VERSION} \
            https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg" 2>/dev/null || {
            echo "Version not found, using main branch..."
            git clone --depth 1 https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg"
        }
    fi
}

# Check for Java first (preferred for speed)
if java -version &>/dev/null; then
    echo "Running RCK with local Java..."

    clone_iceberg_repo
    cd "$TEMP_DIR/iceberg"

    # Run the RCK tests
    echo ""
    echo "Running RCK tests against $ICEBERG_DO_URL..."
    echo ""

    ./gradlew :iceberg-open-api:test \
        --tests RESTCompatibilityKitSuite \
        -Drck.local=false \
        -Drck.uri="$ICEBERG_DO_URL" \
        -Drck.warehouse="$WAREHOUSE" \
        -Drck.requires-namespace-create=true \
        -Drck.supports-serverside-retry=true \
        --info

elif command -v docker &>/dev/null; then
    echo "Running RCK via Docker (no local Java found)..."

    clone_iceberg_repo

    # Run Gradle inside Docker container
    echo ""
    echo "Running RCK tests against $ICEBERG_DO_URL..."
    echo ""

    # Build environment variables for Docker
    # When using MinIO, pass the real credentials; otherwise use mock values.
    # Mock AWS credentials are required to satisfy the AWS SDK initialization.
    # Without these, tests fail with "Unable to load region from any of the providers".
    if [ "$USE_MINIO" = true ]; then
        DOCKER_ENV_ARGS="-e AWS_REGION=us-east-1 \
            -e AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY \
            -e AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY \
            -e S3_ENDPOINT=$MINIO_ENDPOINT \
            -e S3_PATH_STYLE_ACCESS=true"
    else
        # Dummy values for SDK initialization - server provides real R2 credentials via /v1/config
        DOCKER_ENV_ARGS="-e AWS_REGION=us-east-1 \
            -e AWS_ACCESS_KEY_ID=testing \
            -e AWS_SECRET_ACCESS_KEY=testing"
    fi

    docker run --rm \
        -v "$TEMP_DIR/iceberg:/workspace" \
        -w /workspace \
        --network host \
        $DOCKER_ENV_ARGS \
        gradle:8.5-jdk17 \
        ./gradlew :iceberg-open-api:test \
            --tests RESTCompatibilityKitSuite \
            -Drck.local=false \
            -Drck.uri="$ICEBERG_DO_URL" \
            -Drck.warehouse="$WAREHOUSE" \
            -Drck.requires-namespace-create=true \
            -Drck.supports-serverside-retry=true \
            --info --no-daemon

else
    echo "Neither Java nor Docker found. Please install one of them to run RCK tests."
    echo ""
    echo "Option 1: Install Java 17+"
    echo "  brew install openjdk@17"
    echo ""
    echo "Option 2: Install Docker"
    echo "  brew install --cask docker"
    echo ""
    exit 1
fi

echo ""
echo "✓ RCK tests completed!"

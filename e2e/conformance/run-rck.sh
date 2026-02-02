#!/bin/bash
#
# Run Apache Iceberg REST Compatibility Kit (RCK) against iceberg.do
#
# This uses the official Java-based conformance tests from Apache Iceberg.
# See: https://github.com/apache/iceberg/tree/main/open-api#rest-compatibility-kit-rck
#

set -e

# Configuration
ICEBERG_DO_URL="${ICEBERG_DO_URL:-https://iceberg-do.dotdo.workers.dev}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.7.1}"
WAREHOUSE="${WAREHOUSE:-rck-$(date +%s)}"

echo "=============================================="
echo "Apache Iceberg REST Compatibility Kit (RCK)"
echo "=============================================="
echo ""
echo "Target:   $ICEBERG_DO_URL"
echo "Iceberg:  $ICEBERG_VERSION"
echo "Warehouse: $WAREHOUSE"
echo ""

# Check for Java first (preferred for speed)
if java -version &>/dev/null; then
    echo "Running RCK with local Java..."

    # Create a temporary directory for the test
    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    # Clone iceberg repo (shallow)
    echo "Cloning Apache Iceberg (shallow)..."
    git clone --depth 1 --branch apache-iceberg-${ICEBERG_VERSION} \
        https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg" 2>/dev/null || {
        echo "Using main branch..."
        git clone --depth 1 https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg"
    }

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

    # Create a temporary directory for the test
    TEMP_DIR=$(mktemp -d)
    trap "rm -rf $TEMP_DIR" EXIT

    # Clone iceberg repo (shallow)
    echo "Cloning Apache Iceberg (shallow)..."
    git clone --depth 1 --branch apache-iceberg-${ICEBERG_VERSION} \
        https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg" 2>/dev/null || {
        echo "Using main branch..."
        git clone --depth 1 https://github.com/apache/iceberg.git "$TEMP_DIR/iceberg"
    }

    # Run Gradle inside Docker container
    echo ""
    echo "Running RCK tests against $ICEBERG_DO_URL..."
    echo ""

    docker run --rm \
        -v "$TEMP_DIR/iceberg:/workspace" \
        -w /workspace \
        --network host \
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

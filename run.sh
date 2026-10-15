#!/usr/bin/env bash
set -e

# 1. Resolve script directory and switch to it
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "===================================================================="
echo "       Starting Fake News Classification Pipeline"
echo "       Kafka | Hadoop HDFS | TF-IDF NLP | Docker"
echo "===================================================================="

# 2. Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH. Please install Docker."
    exit 1
fi

# 3. Locate compose configuration file
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
    COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "[ERROR] Could not find docker-compose.yml or compose.yaml in $SCRIPT_DIR"
    echo "Please ensure you run this script from within the cloned repository."
    exit 1
fi

echo "[1/3] Using configuration file: $COMPOSE_FILE"

# 4. Ensure mount directories exist
mkdir -p "$SCRIPT_DIR/data/models" "$SCRIPT_DIR/data/raw" "$SCRIPT_DIR/data/hdfs_buffer"

# 5. Determine Docker Compose command
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose -f $COMPOSE_FILE"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose -f $COMPOSE_FILE"
else
    echo "[ERROR] Neither 'docker compose' nor 'docker-compose' command found."
    exit 1
fi

echo ""
echo "[2/3] Building and launching containers..."
$COMPOSE_CMD up --build -d

echo ""
echo "===================================================================="
echo "[3/3] Pipeline is Live!"
echo "===================================================================="
echo " - Interactive Web Dashboard : http://localhost:8000"
echo " - Swagger REST API Docs    : http://localhost:8000/docs"
echo " - Hadoop HDFS Web Console   : http://localhost:9870"
echo " - Kafka Broker Endpoint     : localhost:29092"
echo "===================================================================="
echo ""
echo "To view live logs: $COMPOSE_CMD logs -f"
echo "To stop:           $COMPOSE_CMD down"

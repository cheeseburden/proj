#!/usr/bin/env bash
set -e

echo "===================================================================="
echo "       Starting Fake News Classification Pipeline"
echo "       Kafka | Hadoop HDFS | TF-IDF NLP | Docker"
echo "===================================================================="

if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH."
    exit 1
fi

COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null; then
    COMPOSE_CMD="docker-compose"
fi

echo ""
echo "[1/2] Building and launching containers..."
$COMPOSE_CMD up --build -d

echo ""
echo "===================================================================="
echo "[2/2] Pipeline is Live!"
echo "===================================================================="
echo " - Interactive Web Dashboard : http://localhost:8000"
echo " - Swagger REST API Docs    : http://localhost:8000/docs"
echo " - Hadoop HDFS Web Console   : http://localhost:9870"
echo " - Kafka Broker Endpoint     : localhost:29092"
echo "===================================================================="
echo ""
echo "To view live logs: $COMPOSE_CMD logs -f"
echo "To stop:           $COMPOSE_CMD down"

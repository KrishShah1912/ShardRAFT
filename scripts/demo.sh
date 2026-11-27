#!/bin/bash

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo
echo "ShardRAFT Demo"
echo "=============="
echo
echo "Checking prerequisites..."

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: Docker is not installed."
    echo "Install Docker Desktop and run this command again."
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "Error: Docker Compose is unavailable."
    echo "Install or update Docker Desktop and try again."
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker is installed but is not running."
    echo "Start Docker Desktop and try again."
    exit 1
fi

echo "  Docker ............ ready"
echo "  Docker Compose .... ready"

echo
echo "Building and starting ShardRAFT..."
echo

docker compose up -d --build

echo
echo "Waiting for ShardRAFT..."

READY=0

for _ in $(seq 1 90); do
    if curl -fsS \
        http://127.0.0.1:18080/ \
        >/dev/null 2>&1; then
        READY=1
        break
    fi

    sleep 1
done

if [ "$READY" -ne 1 ]; then
    echo
    echo "Error: ShardRAFT did not become ready."
    echo
    echo "Run:"
    echo "  docker compose ps"
    echo "  docker compose logs"
    exit 1
fi

echo
echo "Checking storage cluster..."

for port in 18081 18082 18083 18084 18085; do
    if ! curl -fsS \
        "http://127.0.0.1:${port}/status" \
        >/dev/null 2>&1; then

        echo "Error: storage node on port ${port} is unavailable."
        exit 1
    fi
done

echo "  Storage nodes ..... 5/5 healthy"
echo "  Gateways .......... running"
echo "  Dashboard ......... healthy"

echo
echo "================================="
echo "ShardRAFT is ready"
echo "================================="
echo
echo "Open the dashboard:"
echo "  http://127.0.0.1:18080"
echo
echo "Stop the demo:"
echo "  make demo-down"
echo

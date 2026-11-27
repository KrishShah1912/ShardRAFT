#!/bin/bash

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo
echo "Stopping ShardRAFT demo..."
echo

docker compose down

echo
echo "ShardRAFT demo stopped."
echo "Persistent database volumes were preserved."

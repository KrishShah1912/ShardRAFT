#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT"

exec go run ./cmd/shardraft-supervisor \
        -root "$ROOT" \
        -addr 127.0.0.1:8090

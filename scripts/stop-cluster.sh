#!/bin/bash

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDS="$ROOT/.dev-cluster/pids"

if [ ! -d "$PIDS" ]; then
	echo "No development cluster is running."
	exit 0
fi

stopped=0

for file in "$PIDS"/*.pid; do
	[ -e "$file" ] || continue

	pid="$(cat "$file")"

	if kill -0 "$pid" 2>/dev/null; then
		echo "Stopping PID $pid..."
		kill "$pid" 2>/dev/null || true
		stopped=1
	fi

	rm -f "$file"
done

if [ "$stopped" -eq 1 ]; then
	sleep 1
fi

echo "ShardRAFT development cluster stopped."

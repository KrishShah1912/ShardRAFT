#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUNTIME="$ROOT/.dev-cluster"
DATA="$RUNTIME/data"
LOGS="$RUNTIME/logs"
PIDS="$RUNTIME/pids"

mkdir -p "$DATA" "$LOGS" "$PIDS"

if [ -f "$PIDS/node1.pid" ] ||
   [ -f "$PIDS/node2.pid" ] ||
   [ -f "$PIDS/node3.pid" ]; then
	echo "A development cluster may already be running."
	echo "Run: make stop-cluster"
	exit 1
fi

echo "Building shardraftd..."
go build -o "$RUNTIME/shardraftd" ./cmd/shardraftd

start_node() {
	id="$1"
	port="$2"
	peer_ids="$3"
	peer_addrs="$4"

	echo "Starting Node $id on port $port..."

	"$RUNTIME/shardraftd" \
		-id "$id" \
		-peers "$peer_ids" \
		-peer-addrs "$peer_addrs" \
		-data-dir "$DATA/node-$id" \
		-http-addr "127.0.0.1:$port" \
		-election-timeout 10 \
		-tick-interval 100ms \
		> "$LOGS/node-$id.log" 2>&1 &

	echo $! > "$PIDS/node$id.pid"
}

cleanup_on_failure() {
	echo
	echo "Cluster startup failed."
	"$ROOT/scripts/stop-cluster.sh" >/dev/null 2>&1 || true
	exit 1
}

trap cleanup_on_failure ERR

start_node \
	1 \
	8081 \
	"2,3" \
	"2=127.0.0.1:8082,3=127.0.0.1:8083"

start_node \
	2 \
	8082 \
	"1,3" \
	"1=127.0.0.1:8081,3=127.0.0.1:8083"

start_node \
	3 \
	8083 \
	"1,2" \
	"1=127.0.0.1:8081,2=127.0.0.1:8082"

sleep 3

for port in 8081 8082 8083; do
	if ! curl -fsS \
		"http://127.0.0.1:$port/status" \
		>/dev/null; then
		echo "Node on port $port did not become ready."
		exit 1
	fi
done

trap - ERR

echo
echo "ShardRAFT development cluster is running."
echo
echo "Node 1: http://127.0.0.1:8081"
echo "Node 2: http://127.0.0.1:8082"
echo "Node 3: http://127.0.0.1:8083"
echo
echo "Status:"
echo "  make cluster-status"
echo
echo "Events:"
echo "  curl -N http://127.0.0.1:8081/api/v1/events"
echo
echo "Logs:"
echo "  $LOGS"
echo
echo "Stop:"
echo "  make stop-cluster"

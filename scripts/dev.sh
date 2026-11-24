#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLEANED_UP=0
STARTED_SUPERVISOR=0

cleanup() {
	if [ "$CLEANED_UP" -eq 1 ]; then
		return
	fi

	CLEANED_UP=1

	echo
	echo "Stopping ShardRAFT development environment..."

	if [ -n "${WEB_PID:-}" ]; then
		kill "$WEB_PID" 2>/dev/null || true
	fi

	if [ "$STARTED_SUPERVISOR" -eq 1 ] &&
	   [ -n "${SUPERVISOR_PID:-}" ]; then
		kill "$SUPERVISOR_PID" 2>/dev/null || true
	fi
}

trap cleanup EXIT INT TERM

echo
echo "ShardRAFT Development Environment"
echo "================================="
echo

echo "Building latest ShardRAFT backend..."

mkdir -p "$ROOT/.dev-cluster"

go build \
	-o "$ROOT/.dev-cluster/shardraftd" \
	./cmd/shardraftd

echo "Backend build complete."
echo

if curl -fsS \
	"http://127.0.0.1:8090/health" \
	>/dev/null 2>&1; then

	echo "Supervisor already running."
else
	echo "Starting supervisor..."

	go run ./cmd/shardraft-supervisor \
		-root "$ROOT" \
		-addr 127.0.0.1:8090 &

	SUPERVISOR_PID=$!
	STARTED_SUPERVISOR=1

	for _ in $(seq 1 50); do
		if curl -fsS \
			"http://127.0.0.1:8090/health" \
			>/dev/null 2>&1; then
			break
		fi

		sleep 0.1
	done

	if ! curl -fsS \
		"http://127.0.0.1:8090/health" \
		>/dev/null 2>&1; then

		echo "Supervisor failed to start."
		exit 1
	fi

	echo "Supervisor ready on :8090."
fi

ensure_node() {
	NODE="$1"
	PORT="$((8080 + NODE))"

	if curl -fsS \
		"http://127.0.0.1:$PORT/status" \
		>/dev/null 2>&1; then

		echo "Node $NODE already online."
		return
	fi

	echo "Starting Node $NODE..."

	RESPONSE="$(
		curl -sS \
			-X POST \
			"http://127.0.0.1:8090/api/v1/control/nodes/$NODE/start"
	)" || {
		echo "Failed to request start for Node $NODE."
		return 1
	}

	for _ in $(seq 1 50); do
		if curl -fsS \
			"http://127.0.0.1:$PORT/status" \
			>/dev/null 2>&1; then

			echo "Node $NODE online on :$PORT."
			return
		fi

		sleep 0.1
	done

	echo "Node $NODE failed to become reachable."
	echo "$RESPONSE"
	return 1
}

echo
echo "Starting cluster..."

ensure_node 1
ensure_node 2
ensure_node 3

echo
echo "Waiting for Raft consensus..."

LEADER=""

for _ in $(seq 1 50); do
	for NODE in 1 2 3; do
		PORT="$((8080 + NODE))"

		STATUS="$(
			curl -fsS \
				"http://127.0.0.1:$PORT/status" \
				2>/dev/null || true
		)"

		if echo "$STATUS" |
		   grep -q '"role":2'; then

			LEADER="$NODE"
			break
		fi
	done

	if [ -n "$LEADER" ]; then
		break
	fi

	sleep 0.1
done

echo
echo "Cluster status"
echo "--------------"

for NODE in 1 2 3; do
	PORT="$((8080 + NODE))"

	if curl -fsS \
		"http://127.0.0.1:$PORT/status" \
		>/dev/null 2>&1; then

		if [ "$NODE" = "$LEADER" ]; then
			echo "Node $NODE   ONLINE   LEADER"
		else
			echo "Node $NODE   ONLINE   FOLLOWER"
		fi
	else
		echo "Node $NODE   OFFLINE"
	fi
done

if [ -n "$LEADER" ]; then
	echo
	echo "Leader elected: Node $LEADER"
else
	echo
	echo "Warning: no leader observed yet."
fi

echo
echo "Starting frontend..."
echo

cd "$ROOT/web"

npm run dev &
WEB_PID=$!

sleep 1

echo
echo "================================="
echo "ShardRAFT is ready"
echo "================================="
echo
echo "Frontend    http://localhost:5173"
echo "Supervisor  http://127.0.0.1:8090"
echo
echo "Use the browser for cluster control."
echo "Press Ctrl+C to stop the development environment."
echo

wait "$WEB_PID"

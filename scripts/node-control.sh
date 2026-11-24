#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUNTIME="$ROOT/.dev-cluster"
DATA="$RUNTIME/data"
LOGS="$RUNTIME/logs"
PIDS="$RUNTIME/pids"
BINARY="$RUNTIME/shardraftd"

ACTION="$1"
NODE="$2"

if [ -z "$ACTION" ] || [ -z "$NODE" ]; then
	echo "Usage: $0 <start|stop|restart> <node>"
	exit 1
fi

case "$NODE" in
	1)
		PORT=8081
		PEERS="2,3"
		PEER_ADDRS="2=127.0.0.1:8082,3=127.0.0.1:8083"
		;;
	2)
		PORT=8082
		PEERS="1,3"
		PEER_ADDRS="1=127.0.0.1:8081,3=127.0.0.1:8083"
		;;
	3)
		PORT=8083
		PEERS="1,2"
		PEER_ADDRS="1=127.0.0.1:8081,2=127.0.0.1:8082"
		;;
	*)
		echo "NODE must be 1, 2, or 3"
		exit 1
		;;
esac

PID_FILE="$PIDS/node${NODE}.pid"

stop_node() {
	if [ ! -f "$PID_FILE" ]; then
		echo "Node $NODE is not running."
		return
	fi

	PID="$(cat "$PID_FILE")"

	if kill -0 "$PID" 2>/dev/null; then
		echo "Stopping Node $NODE (PID $PID)..."
		kill "$PID" 2>/dev/null || true

		for _ in $(seq 1 30); do
			if ! kill -0 "$PID" 2>/dev/null; then
				break
			fi
			sleep 0.1
		done
	fi

	rm -f "$PID_FILE"

	echo "Node $NODE stopped."
}

start_node() {
	mkdir -p "$DATA" "$LOGS" "$PIDS"

	if [ -f "$PID_FILE" ]; then
		PID="$(cat "$PID_FILE")"

		if kill -0 "$PID" 2>/dev/null; then
			echo "Node $NODE is already running (PID $PID)."
			exit 1
		fi

		rm -f "$PID_FILE"
	fi

	if [ ! -x "$BINARY" ]; then
		echo "Building shardraftd..."
		cd "$ROOT"
		go build -o "$BINARY" ./cmd/shardraftd
	fi

	echo "Starting Node $NODE on port $PORT..."

	"$BINARY" \
		-id "$NODE" \
		-peers "$PEERS" \
		-peer-addrs "$PEER_ADDRS" \
		-data-dir "$DATA/node-$NODE" \
		-http-addr "127.0.0.1:$PORT" \
		-election-timeout 10 \
		-tick-interval 100ms \
		> "$LOGS/node-$NODE.log" 2>&1 &

	PID=$!
	echo "$PID" > "$PID_FILE"

	for _ in $(seq 1 50); do
		if curl -fsS \
			"http://127.0.0.1:$PORT/status" \
			>/dev/null 2>&1; then

			echo "Node $NODE started (PID $PID)."
			return
		fi

		if ! kill -0 "$PID" 2>/dev/null; then
			echo "Node $NODE exited during startup."
			tail -30 "$LOGS/node-$NODE.log"
			rm -f "$PID_FILE"
			exit 1
		fi

		sleep 0.1
	done

	echo "Node $NODE did not become ready."
	exit 1
}

case "$ACTION" in
	start)
		start_node
		;;

	stop)
		stop_node
		;;

	restart)
		stop_node
		sleep 0.5
		start_node
		;;

	*)
		echo "Unknown action: $ACTION"
		exit 1
		;;
esac

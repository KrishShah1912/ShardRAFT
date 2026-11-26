#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REQUESTS="${REQUESTS:-1000}"
KEYS="${KEYS:-100}"
REPEATS="${REPEATS:-1}"
SEED="${SEED:-42}"
CHECK_TIMEOUT="${CHECK_TIMEOUT:-30s}"
TARGETS="${TARGETS:-http://127.0.0.1:18091,http://127.0.0.1:18092}"

MODE="${1:-full}"

case "$MODE" in
  full|smoke|--dry-run)
    ;;
  -h|--help)
    echo "Usage: scripts/run_experiments.sh [full|smoke|--dry-run]"
    echo
    echo "Environment variables:"
    echo "  REQUESTS       Operations per experiment (1000)"
    echo "  KEYS           Key space size (100)"
    echo "  REPEATS        Repetitions per experiment (1)"
    echo "  SEED           Initial workload seed (42)"
    echo "  CHECK_TIMEOUT  Porcupine timeout (30s)"
    echo "  TARGETS        Comma-separated gateway URLs"
    exit 0
    ;;
  *)
    echo "Invalid mode: $MODE" >&2
    exit 2
    ;;
esac

if [[ "$#" -gt 1 ]]; then
  echo "Unexpected arguments" >&2
  exit 2
fi

for value in "$REQUESTS" "$KEYS" "$REPEATS"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "Expected positive integer: $value" >&2
    exit 2
  fi
done

if [[ ! "$SEED" =~ ^[0-9]+$ ]]; then
  echo "Invalid seed" >&2
  exit 2
fi

if [[ "$MODE" == "smoke" ]]; then
  REQUESTS=100
  KEYS=20
  REPEATS=1
fi

MATRIX=(
  "baseline uniform 0.5 128 4"
  "zipfian zipfian 0.5 128 4"
  "read-heavy uniform 0.9 128 4"
  "write-heavy uniform 0.1 128 4"
  "record-1k uniform 0.5 1024 4"
  "record-4k uniform 0.5 4096 4"
  "concurrency-1 uniform 0.5 128 1"
  "concurrency-16 uniform 0.5 128 16"
)

if [[ "$MODE" == "smoke" ]]; then
  MATRIX=(
    "baseline uniform 0.5 128 4"
  )
fi

echo "ShardRAFT Experiment Runner"
echo "==========================="
echo "Mode: $MODE"
echo "Requests: $REQUESTS"
echo "Keys: $KEYS"
echo "Repeats: $REPEATS"
echo "Seed: $SEED"
echo "Targets: $TARGETS"
echo "WAL mode: existing default configuration"
echo

if [[ "$MODE" == "--dry-run" ]]; then
  for experiment in "${MATRIX[@]}"; do
    echo "Planned experiment: $experiment"
  done

  echo "Total planned runs: $((${#MATRIX[@]} * REPEATS))"
  echo "Dry run completed; no workloads executed"
  exit 0
fi

command -v go >/dev/null
command -v python3 >/dev/null

mkdir -p bin results

echo "===== BUILD EXPERIMENT TOOLS ====="

go build -o bin/bench ./cmd/bench
go build -o bin/checker ./cmd/checker

SESSION="$(mktemp -d results/experiments-$(date +%Y%m%d-%H%M%S)-XXXXXX)"

echo "Experiment session: $SESSION"

git rev-parse HEAD > "$SESSION/git-head.txt"
git status --short > "$SESSION/git-status.txt"
go version > "$SESSION/go-version.txt"
uname -a > "$SESSION/environment.txt"

docker compose ps > "$SESSION/cluster-status.txt"

printf '%s\n' \
  "mode=$MODE" \
  "requests=$REQUESTS" \
  "keys=$KEYS" \
  "repeats=$REPEATS" \
  "seed=$SEED" \
  "targets=$TARGETS" \
  "wal_mode=default-unverified" \
  > "$SESSION/session-config.txt"

FAILED_RUNS=0
TOTAL_RUNS=0

run_experiment() {
  local name="$1"
  local distribution="$2"
  local ratio="$3"
  local size="$4"
  local concurrency="$5"
  local repetition="$6"

  local output="$SESSION/$name/repeat-$repetition"
  local run_seed=$((SEED + repetition - 1))

  mkdir -p "$output"

  TOTAL_RUNS=$((TOTAL_RUNS + 1))

  echo
  echo "===== EXPERIMENT: $name / REPEAT $repetition ====="

  local bench_exit=0

  ./bin/bench \
    -targets "$TARGETS" \
    -requests "$REQUESTS" \
    -keys "$KEYS" \
    -concurrency "$concurrency" \
    -distribution "$distribution" \
    -read-ratio "$ratio" \
    -value-size "$size" \
    -seed "$run_seed" \
    -timeout 15s \
    -output "$output" \
    2>&1 | tee "$output/benchmark.log" || bench_exit=$?

  echo "$bench_exit" > "$output/benchmark-exit.txt"

  local history_file

  history_file="$(find "$output" \
    -type f -name history.json -print -quit)"

  if [[ -z "$history_file" ]]; then
    echo "No history generated" >&2
    FAILED_RUNS=$((FAILED_RUNS + 1))
    return 0
  fi

  local run_dir
  run_dir="$(dirname "$history_file")"

  local checker_exit=0

  ./bin/checker \
    -history "$history_file" \
    -assume-empty \
    -timeout "$CHECK_TIMEOUT" \
    -json \
    > "$run_dir/checker.json" \
    2> "$output/checker-error.log" || checker_exit=$?

  echo "$checker_exit" > "$output/checker-exit.txt"

  if [[ "$bench_exit" -ne 0 || "$checker_exit" -ne 0 ]]; then
    FAILED_RUNS=$((FAILED_RUNS + 1))
    echo "Experiment unsuccessful; raw results preserved"
  else
    echo "Experiment and correctness check passed"
  fi
}

for experiment in "${MATRIX[@]}"; do
  read -r name distribution ratio size concurrency <<< "$experiment"

  for ((repeat=1; repeat<=REPEATS; repeat++)); do
    run_experiment \
      "$name" \
      "$distribution" \
      "$ratio" \
      "$size" \
      "$concurrency" \
      "$repeat"
  done
done

echo
echo "===== GENERATE EXPERIMENT SUMMARY CSV ====="

python3 - "$SESSION" <<'PY'
import csv
import json
import sys
from pathlib import Path

session = Path(sys.argv[1])

fields = [
    "experiment",
    "repetition",
    "run_id",
    "distribution",
    "concurrency",
    "keys",
    "read_ratio",
    "value_size",
    "seed",
    "requests",
    "recorded",
    "successful",
    "failed",
    "unknown",
    "pending",
    "reads",
    "writes",
    "duration_ms",
    "throughput_ops",
    "average_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "checker_verdict",
    "benchmark_exit",
    "checker_exit",
    "wal_mode",
    "valid",
    "run_directory",
]

rows = []

for path in sorted(session.rglob("summary.json")):
    run_dir = path.parent
    repetition_dir = run_dir.parent
    experiment_dir = repetition_dir.parent

    summary = json.loads(path.read_text())

    checker_path = run_dir / "checker.json"

    try:
        checker = json.loads(checker_path.read_text())
    except (OSError, ValueError):
        checker = {}

    verdict = checker.get("verdict", "UNAVAILABLE")

    def exit_code(filename):
        try:
            return int(
                (repetition_dir / filename).read_text().strip()
            )
        except (OSError, ValueError):
            return -1

    bench_exit = exit_code("benchmark-exit.txt")
    checker_exit = exit_code("checker-exit.txt")

    valid = (
        bench_exit == 0
        and checker_exit == 0
        and verdict == "LINEARIZABLE"
        and summary["successful"] == summary["requests"]
        and summary["recorded"] == summary["requests"]
        and summary["failed"] == 0
        and summary["unknown"] == 0
        and summary["pending"] == 0
    )

    row = {
        field: summary.get(field, "")
        for field in fields
    }

    row.update({
        "experiment": experiment_dir.name,
        "repetition": repetition_dir.name,
        "checker_verdict": verdict,
        "benchmark_exit": bench_exit,
        "checker_exit": checker_exit,
        "wal_mode": "default-unverified",
        "valid": str(valid).lower(),
        "run_directory": str(run_dir),
    })

    rows.append(row)

output = session / "experiment-summary.csv"

with output.open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)

print("Summary CSV:", output)
print("Recorded experiment runs:", len(rows))
print("Valid runs:", sum(row["valid"] == "true" for row in rows))

for row in rows:
    print(
        f"{row['experiment']:16s} "
        f"{row['throughput_ops']:10.2f} ops/sec "
        f"p99={row['p99_ms']:.3f} ms "
        f"verdict={row['checker_verdict']} "
        f"valid={row['valid']}"
    )
PY

echo
echo "===== EXPERIMENT SESSION COMPLETED ====="
echo "Total runs: $TOTAL_RUNS"
echo "Failed runs: $FAILED_RUNS"
echo "Results: $SESSION"

if [[ "$FAILED_RUNS" -ne 0 ]]; then
  exit 1
fi

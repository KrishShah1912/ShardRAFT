# Correctness and Performance Evaluation

## 1. Overview

ShardRAFT provides history recording, single-key linearizability
checking, configurable workload generation, automated experiments,
fault measurements, and isolated WAL synchronization measurements.

## 2. Correctness checker

The history recorder captures operation inputs, outputs, invocation
and completion timestamps, request identities, and operation outcomes.

The checker uses Porcupine to evaluate single-key histories against
the key-value state machine, including mutation deduplication.
Failed, pending, or uncertain operations prevent a conclusive
positive verdict for their affected key histories.

The experiments use fresh key namespaces and assume initially empty
state with no external writes to those namespaces.

## 3. Running the tools

Start the local deployment:

```bash
docker compose up -d
```

Build the evaluation tools:

```bash
mkdir -p bin
go build -o bin/bench ./cmd/bench
go build -o bin/checker ./cmd/checker
go build -o bin/walbench ./cmd/walbench
```

Run a configurable workload:

```bash
./bin/bench \
  -targets http://127.0.0.1:18091,http://127.0.0.1:18092 \
  -requests 1000 \
  -concurrency 4 \
  -keys 100 \
  -distribution uniform \
  -read-ratio 0.5 \
  -value-size 128 \
  -seed 42 \
  -output results
```

The benchmark also supports Zipfian key selection through
`-distribution zipfian` and `-zipf-s`.

Check a recorded history:

```bash
./bin/checker -history path/to/history.json -assume-empty -timeout 30s -json
```

Run the automated experiment matrix:

```bash
./scripts/run_experiments.sh --dry-run
./scripts/run_experiments.sh smoke
REQUESTS=1000 KEYS=100 REPEATS=1 ./scripts/run_experiments.sh full
```

## 4. Distributed performance results

The local deployment contains five storage nodes, four data shards,
a replicated metadata group, and two HTTP gateways.

Each experiment executed 1,000 operations. Across eight experiments,
all 8,000 operations succeeded and all eight histories were
classified as LINEARIZABLE.

| Experiment | Operations | Ops/sec | p50 ms | p95 ms | p99 ms | Verdict |
|---|---:|---:|---:|---:|---:|---|
| baseline | 1000 | 571.60 | 10.775 | 12.314 | 14.197 | LINEARIZABLE |
| concurrency-1 | 1000 | 147.41 | 2.931 | 12.929 | 14.367 | LINEARIZABLE |
| concurrency-16 | 1000 | 834.72 | 15.044 | 44.536 | 60.005 | LINEARIZABLE |
| read-heavy | 1000 | 1483.75 | 1.408 | 11.600 | 12.481 | LINEARIZABLE |
| record-1k | 1000 | 568.49 | 4.500 | 12.474 | 13.160 | LINEARIZABLE |
| record-4k | 1000 | 520.26 | 4.861 | 13.205 | 14.740 | LINEARIZABLE |
| write-heavy | 1000 | 375.63 | 11.388 | 12.538 | 13.432 | LINEARIZABLE |
| zipfian | 1000 | 595.72 | 10.674 | 12.130 | 13.357 | LINEARIZABLE |

Throughput counts successful operations per elapsed second.
Latency percentiles are calculated from successful operations.

The experiment matrix varies key distribution, read/write ratio,
record size, and worker concurrency.

## 5. Failover and recovery measurements

Run the focused Docker fault measurement:

```bash
SHARDRAFT_DOCKER_TEST=1 go test -v ./test \
  -run '^TestDockerFaultMeasurements$' -count=1 -timeout=120s
```

| Measurement | Duration (ms) |
|---|---:|
| docker_stop | 325.293 |
| post_stop_election_wait | 922.101 |
| end_to_end_failover | 1337.355 |
| node_startup | 440.514 |
| replica_recovery | 948.523 |

Shard: 1.
Original leader: node 2, term 5.
New leader: node 1, term 6.

The test verifies committed value preservation, duplicate request
protection, successful operations after failover, and replica catch-up.

Failover is measured from stop initiation to successful read/write
verification. Recovery is measured from restart initiation until
the recovered replica reaches the target applied index.

These measurements include Docker and polling overhead and use
graceful container stops rather than abrupt crashes.

## 6. WAL synchronization microbenchmark

```bash
./bin/walbench \
  -records 10000 \
  -record-size 1024 \
  -sync-interval 10ms \
  -output results
```

The isolated benchmark writes length-prefixed, checksummed records
to temporary files and compares three synchronization policies.

| Mode | Durability | Records | Sync calls | Records/sec | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| always | stronger | 10000 | 10000 | 268.97 | 3.960583 | 4.077292 | 6.054459 |
| timed | weaker | 10000 | 11 | 63222.73 | 0.005500 | 0.018208 | 0.041959 |
| no-sync | weaker | 10000 | 0 | 121306.53 | 0.003250 | 0.012334 | 0.077167 |

Always-sync explicitly synchronizes every record.
Timed-sync checks the 10 ms interval at write boundaries and
synchronizes remaining records at the end of the workload.
No-sync performs no explicit file synchronization.

Timed-sync and no-sync are weaker durability modes.
The production WAL configuration is unchanged.

## 7. Raw experiment artifacts

Generated artifacts are stored under `results/` and are git-ignored.
The tools generate raw results automatically; result files must
not be manually edited.

| Artifact | Purpose |
|---|---|
| operations.csv | Individual distributed operation measurements |
| history.json | Recorded correctness history |
| summary.json | Workload performance statistics |
| config.json | Workload configuration |
| checker.json | Porcupine correctness report |
| experiment-summary.csv | Aggregated workload measurements |
| fault-metrics.csv | Failover and recovery measurements |
| wal-sync-summary.csv | Synchronization policy comparison |
| wal-sync-operations.csv | Individual WAL-style record timings |

Original measurement sessions:

- Distributed experiments: `results/experiments-20261126-145049-NGuY3h`
- Fault measurements: `results/fault-measurement-1795729936119465000`
- WAL microbenchmark: `results/wal-sync-1795729983272073000`

## 8. Limitations

- Each reported experiment has one repetition.
- Results are specific to the local execution environment.
- Linearizable recorded histories are not a formal proof of correctness.
- The checker evaluates single-key operations, not multi-key transactions.
- Fault measurements do not establish power-loss durability.
- Replica recovery measures observed catch-up, not isolated disk replay.
- WAL measurements isolate file synchronization costs and do not
  exercise the production segmented WAL or distributed replication.
- Timed synchronization is checked at write boundaries, not by
  a background timer.
- Timed-sync and no-sync throughput must not be reported as
  production distributed ShardRAFT throughput.
- Distributed workload measurements use the existing default WAL
  configuration, recorded as default-unverified.

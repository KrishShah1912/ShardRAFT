# Integration Validation

## Tested Versions

- Baseline: 8c28c16
- Integrated implementation: d25d418
- Feature: Automatic request routing, leader discovery,
  and safe mutation retries

The baseline and integrated implementations were tested in
separate Git worktrees with isolated persistent storage.

## Regression Testing

| Validation | Result |
|---|---|
| Backend routing tests | Passed |
| Frontend routing tests | 22/22 passed |
| Full backend regression | Passed |
| Race detection | Passed |
| Go vet | Passed |
| Go build | Passed |
| Frontend production build | Passed |
| TypeScript validation | Passed |
| Baseline Playwright E2E | 3/3 passed |
| Integrated Playwright E2E | 3/3 passed |

## End-to-End Testing

| Test | Baseline | Integrated |
|---|---|---|
| Leader failover and recovery | Passed | Passed |
| Single-node failure tolerance | Passed | Passed |
| Dashboard smoke test | Passed | Passed |
| Observed failover | 3031 ms | 3032 ms |

The integrated cluster elected a replacement leader,
accepted additional writes, and recovered the former leader.

## Benchmark Configuration

All benchmarks used a local three-node cluster and
128-byte values with a five-second request timeout.

| Workload | Requests | Concurrency | Read Ratio |
|---|---:|---:|---:|
| Write-only | 100 | 4 | 0% |
| Mixed | 200 | 8 | 80% |

## Write-Only Results

| Metric | Baseline | Integrated |
|---|---:|---:|
| Successful requests | 100 | 100 |
| Failed requests | 0 | 0 |
| Throughput (ops/sec) | 55.01 | 58.83 |
| Average latency (ms) | 72.693 | 67.524 |
| P50 latency (ms) | 71.056 | 70.064 |
| P95 latency (ms) | 105.144 | 103.741 |
| P99 latency (ms) | 107.107 | 105.929 |

## Mixed-Workload Results

| Metric | Baseline | Integrated |
|---|---:|---:|
| Successful requests | 200 | 200 |
| Failed requests | 0 | 0 |
| Reads | 161 | 161 |
| Writes | 39 | 39 |
| Throughput (ops/sec) | 282.97 | 255.12 |
| Average latency (ms) | 27.192 | 30.371 |
| P50 latency (ms) | 11.630 | 23.931 |
| P95 latency (ms) | 123.979 | 85.000 |
| P99 latency (ms) | 164.677 | 112.935 |
| Average read latency (ms) | 10.739 | 21.380 |
| Average write latency (ms) | 95.112 | 67.486 |

## Replication Verification

The integrated cluster finished with all three nodes
synchronized at commit and applied index 175.

Commit progression:

- Initial post-E2E index: 4
- Write-only benchmark: 120
- Mixed benchmark: 175

Each benchmark included 16 preparation writes.

## Storage Recovery

All six original database records were recovered on all
three nodes after launching the integrated implementation.

The original storage directory was backed up and remained
unchanged throughout isolated integration testing.

## Compatibility

The following teammate-owned files were verified unchanged
relative to the baseline:

- cmd/shardraft-bench/
- web/e2e/
- web/playwright.config.ts
- web/package-lock.json

## Interpretation and Limitations

Both versions completed all 300 measured workload requests
without failures.

The integrated implementation showed higher write-only
throughput and lower mixed-workload tail latency in these
individual runs, but lower mixed-workload throughput and
higher average read latency.

Additional leader verification may contribute to read
overhead, but the measurements do not establish causation.

These are preliminary local smoke benchmarks. Starting
database sizes, leader identities, and runtime conditions
differed between runs. The benchmark discovers the leader
before executing its workload, so these results do not
directly measure follower-forwarding overhead.

Repeated controlled experiments are required before
making production performance or scalability claims.

Passing these tests does not constitute a formal proof
of Raft safety or linearizability.

# Step 8: Multi-Raft Sharding and Metadata

## Overview

ShardRAFT supports four physical data shards and an
independent replicated metadata group across five
storage nodes.

The existing single-shard deployment mode remains
available. Multi-Raft mode is enabled using the
`-multi-raft` daemon flag.

## Shard Placement

| Group | Slot Range | Replicas |
|---|---|---|
| S1 | 0-16383 | n1, n2, n3 |
| S2 | 16384-32767 | n2, n3, n4 |
| S3 | 32768-49151 | n3, n4, n5 |
| S4 | 49152-65535 | n4, n5, n1 |
| Meta | None | n1, n2, n3 |

The 65,536 hash slots are divided into four equal
ranges of 16,384 slots.

The metadata group uses group ID 5 and is excluded
from the data-shard routing map.

## Implementation

- Deterministic CRC32-based key-to-slot hashing.
- Independent Raft groups for each physical shard.
- Per-group WAL and storage isolation.
- Group-aware gRPC Raft message dispatch.
- Group-specific outbound Raft transport.
- Persistent metadata state machine.
- Versioned shard map with automatic bootstrap.
- Metadata discovery through the existing MetaService.
- Shard-aware GET, PUT, DELETE and CAS dispatch.
- Existing client-side routing and leader redirection.
- Existing request deduplication and linearizable reads.
- Multi-Raft daemon status reporting.

## Validation Results

### Group Management

Focused tests verified local replica placement,
independent group replication, storage recovery and
cross-group isolation using an in-process simulated
network.

### Metadata

Metadata tests verified:

- Replication across three metadata replicas.
- Versioned shard map updates.
- Metadata WAL recovery.
- Automatic bootstrap over real gRPC.
- Consistent discovery responses from all three replicas.
- Correct shard ranges and replica addresses.
- Rejection of uncommitted metadata discovery.
- Rejection of invalid metadata messages.

### Deterministic Routing

The client and server produced identical hash slots
for all 10,000 tested keys.

| Shard | Keys |
|---|---:|
| S1 | 2500 |
| S2 | 2494 |
| S3 | 2502 |
| S4 | 2504 |
| Total | 10000 |

All 28 invalid routing operation checks passed.
Rejected requests left local Raft statuses unchanged.

### End-to-End Integration

Test: TestMultiShardEndToEnd

Five daemon instances communicated through real gRPC
within one test process.

| Shard | Tested Slot | PUT | Linearizable GET |
|---|---:|---|---|
| S1 | 15971 | PASS | PASS |
| S2 | 24399 | PASS | PASS |
| S3 | 39766 | PASS | PASS |
| S4 | 64122 | PASS | PASS |

Additional successful checks:

- Automatic metadata discovery.
- Duplicate mutation did not overwrite a newer value.
- Successful compare-and-swap.
- Compare-and-swap mismatch rejection.
- Successful deletion.
- Independent values preserved across other shards.
- Graceful cluster shutdown.

Observed integration test duration: 1.41 seconds.
Observed package test duration: 1.785 seconds.

## Reproduce Focused Validation

```bash
go test -v -count=1 -run '^TestMultiKVGRPC' ./cmd/shardraftd

go test -v -count=1 -run '^TestMultiMetaGRPC' ./cmd/shardraftd

go test -v -count=1 -timeout=60s \
  -run '^TestMultiShardEndToEnd$' ./cmd/shardraftd
```

## Limitations

- Shard placement is fixed to the initial topology.
- Live shard migration and rebalancing are not implemented.
- Metadata discovery reads locally committed metadata.
- Metadata discovery is not a linearizable operation.
- Dynamic metadata versions are not synchronized with
  physical shard ownership.
- The end-to-end test validates normal operation rather
  than abrupt process failure or container recovery.
- Duplicate handling was validated through observable
  KV behavior, not a commit-index measurement.
- These tests do not constitute a formal correctness proof.

Docker deployment, container restart, leader failure
and follower failure validation are reserved for Step 9.

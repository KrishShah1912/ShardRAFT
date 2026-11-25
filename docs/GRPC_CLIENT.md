# Shard-Aware gRPC Client

## Overview

ShardRAFT implements a reusable Go gRPC client supporting
metadata discovery, logical hash-slot routing, leader-aware
request routing, bounded retries, stable mutation identities,
and linearizable reads.

This implementation completes Step 7.20.

## Current Architecture

| Property | Value |
|---|---|
| Active shards | 1 |
| Logical hash slots | 65536 |
| Shard ID | 1 |
| Slot range | 0-65535 |
| Routing epoch | 1 |
| Hash algorithm | CRC32 IEEE |
| Default read mode | Linearizable |
| Distributed test topology | Three replicas |

All logical slots belong to shard 1. Physical data partitioning
and multiple independent Raft groups are not implemented.

## Implementation Components

| File | Responsibility |
|---|---|
| hash_slot.go | CRC32 logical slot calculation |
| shard_map.go | Metadata validation and slot lookup |
| metadata_cache.go | Thread-safe metadata caching |
| connection_manager.go | Reusable gRPC connections |
| leader_router.go | Leader-aware endpoint selection |
| metadata_refresh.go | Metadata discovery and refresh |
| retry.go | Bounded retries and mutation identities |
| client.go | Unified KV operations |

All client implementation files reside in internal/client.

## Hash-Slot Routing

The logical hash slot is calculated as:

    slot = CRC32_IEEE(key) % 65536

The routing table validates complete slot ownership and rejects
overlapping ranges, missing slots, and malformed metadata.

## Metadata Management

The client discovers shard ownership through MetaService.

Validated routing snapshots are cached with synchronized access.
Older routing epochs are rejected.

Metadata refresh occurs when the cache expires, metadata is
unavailable, the application requests a refresh, or the server
returns WRONG_SHARD.

## Leader-Aware Routing

The client prefers the known leader and can attempt alternative
replicas when the preferred endpoint becomes unavailable.

NOT_LEADER responses provide advisory leader hints.

Hints are validated against known shard membership, routing
epochs, and observed Raft terms.

Replica addresses are obtained from discovered metadata.

## Retry Configuration

| Setting | Default |
|---|---|
| Maximum attempts | 5 |
| Per-attempt timeout | 2 seconds |
| Initial backoff | 25 milliseconds |
| Maximum backoff | 250 milliseconds |
| Metadata TTL | 30 seconds |
| Metadata attempt timeout | 1 second |

Retryable routing responses include NOT_LEADER, WRONG_SHARD,
LEADER_UNAVAILABLE, and OUTCOME_UNKNOWN.

Selected transient gRPC failures are also retryable.

Permanent validation failures and request identity conflicts
are not automatically retried.

## Mutation Identity

Each logical mutation has a client ID and request ID.

Automatic retries preserve the original mutation identity
and payload.

Explicit identity reuse supports application-level replay
after an uncertain mutation result.

An unknown mutation outcome does not prove that the mutation
failed. Applications should retain the original identity and
payload when replaying unresolved operations.

Durable server-side deduplication was validated through
duplicate requests, leader failover, and quorum recovery.

## Client API

| Method | Purpose |
|---|---|
| DefaultConfig | Create default configuration |
| NewClient | Initialize the client |
| ClientID | Return the client identifier |
| RefreshMetadata | Refresh routing metadata |
| NewMutationIdentity | Generate a mutation identity |
| Get | Perform a linearizable read |
| GetLocal | Perform a local read |
| Put | Store or update a value |
| PutWithIdentity | Execute PUT with explicit identity |
| Delete | Remove a key |
| DeleteWithIdentity | Execute DELETE with explicit identity |
| CompareAndSwap | Conditionally update a value |
| CompareAndSwapWithIdentity | Execute CAS with explicit identity |
| Close | Release managed connections |

Get uses quorum-confirmed linearizable reads by default.

GetLocal explicitly permits local reads that may be stale.

The client is currently an internal Go package intended for
use within the ShardRAFT module.

## Validation Results

Validation completed on November 25, 2026.

| Validation | Result |
|---|---|
| Dedicated unified client tests | 14/14 passed |
| Repeated unified client tests | 700/700 passed |
| Distributed integration tests | 12/12 passed |
| Repeated distributed integration tests | 120/120 passed |
| Client unit-test package coverage | 92.5% |
| Combined client package coverage | 93.9% |
| Normal backend regression | 14/14 packages passed |
| Backend race detection | 14/14 packages passed |
| Shuffled backend regression | 14/14 packages passed |
| Total backend package test executions | 42/42 passed |
| Frontend unit tests | 25/25 passed |
| Backend build | Passed |
| Frontend production build | Passed |
| Static analysis | Passed |
| Module verification | Passed |
| Formatting and whitespace | Passed |
| Protobuf contracts | Unchanged |
| Module dependencies | Unchanged |

Two additional Go packages contain no test files.

## Distributed Integration

Production daemon integration tests validated:

- Bootstrap metadata discovery.
- Distributed CRUD and compare-and-swap.
- Linearizable and local reads.
- Automatic leader failover.
- Bootstrap endpoint fallback.
- Durable mutation deduplication.
- Explicit metadata refresh.
- Quorum loss and recovery.
- Concurrent client operations.
- Deadline enforcement.
- Pre-dispatch failure classification.
- Client and daemon cleanup.

Duplicate mutation replay and cross-leader replay completed
without additional mutation commits in the tested scenarios.

Quorum recovery restored stopped replicas using existing
storage and replayed the unresolved mutation with its
original identity.

## Reproducing Validation

Run the following commands from the repository root:

    go test ./internal/client -count=1
    go test -race ./internal/client -count=1

    go test ./cmd/shardraftd -run '^TestDistributedClient' -count=1
    go test -race ./cmd/shardraftd -run '^TestDistributedClient' -count=1

    go test ./cmd/shardraftd -run '^TestDistributedClient' -shuffle=on -count=10 -timeout=600s

    go test ./... -count=1 -timeout=600s
    go test -race ./... -count=1 -timeout=600s
    go test ./... -shuffle=on -count=1 -timeout=600s

    go test ./internal/client ./cmd/shardraftd -coverpkg=./internal/client -coverprofile=/tmp/shardraft-client-coverage.out -count=1 -timeout=600s

    go tool cover -func=/tmp/shardraft-client-coverage.out

    go build ./...
    go vet ./...
    go mod verify

    (cd web && npm test)
    (cd web && npm run build)

## Known Limitations

The following capabilities remain future work:

- Physical data partitioning.
- Multiple independent Raft groups.
- Dynamic shard splitting and migration.
- Automatic shard rebalancing.
- Dynamic ownership epoch advancement.
- Replicated metadata consensus.
- Shard-aware command-line client.
- Integration of the new client into the HTTP gateway.
- Production authentication and transport security.

Metadata refresh tests include synthetic ownership epoch changes.
Production ownership remains static.

Distributed failover tests use graceful shutdown rather than
abrupt process termination.

Browser end-to-end tests were not rerun for this milestone.

Applications must retain mutation identities across their own
process restarts when unresolved operations require replay.

The default gRPC transport uses insecure credentials for
local development.

Test coverage does not constitute a formal correctness proof.

## Related Documentation

- [gRPC Metadata Service](GRPC_METADATA_SERVICE.md)
- [gRPC KV Service](GRPC_KV_SERVICE.md)
- [ReadIndex](READ_INDEX.md)
- [Request Routing](REQUEST_ROUTING.md)
- [Development Checklist](CHECKLIST.md)

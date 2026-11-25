# gRPC Metadata Service

## 1. Overview

ShardRAFT exposes a gRPC metadata discovery service that allows
clients to discover shard ownership, routing epochs, replica
membership, replica endpoints, and the locally observed Raft leader.

MetaService runs on the same production gRPC listener as:

- RaftService
- AdminService
- KVService

The implementation uses the existing protobuf definitions in
proto/meta.proto without introducing additional dependencies.

## 2. Architecture

The current implementation supports a static single-shard
deployment.

| Property | Value |
|---|---|
| Total logical hash slots | 65536 |
| Shard ID | 1 |
| Initial routing epoch | 1 |
| First slot | 0 |
| Last slot | 65535 |
| Slot range semantics | Inclusive |
| Replica membership | Configured Raft membership |
| Leader discovery | Local Raft status snapshot |

All logical hash slots belong to shard 1.

The metadata routing epoch uses the same initial epoch as
KVService to ensure consistency between discovery and request
validation.

## 3. RPC Interface

### GetShardMap

Returns the complete shard ownership map.

The response includes:

- Routing epoch
- Total logical hash slots
- Shard identifiers
- Shard routing epochs
- Inclusive slot ranges
- Replica identifiers
- Replica gRPC endpoints
- Locally observed leader identifiers

### GetShard

Returns metadata for a requested shard identifier.

Behavior:

| Request | Result |
|---|---|
| Existing shard | found=true with shard metadata |
| Unknown positive shard ID | found=false |
| Zero shard ID | InvalidArgument |
| Missing request | InvalidArgument |
| Unavailable local shard | Unavailable |
| Canceled context | Canceled |
| Expired deadline | DeadlineExceeded |

The unavailable-shard behavior applies to requests requiring
a local status snapshot. Unknown positive shard identifiers
can be rejected without accessing local shard state.

## 4. Replica Discovery

Replica membership is constructed from the local node and
configured Raft peers.

Replica identifiers are deduplicated and returned in ascending
node ID order.

The service copies constructor inputs and constructs independent
response objects to prevent callers from modifying internal
metadata state.

Replica membership represents configured ownership rather than
the set of currently reachable nodes.

An unavailable replica remains part of the shard map.

## 5. Endpoint Advertisement

Replica endpoints use configured gRPC addresses rather than
HTTP transport addresses.

The local endpoint preserves the configured hostname or IP
address and uses the actual bound listener port.

This supports dynamically allocated listener ports.

Wildcard listeners, including 0.0.0.0 and ::, do not identify
a concrete client destination and therefore advertise an
empty local endpoint.

Missing peer endpoints are represented by empty addresses
without removing replica membership.

Clients must treat missing addresses as unavailable routing
information.

## 6. Leader Discovery

Leader information is obtained from a synchronized local
Raft status snapshot.

The metadata service applies the following rules:

- A local leader advertises its own node identifier.
- A follower advertises its observed leader identifier.
- A candidate advertises leader ID zero.
- Unknown leaders are represented by leader ID zero.
- Leader identifiers outside configured membership are rejected.

Leader changes do not modify the routing epoch.

The routing epoch describes shard ownership configuration,
not the Raft election term.

Leader information is advisory and may be stale during
elections, network failures, or quorum loss.

Metadata discovery does not establish linearizable leadership
or guarantee that a KV mutation can be committed.

## 7. Production Integration

MetaService is registered during production gRPC server
initialization.

The service reuses the existing daemon listener, lifecycle,
Raft status interface, and configured peer addresses.

No additional listener or background metadata worker is needed.

Metadata requests do not append Raft log entries or modify
persistent KV state.

No protobuf regeneration or dependency changes are required.

## 8. Implementation Files

| File | Responsibility |
|---|---|
| cmd/shardraftd/meta_grpc.go | Metadata RPC implementation |
| cmd/shardraftd/meta_grpc_address.go | Local endpoint advertisement |
| cmd/shardraftd/grpc_server.go | Production service registration |
| cmd/shardraftd/meta_grpc_test.go | Metadata unit tests |
| cmd/shardraftd/meta_grpc_address_test.go | Endpoint validation tests |
| cmd/shardraftd/meta_grpc_integration_test.go | Real gRPC integration tests |
| cmd/shardraftd/meta_grpc_daemon_test.go | Distributed production daemon tests |

## 9. Validation Results

Validation was performed on macOS ARM64 using Go 1.27.3.

### Metadata Testing

- 29 metadata test functions passed.
- 16 core metadata unit test functions passed.
- 15 endpoint advertisement test cases passed.
- 7 real gRPC integration tests passed.
- 5 distributed production daemon tests passed.
- Metadata race detection passed.
- Repeated shuffled metadata stress testing passed.
- 50 distributed stress test executions passed.

### Distributed Validation

Three-node production daemon tests verified:

- Consistent replica discovery across all nodes.
- Shard lookup from leaders and followers.
- Automatic leader failover.
- Stable shard ownership across leader changes.
- Stable routing epoch across leader changes.
- Concurrent metadata discovery and KV mutations.
- Metadata availability during quorum loss.

The concurrent discovery test successfully executed:

- 1200 GetShardMap RPCs.
- 1200 GetShard RPCs.
- 20 KV mutations.

The observed failover changed the leader from node 1 to
node 2 and advanced the Raft term from 1 to 2 while
preserving routing epoch 1.

### Source Statement Coverage

| Function | Coverage |
|---|---|
| newMetaGRPCServer | 100.0% |
| shardInfo | 93.8% |
| GetShardMap | 100.0% |
| GetShard | 100.0% |
| metaGRPCLocalAddress | 100.0% |

Metadata implementation statement coverage:

- Covered statements: 81 of 83.
- Metadata source coverage: 97.59%.

The targeted metadata test run reported 36.9% overall daemon
package coverage. This is distinct from metadata-specific
source coverage.

### Full System Regression

- All 13 backend packages containing tests passed.
- All 13 tested packages passed race detection.
- All 13 tested packages passed shuffled regression.
- Two packages contain no test files.
- Backend build passed.
- Go static analysis passed.
- Go module verification passed.
- All 25 frontend unit tests passed.
- Frontend production build passed.
- Formatting and whitespace validation passed.

No protobuf or dependency changes were introduced.

## 10. Reproducing Validation

Run metadata tests:

    go test ./cmd/shardraftd -run '^TestMetaGRPC' -count=1

Run metadata race detection:

    go test -race ./cmd/shardraftd -run '^TestMetaGRPC' -count=1

Run distributed stress tests:

    go test ./cmd/shardraftd -run '^TestMetaGRPCDaemon' \
      -count=10 -shuffle=on -timeout=300s

Run complete backend validation:

    go test ./... -count=1 -timeout=600s
    go test -race ./... -count=1 -timeout=600s
    go test ./... -count=1 -shuffle=on -timeout=600s
    go build ./...
    go vet ./...
    go mod verify

Run frontend compatibility checks:

    cd web
    npm test
    npm run build

## 11. Known Limitations

The current metadata service intentionally uses static
single-shard ownership.

The following capabilities remain future work:

- Dynamic multi-shard ownership.
- Shard splitting and migration.
- Dynamic replica membership.
- Persistent metadata configuration.
- Routing epoch advancement after ownership changes.
- Explicit advertised-address configuration for wildcard listeners.
- Dynamic ownership changes across multiple physical shards.

The current validation also has the following boundaries:

- Leader failover uses graceful daemon shutdown rather than
  abrupt process termination.
- Concurrent metadata discovery and leader failover are tested
  separately rather than simultaneously.
- Metadata availability without quorum does not establish
  KV read or write availability.
- Browser end-to-end tests were not executed for this milestone.
- Frontend compatibility was validated through unit tests and
  production compilation.

## 12. Conclusion

The gRPC metadata service provides a validated discovery
interface for the current ShardRAFT deployment.

Clients can discover static shard ownership, replica membership,
gRPC endpoints, routing epochs, and observed leader information
through the shared production gRPC listener.

The metadata service is consumed by the shard-aware gRPC client
implemented in Step 7.20. Client-side hash-slot routing, metadata
refresh, leader-aware retries, and unified KV operations are
implemented and validated.

See [GRPC_CLIENT.md](GRPC_CLIENT.md) for complete client documentation.

Physical multi-shard support remains future work.

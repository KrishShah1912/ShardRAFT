# Build checklist

## Phase 0: repository and tooling
- [ ] T0.1 Clean clone builds
- [ ] T0.2 Protocol generation is repeatable
- [ ] T0.3 Five-node Compose cluster starts
- [ ] T0.4 Admin.Status responds on each node
- [ ] T0.5 CI passes

## Phase 1: simulator and elections
- [ ] T1.1–T1.11 pass
- [ ] Randomized election tests pass
- [ ] Race detector passes

## Phase 2: replication and commit
- [ ] T2.1–T2.10 pass
- [ ] T2.11–T2.12 pass across at least 500 seeds
- [ ] Race detector passes

## Step 7: request routing and integration

- [x] Automatic leader discovery
- [x] Follower-to-leader request forwarding
- [x] Bounded request retries
- [x] Mutation identity preservation
- [x] Duplicate mutation handling
- [x] Structured routing errors
- [x] Frontend automatic routing
- [x] Backend routing tests
- [x] Frontend routing tests
- [x] Live leader failover validation
- [x] Quorum loss and recovery validation
- [x] Original persistent storage recovery
- [x] Teammate implementation integration
- [x] Full backend regression and race tests
- [x] Integrated Playwright E2E tests
- [x] Preliminary throughput and latency benchmarks
- [x] Request routing documentation
- [x] Integration validation documentation

## Step 7.18.9: gRPC KV service daemon integration

- [x] Register KVService on the production gRPC listener
- [x] Preserve RaftService and AdminService registration
- [x] Connect the persistent production shard
- [x] Configure gRPC leader-address hints
- [x] Validate peer-address mapping
- [x] Validate real gRPC CRUD and CAS operations
- [x] Validate default linearizable reads
- [x] Validate explicit follower LOCAL reads
- [x] Validate durable request deduplication
- [x] Validate three-node gRPC replication
- [x] Validate automatic production leader failover
- [x] Validate cross-leader request deduplication
- [x] Validate mutation safety during quorum loss
- [x] Validate graceful shutdown and listener cleanup
- [x] Pass repeated cluster and daemon stress tests
- [x] Pass complete backend regression
- [x] Pass complete backend race detection
- [x] Pass shuffled backend regression
- [x] Preserve HTTP compatibility
- [x] Pass frontend unit tests
- [x] Pass frontend production build
- [x] Pass existing Playwright browser tests
- [x] Complete integration documentation

## Step 7.19 - gRPC Metadata Service

- [x] Synchronize and validate the main branch.
- [x] Inspect metadata protobuf definitions.
- [x] Create the metadata feature branch.
- [x] Implement the core metadata gRPC service.
- [x] Implement static ownership of 65536 logical slots.
- [x] Maintain routing epoch consistency with KVService.
- [x] Implement GetShardMap and GetShard RPCs.
- [x] Implement Raft leader and replica endpoint discovery.
- [x] Validate metadata behavior through unit tests.
- [x] Register MetaService on the production gRPC listener.
- [x] Validate distributed metadata discovery and leader failover.
- [x] Complete backend regression, race and coverage validation.
- [x] Complete metadata service documentation.
- [x] Commit, push and merge the metadata service pull request (PR #13).

### Validation Summary

- 29 metadata test functions passed.
- 97.59% metadata source statement coverage (81/83).
- 50 distributed stress test executions passed.
- 2400 metadata RPCs and 20 KV mutations per concurrent test.
- All 13 tested backend packages passed normal, race and shuffled tests.
- All 25 frontend unit tests passed.
- Backend and frontend builds passed.
- Static analysis and module verification passed.
- Existing protobuf definitions and dependencies preserved.

See [GRPC_METADATA_SERVICE.md](GRPC_METADATA_SERVICE.md)
for architecture, RPC behavior, validation results and limitations.


## Step 7.20 - Shard-Aware gRPC Client

- [x] 7.20.1 Inspect existing client infrastructure.
- [x] 7.20.2 Implement CRC32 hash-slot calculation.
- [x] 7.20.3 Implement shard map validation and slot lookup.
- [x] 7.20.4 Implement thread-safe metadata caching.
- [x] 7.20.5 Implement gRPC connection management.
- [x] 7.20.6 Implement leader-aware request routing.
- [x] 7.20.7 Implement metadata refresh on WRONG_SHARD.
- [x] 7.20.8 Implement bounded retries and stable mutation identities.
- [x] 7.20.9 Implement unified shard-aware KV operations.
- [x] 7.20.10 Complete dedicated client unit testing.
- [x] 7.20.11 Complete distributed integration testing.
- [x] 7.20.12 Complete final regression and documentation.
- [x] Merge client implementation into main through PR #13.

### Validation Summary

- 14 unified client tests passed.
- 700 repeated unified client test executions passed.
- 12 distributed integration tests passed.
- 120 repeated distributed integration test executions passed.
- 92.5% client unit-test package coverage.
- 93.9% combined client statement coverage.
- 42 backend package test executions passed.
- 25 frontend unit tests passed.
- Backend and frontend builds passed.
- Static analysis and module verification passed.
- Protobuf contracts and module dependencies preserved.

Browser E2E tests were not rerun for this milestone.

See [GRPC_CLIENT.md](GRPC_CLIENT.md) for implementation
details, validation results, and remaining limitations.

Physical sharding and multiple Raft groups remain future work.


## Step 7.21 - HTTP-gRPC Gateway

- [x] 7.21.1 Synchronize and validate the baseline.
- [x] 7.21.2 Inspect gateway architecture and API contracts.
- [x] 7.21.3 Implement gateway configuration and client interface.
- [x] 7.21.4 Implement the HTTP GET handler.
- [x] 7.21.5 Implement the HTTP PUT handler.
- [x] 7.21.6 Implement the HTTP DELETE handler.
- [x] 7.21.7 Implement the HTTP CAS handler.
- [x] 7.21.8 Implement structured HTTP error translation.
- [x] 7.21.9 Implement server lifecycle and graceful shutdown.
- [x] 7.21.10 Complete gateway unit and lifecycle tests.
- [x] 7.21.11 Complete distributed integration testing.
- [x] 7.21.12 Complete final documentation and merge.

### Distributed Integration Validation

- [x] Inspect distributed testing infrastructure.
- [x] Validate bootstrap discovery and CRUD.
- [x] Validate leader failover and durable deduplication.
- [x] Validate request identity conflicts.
- [x] Implement committed conflict confirmation.
- [x] Validate quorum loss and recovery.
- [x] Validate concurrent distinct mutations.
- [x] Validate concurrent identical mutations.
- [x] Validate concurrent conflicting identities.
- [x] Validate concurrent CAS contention.
- [x] Validate concurrent DELETE operations.
- [x] Validate replica convergence.
- [x] Validate complete backend unavailability.
- [x] Validate gateway restart and data persistence.
- [x] Validate durable deduplication after gateway restart.
- [x] Complete distributed stress and race regression.

### Finalization

- [x] 7.21.12.1 Inspect documentation and final changes.
- [x] 7.21.12.2 Create dedicated gateway documentation.
- [x] 7.21.12.3 Update README and implementation checklist.
- [x] 7.21.12.4 Complete final pre-commit validation.
- [x] 7.21.12.5 Commit, push, and merge the pull request.

### Validation Summary

- 78 gateway test functions.
- 59 gateway unit test functions.
- 19 distributed integration test functions.
- 190 distributed test executions passed.
- 95.5% gateway unit statement coverage.
- 92.5% gRPC client statement coverage.
- 100% statement coverage for all four KV handlers.
- No race warnings reported in the final regression.
- Complete backend normal and race regression passed.
- Build, static analysis, and module verification passed.
- Existing Go module dependencies preserved.
- Additive committed-conflict protobuf extension validated.

See [HTTP_GRPC_GATEWAY.md](HTTP_GRPC_GATEWAY.md)
for architecture, API behavior, validation results,
and remaining limitations.

The gateway is validated against a real three-replica,
single-shard deployment. Physical multi-shard deployment
and exhaustive crash recovery verification remain
future work.


## Step 7.22 - gRPC Command-Line Client

- [x] 7.22.1 Implement CLI configuration and command parsing.
- [x] 7.22.2 Implement client initialization and lifecycle.
- [x] 7.22.3 Implement GET.
- [x] 7.22.4 Implement PUT.
- [x] 7.22.5 Implement DELETE.
- [x] 7.22.6 Implement compare-and-swap.
- [x] 7.22.7 Validate mutation identity safety.
- [x] 7.22.8 Validate output and exit-code contracts.
- [x] 7.22.9 Complete comprehensive unit validation.
- [x] 7.22.10 Complete distributed integration validation.
- [x] 7.22.11 Complete final repository regression.
- [ ] 7.22.12 Complete documentation and merge.

### Distributed Integration Validation

- [x] Validate the distributed CLI test harness.
- [x] Validate CRUD, CAS, and read modes.
- [x] Validate bootstrap discovery and fallback.
- [x] Validate leader failover and recovery.
- [x] Validate durable request deduplication.
- [x] Validate conflicting mutation identities.
- [x] Validate quorum loss and uncertain mutation recovery.
- [x] Validate complete backend outage and recovery.
- [x] Validate concurrent reads and mutations.
- [x] Complete distributed stress and race validation.

### Validation Summary

- 127 CLI unit test functions.
- 30 distributed integration test functions.
- 150 final normal distributed executions passed.
- 60 final race distributed executions passed.
- 210 final distributed executions passed.
- 98.1% CLI unit statement coverage.
- All 17 repository packages with tests passed.
- Build, static analysis, and module verification passed.
- Existing backend and protobuf contracts preserved.
- Existing Go module dependencies preserved.

See [GRPC_CLI.md](GRPC_CLI.md) for configuration, command
contracts, validation results, and remaining limitations.

The CLI is validated against a three-replica, single-shard
deployment. Physical multi-shard deployment remains future work.


## Step 8 - Sharding and the Metadata Group

- [x] Implement deterministic CRC32 slot calculation.
- [x] Define the four-shard layout and metadata group.
- [x] Implement the versioned shard map with validation.
- [x] Validate contiguous ranges with no gaps or overlaps.
- [x] Validate unique shards and unique replicas.
- [x] Implement the metadata SET_SHARD_MAP state machine.
- [x] Enforce expected-version and current-plus-one updates.
- [x] Leave state unchanged on rejected commands.
- [x] Implement metadata snapshot and restore.
- [x] Implement the key-to-group router.
- [x] Implement the typed wrong-shard ownership check.
- [x] Implement the ownership-enforcing group wrapper.
- [x] Implement the local dispatcher with not-local errors.
- [x] Complete meta and shard unit tests.
- [x] Pass race detection and static analysis.
- [x] Complete sharding documentation.

### Validation Summary

- A key always maps to the same slot.
- Every slot has exactly one owner, with correct boundaries.
- Keys spread across all four shards.
- Requests route to the group that owns the key.
- Ownership follows shard map updates.
- The state machine is deterministic and rejects stale,
  non-consecutive, invalid and unknown commands.
- `go test -race -count=1 ./internal/meta ./internal/shard` passed.
- `go vet ./...` passed.

See [SHARDING.md](SHARDING.md) for architecture, the ownership
model, request flow and limitations.

The production daemon still runs a single Raft group owning all
65,536 slots. Multiple physical Raft groups and shard rebalancing
remain future work.

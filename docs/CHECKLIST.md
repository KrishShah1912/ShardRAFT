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

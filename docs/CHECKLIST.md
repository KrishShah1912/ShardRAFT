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

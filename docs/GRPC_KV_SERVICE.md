# gRPC KV Service

## 1. Objective

Expose the existing persistent KV store through the
generated ShardRAFT gRPC KVService interface.

Reuse the existing Raft consensus, ReadIndex, mutation
deduplication, WAL, snapshot, and storage implementations.

## 2. Scope

Implement four unary RPC operations:

- Get
- Put
- Delete
- CompareAndSwap

The existing HTTP endpoints remain compatible.

Request forwarding, automatic client retries, gateway
integration, and distributed shard discovery are handled
in subsequent implementation steps.

## 3. Architecture

The gRPC KV Service translates protobuf requests into
existing shard operations.

Read operations:

- LINEARIZABLE: Shard.GetLinearizable
- LOCAL: Shard.Get

Mutation operations:

- Validate kvstore.Command
- Check existing applied request result
- Submit through Shard.Propose
- Wait for matching applied request result
- Return the original deterministic result

The service must not directly modify the KV store.

## 4. Service Interface

Implement the generated KVServiceServer interface.

Embed UnimplementedKVServiceServer by value.

Use a narrow internal interface exposing:

- StatusSnapshot
- Get
- GetLinearizable
- Propose
- CheckedRequestResult

The production shard must satisfy this interface.

## 5. Request Validation

Reject nil requests and missing request headers.

Require a nonempty key.

Require an explicit supported shard ID.

The initial deployment serves shard ID 1.

Epoch 0 represents an unspecified routing epoch.
Epoch 1 represents the initial static configuration.

Reject unsupported epochs using WRONG_SHARD.

Mutations require both client_id and request_id.

Reads do not require mutation identifiers.
If supplied, identifiers must be paired.

Reject unknown ReadMode enum values.

Empty values are valid KV values.

Validate mutation commands using kvstore.Command.Validate.

Apply a bounded request-size policy.

## 6. Linearizable Get

LINEARIZABLE is the default protobuf read mode.

Invoke Shard.GetLinearizable using the RPC context.

The existing ReadIndex implementation establishes:

- Current leadership
- Current-term committed entry
- Fresh quorum confirmation
- Applied-index barrier

Never substitute a local read after ReadIndex failure.

A missing key is distinguishable from an empty value.

## 7. Local Get

LOCAL explicitly requests a potentially stale read.

Local reads may execute on followers.

Invoke Shard.Get without requiring leadership.

Local reads do not establish quorum availability.

The caller must explicitly select this consistency mode.

## 8. Put

Translate the request into kvstore.OpPut.

Preserve the key, value, client ID, and request ID.

A successful proposal alone is not an acknowledgement.

Return success only after a matching applied request
result is available.

## 9. Delete

Translate the request into kvstore.OpDelete.

Preserve the existing state-machine semantics.

Deleting an existing key returns applied=true.

Deleting a missing key returns applied=false.

Both are valid completed mutation outcomes.

## 10. CompareAndSwap

Translate the request into kvstore.OpCompareAndSwap.

Preserve the expected value and replacement value.

Perform the comparison through the replicated state
machine, not through a separate read followed by Put.

A matching existing value returns applied=true.

A missing key or mismatched value returns applied=false.

A failed comparison is not a routing failure.

## 11. Deduplication

Use client_id and request_id as the request identity.

An identical completed retry returns its original result.

A conflicting request using the same identity returns
REQUEST_CONFLICT.

An identical pending request waits for the existing
request outcome.

Do not introduce a second deduplication store.

Preserve existing WAL and snapshot recovery semantics.

## 12. Mutation Completion

Validate leadership before submitting a mutation.

Use the existing checked request-result API.

Wait for a matching applied result.

Use a bounded completion timeout of three seconds.

Use a polling interval of ten milliseconds, consistent
with the existing HTTP mutation implementation.

Honor caller cancellation and deadlines.

Do not hold the shard mutex while waiting.

Do not acknowledge an unconfirmed proposal.

## 13. Error Mapping

Use protobuf RoutingError for application-level errors.

OK:
  Successful operation.

NOT_LEADER:
  The receiving node cannot serve a leader-only operation.

WRONG_SHARD:
  Unsupported shard ID or routing epoch.

LEADER_UNAVAILABLE:
  The node or required read quorum is unavailable.
  A newly elected leader may not yet be read-ready.

OUTCOME_UNKNOWN:
  A submitted mutation cannot be confirmed.
  The mutation may still commit.

REQUEST_CONFLICT:
  Request identity reused with different command data.

INVALID_REQUEST:
  Invalid request fields or unsupported read mode.

KEY_NOT_FOUND:
  A successful read confirms the key is absent.

INTERNAL:
  Unexpected internal failure or inconsistent state.

Use native gRPC cancellation and deadline status codes
when the RPC context has expired.

A transport error after mutation submission must be
treated by the future client as a potentially unknown
mutation outcome.

Do not expose raw internal storage errors to clients.

## 14. Routing Metadata

Populate routing information from synchronized local
status when available:

- Leader ID
- Leader address
- Current term
- Shard ID
- Routing epoch

Leader information is advisory, not proof of quorum.

Use only configured gRPC addresses.

Never substitute an HTTP address for a gRPC address.

Leave unavailable leader addresses empty.

## 15. Concurrency and Safety

Use existing synchronized shard APIs.

Do not introduce independent Raft state.

Do not hold locks while waiting for quorum confirmation
or mutation completion.

Keep request lifetimes bounded.

Ensure cancellation does not leak pending read requests.

A canceled mutation may still commit.

## 16. Daemon Integration

Register KVService on the existing gRPC listener.

Preserve RaftService and AdminService registration.

Preserve existing HTTP transport compatibility.

Preserve existing graceful shutdown behavior.

## 17. Testing Requirements

Test the following categories:

- Request validation
- Error mapping
- Linearizable reads
- Explicit local reads
- Missing and empty values
- Put operations
- Delete operations
- CompareAndSwap operations
- Duplicate requests
- Conflicting requests
- Pending requests
- Cancellation and deadlines
- Follower rejection
- Leader failover
- Quorum loss and recovery
- Concurrent requests
- WAL and snapshot recovery
- Real gRPC serialization
- Daemon service registration
- Existing HTTP compatibility

## 18. Acceptance Criteria

All four RPC methods are implemented.

Default reads use ReadIndex.

Mutation acknowledgement requires an applied result.

Existing deduplication guarantees are preserved.

Backend tests, race detection, static analysis, and
builds pass.

Live three-node gRPC tests pass.

Frontend unit and Playwright tests remain compatible.

Original development storage remains unchanged.

## 19. Implementation Boundaries

This step does not implement:

- Automatic request forwarding
- Automatic client retries
- Gateway migration
- Distributed shard ownership
- Dynamic routing epochs
- Shard rebalancing
- Network snapshot installation

The initial shard and epoch configuration is static.

## 20. Production Daemon Integration

KVService is registered on the existing daemon gRPC
listener alongside RaftService and AdminService.

The production daemon supplies the persistent shard
instance directly to the KV service implementation.

Configured gRPC peer addresses are converted into a
node-ID-to-address map for leader routing hints.

HTTP peer addresses are never substituted for gRPC
leader addresses.

Existing HTTP endpoints, transport configuration,
daemon lifecycle, and graceful shutdown are preserved.

### Integration Files

- cmd/shardraftd/grpc_server.go
- cmd/shardraftd/main.go
- cmd/shardraftd/kv_grpc_peers.go
- cmd/shardraftd/kv_grpc_peers_test.go
- cmd/shardraftd/kv_grpc_integration_test.go
- cmd/shardraftd/kv_grpc_cluster_test.go
- cmd/shardraftd/kv_grpc_daemon_test.go

## 21. Integration Validation Results

Validation was completed on November 25, 2026.

| Validation | Result |
|---|---|
| gRPC peer-address mapping | 5/5 passed |
| Single-node wire integration | 9/9 passed |
| Three-node gRPC cluster integration | 6/6 passed |
| Production daemon integration | 3/3 passed |
| Targeted cluster repeated executions | 156/156 passed |
| Production daemon stress executions | 30/30 passed |
| Complete backend regression | 13/13 packages passed |
| Complete backend race detection | 13/13 packages passed |
| Shuffled backend regression | 13/13 packages passed |
| HTTP compatibility regression | Passed |
| Request retry and failover regression | Passed |
| Frontend unit tests | 25/25 passed |
| Frontend Playwright tests | 3/3 passed |
| Backend build | Passed |
| Frontend production build | Passed |
| Go vet | Passed |
| Go module verification | Passed |
| Go formatting | Passed |
| Whitespace validation | Passed |

No race conditions were reported during the executed
race-enabled test suites.

## 22. Distributed Integration Coverage

The three-node integration tests verify:

- Replicated mutations through real gRPC clients.
- Linearizable reads through the elected leader.
- Follower rejection of leader-only operations.
- Configured leader-address hints.
- Explicit follower LOCAL reads.
- Request deduplication across leadership changes.
- Read availability with one unavailable follower.
- Linearizable read rejection during quorum loss.
- Read recovery after quorum restoration.

Production daemon tests additionally verify:

- Shared Raft, Admin, and KV gRPC listeners.
- HTTP and gRPC service coexistence.
- Automatic leader election after graceful shutdown.
- Cross-leader mutation deduplication.
- Mutation acknowledgement safety during quorum loss.
- Unchanged commit and applied indexes during the
  tested isolated mutation.
- Graceful shutdown and listener cleanup.

## 23. Frontend Compatibility

All 25 frontend unit tests and all three existing
Playwright browser tests passed.

The browser tests covered:

- Dashboard and cluster availability.
- Single-node failure tolerance.
- Automatic leader failover.
- Continued writes after leader replacement.
- Recovery of the former leader.

Observed browser failover results:

| Metric | Observed Result |
|---|---|
| Original leader | Node 1 |
| Original term | 9 |
| Replacement leader | Node 3 |
| Replacement term | 10 |
| Observed failover | 3037 ms |
| Initial write commit index | 18 |
| Post-failover commit index | 20 |
| Former leader recovery | Successful |

These observations describe one browser test execution
and are not a controlled performance benchmark.

No frontend source changes were required.

## 24. Safety and Validation Limitations

Passing integration tests does not constitute a formal
proof of Raft safety or linearizability.

The production quorum-loss test confirmed that an
unconfirmed mutation was not acknowledged as successful.

OUTCOME_UNKNOWN does not guarantee that a mutation
will never commit. A subsequent retry must preserve
the original client ID, request ID, and command payload.

Production daemon failover tests use graceful shutdown.
Abrupt process termination and uncertain mutation retry
after production quorum restoration require additional
validation.

The browser tests exercise the existing HTTP-based
frontend rather than direct browser gRPC communication.

Production integration tests use temporary storage.
The browser test output alone does not establish that
original development storage remained unchanged.

Automatic gRPC client retries, gateway integration,
dynamic shard discovery, and multi-shard routing remain
outside the scope of this integration milestone.

# ReadIndex and Linearizable Reads

## 1. Implementation Status

ReadIndex is implemented across the Raft consensus layer,
Raft driver, shard state machine, and network simulator.

The shard exposes GetLinearizable for quorum-confirmed reads
without appending a new Raft log entry for every read.

The existing local Get API remains available and does not
provide the same linearizability guarantee.

## 2. Objectives

- Provide quorum-confirmed linearizable reads.
- Prevent successful reads from stale leaders.
- Require a committed entry from the current leader term.
- Wait until the state machine has applied the read barrier.
- Avoid per-read Raft log entries.
- Support concurrent requests, cancellation, and deadlines.
- Preserve existing persistence and recovery guarantees.

## 3. Architecture

The read path consists of four layers:

| Layer | Responsibility |
|---|---|
| Raft | Leadership validation and quorum confirmation |
| Raft driver | Persistence ordering and applied-index barrier |
| Shard | Request lifecycle and state-machine lookup |
| Transport | Delivery of correlated heartbeat messages |

The simulator provides deterministic testing of quorum,
message delivery, application barriers, and fault scenarios.

## 4. Read Request Lifecycle

1. The shard validates the context and key.
2. The current node must be the leader.
3. Raft verifies that an entry from its current term is committed.
4. A bounded pending read request is registered.
5. The current commit index becomes the read barrier.
6. Correlated heartbeat probes are sent to peers.
7. Distinct acknowledgements establish a majority quorum.
8. Raft publishes a ReadState confirmation.
9. The driver waits for the state machine to apply the barrier.
10. The shard revalidates leadership and request identity.
11. The requested value is returned from the state machine.
12. Pending request resources are released.

## 5. Raft ReadIndex API

The Raft layer exposes:

- ReadIndex(context)
- CancelReadIndex(term, context)
- Ready()
- AdvanceReadStates(states)

ReadIndex requires a valid leader, a nonempty bounded context,
and a committed entry from the current term.

ReadState contains:

- RequestID
- Context
- Term
- Index

Read confirmations are copied when exposed through Ready.

Read confirmations are acknowledged explicitly through
AdvanceReadStates. The ordinary Advance operation does not
implicitly discard unacknowledged read confirmations.

## 6. Quorum Confirmation

ReadIndex uses correlated heartbeat messages with nonzero
request identifiers.

The leader counts its own vote and acknowledgements from
distinct configured peers.

Acknowledgements must match the active request and leader term.

Duplicate, rejected, malformed, unknown, and obsolete responses
cannot establish a valid read quorum.

Ordinary replication heartbeats remain independent of the
correlated read confirmation mechanism.

A single-node cluster can establish a quorum using its own vote.

## 7. Current-Term Commit Requirement

A newly elected leader cannot serve ReadIndex requests until
it has committed an entry from its current term.

This prevents a leader from relying exclusively on commit
information inherited from earlier terms.

Requests submitted before this requirement is satisfied
return ErrReadNotReady.

## 8. Applied-Index Barrier

Quorum confirmation alone is insufficient.

The state machine must apply all committed entries through
the confirmed read index before the value can be returned.

The driver defers read confirmations until the applied-index
provider reports sufficient progress.

The shard independently verifies the applied-index barrier
before accessing the key-value store.

## 9. Driver Processing Order

The driver preserves the following processing order:

1. Persist log changes.
2. Persist hard-state changes.
3. Synchronize durable storage.
4. Apply committed entries.
5. Advance ordinary Raft state.
6. Send outbound messages.
7. Deliver and acknowledge eligible read confirmations.

Processing failures prevent successful completion of
unconfirmed or unsafe reads.

## 10. Public Shard API

The linearizable read API is:

    GetLinearizable(ctx context.Context, key string) (string, bool, error)

Example:

    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    value, found, err := s.GetLinearizable(ctx, "alpha")

    if err != nil {
        return err
    }

    if found {
        fmt.Println(value)
    }

The example assumes an initialized shard and the required
standard-library imports.

A successful lookup of a missing key returns found=false
without treating the missing key as a consensus failure.

## 11. Cancellation and Deadlines

GetLinearizable applies a maximum internal timeout of
three seconds. Earlier caller deadlines are respected.

Canceled and expired requests release pending read resources.

Cancellation removes matching pending or completed Raft
read confirmations and queued correlated probes.

Late acknowledgements cannot revive canceled requests.

The shard validates ownership of a pending request before
allowing cancellation to modify its lifecycle.

## 12. Leadership Changes and Failures

Pending reads are invalidated when leadership or the
associated leader term becomes obsolete.

A former leader cannot successfully complete a read using
an obsolete confirmation.

Without a reachable majority, a read cannot establish a
fresh quorum and eventually terminates through its deadline.

Closing a shard or stopping its driver invalidates outstanding
requests.

The linearizable read path does not silently fall back to
a local read when confirmation fails.

## 13. Resource Limits

| Resource | Limit |
|---|---:|
| Raft read context size | 4096 bytes |
| Raft pending and completed read capacity | 1024 |
| Shard pending read capacity | 1024 |
| Internal shard read context size | 16 bytes |
| Maximum internal read timeout | 3 seconds |

Read contexts are copied to prevent caller-side mutation.

Request identifiers are allocated monotonically within
the lifetime of their owning instance.

Obsolete confirmations are pruned from simulator queues.

## 14. Persistence and Recovery

ReadIndex requests and confirmations are volatile.

They are not persisted as additional WAL records, snapshot
entries, or mutation deduplication records.

Recovery restores durable Raft and key-value state without
replaying outstanding read requests.

After restart, the node must establish fresh leadership and
satisfy the current-term commit requirement before serving
linearizable reads.

Tests cover WAL restart, compacted restart, repeated restart,
and rejection of obsolete read confirmations.

## 15. Testing Strategy

The implementation is validated through:

- Raft unit tests for request registration and quorum safety.
- Heartbeat validation and acknowledgement tests.
- Current-term commit and snapshot-boundary tests.
- ReadState ownership and acknowledgement tests.
- Driver persistence and application-barrier tests.
- Simulator message delivery and fault-injection tests.
- Shard API, concurrency, cancellation, and recovery tests.
- Live TCP gRPC integration tests.

The live ReadIndex suite contains 27 tests covering election,
replication, existing and missing keys, overwrites, deletion,
concurrent reads, failover, stale leaders, quorum loss,
cancellation, and quorum restoration.

Live tests use real TCP gRPC connections and temporary
storage within the test process.

## 16. Regression Verification

Run the complete backend regression suite:

    go test -count=1 ./...

Run race detection:

    go test -race -count=1 ./...

Run focused live ReadIndex integration tests:

    go test -count=1 -run '^TestLiveReadIndex' ./cmd/shardraftd

Run static and build verification:

    go vet ./...
    go build ./...
    go mod verify

The implementation has also been exercised using repeated
and shuffled test runs.

## 17. Frontend Compatibility

The ReadIndex implementation has been integrated with
the validation and performance dashboards from main.

Integration verification included:

- Full backend regression and race detection.
- 22 frontend routing unit tests.
- Frontend TypeScript and Vite production builds.
- Three Playwright end-to-end tests.
- Leader failover and replica recovery.
- Supervisor benchmark endpoint availability.

Frontend acceptance was performed using isolated temporary
storage. The original development database was preserved.

The existing validation report generator has a known
frontend TAP test-counting discrepancy. The frontend tests
pass independently of this reporting issue.

## 18. Current Scope and Limitations

ReadIndex provides the internal foundation for the upcoming
gRPC KV Service.

The existing HTTP GET endpoint is not upgraded to
quorum-confirmed linearizable reads by this milestone.

The gRPC KV Service, client-facing read-mode selection,
request routing, and gateway integration are subsequent
milestones.

The implementation does not rely on leader leases.

ReadIndex safety requires a fresh quorum confirmation and
an applied state-machine barrier for each successful read.

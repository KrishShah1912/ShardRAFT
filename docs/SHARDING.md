# Sharding and the Metadata Group

## 1. Overview

ShardRAFT partitions the key space into 65,536 logical hash slots and
assigns every slot to exactly one replication group. Step 8 introduces
the in-process building blocks for that partitioning:

- A deterministic key-to-slot function.
- A versioned, self-validating shard map.
- A metadata state machine that installs new shard maps safely.
- A router, ownership check, group wrapper, and dispatcher that route
  requests to the group owning each key.

These components live in `internal/meta` and `internal/shard`. They do
not replace or modify the existing single-group production path; they
provide the ownership model on which physical sharding is built.

## 2. Slot Model

| Property | Value |
|---|---|
| Total logical hash slots | 65536 |
| Slot function | CRC32 IEEE modulo 65536 |
| Slot range semantics | Inclusive |
| Slot of a key | Deterministic and stable |

`meta.SlotForKey(key)` hashes the raw key bytes with CRC32 IEEE and
reduces the result modulo `meta.TotalSlots`. The same key always maps
to the same slot, and every slot is within `[0, TotalSlots-1]`.

## 3. Shard Layout

The initial layout splits the slot space into four equal ranges. Each
data shard is replicated across three nodes. A separate metadata group
replicates the shard map itself.

| Shard | Slot range | Replicas |
|---|---|---|
| S1 | 0–16383 | n1, n2, n3 |
| S2 | 16384–32767 | n2, n3, n4 |
| S3 | 32768–49151 | n3, n4, n5 |
| S4 | 49152–65535 | n4, n5, n1 |
| Meta group | — | n1, n2, n3 |

`meta.DefaultShardMap()` returns this layout at version 1. The metadata
group does not own data slots; it holds the authoritative shard map.

## 4. Shard Map

A `meta.ShardMap` is a versioned assignment of every slot to a single
owning shard.

```go
type ShardMap struct {
    Version uint64
    Shards  []Shard
}

type Shard struct {
    ID       ShardID
    Range    SlotRange
    Replicas []raft.NodeID
}
```

`ShardMap.Validate` enforces the invariants required for unambiguous
routing:

- The version is nonzero.
- Shard IDs are nonzero and unique.
- Each shard has at least one replica, with nonzero unique node IDs.
- Slot ranges are well formed and lie within the slot space.
- The ranges are contiguous with no gaps or overlaps, so every slot is
  owned exactly once.

`OwnerForSlot` and `OwnerForKey` resolve a slot or key to its owning
shard. Both `ShardMap` and the shards it returns are deep-copied so
that callers cannot mutate shared routing state.

## 5. Metadata State Machine

`meta.StateMachine` applies `SET_SHARD_MAP` commands and holds the
current shard map. It starts empty at version 0 and is safe for
concurrent use.

```go
type Command struct {
    Type            CommandType
    ExpectedVersion uint64
    ShardMap        ShardMap
}
```

A command is accepted only when all of the following hold:

- The command type is `SET_SHARD_MAP`.
- The new shard map is valid.
- The expected version equals the current version.
- The new map's version equals the current version plus one.

When a command is rejected the state is left unchanged. The state
machine is deterministic: applying the same command sequence to two
state machines produces identical state.

`Snapshot` serializes the current shard map, and `Restore` replaces the
state from a snapshot. An empty snapshot restores the initial
version-0 state; a non-empty snapshot must contain a valid shard map.

## 6. Routing Components

The `internal/shard` package adds four routing components. They are
additive and do not change the existing `Shard` type.

### Router

`Router` resolves keys against the current shard map and allows the map
to be replaced at runtime.

- `Route(key)` returns the key's slot, owning group ID, and replicas.
- `CheckOwnership(groupID, key)` reports whether a group owns a key. It
  returns a typed `*WrongShardError` naming the real owner and its
  replicas otherwise.
- `SetShardMap(map)` installs a new validated map, after which
  ownership decisions follow the updated map immediately.

### Group

`Group` wraps an existing `Shard` as a replication group. Every
`Propose` and `Get` first checks ownership against the router's current
map, so a group rejects keys it does not own with a `*WrongShardError`,
even after the map changes.

### Dispatcher

`Dispatcher` holds the locally hosted groups and routes each request to
the local group that owns the key. A key owned by a group that is not
hosted locally is rejected with a typed `*NotLocalError` carrying the
owning replicas, so the caller knows where to retry.

## 7. Request Flow

```text
key
  -> SlotForKey(key)
  -> ShardMap.OwnerForSlot(slot)
  -> owning group
       local  -> Group.Propose / Group.Get (ownership re-checked)
       remote -> NotLocalError{owner, replicas}
```

## 8. Testing

The Step 8 tests cover:

- A key always maps to the same slot, within range.
- Every slot has exactly one owner, with correct boundaries.
- Keys spread across all four shards.
- Requests route to the group that owns the key.
- Ownership follows shard map updates.
- The state machine is deterministic and rejects stale, non-consecutive,
  invalid, and unknown commands.
- Shard map validation rejects gaps, overlaps, invalid ranges, and
  invalid replicas.

Run the Step 8 tests with:

```bash
go test -race -count=1 ./internal/meta ./internal/shard
```

## 9. Limitations

- The router, group, and dispatcher operate in process; cross-node
  forwarding of non-local requests is not implemented here.
- Shard rebalancing and live slot migration remain future work.
- The production daemon still runs a single Raft group owning all
  65,536 slots; wiring multiple physical groups through the dispatcher
  is a later step.

## Step 8 Implementation Status

The physical multi-Raft implementation is now integrated
into the storage daemon.

The initial four-shard topology is operational across
five storage nodes, with an independent metadata Raft
group hosted by nodes 1, 2 and 3.

The production client discovers the committed shard map
and routes KV operations to the appropriate data group.

The initial topology is fixed. Live migration and
rebalancing remain outside the implemented scope.

Earlier implementation-status descriptions in this
document describe the preceding in-process milestone.

See [Multi-Raft Acceptance](MULTI_RAFT_ACCEPTANCE.md)
for current validation results and limitations.

import test from "node:test";
import assert from "node:assert/strict";

import {
  captureClusterSnapshot,
  compareClusterSnapshots,
  quorumTransition,
  reachabilityTransition,
  observeReplicaCatchUp,
} from "../.test-build/multi/faultSnapshot.js";

import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
} from "../.test-build/multi/clusterState.js";

const BASE = 1700000000000;

function fixture(options = {}) {
  const {
    time = BASE,
    stopped = [],
    leader = 1,
    term = 4,
    commit = 100,
    applied = commit,
    metadataCounters = true,
  } = options;

  const observations = [1, 2, 3, 4, 5].map(id => ({
    id,
    reachable: !stopped.includes(id),
    observedAt: time,
  }));

  const shards = DATA_SHARDS.map(shardId => {
    const members = SHARD_PLACEMENT[shardId];

    const active = members.filter(
      id => !stopped.includes(id),
    );

    const leaderId = active.includes(leader)
      ? leader
      : active[0] ?? null;

    const replicas = members.map(nodeId => {
      const reachable = active.includes(nodeId);

      return {
        nodeId,
        reachable,
        role: !reachable
          ? "unknown"
          : nodeId === leaderId
            ? "leader"
            : "follower",
        term: reachable ? term : null,
        leaderId: reachable ? leaderId : null,
        commitIndex: reachable ? commit : null,
        appliedIndex: reachable ? applied : null,
      };
    });

    return {
      shardId,
      leaderId,
      replicas,
      reachableReplicas: active.length,
      majorityReachable: active.length >= 2,
      replicationLag: active.length >= 2 ? 0 : null,
      state: active.length < 2
        ? "unavailable"
        : active.length < 3
          ? "degraded"
          : "healthy",
    };
  });

  const metadataNodes = [1, 2, 3].filter(
    id => !stopped.includes(id),
  );

  const metadataLeader = metadataNodes.includes(leader)
    ? leader
    : metadataNodes[0] ?? null;

  const metadata = metadataNodes.map(node_id => ({
    node_id,
    role: node_id === metadataLeader
      ? "leader"
      : "follower",
    ...(metadataCounters
      ? {
          term,
          leader_id: metadataLeader,
          commit_index: commit,
          applied_index: applied,
        }
      : {}),
  }));

  return {
    loading: false,
    lastUpdated: time,
    observations,
    shards,
    metadata,
    metadataLeader,
  };
}

function snapshot(options = {}) {
  return captureClusterSnapshot(
    fixture(options),
    options.time ?? BASE,
  );
}

function group(state, id = 1) {
  return state.groups.find(
    item => item.groupId === id,
  );
}

function replica(state, id = 2) {
  return state.replicas.find(
    item => item.nodeId === id,
  );
}

test("Healthy cluster captures all five groups", () => {
  const state = snapshot();

  assert.equal(state.available, true);
  assert.equal(state.groups.length, 5);
  assert.equal(state.nodes.length, 5);
  assert.equal(state.reachableNodes, 5);
  assert.equal(state.reachableMajorities, 5);
});

test("Configured replica placement is preserved", () => {
  const state = snapshot();

  for (const id of DATA_SHARDS) {
    assert.deepEqual(
      group(state, id).replicas.map(r => r.nodeId),
      SHARD_PLACEMENT[id],
    );
  }

  assert.deepEqual(
    group(state, 5).replicas.map(r => r.nodeId),
    [1, 2, 3],
  );
});

test("Leader counters are captured", () => {
  const shard = group(snapshot());

  assert.equal(shard.leaderId, 1);
  assert.equal(shard.term, 4);
  assert.equal(shard.commitIndex, 100);
  assert.equal(shard.appliedIndex, 100);
});

test("Metadata group is captured independently", () => {
  const metadata = group(snapshot(), 5);

  assert.equal(metadata.label, "Metadata");
  assert.equal(metadata.leaderId, 1);
  assert.equal(metadata.reachable, 3);
  assert.equal(metadata.majority, true);
});

test("Missing metadata counters remain unknown", () => {
  const metadata = group(snapshot({
    metadataCounters: false,
  }), 5);

  assert.equal(metadata.term, null);
  assert.equal(metadata.commitIndex, null);
  assert.equal(metadata.appliedIndex, null);
  assert.equal(metadata.commitSpread, null);
});

test("Snapshot objects are deeply frozen", () => {
  const state = snapshot();

  assert.ok(Object.isFrozen(state));
  assert.ok(Object.isFrozen(state.groups));
  assert.ok(Object.isFrozen(state.nodes));

  for (const node of state.nodes) {
    assert.ok(Object.isFrozen(node));
  }

  for (const shard of state.groups) {
    assert.ok(Object.isFrozen(shard));
    assert.ok(Object.isFrozen(shard.replicas));

    for (const item of shard.replicas) {
      assert.ok(Object.isFrozen(item));
    }
  }
});

test("Snapshots are detached from mutable input", () => {
  const cluster = fixture();
  const state = captureClusterSnapshot(cluster, BASE);

  cluster.shards[0].replicas[0].commitIndex = 999;
  cluster.observations[0].reachable = false;
  cluster.metadata[0].term = 99;

  assert.equal(group(state).commitIndex, 100);
  assert.equal(state.nodes[0].reachable, true);
  assert.equal(group(state, 5).term, 4);
});

test("Capture does not mutate source observations", () => {
  const cluster = fixture();
  const original = JSON.stringify(cluster);

  captureClusterSnapshot(cluster, BASE);

  assert.equal(JSON.stringify(cluster), original);
});

test("Loading observations are unavailable", () => {
  const cluster = fixture();
  cluster.loading = true;

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(state.available, false);
  assert.equal(state.reachableNodes, null);
  assert.equal(group(state).majority, null);
});

test("Stale observations are unavailable", () => {
  const state = captureClusterSnapshot(
    fixture(),
    BASE + 10001,
  );

  assert.equal(state.available, false);
  assert.equal(group(state).term, null);
});

test("Freshness boundary is inclusive", () => {
  const state = captureClusterSnapshot(
    fixture(),
    BASE + 10000,
  );

  assert.equal(state.available, true);
});

test("Future observation timestamps are rejected", () => {
  const state = captureClusterSnapshot(
    fixture(),
    BASE - 1,
  );

  assert.equal(state.available, false);
});

test("Missing observation timestamps are rejected", () => {
  const cluster = fixture();
  cluster.lastUpdated = null;

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(state.available, false);
  assert.equal(state.observationAgeMs, null);
});

test("Missing physical node observations are rejected", () => {
  const cluster = fixture();
  cluster.observations.pop();

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(state.available, false);
});

test("Duplicate physical node observations are rejected", () => {
  const cluster = fixture();

  cluster.observations.push(
    cluster.observations[0],
  );

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(state.available, false);
});

test("Missing group observations remain unknown", () => {
  const cluster = fixture();

  cluster.shards = cluster.shards.filter(
    shard => shard.shardId !== 1,
  );

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(group(state).reachable, null);
  assert.equal(group(state).majority, null);
  assert.equal(state.reachableMajorities, null);
});

test("Unreachable replica counters remain unknown", () => {
  const state = snapshot({ stopped: [1] });
  const item = replica(group(state), 1);

  assert.equal(item.reachable, false);
  assert.equal(item.term, null);
  assert.equal(item.commitIndex, null);
  assert.equal(item.appliedIndex, null);
});

test("Single node loss retains all reachable majorities", () => {
  const state = snapshot({ stopped: [1] });

  assert.equal(state.reachableNodes, 4);
  assert.equal(state.reachableMajorities, 5);
  assert.equal(group(state).reachable, 2);
  assert.equal(group(state).majority, true);
});

test("Two unavailable members reduce group majority", () => {
  const state = snapshot({ stopped: [1, 2] });

  assert.equal(group(state).reachable, 1);
  assert.equal(group(state).majority, false);
  assert.equal(group(state, 5).majority, false);
});

test("Multiple observed leaders are treated as ambiguous", () => {
  const cluster = fixture();

  cluster.shards[0].replicas[1].role = "leader";

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(group(state).leaderId, null);
  assert.equal(group(state).term, null);
  assert.equal(group(state).commitIndex, null);
});

test("Invalid counters remain unknown", () => {
  const cluster = fixture();

  cluster.shards[0].replicas[0].term = -1;
  cluster.shards[0].replicas[0].commitIndex = NaN;
  cluster.shards[0].replicas[0].appliedIndex = 1.5;

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(group(state).term, null);
  assert.equal(group(state).commitIndex, null);
  assert.equal(group(state).appliedIndex, null);
});

test("Commit spread uses observed reachable replicas", () => {
  const cluster = fixture();

  cluster.shards[0].replicas[1].commitIndex = 95;

  const state = captureClusterSnapshot(cluster, BASE);

  assert.equal(group(state).commitSpread, 5);
});

test("All quorum transitions are classified", () => {
  assert.equal(quorumTransition(true, true), "retained");
  assert.equal(quorumTransition(true, false), "lost");
  assert.equal(quorumTransition(false, true), "restored");

  assert.equal(
    quorumTransition(false, false),
    "unavailable",
  );

  assert.equal(quorumTransition(null, true), "unknown");
  assert.equal(quorumTransition(true, null), "unknown");
});

test("All reachability transitions are classified", () => {
  assert.equal(
    reachabilityTransition(true, true),
    "unchanged",
  );

  assert.equal(
    reachabilityTransition(false, false),
    "unchanged",
  );

  assert.equal(
    reachabilityTransition(true, false),
    "lost",
  );

  assert.equal(
    reachabilityTransition(false, true),
    "restored",
  );

  assert.equal(
    reachabilityTransition(null, true),
    "unknown",
  );
});

test("Equal leader and follower indexes are recognized", () => {
  const shard = group(snapshot());

  assert.equal(
    observeReplicaCatchUp(replica(shard), shard),
    "at-observed-leader-index",
  );
});

test("Behind follower is distinguished", () => {
  const shard = group(snapshot());

  assert.equal(
    observeReplicaCatchUp({
      ...replica(shard),
      commitIndex: 90,
      appliedIndex: 90,
    }, shard),
    "behind-observed-leader",
  );
});

test("Ahead observation is not called caught up", () => {
  const shard = group(snapshot());

  assert.equal(
    observeReplicaCatchUp({
      ...replica(shard),
      commitIndex: 101,
    }, shard),
    "ahead-of-observed-leader",
  );
});

test("Different Raft terms prevent catch-up inference", () => {
  const shard = group(snapshot());

  assert.equal(
    observeReplicaCatchUp({
      ...replica(shard),
      term: 3,
    }, shard),
    "unknown",
  );
});

test("Missing indexes prevent catch-up inference", () => {
  const shard = group(snapshot());

  assert.equal(
    observeReplicaCatchUp({
      ...replica(shard),
      appliedIndex: null,
    }, shard),
    "unknown",
  );
});

test("Unreachable replicas are classified as unavailable", () => {
  const shard = group(snapshot({ stopped: [2] }));

  assert.equal(
    observeReplicaCatchUp(replica(shard), shard),
    "unavailable",
  );
});

test("Leader transition and index deltas are calculated", () => {
  const before = snapshot();

  const after = snapshot({
    time: BASE + 2000,
    stopped: [1],
    leader: 2,
    term: 5,
    commit: 110,
    applied: 108,
  });

  const result = compareClusterSnapshots(before, after);
  const shard = group(result);

  assert.equal(result.comparable, true);
  assert.equal(result.observationIntervalMs, 2000);
  assert.equal(result.reachableNodeDelta, -1);

  assert.equal(shard.leaderChanged, true);
  assert.equal(shard.termDelta, 1);
  assert.equal(shard.commitDelta, 10);
  assert.equal(shard.appliedDelta, 8);
  assert.equal(shard.reachableDelta, -1);
  assert.equal(shard.quorum, "retained");
});

test("Replica loss is recorded without fabricated deltas", () => {
  const result = compareClusterSnapshots(
    snapshot(),
    snapshot({
      time: BASE + 1000,
      stopped: [1],
    }),
  );

  const item = replica(group(result), 1);

  assert.equal(item.reachability, "lost");
  assert.equal(item.commitDelta, null);
  assert.equal(item.appliedDelta, null);
  assert.equal(item.catchUp, "unavailable");
});

test("Replica recovery is recorded", () => {
  const result = compareClusterSnapshots(
    snapshot({ stopped: [1] }),
    snapshot({
      time: BASE + 1000,
      leader: 2,
      term: 5,
    }),
  );

  const item = replica(group(result), 1);

  assert.equal(result.reachableNodeDelta, 1);
  assert.equal(item.reachability, "restored");

  assert.equal(
    item.catchUp,
    "at-observed-leader-index",
  );
});

test("Quorum loss and restoration are calculated", () => {
  const healthy = snapshot();

  const unavailable = snapshot({
    time: BASE + 1000,
    stopped: [1, 2],
  });

  const recovered = snapshot({
    time: BASE + 2000,
  });

  const loss = compareClusterSnapshots(
    healthy,
    unavailable,
  );

  const restoration = compareClusterSnapshots(
    unavailable,
    recovered,
  );

  assert.equal(group(loss).quorum, "lost");
  assert.equal(group(restoration).quorum, "restored");
});

test("Same observation timestamp is not comparable", () => {
  const result = compareClusterSnapshots(
    snapshot(),
    snapshot({ commit: 110 }),
  );

  assert.equal(result.comparable, false);
  assert.equal(result.observationIntervalMs, null);
  assert.equal(group(result).commitDelta, null);
  assert.equal(group(result).quorum, "unknown");
});

test("Older after observation is not comparable", () => {
  const result = compareClusterSnapshots(
    snapshot({ time: BASE + 1000 }),
    snapshot(),
  );

  assert.equal(result.comparable, false);
  assert.equal(result.reachableNodeDelta, null);
});

test("Unavailable after snapshot is not comparable", () => {
  const unavailable = captureClusterSnapshot(
    fixture(),
    BASE + 20000,
  );

  const result = compareClusterSnapshots(
    snapshot(),
    unavailable,
  );

  assert.equal(result.comparable, false);
  assert.equal(group(result).leaderChanged, null);
  assert.equal(group(result).commitDelta, null);
});

test("Unknown leadership does not fabricate a transition", () => {
  const cluster = fixture({ time: BASE + 1000 });

  cluster.shards[0].replicas.forEach(item => {
    item.role = "candidate";
  });

  const result = compareClusterSnapshots(
    snapshot(),
    captureClusterSnapshot(cluster, BASE + 1000),
  );

  assert.equal(group(result).leaderChanged, null);
  assert.equal(group(result).termDelta, null);
});

test("Negative index deltas are preserved", () => {
  const result = compareClusterSnapshots(
    snapshot({ commit: 100 }),
    snapshot({
      time: BASE + 1000,
      commit: 95,
    }),
  );

  assert.equal(group(result).commitDelta, -5);
});

test("Comparison results are immutable", () => {
  const result = compareClusterSnapshots(
    snapshot(),
    snapshot({ time: BASE + 1000 }),
  );

  assert.ok(Object.isFrozen(result));
  assert.ok(Object.isFrozen(result.groups));

  for (const item of result.groups) {
    assert.ok(Object.isFrozen(item));
    assert.ok(Object.isFrozen(item.replicas));

    for (const entry of item.replicas) {
      assert.ok(Object.isFrozen(entry));
    }
  }
});

test("Comparison does not modify either snapshot", () => {
  const before = snapshot();

  const after = snapshot({
    time: BASE + 1000,
    stopped: [1],
  });

  const originalBefore = JSON.stringify(before);
  const originalAfter = JSON.stringify(after);

  compareClusterSnapshots(before, after);

  assert.equal(JSON.stringify(before), originalBefore);
  assert.equal(JSON.stringify(after), originalAfter);
});

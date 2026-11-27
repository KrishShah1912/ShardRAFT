import test from "node:test";
import assert from "node:assert/strict";

import {
  isReplicationEvent,
  replicationObservation,
  correlateReplication,
  mergeReplicationEvents,
  commitObservation,
} from "../.test-build/multi/replicationTelemetry.js";

let sequence = 0;

function event(type, node, data, overrides = {}) {
  sequence += 1;

  return {
    id: sequence,
    timestamp: new Date(
      1700000000000 + sequence * 10,
    ).toISOString(),
    node_id: node,
    shard_id: 1,
    category: "raft",
    type,
    term: 9,
    data,
    ...overrides,
  };
}

function sent(overrides = {}) {
  return event("raft.append.sent", 2, {
    to: 1,
    request_id: 42,
    previous_index: 100,
    entries: 3,
    ...overrides,
  });
}

function received() {
  return event("raft.append.received", 1, {
    from: 2,
    request_id: 42,
    previous_index: 100,
    entries: 3,
  });
}

function accepted() {
  return event("raft.append.accepted", 1, {
    from: 2,
    request_id: 42,
    index: 103,
  });
}

function acknowledged() {
  return event("raft.replication.progress", 2, {
    peer_id: 1,
    request_id: 42,
    match_index: 103,
    next_index: 104,
  });
}

test("extracts actual replicated log range", () => {
  const item = replicationObservation(sent());

  assert.equal(item.stage, "sent");
  assert.equal(item.entries, 3);
  assert.equal(item.firstIndex, 101);
  assert.equal(item.lastIndex, 103);
});

test("rejects zero-entry append traffic", () => {
  assert.equal(
    replicationObservation(sent({ entries: 0 })),
    null,
  );
});

test("excludes heartbeat traffic", () => {
  assert.equal(
    isReplicationEvent(
      event("raft.heartbeat.sent", 2, {}),
    ),
    false,
  );
});

test("rejects invalid shard membership", () => {
  assert.equal(
    replicationObservation(sent({ to: 5 })),
    null,
  );
});

test("correlates all four replication stages", () => {
  const items = correlateReplication([
    sent(),
    received(),
    accepted(),
    acknowledged(),
  ]);

  assert.equal(items.length, 1);
  assert.equal(items[0].stage, "acknowledged");
  assert.equal(items[0].entries, 3);
  assert.equal(items[0].firstIndex, 101);
  assert.equal(items[0].lastIndex, 103);
});

test("handles out-of-order event arrival", () => {
  const events = [
    sent(),
    received(),
    accepted(),
    acknowledged(),
  ];

  const items = correlateReplication(
    events.reverse(),
  );

  assert.equal(items.length, 1);
  assert.equal(items[0].stage, "acknowledged");
  assert.equal(items[0].entries, 3);
});

test("acceptance is not leader acknowledgment", () => {
  const items = correlateReplication([
    sent(),
    accepted(),
  ]);

  assert.equal(items[0].stage, "accepted");
});

test("does not infer data replication from progress alone", () => {
  assert.equal(
    correlateReplication([acknowledged()]).length,
    0,
  );
});

test("correlates follower-side rejection", () => {
  const items = correlateReplication([
    sent(),
    event("raft.append.rejected", 1, {
      from: 2,
      request_id: 42,
      index: 100,
      reason: "missing_previous_entry",
    }),
  ]);

  assert.equal(items[0].stage, "rejected");
  assert.equal(items[0].leaderId, 2);
  assert.equal(items[0].followerId, 1);
});

test("correlates leader-side rejection", () => {
  const items = correlateReplication([
    sent(),
    event("raft.append.rejected", 2, {
      from: 1,
      request_id: 42,
      index: 100,
      reason: "follower_rejected",
      next_index: 99,
    }),
  ]);

  assert.equal(items[0].stage, "rejected");
  assert.equal(items[0].leaderId, 2);
  assert.equal(items[0].followerId, 1);
});

test("separates identical request IDs across terms", () => {
  const first = sent();
  const second = { ...sent(), term: 10 };

  assert.equal(
    correlateReplication([first, second]).length,
    2,
  );
});

test("separates requests across followers", () => {
  assert.equal(
    correlateReplication([
      sent(),
      sent({ to: 3 }),
    ]).length,
    2,
  );
});

test("extracts majority commit advancement", () => {
  const commit = commitObservation(
    event("raft.commit.advanced", 2, {
      previous_commit: 100,
      commit_index: 103,
      replicas: 2,
      majority: 2,
      source: "leader_quorum",
    }),
  );

  assert.equal(commit.advancedBy, 3);
  assert.equal(commit.replicas, 2);
  assert.equal(commit.majority, 2);
});

test("distinguishes follower commit advancement", () => {
  const commit = commitObservation(
    event("raft.commit.advanced", 1, {
      previous_commit: 100,
      commit_index: 103,
      source: "append_entries",
    }),
  );

  assert.equal(commit.source, "append_entries");
  assert.equal(commit.replicas, null);
});

test("rejects non-advancing commit events", () => {
  assert.equal(
    commitObservation(
      event("raft.commit.advanced", 2, {
        previous_commit: 103,
        commit_index: 100,
        source: "leader_quorum",
      }),
    ),
    null,
  );
});

test("retains replication events independently", () => {
  const append = sent();

  const result = mergeReplicationEvents([], [
    append,
    event("raft.heartbeat.sent", 2, {}),
  ]);

  assert.equal(result.length, 1);
  assert.equal(result[0].id, append.id);
});

test("deduplicates replayed replication events", () => {
  const append = sent();

  assert.equal(
    mergeReplicationEvents([append], [append]).length,
    1,
  );
});

test("bounds replication history", () => {
  const events = Array.from(
    { length: 20 },
    () => sent(),
  );

  assert.equal(
    mergeReplicationEvents([], events, 5).length,
    5,
  );
});

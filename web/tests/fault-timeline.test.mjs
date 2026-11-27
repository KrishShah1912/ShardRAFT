import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFaultTimeline,
  FAULT_TIMELINE_LIMIT,
} from "../src/multi/faultTimeline.ts";

const BASE = 1700000000000;

const timestamp = offset =>
  new Date(BASE + offset).toISOString();

function event(type, data = {}, overrides = {}) {
  return {
    id: 1,
    timestamp: timestamp(100),
    category: "raft",
    type,
    node_id: 1,
    shard_id: 1,
    term: 2,
    data,
    ...overrides,
  };
}

function action(overrides = {}) {
  return {
    id: "action-1",
    node: 1,
    action: "stop",
    startedAt: timestamp(0),
    completedAt: timestamp(200),
    success: true,
    durationMs: 200,
    ...overrides,
  };
}

function input(overrides = {}) {
  return {
    electionEvents: [],
    replicationEvents: [],
    actions: [],
    leadership: [],
    reachability: [],
    ...overrides,
  };
}

test("Empty observations produce an empty timeline", () => {
  assert.deepEqual(buildFaultTimeline(input()), []);
});

test("Election initiation is normalized", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.election.started"),
    ],
  }));

  assert.equal(row.title, "Election started");
  assert.equal(row.source, "telemetry");
  assert.equal(row.kind, "election");
  assert.equal(row.term, 2);
  assert.equal(row.shardId, 1);
});

test("Vote requests identify the destination", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.vote.requested", { to: 2 }),
    ],
  }));

  assert.match(row.detail, /Node 2/);
});

test("Vote grants are recorded", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.vote.granted", {
        candidate_id: 2,
      }),
    ],
  }));

  assert.equal(row.title, "Vote granted");
  assert.equal(row.tone, "success");
});

test("Vote rejections are recorded", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.vote.rejected", {
        candidate_id: 2,
      }),
    ],
  }));

  assert.equal(row.title, "Vote rejected");
  assert.equal(row.tone, "warning");
});

test("Leader election is classified as leadership", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.leader.elected"),
    ],
  }));

  assert.equal(row.kind, "leadership");
  assert.equal(row.tone, "success");
});

test("Term changes preserve previous and current terms", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.term.changed", {
        previous_term: 1,
      }),
    ],
  }));

  assert.match(row.detail, /1 → 2/);
});

test("Role changes preserve observed roles", () => {
  const [row] = buildFaultTimeline(input({
    electionEvents: [
      event("raft.role.changed", {
        previous_role: "candidate",
        role: "leader",
      }),
    ],
  }));

  assert.match(row.detail, /candidate → leader/);
});

test("Commit advancement is normalized", () => {
  const [row] = buildFaultTimeline(input({
    replicationEvents: [
      event("raft.commit.advanced", {
        previous_commit: 10,
        commit_index: 15,
        source: "leader",
      }),
    ],
  }));

  assert.equal(row.kind, "commit");
  assert.match(row.detail, /10 → 15/);
});

test("Invalid commit advancement is excluded", () => {
  const rows = buildFaultTimeline(input({
    replicationEvents: [
      event("raft.commit.advanced", {
        previous_commit: 15,
        commit_index: 10,
        source: "leader",
      }),
    ],
  }));

  assert.equal(rows.length, 0);
});

test("Replication progress identifies the follower", () => {
  const [row] = buildFaultTimeline(input({
    replicationEvents: [
      event("raft.replication.progress", {
        peer_id: 2,
        request_id: 10,
        match_index: 25,
      }),
    ],
  }));

  assert.equal(row.kind, "replication");
  assert.equal(row.nodeId, 2);
  assert.match(row.detail, /match index: 25/);
});

test("Replication rejection is distinguished", () => {
  const [row] = buildFaultTimeline(input({
    replicationEvents: [
      event("raft.append.rejected", {
        from: 2,
        request_id: 10,
        reason: "follower_rejected",
      }),
    ],
  }));

  assert.equal(row.title, "Replication rejected");
  assert.equal(row.tone, "warning");
});

test("Append send events do not flood the timeline", () => {
  const rows = buildFaultTimeline(input({
    replicationEvents: [
      event("raft.append.sent", {
        to: 2,
        request_id: 10,
        entries: 1,
        previous_index: 10,
      }),
    ],
  }));

  assert.equal(rows.length, 0);
});

test("Docker action creates request and confirmation rows", () => {
  const rows = buildFaultTimeline(input({
    actions: [action()],
  }));

  assert.equal(rows.length, 2);
  assert.equal(rows[0].title, "Docker STOP confirmed");
  assert.equal(rows[1].title, "Docker STOP requested");
  assert.equal(rows[0].source, "docker");
  assert.match(rows[0].detail, /200 ms API duration/);
});

test("Unconfirmed Docker actions remain unconfirmed", () => {
  const [row] = buildFaultTimeline(input({
    actions: [action({ success: false })],
  }));

  assert.equal(row.title, "Docker STOP unconfirmed");
  assert.equal(row.tone, "warning");
});

test("Recovery start action is recorded", () => {
  const [row] = buildFaultTimeline(input({
    actions: [action({ action: "start" })],
  }));

  assert.equal(row.title, "Docker START confirmed");
});

test("Polled data leadership change is recorded", () => {
  const [row] = buildFaultTimeline(input({
    leadership: [{
      id: "leader-1",
      timestamp: timestamp(300),
      group: "Shard 1",
      previous: 1,
      current: 2,
    }],
  }));

  assert.equal(row.source, "polling");
  assert.equal(row.shardId, 1);
  assert.match(row.detail, /Node 1 → Node 2/);
});

test("Metadata leadership maps to group five", () => {
  const [row] = buildFaultTimeline(input({
    leadership: [{
      id: "metadata-1",
      timestamp: timestamp(300),
      group: "Metadata group",
      previous: 1,
      current: 3,
    }],
  }));

  assert.equal(row.shardId, 5);
  assert.equal(row.source, "polling");
});

test("Unknown leader is not reported as elected", () => {
  const [row] = buildFaultTimeline(input({
    leadership: [{
      id: "leader-2",
      timestamp: timestamp(300),
      group: "Shard 1",
      previous: 1,
      current: null,
    }],
  }));

  assert.equal(row.title, "Leader no longer observed");
  assert.equal(row.tone, "warning");
});

test("Reachability loss is recorded", () => {
  const [row] = buildFaultTimeline(input({
    reachability: [{
      id: "reach-1",
      timestamp: timestamp(400),
      nodeId: 1,
      reachable: false,
    }],
  }));

  assert.equal(row.title, "Node no longer reachable");
  assert.equal(row.kind, "recovery");
});

test("Reachability recovery is recorded", () => {
  const [row] = buildFaultTimeline(input({
    reachability: [{
      id: "reach-2",
      timestamp: timestamp(500),
      nodeId: 1,
      reachable: true,
    }],
  }));

  assert.equal(row.title, "Node reachable again");
  assert.equal(row.source, "polling");
});

test("Duplicate telemetry events are deduplicated", () => {
  const item = event("raft.leader.elected");

  const rows = buildFaultTimeline(input({
    electionEvents: [item, item, item],
  }));

  assert.equal(rows.length, 1);
});

test("Different observation sources remain distinct", () => {
  const rows = buildFaultTimeline(input({
    electionEvents: [
      event("raft.leader.elected"),
    ],
    leadership: [{
      id: "leader-3",
      timestamp: timestamp(100),
      group: "Shard 1",
      previous: 2,
      current: 1,
    }],
  }));

  assert.equal(rows.length, 2);

  assert.deepEqual(
    new Set(rows.map(row => row.source)),
    new Set(["telemetry", "polling"]),
  );
});

test("Invalid timestamps are excluded", () => {
  const rows = buildFaultTimeline(input({
    electionEvents: [
      event("raft.election.started", {}, {
        timestamp: "invalid",
      }),
    ],
  }));

  assert.equal(rows.length, 0);
});

function mixedInput() {
  return input({
    electionEvents: [
      event("raft.election.started"),
      event("raft.leader.elected", {}, {
        id: 2,
        node_id: 2,
        shard_id: 2,
        timestamp: timestamp(300),
      }),
    ],
    actions: [action()],
    reachability: [{
      id: "reach-3",
      timestamp: timestamp(500),
      nodeId: 1,
      reachable: true,
    }],
  });
}

test("Timeline is sorted newest first", () => {
  const rows = buildFaultTimeline(mixedInput());

  assert.deepEqual(
    rows.map(row => Date.parse(row.timestamp)),
    [500, 300, 200, 100, 0].map(n => BASE + n),
  );
});

test("Shard filter excludes unrelated entries", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    shardId: 2,
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].shardId, 2);
});

test("Node filter selects associated observations", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    nodeId: 1,
  });

  assert.equal(rows.length, 4);
  assert.ok(rows.every(row => row.nodeId === 1));
});

test("Source filter selects Docker observations", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    source: "docker",
  });

  assert.equal(rows.length, 2);
  assert.ok(rows.every(row => row.source === "docker"));
});

test("Kind filter selects leadership observations", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    kind: "leadership",
  });

  assert.equal(rows.length, 1);
});

test("Time window includes both boundaries", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    since: BASE + 100,
    until: BASE + 300,
  });

  assert.equal(rows.length, 3);
});

test("Combined filters are applied together", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    nodeId: 1,
    source: "docker",
    since: BASE + 100,
  });

  assert.equal(rows.length, 1);
  assert.equal(rows[0].title, "Docker STOP confirmed");
});

test("Custom timeline limit is respected", () => {
  const rows = buildFaultTimeline(mixedInput(), {
    limit: 2,
  });

  assert.equal(rows.length, 2);
  assert.equal(rows[0].title, "Node reachable again");
});

test("Default history limit is enforced", () => {
  const rows = buildFaultTimeline(input({
    electionEvents: Array.from(
      { length: 300 },
      (_, i) => event("raft.election.started", {}, {
        id: i + 1,
        timestamp: timestamp(i),
      }),
    ),
  }));

  assert.equal(rows.length, FAULT_TIMELINE_LIMIT);
  assert.equal(rows.length, 200);
});

test("Input observations are not mutated", () => {
  const original = mixedInput();
  const snapshot = JSON.stringify(original);

  buildFaultTimeline(original);

  assert.equal(JSON.stringify(original), snapshot);
});

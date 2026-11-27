import test from "node:test";
import assert from "node:assert/strict";

import {
  isElectionEvent,
  mergeElectionEvents,
  electionEventLabel,
  electionEventColor,
  electionTerms,
  electionTimeline,
} from "../.test-build/multi/electionTelemetry.js";

function event(id, type, overrides = {}) {
  return {
    id,
    node_id: 2,
    shard_id: 1,
    term: 9,
    category: "raft",
    type,
    timestamp: new Date(
      Date.UTC(2026, 10, 26, 20, 0, 0) + id * 1000,
    ).toISOString(),
    data: {},
    ...overrides,
  };
}

test("recognizes election events", () => {
  for (const type of [
    "raft.role.changed",
    "raft.term.changed",
    "raft.election.started",
    "raft.vote.requested",
    "raft.vote.granted",
    "raft.vote.rejected",
    "raft.leader.elected",
  ]) {
    assert.equal(isElectionEvent(event(1, type)), true);
  }
});

test("excludes heartbeat and replication traffic", () => {
  for (const type of [
    "raft.heartbeat.sent",
    "raft.append.accepted",
    "raft.commit.advanced",
  ]) {
    assert.equal(isElectionEvent(event(1, type)), false);
  }
});

test("retains election history during heartbeat traffic", () => {
  const previous = [
    event(1, "raft.leader.elected"),
  ];

  const heartbeats = Array.from(
    { length: 5000 },
    (_, index) =>
      event(index + 2, "raft.heartbeat.sent"),
  );

  assert.equal(
    mergeElectionEvents(previous, heartbeats),
    previous,
  );
});

test("deduplicates repeated election events", () => {
  const elected = event(1, "raft.leader.elected");

  assert.equal(
    mergeElectionEvents([elected], [elected]).length,
    1,
  );
});

test("bounds retained election history", () => {
  const events = Array.from(
    { length: 1200 },
    (_, index) =>
      event(index + 1, "raft.term.changed"),
  );

  assert.equal(
    mergeElectionEvents([], events).length,
    1000,
  );
});

test("separates terms by shard", () => {
  const events = [
    event(1, "raft.election.started"),
    event(2, "raft.leader.elected", { term: 10 }),
    event(3, "raft.leader.elected", {
      shard_id: 2,
      term: 20,
    }),
  ];

  assert.deepEqual(electionTerms(events, 1), [10, 9]);
});

test("filters timeline by shard and term", () => {
  const events = [
    event(1, "raft.election.started"),
    event(2, "raft.vote.requested"),
    event(3, "raft.leader.elected", { term: 10 }),
    event(4, "raft.vote.granted", { shard_id: 2 }),
  ];

  assert.deepEqual(
    electionTimeline(events, 1, 9).map(item => item.id),
    [1, 2],
  );
});

test("orders election timeline chronologically", () => {
  const events = [
    event(3, "raft.leader.elected"),
    event(1, "raft.election.started"),
    event(2, "raft.vote.granted"),
  ];

  assert.deepEqual(
    electionTimeline(events, 1).map(item => item.id),
    [1, 2, 3],
  );
});

test("provides election labels", () => {
  assert.equal(
    electionEventLabel(event(1, "raft.leader.elected")),
    "Leader elected",
  );
});

test("distinguishes granted and rejected votes", () => {
  assert.notEqual(
    electionEventColor(event(1, "raft.vote.granted")),
    electionEventColor(event(2, "raft.vote.rejected")),
  );
});

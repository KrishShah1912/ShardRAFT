import test from "node:test";
import assert from "node:assert/strict";

import {
  parseLiveEvent,
  eventIdentity,
  heartbeatObservation,
  heartbeatIdentity,
  mergeHeartbeat,
  mergeLiveEvents,
  isRecentEvent,
} from "../.test-build/multi/liveTelemetry.js";

const timestamp = "2026-11-26T23:37:01.024658173Z";

const sent = {
  id: 145103,
  timestamp,
  node_id: 2,
  shard_id: 1,
  category: "raft",
  type: "raft.heartbeat.sent",
  term: 9,
  data: {
    to: 1,
    request_id: 15231,
    entries: 0,
  },
};

const received = {
  ...sent,
  id: 468267,
  node_id: 1,
  type: "raft.heartbeat.received",
  data: {
    from: 2,
    request_id: 15231,
  },
};

const accepted = {
  ...received,
  id: 468268,
  type: "raft.heartbeat.accepted",
};

test("parses valid shard heartbeat", () => {
  assert.deepEqual(parseLiveEvent(sent, 2), sent);
});

test("rejects incorrect source node", () => {
  assert.equal(parseLiveEvent(sent, 3), null);
});

test("rejects invalid shard placement", () => {
  assert.equal(
    parseLiveEvent({ ...sent, shard_id: 3 }, 2),
    null,
  );
});

test("rejects malformed payloads", () => {
  for (const value of [
    null,
    [],
    {},
    { ...sent, id: 0 },
    { ...sent, timestamp: "invalid" },
    { ...sent, data: [] },
  ]) {
    assert.equal(parseLiveEvent(value, 2), null);
  }
});

test("distinguishes heartbeat stages", () => {
  assert.equal(
    heartbeatObservation(sent).stage,
    "sent",
  );
  assert.equal(
    heartbeatObservation(received).stage,
    "received",
  );
  assert.equal(
    heartbeatObservation(accepted).stage,
    "accepted",
  );
});

test("correlates heartbeat stages across nodes", () => {
  const identities = [sent, received, accepted]
    .map(heartbeatObservation)
    .map(heartbeatIdentity);

  assert.equal(new Set(identities).size, 1);
});

test("does not downgrade accepted heartbeat", () => {
  const first = heartbeatObservation(accepted);
  const late = heartbeatObservation(sent);

  assert.equal(
    mergeHeartbeat(first, late).stage,
    "accepted",
  );
});

test("advances heartbeat stage", () => {
  assert.equal(
    mergeHeartbeat(
      heartbeatObservation(sent),
      heartbeatObservation(received),
    ).stage,
    "received",
  );
});

test("separates different Raft terms", () => {
  assert.notEqual(
    heartbeatIdentity(heartbeatObservation(sent)),
    heartbeatIdentity(
      heartbeatObservation({ ...sent, term: 10 }),
    ),
  );
});

test("rejects invalid heartbeat peers", () => {
  assert.equal(
    heartbeatObservation({
      ...sent,
      data: { to: 5, request_id: 15231 },
    }),
    null,
  );
});

test("does not treat transport delivery as acceptance", () => {
  assert.equal(
    heartbeatObservation({
      ...sent,
      category: "transport",
      type: "transport.message.delivered",
    }),
    null,
  );
});

test("deduplicates repeated events", () => {
  assert.equal(
    mergeLiveEvents([sent], [sent]).length,
    1,
  );
});

test("handles event ID reuse after restart", () => {
  assert.notEqual(
    eventIdentity(sent),
    eventIdentity({
      ...sent,
      timestamp: "2026-11-26T23:38:01Z",
    }),
  );
});

test("bounds retained event history", () => {
  assert.equal(
    mergeLiveEvents(
      [sent],
      [received, accepted],
      2,
    ).length,
    2,
  );
});

test("rejects stale events for live animation", () => {
  const now = Date.parse(timestamp);

  assert.equal(isRecentEvent(timestamp, now), true);
  assert.equal(
    isRecentEvent(timestamp, now + 5000),
    false,
  );
  assert.equal(
    isRecentEvent(timestamp, now - 1000),
    false,
  );
});

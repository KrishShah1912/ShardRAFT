import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const {
  deriveShardStates,
  SHARD_PLACEMENT,
} = require(
  "../.test-build/multi/clusterState.js"
);

const {
  normalizeShardMap,
  parseDockerServices,
} = require(
  "../.test-build/api/multiDiagnostics.js"
);

const {
  parseMultiRaftStatus,
} = require(
  "../.test-build/api/multiRaft.js"
);

const leaders = {
  1: 1,
  2: 2,
  3: 3,
  4: 1,
};

function observations() {
  return [1, 2, 3, 4, 5].map(id => ({
    id,
    reachable: true,
    observedAt: Date.now(),
    status: {
      id,
      mode: "multi-raft",
      groups: Object.entries(SHARD_PLACEMENT)
        .filter(([, nodes]) => nodes.includes(id))
        .map(([shard]) => ({
          shard_id: Number(shard),
          role: leaders[shard] === id
            ? "leader"
            : "follower",
          term: 1,
          leader_id: leaders[shard],
          commit_index: 100,
          applied_index: 100,
        })),
    },
  }));
}

function offline(nodes, ids) {
  return nodes.map(node =>
    ids.includes(node.id)
      ? {
          id: node.id,
          reachable: false,
          observedAt: Date.now(),
          error: "unreachable",
        }
      : node,
  );
}

test("four healthy shards with independent leaders", () => {
  const states = deriveShardStates(observations());

  assert.equal(states.length, 4);

  for (const shard of states) {
    assert.equal(shard.state, "healthy");
    assert.equal(shard.reachableReplicas, 3);
    assert.equal(shard.leaderId, leaders[shard.shardId]);
    assert.equal(shard.replicationLag, 0);
  }
});

test("one missing follower produces degraded shard", () => {
  const states = deriveShardStates(
    offline(observations(), [5]),
  );

  assert.equal(states[0].state, "healthy");
  assert.equal(states[2].state, "degraded");
  assert.equal(states[3].state, "degraded");
});

test("missing leader is not replaced with a fake leader", () => {
  const states = deriveShardStates(
    offline(observations(), [1]),
  );

  assert.equal(states[0].state, "electing");
  assert.equal(states[0].leaderId, null);
  assert.equal(states[0].majorityReachable, true);
});

test("two unreachable replicas remove observed majority", () => {
  const states = deriveShardStates(
    offline(observations(), [1, 2]),
  );

  assert.equal(states[0].state, "unavailable");
  assert.equal(states[0].majorityReachable, false);
});

test("replication lag uses observed commit indexes", () => {
  const nodes = observations();

  nodes[1].status.groups.find(
    group => group.shard_id === 1,
  ).commit_index = 95;

  const states = deriveShardStates(nodes);

  assert.equal(states[0].replicationLag, 5);
});

test("conflicting leader observations are not trusted", () => {
  const nodes = observations();

  nodes[1].status.groups.find(
    group => group.shard_id === 1,
  ).role = "leader";

  const states = deriveShardStates(nodes);

  assert.equal(states[0].leaderId, null);
  assert.equal(states[0].state, "electing");
});

test("missing protobuf zero fields are normalized", () => {
  const response = normalizeShardMap({
    node_id: 1,
    consistency: "local",
    shard_map: {
      epoch: 1,
      total_slots: 65536,
      shards: [{
        shard_id: 1,
        epoch: 1,
        slot_ranges: [{ end_slot: 16383 }],
        replicas: [],
      }],
    },
  });

  assert.equal(
    response.shard_map.shards[0]
      .slot_ranges[0].start_slot,
    0,
  );

  assert.equal(
    response.shard_map.shards[0].leader_id,
    0,
  );
});

test("Docker newline-delimited JSON is parsed", () => {
  const result = parseDockerServices(
    '{"Service":"n1","State":"running","Health":"healthy"}\n' +
    '{"Service":"n2","State":"exited"}',
  );

  assert.equal(result.length, 2);
  assert.equal(result[0].health, "healthy");
  assert.equal(result[1].state, "exited");
});

test("Docker JSON array is parsed", () => {
  const result = parseDockerServices(
    '[{"Service":"gateway1","State":"running"}]',
  );

  assert.equal(result[0].service, "gateway1");
});

test("invalid node identity is rejected", () => {
  assert.throws(() =>
    parseMultiRaftStatus(
      observations()[0].status,
      2,
    ),
  );
});

test("duplicate group identifiers are rejected", () => {
  const status = observations()[0].status;

  status.groups.push(status.groups[0]);

  assert.throws(() =>
    parseMultiRaftStatus(status, 1),
  );
});

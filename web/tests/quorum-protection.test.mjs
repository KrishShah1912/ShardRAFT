import test from "node:test";
import assert from "node:assert/strict";

import {
  assessNodeQuorum,
  MAX_OBSERVATION_AGE_MS,
} from "../src/multi/quorumProtection.ts";

import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
  METADATA_REPLICAS,
} from "../src/multi/clusterState.ts";

const NOW = 1700000000000;

function fixture(offline = []) {
  const unavailable = new Set(offline);

  return {
    loading: false,
    lastUpdated: NOW,

    observations: [1, 2, 3, 4, 5].map(id => ({
      id,
      reachable: !unavailable.has(id),
    })),

    shards: DATA_SHARDS.map(shardId => {
      const replicas = SHARD_PLACEMENT[shardId].map(
        nodeId => ({
          nodeId,
          reachable: !unavailable.has(nodeId),
        }),
      );

      return {
        shardId,
        replicas,
        leaderId:
          replicas.find(replica => replica.reachable)
            ?.nodeId ?? null,
      };
    }),

    metadata: METADATA_REPLICAS
      .filter(id => !unavailable.has(id))
      .map(node_id => ({ node_id })),

    metadataLeader:
      METADATA_REPLICAS.find(
        id => !unavailable.has(id),
      ) ?? null,
  };
}

for (const node of [1, 2, 3, 4, 5]) {
  test(`Healthy cluster permits stopping Node ${node}`, () => {
    const result = assessNodeQuorum(
      fixture(),
      node,
      NOW,
    );

    assert.equal(result.allowed, true);
    assert.equal(result.groups.length, 5);
    assert.equal(result.risks.length, 0);

    for (const group of result.groups) {
      assert.equal(group.current, 3);
      assert.equal(group.required, 2);

      assert.equal(
        group.remaining,
        group.members.includes(node) ? 2 : 3,
      );

      assert.equal(group.majorityRetained, true);
    }
  });
}

test("Correct hosted group membership for every node", () => {
  const expected = {
    1: ["shard-1", "shard-4", "metadata"],
    2: ["shard-1", "shard-2", "metadata"],
    3: ["shard-1", "shard-2", "shard-3", "metadata"],
    4: ["shard-2", "shard-3", "shard-4"],
    5: ["shard-3", "shard-4"],
  };

  for (const node of [1, 2, 3, 4, 5]) {
    const result = assessNodeQuorum(
      fixture(),
      node,
      NOW,
    );

    assert.deepEqual(
      result.affected.map(group => group.id),
      expected[node],
    );
  }
});

test("Second failure affecting a data quorum is blocked", () => {
  const result = assessNodeQuorum(
    fixture([4]),
    5,
    NOW,
  );

  assert.equal(result.allowed, false);

  assert.ok(
    result.risks.some(group => group.id === "shard-3"),
  );

  assert.ok(
    result.risks.some(group => group.id === "shard-4"),
  );
});

test("Metadata quorum loss is blocked", () => {
  const result = assessNodeQuorum(
    fixture([1]),
    2,
    NOW,
  );

  assert.equal(result.allowed, false);

  const metadata = result.groups.find(
    group => group.id === "metadata",
  );

  assert.equal(metadata.current, 2);
  assert.equal(metadata.remaining, 1);
  assert.equal(metadata.majorityRetained, false);
});

test("Metadata failure alone can block an action", () => {
  const cluster = fixture();

  cluster.metadata = [
    { node_id: 1 },
    { node_id: 2 },
  ];

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.allowed, false);

  assert.deepEqual(
    result.risks.map(group => group.id),
    ["metadata"],
  );
});

test("Existing unavailable group blocks unrelated stop", () => {
  const cluster = fixture();

  cluster.shards.find(
    shard => shard.shardId === 1,
  ).replicas.forEach(replica => {
    replica.reachable = replica.nodeId === 1;
  });

  const result = assessNodeQuorum(cluster, 5, NOW);

  assert.equal(result.allowed, false);

  assert.ok(
    result.risks.some(
      group => group.id === "shard-1" && !group.affected,
    ),
  );
});

test("Already unavailable target is not subtracted twice", () => {
  const result = assessNodeQuorum(
    fixture([1]),
    1,
    NOW,
  );

  assert.equal(result.allowed, true);

  for (const group of result.affected) {
    assert.equal(group.current, 2);
    assert.equal(group.remaining, 2);
  }
});

test("Missing metadata observations block disruptive actions", () => {
  const cluster = fixture();
  cluster.metadata = [];

  const result = assessNodeQuorum(cluster, 5, NOW);

  assert.equal(result.allowed, false);

  assert.ok(
    result.risks.some(group => group.id === "metadata"),
  );
});

test("Duplicate metadata observations do not inflate quorum", () => {
  const cluster = fixture();

  cluster.metadata = [
    { node_id: 1 },
    { node_id: 1 },
    { node_id: 1 },
  ];

  const result = assessNodeQuorum(cluster, 5, NOW);

  const metadata = result.groups.find(
    group => group.id === "metadata",
  );

  assert.equal(metadata.current, 1);
  assert.equal(result.allowed, false);
});

test("Unknown metadata member is not counted", () => {
  const cluster = fixture();

  cluster.metadata = [
    { node_id: 1 },
    { node_id: 99 },
  ];

  const result = assessNodeQuorum(cluster, 5, NOW);

  assert.equal(result.allowed, false);

  assert.equal(
    result.groups.find(
      group => group.id === "metadata",
    ).current,
    1,
  );
});

test("Missing data group observations block actions", () => {
  const cluster = fixture();

  cluster.shards = cluster.shards.filter(
    shard => shard.shardId !== 2,
  );

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.allowed, false);

  assert.ok(
    result.risks.some(group => group.id === "shard-2"),
  );
});

test("Loading cluster observations block actions", () => {
  const cluster = fixture();
  cluster.loading = true;

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.observationsAvailable, false);
  assert.equal(result.allowed, false);
});

test("Missing observation timestamp blocks actions", () => {
  const cluster = fixture();
  cluster.lastUpdated = null;

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.observationAgeMs, null);
  assert.equal(result.allowed, false);
});

test("Stale observations block actions", () => {
  const cluster = fixture();

  const result = assessNodeQuorum(
    cluster,
    1,
    NOW + MAX_OBSERVATION_AGE_MS + 1,
  );

  assert.equal(result.observationsAvailable, false);
  assert.equal(result.allowed, false);
});

test("Observation freshness boundary is accepted", () => {
  const result = assessNodeQuorum(
    fixture(),
    1,
    NOW + MAX_OBSERVATION_AGE_MS,
  );

  assert.equal(result.observationsAvailable, true);
  assert.equal(result.allowed, true);
});

test("Future observation timestamp is rejected", () => {
  const cluster = fixture();
  cluster.lastUpdated = NOW + 1000;

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.allowed, false);
});

test("Incomplete physical observations block actions", () => {
  const cluster = fixture();

  cluster.observations =
    cluster.observations.filter(node => node.id !== 5);

  const result = assessNodeQuorum(cluster, 1, NOW);

  assert.equal(result.observationsAvailable, false);
  assert.equal(result.allowed, false);
});

test("Invalid node membership is rejected", () => {
  const result = assessNodeQuorum(fixture(), 99, NOW);

  assert.equal(result.allowed, false);
  assert.equal(result.affected.length, 0);
});

test("Assessment preserves original cluster observations", () => {
  const cluster = fixture();
  const original = JSON.stringify(cluster);

  assessNodeQuorum(cluster, 1, NOW);

  assert.equal(JSON.stringify(cluster), original);
});

test("Exhaustive node availability combinations", () => {
  let checked = 0;

  for (let mask = 0; mask < 32; mask++) {
    const offline = [1, 2, 3, 4, 5].filter(
      id => (mask & (1 << (id - 1))) !== 0,
    );

    for (const target of [1, 2, 3, 4, 5]) {
      const cluster = fixture(offline);

      const result = assessNodeQuorum(
        cluster,
        target,
        NOW,
      );

      const memberships = [
        ...Object.values(SHARD_PLACEMENT),
        METADATA_REPLICAS,
      ];

      const expected = memberships.every(
        members => members.filter(
          id => id !== target && !offline.includes(id),
        ).length >= 2,
      );

      assert.equal(
        result.allowed,
        expected,
        `Offline=${offline.join(",")} target=${target}`,
      );

      checked++;
    }
  }

  assert.equal(checked, 160);

  console.log(
    `Validated ${checked} node availability combinations`,
  );
});

import test from "node:test";
import assert from "node:assert/strict";

import {
  createDemoWorkload,
  validateDemoRoute,
  DEFAULT_DEMO_CONFIG,
} from "../src/multi/demoWorkload.ts";

import {
  previewKeyRouting,
} from "../src/multi/keyRouting.ts";

const map = {
  epoch: 1,
  total_slots: 65536,
  shards: [
    {
      shard_id: 1,
      slot_ranges: [
        { start_slot: 0, end_slot: 16383 },
      ],
      replicas: [1, 2, 3].map(node_id => ({ node_id })),
    },
    {
      shard_id: 2,
      slot_ranges: [
        { start_slot: 16384, end_slot: 32767 },
      ],
      replicas: [2, 3, 4].map(node_id => ({ node_id })),
    },
    {
      shard_id: 3,
      slot_ranges: [
        { start_slot: 32768, end_slot: 49151 },
      ],
      replicas: [3, 4, 5].map(node_id => ({ node_id })),
    },
    {
      shard_id: 4,
      slot_ranges: [
        { start_slot: 49152, end_slot: 65535 },
      ],
      replicas: [1, 4, 5].map(node_id => ({ node_id })),
    },
  ],
};

function config(overrides = {}) {
  return {
    ...DEFAULT_DEMO_CONFIG,
    ...overrides,
  };
}

for (const shardId of [1, 2, 3, 4]) {
  test(`Shard ${shardId}: generate 20 correctly routed keys`, () => {
    const workload = createDemoWorkload(
      config({ shardId, count: 20 }),
      map,
      `test-shard-${shardId}`,
    );

    assert.equal(workload.operations.length, 20);

    const keys = new Set();

    for (const operation of workload.operations) {
      const route = previewKeyRouting(
        operation.command.key,
        map,
      );

      assert.equal(route.shardId, shardId);
      assert.equal(route.slot, operation.slot);
      assert.equal(route.hash, operation.hash);
      assert.equal(operation.command.operation, "PUT");

      assert.ok(
        operation.slot >= (shardId - 1) * 16384,
      );

      assert.ok(
        operation.slot < shardId * 16384,
      );

      keys.add(operation.command.key);

      assert.doesNotThrow(() =>
        validateDemoRoute(operation, map),
      );
    }

    assert.equal(keys.size, 20);

    console.log(
      `Shard ${shardId}: 20/20 correctly routed unique keys`,
    );
  });
}

test("Default workload configuration", () => {
  const workload = createDemoWorkload(
    DEFAULT_DEMO_CONFIG,
    map,
    "default-test",
  );

  assert.equal(workload.operations.length, 10);
  assert.equal(workload.config.intervalMs, 2000);
  assert.equal(workload.epoch, 1);
});

test("Unique mutation identities within a run", () => {
  const workload = createDemoWorkload(
    config(),
    map,
    "identity-test",
  );

  const identities = workload.operations.map(
    operation =>
      `${operation.command.clientId}:${operation.command.requestId}`,
  );

  assert.equal(
    new Set(identities).size,
    workload.operations.length,
  );
});

test("Independent runs generate different identities and keys", () => {
  const first = createDemoWorkload(config(), map);
  const second = createDemoWorkload(config(), map);

  assert.notEqual(first.runId, second.runId);

  const keys = new Set([
    ...first.operations.map(op => op.command.key),
    ...second.operations.map(op => op.command.key),
  ]);

  assert.equal(keys.size, 20);

  assert.notEqual(
    first.operations[0].command.clientId,
    second.operations[0].command.clientId,
  );
});

test("Invalid shard is rejected", () => {
  assert.throws(() =>
    createDemoWorkload(
      config({ shardId: 5 }),
      map,
    ),
  );
});

test("Invalid operation counts are rejected", () => {
  for (const count of [0, -1, 21, 1.5]) {
    assert.throws(() =>
      createDemoWorkload(config({ count }), map),
    );
  }
});

test("Invalid intervals are rejected", () => {
  for (const intervalMs of [0, 999, 5001, 2000.5]) {
    assert.throws(() =>
      createDemoWorkload(config({ intervalMs }), map),
    );
  }
});

test("Invalid metadata slot count is rejected", () => {
  assert.throws(() =>
    createDemoWorkload(
      config(),
      { ...map, total_slots: 100 },
    ),
  );
});

test("Missing target shard is rejected", () => {
  assert.throws(() =>
    createDemoWorkload(
      config({ shardId: 4 }),
      {
        ...map,
        shards: map.shards.filter(
          shard => shard.shard_id !== 4,
        ),
      },
    ),
  );
});

test("Ambiguous target shard is rejected", () => {
  assert.throws(() =>
    createDemoWorkload(
      config(),
      {
        ...map,
        shards: [...map.shards, map.shards[0]],
      },
    ),
  );
});

test("Changed destination is detected before execution", () => {
  const workload = createDemoWorkload(
    config(),
    map,
    "route-change-test",
  );

  const changed = structuredClone(map);

  changed.epoch = 2;

  changed.shards[0].slot_ranges = [];
  changed.shards[1].slot_ranges.push({
    start_slot: 0,
    end_slot: 16383,
  });

  assert.throws(() =>
    validateDemoRoute(
      workload.operations[0],
      changed,
    ),
  );
});

test("Overlapping slot ranges are rejected during revalidation", () => {
  const workload = createDemoWorkload(
    config(),
    map,
    "overlap-test",
  );

  const changed = structuredClone(map);

  changed.shards[1].slot_ranges.push({
    start_slot: 0,
    end_slot: 16383,
  });

  assert.throws(() =>
    validateDemoRoute(
      workload.operations[0],
      changed,
    ),
  );
});

test("Unchanged routing remains valid after epoch advancement", () => {
  const workload = createDemoWorkload(
    config(),
    map,
    "epoch-test",
  );

  assert.doesNotThrow(() =>
    validateDemoRoute(
      workload.operations[0],
      { ...map, epoch: 2 },
    ),
  );
});

test("Invalid demonstration identity is rejected", () => {
  assert.throws(() =>
    createDemoWorkload(
      config(),
      map,
      "invalid/run/id",
    ),
  );
});

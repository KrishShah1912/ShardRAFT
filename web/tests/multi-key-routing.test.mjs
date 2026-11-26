import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";

const require = createRequire(import.meta.url);

const {
  crc32,
  keySlot,
  findShardForSlot,
  previewKeyRouting,
} = require("../.test-build/multi/keyRouting.js");

const vectors = JSON.parse(
  readFileSync(
    new URL("./fixtures/crc32-vectors.json", import.meta.url),
    "utf8",
  ),
);

const placements = [
  [1, 2, 3],
  [2, 3, 4],
  [3, 4, 5],
  [1, 4, 5],
];

function layout() {
  return {
    epoch: 1,
    total_slots: 65536,
    shards: placements.map((nodes, index) => ({
      shard_id: index + 1,
      slot_ranges: [{
        start_slot: index * 16384,
        end_slot: (index + 1) * 16384 - 1,
      }],
      replicas: nodes.map(node_id => ({ node_id })),
      leader_id: nodes[0],
    })),
  };
}

for (const vector of vectors) {
  test(`Go CRC32 parity: ${JSON.stringify(vector.key)}`, () => {
    assert.equal(crc32(vector.key), vector.hash);
    assert.equal(keySlot(vector.key), vector.slot);
  });
}

test("all shard boundary slots route correctly", () => {
  const map = layout();

  for (let i = 0; i < 4; i++) {
    assert.equal(
      findShardForSlot(i * 16384, map).shard_id,
      i + 1,
    );

    assert.equal(
      findShardForSlot((i + 1) * 16384 - 1, map).shard_id,
      i + 1,
    );
  }
});

test("all 65536 slots have exactly one owner", () => {
  const map = layout();

  for (let slot = 0; slot < 65536; slot++) {
    assert.equal(
      findShardForSlot(slot, map).shard_id,
      Math.floor(slot / 16384) + 1,
    );
  }
});

test("invalid slots are rejected", () => {
  const map = layout();

  for (const slot of [-1, 65536, 1.5, NaN]) {
    assert.equal(findShardForSlot(slot, map), null);
  }
});

test("missing shard ownership is not fabricated", () => {
  const map = layout();
  map.shards = [];

  const result = previewKeyRouting("demo:user", map);

  assert.equal(result.shardId, null);
  assert.deepEqual(result.replicaIds, []);
  assert.equal(result.leaderHint, null);
});

test("overlapping ownership is rejected", () => {
  const map = layout();

  map.shards[1].slot_ranges = [
    { start_slot: 0, end_slot: 32767 },
  ];

  assert.equal(findShardForSlot(100, map), null);
});

test("routing preview returns metadata epoch and replicas", () => {
  const map = layout();

  const result = previewKeyRouting("demo:user", map);

  assert.equal(result.epoch, 1);
  assert.equal(result.replicaIds.length, 3);

  assert.deepEqual(
    result.replicaIds,
    placements[result.shardId - 1],
  );
});

test("missing leader hint remains unknown", () => {
  const map = layout();

  for (const shard of map.shards) {
    delete shard.leader_id;
  }

  assert.equal(
    previewKeyRouting("demo:user", map).leaderHint,
    null,
  );
});

test("leader outside replica membership is rejected", () => {
  const map = layout();

  for (const shard of map.shards) {
    shard.leader_id = 99;
  }

  assert.equal(
    previewKeyRouting("demo:user", map).leaderHint,
    null,
  );
});

test("invalid metadata slot count is rejected", () => {
  const map = layout();
  map.total_slots = 1024;

  assert.equal(findShardForSlot(100, map), null);
});

test("empty routing preview key is rejected", () => {
  assert.throws(() => previewKeyRouting("", layout()));
});

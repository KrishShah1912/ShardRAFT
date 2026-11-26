export const TOTAL_SLOTS = 65536;

export interface RoutingShard {
  shard_id: number;
  slot_ranges: {
    start_slot: number;
    end_slot: number;
  }[];
  replicas: {
    node_id: number;
    address?: string;
  }[];
  leader_id?: number;
}

export interface RoutingMap {
  epoch: number;
  total_slots: number;
  shards: RoutingShard[];
}

export interface KeyRoutingPreview {
  key: string;
  hash: number;
  slot: number;
  shardId: number | null;
  replicaIds: number[];
  leaderHint: number | null;
  epoch: number;
}

const CRC32_TABLE = new Uint32Array(256);

for (let i = 0; i < 256; i++) {
  let crc = i;

  for (let bit = 0; bit < 8; bit++) {
    crc = (crc & 1)
      ? (crc >>> 1) ^ 0xedb88320
      : crc >>> 1;
  }

  CRC32_TABLE[i] = crc >>> 0;
}

export function crc32(key: string): number {
  const bytes = new TextEncoder().encode(key);

  let crc = 0xffffffff;

  for (const byte of bytes) {
    crc =
      (crc >>> 8) ^
      CRC32_TABLE[(crc ^ byte) & 0xff];
  }

  return (crc ^ 0xffffffff) >>> 0;
}

export function keySlot(key: string): number {
  return crc32(key) % TOTAL_SLOTS;
}

export function findShardForSlot(
  slot: number,
  map: RoutingMap,
): RoutingShard | null {
  if (
    map.total_slots !== TOTAL_SLOTS ||
    !Number.isInteger(slot) ||
    slot < 0 ||
    slot >= TOTAL_SLOTS
  ) {
    return null;
  }

  const matches = map.shards.filter(
    shard => shard.slot_ranges.some(
      range =>
        slot >= range.start_slot &&
        slot <= range.end_slot,
    ),
  );

  return matches.length === 1
    ? matches[0]
    : null;
}

export function previewKeyRouting(
  key: string,
  map: RoutingMap,
): KeyRoutingPreview {
  if (!key.trim()) {
    throw new Error("A key is required.");
  }

  const hash = crc32(key);
  const slot = hash % TOTAL_SLOTS;

  const shard = findShardForSlot(slot, map);

  const replicaIds =
    shard?.replicas.map(replica => replica.node_id) ?? [];

  const leader = shard?.leader_id ?? 0;

  return {
    key,
    hash,
    slot,
    shardId: shard?.shard_id ?? null,
    replicaIds,
    leaderHint: replicaIds.includes(leader)
      ? leader
      : null,
    epoch: map.epoch,
  };
}

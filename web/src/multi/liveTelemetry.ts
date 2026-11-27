export type LiveEvent = {
  id: number;
  timestamp: string;
  node_id: number;
  shard_id: number;
  category: string;
  type: string;
  term?: number;
  data: Record<string, unknown>;
};

export type HeartbeatStage =
  | "sent"
  | "received"
  | "accepted";

export type HeartbeatObservation = {
  shardId: number;
  leaderId: number;
  followerId: number;
  term: number;
  requestId: number;
  stage: HeartbeatStage;
  timestamp: string;
};

export const LIVE_EVENT_TYPES = [
  "raft.role.changed",
  "raft.term.changed",
  "raft.election.started",
  "raft.vote.requested",
  "raft.vote.granted",
  "raft.vote.rejected",
  "raft.leader.elected",
  "raft.entry.appended",
  "raft.noop.appended",
  "raft.append.sent",
  "raft.append.received",
  "raft.append.accepted",
  "raft.append.rejected",
  "raft.replication.progress",
  "raft.commit.advanced",
  "raft.heartbeat.sent",
  "raft.heartbeat.received",
  "raft.heartbeat.accepted",
  "transport.message.queued",
  "transport.message.sent",
  "transport.message.delivered",
] as const;

const PLACEMENT: Record<number, readonly number[]> = {
  1: [1, 2, 3],
  2: [2, 3, 4],
  3: [3, 4, 5],
  4: [4, 5, 1],
};

function isRecord(
  value: unknown,
): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

function positiveInteger(
  value: unknown,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value > 0
  );
}

export function parseLiveEvent(
  raw: unknown,
  expectedNode: number,
): LiveEvent | null {
  if (!isRecord(raw)) return null;

  if (
    !positiveInteger(raw.id) ||
    raw.node_id !== expectedNode ||
    !positiveInteger(raw.shard_id) ||
    !PLACEMENT[raw.shard_id]?.includes(expectedNode) ||
    typeof raw.timestamp !== "string" ||
    !Number.isFinite(Date.parse(raw.timestamp)) ||
    typeof raw.category !== "string" ||
    typeof raw.type !== "string" ||
    !isRecord(raw.data)
  ) {
    return null;
  }

  if (
    raw.term !== undefined &&
    !positiveInteger(raw.term)
  ) {
    return null;
  }

  return {
    id: raw.id,
    timestamp: raw.timestamp,
    node_id: expectedNode,
    shard_id: raw.shard_id,
    category: raw.category,
    type: raw.type,
    term: raw.term as number | undefined,
    data: raw.data,
  };
}

export function eventIdentity(
  event: LiveEvent,
): string {
  return [
    event.node_id,
    event.id,
    event.timestamp,
  ].join(":");
}

export function heartbeatObservation(
  event: LiveEvent,
): HeartbeatObservation | null {
  let stage: HeartbeatStage;

  switch (event.type) {
    case "raft.heartbeat.sent":
      stage = "sent";
      break;
    case "raft.heartbeat.received":
      stage = "received";
      break;
    case "raft.heartbeat.accepted":
      stage = "accepted";
      break;
    default:
      return null;
  }

  if (event.category !== "raft") return null;

  const leaderId =
    stage === "sent"
      ? event.node_id
      : event.data.from;

  const followerId =
    stage === "sent"
      ? event.data.to
      : event.node_id;

  const requestId = event.data.request_id;

  if (
    !positiveInteger(leaderId) ||
    !positiveInteger(followerId) ||
    !positiveInteger(requestId) ||
    !positiveInteger(event.term) ||
    leaderId === followerId
  ) {
    return null;
  }

  const replicas = PLACEMENT[event.shard_id];

  if (
    !replicas?.includes(leaderId) ||
    !replicas.includes(followerId)
  ) {
    return null;
  }

  return {
    shardId: event.shard_id,
    leaderId,
    followerId,
    term: event.term,
    requestId,
    stage,
    timestamp: event.timestamp,
  };
}

export function heartbeatIdentity(
  heartbeat: HeartbeatObservation,
): string {
  return [
    heartbeat.shardId,
    heartbeat.term,
    heartbeat.leaderId,
    heartbeat.followerId,
    heartbeat.requestId,
  ].join(":");
}

const STAGE_ORDER: Record<HeartbeatStage, number> = {
  sent: 0,
  received: 1,
  accepted: 2,
};

export function mergeHeartbeat(
  previous: HeartbeatObservation | undefined,
  incoming: HeartbeatObservation,
): HeartbeatObservation {
  if (
    !previous ||
    heartbeatIdentity(previous) !==
      heartbeatIdentity(incoming)
  ) {
    return incoming;
  }

  if (
    STAGE_ORDER[incoming.stage] >
    STAGE_ORDER[previous.stage]
  ) {
    return incoming;
  }

  if (
    incoming.stage === previous.stage &&
    incoming.timestamp > previous.timestamp
  ) {
    return incoming;
  }

  return previous;
}

export function mergeLiveEvents(
  previous: readonly LiveEvent[],
  incoming: readonly LiveEvent[],
  limit = 2000,
): LiveEvent[] {
  const events = new Map<string, LiveEvent>();

  for (const event of [...previous, ...incoming]) {
    events.set(eventIdentity(event), event);
  }

  return [...events.values()]
    .sort(
      (a, b) =>
        Date.parse(b.timestamp) -
          Date.parse(a.timestamp) ||
        b.timestamp.localeCompare(a.timestamp) ||
        b.id - a.id ||
        b.node_id - a.node_id,
    )
    .slice(0, Math.max(0, limit));
}

export function isRecentEvent(
  timestamp: string,
  now: number,
  maxAgeMs = 3000,
): boolean {
  const time = Date.parse(timestamp);
  const age = now - time;

  return (
    Number.isFinite(time) &&
    age >= 0 &&
    age <= maxAgeMs
  );
}

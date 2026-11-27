import {
  mergeLiveEvents,
  type LiveEvent,
} from "./liveTelemetry";

export type ReplicationStage =
  | "sent"
  | "received"
  | "accepted"
  | "acknowledged"
  | "rejected";

export type ReplicationObservation = {
  shardId: number;
  term: number;
  leaderId: number;
  followerId: number;
  requestId: number;
  stage: ReplicationStage;
  timestamp: string;
  entries: number | null;
  firstIndex: number | null;
  lastIndex: number | null;
  reason: string | null;
};

const PLACEMENT: Record<number, readonly number[]> = {
  1: [1, 2, 3],
  2: [2, 3, 4],
  3: [3, 4, 5],
  4: [4, 5, 1],
};

const TYPES = new Set([
  "raft.entry.appended",
  "raft.noop.appended",
  "raft.append.sent",
  "raft.append.received",
  "raft.append.accepted",
  "raft.append.rejected",
  "raft.replication.progress",
  "raft.commit.advanced",
]);

function integer(
  value: unknown,
  minimum = 0,
): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= minimum
  );
}

export function isReplicationEvent(
  event: LiveEvent,
): boolean {
  return (
    event.category === "raft" &&
    TYPES.has(event.type)
  );
}

export function mergeReplicationEvents(
  previous: readonly LiveEvent[],
  incoming: readonly LiveEvent[],
  limit = 4000,
): LiveEvent[] {
  return mergeLiveEvents(
    previous,
    incoming.filter(isReplicationEvent),
    limit,
  );
}

export function replicationIdentity(
  observation: ReplicationObservation,
): string {
  return [
    observation.shardId,
    observation.term,
    observation.leaderId,
    observation.followerId,
    observation.requestId,
  ].join(":");
}

export function replicationObservation(
  event: LiveEvent,
): ReplicationObservation | null {
  if (
    !isReplicationEvent(event) ||
    !integer(event.term, 1)
  ) {
    return null;
  }

  const data = event.data;

  let stage: ReplicationStage;
  let leaderId: unknown;
  let followerId: unknown;

  switch (event.type) {
    case "raft.append.sent":
      stage = "sent";
      leaderId = event.node_id;
      followerId = data.to;
      break;

    case "raft.append.received":
      stage = "received";
      leaderId = data.from;
      followerId = event.node_id;
      break;

    case "raft.append.accepted":
      stage = "accepted";
      leaderId = data.from;
      followerId = event.node_id;
      break;

    case "raft.replication.progress":
      stage = "acknowledged";
      leaderId = event.node_id;
      followerId = data.peer_id;
      break;

    case "raft.append.rejected":
      stage = "rejected";

      if (data.reason === "follower_rejected") {
        leaderId = event.node_id;
        followerId = data.from;
      } else {
        leaderId = data.from;
        followerId = event.node_id;
      }

      break;

    default:
      return null;
  }

  if (
    !integer(leaderId, 1) ||
    !integer(followerId, 1) ||
    !integer(data.request_id, 1) ||
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

  let entries: number | null = null;
  let firstIndex: number | null = null;
  let lastIndex: number | null = null;

  if (stage === "sent" || stage === "received") {
    if (
      !integer(data.entries, 1) ||
      !integer(data.previous_index) ||
      !Number.isSafeInteger(
        data.previous_index + data.entries,
      )
    ) {
      return null;
    }

    entries = data.entries;
    firstIndex = data.previous_index + 1;
    lastIndex = data.previous_index + data.entries;
  }

  if (stage === "accepted") {
    if (!integer(data.index)) return null;
    lastIndex = data.index;
  }

  if (stage === "acknowledged") {
    if (!integer(data.match_index)) return null;
    lastIndex = data.match_index;
  }

  return {
    shardId: event.shard_id,
    term: event.term,
    leaderId,
    followerId,
    requestId: data.request_id,
    stage,
    timestamp: event.timestamp,
    entries,
    firstIndex,
    lastIndex,
    reason:
      typeof data.reason === "string"
        ? data.reason
        : null,
  };
}

const STAGE_ORDER: Record<ReplicationStage, number> = {
  sent: 0,
  received: 1,
  accepted: 2,
  acknowledged: 3,
  rejected: 3,
};

export function correlateReplication(
  events: readonly LiveEvent[],
): ReplicationObservation[] {
  const observations =
    new Map<string, ReplicationObservation>();

  for (const event of events) {
    const incoming = replicationObservation(event);

    if (!incoming) continue;

    const key = replicationIdentity(incoming);
    const previous = observations.get(key);

    if (!previous) {
      observations.set(key, incoming);
      continue;
    }

    const newer =
      Date.parse(incoming.timestamp) >=
      Date.parse(previous.timestamp);

    const advances =
      STAGE_ORDER[incoming.stage] >
      STAGE_ORDER[previous.stage];

    const sameStageOrder =
      STAGE_ORDER[incoming.stage] ===
      STAGE_ORDER[previous.stage];

    const latest =
      advances || (sameStageOrder && newer)
        ? incoming
        : previous;

    observations.set(key, {
      ...latest,
      entries:
        latest.entries ??
        incoming.entries ??
        previous.entries,
      firstIndex:
        latest.firstIndex ??
        incoming.firstIndex ??
        previous.firstIndex,
      lastIndex:
        latest.lastIndex ??
        incoming.lastIndex ??
        previous.lastIndex,
    });
  }

  return [...observations.values()]
    // A progress or rejection event alone does not
    // establish that the request carried log entries.
    .filter(item => item.entries !== null)
    .sort(
      (a, b) =>
        Date.parse(b.timestamp) -
        Date.parse(a.timestamp),
    );
}

export type CommitObservation = {
  shardId: number;
  nodeId: number;
  term: number;
  previousCommit: number;
  commitIndex: number;
  advancedBy: number;
  source: string;
  replicas: number | null;
  majority: number | null;
  timestamp: string;
};

export function commitObservation(
  event: LiveEvent,
): CommitObservation | null {
  if (
    event.category !== "raft" ||
    event.type !== "raft.commit.advanced" ||
    !integer(event.term, 1)
  ) {
    return null;
  }

  const data = event.data;

  if (
    !integer(data.previous_commit) ||
    !integer(data.commit_index) ||
    data.commit_index <= data.previous_commit ||
    typeof data.source !== "string" ||
    !PLACEMENT[event.shard_id]?.includes(event.node_id)
  ) {
    return null;
  }

  return {
    shardId: event.shard_id,
    nodeId: event.node_id,
    term: event.term,
    previousCommit: data.previous_commit,
    commitIndex: data.commit_index,
    advancedBy:
      data.commit_index - data.previous_commit,
    source: data.source,
    replicas:
      integer(data.replicas, 1)
        ? data.replicas
        : null,
    majority:
      integer(data.majority, 1)
        ? data.majority
        : null,
    timestamp: event.timestamp,
  };
}

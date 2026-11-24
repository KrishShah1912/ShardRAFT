import type { RaftEvent } from "../types";

export type EventMode =
  | "important"
  | "all"
  | "heartbeat";

export type EventSeverity =
  | "normal"
  | "success"
  | "warning"
  | "danger";

export interface HeartbeatPeerTelemetry {
  nodeId: number;
  leaderId: number;
  lastSentAt?: number;
  lastReceivedAt?: number;
  lastAcceptedAt?: number;
  lastSeenAt?: number;
  ageMs?: number;
  sent: number;
  received: number;
  accepted: number;
  deliveryPercent: number;
  status: "healthy" | "delayed" | "lost" | "unknown";
  pulses: number[];
}

export interface RequestLifecycle {
  requestId: string;
  events: RaftEvent[];
  startedAt: number;
  completedAt?: number;
  durationMs?: number;
  from?: number;
  to?: number;
  term?: number;
  index?: number;
  entries?: number;
  committed: boolean;
}

export function isHeartbeatEvent(
  event: RaftEvent,
): boolean {
  return event.type.startsWith("raft.heartbeat.");
}

export function isImportantEvent(
  event: RaftEvent,
): boolean {
  if (isHeartbeatEvent(event)) {
    return false;
  }

  return [
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
  ].includes(event.type);
}

export function eventSeverity(
  event: RaftEvent,
): EventSeverity {
  if (
    event.type.includes("rejected") ||
    event.type.includes("failure")
  ) {
    return "danger";
  }

  if (
    event.type === "raft.election.started" ||
    event.type === "raft.term.changed" ||
    event.type === "raft.role.changed"
  ) {
    return "warning";
  }

  if (
    event.type === "raft.leader.elected" ||
    event.type === "raft.commit.advanced" ||
    event.type === "raft.replication.progress"
  ) {
    return "success";
  }

  return "normal";
}

export function eventCategory(
  event: RaftEvent,
): string {
  if (isHeartbeatEvent(event)) {
    return "HEARTBEAT";
  }

  if (
    event.type.includes("vote") ||
    event.type.includes("election") ||
    event.type.includes("leader") ||
    event.type.includes("role") ||
    event.type.includes("term")
  ) {
    return "ELECTION";
  }

  if (
    event.type.includes("append") ||
    event.type.includes("replication") ||
    event.type.includes("commit") ||
    event.type.includes("entry") ||
    event.type.includes("noop")
  ) {
    return "REPLICATION";
  }

  return event.category?.toUpperCase() || "RAFT";
}

export function requestID(
  event: RaftEvent,
): string | undefined {
  const value = event.data?.request_id;

  if (
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return String(value);
  }

  return undefined;
}

export function eventNodes(
  event: RaftEvent,
): {
  from?: number;
  to?: number;
} {
  const fromValue = event.data?.from;
  const toValue = event.data?.to;

  const from =
    typeof fromValue === "number"
      ? fromValue
      : undefined;

  const to =
    typeof toValue === "number"
      ? toValue
      : undefined;

  return { from, to };
}

export function buildRequestLifecycles(
  events: RaftEvent[],
): RequestLifecycle[] {
  const groups = new Map<string, RaftEvent[]>();

  for (const event of events) {
    const id = requestID(event);

    if (!id) {
      continue;
    }

    const group = groups.get(id) ?? [];
    group.push(event);
    groups.set(id, group);
  }

  const result: RequestLifecycle[] = [];

  for (const [id, grouped] of groups) {
    const sorted = [...grouped].sort(
      (a, b) =>
        new Date(a.timestamp).getTime() -
        new Date(b.timestamp).getTime(),
    );

    const first = sorted[0];
    const last = sorted[sorted.length - 1];

    const startedAt = new Date(
      first.timestamp,
    ).getTime();

    const completedAt = new Date(
      last.timestamp,
    ).getTime();

    let from: number | undefined;
    let to: number | undefined;
    let index: number | undefined;
    let entries: number | undefined;

    for (const event of sorted) {
      const nodes = eventNodes(event);

      if (nodes.from !== undefined) {
        from = nodes.from;
      }

      if (nodes.to !== undefined) {
        to = nodes.to;
      }

      const eventIndex = event.data?.index;

      if (typeof eventIndex === "number") {
        index = eventIndex;
      }

      const eventEntries = event.data?.entries;

      if (typeof eventEntries === "number") {
        entries = eventEntries;
      }
    }

    result.push({
      requestId: id,
      events: sorted,
      startedAt,
      completedAt,
      durationMs: Math.max(
        0,
        completedAt - startedAt,
      ),
      from,
      to,
      term: first.term,
      index,
      entries,
      committed: sorted.some(
        (event) =>
          event.type ===
            "raft.replication.progress" ||
          event.type === "raft.commit.advanced",
      ),
    });
  }

  return result.sort(
    (a, b) => b.startedAt - a.startedAt,
  );
}

export function buildHeartbeatTelemetry(
  events: RaftEvent[],
  leaderId?: number,
): HeartbeatPeerTelemetry[] {
  if (!leaderId) {
    return [];
  }

  const now = Date.now();

  const heartbeatEvents = events
    .filter(isHeartbeatEvent)
    .sort(
      (a, b) =>
        new Date(a.timestamp).getTime() -
        new Date(b.timestamp).getTime(),
    );

  const peers = [1, 2, 3].filter(
    (id) => id !== leaderId,
  );

  return peers.map((peerId) => {
    let sent = 0;
    let received = 0;
    let accepted = 0;

    let lastSentAt: number | undefined;
    let lastReceivedAt: number | undefined;
    let lastAcceptedAt: number | undefined;

    const pulses: number[] = [];

    for (const event of heartbeatEvents) {
      const timestamp = new Date(
        event.timestamp,
      ).getTime();

      const { from, to } = eventNodes(event);

      if (
        event.type === "raft.heartbeat.sent" &&
        event.node_id === leaderId &&
        to === peerId
      ) {
        sent += 1;
        lastSentAt = timestamp;
        pulses.push(timestamp);
      }

      if (
        event.type ===
          "raft.heartbeat.received" &&
        event.node_id === peerId &&
        from === leaderId
      ) {
        received += 1;
        lastReceivedAt = timestamp;
      }

      if (
        event.type ===
          "raft.heartbeat.accepted" &&
        event.node_id === peerId &&
        from === leaderId
      ) {
        accepted += 1;
        lastAcceptedAt = timestamp;
      }
    }

    const lastSeenAt =
      lastAcceptedAt ??
      lastReceivedAt ??
      lastSentAt;

    const ageMs =
      lastSeenAt === undefined
        ? undefined
        : Math.max(0, now - lastSeenAt);

    let status:
      | "healthy"
      | "delayed"
      | "lost"
      | "unknown" = "unknown";

    if (ageMs !== undefined) {
      if (ageMs < 800) {
        status = "healthy";
      } else if (ageMs < 2500) {
        status = "delayed";
      } else {
        status = "lost";
      }
    }

    return {
      nodeId: peerId,
      leaderId,
      lastSentAt,
      lastReceivedAt,
      lastAcceptedAt,
      lastSeenAt,
      ageMs,
      sent,
      received,
      accepted,
      deliveryPercent:
        sent === 0
          ? 0
          : Math.min(
              100,
              (accepted / sent) * 100,
            ),
      status,
      pulses: pulses.slice(-80),
    };
  });
}

export function formatAge(
  value?: number,
): string {
  if (value === undefined) {
    return "—";
  }

  if (value < 1000) {
    return `${Math.round(value)} ms`;
  }

  return `${(value / 1000).toFixed(1)} s`;
}

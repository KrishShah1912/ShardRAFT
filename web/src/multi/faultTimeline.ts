import {
  eventIdentity,
  type LiveEvent,
} from "./liveTelemetry";

import {
  isElectionEvent,
  electionEventLabel,
} from "./electionTelemetry";

import {
  commitObservation,
  replicationObservation,
} from "./replicationTelemetry";

export type FaultTimelineSource =
  | "telemetry"
  | "docker"
  | "polling";

export type FaultTimelineKind =
  | "election"
  | "leadership"
  | "commit"
  | "replication"
  | "control"
  | "recovery";

export type FaultTimelineTone =
  | "info"
  | "success"
  | "warning"
  | "danger";

export interface FaultTimelineEntry {
  id: string;
  timestamp: string;
  source: FaultTimelineSource;
  kind: FaultTimelineKind;
  tone: FaultTimelineTone;
  title: string;
  detail: string;
  shardId: number | null;
  nodeId: number | null;
  term: number | null;
}

export interface FaultActionObservation {
  id: string;
  node: number;
  action: "start" | "stop" | "restart";
  startedAt: string;
  completedAt: string;
  success: boolean;
  durationMs: number;
}

export interface FaultLeaderObservation {
  id: string;
  timestamp: string;
  group: string;
  previous: number | null;
  current: number | null;
}

export interface FaultReachabilityObservation {
  id: string;
  timestamp: string;
  nodeId: number;
  reachable: boolean;
}

export interface FaultTimelineInput {
  electionEvents: readonly LiveEvent[];
  replicationEvents: readonly LiveEvent[];
  actions: readonly FaultActionObservation[];
  leadership: readonly FaultLeaderObservation[];
  reachability?: readonly FaultReachabilityObservation[];
}

export interface FaultTimelineFilter {
  shardId?: number;
  nodeId?: number;
  source?: FaultTimelineSource;
  kind?: FaultTimelineKind;
  since?: number;
  until?: number;
  limit?: number;
}

export const FAULT_TIMELINE_LIMIT = 200;

function validTimestamp(value: string): boolean {
  return Number.isFinite(Date.parse(value));
}

function nodeLabel(value: number | null): string {
  return value === null
    ? "Unknown"
    : `Node ${value}`;
}

function field(value: unknown): string {
  return typeof value === "number" ||
    typeof value === "string"
    ? String(value)
    : "Unknown";
}

function electionEntry(
  event: LiveEvent,
): FaultTimelineEntry | null {
  if (!isElectionEvent(event)) return null;

  let detail = `Node ${event.node_id}`;
  let tone: FaultTimelineTone = "info";

  switch (event.type) {
    case "raft.election.started":
      detail += " started an election";
      tone = "warning";
      break;

    case "raft.vote.requested":
      detail += ` requested vote from Node ${field(event.data.to)}`;
      break;

    case "raft.vote.granted":
      detail += ` granted vote to Node ${field(event.data.candidate_id)}`;
      tone = "success";
      break;

    case "raft.vote.rejected":
      detail += ` rejected vote for Node ${field(event.data.candidate_id)}`;
      tone = "warning";
      break;

    case "raft.leader.elected":
      detail += " elected leader";
      tone = "success";
      break;

    case "raft.term.changed":
      detail += `: term ${field(event.data.previous_term)} → ${field(event.term)}`;
      break;

    case "raft.role.changed":
      detail += `: ${field(event.data.previous_role)} → ${field(event.data.role)}`;
      break;
  }

  return {
    id: `sse:${eventIdentity(event)}`,
    timestamp: event.timestamp,
    source: "telemetry",
    kind: event.type === "raft.leader.elected"
      ? "leadership"
      : "election",
    tone,
    title: electionEventLabel(event),
    detail,
    shardId: event.shard_id,
    nodeId: event.node_id,
    term: event.term ?? null,
  };
}

function replicationEntry(
  event: LiveEvent,
): FaultTimelineEntry | null {
  const commit = commitObservation(event);

  if (commit) {
    return {
      id: `sse:${eventIdentity(event)}`,
      timestamp: commit.timestamp,
      source: "telemetry",
      kind: "commit",
      tone: "success",
      title: "Commit index advanced",
      detail:
        `Node ${commit.nodeId}: ` +
        `${commit.previousCommit} → ${commit.commitIndex}` +
        ` (${commit.source})`,
      shardId: commit.shardId,
      nodeId: commit.nodeId,
      term: commit.term,
    };
  }

  const observation = replicationObservation(event);

  if (!observation) return null;

  if (
    observation.stage !== "acknowledged" &&
    observation.stage !== "rejected"
  ) {
    return null;
  }

  const rejected = observation.stage === "rejected";

  return {
    id: `sse:${eventIdentity(event)}`,
    timestamp: observation.timestamp,
    source: "telemetry",
    kind: "replication",
    tone: rejected ? "warning" : "info",
    title: rejected
      ? "Replication rejected"
      : "Replication progress observed",
    detail:
      `Leader ${observation.leaderId}, ` +
      `follower ${observation.followerId}, ` +
      (rejected
        ? `reason: ${observation.reason ?? "Unknown"}`
        : `match index: ${observation.lastIndex ?? "Unknown"}`),
    shardId: observation.shardId,
    nodeId: observation.followerId,
    term: observation.term,
  };
}

function actionEntries(
  action: FaultActionObservation,
): FaultTimelineEntry[] {
  const label = action.action.toUpperCase();

  return [
    {
      id: `docker:${action.id}:started`,
      timestamp: action.startedAt,
      source: "docker",
      kind: "control",
      tone: "info",
      title: `Docker ${label} requested`,
      detail: `Node ${action.node}`,
      shardId: null,
      nodeId: action.node,
      term: null,
    },
    {
      id: `docker:${action.id}:completed`,
      timestamp: action.completedAt,
      source: "docker",
      kind: "control",
      tone: action.success ? "success" : "warning",
      title: action.success
        ? `Docker ${label} confirmed`
        : `Docker ${label} unconfirmed`,
      detail:
        `Node ${action.node} · ` +
        `${Math.round(action.durationMs)} ms API duration`,
      shardId: null,
      nodeId: action.node,
      term: null,
    },
  ];
}

function leadershipEntry(
  change: FaultLeaderObservation,
): FaultTimelineEntry {
  const match = /^Shard ([1-4])$/.exec(change.group);

  return {
    id: `poll:leader:${change.id}`,
    timestamp: change.timestamp,
    source: "polling",
    kind: "leadership",
    tone: change.current === null
      ? "warning"
      : "success",
    title: change.current === null
      ? "Leader no longer observed"
      : "Leader change observed",
    detail:
      `${change.group}: ` +
      `${nodeLabel(change.previous)} → ` +
      `${nodeLabel(change.current)}`,
    shardId: match
      ? Number(match[1])
      : change.group === "Metadata group"
        ? 5
        : null,
    nodeId: change.current ?? change.previous,
    term: null,
  };
}

function reachabilityEntry(
  observation: FaultReachabilityObservation,
): FaultTimelineEntry {
  return {
    id: `poll:reachability:${observation.id}`,
    timestamp: observation.timestamp,
    source: "polling",
    kind: "recovery",
    tone: observation.reachable
      ? "success"
      : "warning",
    title: observation.reachable
      ? "Node reachable again"
      : "Node no longer reachable",
    detail:
      `Node ${observation.nodeId} · ` +
      "HTTP reachability observation",
    shardId: null,
    nodeId: observation.nodeId,
    term: null,
  };
}

export function buildFaultTimeline(
  input: FaultTimelineInput,
  filter: FaultTimelineFilter = {},
): FaultTimelineEntry[] {
  const entries = new Map<string, FaultTimelineEntry>();

  function add(entry: FaultTimelineEntry | null) {
    if (!entry || !validTimestamp(entry.timestamp)) {
      return;
    }

    entries.set(entry.id, entry);
  }

  for (const event of input.electionEvents) {
    add(electionEntry(event));
  }

  for (const event of input.replicationEvents) {
    add(replicationEntry(event));
  }

  for (const action of input.actions) {
    for (const entry of actionEntries(action)) {
      add(entry);
    }
  }

  for (const change of input.leadership) {
    add(leadershipEntry(change));
  }

  for (const observation of input.reachability ?? []) {
    add(reachabilityEntry(observation));
  }

  const limit = Number.isSafeInteger(filter.limit) &&
    (filter.limit ?? 0) > 0
    ? Math.min(filter.limit!, 1000)
    : FAULT_TIMELINE_LIMIT;

  return [...entries.values()]
    .filter(entry => {
      const timestamp = Date.parse(entry.timestamp);

      return (
        (filter.shardId === undefined ||
          entry.shardId === filter.shardId) &&
        (filter.nodeId === undefined ||
          entry.nodeId === filter.nodeId) &&
        (filter.source === undefined ||
          entry.source === filter.source) &&
        (filter.kind === undefined ||
          entry.kind === filter.kind) &&
        (filter.since === undefined ||
          timestamp >= filter.since) &&
        (filter.until === undefined ||
          timestamp <= filter.until)
      );
    })
    .sort((a, b) =>
      Date.parse(b.timestamp) -
      Date.parse(a.timestamp) ||
      a.id.localeCompare(b.id),
    )
    .slice(0, limit);
}

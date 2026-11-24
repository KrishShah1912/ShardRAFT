import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

export type FaultExperimentStatus =
  | "idle"
  | "running"
  | "recovering"
  | "completed"
  | "failed";

export interface FaultTimelineEvent {
  id: string;
  timestamp: number;
  type:
    | "experiment"
    | "node"
    | "election"
    | "leader"
    | "term"
    | "transport"
    | "recovery"
    | "commit";
  title: string;
  detail: string;
  nodeId?: number;
}

export interface FaultExperiment {
  id: string;
  name: string;
  targetNodeId: number;
  previousLeaderId?: number;
  newLeaderId?: number;
  startedAt: number;
  leaderLostAt?: number;
  newLeaderAt?: number;
  recoveredAt?: number;
  startingTerm: number;
  endingTerm?: number;
  startingCommit: number;
  endingCommit?: number;
  startingRetries: number;
  startingFailures: number;
  endingRetries?: number;
  endingFailures?: number;
  status: FaultExperimentStatus;
}

export function currentLeader(
  nodes: NodeStatus[],
): NodeStatus | undefined {
  return nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );
}

export function reachableCount(
  nodes: NodeStatus[],
): number {
  return nodes.filter(
    (node) => node.reachable !== false,
  ).length;
}

export function hasQuorum(
  nodes: NodeStatus[],
): boolean {
  return reachableCount(nodes) >= 2;
}

export function startFaultExperiment(
  name: string,
  targetNodeId: number,
  nodes: NodeStatus[],
  transport?: TransportStats,
): FaultExperiment {
  const leader = currentLeader(nodes);

  return {
    id: `${Date.now()}-${targetNodeId}`,
    name,
    targetNodeId,
    previousLeaderId: leader?.id,
    startedAt: Date.now(),
    startingTerm:
      leader?.term ??
      Math.max(
        0,
        ...nodes.map(
          (node) => node.term,
        ),
      ),
    startingCommit:
      leader?.commit_index ?? 0,
    startingRetries:
      transport?.retries ?? 0,
    startingFailures:
      transport?.failures ?? 0,
    status: "running",
  };
}

export function experimentDuration(
  experiment?: FaultExperiment,
): number | undefined {
  if (!experiment) {
    return undefined;
  }

  const end =
    experiment.recoveredAt ??
    experiment.newLeaderAt ??
    Date.now();

  return Math.max(
    0,
    end - experiment.startedAt,
  );
}

export function failoverDuration(
  experiment?: FaultExperiment,
): number | undefined {
  if (
    !experiment?.leaderLostAt ||
    !experiment.newLeaderAt
  ) {
    return undefined;
  }

  return Math.max(
    0,
    experiment.newLeaderAt -
      experiment.leaderLostAt,
  );
}

export function faultRelevantEvent(
  event: RaftEvent,
): boolean {
  return (
    event.type ===
      "raft.election.started" ||
    event.type ===
      "raft.term.changed" ||
    event.type ===
      "raft.role.changed" ||
    event.type ===
      "raft.vote.requested" ||
    event.type ===
      "raft.vote.granted" ||
    event.type ===
      "raft.vote.rejected" ||
    event.type ===
      "raft.leader.elected" ||
    event.type ===
      "raft.commit.advanced" ||
    event.type ===
      "transport.retry" ||
    event.type ===
      "transport.failure"
  );
}

export function eventToTimeline(
  event: RaftEvent,
): FaultTimelineEvent {
  let type:
    FaultTimelineEvent["type"] =
      "experiment";

  let title = humanize(
    event.type,
  );

  if (
    event.type.includes("election") ||
    event.type.includes("vote")
  ) {
    type = "election";
  } else if (
    event.type.includes("leader")
  ) {
    type = "leader";
  } else if (
    event.type.includes("term")
  ) {
    type = "term";
  } else if (
    event.type.includes("transport")
  ) {
    type = "transport";
  } else if (
    event.type.includes("commit")
  ) {
    type = "commit";
  }

  return {
    id: `event-${event.node_id}-${event.id}`,
    timestamp: new Date(
      event.timestamp,
    ).getTime(),
    type,
    title,
    detail: describeEvent(event),
    nodeId: event.node_id,
  };
}

function describeEvent(
  event: RaftEvent,
): string {
  const data = event.data ?? {};

  switch (event.type) {
    case "raft.election.started":
      return `Node ${event.node_id} started an election for term ${event.term ?? "—"}.`;

    case "raft.vote.requested":
      return `Node ${event.node_id} requested a vote from Node ${String(
        data.to ?? "—",
      )}.`;

    case "raft.vote.granted":
      return `Node ${event.node_id} granted a vote to Node ${String(
        data.to ??
          data.candidate_id ??
          "—",
      )}.`;

    case "raft.leader.elected":
      return `Node ${event.node_id} became leader for term ${event.term ?? "—"}.`;

    case "raft.term.changed":
      return `Node ${event.node_id} advanced to term ${event.term ?? "—"}.`;

    case "raft.commit.advanced":
      return `Commit index advanced to ${String(
        data.commit_index ?? "—",
      )}.`;

    case "transport.retry":
      return `Transport to Node ${String(
        data.peer_id ?? "—",
      )} will retry after ${String(
        data.delay_ms ?? "—",
      )} ms.`;

    case "transport.failure":
      return `Transport to Node ${String(
        data.peer_id ?? "—",
      )} exhausted delivery attempts.`;

    default:
      return `Node ${event.node_id} emitted ${event.type}.`;
  }
}

function humanize(
  value: string,
): string {
  return value
    .replace(/^raft\./, "")
    .replace(/^transport\./, "")
    .split(".")
    .map(
      (part) =>
        part.charAt(0).toUpperCase() +
        part.slice(1),
    )
    .join(" ");
}

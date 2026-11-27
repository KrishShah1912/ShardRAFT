import {
  mergeLiveEvents,
  type LiveEvent,
} from "./liveTelemetry";

export const ELECTION_TYPES = [
  "raft.role.changed",
  "raft.term.changed",
  "raft.election.started",
  "raft.vote.requested",
  "raft.vote.granted",
  "raft.vote.rejected",
  "raft.leader.elected",
] as const;

const TYPES = new Set<string>(ELECTION_TYPES);

export function isElectionEvent(
  event: LiveEvent,
): boolean {
  return TYPES.has(event.type);
}

export function mergeElectionEvents(
  previous: LiveEvent[],
  incoming: LiveEvent[],
  limit = 1000,
): LiveEvent[] {
  const relevant = incoming.filter(isElectionEvent);

  if (relevant.length === 0) {
    return previous;
  }

  return mergeLiveEvents(
    previous,
    relevant,
    limit,
  );
}

export function electionEventLabel(
  event: LiveEvent,
): string {
  switch (event.type) {
    case "raft.election.started":
      return "Election started";

    case "raft.vote.requested":
      return "Vote requested";

    case "raft.vote.granted":
      return "Vote granted";

    case "raft.vote.rejected":
      return "Vote rejected";

    case "raft.leader.elected":
      return "Leader elected";

    case "raft.term.changed":
      return "Term changed";

    case "raft.role.changed":
      return "Role changed";

    default:
      return event.type;
  }
}

export function electionEventColor(
  event: LiveEvent,
): string {
  switch (event.type) {
    case "raft.leader.elected":
    case "raft.vote.granted":
      return "#34d399";

    case "raft.vote.rejected":
      return "#f87171";

    case "raft.vote.requested":
      return "#60a5fa";

    default:
      return "#fbbf24";
  }
}

export function electionTerms(
  events: LiveEvent[],
  shardId: number,
): number[] {
  return [...new Set(
    events
      .filter(event =>
        isElectionEvent(event) &&
        event.shard_id === shardId &&
        typeof event.term === "number",
      )
      .map(event => event.term as number),
  )].sort((a, b) => b - a);
}

export function electionTimeline(
  events: LiveEvent[],
  shardId: number,
  term?: number,
): LiveEvent[] {
  return events
    .filter(event =>
      isElectionEvent(event) &&
      event.shard_id === shardId &&
      (term === undefined || event.term === term),
    )
    .sort((a, b) =>
      Date.parse(a.timestamp) -
      Date.parse(b.timestamp) ||
      a.timestamp.localeCompare(b.timestamp) ||
      a.node_id - b.node_id ||
      a.id - b.id,
    );
}

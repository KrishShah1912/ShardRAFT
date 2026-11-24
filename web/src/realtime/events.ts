import type { RaftEvent } from "../types";

export function connectEventStream(
  nodeId: number,
  onEvent: (event: RaftEvent) => void,
  onState: (connected: boolean) => void,
): () => void {
  const stream = new EventSource(
    `/node${nodeId}/api/v1/events`,
  );

  stream.onopen = () => {
    onState(true);
  };

  stream.onerror = () => {
    onState(false);
  };

  const eventNames = [
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
  ];

  for (const name of eventNames) {
    stream.addEventListener(name, (message) => {
      try {
        const event = JSON.parse(
          (message as MessageEvent).data,
        ) as RaftEvent;

        onEvent(event);
      } catch {
        // Ignore malformed observability events.
      }
    });
  }

  return () => {
    stream.close();
    onState(false);
  };
}

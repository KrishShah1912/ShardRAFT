import {
  ArrowDown,
  CheckCircle2,
  GitBranch,
  Radio,
  Server,
} from "lucide-react";

import type {
  NodeStatus,
  RaftDebugState,
  RaftEvent,
} from "../types";

interface Props {
  nodes: NodeStatus[];
  raft?: RaftDebugState;
  events: RaftEvent[];
}

export function Replication({ nodes, raft, events }: Props) {
  const leader = nodes.find(
    (node) => node.role === 2 && node.reachable !== false,
  );

  const recentReplication = events
    .filter(
      (event) =>
        event.type === "raft.append.sent" ||
        event.type === "raft.append.received" ||
        event.type === "raft.append.accepted" ||
        event.type === "raft.append.rejected" ||
        event.type === "raft.replication.progress" ||
        event.type === "raft.commit.advanced",
    )
    .slice(0, 10);

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">CONSENSUS</div>
          <h1>Replication</h1>
          <p>
            Live follower progress, AppendEntries and quorum state.
          </p>
        </div>

        <div className="live-indicator connected">
          <Radio size={14} />
          LIVE
        </div>
      </header>

      <section className="replication-hero">
        <div className="replication-node leader-node">
          <Server size={19} />
          <span>NODE {leader?.id ?? "—"}</span>
          <strong>LEADER</strong>
          <small>
            Term {leader?.term ?? 0} · Commit{" "}
            {leader?.commit_index ?? 0}
          </small>
        </div>

        <div className="replication-arrow">
          <ArrowDown size={20} />
          <span>AppendEntries</span>
        </div>

        <div className="replication-followers">
          {(raft?.peers ?? []).map((peer) => {
            const lag = Math.max(
              0,
              (raft?.last_log_index ?? 0) - peer.match_index,
            );

            const progress =
              (raft?.last_log_index ?? 0) === 0
                ? 100
                : Math.min(
                    100,
                    (peer.match_index /
                      (raft?.last_log_index ?? 1)) *
                      100,
                  );

            return (
              <div
                className="replication-peer"
                key={peer.node_id}
              >
                <div className="replication-peer-title">
                  <div>
                    <span>NODE {peer.node_id}</span>
                    <strong>FOLLOWER</strong>
                  </div>

                  <CheckCircle2 size={17} />
                </div>

                <div className="progress-track">
                  <div
                    className="progress-value"
                    style={{ width: `${progress}%` }}
                  />
                </div>

                <div className="replication-stats">
                  <div>
                    <span>MATCH</span>
                    <strong>{peer.match_index}</strong>
                  </div>
                  <div>
                    <span>NEXT</span>
                    <strong>{peer.next_index}</strong>
                  </div>
                  <div>
                    <span>LAG</span>
                    <strong>{lag}</strong>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      <section className="two-column">
        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Replication state</h2>
              <p>Leader-side follower tracking</p>
            </div>
            <GitBranch size={17} />
          </div>

          <div className="consensus-list">
            <Row
              label="Last log index"
              value={String(raft?.last_log_index ?? "—")}
            />
            <Row
              label="Commit index"
              value={String(raft?.commit_index ?? "—")}
            />
            <Row
              label="Applied index"
              value={String(raft?.applied_index ?? "—")}
            />
            <Row
              label="Snapshot boundary"
              value={String(raft?.snapshot_index ?? "—")}
            />
          </div>
        </div>

        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Recent protocol activity</h2>
              <p>Meaningful replication transitions</p>
            </div>
          </div>

          <div className="event-list">
            {recentReplication.length === 0 && (
              <div className="empty-state">
                Waiting for replication activity…
              </div>
            )}

            {recentReplication.map((event) => (
              <div
                className="event-row"
                key={`${event.node_id}-${event.id}`}
              >
                <div className="event-dot" />
                <div className="event-copy">
                  <strong>{event.type}</strong>
                  <span>
                    Node {event.node_id} · Request{" "}
                    {event.data?.request_id
                      ? `#${String(event.data.request_id)}`
                      : "—"}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </>
  );
}

function Row({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="consensus-row">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

import {
  Activity,
  CheckCircle2,
  Crown,
  Layers3,
  Radio,
} from "lucide-react";
import { NodeCard } from "../components/NodeCard";
import {
  formatEventName,
  relativeTime,
} from "../lib/format";
import type { NodeStatus, RaftEvent } from "../types";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  connected: boolean;
}

export function Overview({
  nodes,
  events,
  connected,
}: Props) {
  const reachable = nodes.filter(
    (node) => node.reachable !== false,
  );

  const leader = nodes.find(
    (node) => node.role === 2 && node.reachable !== false,
  );

  const highestTerm = Math.max(
    0,
    ...nodes.map((node) => node.term),
  );

  const commit = leader?.commit_index ?? 0;

  const healthy =
    reachable.length === 3 &&
    nodes.filter((node) => node.role === 2).length === 1;

  const meaningfulEvents = events
    .filter((event) => {
      if (event.type !== "raft.append.sent") {
        return true;
      }

      return Number(event.data?.entries ?? 0) > 0;
    })
    .slice(0, 8);

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">SHARDRAFT CONSOLE</div>
          <h1>Cluster Overview</h1>
          <p>
            Real-time consensus, replication and database
            observability.
          </p>
        </div>

        <div
          className={
            connected
              ? "live-indicator connected"
              : "live-indicator"
          }
        >
          <Radio size={14} />
          {connected ? "LIVE" : "RECONNECTING"}
        </div>
      </header>

      <section className="health-banner">
        <div
          className={`health-icon ${
            healthy ? "healthy" : "warning"
          }`}
        >
          <CheckCircle2 size={22} />
        </div>

        <div>
          <strong>
            {healthy
              ? "Cluster operating normally"
              : "Cluster state requires attention"}
          </strong>
          <span>
            {reachable.length} of 3 nodes reachable
            {leader
              ? ` · Node ${leader.id} is leader`
              : " · No leader observed"}
          </span>
        </div>
      </section>

      <section className="metrics-grid">
        <Metric
          icon={Activity}
          label="CLUSTER"
          value={`${reachable.length} / 3`}
          detail={healthy ? "Healthy" : "Degraded"}
        />

        <Metric
          icon={Crown}
          label="LEADER"
          value={leader ? `Node ${leader.id}` : "—"}
          detail={
            leader
              ? `Term ${leader.term}`
              : "Election pending"
          }
        />

        <Metric
          icon={Layers3}
          label="CURRENT TERM"
          value={String(highestTerm)}
          detail="Raft epoch"
        />

        <Metric
          icon={CheckCircle2}
          label="COMMIT INDEX"
          value={String(commit)}
          detail={
            leader?.applied_index === commit
              ? "Fully applied"
              : `Applied ${leader?.applied_index ?? 0}`
          }
        />
      </section>

      <section className="section-block">
        <div className="section-heading">
          <div>
            <h2>Cluster topology</h2>
            <p>
              Live consensus state from all ShardRAFT nodes.
            </p>
          </div>

          <span className="section-chip">RAFT GROUP 01</span>
        </div>

        <div className="topology">
          {leader && (
            <div className="leader-row">
              <NodeCard node={leader} />
            </div>
          )}

          {leader && (
            <div className="topology-lines">
              <span />
              <span className="replication-label">
                AppendEntries
              </span>
              <span />
            </div>
          )}

          <div className="followers-row">
            {nodes
              .filter((node) => node.id !== leader?.id)
              .map((node) => (
                <NodeCard key={node.id} node={node} />
              ))}
          </div>
        </div>
      </section>

      <section className="two-column">
        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Replication activity</h2>
              <p>Meaningful Raft transitions</p>
            </div>
            <Activity size={17} />
          </div>

          <div className="event-list">
            {meaningfulEvents.length === 0 && (
              <div className="empty-state">
                Waiting for replication activity…
              </div>
            )}

            {meaningfulEvents.map((event) => (
              <div className="event-row" key={event.id}>
                <div className="event-dot" />
                <div className="event-copy">
                  <strong>
                    {formatEventName(event.type)}
                  </strong>
                  <span>
                    Node {event.node_id} · Term{" "}
                    {event.term ?? "—"}
                  </span>
                </div>
                <time>{relativeTime(event.timestamp)}</time>
              </div>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Consensus</h2>
              <p>Current replicated state</p>
            </div>
            <Crown size={17} />
          </div>

          <div className="consensus-list">
            <ConsensusRow
              label="Leader"
              value={leader ? `Node ${leader.id}` : "None"}
            />
            <ConsensusRow
              label="Term"
              value={String(highestTerm)}
            />
            <ConsensusRow
              label="Commit"
              value={String(commit)}
            />
            <ConsensusRow
              label="Applied"
              value={String(leader?.applied_index ?? 0)}
            />
            <ConsensusRow
              label="Quorum"
              value={
                reachable.length >= 2
                  ? "Available"
                  : "Unavailable"
              }
            />
          </div>
        </div>
      </section>
    </>
  );
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="metric-card">
      <div className="metric-icon">
        <Icon size={18} />
      </div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function ConsensusRow({
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

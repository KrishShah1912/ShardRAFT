import {
  Activity,
  CheckCircle2,
  Crown,
  Layers3,
  Radio,
} from "lucide-react";
import { useState } from "react";

import type { ControlledNode } from "../api/control";
import { TopologyNode } from "../components/TopologyNode";
import {
  formatEventName,
  relativeTime,
} from "../lib/format";
import type { NodeStatus, RaftEvent } from "../types";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  connected: boolean;
  controlledNodes: ControlledNode[];

  onStart: (id: number) => Promise<void>;
  onStop: (id: number) => Promise<void>;
  onRestart: (id: number) => Promise<void>;
}

export function Overview({
  nodes,
  events,
  connected,
  controlledNodes,
  onStart,
  onStop,
  onRestart,
}: Props) {
  const [busyNode, setBusyNode] = useState<number>();
  const [confirmLeader, setConfirmLeader] =
    useState<number>();

  const reachable = nodes.filter(
    (node) => node.reachable !== false,
  );

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const highestTerm = Math.max(
    0,
    ...nodes.map((node) => node.term),
  );

  const commit = leader?.commit_index ?? 0;

  const healthy =
    reachable.length === 3 &&
    nodes.filter(
      (node) =>
        node.role === 2 &&
        node.reachable !== false,
    ).length === 1;

  const meaningfulEvents = events
    .filter(
      (event) =>
        !event.type.startsWith("raft.heartbeat."),
    )
    .slice(0, 8);

  function controlled(id: number) {
    return controlledNodes.find(
      (node) => node.id === id,
    );
  }

  async function execute(
    id: number,
    action: () => Promise<void>,
  ) {
    setBusyNode(id);

    try {
      await action();
    } finally {
      setBusyNode(undefined);
      setConfirmLeader(undefined);
    }
  }

  function stopNode(node: NodeStatus) {
    const isLeader =
      node.role === 2 &&
      node.reachable !== false;

    if (isLeader) {
      setConfirmLeader(node.id);
      return;
    }

    void execute(
      node.id,
      () => onStop(node.id),
    );
  }

  function renderTopologyNode(node: NodeStatus) {
    return (
      <TopologyNode
        key={node.id}
        node={node}
        control={controlled(node.id)}
        busy={busyNode === node.id}
        onStart={() =>
          void execute(
            node.id,
            () => onStart(node.id),
          )
        }
        onRestart={() =>
          void execute(
            node.id,
            () => onRestart(node.id),
          )
        }
        onStop={() => stopNode(node)}
      />
    );
  }

  const followers = nodes.filter(
    (node) => node.id !== leader?.id,
  );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            SHARDRAFT CONSOLE
          </div>

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
          value={
            leader
              ? `Node ${leader.id}`
              : "—"
          }
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
              : `Applied ${
                  leader?.applied_index ?? 0
                }`
          }
        />
      </section>

      <section className="section-block">
        <div className="section-heading">
          <div>
            <h2>Live cluster topology</h2>

            <p>
              Control nodes and observe consensus changes
              directly from the browser.
            </p>
          </div>

          <div className="topology-heading-actions">
            <span className="section-chip">
              RAFT GROUP 01
            </span>

            <span className="section-chip control-chip">
              CONTROL ENABLED
            </span>
          </div>
        </div>

        <div className="controlled-topology">
          {leader ? (
            <div className="controlled-leader-row">
              {renderTopologyNode(leader)}
            </div>
          ) : (
            <div className="election-state">
              <div className="election-pulse" />
              <strong>Leader election in progress</strong>
              <span>
                Waiting for a node to reach quorum.
              </span>
            </div>
          )}

          {leader && (
            <div className="controlled-topology-links">
              <div className="topology-link left">
                <span className="link-line" />
                <span className="link-packet" />
              </div>

              <div className="topology-link-label">
                AppendEntries / Heartbeat
              </div>

              <div className="topology-link right">
                <span className="link-line" />
                <span className="link-packet" />
              </div>
            </div>
          )}

          <div className="controlled-followers-row">
            {followers.map(renderTopologyNode)}
          </div>
        </div>

        {confirmLeader !== undefined && (
          <div className="overview-modal-backdrop">
            <div className="overview-confirm-modal">
              <div className="leader-warning-icon">
                <Crown size={21} />
              </div>

              <div>
                <span className="eyebrow">
                  RAFT FAILURE EXPERIMENT
                </span>

                <h3>
                  Stop Node {confirmLeader}?
                </h3>

                <p>
                  Node {confirmLeader} is the current
                  Raft leader. Stopping it will break its
                  heartbeat stream and force the remaining
                  nodes to elect a replacement leader.
                </p>
              </div>

              <div className="overview-confirm-details">
                <div>
                  <span>EXPECTED QUORUM</span>
                  <strong>2 / 3</strong>
                </div>

                <div>
                  <span>EXPECTED RESULT</span>
                  <strong>Automatic failover</strong>
                </div>
              </div>

              <div className="overview-confirm-actions">
                <button
                  onClick={() =>
                    setConfirmLeader(undefined)
                  }
                >
                  Cancel
                </button>

                <button
                  className="danger-button"
                  disabled={
                    busyNode === confirmLeader
                  }
                  onClick={() =>
                    void execute(
                      confirmLeader,
                      () =>
                        onStop(confirmLeader),
                    )
                  }
                >
                  Stop leader
                </button>
              </div>
            </div>
          </div>
        )}
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
              <div
                className="event-row"
                key={`${event.node_id}-${event.id}`}
              >
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

                <time>
                  {relativeTime(event.timestamp)}
                </time>
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
              value={
                leader
                  ? `Node ${leader.id}`
                  : "None"
              }
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
              value={String(
                leader?.applied_index ?? 0,
              )}
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
      <div className="metric-card-top">
        <span>{label}</span>
        <Icon size={18} />
      </div>

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

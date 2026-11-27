import { useState } from "react";

import {
  ChevronDown,
  ChevronUp,
  Server,
  ShieldCheck,
} from "lucide-react";

import {
  Panel,
  StatusBadge,
  DataTable,
  EmptyState,
} from "../design-system";

import type {
  Tone,
} from "../design-system";

import type {
  ReplicaState,
  ShardState,
} from "./clusterState";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import "./multiOverviewShardHealth.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const SHARD_COLORS: Record<number, string> = {
  1: "#60a5fa",
  2: "#2dd4bf",
  3: "#a78bfa",
  4: "#eab308",
};

const STATE_TONES: Record<
  ShardState["state"],
  Tone
> = {
  healthy: "success",
  degraded: "warning",
  electing: "warning",
  unavailable: "danger",
};

function formatIndex(value: number | null) {
  return value === null
    ? "—"
    : value.toLocaleString();
}

function replicaTone(
  replica: ReplicaState,
): Tone {
  if (!replica.reachable) return "danger";
  if (replica.role === "leader") return "success";
  if (replica.role === "candidate") return "warning";
  return "primary";
}

function ShardHealthCard({
  shard,
}: {
  shard: ShardState;
}) {
  const [expanded, setExpanded] = useState(false);

  const leader = shard.replicas.find(
    replica => replica.nodeId === shard.leaderId,
  );

  const color = SHARD_COLORS[shard.shardId];

  return (
    <article className="sr-shard-health-card">
      <header className="sr-shard-health-heading">
        <div
          className="sr-shard-health-name"
          style={{ color }}
        >
          <Server size={15} />
          Shard {shard.shardId}
        </div>

        <StatusBadge tone={STATE_TONES[shard.state]}>
          {shard.state}
        </StatusBadge>
      </header>

      <div className="sr-shard-replica-grid">
        {shard.replicas.map(replica => (
          <div
            key={replica.nodeId}
            className="sr-shard-replica"
            data-tone={replicaTone(replica)}
          >
            <span className="sr-shard-replica-role">
              {replica.reachable
                ? replica.role
                : "Unreachable"}
            </span>

            <strong>Node {replica.nodeId}</strong>

            <div
              className="sr-shard-replica-indicator"
              aria-hidden="true"
            />

            <span className="sr-shard-replica-term">
              Term {formatIndex(replica.term)}
            </span>

            <div className="sr-shard-replica-index">
              <span>Commit</span>
              <strong>
                {formatIndex(replica.commitIndex)}
              </strong>
            </div>

            <div className="sr-shard-replica-index">
              <span>Applied</span>
              <strong>
                {formatIndex(replica.appliedIndex)}
              </strong>
            </div>
          </div>
        ))}
      </div>

      <div className="sr-shard-health-summary">
        <div>
          <span>Observed leader</span>
          <strong>
            {leader
              ? `Node ${leader.nodeId}`
              : "Unknown"}
          </strong>
        </div>

        <div>
          <span>Reachable replicas</span>
          <strong>
            {shard.reachableReplicas}/3
          </strong>
        </div>

        <div>
          <span>Commit spread</span>
          <strong
            data-tone={
              shard.replicationLag === null
                ? "neutral"
                : shard.replicationLag === 0
                  ? "success"
                  : "warning"
            }
          >
            {formatIndex(shard.replicationLag)}
          </strong>
        </div>

        <div>
          <span>Majority reachable</span>
          <strong
            data-tone={
              shard.majorityReachable
                ? "success"
                : "danger"
            }
          >
            {shard.majorityReachable
              ? "Yes"
              : "No"}
          </strong>
        </div>
      </div>

      <button
        type="button"
        className="sr-button sr-shard-expand"
        aria-expanded={expanded}
        aria-controls={
          `sr-shard-details-${shard.shardId}`
        }
        onClick={() => setExpanded(value => !value)}
      >
        {expanded
          ? <ChevronUp size={14} />
          : <ChevronDown size={14} />}

        {expanded
          ? "Hide replica details"
          : "Inspect replica details"}
      </button>

      <div
        id={`sr-shard-details-${shard.shardId}`}
        hidden={!expanded}
      >
        {expanded && (
          <DataTable<ReplicaState>
            label={`Shard ${shard.shardId} replica details`}
            rows={shard.replicas}
            rowKey={replica => replica.nodeId}
            columns={[
              {
                key: "node",
                header: "Node",
                render: replica => `N${replica.nodeId}`,
              },
              {
                key: "role",
                header: "Role",
                render: replica => (
                  <StatusBadge
                    tone={replicaTone(replica)}
                  >
                    {replica.reachable
                      ? replica.role
                      : "Unreachable"}
                  </StatusBadge>
                ),
              },
              {
                key: "term",
                header: "Term",
                numeric: true,
                render: replica =>
                  formatIndex(replica.term),
              },
              {
                key: "commit",
                header: "Commit",
                numeric: true,
                render: replica =>
                  formatIndex(replica.commitIndex),
              },
              {
                key: "applied",
                header: "Applied",
                numeric: true,
                render: replica =>
                  formatIndex(replica.appliedIndex),
              },
              {
                key: "backlog",
                header: "Apply backlog",
                numeric: true,
                render: replica =>
                  replica.commitIndex === null ||
                  replica.appliedIndex === null
                    ? "—"
                    : formatIndex(
                        replica.commitIndex -
                        replica.appliedIndex,
                      ),
              },
            ]}
          />
        )}
      </div>
    </article>
  );
}

export function MultiOverviewShardHealth({
  cluster,
}: Props) {
  const awaiting =
    cluster.loading || !cluster.lastUpdated;

  const majorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  return (
    <Panel
      className="sr-overview-shard-health"
      title={
        <span className="sr-shard-panel-title">
          <ShieldCheck size={18} />
          Shard Health
        </span>
      }
      description="Observed replica roles, terms and log progress across all four data shards."
      actions={
        <StatusBadge
          tone={
            awaiting
              ? "neutral"
              : majorities === 4
                ? "success"
                : "warning"
          }
        >
          {awaiting
            ? "Awaiting observations"
            : `${majorities}/4 majorities reachable`}
        </StatusBadge>
      }
    >
      {awaiting ? (
        <EmptyState
          title="Awaiting cluster observations"
          description="Replica status will appear when cluster observations are available."
        />
      ) : (
        <div className="sr-shard-health-grid">
          {cluster.shards.map(shard => (
            <ShardHealthCard
              key={shard.shardId}
              shard={shard}
            />
          ))}
        </div>
      )}

      <p className="sr-shard-health-note">
        Reachability does not prove consensus availability.
        Commit spread is the difference between the highest
        and lowest observed commit indexes among reachable
        replicas. Observations are not atomic.
      </p>
    </Panel>
  );
}

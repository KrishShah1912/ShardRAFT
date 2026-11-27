import { useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import {
  DATA_SHARDS,
  METADATA_REPLICAS,
} from "./clusterState";

import {
  STORAGE_NODE_IDS,
} from "../api/multiRaft";

import type {
  DataShardID,
} from "../api/multiDiagnostics";

import { MultiLiveTopology } from "./MultiLiveTopology";
import { MultiElectionTelemetry } from "./MultiElectionTelemetry";
import type { MultiLiveTelemetry } from "./useMultiLiveTelemetry";

import { MultiReplicationTelemetry } from "./MultiReplicationTelemetry";

import "./multiConsensus.css";

interface Props {
  cluster: MultiRaftClusterState;
}

function Role({
  role,
}: {
  role: string;
}) {
  const known = [
    "leader",
    "follower",
    "candidate",
  ].includes(role);

  return (
    <span
      className={`mc-role mc-${known ? role : "unknown"}`}
    >
      {role}
    </span>
  );
}

export function MultiCluster({
  cluster,
  telemetry,
}: Props & { telemetry: MultiLiveTelemetry }) {
  return (
    <section className="mc-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            DISTRIBUTED CONSENSUS
          </div>

          <h1>Cluster</h1>

          <p>
            Five physical nodes and five independent
            Raft groups.
          </p>
        </div>
      </header>

      <MultiLiveTopology
        cluster={cluster}
        telemetry={telemetry}
      />

      <MultiElectionTelemetry
        cluster={cluster}
        telemetry={telemetry}
      />

      <div className="mc-node-grid">
        {STORAGE_NODE_IDS.map(id => {
          const observation =
            cluster.observations.find(
              node => node.id === id,
            );

          const service = cluster.services.find(
            item => item.service === `n${id}`,
          );

          const groups = observation?.reachable
            ? observation.status.groups
            : [];

          const metadata = cluster.metadata.find(
            item => item.node_id === id,
          );

          return (
            <article className="ma-panel" key={id}>
              <div className="mc-card-heading">
                <h2>Node {id}</h2>

                <span
                  className={
                    observation?.reachable
                      ? "mc-online"
                      : "mc-offline"
                  }
                >
                  {cluster.loading
                    ? "Connecting"
                    : observation?.reachable
                      ? "Reachable"
                      : "Not reachable"}
                </span>
              </div>

              <div className="mc-node-details">
                <span>HTTP endpoint</span>
                <strong>:{18080 + id}</strong>

                <span>Container state</span>
                <strong>
                  {service?.state ?? "Unknown"}
                </strong>

                <span>Container health</span>
                <strong>
                  {service?.health || "Unknown"}
                </strong>

                <span>Observed data groups</span>
                <strong>{groups.length}</strong>
              </div>

              {groups.map(group => (
                <div
                  className="mc-group"
                  key={group.shard_id}
                >
                  <div className="mc-card-heading">
                    <strong>
                      Shard {group.shard_id}
                    </strong>

                    <Role role={group.role} />
                  </div>

                  <small>
                    Term {group.term}
                    {" · "}
                    Commit {group.commit_index}
                    {" · "}
                    Applied {group.applied_index}
                  </small>
                </div>
              ))}

              {id <= 3 && (
                <div className="mc-group mc-meta">
                  <div className="mc-card-heading">
                    <strong>Metadata group</strong>

                    <Role
                      role={metadata?.role ?? "unknown"}
                    />
                  </div>

                  <small>
                    Term {metadata?.term ?? "—"}
                    {" · "}
                    Commit {metadata?.commit_index ?? "—"}
                  </small>
                </div>
              )}

              {observation && !observation.reachable && (
                <p className="mc-error">
                  {observation.error}
                </p>
              )}
            </article>
          );
        })}
      </div>

      <article className="ma-panel">
        <h2>Raft group membership</h2>

        <div className="ma-table-wrap">
          <table className="mc-matrix">
            <thead>
              <tr>
                <th>Raft group</th>

                {STORAGE_NODE_IDS.map(id => (
                  <th key={id}>Node {id}</th>
                ))}
              </tr>
            </thead>

            <tbody>
              {cluster.shards.map(shard => (
                <tr key={shard.shardId}>
                  <th>Shard {shard.shardId}</th>

                  {STORAGE_NODE_IDS.map(id => {
                    const replica = shard.replicas.find(
                      item => item.nodeId === id,
                    );

                    return (
                      <td key={id}>
                        {replica
                          ? <Role role={replica.role} />
                          : <span className="mc-muted">—</span>}
                      </td>
                    );
                  })}
                </tr>
              ))}

              <tr>
                <th>Metadata (5)</th>

                {STORAGE_NODE_IDS.map(id => {
                  const member = METADATA_REPLICAS.some(
                    node => node === id,
                  );

                  const metadata = cluster.metadata.find(
                    item => item.node_id === id,
                  );

                  return (
                    <td key={id}>
                      {member
                        ? <Role role={metadata?.role ?? "unknown"} />
                        : <span className="mc-muted">—</span>}
                    </td>
                  );
                })}
              </tr>
            </tbody>
          </table>
        </div>

        <p>
          Roles are independently observed from each
          replica. A missing observation is not treated
          as a confirmed follower or leader.
        </p>
      </article>

      <article className="ma-panel">
        <h2>HTTP gateways</h2>

        <div className="mc-gateway-grid">
          {[1, 2].map(id => {
            const service = cluster.services.find(
              item => item.service === `gateway${id}`,
            );

            return (
              <div className="mc-group" key={id}>
                <strong>Gateway {id}</strong>

                <p>HTTP port {18090 + id}</p>

                <div>
                  Container state:{" "}
                  {service?.state ?? "Unknown"}
                </div>
              </div>
            );
          })}
        </div>

        <p>
          Container state does not establish gateway
          request availability.
        </p>
      </article>
    </section>
  );
}

export function MultiReplication({
  cluster,
  telemetry,
}: Props & { telemetry: MultiLiveTelemetry }) {
  const [selectedShard, setSelectedShard] =
    useState<DataShardID>(1);

  const shard = cluster.shards.find(
    item => item.shardId === selectedShard,
  );

  const leader = shard?.replicas.find(
    replica => replica.nodeId === shard.leaderId,
  );

  const maxCommit = Math.max(
    1,
    ...(shard?.replicas.map(
      replica => replica.commitIndex ?? 0,
    ) ?? []),
  );

  return (
    <section className="mc-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            RAFT REPLICATION
          </div>

          <h1>Replication</h1>

          <p>
            Inspect commit and applied progress
            independently for every data shard.
          </p>
        </div>
      </header>

      <div className="mc-shard-selector">
        {DATA_SHARDS.map(id => (
          <button
            type="button"
            key={id}
            className={
              selectedShard === id
                ? "mc-selected"
                : ""
            }
            onClick={() => setSelectedShard(id)}
          >
            Shard {id}
          </button>
        ))}
      </div>

      <MultiReplicationTelemetry
        cluster={cluster}
        telemetry={telemetry}
        selectedShard={selectedShard}
      />

      <div className="ma-stat-grid">
        {[
          {
            label: "Observed leader",
            value: shard?.leaderId
              ? `Node ${shard.leaderId}`
              : "Unknown",
          },
          {
            label: "Leader term",
            value: leader?.term ?? "—",
          },
          {
            label: "Reachable replicas",
            value: `${shard?.reachableReplicas ?? 0}/3`,
          },
          {
            label: "Leader commit index",
            value: leader?.commitIndex ?? "—",
          },
          {
            label: "Leader applied index",
            value: leader?.appliedIndex ?? "—",
          },
          {
            label: "Observed commit spread",
            value: shard?.replicationLag ?? "—",
          },
        ].map(stat => (
          <article
            className="ma-stat"
            key={stat.label}
          >
            <span>{stat.label}</span>

            <strong>
              {cluster.loading ? "—" : stat.value}
            </strong>
          </article>
        ))}
      </div>

      <article className="ma-panel">
        <h2>Shard {selectedShard} replication progress</h2>

        <div className="mc-legend">
          <span>
            <i className="mc-commit-color" />
            Commit index
          </span>

          <span>
            <i className="mc-applied-color" />
            Applied index
          </span>
        </div>

        <div className="mc-replication-grid">
          {shard?.replicas.map(replica => (
            <div
              className="mc-replication-card"
              key={replica.nodeId}
            >
              <div className="mc-card-heading">
                <strong>Node {replica.nodeId}</strong>
                <Role role={replica.role} />
              </div>

              {replica.reachable ? (
                <>
                  <small>
                    Current term: {replica.term}
                  </small>

                  <div className="mc-bar-label">
                    <span>Committed</span>
                    <strong>{replica.commitIndex}</strong>
                  </div>

                  <div className="mc-track">
                    <div
                      className="mc-commit-color"
                      style={{
                        width: `${
                          (replica.commitIndex ?? 0)
                          / maxCommit * 100
                        }%`,
                      }}
                    />
                  </div>

                  <div className="mc-bar-label">
                    <span>Applied</span>
                    <strong>{replica.appliedIndex}</strong>
                  </div>

                  <div className="mc-track">
                    <div
                      className="mc-applied-color"
                      style={{
                        width: `${
                          Math.min(
                            100,
                            (replica.appliedIndex ?? 0)
                            / maxCommit * 100,
                          )
                        }%`,
                      }}
                    />
                  </div>

                  <div className="mc-apply-gap">
                    Apply backlog:{" "}
                    {(replica.commitIndex ?? 0) -
                      (replica.appliedIndex ?? 0)}
                  </div>
                </>
              ) : (
                <p>
                  Replica not observed. No current
                  replication counters available.
                </p>
              )}
            </div>
          ))}
        </div>

        <p>
          Bars are scaled to the highest observed commit
          index for this shard. They are not percentages
          of total database completion.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Replica consensus state</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Term</th>
                <th>Reported leader</th>
                <th>Commit</th>
                <th>Applied</th>
                <th>Apply backlog</th>
              </tr>
            </thead>

            <tbody>
              {shard?.replicas.map(replica => (
                <tr key={replica.nodeId}>
                  <td>Node {replica.nodeId}</td>
                  <td><Role role={replica.role} /></td>
                  <td>{replica.term ?? "—"}</td>
                  <td>{replica.leaderId || "—"}</td>
                  <td>{replica.commitIndex ?? "—"}</td>
                  <td>{replica.appliedIndex ?? "—"}</td>
                  <td>
                    {replica.commitIndex !== null &&
                    replica.appliedIndex !== null
                      ? replica.commitIndex -
                        replica.appliedIndex
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </article>

      <article className="ma-panel">
        <h2>Cross-shard replication summary</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Shard</th>
                <th>Observed leader</th>
                <th>Reachable replicas</th>
                <th>Majority reachable</th>
                <th>Commit spread</th>
              </tr>
            </thead>

            <tbody>
              {cluster.shards.map(item => (
                <tr key={item.shardId}>
                  <td>Shard {item.shardId}</td>
                  <td>
                    {item.leaderId
                      ? `Node ${item.leaderId}`
                      : "Unknown"}
                  </td>
                  <td>{item.reachableReplicas}/3</td>
                  <td>
                    {cluster.loading
                      ? "Unknown"
                      : item.majorityReachable
                        ? "Yes"
                        : "No"}
                  </td>
                  <td>{item.replicationLag ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p>
          Reachable majority is an HTTP observation,
          not proof of successful Raft communication.
          Commit spread compares observed counters,
          not uncommitted log entries.
        </p>
      </article>
    </section>
  );
}

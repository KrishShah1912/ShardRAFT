import { useState } from "react";
import {
  Server,
  Database,
  HardDrive,
  Layers,
  Radio,
} from "lucide-react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
} from "./clusterState";

import "./multiPhysicalDeployment.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const NODES = [1, 2, 3, 4, 5] as const;

const COLORS: Record<number, string> = {
  1: "#60a5fa",
  2: "#2dd4bf",
  3: "#c084fc",
  4: "#fbbf24",
};

export function MultiPhysicalDeployment({
  cluster,
}: Props) {
  const [selected, setSelected] = useState<number>(0);
  const [animate, setAnimate] = useState(true);

  const reachable = cluster.observations.filter(
    node => node.reachable,
  ).length;

  return (
    <article className="ma-panel mpd-panel">
      <div className="mpd-header">
        <div>
          <div className="mpd-eyebrow">
            PHYSICAL INFRASTRUCTURE
          </div>
          <h2>Physical node placement</h2>
          <p>
            Five storage nodes hosting independent
            consensus groups and persistent storage.
          </p>
        </div>

        <label className="mpd-toggle">
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Animate deployment
        </label>
      </div>

      <div className="mpd-summary">
        {[
          ["Physical nodes", "5"],
          ["Reachable nodes", `${reachable}/5`],
          ["Data Raft groups", "4"],
          ["Data replicas", "12"],
          ["Metadata replicas", "3"],
        ].map(([label, value]) => (
          <div key={label}>
            <small>{label}</small>
            <strong>{value}</strong>
          </div>
        ))}
      </div>

      <div className="mpd-filters">
        <button
          className={selected === 0 ? "active" : ""}
          onClick={() => setSelected(0)}
        >
          All groups
        </button>

        {DATA_SHARDS.map(id => (
          <button
            key={id}
            className={selected === id ? "active" : ""}
            style={{ color: COLORS[id] }}
            onClick={() => setSelected(id)}
          >
            <span
              className="mpd-dot"
              style={{ background: COLORS[id] }}
            />
            Shard {id}
          </button>
        ))}

        <button
          className={selected === 5 ? "active" : ""}
          onClick={() => setSelected(5)}
        >
          Metadata
        </button>
      </div>

      <div className={
        `mpd-canvas ${animate ? "" : "mpd-static"}`
      }>
        <div className="mpd-network">
          <Radio size={17} />
          <strong>Distributed gRPC Network</strong>
          <small>Configured transport topology</small>
        </div>

        <div className="mpd-racks">
          {NODES.map(nodeId => {
            const node = cluster.observations.find(
              item => item.id === nodeId,
            );

            const online = node?.reachable === true;

            const hosted = DATA_SHARDS.filter(
              id => SHARD_PLACEMENT[id].includes(nodeId),
            );

            const highlighted =
              selected === 0 ||
              (selected === 5
                ? nodeId <= 3
                : hosted.some(id => id === selected));

            return (
              <div
                key={nodeId}
                className={[
                  "mpd-rack-column",
                  highlighted ? "" : "mpd-dim",
                ].join(" ")}
              >
                <div className={
                  `mpd-link ${online ? "online" : "offline"}`
                }>
                  <span />
                </div>

                <div className={
                  `mpd-rack ${online ? "online" : "offline"}`
                }>
                  <div className="mpd-rack-header">
                    <Server size={22} />
                    <div>
                      <strong>Node {nodeId}</strong>
                      <small>HTTP :{18080 + nodeId}</small>
                    </div>
                    <span className="mpd-status-light" />
                  </div>

                  <div className="mpd-node-state">
                    {cluster.loading
                      ? "Loading"
                      : online
                        ? "Reachable"
                        : "Not reachable"}
                  </div>

                  <div className="mpd-replicas">
                    {hosted.map(shardId => {
                      const shard = cluster.shards.find(
                        item => item.shardId === shardId,
                      );

                      const replica = shard?.replicas.find(
                        item => item.nodeId === nodeId,
                      );

                      const leader =
                        replica?.reachable &&
                        replica.role === "leader";

                      const focused =
                        selected === 0 ||
                        selected === shardId;

                      return (
                        <div
                          key={shardId}
                          className={[
                            "mpd-replica",
                            leader ? "mpd-leader" : "",
                            focused ? "" : "mpd-dim",
                            replica?.reachable
                              ? ""
                              : "mpd-unobserved",
                          ].join(" ")}
                          style={{
                            borderColor: COLORS[shardId],
                          }}
                        >
                          <div className="mpd-replica-title">
                            <Layers
                              size={15}
                              color={COLORS[shardId]}
                            />
                            <strong style={{
                              color: COLORS[shardId],
                            }}>
                              Shard {shardId}
                            </strong>
                          </div>

                          <div className={
                            `mpd-role ${leader ? "leader" : ""}`
                          }>
                            {replica?.reachable
                              ? replica.role
                              : "Not observed"}
                          </div>

                          <div className="mpd-replica-stats">
                            <div>
                              <small>Term</small>
                              <b>{replica?.term ?? "—"}</b>
                            </div>
                            <div>
                              <small>Commit</small>
                              <b>
                                {replica?.commitIndex ?? "—"}
                              </b>
                            </div>
                            <div>
                              <small>Applied</small>
                              <b>
                                {replica?.appliedIndex ?? "—"}
                              </b>
                            </div>
                          </div>
                        </div>
                      );
                    })}

                    {nodeId <= 3 && (
                      <div className={[
                        "mpd-replica mpd-meta",
                        selected === 0 || selected === 5
                          ? ""
                          : "mpd-dim",
                      ].join(" ")}>
                        <div className="mpd-replica-title">
                          <Database size={15} />
                          <strong>Metadata</strong>
                        </div>
                        <div className="mpd-role">
                          {cluster.metadataLeader === nodeId
                            ? "Observed leader"
                            : "Configured replica"}
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="mpd-storage">
                    <HardDrive size={19} />
                    <strong>Persistent volume</strong>

                    <div className="mpd-storage-layer">
                      Segmented WAL
                    </div>
                    <div className="mpd-storage-layer">
                      Pebble KV
                    </div>
                    <div className="mpd-storage-layer">
                      Snapshots
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <div className="mpd-legend">
        <span>
          <i style={{ background: "#34d399" }} />
          Reachable
        </span>
        <span>
          <i style={{ background: "#60a5fa" }} />
          Observed leader
        </span>
        <span>
          <i style={{ background: "#f87171" }} />
          Not reachable
        </span>
        <span>
          <i style={{ background: "#c084fc" }} />
          Metadata group
        </span>
      </div>

      <p className="mpd-note">
        Replica roles and counters reflect live cluster
        observations. Network motion illustrates connectivity,
        not individual message delivery. Storage layers show
        configured architecture, not measured disk activity.
      </p>
    </article>
  );
}

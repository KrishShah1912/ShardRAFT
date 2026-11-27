import { useState } from "react";

import {
  Server,
  ShieldCheck,
  ShieldAlert,
  GitBranch,
  Activity,
  Database,
} from "lucide-react";

import {
  STORAGE_NODE_IDS,
  type StorageNodeID,
} from "../api/multiRaft";

import {
  METADATA_REPLICAS,
} from "./clusterState";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import { assessNodeQuorum } from "./quorumProtection";

import "./multiFaultTopology.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const POSITIONS = [
  { x: 400, y: 76 },
  { x: 650, y: 205 },
  { x: 555, y: 395 },
  { x: 245, y: 395 },
  { x: 150, y: 205 },
];

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

export function MultiFaultTopology({
  cluster,
}: Props) {
  const [selected, setSelected] =
    useState<StorageNodeID>(1);

  const assessment = assessNodeQuorum(
    cluster,
    selected,
  );

  const observed =
    assessment.observationsAvailable;

  const service = cluster.services.find(
    item => item.service === `n${selected}`,
  );

  const selectedObservation =
    cluster.observations.find(
      item => item.id === selected,
    );

  const affected = assessment.affected.map(group => ({
    ...group,
    leader: group.leaderId,
    color: group.id === "metadata"
      ? "#94a3b8"
      : COLORS[Number(group.id.split("-")[1]) - 1],
  }));

  const metadataNodes = new Set(
    cluster.metadata
      .filter(item =>
        METADATA_REPLICAS.some(
          id => id === item.node_id,
        ),
      )
      .map(item => item.node_id),
  );

  const risks = assessment.risks;

  const dataMajorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  const metadataMajority =
    metadataNodes.size >= 2;

  const leaderGroups = affected.filter(
    group => group.leader === selected,
  );

  return (
    <article className="ma-panel mft-panel">
      <div className="mft-heading">
        <div className="mft-heading-icon">
          <Activity size={22} />
        </div>

        <div>
          <div className="mft-eyebrow">
            FAILURE IMPACT EXPLORER
          </div>

          <h2>Interactive failure topology</h2>

          <p>
            Select a physical node to inspect
            its hosted groups and projected
            quorum availability.
          </p>
        </div>

        <span className="mft-live">
          <i />
          {observed
            ? "Live observations"
            : "Observations unavailable"}
        </span>
      </div>

      <div className="mft-metrics">
        <div>
          <span>Reachable nodes</span>
          <strong>
            {observed
              ? `${cluster.reachableNodes}/5`
              : "—"}
          </strong>
        </div>

        <div>
          <span>Data leaders</span>
          <strong>
            {observed
              ? `${cluster.shards.filter(
                  shard => shard.leaderId !== null,
                ).length}/4`
              : "—"}
          </strong>
        </div>

        <div>
          <span>Reachable majorities</span>
          <strong>
            {observed
              ? `${dataMajorities +
                  Number(metadataMajority)}/5`
              : "—"}
          </strong>
        </div>

        <div>
          <span>Selected node</span>
          <strong>Node {selected}</strong>
        </div>
      </div>

      <div className="mft-workspace">
        <div className="mft-canvas">
          <svg
            viewBox="0 0 800 480"
            aria-label="Physical node topology"
          >
            <defs>
              <pattern
                id="mft-grid"
                width="24"
                height="24"
                patternUnits="userSpaceOnUse"
              >
                <path
                  d="M24 0H0V24"
                  fill="none"
                  stroke="#26364a"
                  strokeWidth=".5"
                  opacity=".5"
                />
              </pattern>
            </defs>

            <rect
              width="800"
              height="480"
              fill="url(#mft-grid)"
            />

            {cluster.shards.map(shard =>
              shard.replicas.flatMap((left, i) =>
                shard.replicas
                  .slice(i + 1)
                  .map(right => {
                    const a =
                      POSITIONS[left.nodeId - 1];

                    const b =
                      POSITIONS[right.nodeId - 1];

                    const active =
                      left.nodeId === selected ||
                      right.nodeId === selected;

                    return (
                      <line
                        key={`${shard.shardId}-${left.nodeId}-${right.nodeId}`}
                        x1={a.x}
                        y1={a.y}
                        x2={b.x}
                        y2={b.y}
                        stroke={COLORS[shard.shardId - 1]}
                        strokeWidth={active ? 2 : 1}
                        strokeDasharray="6 6"
                        opacity={active ? .65 : .12}
                      />
                    );
                  }),
              ),
            )}

            <circle
              cx="400"
              cy="255"
              r="65"
              fill="#111e2d"
              stroke="#334155"
            />

            <text
              x="400"
              y="244"
              textAnchor="middle"
              fill="#94a3b8"
              fontSize="12"
            >
              MULTI-RAFT
            </text>

            <text
              x="400"
              y="270"
              textAnchor="middle"
              fill="#e2e8f0"
              fontSize="23"
              fontWeight="700"
            >
              5 Groups
            </text>

            {STORAGE_NODE_IDS.map(id => {
              const point = POSITIONS[id - 1];

              const node = cluster.observations.find(
                item => item.id === id,
              );

              const container = cluster.services.find(
                item => item.service === `n${id}`,
              );

              const stopped =
                container?.state === "exited" ||
                container?.state === "created";

              const reachable =
                Boolean(node?.reachable);

              const active = selected === id;

              const color = !observed
                ? "#64748b"
                : stopped
                  ? "#f87171"
                  : reachable
                    ? "#34d399"
                    : "#fbbf24";

              const label = !observed
                ? "Awaiting data"
                : stopped
                  ? "Stopped"
                  : reachable
                    ? "Reachable"
                    : "Not observed";

              const hosted = cluster.shards.filter(
                shard => shard.replicas.some(
                  replica => replica.nodeId === id,
                ),
              );

              return (
                <g
                  key={id}
                  className="mft-svg-node"
                  role="button"
                  tabIndex={0}
                  aria-label={`Inspect Node ${id}`}
                  aria-pressed={active}
                  onClick={() => setSelected(id)}
                  onKeyDown={event => {
                    if (
                      event.key === "Enter" ||
                      event.key === " "
                    ) {
                      event.preventDefault();
                      setSelected(id);
                    }
                  }}
                >
                  {active && (
                    <rect
                      x={point.x - 84}
                      y={point.y - 59}
                      width="168"
                      height="118"
                      rx="15"
                      fill="#60a5fa12"
                      stroke="#60a5fa"
                      strokeDasharray="4 4"
                    />
                  )}

                  <rect
                    x={point.x - 77}
                    y={point.y - 52}
                    width="154"
                    height="104"
                    rx="12"
                    fill="#142131"
                    stroke={active ? "#60a5fa" : color}
                    strokeWidth={active ? 2 : 1.2}
                  />

                  <circle
                    cx={point.x + 59}
                    cy={point.y - 34}
                    r="5"
                    fill={color}
                  />

                  <text
                    x={point.x}
                    y={point.y - 18}
                    textAnchor="middle"
                    fill="#e2e8f0"
                    fontSize="17"
                    fontWeight="600"
                  >
                    Node {id}
                  </text>

                  <text
                    x={point.x}
                    y={point.y + 3}
                    textAnchor="middle"
                    fill={color}
                    fontSize="12"
                  >
                    {label}
                  </text>

                  <text
                    x={point.x}
                    y={point.y + 27}
                    textAnchor="middle"
                    fill="#94a3b8"
                    fontSize="11"
                  >
                    {[
                      ...hosted.map(
                        shard => `S${shard.shardId}`,
                      ),
                      ...(id <= 3 ? ["META"] : []),
                    ].join(" · ")}
                  </text>
                </g>
              );
            })}
          </svg>

          <div className="mft-legend">
            <span>
              <i style={{ background: "#34d399" }} />
              Reachable
            </span>

            <span>
              <i style={{ background: "#f87171" }} />
              Stopped
            </span>

            <span>
              <i style={{ background: "#fbbf24" }} />
              Not observed
            </span>

            <span>
              <i style={{ background: "#60a5fa" }} />
              Selected
            </span>
          </div>

          <p className="mft-note">
            Dashed links represent configured
            data-group membership, not measured
            network connectivity.
          </p>
        </div>

        <aside className="mft-inspector">
          <div className="mft-inspector-title">
            <Server size={20} />

            <div>
              <h3>Node {selected}</h3>
              <small>Physical node inspector</small>
            </div>
          </div>

          <div className="mft-fields">
            <span>Container</span>
            <strong>
              {service?.state ?? "Unknown"}
            </strong>

            <span>Health</span>
            <strong>
              {service?.health || "Unknown"}
            </strong>

            <span>HTTP endpoint</span>
            <strong>:{18080 + selected}</strong>

            <span>Reachability</span>
            <strong>
              {!observed
                ? "Unknown"
                : selectedObservation?.reachable
                  ? "Reachable"
                  : "Not observed"}
            </strong>

            <span>Hosted groups</span>
            <strong>{affected.length}</strong>

            <span>Observed leaderships</span>
            <strong>
              {observed ? leaderGroups.length : "—"}
            </strong>
          </div>

          <div className="mft-impact-heading">
            <ShieldCheck size={17} />
            Projected stop impact
          </div>

          {affected.map(group => (
            <div
              className="mft-group"
              key={group.id}
            >
              <div className="mft-group-heading">
                {group.id === "metadata"
                  ? <Database size={15} />
                  : <GitBranch size={15} />}

                <strong style={{ color: group.color }}>
                  {group.label}
                </strong>

                {group.leader === selected && (
                  <small>Leader hosted</small>
                )}
              </div>

              <div className="mft-group-counts">
                <span>
                  Current
                  <b>
                    {observed
                      ? `${group.current}/3`
                      : "—"}
                  </b>
                </span>

                <span className="mft-arrow">→</span>

                <span>
                  After stop
                  <b>
                    {observed
                      ? `${group.remaining}/3`
                      : "—"}
                  </b>
                </span>
              </div>

              <div className="mft-replicas">
                {[0, 1, 2].map(index => (
                  <i
                    key={index}
                    style={{
                      background:
                        observed &&
                        index < group.remaining
                          ? group.remaining >= 2
                            ? "#34d399"
                            : "#f87171"
                          : "#334155",
                    }}
                  />
                ))}
              </div>

              <small
                style={{
                  color: !observed
                    ? "#94a3b8"
                    : group.remaining >= 2
                      ? "#34d399"
                      : "#f87171",
                }}
              >
                {!observed
                  ? "Awaiting observations"
                  : group.remaining >= 2
                    ? "Reachable majority retained"
                    : "Reachable majority at risk"}
              </small>
            </div>
          ))}

          <div
            className={
              "mft-assessment " +
              (!observed
                ? "unknown"
                : risks.length
                  ? "danger"
                  : "safe")
            }
          >
            {risks.length
              ? <ShieldAlert size={20} />
              : <ShieldCheck size={20} />}

            <div>
              <strong>
                {!observed
                  ? "Awaiting observations"
                  : risks.length
                    ? "Quorum risk detected"
                    : "All group majorities retained"}
              </strong>

              <p>
                {assessment.reason}
              </p>
            </div>
          </div>
        </aside>
      </div>

      <p className="mft-note">
        This is a read-only impact preview based on
        the latest cluster observations. Selecting
        a node does not execute a Docker action.
        Reachable majorities do not guarantee
        successful consensus or writes.
      </p>
    </article>
  );
}

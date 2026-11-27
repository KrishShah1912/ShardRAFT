import { useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import {
  heartbeatIdentity,
} from "./liveTelemetry";

import "./multiLiveTopology.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

const NODES = [
  { id: 1, x: 400, y: 75 },
  { id: 2, x: 675, y: 245 },
  { id: 3, x: 570, y: 465 },
  { id: 4, x: 230, y: 465 },
  { id: 5, x: 125, y: 245 },
];

const GROUPS = [
  { id: 1, nodes: [1, 2, 3], color: "#60a5fa" },
  { id: 2, nodes: [2, 3, 4], color: "#2dd4bf" },
  { id: 3, nodes: [3, 4, 5], color: "#c084fc" },
  { id: 4, nodes: [1, 4, 5], color: "#fbbf24" },
];

const META = {
  id: 5,
  nodes: [1, 2, 3],
  color: "#94a3b8",
};

export function MultiLiveTopology({
  cluster,
  telemetry,
}: Props) {
  const [selectedGroup, setSelectedGroup] =
    useState(0);

  const [selectedNode, setSelectedNode] =
    useState<number | null>(null);

  const [animate, setAnimate] = useState(true);

  const visibleGroups = selectedGroup === 0
    ? [...GROUPS, META]
    : [...GROUPS, META].filter(
        group => group.id === selectedGroup,
      );

  const observation = (id: number) =>
    cluster.observations.find(
      node => node.id === id,
    );

  const reachable = (id: number) =>
    observation(id)?.reachable === true;

  const stream = (id: number) =>
    telemetry.streams.find(
      item => item.nodeId === id,
    );

  const leaderFor = (id: number) =>
    id === 5
      ? cluster.metadataLeader
      : cluster.shards.find(
          shard => shard.shardId === id,
        )?.leaderId ?? null;

  function roleFor(
    nodeId: number,
    groupId: number,
  ): string {
    if (!reachable(nodeId)) {
      return "unknown";
    }

    if (groupId === 5) {
      return cluster.metadata.find(
        item => item.node_id === nodeId,
      )?.role ?? "unknown";
    }

    const node = observation(nodeId);

    return node?.reachable
      ? node.status.groups.find(
          group => group.shard_id === groupId,
        )?.role ?? "unknown"
      : "unknown";
  }

  const selectedObservation =
    selectedNode === null
      ? undefined
      : observation(selectedNode);

  const selectedService =
    selectedNode === null
      ? undefined
      : cluster.services.find(
          item => item.service === `n${selectedNode}`,
        );

  const selectedMetadata =
    selectedNode === null
      ? undefined
      : cluster.metadata.find(
          item => item.node_id === selectedNode,
        );

  return (
    <article
      className="ma-panel mtp-panel"
      data-testid="multi-live-topology"
    >
      <div className="mtp-heading">
        <div>
          <div className="eyebrow">
            PHYSICAL CLUSTER NETWORK
          </div>

          <h2>Live Cluster Topology</h2>

          <p>
            Five physical nodes hosting four data
            shards and one metadata Raft group.
          </p>
        </div>

        <div className="mtp-status">
          {cluster.reachableNodes}/5 reachable
          {" · "}
          {telemetry.connectedNodes}/5 SSE connected
        </div>
      </div>

      <div className="mtp-controls">
        <div className="mtp-tabs">
          {[
            { id: 0, label: "All groups" },
            ...GROUPS.map(group => ({
              id: group.id,
              label: `Shard ${group.id}`,
            })),
            { id: 5, label: "Metadata" },
          ].map(group => (
            <button
              key={group.id}
              type="button"
              aria-pressed={
                selectedGroup === group.id
              }
              className={
                selectedGroup === group.id
                  ? "active"
                  : ""
              }
              onClick={() =>
                setSelectedGroup(group.id)
              }
            >
              {group.label}
            </button>
          ))}
        </div>

        <label className="mtp-toggle">
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Animate traffic
        </label>
      </div>

      <div className="mtp-graph">
        <svg
          viewBox="0 0 800 550"
          aria-label="Physical Multi-Raft cluster"
        >
          <circle
            cx="400"
            cy="285"
            r="175"
            fill="none"
            stroke="#64748b"
            strokeOpacity=".12"
            strokeDasharray="4 10"
          />

          {visibleGroups.map(group => {
            const leader = leaderFor(group.id);

            const links: [number, number][] =
              leader === null
                ? group.nodes.flatMap(
                    (node, index) =>
                      group.nodes.slice(index + 1)
                        .map(peer =>
                          [node, peer] as [number, number],
                        ),
                  )
                : group.nodes
                    .filter(node => node !== leader)
                    .map(node => [leader, node]);

            return links.map(([from, to]) => {
              const a = NODES.find(
                node => node.id === from,
              );

              const b = NODES.find(
                node => node.id === to,
              );

              if (!a || !b) return null;

              const dx = b.x - a.x;
              const dy = b.y - a.y;

              const length = Math.hypot(dx, dy);

              const ux = dx / length;
              const uy = dy / length;

              const x1 = a.x + ux * 62;
              const y1 = a.y + uy * 62;

              const x2 = b.x - ux * 62;
              const y2 = b.y - uy * 62;

              const bend = (group.id - 3) * 25;

              const cx =
                (x1 + x2) / 2 - uy * bend;

              const cy =
                (y1 + y2) / 2 + ux * bend;

              const path =
                `M${x1} ${y1} Q${cx} ${cy} ${x2} ${y2}`;

              const termObservation =
                observation(from);

              const currentTerm =
                termObservation?.reachable
                  ? termObservation.status.groups.find(
                      item => item.shard_id === group.id,
                    )?.term
                  : undefined;

              const packets =
                group.id === 5 ||
                leader === null ||
                currentTerm === undefined ||
                !reachable(from) ||
                !reachable(to) ||
                stream(from)?.status !== "connected" ||
                stream(to)?.status !== "connected"
                  ? []
                  : telemetry.heartbeats.filter(
                      item =>
                        item.shardId === group.id &&
                        item.leaderId === from &&
                        item.followerId === to &&
                        item.term === currentTerm,
                    ).slice(0, 16);

              const active = packets.length > 0;

              return (
                <g key={`${group.id}:${from}:${to}`}>
                  <path
                    d={path}
                    fill="none"
                    stroke={group.color}
                    strokeWidth={active ? 2.5 : 1.5}
                    strokeDasharray={
                      group.id === 5 ? "3 8" : "8 7"
                    }
                    opacity={
                      active
                        ? ".9"
                        : reachable(from) &&
                          reachable(to)
                          ? ".35"
                          : ".12"
                    }
                    className={
                      animate && active
                        ? "mtp-active-path"
                        : undefined
                    }
                  />

                  {animate && packets.map(
                    (packet, index) => (
                      <circle
                        key={
                          heartbeatIdentity(packet)
                          + ":" + packet.stage
                        }
                        className="mtp-packet"
                        cx="0"
                        cy="0"
                        r="4"
                        fill={group.color}
                        style={{
                          color: group.color,
                          offsetPath: `path('${path}')`,
                          offsetRotate: "0deg",
                          animationDelay:
                            `${index * 180}ms`,
                        }}
                      />
                    ),
                  )}
                </g>
              );
            });
          })}

          <text
            x="400"
            y="270"
            textAnchor="middle"
            fill="currentColor"
            fontSize="24"
            fontWeight="600"
          >
            Multi-Raft
          </text>

          <text
            x="400"
            y="295"
            textAnchor="middle"
            fill="currentColor"
            opacity=".6"
            fontSize="13"
          >
            {cluster.reachableNodes}/5 storage nodes
          </text>

          <text
            x="400"
            y="315"
            textAnchor="middle"
            fill="currentColor"
            opacity=".6"
            fontSize="12"
          >
            {selectedGroup === 0
              ? "All Raft groups"
              : selectedGroup === 5
                ? "Metadata group"
                : `Shard ${selectedGroup}`}
          </text>

          {NODES.map(node => {
            const online = reachable(node.id);

            const selected =
              selectedNode === node.id;

            const memberships =
              [...GROUPS, META].filter(
                group => group.nodes.includes(node.id),
              );

            const relevant =
              selectedGroup === 0 ||
              memberships.some(
                group => group.id === selectedGroup,
              );

            const color = online
              ? "#34d399"
              : "#f87171";

            const leading = memberships.filter(
              group =>
                roleFor(node.id, group.id) === "leader",
            );

            return (
              <g
                key={node.id}
                role="button"
                tabIndex={0}
                aria-label={`Inspect Node ${node.id}`}
                aria-pressed={selected}
                className="mtp-node"
                opacity={relevant ? 1 : .35}
                onClick={() =>
                  setSelectedNode(node.id)
                }
                onKeyDown={event => {
                  if (
                    event.key === "Enter" ||
                    event.key === " "
                  ) {
                    event.preventDefault();
                    setSelectedNode(node.id);
                  }
                }}
              >
                <rect
                  x={node.x - 88}
                  y={node.y - 49}
                  width="176"
                  height="98"
                  rx="12"
                  className="mtp-node-bg"
                  stroke={
                    selected ? "#60a5fa" : color
                  }
                  strokeWidth={selected ? 3 : 1.5}
                />

                <circle
                  cx={node.x - 69}
                  cy={node.y - 30}
                  r="4"
                  fill={color}
                />

                <text
                  x={node.x}
                  y={node.y - 23}
                  textAnchor="middle"
                  fill="currentColor"
                  fontSize="18"
                  fontWeight="600"
                >
                  Node {node.id}
                </text>

                <text
                  x={node.x}
                  y={node.y - 5}
                  textAnchor="middle"
                  fill={color}
                  fontSize="11"
                >
                  {cluster.loading
                    ? "Connecting"
                    : online
                      ? "Reachable"
                      : "Unreachable"}
                </text>

                {memberships.map((group, index) => {
                  const x =
                    node.x -
                    memberships.length * 17 +
                    index * 34;

                  const role =
                    roleFor(node.id, group.id);

                  return (
                    <g key={group.id}>
                      <rect
                        x={x}
                        y={node.y + 4}
                        width="30"
                        height="18"
                        rx="4"
                        fill={group.color}
                        fillOpacity={
                          role === "leader" ? ".28" : ".08"
                        }
                        stroke={group.color}
                        strokeWidth={
                          role === "leader" ? 1.8 : .6
                        }
                      />

                      <text
                        x={x + 15}
                        y={node.y + 17}
                        textAnchor="middle"
                        fill={group.color}
                        fontSize="10"
                        fontWeight="600"
                      >
                        {group.id === 5
                          ? "M"
                          : `S${group.id}`}
                      </text>

                      <title>
                        {group.id === 5
                          ? "Metadata"
                          : `Shard ${group.id}`}
                        {`: ${role}`}
                      </title>
                    </g>
                  );
                })}

                <text
                  x={node.x}
                  y={node.y + 38}
                  textAnchor="middle"
                  fill="currentColor"
                  opacity=".65"
                  fontSize="10"
                >
                  {leading.length
                    ? `Leads ${leading.map(group =>
                        group.id === 5
                          ? "M"
                          : `S${group.id}`,
                      ).join(", ")}`
                    : `SSE: ${stream(node.id)?.status
                        ?? "unknown"}`}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      <div className="mtp-legend">
        {[...GROUPS, META].map(group => (
          <span key={group.id}>
            <i style={{ background: group.color }} />
            {group.id === 5
              ? "Metadata"
              : `Shard ${group.id}`}
          </span>
        ))}
      </div>

      <div className="mtp-group-grid">
        {[...GROUPS, META].map(group => (
          <button
            key={group.id}
            type="button"
            className="mtp-group-card"
            onClick={() => setSelectedGroup(group.id)}
            style={{ borderColor: group.color }}
          >
            <span style={{ color: group.color }}>
              {group.id === 5
                ? "Metadata"
                : `Shard ${group.id}`}
            </span>

            <strong>
              {leaderFor(group.id) === null
                ? "Leader unknown"
                : `Leader: Node ${leaderFor(group.id)}`}
            </strong>

            <small>
              {group.nodes.filter(reachable).length}/3
              {" nodes reachable"}
            </small>
          </button>
        ))}
      </div>

      {selectedNode !== null ? (
        <section className="mtp-inspector">
          <div className="mtp-heading">
            <h3>Node {selectedNode} inspector</h3>

            <button
              type="button"
              onClick={() => setSelectedNode(null)}
            >
              Close
            </button>
          </div>

          <div className="mtp-details">
            <div>
              <span>Reachability</span>
              <strong>
                {reachable(selectedNode)
                  ? "Reachable"
                  : "Unreachable"}
              </strong>
            </div>

            <div>
              <span>SSE connection</span>
              <strong>
                {stream(selectedNode)?.status
                  ?? "Unknown"}
              </strong>
            </div>

            <div>
              <span>Container</span>
              <strong>
                {selectedService?.state ?? "Unknown"}
              </strong>
            </div>

            <div>
              <span>HTTP port</span>
              <strong>{18080 + selectedNode}</strong>
            </div>
          </div>

          <div className="mtp-table-wrap">
            <table className="mtp-table">
              <thead>
                <tr>
                  <th>Group</th>
                  <th>Role</th>
                  <th>Term</th>
                  <th>Commit</th>
                  <th>Applied</th>
                </tr>
              </thead>

              <tbody>
                {selectedObservation?.reachable &&
                  selectedObservation.status.groups.map(
                    group => (
                      <tr key={group.shard_id}>
                        <td>Shard {group.shard_id}</td>
                        <td>{group.role}</td>
                        <td>{group.term}</td>
                        <td>{group.commit_index}</td>
                        <td>{group.applied_index}</td>
                      </tr>
                    ),
                  )}

                {selectedMetadata && (
                  <tr>
                    <td>Metadata</td>
                    <td>{selectedMetadata.role}</td>
                    <td>{selectedMetadata.term}</td>
                    <td>{selectedMetadata.commit_index}</td>
                    <td>—</td>
                  </tr>
                )}

                {!selectedObservation?.reachable && (
                  <tr>
                    <td colSpan={5}>
                      Data group status unavailable.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      ) : (
        <p className="mtp-note">
          Select any physical node to inspect its
          hosted Raft groups and current status.
        </p>
      )}

      <p className="mtp-note">
        Animated paths represent recent observed data
        shard heartbeats, not measured network latency.
        Metadata links show membership and observed
        leadership only. Reachability does not prove
        consensus availability.
      </p>
    </article>
  );
}

import { useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import {
  heartbeatIdentity,
  type HeartbeatStage,
} from "./liveTelemetry";

import "./multiHeartbeatTelemetry.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

const PLACEMENT: Record<number, number[]> = {
  1: [1, 2, 3],
  2: [2, 3, 4],
  3: [3, 4, 5],
  4: [1, 4, 5],
};

const COLORS: Record<HeartbeatStage, string> = {
  sent: "#60a5fa",
  received: "#fbbf24",
  accepted: "#34d399",
};

export function MultiHeartbeatTelemetry({
  cluster,
  telemetry,
}: Props) {
  const [shardId, setShardId] = useState(1);
  const [animate, setAnimate] = useState(true);

  const shard = cluster.shards.find(
    item => item.shardId === shardId,
  );

  const leader = shard?.leaderId ?? null;

  const replicas = PLACEMENT[shardId];

  const followers = replicas.filter(
    node => node !== leader,
  );

  const reachable = (nodeId: number) =>
    cluster.observations.some(
      node => node.id === nodeId &&
        node.reachable,
    );

  const streamStatus = (nodeId: number) =>
    telemetry.streams.find(
      stream => stream.nodeId === nodeId,
    )?.status ?? "disconnected";

  const heartbeats = telemetry.heartbeats.filter(
    item => item.shardId === shardId,
  );

  const activeHeartbeats = heartbeats.filter(
    item =>
      item.leaderId === leader &&
      reachable(item.leaderId) &&
      reachable(item.followerId) &&
      streamStatus(item.leaderId) === "connected" &&
      streamStatus(item.followerId) === "connected",
  );

  const counts = {
    sent: telemetry.recentEvents.filter(
      event => event.shard_id === shardId &&
        event.type === "raft.heartbeat.sent",
    ).length,

    received: telemetry.recentEvents.filter(
      event => event.shard_id === shardId &&
        event.type === "raft.heartbeat.received",
    ).length,

    accepted: telemetry.recentEvents.filter(
      event => event.shard_id === shardId &&
        event.type === "raft.heartbeat.accepted",
    ).length,
  };

  function renderNode(
    nodeId: number,
    x: number,
    y: number,
  ) {
    const online = reachable(nodeId);
    const isLeader = nodeId === leader;

    const color = !online
      ? "#f87171"
      : isLeader
        ? "#34d399"
        : "#60a5fa";

    return (
      <g key={nodeId}>
        <rect
          x={x - 80}
          y={y - 35}
          width="160"
          height="70"
          rx="12"
          fill={color}
          fillOpacity=".08"
          stroke={color}
          strokeWidth="1.5"
        />

        <circle
          cx={x - 62}
          cy={y - 17}
          r="4"
          fill={color}
        />

        <text
          x={x}
          y={y - 12}
          textAnchor="middle"
          fill={color}
          fontSize="11"
          fontWeight="600"
        >
          {!online
            ? "UNREACHABLE"
            : isLeader
              ? "LEADER"
              : leader === null
                ? "REPLICA"
                : "REPLICA"}
        </text>

        <text
          x={x}
          y={y + 9}
          textAnchor="middle"
          fill="currentColor"
          fontSize="19"
          fontWeight="600"
        >
          Node {nodeId}
        </text>

        <text
          x={x}
          y={y + 25}
          textAnchor="middle"
          fill="currentColor"
          opacity=".65"
          fontSize="10"
        >
          SSE: {streamStatus(nodeId)}
        </text>
      </g>
    );
  }

  return (
    <article
      className="ma-panel mh-panel"
      data-testid="multi-heartbeat-panel"
    >
      <div className="mh-heading">
        <div>
          <div className="eyebrow">
            LIVE CONSENSUS TELEMETRY
          </div>

          <h2>Live Raft Heartbeats</h2>

          <p>
            Observed heartbeat traffic across
            the selected data shard.
          </p>
        </div>

        <span className="mh-connection">
          {telemetry.connectedNodes}/5 SSE connected
        </span>
      </div>

      <div className="mh-controls">
        <div className="mh-tabs">
          {[1, 2, 3, 4].map(id => (
            <button
              key={id}
              type="button"
              aria-pressed={id === shardId}
              className={
                id === shardId ? "active" : ""
              }
              onClick={() => setShardId(id)}
            >
              Shard {id}
            </button>
          ))}
        </div>

        <label className="mh-toggle">
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Animate packets
        </label>
      </div>

      <div className="mh-stats">
        {([
          ["sent", counts.sent],
          ["received", counts.received],
          ["accepted", counts.accepted],
        ] as const).map(([stage, count]) => (
          <div key={stage}>
            <span>{stage}</span>

            <strong style={{ color: COLORS[stage] }}>
              {count}
            </strong>

            <small>Last 3 seconds</small>
          </div>
        ))}

        <div>
          <span>Observed leader</span>
          <strong>
            {leader === null
              ? "Unknown"
              : `Node ${leader}`}
          </strong>
          <small>Node status API</small>
        </div>
      </div>

      <div className="mh-diagram">
        <svg
          viewBox="0 0 640 300"
          role="img"
          aria-label={`Shard ${shardId} heartbeat topology`}
        >
          {leader !== null ? (
            <>
              {followers.map((nodeId, index) => {
                const x = index === 0 ? 150 : 490;

                const path =
                  `M320 100 Q320 165 ${x} 225`;

                const packets = activeHeartbeats
                  .filter(item =>
                    item.followerId === nodeId,
                  )
                  .slice(0, 16);

                const latest = packets[0];

                return (
                  <g key={nodeId}>
                    <path
                      d={path}
                      fill="none"
                      stroke={
                        latest
                          ? COLORS[latest.stage]
                          : "#64748b"
                      }
                      className={
                        animate && latest
                          ? "mh-active-path"
                          : undefined
                      }
                      strokeWidth="2.5"
                      strokeDasharray="8 7"
                      opacity={latest ? ".9" : ".35"}
                    />

                    {animate && packets.map(
                      (packet, packetIndex) => (
                        <circle
                          className="mh-live-packet"
                          key={
                            heartbeatIdentity(packet)
                            + ":" + packet.stage
                          }
                          cx="0"
                          cy="0"
                          r="5"
                          fill={COLORS[packet.stage]}
                          style={{
                            offsetPath: `path('${path}')`,
                            offsetRotate: "0deg",
                            animationDelay:
                              `${packetIndex * 180}ms`,
                            color: COLORS[packet.stage],
                          }}
                        />
                      ),
                    )}

                    <text
                      x={index === 0 ? 185 : 455}
                      y="163"
                      fontSize="11"
                      textAnchor="middle"
                      fill={
                        latest
                          ? COLORS[latest.stage]
                          : "currentColor"
                      }
                    >
                      {latest
                        ? latest.stage
                        : "No recent observation"}
                    </text>

                    {renderNode(nodeId, x, 260)}
                  </g>
                );
              })}

              {renderNode(leader, 320, 65)}
            </>
          ) : (
            <>
              <text
                x="320"
                y="65"
                textAnchor="middle"
                fill="#fbbf24"
                fontSize="16"
              >
                No unique leader observed
              </text>

              {replicas.map((node, index) =>
                renderNode(
                  node,
                  110 + index * 210,
                  165,
                ),
              )}
            </>
          )}
        </svg>
      </div>

      <div className="mh-legend">
        {Object.entries(COLORS).map(
          ([stage, color]) => (
            <span key={stage}>
              <i style={{ background: color }} />
              {stage}
            </span>
          ),
        )}
      </div>

      <h3>Recent heartbeat observations</h3>

      <div className="mh-table-wrap">
        <table className="mh-table">
          <thead>
            <tr>
              <th>Leader</th>
              <th>Replica</th>
              <th>Term</th>
              <th>Request ID</th>
              <th>Stage</th>
              <th>Observed at</th>
            </tr>
          </thead>

          <tbody>
            {heartbeats.slice(0, 8).map(item => (
              <tr key={heartbeatIdentity(item)}>
                <td>Node {item.leaderId}</td>
                <td>Node {item.followerId}</td>
                <td>{item.term}</td>
                <td>{item.requestId}</td>
                <td style={{
                  color: COLORS[item.stage],
                }}>
                  {item.stage}
                </td>
                <td>
                  {new Date(
                    item.timestamp,
                  ).toLocaleTimeString()}
                </td>
              </tr>
            ))}

            {heartbeats.length === 0 && (
              <tr>
                <td colSpan={6}>
                  Waiting for recent heartbeat events.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <p className="mh-note">
        Packets represent observed heartbeat events,
        not measured network transit time.
        Acceptance does not establish quorum health.
        Metadata heartbeats are not included.
        Client buffer drops: {telemetry.droppedEvents}.
      </p>
    </article>
  );
}

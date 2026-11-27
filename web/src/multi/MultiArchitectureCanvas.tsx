import { useId, useState } from "react";

import {
  Activity,
  Database,
  Network,
  Server,
  Workflow,
} from "lucide-react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import {
  heartbeatIdentity,
} from "./liveTelemetry";

import "./multiArchitectureCanvas.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

const GROUPS = [
  { id: 1, nodes: [1, 2, 3] },
  { id: 2, nodes: [2, 3, 4] },
  { id: 3, nodes: [3, 4, 5] },
  { id: 4, nodes: [1, 4, 5] },
];

export function MultiArchitectureCanvas({
  cluster,
  telemetry,
}: Props) {
  const [animate, setAnimate] = useState(true);
  const [selected, setSelected] = useState(0);

  const uid = useId();
  const gridId = `${uid}-grid`;
  const glowId = `${uid}-glow`;

  const map = cluster.shardMap?.shard_map;

  const leaders = cluster.shards.filter(
    shard => shard.leaderId !== null,
  ).length;

  const majorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  const connected = (nodeId: number) =>
    telemetry.streams.some(
      stream =>
        stream.nodeId === nodeId &&
        stream.status === "connected",
    );

  function path(
    d: string,
    color: string,
    active = true,
    illustrative = false,
  ) {
    return (
      <g>
        <path
          d={d}
          fill="none"
          stroke={color}
          strokeWidth="7"
          opacity={active ? ".07" : ".025"}
        />

        <path
          d={d}
          fill="none"
          stroke={color}
          strokeWidth={active ? 1.8 : 1}
          strokeDasharray="7 8"
          opacity={active ? ".8" : ".18"}
          className={
            animate && active && illustrative
              ? "mxa-flow-line"
              : undefined
          }
        />
      </g>
    );
  }

  function packet(
    key: string,
    d: string,
    color: string,
    delay = 0,
  ) {
    if (!animate) return null;

    return (
      <circle
        key={key}
        r="4"
        fill={color}
        className="mxa-packet"
        style={{
          color,
          offsetPath: `path('${d}')`,
          offsetRotate: "0deg",
          animationDelay: `${delay}ms`,
        }}
      />
    );
  }

  return (
    <article className="ma-panel mxa-panel">
      <div className="mxa-heading">
        <div>
          <div className="eyebrow">
            DISTRIBUTED SYSTEM VISUALIZER
          </div>

          <h2>Architecture Control Plane</h2>

          <p>
            Logical routing, independent Raft groups
            and persistent storage.
          </p>
        </div>

        <label className="mxa-toggle">
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Enable motion
        </label>
      </div>

      <div className="mxa-metrics">
        {[
          {
            icon: Server,
            label: "Storage nodes",
            value: `${cluster.reachableNodes}/5`,
          },
          {
            icon: Network,
            label: "Observed leaders",
            value: `${leaders}/4`,
          },
          {
            icon: Workflow,
            label: "Reachable majorities",
            value: `${majorities}/4`,
          },
          {
            icon: Activity,
            label: "Live SSE streams",
            value: `${telemetry.connectedNodes}/5`,
          },
          {
            icon: Database,
            label: "Metadata epoch",
            value: map?.epoch ?? "Unknown",
          },
        ].map(item => (
          <div
            className="mxa-metric"
            key={item.label}
          >
            <item.icon size={17} />
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <div className="mxa-controls">
        {[0, 1, 2, 3, 4].map(id => (
          <button
            key={id}
            type="button"
            aria-pressed={selected === id}
            className={
              selected === id ? "active" : ""
            }
            onClick={() => setSelected(id)}
          >
            {id === 0
              ? "Complete architecture"
              : `Shard ${id}`}
          </button>
        ))}
      </div>

      <div className="mxa-canvas">
        <svg
          viewBox="0 0 1200 860"
          aria-label="Multi-Raft logical architecture"
        >
          <defs>
            <pattern
              id={gridId}
              width="24"
              height="24"
              patternUnits="userSpaceOnUse"
            >
              <path
                d="M24 0H0V24"
                fill="none"
                stroke="#233247"
                strokeWidth=".6"
                opacity=".65"
              />
            </pattern>

            <filter id={glowId}>
              <feGaussianBlur
                stdDeviation="3"
                result="blur"
              />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          <rect
            width="1200"
            height="860"
            fill={`url(#${gridId})`}
          />

          <g className="mxa-layer-label">
            <text x="24" y="30">01 / CLIENT</text>
            <text x="24" y="150">02 / GATEWAYS</text>
            <text x="24" y="286">03 / ROUTING</text>
            <text x="24" y="438">04 / CONSENSUS</text>
            <text x="24" y="778">05 / STORAGE</text>
          </g>

          {/* Client */}
          <rect
            x="440" y="22"
            width="320" height="78"
            rx="13"
            className="mxa-svg-box"
            stroke="#60a5fa"
          />

          <text
            x="600" y="53"
            className="mxa-svg-title"
          >
            Client / Query Console
          </text>

          <text
            x="600" y="77"
            className="mxa-svg-subtitle"
          >
            GET · PUT · DELETE · CAS
          </text>

          {/* Configured gateway paths */}
          {[360, 840].map((x, index) => {
            const d =
              `M600 100 V120 H${x} V158`;

            return (
              <g key={x}>
                {path(d, "#60a5fa", true, true)}

                {packet(
                  `client-${index}`,
                  d,
                  "#60a5fa",
                  index * 350,
                )}
              </g>
            );
          })}

          {/* Gateways */}
          {[360, 840].map((x, index) => (
            <g key={x}>
              <rect
                x={x - 135}
                y="158"
                width="270"
                height="88"
                rx="12"
                className="mxa-svg-box"
                stroke="#60a5fa"
              />

              <text
                x={x}
                y="185"
                className="mxa-svg-title"
              >
                Gateway {index + 1}
              </text>

              <text
                x={x}
                y="207"
                className="mxa-svg-subtitle"
              >
                HTTP :{18091 + index}
              </text>

              <text
                x={x}
                y="228"
                className="mxa-svg-small"
              >
                Configured endpoint
              </text>
            </g>
          ))}

          {/* Metadata discovery paths */}
          {path(
            "M360 246 V268 H270 V306",
            "#c084fc",
            true,
            true,
          )}

          {path(
            "M840 246 V260 H480 V306",
            "#c084fc",
            true,
            true,
          )}

          {/* Key routing paths */}
          {path(
            "M360 246 V278 H720 V306",
            "#2dd4bf",
            true,
            true,
          )}

          {path(
            "M840 246 V306",
            "#2dd4bf",
            true,
            true,
          )}

          {/* Metadata group */}
          <rect
            x="145" y="306"
            width="410" height="108"
            rx="12"
            className="mxa-svg-box"
            stroke="#c084fc"
          />

          <text
            x="350" y="331"
            className="mxa-svg-title"
          >
            Metadata Raft Group
          </text>

          {[1, 2, 3].map((id, index) => {
            const x = 215 + index * 135;

            const leader =
              cluster.metadataLeader === id;

            const metadata =
              cluster.metadata.find(
                item => item.node_id === id,
              );

            return (
              <g key={id}>
                <rect
                  x={x - 48}
                  y="344"
                  width="96"
                  height="32"
                  rx="6"
                  fill={
                    leader
                      ? "#382552"
                      : "#151f2e"
                  }
                  stroke={
                    metadata
                      ? "#c084fc"
                      : "#64748b"
                  }
                  strokeWidth={leader ? 2 : 1}
                />

                <text
                  x={x}
                  y="365"
                  className="mxa-svg-small"
                >
                  N{id} · {
                    leader ? "Leader" :
                    metadata?.role ?? "Unknown"
                  }
                </text>
              </g>
            );
          })}

          <text
            x="350" y="398"
            className="mxa-svg-subtitle"
          >
            Discovery · Epoch {map?.epoch ?? "Unknown"}
          </text>

          {/* Routing engine */}
          <rect
            x="645" y="306"
            width="410" height="108"
            rx="12"
            className="mxa-svg-box"
            stroke="#2dd4bf"
          />

          <text
            x="850" y="334"
            className="mxa-svg-title"
          >
            Key Routing Engine
          </text>

          <text
            x="850" y="361"
            className="mxa-svg-subtitle"
          >
            CRC32 → Hash slot → Shard
          </text>

          <text
            x="850" y="389"
            className="mxa-svg-small"
          >
            65,536 configured hash slots
          </text>

          {path(
            "M555 360 H645",
            "#c084fc",
            true,
            true,
          )}

          {/* Data shard groups */}
          {GROUPS.map((group, index) => {
            const x = 20 + index * 295;
            const center = x + 137.5;
            const color = COLORS[index];

            const shard = cluster.shards.find(
              item => item.shardId === group.id,
            );

            const leader = shard?.replicas.find(
              item =>
                item.nodeId === shard.leaderId,
            );

            const relevant =
              selected === 0 ||
              selected === group.id;

            const ordered = [...group.nodes].sort(
              (a, b) =>
                Number(b === shard?.leaderId) -
                Number(a === shard?.leaderId),
            );

            const positions = [
              { x: center, y: 560 },
              { x: center - 67, y: 659 },
              { x: center + 67, y: 659 },
            ];

            const routingPath =
              `M850 414 V452 H${center} V478`;

            return (
              <g
                key={group.id}
                opacity={relevant ? 1 : .22}
              >
                {path(
                  routingPath,
                  color,
                  relevant,
                  true,
                )}

                <rect
                  x={x}
                  y="478"
                  width="275"
                  height="265"
                  rx="13"
                  className="mxa-svg-box"
                  stroke={color}
                  strokeWidth="1.5"
                />

                <text
                  x={center}
                  y="503"
                  textAnchor="middle"
                  fill={color}
                  fontSize="17"
                  fontWeight="700"
                >
                  SHARD {group.id}
                </text>

                <text
                  x={center}
                  y="523"
                  className="mxa-svg-small"
                >
                  {cluster.loading
                    ? "Connecting"
                    : shard?.state ?? "Unknown"}
                  {" · "}
                  {shard?.reachableReplicas ?? 0}/3
                  {" observed"}
                </text>

                {ordered.slice(1).map(
                  (nodeId, peerIndex) => {
                    const target =
                      positions[peerIndex + 1];

                    const replica =
                      shard?.replicas.find(
                        item =>
                          item.nodeId === nodeId,
                      );

                    const d =
                      `M${center} 587 ` +
                      `Q${center} 610 ` +
                      `${target.x} 631`;

                    const active =
                      !!leader?.reachable &&
                      !!replica?.reachable;

                    const packets =
                      active &&
                      leader &&
                      connected(leader.nodeId) &&
                      connected(nodeId)
                        ? telemetry.heartbeats
                            .filter(item =>
                              item.shardId === group.id &&
                              item.leaderId === leader.nodeId &&
                              item.followerId === nodeId &&
                              item.term === leader.term,
                            )
                            .slice(0, 3)
                        : [];

                    return (
                      <g key={nodeId}>
                        {leader && path(
                          d,
                          active ? color : "#f87171",
                          active,
                          packets.length > 0,
                        )}

                        {relevant && packets.map(
                          (item, packetIndex) =>
                            packet(
                              heartbeatIdentity(item),
                              d,
                              color,
                              packetIndex * 160,
                            ),
                        )}
                      </g>
                    );
                  },
                )}

                {ordered.map((nodeId, nodeIndex) => {
                  const position =
                    positions[nodeIndex];

                  const replica =
                    shard?.replicas.find(
                      item => item.nodeId === nodeId,
                    );

                  const isLeader =
                    shard?.leaderId === nodeId;

                  const online =
                    replica?.reachable === true;

                  const stroke = cluster.loading
                    ? "#64748b"
                    : !online
                      ? "#f87171"
                      : isLeader
                        ? color
                        : "#34d399";

                  return (
                    <g key={nodeId}>
                      {isLeader && online && (
                        <circle
                          cx={position.x}
                          cy={position.y}
                          r="35"
                          fill="none"
                          stroke={color}
                          strokeWidth="1.5"
                          className={
                            animate
                              ? "mxa-leader-ring"
                              : undefined
                          }
                        />
                      )}

                      <rect
                        x={position.x - 54}
                        y={position.y - 27}
                        width="108"
                        height="54"
                        rx="9"
                        fill={
                          isLeader
                            ? `${color}20`
                            : "#111c2b"
                        }
                        stroke={stroke}
                        strokeWidth={
                          isLeader ? 2 : 1
                        }
                      />

                      <circle
                        cx={position.x - 42}
                        cy={position.y - 15}
                        r="3"
                        fill={stroke}
                      />

                      <text
                        x={position.x}
                        y={position.y - 4}
                        className="mxa-svg-title"
                        style={{ fontSize: 15 }}
                      >
                        N{nodeId}
                      </text>

                      <text
                        x={position.x}
                        y={position.y + 14}
                        className="mxa-svg-small"
                      >
                        {online
                          ? replica?.role
                          : "Not observed"}
                      </text>
                    </g>
                  );
                })}

                <text
                  x={center}
                  y="710"
                  className="mxa-svg-small"
                >
                  Term {leader?.term ?? "—"}
                  {" · Commit "}
                  {leader?.commitIndex ?? "—"}
                </text>

                <text
                  x={center}
                  y="729"
                  className="mxa-svg-small"
                >
                  Commit spread:
                  {" "}
                  {shard?.replicationLag ?? "—"}
                </text>

                {path(
                  `M${center} 743 V790`,
                  color,
                  relevant,
                  false,
                )}
              </g>
            );
          })}

          {/* Persistent storage */}
          <rect
            x="20"
            y="790"
            width="1160"
            height="53"
            rx="10"
            className="mxa-svg-box"
            stroke="#64748b"
          />

          <text
            x="600" y="811"
            className="mxa-svg-title"
          >
            Replicated Persistent Storage
          </text>

          <text
            x="600" y="831"
            className="mxa-svg-subtitle"
          >
            Segmented WAL · Raft Log · Pebble · Snapshots
          </text>
        </svg>
      </div>

      <div className="mxa-legend">
        <span>
          <i style={{ background: "#60a5fa" }} />
          Configured client paths
        </span>

        <span>
          <i style={{ background: "#c084fc" }} />
          Metadata discovery
        </span>

        <span>
          <i style={{ background: "#2dd4bf" }} />
          Key routing
        </span>

        <span>
          <i style={{ background: "#34d399" }} />
          Observed replica
        </span>

        <span>
          <i style={{ background: "#f87171" }} />
          Unobserved replica
        </span>
      </div>

      <p className="mxa-note">
        Client, gateway and routing motion is
        illustrative. Data shard packets represent
        recent observed heartbeats, not measured
        latency or confirmed response delivery.
        Metadata links show configured discovery
        relationships. Reachability does not prove
        consensus availability.
      </p>
    </article>
  );
}

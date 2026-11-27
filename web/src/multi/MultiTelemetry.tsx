import MultiEventInspector from "./MultiEventInspector";
import { ChartHover } from "./ChartHover";
import { MultiLiveMetrics } from "./MultiLiveMetrics";
import type { MultiLiveTelemetry } from "./useMultiLiveTelemetry";

import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getShardDiagnostic,
  type DataShardID,
} from "../api/multiDiagnostics";

import {
  STORAGE_NODE_IDS,
  type StorageNodeID,
} from "../api/multiRaft";

import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
} from "./clusterState";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import {
  eventIdentity,
  LIVE_EVENT_TYPES,
  type LiveEvent,
} from "./liveTelemetry";

import "./multiTelemetry.css";

interface Props {
  cluster: MultiRaftClusterState;
}

interface TransportCounters {
  enqueued: number;
  sent: number;
  delivered: number;
  retries: number;
  failures: number;
  backpressure: number;
}

interface PeerStats extends TransportCounters {
  peer_id: number;
  address: string;
  queue_depth: number;
  queue_capacity: number;
  last_latency_ms?: number;
  average_latency_ms?: number;
  last_success_at?: string;
  last_failure_at?: string;
  last_error?: string;
}

interface TransportData extends TransportCounters {
  closed: boolean;
  peers: PeerStats[];
}

function Header({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <header className="page-header">
      <div>
        <div className="eyebrow">
          MULTI-RAFT OBSERVABILITY
        </div>

        <h1>{title}</h1>
        <p>{description}</p>
      </div>
    </header>
  );
}

function Stats({
  items,
}: {
  items: {
    label: string;
    value: string | number;
  }[];
}) {
  return (
    <div className="ma-stat-grid">
      {items.map(item => (
        <article
          className="ma-stat"
          key={item.label}
        >
          <span>{item.label}</span>

          <strong>
            {typeof item.value === "number"
              ? item.value.toLocaleString()
              : item.value}
          </strong>
        </article>
      ))}
    </div>
  );
}

function formatLatency(value?: number) {
  return typeof value === "number"
    ? `${value.toFixed(3)} ms`
    : "—";
}

function formatTime(value?: string) {
  if (!value) return "—";

  const date = new Date(value);

  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleTimeString();
}

function TransportReader({
  node,
  shard,
}: {
  node: StorageNodeID;
  shard: DataShardID;
}) {
  const [data, setData] =
    useState<TransportData | null>(null);

  const [error, setError] = useState("");
  const [updated, setUpdated] = useState("");
  const [paused, setPaused] = useState(false);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    let active = true;
    let busy = false;

    async function poll() {
      if (!active || busy) return;

      busy = true;

      try {
        const result =
          await getShardDiagnostic<TransportData>(
            node,
            shard,
            "transport",
          );

        if (!active) return;

        if (
          !result.data ||
          !Array.isArray(result.data.peers)
        ) {
          throw new Error(
            "Invalid transport response",
          );
        }

        setData(result.data);
        setError("");
        setUpdated(new Date().toISOString());
      } catch (failure) {
        if (active) {
          setData(null);
          setError(String(failure));
        }
      } finally {
        busy = false;
      }
    }

    void poll();

    const timer = paused
      ? undefined
      : window.setInterval(
          () => void poll(),
          2000,
        );

    return () => {
      active = false;

      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [node, shard, paused, refresh]);

  return (
    <>
      <article className="ma-panel">
        <div className="mt-heading">
          <strong>
            Node {node} · Shard {shard} · gRPC
          </strong>

          <div className="mt-controls">
            <button
              onClick={() => setPaused(p => !p)}
            >
              {paused ? "Resume" : "Pause"}
            </button>

            <button
              onClick={() => setRefresh(n => n + 1)}
            >
              Refresh
            </button>
          </div>
        </div>

        <p>
          Last successful observation:{" "}
          {formatTime(updated)}
          {" · "}
          {paused ? "Polling paused" : "Polling every 2s"}
        </p>
      </article>

      {error && (
        <div className="mt-warning" role="alert">
          {error}
        </div>
      )}

      {!data && !error && (
        <article className="ma-panel">
          Loading transport observations...
        </article>
      )}

      {data && (
        <>
          <Stats
            items={[
              {
                label: "Enqueued messages",
                value: data.enqueued,
              },
              {
                label: "Sent messages",
                value: data.sent,
              },
              {
                label: "Delivered messages",
                value: data.delivered,
              },
              {
                label: "Retries",
                value: data.retries,
              },
              {
                label: "Failures",
                value: data.failures,
              },
              {
                label: "Backpressure",
                value: data.backpressure,
              },
            ]}
          />

          <article className="ma-panel">
            <h2>Peer transport connections</h2>

            <p>
              Transport state:{" "}
              {data.closed ? "Closed" : "Open"}
            </p>

            <div className="mt-peer-grid">
              {data.peers.map(peer => {
                const utilization =
                  peer.queue_capacity > 0
                    ? peer.queue_depth /
                      peer.queue_capacity * 100
                    : 0;

                return (
                  <div
                    className="mt-peer"
                    key={peer.peer_id}
                  >
                    <div className="mt-heading">
                      <strong>
                        Node {node} → Node {peer.peer_id}
                      </strong>

                      <small>{peer.address}</small>
                    </div>

                    <div className="mt-heading">
                      <span>Queue utilization</span>

                      <strong>
                        {peer.queue_depth}/
                        {peer.queue_capacity}
                      </strong>
                    </div>

                    <div className="mt-track">
                      <div
                        style={{
                          width: `${
                            Math.min(
                              100,
                              Math.max(0, utilization),
                            )
                          }%`,
                        }}
                      />
                    </div>

                    <div className="mt-fields">
                      <span>Enqueued</span>
                      <strong>{peer.enqueued}</strong>

                      <span>Sent</span>
                      <strong>{peer.sent}</strong>

                      <span>Delivered</span>
                      <strong>{peer.delivered}</strong>

                      <span>Retries</span>
                      <strong>{peer.retries}</strong>

                      <span>Failures</span>
                      <strong>{peer.failures}</strong>

                      <span>Backpressure</span>
                      <strong>{peer.backpressure}</strong>

                      <span>Last latency</span>

                      <strong>
                        {formatLatency(
                          peer.last_latency_ms,
                        )}
                      </strong>

                      <span>Average latency</span>

                      <strong>
                        {formatLatency(
                          peer.average_latency_ms,
                        )}
                      </strong>

                      <span>Last success</span>

                      <strong>
                        {formatTime(
                          peer.last_success_at,
                        )}
                      </strong>

                      <span>Last failure</span>

                      <strong>
                        {formatTime(
                          peer.last_failure_at,
                        )}
                      </strong>
                    </div>

                    {peer.last_error && (
                      <div className="mt-warning">
                        {peer.last_error}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>

            <p>
              Counters belong to the selected local
              transport instance and may reset after
              a process restart.
            </p>
          </article>

          <article className="ma-panel">
            <details>
              <summary>Raw transport statistics</summary>

              <pre className="mt-json">
                {JSON.stringify(data, null, 2)}
              </pre>
            </details>
          </article>
        </>
      )}
    </>
  );
}

export function MultiTransport({
  cluster,
}: Props) {
  const [shard, setShard] =
    useState<DataShardID>(1);

  const [preferredNode, setPreferredNode] =
    useState<StorageNodeID>(1);

  const replicas =
    SHARD_PLACEMENT[shard] as readonly StorageNodeID[];

  const node = replicas.includes(preferredNode)
    ? preferredNode
    : replicas[0];

  const observed = cluster.shards.find(
    item => item.shardId === shard,
  );

  return (
    <section className="mt-page">
      <Header
        title="Transport"
        description="Inspect per-shard gRPC message delivery and peer queues."
      />

      <article className="ma-panel">
        <h2>Select transport instance</h2>

        <div className="mt-selectors">
          <label>
            Data shard

            <select
              value={shard}
              onChange={event =>
                setShard(
                  Number(event.target.value) as DataShardID,
                )
              }
            >
              {DATA_SHARDS.map(id => (
                <option key={id} value={id}>
                  Shard {id}
                </option>
              ))}
            </select>
          </label>

          <label>
            Source replica

            <select
              value={node}
              onChange={event =>
                setPreferredNode(
                  Number(event.target.value) as StorageNodeID,
                )
              }
            >
              {replicas.map(id => (
                <option key={id} value={id}>
                  Node {id}
                  {" — "}
                  {observed?.replicas.find(
                    item => item.nodeId === id,
                  )?.role ?? "Unknown"}
                </option>
              ))}
            </select>
          </label>
        </div>
      </article>

      <TransportReader
        key={`${shard}-${node}`}
        node={node}
        shard={shard}
      />
    </section>
  );
}

interface MetricSample {
  timestamp: number;
  reachable: number;
  leaders: number;
  majorities: number;
}

function HistoryChart({
  samples,
}: {
  samples: MetricSample[];
}) {
  const width = 800;
  const height = 200;

  const series = [
    {
      field: "reachable" as const,
      color: "#60a5fa",
      strokeWidth: 2.5,
      dash: undefined,
    },
    {
      field: "majorities" as const,
      color: "#2dd4bf",
      strokeWidth: 4,
      dash: undefined,
    },
    {
      field: "leaders" as const,
      color: "#fbbf24",
      strokeWidth: 2.5,
      dash: "8 6",
    },
  ];

  function x(index: number) {
    return 40 + index /
      Math.max(1, samples.length - 1) * 740;
  }

  function y(value: number) {
    return 175 - value / 5 * 150;
  }

  return (
    <svg
      className="mt-chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Recent cluster observation history"
    >
      {[0, 1, 2, 3, 4, 5].map(value => (
        <g key={value}>
          <line
            x1="40"
            x2="780"
            y1={y(value)}
            y2={y(value)}
            stroke="currentColor"
            opacity=".1"
          />

          <text
            x="20"
            y={y(value) + 4}
            fill="currentColor"
            fontSize="11"
          >
            {value}
          </text>
        </g>
      ))}

      {series.map(item => (
        <g key={item.field}>
          <polyline
            points={samples.map((sample, index) =>
              `${x(index)},${y(sample[item.field])}`
            ).join(" ")}
            fill="none"
            stroke={item.color}
            strokeWidth={item.strokeWidth}
            strokeDasharray={item.dash}
            strokeLinejoin="round"
            strokeLinecap="round"
          />

          {samples.length === 1 && (
            <circle
              cx={x(0)}
              cy={y(samples[0][item.field])}
              r={item.field === "majorities" ? 4 : 2}
              fill={item.color}
            />
          )}
        </g>
      ))}
    <ChartHover
        points={samples.map((sample, index) => ({
          timestamp: sample.timestamp,
          x: x(index),
          values: [
            {
              label: "Reachable nodes",
              value: sample.reachable,
              color: "#60a5fa",
              y: y(sample.reachable),
            },
            {
              label: "Observed leaders",
              value: sample.leaders,
              color: "#fbbf24",
              y: y(sample.leaders),
            },
            {
              label: "Reachable majorities",
              value: sample.majorities,
              color: "#2dd4bf",
              y: y(sample.majorities),
            },
          ],
        }))}
        left={40}
        right={780}
        top={25}
        bottom={175}
        width={width}
      />
    </svg>
  );
}

export function MultiMetrics({
  cluster,
  telemetry,
}: Props & { telemetry: MultiLiveTelemetry }) {
  const [samples, setSamples] =
    useState<MetricSample[]>([]);

  const leaders = cluster.shards.filter(
    shard => shard.leaderId !== null,
  ).length;

  const majorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  const reachable = cluster.reachableNodes;

  useEffect(() => {
    if (!cluster.lastUpdated) return;

    const timestamp = new Date(
      cluster.lastUpdated,
    ).getTime();

    if (!Number.isFinite(timestamp)) return;

    setSamples(previous => {
      if (
        previous[previous.length - 1]?.timestamp
        === timestamp
      ) {
        return previous;
      }

      return [
        ...previous,
        {
          timestamp,
          reachable,
          leaders,
          majorities,
        },
      ].slice(-60);
    });
  }, [
    cluster.lastUpdated,
    reachable,
    leaders,
    majorities,
  ]);

  return (
    <section className="mt-page">
      <Header
        title="Metrics"
        description="Live cluster observations and per-shard consensus counters."
      />

      <MultiLiveMetrics telemetry={telemetry} />

      <Stats
        items={[
          {
            label: "Reachable storage nodes",
            value: cluster.loading
              ? "—"
              : `${reachable}/5`,
          },
          {
            label: "Observed data leaders",
            value: cluster.loading
              ? "—"
              : `${leaders}/4`,
          },
          {
            label: "Reachable data majorities",
            value: cluster.loading
              ? "—"
              : `${majorities}/4`,
          },
        ]}
      />

      <article className="ma-panel">
        <div className="mt-heading">
          <h2>Cluster observation history</h2>

          <button onClick={() => setSamples([])}>
            Clear history
          </button>
        </div>

        <div className="mt-legend">
          <span style={{ color: "#60a5fa" }}>
            ● Reachable nodes
          </span>

          <span style={{ color: "#fbbf24" }}>
            ● Observed leaders
          </span>

          <span style={{ color: "#2dd4bf" }}>
            ● Reachable majorities
          </span>
        </div>

        {samples.length > 0 ? (
          <HistoryChart samples={samples} />
        ) : (
          <p>Waiting for observations...</p>
        )}

        <div className="mt-heading">
          <small>
            {samples.length} / 60 observations
          </small>

          <small>
            {samples.length > 0
              ? formatTime(
                  new Date(
                    samples[0].timestamp,
                  ).toISOString(),
                )
              : "—"}
            {" – "}
            {samples.length > 0
              ? formatTime(
                  new Date(
                    samples[samples.length - 1]
                      .timestamp,
                  ).toISOString(),
                )
              : "—"}
          </small>
        </div>

        <p>
          Horizontal positions represent observation
          order, not uniform elapsed time. History is
          retained while this page is mounted.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Per-replica consensus metrics</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Shard</th>
                <th>Node</th>
                <th>Role</th>
                <th>Term</th>
                <th>Commit</th>
                <th>Applied</th>
                <th>Apply backlog</th>
              </tr>
            </thead>

            <tbody>
              {cluster.shards.flatMap(shard =>
                shard.replicas.map(replica => (
                  <tr
                    key={`${shard.shardId}-${replica.nodeId}`}
                  >
                    <td>Shard {shard.shardId}</td>
                    <td>Node {replica.nodeId}</td>
                    <td>{replica.role}</td>
                    <td>{replica.term ?? "—"}</td>
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
                )),
              )}
            </tbody>
          </table>
        </div>

        <p>
          Commit indexes from different Raft groups
          are independent and must not be interpreted
          as one global log.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Metadata consensus metrics</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Term</th>
                <th>Commit</th>
                <th>Applied</th>
              </tr>
            </thead>

            <tbody>
              {cluster.metadata.map(status => (
                <tr key={status.node_id}>
                  <td>Node {status.node_id}</td>
                  <td>{status.role}</td>
                  <td>{status.term}</td>
                  <td>{status.commit_index}</td>
                  <td>{status.applied_index}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </article>
    </section>
  );
}

export function MultiEvents({
  telemetry,
}: {
  telemetry: MultiLiveTelemetry;
}) {
  const [pausedEvents, setPausedEvents] =
    useState<LiveEvent[] | null>(null);

  const [clearedAt, setClearedAt] =
    useState<number | null>(null);

  const [inspection, setInspection] = useState<{
    event: LiveEvent;
    events: LiveEvent[];
  } | null>(null);

  const [node, setNode] = useState("all");
  const [shard, setShard] = useState("all");
  const [category, setCategory] = useState("all");
  const [search, setSearch] = useState("");
  const [limit, setLimit] = useState(100);
  const [quickFilter, setQuickFilter] = useState("all");
  const [eventType, setEventType] = useState("all");
  const [term, setTerm] = useState("");
  const [requestId, setRequestId] = useState("");

  const quickFilters = [
    ["all", "All events"],
    ["consensus", "Consensus"],
    ["elections", "Elections"],
    ["replication", "Replication"],
    ["commits", "Commits"],
    ["heartbeats", "Heartbeats"],
    ["transport", "Transport"],
  ] as const;

  const paused = pausedEvents !== null;

  const sharedEvents = useMemo(() => {
    const merged = new Map<string, LiveEvent>();

    for (const event of [
      ...telemetry.events,
      ...telemetry.electionEvents,
      ...telemetry.replicationEvents,
    ]) {
      merged.set(eventIdentity(event), event);
    }

    return [...merged.values()].sort(
      (a, b) =>
        Date.parse(b.timestamp) -
          Date.parse(a.timestamp) ||
        b.timestamp.localeCompare(a.timestamp) ||
        b.id - a.id ||
        b.node_id - a.node_id,
    );
  }, [
    telemetry.events,
    telemetry.electionEvents,
    telemetry.replicationEvents,
  ]);

  const sourceEvents =
    pausedEvents ?? sharedEvents;

  const events = useMemo(
    () => sourceEvents.filter(event =>
      clearedAt === null ||
      Date.parse(event.timestamp) > clearedAt
    ),
    [sourceEvents, clearedAt],
  );

  const updated = events[0]?.timestamp ?? "";

  const errors = telemetry.streams
    .filter(stream => stream.status !== "connected")
    .map(stream =>
      `Node ${stream.nodeId}: ${stream.status}`
    );

  function togglePause() {
    if (paused) {
      setPausedEvents(null);
    } else {
      setPausedEvents([...events]);
    }
  }

  function clearHistory() {
    setInspection(null);
    setClearedAt(Date.now());
    setLimit(100);

    if (paused) {
      setPausedEvents([]);
    }
  }

  const categories = Array.from(
    new Set(events.map(event => event.category)),
  ).sort();

  const eventTypes = useMemo(
    () => Array.from(new Set([
      ...LIVE_EVENT_TYPES,
      ...events.map(event => event.type),
    ])).sort(),
    [events],
  );

  function matchesQuickFilter(event: LiveEvent) {
    switch (quickFilter) {
      case "consensus":
        return event.category === "raft" &&
          !event.type.startsWith("raft.heartbeat.");

      case "elections":
        return (
          event.type.startsWith("raft.election.") ||
          event.type.startsWith("raft.vote.") ||
          event.type === "raft.leader.elected" ||
          event.type === "raft.role.changed" ||
          event.type === "raft.term.changed"
        );

      case "replication":
        return (
          event.type.startsWith("raft.append.") ||
          event.type === "raft.entry.appended" ||
          event.type === "raft.noop.appended" ||
          event.type === "raft.replication.progress"
        );

      case "commits":
        return event.type === "raft.commit.advanced";

      case "heartbeats":
        return event.type.startsWith("raft.heartbeat.");

      case "transport":
        return event.category === "transport";

      default:
        return true;
    }
  }

  const filtered = events.filter(event =>
    matchesQuickFilter(event) &&
    (node === "all" ||
      String(event.node_id) === node) &&
    (shard === "all" ||
      String(event.shard_id) === shard) &&
    (category === "all" ||
      event.category === category) &&
    (eventType === "all" ||
      event.type === eventType) &&
    (!term.trim() ||
      String(event.term ?? "") === term.trim()) &&
    (!requestId.trim() ||
      String(event.data.request_id ?? "") ===
        requestId.trim()) &&
    (!search.trim() ||
      JSON.stringify(event)
        .toLowerCase()
        .includes(search.trim().toLowerCase()))
  );

  function resetFilters() {
    setQuickFilter("all");
    setNode("all");
    setShard("all");
    setCategory("all");
    setEventType("all");
    setTerm("");
    setRequestId("");
    setSearch("");
    setLimit(100);
  }

  return (
    <section className="mt-page mt-events-page">
      <Header
        title="Events"
        description="Inspect observed Raft and transport events across the distributed deployment."
      />

      <Stats
        items={[
          {
            label: "Retained events",
            value: events.length,
          },
          {
            label: "Matching events",
            value: filtered.length,
          },
          {
            label: "Collection state",
            value: paused
              ? "Paused"
              : telemetry.connectedNodes === 5
                ? "Live"
                : telemetry.connectedNodes > 0
                  ? "Partial"
                  : "Disconnected / connecting",
          },
          {
            label: "SSE streams",
            value: `${telemetry.connectedNodes}/5`,
          },
          {
            label: "Raft events",
            value: events.filter(
              event => event.category === "raft",
            ).length,
          },
          {
            label: "Transport events",
            value: events.filter(
              event => event.category === "transport",
            ).length,
          },
          {
            label: "Pending queue evictions",
            value: telemetry.droppedEvents,
          },
        ]}
      />

      <article className="ma-panel">
        <div className="mt-heading">
          <h2>Event filters</h2>

          <div className="mt-controls">
            <button
              onClick={togglePause}
            >
              {paused ? "Resume" : "Pause"}
            </button>

            <button
              onClick={clearHistory}
            >
              Clear local history
            </button>
          </div>
        </div>

        <div className="mt-event-quick-filters">
          {quickFilters.map(([value, label]) => (
            <button
              key={value}
              className={
                quickFilter === value
                  ? "mt-event-quick-active"
                  : ""
              }
              aria-pressed={quickFilter === value}
              onClick={() => {
                setQuickFilter(value);
                setLimit(100);
              }}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="mt-event-filters">
          <label>
            Node

            <select
              value={node}
              onChange={event => {
                setNode(event.target.value);
                setLimit(100);
              }}
            >
              <option value="all">All nodes</option>

              {STORAGE_NODE_IDS.map(id => (
                <option key={id} value={id}>
                  Node {id}
                </option>
              ))}
            </select>
          </label>

          <label>
            Shard

            <select
              value={shard}
              onChange={event => {
                setShard(event.target.value);
                setLimit(100);
              }}
            >
              <option value="all">All shards</option>

              {DATA_SHARDS.map(id => (
                <option key={id} value={id}>
                  Shard {id}
                </option>
              ))}
            </select>
          </label>

          <label>
            Category

            <select
              value={category}
              onChange={event => {
                setCategory(event.target.value);
                setLimit(100);
              }}
            >
              <option value="all">
                All categories
              </option>

              {categories.map(value => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>

          <label>
            Event type

            <select
              value={eventType}
              onChange={event => {
                setEventType(event.target.value);
                setLimit(100);
              }}
            >
              <option value="all">All event types</option>

              {eventTypes.map(value => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>

          <label>
            Term

            <input
              inputMode="numeric"
              value={term}
              placeholder="Any term"
              onChange={event => {
                setTerm(event.target.value);
                setLimit(100);
              }}
            />
          </label>

          <label>
            Request ID

            <input
              inputMode="numeric"
              value={requestId}
              placeholder="Any request ID"
              onChange={event => {
                setRequestId(event.target.value);
                setLimit(100);
              }}
            />
          </label>

          <label>
            Search

            <input
              value={search}
              placeholder="Event type or details"
              onChange={event => {
                setSearch(event.target.value);
                setLimit(100);
              }}
            />
          </label>
        </div>

        <div className="mt-event-filter-footer">
          <small>
            {filtered.length.toLocaleString()} matching
            {" / "}
            {events.length.toLocaleString()} retained events
          </small>

          <button onClick={resetFilters}>
            Reset filters
          </button>
        </div>

        <p>
          Latest visible event:{" "}
          {formatTime(updated)}
          {" · "}
          Combined bounded live and consensus histories
        </p>

        {errors.length > 0 && (
          <details className="mt-warning">
            <summary>
              {errors.length} stream warning(s)
            </summary>

            {errors.map(error => (
              <p key={error}>{error}</p>
            ))}
          </details>
        )}
      </article>

      {inspection && (
        <MultiEventInspector
          key={eventIdentity(inspection.event)}
          event={inspection.event}
          events={inspection.events}
          onClose={() => setInspection(null)}
        />
      )}

      <article className="ma-panel">
        <h2>Event timeline</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Node</th>
                <th>Shard</th>
                <th>Category</th>
                <th>Event</th>
                <th>Term</th>
                <th>Details</th>
              </tr>
            </thead>

            <tbody>
              {filtered.slice(0, limit).map(event => (
                <tr key={eventIdentity(event)}>
                  <td title={event.timestamp}>
                    {formatTime(event.timestamp)}
                  </td>

                  <td>N{event.node_id}</td>

                  <td>
                    {event.shard_id
                      ? `S${event.shard_id}`
                      : "—"}
                  </td>

                  <td>{event.category}</td>

                  <td>
                    <code>{event.type}</code>
                  </td>

                  <td>{event.term ?? "—"}</td>

                  <td>
                    <button
                      className="mei-inspect-button"
                      onClick={() => {
                        setInspection({
                          event,
                          events: [...events],
                        });
                      }}
                    >
                      Inspect
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {filtered.length === 0 && (
          <p>No matching events available.</p>
        )}

        <div className="mt-heading">
          <small>
            Showing {Math.min(limit, filtered.length)}
            {" of "}
            {filtered.length} matching events
          </small>

          {limit < filtered.length && (
            <button
              onClick={() => setLimit(n => n + 100)}
            >
              Show 100 more
            </button>
          )}
        </div>

        <p>
          This timeline uses the shared live SSE
          event buffer. Collection is bounded and
          best-effort, not a lossless audit log.
          Metadata-group events are not included.
          Pause freezes this page only. Resume returns
          to the current shared buffer. Clear hides
          events through the local clear timestamp
          without deleting shared or backend history.
          General, election and replication buffers
          are merged and deduplicated. Queue evictions
          count pending-buffer removals across these
          buffers, not unique lost events. Request IDs
          are not globally unique across nodes, shards
          and terms. All filters are combined.
        </p>
      </article>
    </section>
  );
}

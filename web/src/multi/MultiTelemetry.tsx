import {
  useEffect,
  useState,
} from "react";

import {
  getShardDiagnostic,
  getMultiEventHistory,
  type DataShardID,
  type MultiRaftEvent,
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

  function points(
    field: "reachable" | "leaders" | "majorities",
  ) {
    return samples.map((sample, index) => {
      const x = 40 + index /
        Math.max(1, samples.length - 1) * 740;

      const y = 175 - sample[field] / 5 * 150;

      return `${x},${y}`;
    }).join(" ");
  }

  return (
    <svg
      className="mt-chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Recent cluster observation history"
    >
      {[0, 1, 2, 3, 4, 5].map(value => {
        const y = 175 - value / 5 * 150;

        return (
          <g key={value}>
            <line
              x1="40"
              x2="780"
              y1={y}
              y2={y}
              stroke="currentColor"
              opacity=".1"
            />

            <text
              x="20"
              y={y + 4}
              fill="currentColor"
              fontSize="11"
            >
              {value}
            </text>
          </g>
        );
      })}

      {[
        {
          field: "reachable" as const,
          color: "#60a5fa",
        },
        {
          field: "leaders" as const,
          color: "#fbbf24",
        },
        {
          field: "majorities" as const,
          color: "#2dd4bf",
        },
      ].map(series => (
        <g key={series.field}>
          <polyline
            points={points(series.field)}
            fill="none"
            stroke={series.color}
            strokeWidth="2"
            strokeLinejoin="round"
          />

          {samples.map((sample, index) => (
            <circle
              key={sample.timestamp}
              cx={
                40 + index /
                Math.max(1, samples.length - 1) * 740
              }
              cy={
                175 - sample[series.field] / 5 * 150
              }
              r="2.5"
              fill={series.color}
            />
          ))}
        </g>
      ))}
    </svg>
  );
}

export function MultiMetrics({
  cluster,
}: Props) {
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

function eventIdentity(event: MultiRaftEvent) {
  return [
    event.node_id,
    event.id,
    event.timestamp,
  ].join(":");
}

export function MultiEvents() {
  const [events, setEvents] =
    useState<MultiRaftEvent[]>([]);

  const [errors, setErrors] =
    useState<string[]>([]);

  const [paused, setPaused] = useState(false);
  const [node, setNode] = useState("all");
  const [shard, setShard] = useState("all");
  const [category, setCategory] = useState("all");
  const [search, setSearch] = useState("");
  const [limit, setLimit] = useState(100);
  const [updated, setUpdated] = useState("");

  useEffect(() => {
    if (paused) return;

    let active = true;
    let busy = false;

    async function poll() {
      if (!active || busy) return;

      busy = true;

      try {
        const results = await Promise.allSettled(
          STORAGE_NODE_IDS.map(
            async id => ({
              node: id,
              history: await getMultiEventHistory(id),
            }),
          ),
        );

        if (!active) return;

        const incoming: MultiRaftEvent[] = [];
        const warnings: string[] = [];

        results.forEach((result, index) => {
          if (result.status === "fulfilled") {
            const { node, history } = result.value;

            if (!Array.isArray(history.events)) {
              warnings.push(
                `Node ${node}: invalid event history`,
              );
              return;
            }

            incoming.push(
              ...history.events.filter(
                event => event.node_id === node,
              ),
            );
          } else {
            warnings.push(
              `Node ${STORAGE_NODE_IDS[index]}: ` +
              String(result.reason),
            );
          }
        });

        setErrors(warnings);

        if (incoming.length > 0) {
          setEvents(previous => {
            const merged = new Map(
              previous.map(event => [
                eventIdentity(event),
                event,
              ]),
            );

            for (const event of incoming) {
              merged.set(
                eventIdentity(event),
                event,
              );
            }

            return Array.from(merged.values())
              .sort((a, b) =>
                b.timestamp.localeCompare(
                  a.timestamp,
                ) || b.id - a.id,
              )
              .slice(0, 5000);
          });
        }

        if (warnings.length < STORAGE_NODE_IDS.length) {
          setUpdated(new Date().toISOString());
        }
      } finally {
        busy = false;
      }
    }

    void poll();

    const timer = window.setInterval(
      () => void poll(),
      3000,
    );

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [paused]);

  const categories = Array.from(
    new Set(events.map(event => event.category)),
  ).sort();

  const filtered = events.filter(event =>
    (node === "all" ||
      String(event.node_id) === node) &&
    (shard === "all" ||
      String(event.shard_id) === shard) &&
    (category === "all" ||
      event.category === category) &&
    (
      !search.trim() ||
      JSON.stringify(event)
        .toLowerCase()
        .includes(search.toLowerCase())
    ),
  );

  return (
    <section className="mt-page">
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
              : errors.length
                ? "Partial"
                : updated
                  ? "Polling"
                  : "Connecting",
          },
        ]}
      />

      <article className="ma-panel">
        <div className="mt-heading">
          <h2>Event filters</h2>

          <div className="mt-controls">
            <button
              onClick={() => setPaused(p => !p)}
            >
              {paused ? "Resume" : "Pause"}
            </button>

            <button
              onClick={() => setEvents([])}
            >
              Clear local history
            </button>
          </div>
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

        <p>
          Last successful collection:{" "}
          {formatTime(updated)}
          {" · "}
          Up to 5,000 locally retained events
        </p>

        {errors.length > 0 && (
          <details className="mt-warning">
            <summary>
              {errors.length} collection warning(s)
            </summary>

            {errors.map(error => (
              <p key={error}>{error}</p>
            ))}
          </details>
        )}
      </article>

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
                    <details>
                      <summary>Inspect</summary>

                      <pre className="mt-json">
                        {JSON.stringify(
                          event,
                          null,
                          2,
                        )}
                      </pre>
                    </details>
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
          This timeline uses bounded backend event
          histories. Events may be missed between
          polls. Metadata-group events are not
          currently exposed. Clearing local history
          does not clear backend history.
        </p>
      </article>
    </section>
  );
}

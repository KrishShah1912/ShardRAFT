import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getShardDiagnostic,
  type DataShardID,
  type DiagnosticEndpoint,
  type ShardDiagnostic,
} from "../api/multiDiagnostics";

import type {
  StorageNodeID,
} from "../api/multiRaft";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
} from "./clusterState";

import { keySlot } from "./keyRouting";

import "./multiDataDiagnostics.css";

type DiagnosticPage =
  | "database"
  | "raft-log"
  | "storage";

interface Props {
  cluster: MultiRaftClusterState;
  page: DiagnosticPage;
  onOpenConsole: () => void;
}

interface KVData {
  count: number;
  applied_index: number;
  items: {
    key: string;
    value: string;
  }[];
}

interface SnapshotData {
  exists: boolean;
  valid: boolean;
  version?: number;
  applied_index?: number;
  applied_term?: number;
  records?: number;
  dedup_records?: number;
  error?: string;
}

interface StorageData {
  engine: string;
  records: number;
  applied_index: number;
  wal: Record<string, unknown>;
  snapshot: SnapshotData;
}

const PAGE_CONFIG: Record<
  DiagnosticPage,
  {
    title: string;
    description: string;
    endpoint: DiagnosticEndpoint;
  }
> = {
  database: {
    title: "Database",
    description:
      "Inspect the local key-value state of each shard replica.",
    endpoint: "kv",
  },
  "raft-log": {
    title: "Raft Log",
    description:
      "Inspect replica-specific consensus state and log entries.",
    endpoint: "raft/log",
  },
  storage: {
    title: "Storage",
    description:
      "Inspect persistent storage, segmented WAL and snapshots.",
    endpoint: "storage",
  },
};

function displayValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function RawJSON({ data }: { data: unknown }) {
  return (
    <pre className="md-json">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

function ObjectFields({
  data,
}: {
  data: Record<string, unknown>;
}) {
  return (
    <div className="ma-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Field</th>
            <th>Value</th>
          </tr>
        </thead>

        <tbody>
          {Object.entries(data).map(([key, value]) => (
            <tr key={key}>
              <td>{key.replaceAll("_", " ")}</td>

              <td className="md-value">
                {displayValue(value)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StatGrid({
  items,
}: {
  items: {
    label: string;
    value: unknown;
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
            {displayValue(item.value)}
          </strong>
        </article>
      ))}
    </div>
  );
}

function DatabaseView({
  data,
  onOpenConsole,
}: {
  data: KVData;
  onOpenConsole: () => void;
}) {
  const [search, setSearch] = useState("");
  const [limit, setLimit] = useState(50);

  const items = useMemo(() => {
    const query = search.toLowerCase();

    return data.items.filter(item =>
      item.key.toLowerCase().includes(query) ||
      item.value.toLowerCase().includes(query),
    );
  }, [data.items, search]);

  return (
    <>
      <StatGrid
        items={[
          {
            label: "Local records",
            value: data.count,
          },
          {
            label: "Applied index",
            value: data.applied_index,
          },
          {
            label: "Matching records",
            value: items.length,
          },
        ]}
      />

      <article className="ma-panel">
        <div className="md-heading">
          <h2>Key-value records</h2>

          <button
            type="button"
            onClick={onOpenConsole}
          >
            Open Query Console
          </button>
        </div>

        <input
          className="md-input"
          placeholder="Search keys or values..."
          aria-label="Search database records"
          value={search}
          onChange={event => {
            setSearch(event.target.value);
            setLimit(50);
          }}
        />

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Key</th>
                <th>Value</th>
                <th>Hash slot</th>
              </tr>
            </thead>

            <tbody>
              {items.slice(0, limit).map(item => (
                <tr key={item.key}>
                  <td>
                    <code>{item.key}</code>
                  </td>

                  <td className="md-value">
                    {item.value}
                  </td>

                  <td>{keySlot(item.key)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {items.length === 0 && (
          <div className="md-empty">
            No matching records on this replica.
          </div>
        )}

        <div className="md-heading md-pagination">
          <span>
            Showing {Math.min(limit, items.length)}
            {" of "}
            {items.length} matching records
          </span>

          {limit < items.length && (
            <button
              type="button"
              onClick={() => setLimit(n => n + 50)}
            >
              Show 50 more
            </button>
          )}
        </div>

        <p>
          These records represent local applied state.
          Use the Query Console for linearizable reads
          and replicated mutations.
        </p>
      </article>
    </>
  );
}

function LogView({
  data,
  consensus,
}: {
  data: unknown;
  consensus: unknown;
}) {
  const [limit, setLimit] = useState(50);

  const log =
    data && typeof data === "object"
      ? data as Record<string, unknown>
      : {};

  const entries = Array.isArray(log.entries)
    ? log.entries
    : null;

  const fields = Object.fromEntries(
    Object.entries(log).filter(
      ([key]) => key !== "entries",
    ),
  );

  return (
    <>
      <article className="ma-panel">
        <h2>Replica consensus state</h2>

        {consensus &&
        typeof consensus === "object" ? (
          <ObjectFields
            data={
              consensus as Record<string, unknown>
            }
          />
        ) : (
          <p>Consensus details unavailable.</p>
        )}
      </article>

      <article className="ma-panel">
        <h2>Raft log metadata</h2>

        <ObjectFields data={fields} />
      </article>

      <article className="ma-panel">
        <h2>Raft log entries</h2>

        {entries ? (
          <>
            <p>
              {entries.length} entries returned by
              the selected replica.
            </p>

            <div className="ma-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Index</th>
                    <th>Term</th>
                    <th>Type</th>
                    <th>Entry details</th>
                  </tr>
                </thead>

                <tbody>
                  {entries.slice(0, limit).map(
                    (entry, index) => {
                      const item =
                        entry &&
                        typeof entry === "object"
                          ? entry as Record<
                              string,
                              unknown
                            >
                          : {};

                      return (
                        <tr key={index}>
                          <td>
                            {displayValue(item.index)}
                          </td>

                          <td>
                            {displayValue(item.term)}
                          </td>

                          <td>
                            {displayValue(item.type)}
                          </td>

                          <td>
                            <details>
                              <summary>
                                Inspect entry
                              </summary>

                              <RawJSON data={entry} />
                            </details>
                          </td>
                        </tr>
                      );
                    },
                  )}
                </tbody>
              </table>
            </div>

            {entries.length === 0 && (
              <div className="md-empty">
                No log entries returned.
              </div>
            )}

            {limit < entries.length && (
              <button
                type="button"
                onClick={() => setLimit(n => n + 50)}
              >
                Show 50 more entries
              </button>
            )}
          </>
        ) : (
          <RawJSON data={data} />
        )}

        <p>
          Log entries are local replica observations.
          Compacted entries may no longer be present.
        </p>
      </article>
    </>
  );
}

function StorageView({
  data,
}: {
  data: StorageData;
}) {
  const snapshot = data.snapshot;

  return (
    <>
      <StatGrid
        items={[
          {
            label: "Storage engine",
            value: data.engine,
          },
          {
            label: "Local records",
            value: data.records,
          },
          {
            label: "Applied index",
            value: data.applied_index,
          },
        ]}
      />

      <article className="ma-panel">
        <h2>Persistent storage pipeline</h2>

        <div className="md-pipeline">
          {[
            "Raft log",
            "Segmented WAL",
            "Committed entries",
            "Pebble state machine",
          ].map((label, index) => (
            <div key={label}>
              <span>{index + 1}</span>
              <strong>{label}</strong>
            </div>
          ))}
        </div>

        <p>
          Configured persistence architecture.
          This is not an individual write trace.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Write-ahead log statistics</h2>

        <ObjectFields data={data.wal} />
      </article>

      <article className="ma-panel">
        <h2>Snapshot and recovery state</h2>

        <div
          className={
            snapshot.exists && !snapshot.valid
              ? "md-warning"
              : "md-notice"
          }
        >
          {!snapshot.exists
            ? "No persisted snapshot found."
            : snapshot.valid
              ? "Persisted snapshot passed load validation."
              : "Persisted snapshot could not be validated."}
        </div>

        <StatGrid
          items={[
            {
              label: "Snapshot version",
              value: snapshot.exists
                ? snapshot.version
                : null,
            },
            {
              label: "Snapshot applied index",
              value: snapshot.exists && snapshot.valid
                ? snapshot.applied_index ?? 0
                : null,
            },
            {
              label: "Snapshot applied term",
              value: snapshot.exists && snapshot.valid
                ? snapshot.applied_term ?? 0
                : null,
            },
            {
              label: "Snapshot records",
              value: snapshot.exists && snapshot.valid
                ? snapshot.records ?? 0
                : null,
            },
            {
              label: "Deduplication records",
              value: snapshot.exists && snapshot.valid
                ? snapshot.dedup_records ?? 0
                : null,
            },
            {
              label: "Snapshot exists",
              value: snapshot.exists ? "Yes" : "No",
            },
          ]}
        />

        {snapshot.error && (
          <p role="alert">{snapshot.error}</p>
        )}

        <p>
          Snapshot validation does not independently
          prove crash recovery or power-loss durability.
        </p>
      </article>
    </>
  );
}

function DiagnosticReader({
  page,
  node,
  shard,
  onOpenConsole,
}: {
  page: DiagnosticPage;
  node: StorageNodeID;
  shard: DataShardID;
  onOpenConsole: () => void;
}) {
  const [response, setResponse] =
    useState<ShardDiagnostic | null>(null);

  const [consensus, setConsensus] =
    useState<unknown>(null);

  const [error, setError] =
    useState<string | null>(null);

  const [consensusError, setConsensusError] =
    useState<string | null>(null);

  const [loading, setLoading] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshCount, setRefreshCount] = useState(0);

  const [updatedAt, setUpdatedAt] =
    useState<string | null>(null);

  const endpoint = PAGE_CONFIG[page].endpoint;

  useEffect(() => {
    let active = true;
    let inFlight = false;

    async function refresh() {
      if (!active || inFlight) return;

      inFlight = true;
      setLoading(true);

      try {
        const result = await getShardDiagnostic(
          node,
          shard,
          endpoint,
        );

        if (!active) return;

        setResponse(result);
        setError(null);
        setUpdatedAt(new Date().toISOString());

        if (page === "raft-log") {
          try {
            const state = await getShardDiagnostic(
              node,
              shard,
              "raft",
            );

            if (active) {
              setConsensus(state.data);
              setConsensusError(null);
            }
          } catch (failure) {
            if (active) {
              setConsensus(null);
              setConsensusError(String(failure));
            }
          }
        }
      } catch (failure) {
        if (active) {
          setResponse(null);
          setConsensus(null);
          setError(
            failure instanceof Error
              ? failure.message
              : String(failure),
          );
        }
      } finally {
        inFlight = false;

        if (active) setLoading(false);
      }
    }

    void refresh();

    const timer = autoRefresh
      ? window.setInterval(
          () => void refresh(),
          2500,
        )
      : undefined;

    return () => {
      active = false;

      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [
    node,
    shard,
    page,
    endpoint,
    autoRefresh,
    refreshCount,
  ]);

  return (
    <>
      <article className="ma-panel">
        <div className="md-heading">
          <div>
            <strong>
              Node {node} · Shard {shard}
            </strong>

            <p>
              Local diagnostic endpoint: {endpoint}
            </p>
          </div>

          <div className="md-controls">
            <label>
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={event =>
                  setAutoRefresh(event.target.checked)
                }
              />

              Auto-refresh
            </label>

            <button
              type="button"
              disabled={loading}
              onClick={() =>
                setRefreshCount(n => n + 1)
              }
            >
              Refresh
            </button>
          </div>
        </div>

        <small>
          {loading
            ? "Refreshing diagnostics..."
            : updatedAt
              ? `Last successful observation: ${
                  new Date(updatedAt)
                    .toLocaleTimeString()
                }`
              : "Awaiting diagnostics"}
        </small>
      </article>

      {error && (
        <div className="md-warning" role="alert">
          Diagnostic request failed: {error}
        </div>
      )}

      {!response && loading && !error && (
        <article className="ma-panel md-empty">
          Loading replica diagnostics...
        </article>
      )}

      {response && (
        <>
          {page === "database" && (
            <DatabaseView
              data={response.data as KVData}
              onOpenConsole={onOpenConsole}
            />
          )}

          {page === "raft-log" && (
            <>
              {consensusError && (
                <div className="md-warning">
                  {consensusError}
                </div>
              )}

              <LogView
                data={response.data}
                consensus={consensus}
              />
            </>
          )}

          {page === "storage" && (
            <StorageView
              data={response.data as StorageData}
            />
          )}

          <article className="ma-panel">
            <details>
              <summary>Raw diagnostic response</summary>

              <RawJSON data={response} />
            </details>
          </article>
        </>
      )}
    </>
  );
}

export function MultiDataDiagnostics({
  cluster,
  page,
  onOpenConsole,
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

  const observedShard = cluster.shards.find(
    item => item.shardId === shard,
  );

  const config = PAGE_CONFIG[page];

  return (
    <section className="md-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            REPLICA DIAGNOSTICS
          </div>

          <h1>{config.title}</h1>
          <p>{config.description}</p>
        </div>
      </header>

      <article className="ma-panel">
        <h2>Select shard and replica</h2>

        <div className="md-selectors">
          <label>
            Data shard

            <select
              value={shard}
              onChange={event =>
                setShard(
                  Number(
                    event.target.value,
                  ) as DataShardID,
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
            Storage replica

            <select
              value={node}
              onChange={event =>
                setPreferredNode(
                  Number(
                    event.target.value,
                  ) as StorageNodeID,
                )
              }
            >
              {replicas.map(id => {
                const observed =
                  observedShard?.replicas.find(
                    item => item.nodeId === id,
                  );

                return (
                  <option key={id} value={id}>
                    Node {id}
                    {" — "}
                    {observed?.reachable
                      ? observed.role
                      : "Not observed"}
                  </option>
                );
              })}
            </select>
          </label>
        </div>

        <p>
          Only replicas configured to host the
          selected shard are listed.
        </p>
      </article>

      <DiagnosticReader
        key={`${page}-${shard}-${node}`}
        page={page}
        node={node}
        shard={shard}
        onOpenConsole={onOpenConsole}
      />
    </section>
  );
}

import {
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from "react";

import {
  executeGatewayCommand,
  GatewayRequestError,
  type GatewayCommand,
  type GatewayResult,
  type GatewayAttempt,
  type ReadMode,
} from "../api/multiGateway";

import type { GatewayID } from "../api/multiRaft";
import type { KVOperation } from "../api/routing";
import type { MultiRaftClusterState } from "./useMultiRaftCluster";

import { previewKeyRouting } from "./keyRouting";

import {
  loadPendingMutation,
  savePendingMutation,
  clearPendingMutation,
} from "./pendingMutation";

import "./multiQuery.css";

interface Props {
  cluster: MultiRaftClusterState;
}

interface ExecutionRecord {
  id: string;
  timestamp: string;
  command: GatewayCommand;
  status: "success" | "failed" | "unknown";
  result?: GatewayResult;
  error?: string;
  attempts: GatewayAttempt[];
}

const OPERATIONS: KVOperation[] = [
  "GET",
  "PUT",
  "DELETE",
  "CAS",
];

function newIdentity(): string {
  return crypto.randomUUID();
}

export function MultiQueryConsole({ cluster }: Props) {
  const [restored] = useState(() => {
    try {
      return {
        command: loadPendingMutation(),
        error: "",
      };
    } catch (error) {
      return {
        command: null,
        error: String(error),
      };
    }
  });

  const [storageError, setStorageError] =
    useState(restored.error);

  const [operation, setOperation] =
    useState<KVOperation>(
      restored.command?.operation ?? "GET",
    );

  const [key, setKey] = useState(
    restored.command?.key ?? "demo:user",
  );
  const [value, setValue] = useState(
    restored.command?.value ?? "",
  );
  const [expected, setExpected] = useState(
    restored.command?.expected ?? "",
  );

  const [gateway, setGateway] =
    useState<GatewayID>(1);

  const [readMode, setReadMode] =
    useState<ReadMode>(
      restored.command?.readMode ?? "linearizable",
    );

  const [automaticFailover, setAutomaticFailover] =
    useState(true);

  const [clientId] = useState(
    () => restored.command?.clientId
      ?? `console-${newIdentity()}`,
  );

  const [requestId, setRequestId] =
    useState(
      () => restored.command?.requestId
        ?? newIdentity(),
    );

  const [busy, setBusy] = useState(false);
  const executing = useRef(false);

  const [history, setHistory] =
    useState<ExecutionRecord[]>([]);

  const [selected, setSelected] =
    useState<ExecutionRecord | null>(null);

  const [pending, setPending] =
    useState<GatewayCommand | null>(
      restored.command,
    );

  const routing = useMemo(() => {
    if (!cluster.shardMap || !key.trim()) {
      return null;
    }

    return previewKeyRouting(
      key,
      cluster.shardMap.shard_map,
    );
  }, [key, cluster.shardMap]);

  const observedShard = cluster.shards.find(
    shard => shard.shardId === routing?.shardId,
  );

  const mutation = operation !== "GET";
  const locked =
    busy || pending !== null || !!storageError;

  function resolvePending() {
    clearPendingMutation();
    setPending(null);
    setRequestId(newIdentity());
  }

  async function execute(command: GatewayCommand) {
    if (executing.current || storageError) return;

    const isMutation = command.operation !== "GET";
    const wasPending = pending !== null;

    if (isMutation) {
      try {
        savePendingMutation(command);
        setPending(command);
      } catch (error) {
        setStorageError(
          "Mutation was not dispatched: " + String(error),
        );
        return;
      }
    }

    executing.current = true;
    setBusy(true);

    let record: ExecutionRecord;

    try {
      const result = await executeGatewayCommand(
        command,
        gateway,
        automaticFailover,
      );

      record = {
        id: newIdentity(),
        timestamp: new Date().toISOString(),
        command,
        status: "success",
        result,
        attempts: result.attempts,
      };

      if (isMutation) {
        resolvePending();
      } else {
        setRequestId(newIdentity());
      }
    } catch (error) {
      const gatewayError =
        error instanceof GatewayRequestError
          ? error
          : null;

      const unknown =
        isMutation && (
          gatewayError === null ||
          gatewayError.code === "outcome_unknown" ||
          wasPending
        );

      record = {
        id: newIdentity(),
        timestamp: new Date().toISOString(),
        command,
        status: unknown ? "unknown" : "failed",
        error:
          error instanceof Error
            ? error.message
            : String(error),
        attempts: gatewayError?.attempts ?? [],
      };

      if (unknown) {
        setPending(command);
      } else if (isMutation) {
        try {
          resolvePending();
        } catch (storageFailure) {
          setPending(command);
          setStorageError(String(storageFailure));
        }
      } else {
        setRequestId(newIdentity());
      }
    } finally {
      executing.current = false;
      setBusy(false);
    }

    setSelected(record);

    setHistory(previous =>
      [record, ...previous].slice(0, 50),
    );
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (locked || !key.trim()) return;

    void execute({
      operation,
      key,
      value,
      expected,
      clientId,
      requestId,
      readMode,
    });
  }

  function abandonPending() {
    if (busy || !pending) return;

    const confirmed = window.confirm(
      "The original mutation may already have committed. " +
      "Abandoning this retry does not undo it. Continue?",
    );

    if (!confirmed) return;

    try {
      resolvePending();
      setStorageError("");
    } catch (error) {
      setStorageError(String(error));
    }
  }

  return (
    <section className="mq-console">
      <header className="mq-heading">
        <div>
          <div className="mq-eyebrow">
            DISTRIBUTED KEY-VALUE STORE
          </div>

          <h1>Query Console</h1>

          <p>
            Execute requests through the Multi-Raft
            HTTP gateways.
          </p>
        </div>

        <span className="mq-tag">
          Multi-Raft
        </span>
      </header>

      <div className="mq-grid">
        <article className="mq-panel">
          <h2>Request builder</h2>

          {storageError && (
            <div className="mq-warning" role="alert">
              <strong>Mutation storage unavailable</strong>
              <p>{storageError}</p>
              <p>
                Requests are blocked to avoid losing
                an unresolved mutation identity.
                Reload after resolving the storage issue.
              </p>
            </div>
          )}

          <form onSubmit={submit}>
            <div className="mq-operations">
              {OPERATIONS.map(item => (
                <button
                  key={item}
                  type="button"
                  disabled={locked}
                  className={
                    operation === item
                      ? "mq-operation mq-active"
                      : "mq-operation"
                  }
                  onClick={() => setOperation(item)}
                >
                  {item}
                </button>
              ))}
            </div>

            <label>
              Database key

              <input
                value={key}
                disabled={locked}
                onChange={event =>
                  setKey(event.target.value)
                }
                required
              />
            </label>

            {operation === "CAS" && (
              <label>
                Expected value

                <textarea
                  value={expected}
                  disabled={locked}
                  onChange={event =>
                    setExpected(event.target.value)
                  }
                  rows={3}
                />
              </label>
            )}

            {(operation === "PUT" ||
              operation === "CAS") && (
              <label>
                New value

                <textarea
                  value={value}
                  disabled={locked}
                  onChange={event =>
                    setValue(event.target.value)
                  }
                  rows={4}
                />
              </label>
            )}

            <label>
              Preferred gateway

              <select
                value={gateway}
                disabled={busy}
                onChange={event =>
                  setGateway(
                    Number(event.target.value) as GatewayID,
                  )
                }
              >
                <option value={1}>
                  Gateway 1 — Port 18091
                </option>

                <option value={2}>
                  Gateway 2 — Port 18092
                </option>
              </select>
            </label>

            <label className="mq-checkbox">
              <input
                type="checkbox"
                checked={automaticFailover}
                disabled={busy}
                onChange={event =>
                  setAutomaticFailover(
                    event.target.checked,
                  )
                }
              />

              Automatically try the other gateway
            </label>

            {operation === "GET" && (
              <label>
                Read consistency

                <select
                  value={readMode}
                  disabled={locked}
                  onChange={event =>
                    setReadMode(
                      event.target.value as ReadMode,
                    )
                  }
                >
                  <option value="linearizable">
                    Linearizable
                  </option>

                  <option value="local">
                    Local — potentially stale
                  </option>
                </select>
              </label>
            )}

            {mutation && (
              <div className="mq-identity">
                <small>Client identity</small>
                <code>{clientId}</code>

                <small>Request identity</small>
                <code>{requestId}</code>
              </div>
            )}

            <button
              type="submit"
              className="mq-primary"
              disabled={locked || !key.trim()}
            >
              {busy
                ? "Executing request..."
                : `Execute ${operation}`}
            </button>
          </form>

          {pending && (
            <div className="mq-warning" role="alert">
              <strong>Mutation outcome unknown</strong>

              <p>
                The mutation may have committed.
                Retry with the original request identity.
                The pending payload is preserved across
                reloads in this browser tab.
              </p>

              <p>
                <code>
                  {pending.operation} {pending.key}
                </code>
              </p>

              <p>
                Request ID: <code>{pending.requestId}</code>
              </p>

              <button
                type="button"
                disabled={busy || !!storageError}
                onClick={() => void execute(pending)}
              >
                Retry original request
              </button>

              <button
                type="button"
                disabled={busy}
                onClick={abandonPending}
              >
                Abandon retry
              </button>
            </div>
          )}
        </article>

        <article className="mq-panel">
          <h2>Routing preview</h2>

          <p className="mq-muted">
            Predicted destination using the latest
            observed metadata map.
          </p>

          {routing ? (
            <>
              <div className="mq-stats">
                <div>
                  <small>CRC32 hash</small>
                  <strong>{routing.hash}</strong>
                </div>

                <div>
                  <small>Hash slot</small>
                  <strong>{routing.slot}</strong>
                </div>

                <div>
                  <small>Destination</small>
                  <strong>
                    {routing.shardId
                      ? `Shard ${routing.shardId}`
                      : "Unknown"}
                  </strong>
                </div>

                <div>
                  <small>Metadata epoch</small>
                  <strong>{routing.epoch}</strong>
                </div>
              </div>

              <div className="mq-slotbar">
                {[1, 2, 3, 4].map(shard => (
                  <div
                    key={shard}
                    className={
                      routing.shardId === shard
                        ? "mq-slot mq-slot-active"
                        : "mq-slot"
                    }
                  >
                    S{shard}
                  </div>
                ))}
              </div>

              <h3>Replica placement</h3>

              <div className="mq-replicas">
                {routing.replicaIds.map(node => {
                  const replica =
                    observedShard?.replicas.find(
                      item => item.nodeId === node,
                    );

                  return (
                    <div key={node}>
                      <strong>Node {node}</strong>

                      <small>
                        {replica?.reachable
                          ? replica.role
                          : "Not observed"}
                      </small>
                    </div>
                  );
                })}
              </div>

              <p>
                Observed leader:{" "}
                {observedShard?.leaderId
                  ? `Node ${observedShard.leaderId}`
                  : "Unknown"}
              </p>

              <p>
                Metadata leader hint:{" "}
                {routing.leaderHint
                  ? `Node ${routing.leaderHint}`
                  : "Unknown"}
              </p>

              <p className="mq-muted">
                This preview is not an actual
                gateway execution trace.
              </p>
            </>
          ) : (
            <div className="mq-empty">
              {cluster.loading
                ? "Loading metadata..."
                : "Enter a key with an available shard map."}
            </div>
          )}
        </article>
      </div>

      <article className="mq-panel">
        <h2>Execution result</h2>

        {selected ? (
          <>
            <div className="mq-result-header">
              <span
                className={`mq-status mq-${selected.status}`}
              >
                {selected.status.toUpperCase()}
              </span>

              <code>
                {selected.command.operation}{" "}
                {selected.command.key}
              </code>
            </div>

            {selected.result && (
              <div className="mq-stats">
                <div>
                  <small>Responding gateway</small>
                  <strong>
                    Gateway {selected.result.gatewayId}
                  </strong>
                </div>

                <div>
                  <small>HTTP status</small>
                  <strong>{selected.result.status}</strong>
                </div>

                <div>
                  <small>Total duration</small>
                  <strong>
                    {selected.result.durationMs.toFixed(2)} ms
                  </strong>
                </div>

                <div>
                  <small>Attempts</small>
                  <strong>
                    {selected.attempts.length}
                  </strong>
                </div>
              </div>
            )}

            {selected.error && (
              <p role="alert">{selected.error}</p>
            )}

            <pre className="mq-json">
              {JSON.stringify(
                selected.result?.result ?? {
                  error: selected.error,
                },
                null,
                2,
              )}
            </pre>

            <h3>Gateway attempts</h3>

            <div className="mq-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Gateway</th>
                    <th>HTTP status</th>
                    <th>Duration</th>
                    <th>Outcome</th>
                  </tr>
                </thead>

                <tbody>
                  {selected.attempts.map((attempt, index) => (
                    <tr key={index}>
                      <td>Gateway {attempt.gatewayId}</td>
                      <td>{attempt.status ?? "—"}</td>
                      <td>
                        {attempt.durationMs.toFixed(2)} ms
                      </td>
                      <td>{attempt.outcome}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <div className="mq-empty">
            Execute a request to inspect its response.
          </div>
        )}
      </article>

      <article className="mq-panel">
        <div className="mq-result-header">
          <h2>Request history</h2>

          <button
            type="button"
            disabled={busy || pending !== null}
            onClick={() => {
              setHistory([]);
              setSelected(null);
            }}
          >
            Clear history
          </button>
        </div>

        {history.length === 0 ? (
          <div className="mq-empty">
            No requests executed in this session.
          </div>
        ) : (
          <div className="mq-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Operation</th>
                  <th>Key</th>
                  <th>Outcome</th>
                  <th>Gateway</th>
                  <th>Inspect</th>
                </tr>
              </thead>

              <tbody>
                {history.map(record => (
                  <tr key={record.id}>
                    <td>
                      {new Date(
                        record.timestamp,
                      ).toLocaleTimeString()}
                    </td>

                    <td>{record.command.operation}</td>
                    <td>{record.command.key}</td>
                    <td>{record.status}</td>
                    <td>
                      {record.result?.gatewayId ?? "—"}
                    </td>

                    <td>
                      <button
                        type="button"
                        onClick={() => setSelected(record)}
                      >
                        View
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </article>
    </section>
  );
}

export default MultiQueryConsole;

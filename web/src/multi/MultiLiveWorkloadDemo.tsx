import {
  useEffect,
  useRef,
  useState,
} from "react";

import {
  Activity,
  Play,
  Square,
  RotateCcw,
  CheckCircle2,
  AlertTriangle,
  Clock,
  LoaderCircle,
  XCircle,
  MinusCircle,
} from "lucide-react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  DataShardID,
} from "../api/multiDiagnostics";

import {
  createDemoWorkload,
  DEFAULT_DEMO_CONFIG,
  DEMO_OPERATION_COUNTS,
  DEMO_INTERVALS,
} from "./demoWorkload";

import {
  startDemoExecution,
  summarizeDemoExecution,
  type DemoExecutionHandle,
  type DemoExecutionState,
  type DemoOperationStatus,
} from "./demoExecution";

import "./multiLiveWorkloadDemo.css";

interface Props {
  cluster: MultiRaftClusterState;
  selectedShard: DataShardID;
  onShardChange: (shardId: DataShardID) => void;
  onRunningChange: (running: boolean) => void;
}

const STATUS_ICONS = {
  pending: Clock,
  running: LoaderCircle,
  success: CheckCircle2,
  failed: XCircle,
  unknown: AlertTriangle,
  skipped: MinusCircle,
};

function OperationIcon({
  status,
}: {
  status: DemoOperationStatus;
}) {
  const Icon = STATUS_ICONS[status];

  return (
    <Icon
      size={17}
      className={
        status === "running"
          ? "mwd-spin"
          : undefined
      }
    />
  );
}

export function MultiLiveWorkloadDemo({
  cluster,
  selectedShard: shardId,
  onShardChange,
  onRunningChange,
}: Props) {

  const [count, setCount] = useState(
    DEFAULT_DEMO_CONFIG.count,
  );

  const [intervalMs, setIntervalMs] = useState(
    DEFAULT_DEMO_CONFIG.intervalMs,
  );

  const [execution, setExecution] =
    useState<DemoExecutionState | null>(null);

  const [error, setError] =
    useState<string | null>(null);

  const [busy, setBusy] = useState(false);

  const handleRef =
    useRef<DemoExecutionHandle | null>(null);

  const mountedRef = useRef(false);

  const clusterRef = useRef(cluster);

  useEffect(() => {
    clusterRef.current = cluster;
  }, [cluster]);

  useEffect(() => {
    mountedRef.current = true;

    return () => {
      mountedRef.current = false;
      handleRef.current?.stop();
    };
  }, []);

  const recordedShard =
    execution?.operations[0]?.operation.shardId;

  const map = cluster.shardMap?.shard_map;

  const shard = cluster.shards.find(
    item => item.shardId === shardId,
  );

  const ready =
    Boolean(map) &&
    Boolean(shard?.majorityReachable) &&
    shard?.leaderId != null;

  const summary = execution
    ? summarizeDemoExecution(execution)
    : {
        total: count,
        successful: 0,
        failed: 0,
        unknown: 0,
        skipped: 0,
        pending: count,
        running: 0,
        completed: 0,
        settled: 0,
      };

  const progress = summary.total
    ? summary.settled / summary.total * 100
    : 0;

  const status = execution?.status ?? "idle";

  const hasUnknown = summary.unknown > 0;

  async function start() {
    if (handleRef.current || !ready || execution) {
      return;
    }

    setError(null);

    try {
      const currentMap =
        clusterRef.current.shardMap?.shard_map;

      if (!currentMap) {
        throw new Error(
          "Metadata map is unavailable.",
        );
      }

      const workload = createDemoWorkload(
        { shardId, count, intervalMs },
        currentMap,
      );

      setBusy(true);
      onRunningChange(true);

      const handle = startDemoExecution(
        workload,
        {
          getRoutingMap: () =>
            clusterRef.current.shardMap
              ?.shard_map ?? null,

          onUpdate: state => {
            if (mountedRef.current) {
              setExecution(state);
            }
          },

          preferredGateway: 1,
          automaticFailover: true,
        },
      );

      handleRef.current = handle;

      await handle.done;
    } catch (failure) {
      if (mountedRef.current) {
        setError(
          failure instanceof Error
            ? failure.message
            : String(failure),
        );
      }
    } finally {
      handleRef.current = null;

      if (mountedRef.current) {
        setBusy(false);
        onRunningChange(false);
      }
    }
  }

  function stop() {
    handleRef.current?.stop();
  }

  function reset() {
    if (handleRef.current || busy) return;

    if (
      hasUnknown &&
      !window.confirm(
        "An operation has an uncertain outcome. " +
        "Reset will discard its local execution history. " +
        "Inspect or record its mutation identity first. " +
        "Continue?",
      )
    ) {
      return;
    }

    setExecution(null);
    setError(null);
  }

  const metrics = [
    {
      label: "Successful",
      value: summary.successful,
      tone: "success",
    },
    {
      label: "Failed",
      value: summary.failed,
      tone: "failed",
    },
    {
      label: "Unknown",
      value: summary.unknown,
      tone: "unknown",
    },
    {
      label: "Running",
      value: summary.running,
      tone: "running",
    },
    {
      label: "Pending",
      value: summary.pending,
      tone: "pending",
    },
    {
      label: "Skipped",
      value: summary.skipped,
      tone: "skipped",
    },
  ];

  return (
    <article className="ma-panel mwd-panel">
      <header className="mwd-header">
        <div className="mwd-heading">
          <div className="mwd-heading-icon">
            <Activity size={24} />
          </div>

          <div>
            <div className="mwd-eyebrow">
              LIVE DATABASE EXPERIMENT
            </div>

            <h2>Live Workload Demonstrator</h2>

            <p>
              Execute real PUT operations and observe
              distributed consensus.
            </p>
          </div>
        </div>

        <span className={`mwd-status ${status}`}>
          <i />
          {status === "idle"
            ? ready ? "Ready" : "Unavailable"
            : status}
        </span>
      </header>

      <div className="mwd-config">
        <label>
          Target shard

          <select
            value={shardId}
            disabled={busy || Boolean(execution)}
            onChange={event => {
              const next = Number(
                event.target.value,
              ) as DataShardID;

              onShardChange(next);
            }}
          >
            {[1, 2, 3, 4].map(id => (
              <option key={id} value={id}>
                Shard {id}
              </option>
            ))}
          </select>
        </label>

        <label>
          Operations

          <select
            value={count}
            disabled={busy || Boolean(execution)}
            onChange={event =>
              setCount(Number(event.target.value))
            }
          >
            {DEMO_OPERATION_COUNTS.map(value => (
              <option key={value} value={value}>
                {value} PUTs
              </option>
            ))}
          </select>
        </label>

        <label>
          Delay between writes

          <select
            value={intervalMs}
            disabled={busy || Boolean(execution)}
            onChange={event =>
              setIntervalMs(
                Number(event.target.value),
              )
            }
          >
            {DEMO_INTERVALS.map(value => (
              <option key={value} value={value}>
                {value / 1000} seconds
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="mwd-context">
        <span>
          Target: <strong>Shard {shardId}</strong>
          {recordedShard != null && (
            <> · Recorded run: Shard {recordedShard}</>
          )}
        </span>

        <span>
          Observed leader:{" "}
          <strong>
            {shard?.leaderId != null
              ? `Node ${shard.leaderId}`
              : "Unavailable"}
          </strong>
        </span>

        <span>
          Metadata epoch:{" "}
          <strong>{map?.epoch ?? "Unknown"}</strong>
        </span>

        <span>
          Gateway 1 · Automatic failover
        </span>
      </div>

      <div className="mwd-actions">
        <button
          type="button"
          className="mwd-start"
          disabled={busy || !ready || Boolean(execution)}
          onClick={() => void start()}
        >
          {busy
            ? <LoaderCircle
                size={16}
                className="mwd-spin"
              />
            : <Play size={16} />}

          {busy
            ? "Demonstration running"
            : "Start demonstration"}
        </button>

        <button
          type="button"
          disabled={!busy || status === "stopping"}
          onClick={stop}
        >
          <Square size={15} />

          {status === "stopping"
            ? "Stopping..."
            : "Stop"}
        </button>

        <button
          type="button"
          disabled={busy || !execution}
          onClick={reset}
        >
          <RotateCcw size={15} />
          Reset
        </button>
      </div>

      {!ready && !busy && (
        <div className="mwd-warning">
          <AlertTriangle size={16} />

          Waiting for an observed leader,
          reachable majority and metadata map.
        </div>
      )}

      {(error || execution?.error) && (
        <div className="mwd-warning">
          <AlertTriangle size={16} />

          {error ?? execution?.error}
        </div>
      )}

      <div className="mwd-metrics">
        {metrics.map(metric => (
          <div
            key={metric.label}
            className={`mwd-metric ${metric.tone}`}
          >
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </div>
        ))}
      </div>

      <div className="mwd-progress-header">
        <span>Workload progress</span>

        <strong>
          {summary.settled} / {summary.total} settled
        </strong>
      </div>

      <div
        className="mwd-progress"
        role="progressbar"
        aria-label="Workload progress"
        aria-valuenow={summary.settled}
        aria-valuemin={0}
        aria-valuemax={summary.total}
      >
        <div style={{ width: `${progress}%` }} />
      </div>

      <div className="mwd-operation-grid">
        {Array.from(
          { length: summary.total },
          (_, index) => {
            const result =
              execution?.operations[index];

            const operationStatus =
              result?.status ?? "pending";

            return (
              <div
                key={index}
                className={
                  `mwd-operation ${operationStatus}`
                }
                title={
                  result
                    ? `Slot ${result.operation.slot}`
                    : "Pending operation"
                }
              >
                <OperationIcon
                  status={operationStatus}
                />

                <strong>PUT {index + 1}</strong>

                <span>{operationStatus}</span>
              </div>
            );
          },
        )}
      </div>

      <div className="mwd-timeline-header">
        <h3>Operation timeline</h3>

        {execution?.startedAt != null && (
          <span>
            Started{" "}
            {new Date(
              execution.startedAt,
            ).toLocaleTimeString()}
          </span>
        )}
      </div>

      {execution ? (
        <div className="mwd-table-wrap">
          <table className="mwd-table">
            <thead>
              <tr>
                <th>Operation</th>
                <th>Key</th>
                <th>Slot</th>
                <th>Status</th>
                <th>Gateway</th>
                <th>Duration</th>
              </tr>
            </thead>

            <tbody>
              {execution.operations.map(result => (
                <tr key={result.operation.sequence}>
                  <td>
                    PUT {result.operation.sequence}
                  </td>

                  <td>
                    <details>
                      <summary
                        title={
                          result.operation.command.key
                        }
                      >
                        {result.operation.command.key}
                      </summary>

                      <pre>
                        {JSON.stringify({
                          command:
                            result.operation.command,
                          error: result.error,
                          response: result.response,
                        }, null, 2)}
                      </pre>
                    </details>
                  </td>

                  <td>
                    {result.operation.slot
                      .toLocaleString()}
                  </td>

                  <td>
                    <span
                      className={
                        `mwd-result ${result.status}`
                      }
                    >
                      <OperationIcon
                        status={result.status}
                      />

                      {result.status}
                    </span>
                  </td>

                  <td>
                    {result.gatewayId != null
                      ? `GW ${result.gatewayId}`
                      : "—"}
                  </td>

                  <td>
                    {result.durationMs != null
                      ? `${result.durationMs.toFixed(1)} ms`
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="mwd-empty">
          <Activity size={26} />

          <strong>Ready for a live demonstration</strong>

          <span>
            Select a shard and start the workload.
          </span>
        </div>
      )}

      <footer className="mwd-footer">
        Real writes are persisted in the database.
        Stop allows an in-flight request to settle.
        Reset clears local history only.
        Leaving this component stops future writes.
      </footer>
    </article>
  );
}

export default MultiLiveWorkloadDemo;

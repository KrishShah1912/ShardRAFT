import { useEffect, useState } from "react";

import {
  Database,
  HardDrive,
  Layers,
  FileStack,
  ShieldCheck,
  ArrowRight,
  Activity,
  RefreshCw,
} from "lucide-react";

import {
  getShardDiagnostic,
  type DataShardID,
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

import "./multiArchitectureStorage.css";

interface StorageData {
  engine: string;
  records: number;
  applied_index: number;
  wal: {
    active_segment: number;
    active_size: number;
    max_segment_size: number;
    recovered: boolean;
    closed: boolean;
    unusable: boolean;
  };
  snapshot: {
    exists: boolean;
    valid: boolean;
    applied_index?: number;
    applied_term?: number;
    records?: number;
    error?: string;
  };
}

interface Props {
  cluster: MultiRaftClusterState;
}

function formatBytes(value?: number) {
  if (value === undefined) return "—";

  return `${(value / 1048576).toFixed(2)} MiB`;
}

export function MultiArchitectureStorage({
  cluster,
}: Props) {
  const [shard, setShard] =
    useState<DataShardID>(1);

  const [preferredNode, setPreferredNode] =
    useState<StorageNodeID>(1);

  const replicas = SHARD_PLACEMENT[shard];

  const node = replicas.includes(preferredNode)
    ? preferredNode
    : replicas[0];

  const [observation, setObservation] =
    useState<{
      node: StorageNodeID;
      shard: DataShardID;
      data: StorageData;
      timestamp: string;
    } | null>(null);

  const [error, setError] =
    useState<string | null>(null);

  const [animate, setAnimate] = useState(true);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    let active = true;
    let inFlight = false;

    setObservation(null);
    setError(null);

    async function load() {
      if (!active || inFlight) return;

      inFlight = true;

      try {
        const result =
          await getShardDiagnostic<StorageData>(
            node,
            shard,
            "storage",
          );

        if (active) {
          setObservation({
            node,
            shard,
            data: result.data,
            timestamp: new Date().toISOString(),
          });

          setError(null);
        }
      } catch (failure) {
        if (active) {
          setObservation(null);
          setError(String(failure));
        }
      } finally {
        inFlight = false;
      }
    }

    void load();

    const timer = window.setInterval(
      () => void load(),
      2500,
    );

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [node, shard, refresh]);

  const current =
    observation?.node === node &&
    observation.shard === shard
      ? observation
      : null;

  const data = current?.data;
  const wal = data?.wal;
  const snapshot = data?.snapshot;

  const group = cluster.shards.find(
    item => item.shardId === shard,
  );

  const replica = group?.replicas.find(
    item => item.nodeId === node,
  );

  const utilization =
    wal && wal.max_segment_size > 0
      ? 100 * wal.active_size /
        wal.max_segment_size
      : null;

  const walHealthy =
    wal &&
    wal.recovered &&
    !wal.closed &&
    !wal.unusable;

  const snapshotStatus = !snapshot
    ? "Awaiting observation"
    : !snapshot.exists
      ? "No persisted snapshot"
      : snapshot.valid
        ? "Validated"
        : "Validation failed";

  const stages = [
    {
      label: "Raft Log",
      detail: "Consensus entries",
      value: replica?.commitIndex ?? "—",
      metric: "Observed commit index",
      icon: Layers,
      color: "#60a5fa",
    },
    {
      label: "Segmented WAL",
      detail: "Persistent log records",
      value: wal?.active_segment ?? "—",
      metric: "Active segment",
      icon: FileStack,
      color: "#a78bfa",
    },
    {
      label: "Committed Entries",
      detail: "Consensus boundary",
      value: replica?.commitIndex ?? "—",
      metric: "Observed commit index",
      icon: ShieldCheck,
      color: "#2dd4bf",
    },
    {
      label: "Pebble State Machine",
      detail: "Local applied state",
      value: data?.applied_index ?? "—",
      metric: "Applied index",
      icon: Database,
      color: "#34d399",
    },
  ];

  return (
    <article className="ma-panel mas-panel">
      <header className="mas-header">
        <div>
          <div className="mas-eyebrow">
            PERSISTENCE & RECOVERY
          </div>
          <h2>Live Persistent Storage Engine</h2>
          <p>
            Replica-level WAL, state machine and
            snapshot observations.
          </p>
        </div>

        <div className="mas-controls">
          <label>
            <input
              type="checkbox"
              checked={animate}
              onChange={event =>
                setAnimate(event.target.checked)
              }
            />
            Animate architecture
          </label>

          <button
            type="button"
            onClick={() => setRefresh(n => n + 1)}
          >
            <RefreshCw size={14} />
            Refresh
          </button>
        </div>
      </header>

      <div className="mas-selectors">
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
          Storage replica
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
              </option>
            ))}
          </select>
        </label>

        <div className="mas-observation">
          <Activity size={16} />
          <div>
            <strong>
              {error
                ? "Diagnostic unavailable"
                : current
                  ? "Live observation"
                  : "Awaiting diagnostics"}
            </strong>
            <small>
              {current
                ? new Date(current.timestamp)
                    .toLocaleTimeString()
                : "Replica-local storage"}
            </small>
          </div>
        </div>
      </div>

      {error && (
        <div className="mas-error" role="alert">
          {error}
        </div>
      )}

      <div
        className={
          `mas-canvas ${animate ? "mas-animated" : ""}`
        }
      >
        <div className="mas-canvas-label">
          NODE {node} / SHARD {shard} /
          CONFIGURED STORAGE FLOW
        </div>

        <div className="mas-pipeline">
          {stages.map((stage, index) => {
            const Icon = stage.icon;

            return (
              <div
                className="mas-stage-wrap"
                key={stage.label}
              >
                <div
                  className="mas-stage"
                  style={{ borderColor: stage.color }}
                >
                  <div
                    className="mas-stage-heading"
                    style={{ color: stage.color }}
                  >
                    <Icon size={23} />
                    <span>0{index + 1}</span>
                  </div>

                  <strong>{stage.label}</strong>
                  <small>{stage.detail}</small>

                  <div
                    className="mas-stage-value"
                    style={{ color: stage.color }}
                  >
                    {stage.value}
                  </div>

                  <small>{stage.metric}</small>
                </div>

                {index < stages.length - 1 && (
                  <div className="mas-connector">
                    <ArrowRight size={20} />
                  </div>
                )}
              </div>
            );
          })}
        </div>

        <div className="mas-branches">
          <span />
          <span />
          <span />
        </div>

        <div className="mas-storage-grid">
          <section className="mas-storage-card">
            <div className="mas-card-heading">
              <HardDrive size={21} />
              <strong>Segmented WAL</strong>
            </div>

            <div className="mas-disk">
              <div />
              <div />
              <div />
              <span>SEGMENT {wal?.active_segment ?? "—"}</span>
            </div>

            <div className="mas-progress-label">
              <span>Active segment utilization</span>
              <strong>
                {utilization === null
                  ? "—"
                  : `${utilization.toFixed(2)}%`}
              </strong>
            </div>

            <div className="mas-progress">
              <div
                style={{
                  width: `${
                    Math.min(
                      100,
                      Math.max(0, utilization ?? 0),
                    )
                  }%`,
                }}
              />
            </div>

            <small>
              {formatBytes(wal?.active_size)}
              {" / "}
              {formatBytes(wal?.max_segment_size)}
            </small>

            <div className="mas-status">
              {wal
                ? walHealthy
                  ? "Recovered · Open · Usable"
                  : "Check WAL diagnostic state"
                : "Awaiting WAL diagnostics"}
            </div>
          </section>

          <section className="mas-storage-card">
            <div className="mas-card-heading">
              <Database size={21} />
              <strong>State Machine</strong>
            </div>

            <div className="mas-database">
              <div />
              <div />
              <div />
              <span>
                {data?.engine.toUpperCase() ?? "—"}
              </span>
            </div>

            <div className="mas-metrics">
              <div>
                <small>Local records</small>
                <strong>
                  {data?.records.toLocaleString() ?? "—"}
                </strong>
              </div>
              <div>
                <small>Applied index</small>
                <strong>
                  {data?.applied_index ?? "—"}
                </strong>
              </div>
            </div>

            <div className="mas-status">
              Replica-local applied state
            </div>
          </section>

          <section className="mas-storage-card">
            <div className="mas-card-heading">
              <ShieldCheck size={21} />
              <strong>Snapshot & Recovery</strong>
            </div>

            <div className="mas-snapshot">
              <FileStack size={46} />
            </div>

            <strong>{snapshotStatus}</strong>

            <div className="mas-metrics">
              <div>
                <small>Snapshot index</small>
                <strong>
                  {snapshot?.exists && snapshot.valid
                    ? snapshot.applied_index ?? "—"
                    : "—"}
                </strong>
              </div>
              <div>
                <small>Snapshot records</small>
                <strong>
                  {snapshot?.exists && snapshot.valid
                    ? snapshot.records ?? "—"
                    : "—"}
                </strong>
              </div>
            </div>

            {snapshot?.error && (
              <div className="mas-error">
                {snapshot.error}
              </div>
            )}

            <div className="mas-status">
              Persisted snapshot observation
            </div>
          </section>
        </div>
      </div>

      <p className="mas-note">
        Animated connections represent configured
        architecture, not measured disk writes.
        Storage diagnostics refresh every 2.5 seconds.
        Raft and storage observations are collected
        independently.
      </p>
    </article>
  );
}

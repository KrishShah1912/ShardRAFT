import {
  CheckCircle2,
  Database,
  HardDrive,
  Layers3,
} from "lucide-react";
import type { StorageDebug } from "../types";

export function Storage({
  storage,
}: {
  storage?: StorageDebug;
}) {
  const wal = storage?.wal;
  const snapshot = storage?.snapshot;

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">INFRASTRUCTURE</div>
          <h1>Storage</h1>
          <p>
            Pebble materialized state, segmented WAL and durable
            snapshots.
          </p>
        </div>
      </header>

      <section className="metrics-grid">
        <StorageMetric
          icon={Database}
          label="ENGINE"
          value={storage?.engine ?? "—"}
        />
        <StorageMetric
          icon={Layers3}
          label="RECORDS"
          value={String(storage?.records ?? "—")}
        />
        <StorageMetric
          icon={HardDrive}
          label="WAL SEGMENT"
          value={String(wal?.active_segment ?? "—")}
        />
        <StorageMetric
          icon={CheckCircle2}
          label="STATUS"
          value={wal?.unusable ? "Unhealthy" : "Healthy"}
        />
      </section>

      <section className="two-column">
        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Segmented WAL</h2>
              <p>Current durable Raft log segment</p>
            </div>
          </div>

          <div className="consensus-list">
            <Row
              label="Active segment"
              value={String(wal?.active_segment ?? "—")}
            />
            <Row
              label="Current size"
              value={formatBytes(wal?.active_size)}
            />
            <Row
              label="Maximum size"
              value={formatBytes(wal?.max_segment_size)}
            />
            <Row
              label="Recovered"
              value={wal?.recovered ? "Yes" : "No"}
            />
          </div>
        </div>

        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Snapshot</h2>
              <p>Latest durable state snapshot</p>
            </div>
          </div>

          <div className="consensus-list">
            <Row
              label="Exists"
              value={snapshot?.exists ? "Yes" : "No"}
            />
            <Row
              label="Valid"
              value={snapshot?.valid ? "Yes" : "No"}
            />
            <Row
              label="Applied index"
              value={String(
                snapshot?.applied_index ?? "—",
              )}
            />
            <Row
              label="Records"
              value={String(snapshot?.records ?? "—")}
            />
            <Row
              label="Dedup records"
              value={String(
                snapshot?.dedup_records ?? "—",
              )}
            />
          </div>
        </div>
      </section>

      <section className="durability-flow">
        <FlowStep title="Raft Log" />
        <span>→</span>
        <FlowStep title="Segmented WAL" />
        <span>→</span>
        <FlowStep title="Commit" />
        <span>→</span>
        <FlowStep title="KV State" />
        <span>→</span>
        <FlowStep title="Pebble" />
      </section>
    </>
  );
}

function StorageMetric({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Database;
  label: string;
  value: string;
}) {
  return (
    <div className="metric-card">
      <div className="metric-icon">
        <Icon size={18} />
      </div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Row({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="consensus-row">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function FlowStep({ title }: { title: string }) {
  return <div className="flow-step">{title}</div>;
}

function formatBytes(value?: number): string {
  if (value === undefined) return "—";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KiB`;
  }

  return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}

import {
  Activity,
  CheckCircle2,
  Clock3,
  Gauge,
  GitBranch,
  Radio,
  RefreshCw,
  Server,
  Wifi,
} from "lucide-react";

import {
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  AvailabilityTimeline,
  DualMetricChart,
  MetricLineChart,
} from "../components/MetricCharts";

import {
  availabilityPercent,
  availabilitySample,
  calculateRate,
  createMetricsSnapshot,
  type AvailabilitySample,
  type MetricsSnapshot,
  type TimePoint,
} from "../telemetry/metrics";

import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  transport?: TransportStats;
  connected: boolean;
}

const MAX_POINTS = 60;

export function Metrics({
  nodes,
  events,
  transport,
  connected,
}: Props) {
  const [snapshots, setSnapshots] =
    useState<MetricsSnapshot[]>([]);

  const [availability, setAvailability] =
    useState<AvailabilitySample[]>([]);

  const previous = useRef<
    MetricsSnapshot | undefined
  >(undefined);

  useEffect(() => {
    const capture = () => {
      const snapshot =
        createMetricsSnapshot(
          nodes,
          events,
          transport,
        );

      setSnapshots((current) =>
        [...current, snapshot].slice(
          -MAX_POINTS,
        ),
      );

      setAvailability((current) =>
        [
          ...current,
          availabilitySample(nodes),
        ].slice(-MAX_POINTS),
      );
    };

    capture();

    const interval =
      window.setInterval(
        capture,
        1000,
      );

    return () =>
      window.clearInterval(interval);
  }, [nodes, events, transport]);

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const latest =
    snapshots[snapshots.length - 1];

  const rates = useMemo(() => {
    if (snapshots.length < 2) {
      return {
        messages: 0,
        deliveries: 0,
        heartbeats: 0,
      };
    }

    const current =
      snapshots[snapshots.length - 1];

    const before =
      snapshots[snapshots.length - 2];

    const elapsed =
      current.timestamp -
      before.timestamp;

    return {
      messages: calculateRate(
        current.transportSent,
        before.transportSent,
        elapsed,
      ),

      deliveries: calculateRate(
        current.transportDelivered,
        before.transportDelivered,
        elapsed,
      ),

      heartbeats: calculateRate(
        current.heartbeatEvents,
        before.heartbeatEvents,
        elapsed,
      ),
    };
  }, [snapshots]);

  useEffect(() => {
    previous.current = latest;
  }, [latest]);

  const clusterAvailability =
    availabilityPercent(
      availability,
    );

  const commitSeries = toSeries(
    snapshots,
    (snapshot) =>
      snapshot.commitIndex,
  );

  const appliedSeries = toSeries(
    snapshots,
    (snapshot) =>
      snapshot.appliedIndex,
  );

  const latencySeries = toSeries(
    snapshots,
    (snapshot) =>
      snapshot.averageLatency,
  );

  const lagSeries = toSeries(
    snapshots,
    (snapshot) =>
      snapshot.replicationLag,
  );

  const throughputSeries =
    rateSeries(
      snapshots,
      (snapshot) =>
        snapshot.transportSent,
    );

  const deliverySeries =
    rateSeries(
      snapshots,
      (snapshot) =>
        snapshot.transportDelivered,
    );

  const retryDelta =
    snapshots.length >= 2
      ? Math.max(
          0,
          snapshots[
            snapshots.length - 1
          ].transportRetries -
            snapshots[
              snapshots.length - 2
            ].transportRetries,
        )
      : 0;

  const failureDelta =
    snapshots.length >= 2
      ? Math.max(
          0,
          snapshots[
            snapshots.length - 1
          ].transportFailures -
            snapshots[
              snapshots.length - 2
            ].transportFailures,
        )
      : 0;

  const electionEvents =
    events.filter(
      (event) =>
        event.type ===
          "raft.election.started" ||
        event.type ===
          "raft.leader.elected",
    );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            OBSERVABILITY
          </div>

          <h1>Metrics</h1>

          <p>
            Rolling consensus, transport,
            availability and replication
            telemetry.
          </p>
        </div>

        <div
          className={
            connected
              ? "live-indicator connected"
              : "live-indicator"
          }
        >
          <Radio size={14} />

          {connected
            ? "STREAMING"
            : "RECONNECTING"}
        </div>
      </header>

      <section className="metrics-grid">
        <Metric
          icon={CheckCircle2}
          label="AVAILABILITY"
          value={`${clusterAvailability.toFixed(
            1,
          )}%`}
          detail="Quorum availability"
        />

        <Metric
          icon={Activity}
          label="TRANSPORT RATE"
          value={`${rates.messages.toFixed(
            1,
          )}/s`}
          detail={`${rates.deliveries.toFixed(
            1,
          )}/s delivered`}
        />

        <Metric
          icon={Clock3}
          label="AVG LATENCY"
          value={`${(
            latest?.averageLatency ?? 0
          ).toFixed(2)} ms`}
          detail="Leader transport"
        />

        <Metric
          icon={GitBranch}
          label="REPLICATION LAG"
          value={String(
            latest?.replicationLag ?? 0,
          )}
          detail="Max follower lag"
        />
      </section>

      <section className="metrics-secondary-grid">
        <SmallMetric
          icon={Server}
          label="LEADER"
          value={
            leader
              ? `Node ${leader.id}`
              : "Election"
          }
        />

        <SmallMetric
          icon={Gauge}
          label="TERM"
          value={String(
            leader?.term ??
              Math.max(
                0,
                ...nodes.map(
                  (node) => node.term,
                ),
              ),
          )}
        />

        <SmallMetric
          icon={Wifi}
          label="HEARTBEAT EVENTS"
          value={`${rates.heartbeats.toFixed(
            1,
          )}/s`}
        />

        <SmallMetric
          icon={RefreshCw}
          label="RETRY DELTA"
          value={String(retryDelta)}
        />

        <SmallMetric
          icon={Activity}
          label="FAILURE DELTA"
          value={String(failureDelta)}
        />

        <SmallMetric
          icon={GitBranch}
          label="ELECTION EVENTS"
          value={String(
            electionEvents.length,
          )}
        />
      </section>

      <section className="metrics-chart-grid">
        <DualMetricChart
          title="Consensus progress"
          subtitle="Leader commit and applied indexes"
          firstLabel="Commit"
          secondLabel="Applied"
          first={commitSeries}
          second={appliedSeries}
        />

        <MetricLineChart
          title="Transport throughput"
          subtitle="Outbound Raft delivery attempts per second"
          data={throughputSeries}
          suffix="/s"
          decimals={1}
        />

        <MetricLineChart
          title="Delivery throughput"
          subtitle="Successfully delivered Raft messages per second"
          data={deliverySeries}
          suffix="/s"
          decimals={1}
        />

        <MetricLineChart
          title="Transport latency"
          subtitle="Average leader-to-peer HTTP delivery latency"
          data={latencySeries}
          suffix=" ms"
          decimals={2}
        />

        <MetricLineChart
          title="Replication lag"
          subtitle="Maximum leader commit minus follower applied index"
          data={lagSeries}
          decimals={0}
        />
      </section>

      <section className="panel metrics-availability-panel">
        <div className="panel-heading">
          <div>
            <h2>Node availability</h2>

            <p>
              Rolling reachability and leadership
              history.
            </p>
          </div>

          <span className="section-chip">
            60 SECOND WINDOW
          </span>
        </div>

        <AvailabilityTimeline
          samples={availability}
        />
      </section>

      <section className="two-column">
        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Transport health</h2>
              <p>
                Current leader transport counters.
              </p>
            </div>
          </div>

          <div className="consensus-list">
            <Row
              label="Sent"
              value={String(
                transport?.sent ?? 0,
              )}
            />

            <Row
              label="Delivered"
              value={String(
                transport?.delivered ?? 0,
              )}
            />

            <Row
              label="Retries"
              value={String(
                transport?.retries ?? 0,
              )}
            />

            <Row
              label="Failures"
              value={String(
                transport?.failures ?? 0,
              )}
            />

            <Row
              label="Backpressure"
              value={String(
                transport?.backpressure ??
                  0,
              )}
            />
          </div>
        </div>

        <div className="panel">
          <div className="panel-heading">
            <div>
              <h2>Consensus health</h2>
              <p>
                Current cluster progress.
              </p>
            </div>
          </div>

          <div className="consensus-list">
            <Row
              label="Leader"
              value={
                leader
                  ? `Node ${leader.id}`
                  : "None"
              }
            />

            <Row
              label="Term"
              value={String(
                leader?.term ?? "—",
              )}
            />

            <Row
              label="Commit"
              value={String(
                leader?.commit_index ??
                  "—",
              )}
            />

            <Row
              label="Applied"
              value={String(
                leader?.applied_index ??
                  "—",
              )}
            />

            <Row
              label="Quorum"
              value={
                nodes.filter(
                  (node) =>
                    node.reachable !==
                    false,
                ).length >= 2
                  ? "Available"
                  : "Unavailable"
              }
            />
          </div>
        </div>
      </section>
    </>
  );
}

function toSeries(
  snapshots: MetricsSnapshot[],
  value: (
    snapshot: MetricsSnapshot,
  ) => number,
): TimePoint[] {
  return snapshots.map(
    (snapshot) => ({
      timestamp: snapshot.timestamp,
      value: value(snapshot),
    }),
  );
}

function rateSeries(
  snapshots: MetricsSnapshot[],
  value: (
    snapshot: MetricsSnapshot,
  ) => number,
): TimePoint[] {
  if (snapshots.length < 2) {
    return [];
  }

  const result: TimePoint[] = [];

  for (
    let index = 1;
    index < snapshots.length;
    index++
  ) {
    const current =
      snapshots[index];

    const previous =
      snapshots[index - 1];

    result.push({
      timestamp: current.timestamp,
      value: calculateRate(
        value(current),
        value(previous),
        current.timestamp -
          previous.timestamp,
      ),
    });
  }

  return result;
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="metric-card">
      <div className="metric-card-top">
        <span>{label}</span>
        <Icon size={17} />
      </div>

      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function SmallMetric({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
}) {
  return (
    <div className="metrics-small-card">
      <Icon size={14} />

      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
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

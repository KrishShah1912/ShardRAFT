import { ChartHover } from "./ChartHover";
import {
  useEffect,
  useRef,
  useState,
} from "react";

import {
  Activity,
  Pause,
  Play,
  Trash2,
} from "lucide-react";

import {
  METRIC_SERIES,
  sampleLiveMetrics,
  appendMetricSample,
  type LiveMetricSample,
  type MetricKey,
} from "./liveMetrics";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import "./multiLiveMetrics.css";

interface Props {
  telemetry: MultiLiveTelemetry;
}

function MetricChart({
  samples,
  metric,
  color,
  label,
}: {
  samples: LiveMetricSample[];
  metric: MetricKey;
  color: string;
  label: string;
}) {
  const width = 480;
  const height = 190;

  const left = 42;
  const right = 462;
  const top = 18;
  const bottom = 152;

  const maximum = Math.max(
    4,
    ...samples.map(sample => sample[metric]),
  );

  const ceiling = Math.ceil(maximum / 4) * 4;

  const latest =
    samples[samples.length - 1]?.timestamp ?? 0;

  const earliest = latest - 59000;

  function x(timestamp: number) {
    return left +
      (timestamp - earliest) / 59000 *
      (right - left);
  }

  function y(value: number) {
    return bottom -
      value / ceiling * (bottom - top);
  }

  const visible = samples.filter(
    sample => sample.timestamp >= earliest,
  );

  const segments: LiveMetricSample[][] = [];

  for (const sample of visible) {
    const segment = segments[segments.length - 1];

    const previous =
      segment?.[segment.length - 1];

    if (
      !segment ||
      !previous ||
      sample.timestamp - previous.timestamp > 1500 ||
      sample.complete !== previous.complete
    ) {
      segments.push([sample]);
    } else {
      segment.push(sample);
    }
  }

  const last = visible[visible.length - 1];

  return (
    <svg
      className="slm-chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`${label} over the last 60 seconds`}
    >
      {[0, 1, 2, 3, 4].map(index => {
        const value = ceiling * index / 4;

        return (
          <g key={index}>
            <line
              x1={left}
              x2={right}
              y1={y(value)}
              y2={y(value)}
              stroke="#263547"
              strokeDasharray="3 5"
            />

            <text
              x={left - 9}
              y={y(value) + 4}
              textAnchor="end"
              fill="#91a3b8"
              fontSize="10"
            >
              {value}
            </text>
          </g>
        );
      })}

      {segments.map(segment => {
        const points = segment.map(sample =>
          `${x(sample.timestamp)},${y(sample[metric])}`,
        ).join(" ");

        return (
          <g key={segment[0].timestamp}>
            {segment.length > 1 && (
              <polyline
                points={points}
                fill="none"
                stroke={color}
                strokeWidth="2.5"
                strokeLinejoin="round"
                strokeLinecap="round"
                strokeDasharray={
                  segment[0].complete
                    ? undefined
                    : "4 5"
                }
              />
            )}

            {segment.map(sample => (
              <circle
                key={sample.timestamp}
                cx={x(sample.timestamp)}
                cy={y(sample[metric])}
                r="2"
                fill={color}
                opacity={sample.complete ? 1 : .45}
              />
            ))}
          </g>
        );
      })}

      {last && (
        <circle
          className="slm-latest-point"
          cx={x(last.timestamp)}
          cy={y(last[metric])}
          r="4"
          fill={color}
        />
      )}

      {[
        { offset: 0, label: "-60s" },
        { offset: .25, label: "-45s" },
        { offset: .5, label: "-30s" },
        { offset: .75, label: "-15s" },
        { offset: 1, label: "Latest" },
      ].map(tick => (
        <text
          key={tick.label}
          x={left + tick.offset * (right - left)}
          y="177"
          textAnchor="middle"
          fill="#91a3b8"
          fontSize="10"
        >
          {tick.label}
        </text>
      ))}
    <ChartHover
        points={visible.map(sample => ({
          timestamp: sample.timestamp,
          x: x(sample.timestamp),
          values: [{
            label,
            value: sample[metric],
            color,
            y: y(sample[metric]),
            unit: "/s",
          }],
          coverage: sample.complete
            ? "5/5 SSE connected at sampling"
            : "Partial SSE connectivity",
        }))}
        left={left}
        right={right}
        top={top}
        bottom={bottom}
        width={width}
      />
    </svg>
  );
}

export function MultiLiveMetrics({
  telemetry,
}: Props) {
  const [shard, setShard] =
    useState<number | "all">("all");

  const [paused, setPaused] = useState(false);

  const [samples, setSamples] =
    useState<LiveMetricSample[]>([]);

  const [reset, setReset] = useState(0);

  const telemetryRef = useRef(telemetry);

  useEffect(() => {
    telemetryRef.current = telemetry;
  }, [telemetry]);

  useEffect(() => {
    if (paused) return;

    // Wait for a complete sampling interval.
    // A short delay allows SSE batches to arrive.
    const timer = window.setInterval(() => {
      const current = telemetryRef.current;

      const timestamp =
        Math.floor((Date.now() - 500) / 1000) * 1000;

      const sample = sampleLiveMetrics(
        current.events,
        timestamp,
        shard,
        current.connectedNodes,
      );

      setSamples(previous =>
        appendMetricSample(previous, sample),
      );
    }, 1000);

    return () => window.clearInterval(timer);
  }, [shard, paused, reset]);

  const latest = samples[samples.length - 1];

  function selectShard(value: number | "all") {
    if (value === shard) return;

    setSamples([]);
    setShard(value);
  }

  function clearHistory() {
    setSamples([]);
    setReset(value => value + 1);
  }

  return (
    <article className="ma-panel slm-panel">
      <header className="slm-header">
        <div>
          <div className="eyebrow">
            LIVE EVENT TELEMETRY
          </div>

          <h2>
            <Activity size={19} />
            Real-time cluster activity
          </h2>

          <p>
            Observed Raft and transport events
            in one-second sampling windows.
          </p>
        </div>

        <span className={
          telemetry.connectedNodes === 5
            ? "slm-status slm-connected"
            : "slm-status slm-partial"
        }>
          <i />
          {telemetry.connectedNodes}/5 SSE connected
        </span>
      </header>

      <div className="slm-toolbar">
        <div className="slm-selectors">
          {(["all", 1, 2, 3, 4] as const).map(id => (
            <button
              key={id}
              className={
                shard === id ? "slm-selected" : ""
              }
              aria-pressed={shard === id}
              onClick={() => selectShard(id)}
            >
              {id === "all" ? "All shards" : `Shard ${id}`}
            </button>
          ))}
        </div>

        <div className="slm-controls">
          <button onClick={() => setPaused(value => !value)}>
            {paused ? <Play size={15} /> : <Pause size={15} />}
            {paused ? "Resume" : "Pause"}
          </button>

          <button onClick={clearHistory}>
            <Trash2 size={15} />
            Clear
          </button>
        </div>
      </div>

      <div className="slm-information">
        <span>
          {paused ? "Sampling paused" : "Sampling every 1s"}
        </span>

        <span>
          {samples.length}/60 samples
        </span>

        <span>
          Pending queue evictions: {telemetry.droppedEvents}
        </span>

        <span>
          {latest
            ? `Latest: ${new Date(
                latest.timestamp,
              ).toLocaleTimeString()}`
            : "Awaiting first sample"}
        </span>
      </div>

      {telemetry.connectedNodes !== 5 && (
        <div className="slm-warning">
          Partial telemetry coverage. Observed rates
          may undercount actual cluster activity.
        </div>
      )}

      <div className="slm-metrics">
        {METRIC_SERIES.map(series => (
          <div className="slm-metric" key={series.key}>
            <span>{series.label}</span>

            <strong style={{ color: series.color }}>
              {latest
                ? latest[series.key].toLocaleString()
                : "—"}
            </strong>

            <small>Observed events / sec</small>
          </div>
        ))}
      </div>

      <div className={
        paused
          ? "slm-chart-grid slm-paused"
          : "slm-chart-grid"
      }>
        {METRIC_SERIES.map(series => (
          <article
            className="slm-chart-card"
            key={series.key}
          >
            <div className="slm-chart-heading">
              <strong>
                <i style={{ background: series.color }} />
                {series.label}
              </strong>

              <span>events/s</span>
            </div>

            {samples.length ? (
              <MetricChart
                samples={samples}
                metric={series.key}
                color={series.color}
                label={series.label}
              />
            ) : (
              <div className="slm-empty">
                Waiting for observations...
              </div>
            )}
          </article>
        ))}
      </div>

      <div className="slm-note">
        Rates count observed events, not database
        operations or measured throughput.
        Replication progress may include heartbeat
        responses. Commit advances are events,
        not individual committed entries.
        Dashed chart segments indicate incomplete
        SSE connectivity. Connected streams do not
        guarantee lossless collection.
        Metadata events are not included.
        History is retained while this page is mounted.
      </div>
    </article>
  );
}

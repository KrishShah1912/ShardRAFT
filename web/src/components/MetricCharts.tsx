import type {
  AvailabilitySample,
  TimePoint,
} from "../telemetry/metrics";

interface LineChartProps {
  title: string;
  subtitle: string;
  data: TimePoint[];
  suffix?: string;
  decimals?: number;
}

export function MetricLineChart({
  title,
  subtitle,
  data,
  suffix = "",
  decimals = 1,
}: LineChartProps) {
  const width = 720;
  const height = 190;
  const padding = 26;

  const values = data.map(
    (point) => point.value,
  );

  const maximum = Math.max(
    1,
    ...values,
  );

  const minimum = Math.min(
    0,
    ...values,
  );

  const range = Math.max(
    1,
    maximum - minimum,
  );

  const points = data
    .map((point, index) => {
      const x =
        data.length <= 1
          ? padding
          : padding +
            (index /
              (data.length - 1)) *
              (width - padding * 2);

      const y =
        height -
        padding -
        ((point.value - minimum) /
          range) *
          (height - padding * 2);

      return `${x},${y}`;
    })
    .join(" ");

  const latest =
    data[data.length - 1]?.value ?? 0;

  return (
    <div className="metric-chart-card">
      <div className="metric-chart-heading">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>

        <strong>
          {latest.toFixed(decimals)}
          {suffix}
        </strong>
      </div>

      <div className="metric-chart-canvas">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          preserveAspectRatio="none"
        >
          <defs>
            <linearGradient
              id={`metric-fill-${slug(title)}`}
              x1="0"
              x2="0"
              y1="0"
              y2="1"
            >
              <stop
                offset="0%"
                stopColor="currentColor"
                stopOpacity="0.18"
              />
              <stop
                offset="100%"
                stopColor="currentColor"
                stopOpacity="0"
              />
            </linearGradient>
          </defs>

          {[0.25, 0.5, 0.75].map(
            (ratio) => (
              <line
                key={ratio}
                x1={padding}
                x2={width - padding}
                y1={height * ratio}
                y2={height * ratio}
                className="metric-grid-line"
              />
            ),
          )}

          {data.length > 1 && (
            <>
              <polygon
                className="metric-area"
                fill={`url(#metric-fill-${slug(
                  title,
                )})`}
                points={`${padding},${
                  height - padding
                } ${points} ${
                  width - padding
                },${height - padding}`}
              />

              <polyline
                className="metric-line"
                points={points}
                fill="none"
              />
            </>
          )}

          {data.map((point, index) => {
            const x =
              data.length <= 1
                ? padding
                : padding +
                  (index /
                    (data.length - 1)) *
                    (width -
                      padding * 2);

            const y =
              height -
              padding -
              ((point.value - minimum) /
                range) *
                (height -
                  padding * 2);

            return (
              <circle
                key={`${point.timestamp}-${index}`}
                cx={x}
                cy={y}
                r="2.2"
                className="metric-point"
              />
            );
          })}
        </svg>
      </div>

      <div className="metric-chart-footer">
        <span>
          {data.length
            ? formatClock(
                data[0].timestamp,
              )
            : "—"}
        </span>

        <span>ROLLING WINDOW</span>

        <span>
          {data.length
            ? formatClock(
                data[data.length - 1]
                  .timestamp,
              )
            : "—"}
        </span>
      </div>
    </div>
  );
}

export function DualMetricChart({
  title,
  subtitle,
  firstLabel,
  secondLabel,
  first,
  second,
}: {
  title: string;
  subtitle: string;
  firstLabel: string;
  secondLabel: string;
  first: TimePoint[];
  second: TimePoint[];
}) {
  const width = 720;
  const height = 190;
  const padding = 26;

  const values = [
    ...first.map(
      (point) => point.value,
    ),
    ...second.map(
      (point) => point.value,
    ),
  ];

  const maximum = Math.max(
    1,
    ...values,
  );

  const makePoints = (
    data: TimePoint[],
  ) =>
    data
      .map((point, index) => {
        const x =
          data.length <= 1
            ? padding
            : padding +
              (index /
                (data.length - 1)) *
                (width - padding * 2);

        const y =
          height -
          padding -
          (point.value / maximum) *
            (height - padding * 2);

        return `${x},${y}`;
      })
      .join(" ");

  return (
    <div className="metric-chart-card">
      <div className="metric-chart-heading">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>

        <div className="metric-chart-legend">
          <span>
            <i className="legend-dot first" />
            {firstLabel}
          </span>

          <span>
            <i className="legend-dot second" />
            {secondLabel}
          </span>
        </div>
      </div>

      <div className="metric-chart-canvas">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          preserveAspectRatio="none"
        >
          {[0.25, 0.5, 0.75].map(
            (ratio) => (
              <line
                key={ratio}
                x1={padding}
                x2={width - padding}
                y1={height * ratio}
                y2={height * ratio}
                className="metric-grid-line"
              />
            ),
          )}

          <polyline
            className="metric-line first"
            points={makePoints(first)}
            fill="none"
          />

          <polyline
            className="metric-line second"
            points={makePoints(second)}
            fill="none"
          />
        </svg>
      </div>
    </div>
  );
}

export function AvailabilityTimeline({
  samples,
}: {
  samples: AvailabilitySample[];
}) {
  const nodes = [1, 2, 3];

  return (
    <div className="availability-chart">
      {nodes.map((nodeId) => (
        <div
          className="availability-row"
          key={nodeId}
        >
          <div className="availability-label">
            Node {nodeId}
          </div>

          <div className="availability-track">
            {samples.map(
              (sample, index) => {
                const online =
                  nodeId === 1
                    ? sample.node1
                    : nodeId === 2
                      ? sample.node2
                      : sample.node3;

                const leader =
                  sample.leaderId ===
                  nodeId;

                return (
                  <span
                    key={`${sample.timestamp}-${index}`}
                    className={[
                      "availability-cell",
                      online
                        ? "online"
                        : "offline",
                      leader
                        ? "leader"
                        : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    title={`${formatClock(
                      sample.timestamp,
                    )} · ${
                      online
                        ? "online"
                        : "offline"
                    }${
                      leader
                        ? " · leader"
                        : ""
                    }`}
                  />
                );
              },
            )}
          </div>

          <div className="availability-state">
            {latestAvailability(
              samples,
              nodeId,
            )}
          </div>
        </div>
      ))}

      <div className="availability-axis">
        <span>
          {samples.length
            ? formatClock(
                samples[0].timestamp,
              )
            : "—"}
        </span>

        <span>NOW</span>
      </div>
    </div>
  );
}

function latestAvailability(
  samples: AvailabilitySample[],
  nodeId: number,
) {
  const latest =
    samples[samples.length - 1];

  if (!latest) {
    return "—";
  }

  const online =
    nodeId === 1
      ? latest.node1
      : nodeId === 2
        ? latest.node2
        : latest.node3;

  if (!online) {
    return "OFFLINE";
  }

  if (latest.leaderId === nodeId) {
    return "LEADER";
  }

  return "ONLINE";
}

function formatClock(
  timestamp: number,
) {
  return new Date(
    timestamp,
  ).toLocaleTimeString([], {
    minute: "2-digit",
    second: "2-digit",
  });
}

function slug(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-");
}

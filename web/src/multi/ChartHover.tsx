import { useState } from "react";
import "./chartHover.css";

interface HoverPoint {
  timestamp: number;
  x: number;
  values: {
    label: string;
    value: number;
    color: string;
    y: number;
    unit?: string;
  }[];
  coverage?: string;
}

interface Props {
  points: HoverPoint[];
  left: number;
  right: number;
  top: number;
  bottom: number;
  width: number;
}

export function ChartHover({
  points,
  left,
  right,
  top,
  bottom,
  width,
}: Props) {
  const [timestamp, setTimestamp] =
    useState<number | null>(null);

  const index = points.findIndex(
    point => point.timestamp === timestamp,
  );

  const active = index >= 0
    ? points[index]
    : undefined;

  const tooltipWidth = 218;
  const tooltipHeight = active
    ? 38 + active.values.length * 20 +
      (active.coverage ? 20 : 0)
    : 0;

  const tooltipX = active
    ? Math.max(
        4,
        Math.min(
          width - tooltipWidth - 4,
          active.x + tooltipWidth + 12 > width
            ? active.x - tooltipWidth - 12
            : active.x + 12,
        ),
      )
    : 0;

  const tooltipY = Math.max(
    4,
    Math.min(top + 8, bottom - tooltipHeight),
  );

  function selectNearest(position: number) {
    if (!points.length) return;

    const nearest = points.reduce(
      (best, point) =>
        Math.abs(point.x - position) <
        Math.abs(best.x - position)
          ? point
          : best,
    );

    setTimestamp(nearest.timestamp);
  }

  return (
    <g className="sr-chart-hover">
      {active && (
        <g pointerEvents="none">
          <line
            x1={active.x}
            x2={active.x}
            y1={top}
            y2={bottom}
            stroke="#94a3b8"
            strokeWidth="1"
            strokeDasharray="4 4"
          />

          {active.values.map(item => (
            <circle
              key={item.label}
              cx={active.x}
              cy={item.y}
              r="4"
              fill={item.color}
              stroke="#e2e8f0"
              strokeWidth="1.2"
            />
          ))}

          <g transform={
            `translate(${tooltipX},${tooltipY})`
          }>
            <rect
              width={tooltipWidth}
              height={tooltipHeight}
              rx="7"
              fill="#111c2b"
              stroke="#475569"
            />

            <text
              x="11"
              y="19"
              fill="#e2e8f0"
              fontSize="11"
              fontWeight="600"
            >
              {new Date(
                active.timestamp,
              ).toLocaleTimeString()}
            </text>

            {active.values.map((item, i) => (
              <g key={item.label}>
                <circle
                  cx="13"
                  cy={36 + i * 20}
                  r="3"
                  fill={item.color}
                />

                <text
                  x="23"
                  y={40 + i * 20}
                  fill="#cbd5e1"
                  fontSize="9.5"
                >
                  {item.label}
                </text>

                <text
                  x={tooltipWidth - 11}
                  y={40 + i * 20}
                  textAnchor="end"
                  fill={item.color}
                  fontSize="10"
                  fontWeight="600"
                >
                  {item.value.toLocaleString()}
                  {item.unit ? ` ${item.unit}` : ""}
                </text>
              </g>
            ))}

            {active.coverage && (
              <text
                x="11"
                y={tooltipHeight - 10}
                fill="#94a3b8"
                fontSize="9.5"
              >
                {active.coverage}
              </text>
            )}
          </g>
        </g>
      )}

      <rect
        className="sr-chart-hover-target"
        x={left}
        y={top}
        width={right - left}
        height={bottom - top}
        fill="transparent"
        tabIndex={0}
        aria-label={
          active
            ? `${new Date(
                active.timestamp,
              ).toLocaleTimeString()}: ${
                active.values.map(item =>
                  `${item.label} ${item.value} ${
                    item.unit ?? ""
                  }`,
                ).join(", ")
              }`
            : "Chart observations. Use arrow keys to inspect samples."
        }
        onPointerMove={event => {
          const svg =
            event.currentTarget.ownerSVGElement;

          const matrix =
            svg?.getScreenCTM();

          if (!svg || !matrix) return;

          const point = svg.createSVGPoint();
          point.x = event.clientX;
          point.y = event.clientY;

          const local = point.matrixTransform(
            matrix.inverse(),
          );

          selectNearest(local.x);
        }}
        onPointerLeave={() => setTimestamp(null)}
        onFocus={() => {
          setTimestamp(
            points[points.length - 1]?.timestamp
              ?? null,
          );
        }}
        onBlur={() => setTimestamp(null)}
        onKeyDown={event => {
          if (!points.length) return;

          let next = index >= 0
            ? index
            : points.length - 1;

          switch (event.key) {
            case "ArrowLeft":
              next = Math.max(0, next - 1);
              break;

            case "ArrowRight":
              next = Math.min(
                points.length - 1,
                next + 1,
              );
              break;

            case "Home":
              next = 0;
              break;

            case "End":
              next = points.length - 1;
              break;

            case "Escape":
              setTimestamp(null);
              return;

            default:
              return;
          }

          event.preventDefault();
          setTimestamp(points[next].timestamp);
        }}
      />
    </g>
  );
}

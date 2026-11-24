import {
  ArrowRight,
  X,
} from "lucide-react";

import {
  eventCategory,
  eventNodes,
  requestID,
} from "../telemetry/events";

import type { RaftEvent } from "../types";

interface Props {
  event: RaftEvent;
  events: RaftEvent[];
  onClose: () => void;
}

export function EventInspector({
  event,
  events,
  onClose,
}: Props) {
  const id = requestID(event);
  const nodes = eventNodes(event);

  const lifecycle = id
    ? events
        .filter(
          (candidate) =>
            requestID(candidate) === id,
        )
        .sort(
          (a, b) =>
            new Date(a.timestamp).getTime() -
            new Date(b.timestamp).getTime(),
        )
    : [event];

  const firstTime = new Date(
    lifecycle[0].timestamp,
  ).getTime();

  return (
    <div
      className="event-inspector-backdrop"
      onMouseDown={onClose}
    >
      <aside
        className="event-inspector"
        onMouseDown={(event) =>
          event.stopPropagation()
        }
      >
        <div className="event-inspector-header">
          <div>
            <span className="eyebrow">
              {eventCategory(event)}
            </span>

            <h2>
              {humanize(event.type)}
            </h2>

            <p>
              Event #{event.id}
              {id ? ` · Request #${id}` : ""}
            </p>
          </div>

          <button
            className="inspector-close"
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </div>

        {(nodes.from !== undefined ||
          nodes.to !== undefined) && (
          <div className="inspector-route">
            <NodeBox
              value={
                nodes.from ??
                event.node_id
              }
            />

            <div className="inspector-route-arrow">
              <ArrowRight size={18} />
              <span>
                {eventCategory(event)}
              </span>
            </div>

            <NodeBox
              value={
                nodes.to ??
                event.node_id
              }
            />
          </div>
        )}

        <section className="inspector-section">
          <h3>Protocol details</h3>

          <div className="inspector-kv-grid">
            <Detail
              label="Event ID"
              value={`#${event.id}`}
            />

            <Detail
              label="Node"
              value={`Node ${event.node_id}`}
            />

            <Detail
              label="Term"
              value={String(
                event.term ?? "—",
              )}
            />

            <Detail
              label="Request"
              value={
                id ? `#${id}` : "—"
              }
            />

            {Object.entries(
              event.data ?? {},
            ).map(([key, value]) => (
              <Detail
                key={key}
                label={humanizeKey(key)}
                value={formatValue(value)}
              />
            ))}
          </div>
        </section>

        <section className="inspector-section">
          <h3>
            Request lifecycle
          </h3>

          <div className="request-lifecycle">
            {lifecycle.map(
              (lifecycleEvent) => {
                const time = new Date(
                  lifecycleEvent.timestamp,
                ).getTime();

                return (
                  <div
                    className="lifecycle-step"
                    key={`${lifecycleEvent.node_id}-${lifecycleEvent.id}`}
                  >
                    <div className="lifecycle-marker" />

                    <div>
                      <strong>
                        {humanize(
                          lifecycleEvent.type,
                        )}
                      </strong>

                      <span>
                        Node{" "}
                        {lifecycleEvent.node_id}
                      </span>
                    </div>

                    <time>
                      +
                      {Math.max(
                        0,
                        time - firstTime,
                      )}
                      ms
                    </time>
                  </div>
                );
              },
            )}
          </div>
        </section>

        <section className="inspector-section">
          <h3>Raw event</h3>

          <pre className="inspector-json">
            {JSON.stringify(
              event,
              null,
              2,
            )}
          </pre>
        </section>
      </aside>
    </div>
  );
}

function NodeBox({
  value,
}: {
  value: number;
}) {
  return (
    <div className="inspector-node-box">
      <span>NODE</span>
      <strong>{value}</strong>
    </div>
  );
}

function Detail({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="inspector-detail">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function humanize(value: string): string {
  return value
    .replace(/^raft\./, "")
    .split(".")
    .map(
      (part) =>
        part.charAt(0).toUpperCase() +
        part.slice(1),
    )
    .join(" ");
}

function humanizeKey(value: string): string {
  return value
    .split("_")
    .map(
      (part) =>
        part.charAt(0).toUpperCase() +
        part.slice(1),
    )
    .join(" ");
}

function formatValue(
  value: unknown,
): string {
  if (value === undefined) {
    return "—";
  }

  if (
    typeof value === "object" &&
    value !== null
  ) {
    return JSON.stringify(value);
  }

  return String(value);
}

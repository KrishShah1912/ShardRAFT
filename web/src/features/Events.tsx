import {
  Activity,
  CheckCircle2,
  Crown,
  Radio,
  Search,
  Server,
  ShieldAlert,
} from "lucide-react";

import {
  useMemo,
  useState,
} from "react";

import { EventInspector } from "../components/EventInspector";
import { HeartbeatTelemetry } from "../components/HeartbeatTelemetry";

import {
  eventCategory,
  eventNodes,
  eventSeverity,
  isHeartbeatEvent,
  isImportantEvent,
  requestID,
  type EventMode,
} from "../telemetry/events";

import type {
  NodeStatus,
  RaftEvent,
} from "../types";

interface Props {
  events: RaftEvent[];
  nodes: NodeStatus[];
  connected: boolean;
}

export function Events({
  events,
  nodes,
  connected,
}: Props) {
  const [mode, setMode] =
    useState<EventMode>("important");

  const [nodeFilter, setNodeFilter] =
    useState<number | "all">("all");

  const [categoryFilter, setCategoryFilter] =
    useState("all");

  const [search, setSearch] =
    useState("");

  const [selected, setSelected] =
    useState<RaftEvent>();

  const filtered = useMemo(() => {
    let result =
      mode === "important"
        ? events.filter(isImportantEvent)
        : events;

    if (mode !== "all") {
      result = result.filter(
        (event) =>
          !isHeartbeatEvent(event),
      );
    }

    if (nodeFilter !== "all") {
      result = result.filter(
        (event) =>
          event.node_id === nodeFilter,
      );
    }

    if (categoryFilter !== "all") {
      result = result.filter(
        (event) =>
          eventCategory(event) ===
          categoryFilter,
      );
    }

    const normalized =
      search.trim().toLowerCase();

    if (normalized) {
      result = result.filter((event) => {
        const id = requestID(event);

        return (
          event.type
            .toLowerCase()
            .includes(normalized) ||
          String(event.id).includes(
            normalized,
          ) ||
          String(event.node_id).includes(
            normalized,
          ) ||
          (id ?? "").includes(
            normalized,
          ) ||
          JSON.stringify(
            event.data ?? {},
          )
            .toLowerCase()
            .includes(normalized)
        );
      });
    }

    return result.slice(0, 250);
  }, [
    events,
    mode,
    nodeFilter,
    categoryFilter,
    search,
  ]);

  const importantCount =
    events.filter(isImportantEvent).length;

  const heartbeatCount =
    events.filter(isHeartbeatEvent).length;

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  return (
    <>
      <header className="page-header events-page-header">
        <div>
          <div className="eyebrow">
            OBSERVABILITY
          </div>

          <h1>Live Events</h1>

          <p>
            Explore consensus transitions,
            replication requests and heartbeat
            telemetry from the running cluster.
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

      <section className="event-mode-bar">
        <button
          className={
            mode === "important"
              ? "event-mode active"
              : "event-mode"
          }
          onClick={() =>
            setMode("important")
          }
        >
          <ShieldAlert size={14} />

          <div>
            <strong>Important</strong>
            <span>
              {importantCount} events
            </span>
          </div>
        </button>

        <button
          className={
            mode === "all"
              ? "event-mode active"
              : "event-mode"
          }
          onClick={() => setMode("all")}
        >
          <Activity size={14} />

          <div>
            <strong>All Events</strong>
            <span>
              {events.length} buffered
            </span>
          </div>
        </button>

        <button
          className={
            mode === "heartbeat"
              ? "event-mode active"
              : "event-mode"
          }
          onClick={() =>
            setMode("heartbeat")
          }
        >
          <Radio size={14} />

          <div>
            <strong>
              Heartbeat Telemetry
            </strong>

            <span>
              {heartbeatCount} pulses
            </span>
          </div>
        </button>
      </section>

      {mode === "heartbeat" ? (
        <HeartbeatTelemetry
          events={events}
          nodes={nodes}
        />
      ) : (
        <>
          <section className="event-stat-grid">
            <EventStat
              icon={Crown}
              label="LEADER"
              value={
                leader
                  ? `Node ${leader.id}`
                  : "—"
              }
            />

            <EventStat
              icon={Activity}
              label="BUFFERED"
              value={String(events.length)}
            />

            <EventStat
              icon={CheckCircle2}
              label="IMPORTANT"
              value={String(
                importantCount,
              )}
            />

            <EventStat
              icon={Radio}
              label="HEARTBEATS"
              value={String(
                heartbeatCount,
              )}
            />
          </section>

          <section className="event-toolbar">
            <div className="event-search">
              <Search size={14} />

              <input
                value={search}
                onChange={(event) =>
                  setSearch(
                    event.target.value,
                  )
                }
                placeholder="Search event, request ID, node or payload..."
              />
            </div>

            <select
              value={nodeFilter}
              onChange={(event) =>
                setNodeFilter(
                  event.target.value ===
                    "all"
                    ? "all"
                    : Number(
                        event.target.value,
                      ),
                )
              }
            >
              <option value="all">
                All nodes
              </option>
              <option value="1">
                Node 1
              </option>
              <option value="2">
                Node 2
              </option>
              <option value="3">
                Node 3
              </option>
            </select>

            <select
              value={categoryFilter}
              onChange={(event) =>
                setCategoryFilter(
                  event.target.value,
                )
              }
            >
              <option value="all">
                All categories
              </option>
              <option value="ELECTION">
                Election
              </option>
              <option value="REPLICATION">
                Replication
              </option>
              <option value="HEARTBEAT">
                Heartbeat
              </option>
            </select>
          </section>

          <section className="event-stream-panel">
            <div className="event-stream-heading">
              <div>
                <h2>
                  {mode === "important"
                    ? "Meaningful transitions"
                    : "Protocol stream"}
                </h2>

                <p>
                  {filtered.length} events shown
                </p>
              </div>

              <span className="section-chip">
                NEWEST FIRST
              </span>
            </div>

            <div className="visual-event-list">
              {filtered.length === 0 && (
                <div className="empty-state">
                  No events match the current
                  filters.
                </div>
              )}

              {filtered.map((event) => (
                <VisualEvent
                  key={`${event.node_id}-${event.id}`}
                  event={event}
                  onInspect={() =>
                    setSelected(event)
                  }
                />
              ))}
            </div>
          </section>
        </>
      )}

      {selected && (
        <EventInspector
          event={selected}
          events={events}
          onClose={() =>
            setSelected(undefined)
          }
        />
      )}
    </>
  );
}

function VisualEvent({
  event,
  onInspect,
}: {
  event: RaftEvent;
  onInspect: () => void;
}) {
  const category =
    eventCategory(event);

  const severity =
    eventSeverity(event);

  const nodes = eventNodes(event);
  const id = requestID(event);

  return (
    <article
      className={`visual-event ${severity}`}
    >
      <div
        className={`visual-event-marker ${severity}`}
      />

      <div className="visual-event-main">
        <div className="visual-event-top">
          <div>
            <span className="visual-event-category">
              {category}
            </span>

            <strong>
              {humanize(event.type)}
            </strong>
          </div>

          <time>
            {formatTime(event.timestamp)}
          </time>
        </div>

        <div className="visual-event-route">
          <div className="event-node-pill">
            <Server size={12} />
            Node{" "}
            {nodes.from ??
              event.node_id}
          </div>

          {(nodes.to !== undefined ||
            nodes.from !== undefined) && (
            <>
              <div className="event-route-line">
                <span />
                <small>
                  {category}
                </small>
              </div>

              <div className="event-node-pill">
                <Server size={12} />
                Node{" "}
                {nodes.to ??
                  event.node_id}
              </div>
            </>
          )}
        </div>

        <div className="visual-event-meta">
          <Meta
            label="EVENT"
            value={`#${event.id}`}
          />

          <Meta
            label="TERM"
            value={String(
              event.term ?? "—",
            )}
          />

          <Meta
            label="REQUEST"
            value={
              id ? `#${id}` : "—"
            }
          />

          {typeof event.data?.index ===
            "number" && (
            <Meta
              label="INDEX"
              value={String(
                event.data.index,
              )}
            />
          )}

          {typeof event.data?.entries ===
            "number" && (
            <Meta
              label="ENTRIES"
              value={String(
                event.data.entries,
              )}
            />
          )}
        </div>
      </div>

      <button
        className="inspect-event-button"
        onClick={onInspect}
      >
        Inspect
      </button>
    </article>
  );
}

function EventStat({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
}) {
  return (
    <div className="metric-card">
      <div className="metric-card-top">
        <span>{label}</span>
        <Icon size={17} />
      </div>

      <strong>{value}</strong>
    </div>
  );
}

function Meta({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function humanize(
  value: string,
): string {
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

function formatTime(
  timestamp: string,
): string {
  return new Date(
    timestamp,
  ).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

import {
  useMemo,
  useState,
} from "react";

import {
  Activity,
  Pause,
  Play,
  RotateCcw,
  Crown,
  Database,
  RefreshCw,
  Terminal,
  Zap,
  ArrowLeftRight,
} from "lucide-react";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import {
  buildFaultTimeline,
  type FaultTimelineEntry,
  type FaultTimelineKind,
  type FaultTimelineSource,
  type FaultActionObservation,
  type FaultLeaderObservation,
  type FaultReachabilityObservation,
} from "./faultTimeline";

import "./multiFaultTimeline.css";

interface Props {
  telemetry: MultiLiveTelemetry;
  actions: readonly FaultActionObservation[];
  leadership: readonly FaultLeaderObservation[];
  reachability: readonly FaultReachabilityObservation[];
}

const SOURCE_LABELS: Record<FaultTimelineSource, string> = {
  telemetry: "Raft SSE",
  docker: "Docker API",
  polling: "Cluster polling",
};

const KIND_LABELS: Record<FaultTimelineKind, string> = {
  election: "Election",
  leadership: "Leadership",
  commit: "Commit",
  replication: "Replication",
  control: "Docker control",
  recovery: "Reachability",
};

const KIND_ICONS = {
  election: Zap,
  leadership: Crown,
  commit: Database,
  replication: ArrowLeftRight,
  control: Terminal,
  recovery: RefreshCw,
};

function groupLabel(id: number | null): string {
  if (id === null) return "Physical node";
  if (id === 5) return "Metadata";
  return `Shard ${id}`;
}

function formatTime(value: string): string {
  return new Date(value).toLocaleTimeString(
    undefined,
    {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    },
  );
}

export function MultiFaultTimeline({
  telemetry,
  actions,
  leadership,
  reachability,
}: Props) {
  const [group, setGroup] = useState("all");
  const [node, setNode] = useState("all");
  const [source, setSource] = useState("all");
  const [kind, setKind] = useState("all");
  const [windowSize, setWindowSize] = useState("all");

  const [pausedAt, setPausedAt] =
    useState<number | null>(null);

  const [frozen, setFrozen] =
    useState<FaultTimelineEntry[] | null>(null);

  const [selected, setSelected] =
    useState<FaultTimelineEntry | null>(null);

  const [since, setSince] =
    useState<number | undefined>(undefined);

  const rows = useMemo(
    () => buildFaultTimeline(
      {
        electionEvents: telemetry.electionEvents,
        replicationEvents: telemetry.replicationEvents,
        actions,
        leadership,
        reachability,
      },
      { limit: 1000 },
    ),
    [
      telemetry.electionEvents,
      telemetry.replicationEvents,
      actions,
      leadership,
      reachability,
    ],
  );

  const available = frozen ?? rows;

  const referenceTime = pausedAt ?? telemetry.now;

  const visible = useMemo(() => {
    const lowerBound = Math.max(
      since ?? 0,
      windowSize === "all"
        ? 0
        : referenceTime - Number(windowSize),
    );

    return available.filter(row => {
      const time = Date.parse(row.timestamp);

      return (
        time >= lowerBound &&
        (group === "all" ||
          row.shardId === Number(group)) &&
        (node === "all" ||
          row.nodeId === Number(node)) &&
        (source === "all" ||
          row.source === source) &&
        (kind === "all" ||
          row.kind === kind)
      );
    });
  }, [
    available,
    group,
    node,
    source,
    kind,
    since,
    windowSize,
    referenceTime,
  ]);

  const displayed = visible.slice(0, 200);

  const counts = {
    election: visible.filter(
      row => row.kind === "election",
    ).length,

    leadership: visible.filter(
      row => row.kind === "leadership",
    ).length,

    commit: visible.filter(
      row => row.kind === "commit",
    ).length,

    recovery: visible.filter(
      row => row.kind === "recovery",
    ).length,

    control: visible.filter(
      row => row.kind === "control",
    ).length,
  };

  function togglePause() {
    if (frozen !== null) {
      setFrozen(null);
      setPausedAt(null);
      return;
    }

    setFrozen(rows);
    setPausedAt(telemetry.now);
  }

  function resetFilters() {
    setGroup("all");
    setNode("all");
    setSource("all");
    setKind("all");
    setWindowSize("all");
    setSince(undefined);
    setSelected(null);
  }

  return (
    <article className="ma-panel mft-panel">
      <header className="mft-header">
        <div>
          <div className="eyebrow">
            LIVE RESILIENCE TELEMETRY
          </div>

          <h2>
            <Activity size={20} />
            Live Election &amp; Recovery Timeline
          </h2>

          <p>
            Observed Raft events, Docker actions and
            cluster recovery transitions.
          </p>
        </div>

        <div className="mft-status">
          <span className={
            frozen !== null
              ? "mft-paused"
              : "mft-live"
          }>
            <i />
            {frozen !== null
              ? "Display paused"
              : "Live updates"}
          </span>

          <small>
            {telemetry.connectedNodes}/5 SSE connected
          </small>
        </div>
      </header>

      <div className="mft-metrics">
        {[
          ["Election events", counts.election],
          ["Leadership events", counts.leadership],
          ["Commit advances", counts.commit],
          ["Reachability changes", counts.recovery],
          ["Control events", counts.control],
        ].map(([label, value]) => (
          <div className="mft-metric" key={label}>
            <small>{label}</small>
            <strong>{value}</strong>
          </div>
        ))}
      </div>

      <div className="mft-filters">
        <label>
          Raft group

          <select
            value={group}
            onChange={event =>
              setGroup(event.target.value)
            }
          >
            <option value="all">All groups</option>

            {[1, 2, 3, 4].map(id => (
              <option key={id} value={id}>
                Shard {id}
              </option>
            ))}

            <option value="5">Metadata</option>
          </select>
        </label>

        <label>
          Associated node

          <select
            value={node}
            onChange={event =>
              setNode(event.target.value)
            }
          >
            <option value="all">All nodes</option>

            {[1, 2, 3, 4, 5].map(id => (
              <option key={id} value={id}>
                Node {id}
              </option>
            ))}
          </select>
        </label>

        <label>
          Observation source

          <select
            value={source}
            onChange={event =>
              setSource(event.target.value)
            }
          >
            <option value="all">All sources</option>
            <option value="telemetry">Raft SSE</option>
            <option value="docker">Docker API</option>
            <option value="polling">Cluster polling</option>
          </select>
        </label>

        <label>
          Event type

          <select
            value={kind}
            onChange={event =>
              setKind(event.target.value)
            }
          >
            <option value="all">All events</option>

            {Object.entries(KIND_LABELS).map(
              ([id, label]) => (
                <option key={id} value={id}>
                  {label}
                </option>
              ),
            )}
          </select>
        </label>

        <label>
          Time window

          <select
            value={windowSize}
            onChange={event =>
              setWindowSize(event.target.value)
            }
          >
            <option value="all">Retained history</option>
            <option value="30000">Last 30 seconds</option>
            <option value="60000">Last minute</option>
            <option value="300000">Last 5 minutes</option>
          </select>
        </label>
      </div>

      <div className="mft-toolbar">
        <div className="mft-buttons">
          <button
            type="button"
            onClick={togglePause}
          >
            {frozen !== null
              ? <Play size={14} />
              : <Pause size={14} />}

            {frozen !== null ? "Resume" : "Pause"}
          </button>

          <button
            type="button"
            onClick={() => {
              setSince(referenceTime);
              setSelected(null);
            }}
          >
            Show new events
          </button>

          <button
            type="button"
            onClick={resetFilters}
          >
            <RotateCcw size={14} />
            Reset filters
          </button>
        </div>

        <small>
          Showing {displayed.length} of {visible.length}
          {" "}matching retained events
        </small>
      </div>

      {frozen !== null && (
        <div className="mft-notice">
          Display paused. Shared telemetry collection
          continues in the background.
        </div>
      )}

      {telemetry.connectedNodes < 5 && (
        <div className="mft-notice">
          Some event streams are disconnected.
          The timeline may contain incomplete observations.
        </div>
      )}

      {telemetry.droppedEvents > 0 && (
        <div className="mft-notice">
          Client telemetry buffer evictions observed:
          {" "}{telemetry.droppedEvents}.
          Event history may be incomplete.
        </div>
      )}

      <div className="mft-workspace">
        <div className="mft-events">
          {displayed.length === 0 ? (
            <div className="mft-empty">
              <Activity size={30} />

              <strong>No matching observations</strong>

              <span>
                New events appear automatically when
                telemetry is available and the display
                is not paused.
              </span>
            </div>
          ) : (
            displayed.map(row => {
              const Icon = KIND_ICONS[row.kind];

              return (
                <button
                  type="button"
                  key={row.id}
                  className={
                    `mft-event mft-tone-${row.tone}` +
                    (selected?.id === row.id
                      ? " mft-selected"
                      : "")
                  }
                  onClick={() => setSelected(row)}
                  aria-pressed={selected?.id === row.id}
                >
                  <span className="mft-event-icon">
                    <Icon size={16} />
                  </span>

                  <span className="mft-event-body">
                    <strong>{row.title}</strong>
                    <small>{row.detail}</small>

                    <span className="mft-tags">
                      <span>{groupLabel(row.shardId)}</span>

                      <span>
                        {SOURCE_LABELS[row.source]}
                      </span>

                      {row.term !== null && (
                        <span>Term {row.term}</span>
                      )}
                    </span>
                  </span>

                  <time dateTime={row.timestamp}>
                    {formatTime(row.timestamp)}
                  </time>
                </button>
              );
            })
          )}
        </div>

        <aside className="mft-inspector">
          <div className="eyebrow">
            EVENT INSPECTOR
          </div>

          {selected ? (
            <>
              <h3>{selected.title}</h3>

              <p>{selected.detail}</p>

              <dl>
                <dt>Source</dt>
                <dd>{SOURCE_LABELS[selected.source]}</dd>

                <dt>Event type</dt>
                <dd>{KIND_LABELS[selected.kind]}</dd>

                <dt>Group</dt>
                <dd>{groupLabel(selected.shardId)}</dd>

                <dt>Associated node</dt>
                <dd>
                  {selected.nodeId === null
                    ? "Unknown"
                    : `Node ${selected.nodeId}`}
                </dd>

                <dt>Raft term</dt>
                <dd>{selected.term ?? "Not applicable"}</dd>

                <dt>Timestamp</dt>
                <dd>{selected.timestamp}</dd>
              </dl>

              <details>
                <summary>Normalized event JSON</summary>

                <pre>
                  {JSON.stringify(selected, null, 2)}
                </pre>
              </details>
            </>
          ) : (
            <div className="mft-inspector-empty">
              Select a timeline event to inspect
              its source, group, node, term and timestamp.
            </div>
          )}
        </aside>
      </div>

      <footer className="mft-footer">
        Newest observations first. The component retains
        up to 1,000 normalized entries and displays up
        to 200 matching entries. Group filters exclude
        physical-node-only events. Metadata leadership
        is observed through polling. HTTP reachability
        does not establish replica catch-up. Timestamp
        ordering does not establish causal ordering.
      </footer>
    </article>
  );
}

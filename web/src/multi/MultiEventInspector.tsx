import { useMemo, useState } from "react";
import {
  eventIdentity,
  heartbeatIdentity,
  heartbeatObservation,
  type LiveEvent,
} from "./liveTelemetry";
import {
  replicationIdentity,
  replicationObservation,
  commitObservation,
} from "./replicationTelemetry";
import {
  isElectionEvent,
  electionEventLabel,
  electionTimeline,
} from "./electionTelemetry";
import "./multiEventInspector.css";

type Props = {
  event: LiveEvent;
  events: LiveEvent[];
  onClose: () => void;
};

function formatValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "—";
  }

  return typeof value === "object"
    ? JSON.stringify(value)
    : String(value);
}

function eventLabel(event: LiveEvent): string {
  if (isElectionEvent(event)) {
    return electionEventLabel(event);
  }

  const labels: Record<string, string> = {
    "raft.heartbeat.sent": "Heartbeat sent",
    "raft.heartbeat.received": "Heartbeat received",
    "raft.heartbeat.accepted": "Heartbeat accepted",
    "raft.append.sent": "AppendEntries sent",
    "raft.append.received": "AppendEntries received",
    "raft.append.accepted": "AppendEntries accepted",
    "raft.append.rejected": "AppendEntries rejected",
    "raft.replication.progress": "Replication progress observed",
    "raft.entry.appended": "Log entry appended",
    "raft.noop.appended": "No-op entry appended",
    "raft.commit.advanced": "Commit index advanced",
    "transport.message.queued": "Transport message queued",
    "transport.message.sent": "Transport message sent",
    "transport.message.delivered": "Transport message delivered",
  };

  return labels[event.type] ?? event.type;
}

function eventTone(event: LiveEvent): string {
  if (event.type.includes("rejected")) {
    return "danger";
  }

  if (
    event.type.includes("accepted") ||
    event.type.includes("granted") ||
    event.type.includes("elected") ||
    event.type.includes("delivered") ||
    event.type.includes("commit.advanced") ||
    event.type.includes("replication.progress")
  ) {
    return "success";
  }

  if (event.type.includes("received")) {
    return "received";
  }

  if (
    event.type.includes("changed") ||
    event.type.includes("election.started")
  ) {
    return "warning";
  }

  return "primary";
}

function chronological(events: LiveEvent[]) {
  return [...events].sort(
    (a, b) =>
      Date.parse(a.timestamp) -
        Date.parse(b.timestamp) ||
      a.timestamp.localeCompare(b.timestamp) ||
      a.node_id - b.node_id ||
      a.id - b.id,
  );
}

export default function MultiEventInspector({
  event,
  events,
  onClose,
}: Props) {
  const [copyState, setCopyState] =
    useState("Copy JSON");

  const heartbeat = heartbeatObservation(event);
  const replication = replicationObservation(event);
  const commit = commitObservation(event);

  const correlation = useMemo(() => {
    const selectedHeartbeat =
      heartbeatObservation(event);

    if (selectedHeartbeat) {
      const identity =
        heartbeatIdentity(selectedHeartbeat);

      return {
        title: "Correlated heartbeat timeline",
        description:
          "Matched by shard, term, leader, follower and request ID.",
        events: chronological(
          events.filter(item => {
            const observation =
              heartbeatObservation(item);

            return observation !== null &&
              heartbeatIdentity(observation) === identity;
          }),
        ),
      };
    }

    const selectedReplication =
      replicationObservation(event);

    if (selectedReplication) {
      const identity =
        replicationIdentity(selectedReplication);

      return {
        title: "Correlated replication timeline",
        description:
          "Matched by shard, term, leader, follower and request ID.",
        events: chronological(
          events.filter(item => {
            const observation =
              replicationObservation(item);

            return observation !== null &&
              replicationIdentity(observation) === identity;
          }),
        ),
      };
    }

    if (
      isElectionEvent(event) &&
      event.term !== undefined
    ) {
      return {
        title: "Shard election history",
        description:
          "Observed election events for this shard and term. Multiple candidates may appear.",
        events: electionTimeline(
          events,
          event.shard_id,
          event.term,
        ),
      };
    }

    return {
      title: "Selected event",
      description:
        "A reliable request correlation is not available for this event type.",
      events: [event],
    };
  }, [event, events]);

  const raw = JSON.stringify(event, null, 2);

  const request = heartbeat ?? replication;

  async function copyJSON() {
    try {
      await navigator.clipboard.writeText(raw);
      setCopyState("Copied");
    } catch {
      setCopyState("Copy failed — select JSON");
    }
  }

  const fields = [
    ["Event ID", event.id],
    ["Node", `N${event.node_id}`],
    ["Shard", `S${event.shard_id}`],
    ["Term", event.term],
    ["Category", event.category],
    ["Request ID", event.data.request_id],
  ];

  return (
    <article
      className="ma-panel mei-panel"
      aria-label="Event inspector"
    >
      <header className="mei-header">
        <div>
          <div className="mei-eyebrow">
            LIVE TELEMETRY INSPECTOR
          </div>
          <h2>Event Inspector</h2>
        </div>

        <button onClick={onClose}>
          Close inspector
        </button>
      </header>

      <div className="mei-event-title">
        <span
          className={`mei-dot mei-${eventTone(event)}`}
        />
        <strong>{eventLabel(event)}</strong>
        <code>{event.type}</code>
      </div>

      <div className="mei-timestamp">
        {event.timestamp}
        {" · "}
        Captured inspection snapshot
      </div>

      <div className="mei-fields">
        {fields.map(([label, value]) => (
          <div key={String(label)}>
            <span>{String(label)}</span>
            <strong>{formatValue(value)}</strong>
          </div>
        ))}
      </div>

      {request && (
        <section className="mei-section">
          <h3>Request participants</h3>

          <div className="mei-participants">
            <div className="mei-node">
              <small>Leader</small>
              <strong>N{request.leaderId}</strong>
            </div>

            <div className="mei-direction">
              <span>Request</span>
              <strong>────────→</strong>
            </div>

            <div className="mei-node">
              <small>Follower</small>
              <strong>N{request.followerId}</strong>
            </div>
          </div>

          <p>
            Request {request.requestId}
            {" · "}
            Shard {request.shardId}
            {" · "}
            Term {request.term}
          </p>
        </section>
      )}

      {commit && (
        <section className="mei-section">
          <h3>Commit advancement</h3>

          <div className="mei-fields">
            {[
              ["Previous commit", commit.previousCommit],
              ["New commit", commit.commitIndex],
              ["Advanced by", commit.advancedBy],
              ["Source", commit.source],
              ["Reported replicas", commit.replicas],
              ["Reported majority", commit.majority],
            ].map(([label, value]) => (
              <div key={String(label)}>
                <span>{String(label)}</span>
                <strong>{formatValue(value)}</strong>
              </div>
            ))}
          </div>
        </section>
      )}

      <section className="mei-section">
        <h3>{correlation.title}</h3>
        <p>{correlation.description}</p>

        <div className="mei-timeline">
          {correlation.events.slice(0, 200).map(item => (
            <div
              key={eventIdentity(item)}
              className={
                "mei-timeline-item" +
                (eventIdentity(item) === eventIdentity(event)
                  ? " mei-selected"
                  : "")
              }
            >
              <span
                className={`mei-dot mei-${eventTone(item)}`}
              />

              <div>
                <strong>{eventLabel(item)}</strong>

                <small>
                  N{item.node_id}
                  {" · "}
                  S{item.shard_id}
                  {" · "}
                  Term {item.term ?? "—"}
                </small>

                <code>{item.type}</code>
              </div>

              <time title={item.timestamp}>
                {new Date(item.timestamp)
                  .toLocaleTimeString()}
              </time>
            </div>
          ))}
        </div>

        <small>
          Showing {Math.min(correlation.events.length, 200)}
          {" of "}
          {correlation.events.length} observed events.
          Missing stages are not inferred.
        </small>
      </section>

      <section className="mei-section">
        <h3>Event payload</h3>

        {Object.keys(event.data).length === 0 ? (
          <p>No payload fields.</p>
        ) : (
          <div className="mei-payload">
            {Object.entries(event.data).map(
              ([key, value]) => (
                <div key={key}>
                  <code>{key}</code>
                  <strong>{formatValue(value)}</strong>
                </div>
              ),
            )}
          </div>
        )}
      </section>

      <section className="mei-section">
        <div className="mei-header">
          <h3>Raw event JSON</h3>

          <button onClick={copyJSON}>
            {copyState}
          </button>
        </div>

        <pre className="mei-json">{raw}</pre>
      </section>

      <p className="mei-note">
        Inspection uses a bounded snapshot of observed
        events. Request IDs may be reused after restarts.
        Follower acceptance does not establish leader
        acknowledgement or quorum commitment.
        Transport events without sufficient correlation
        fields are inspected individually.
      </p>
    </article>
  );
}

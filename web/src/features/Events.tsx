import { useMemo, useState } from "react";
import {
  formatEventName,
  relativeTime,
} from "../lib/format";
import type { RaftEvent } from "../types";

export function Events({
  events,
}: {
  events: RaftEvent[];
}) {
  const [showHeartbeats, setShowHeartbeats] =
    useState(false);

  const filtered = useMemo(
    () =>
      events.filter((event) => {
        if (showHeartbeats) {
          return true;
        }

        if (event.type !== "raft.append.sent") {
          return true;
        }

        return Number(event.data?.entries ?? 0) > 0;
      }),
    [events, showHeartbeats],
  );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">OBSERVABILITY</div>
          <h1>Live Events</h1>
          <p>
            Ordered Raft events emitted by the running cluster.
          </p>
        </div>

        <label className="toggle">
          <input
            type="checkbox"
            checked={showHeartbeats}
            onChange={(event) =>
              setShowHeartbeats(event.target.checked)
            }
          />
          Show heartbeat traffic
        </label>
      </header>

      <div className="panel">
        <div className="event-table">
          {filtered.map((event) => (
            <div className="event-detail-row" key={event.id}>
              <span className="event-id">#{event.id}</span>
              <div className="event-dot" />
              <div className="event-copy">
                <strong>
                  {formatEventName(event.type)}
                </strong>
                <span>
                  Node {event.node_id} · Term{" "}
                  {event.term ?? "—"}
                </span>
              </div>
              <code>
                {Object.keys(event.data ?? {}).length
                  ? JSON.stringify(event.data)
                  : "—"}
              </code>
              <time>{relativeTime(event.timestamp)}</time>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}

import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Network,
  Radio,
  RefreshCw,
  Server,
} from "lucide-react";

import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  transport?: TransportStats;
}

export function Transport({
  nodes,
  events,
  transport,
}: Props) {
  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const delivery =
    (transport?.sent ?? 0) === 0
      ? 100
      : Math.min(
          100,
          ((transport?.delivered ?? 0) /
            (transport?.sent ?? 1)) *
            100,
        );

  const recent = events
    .filter((event) =>
      event.type.startsWith("transport."),
    )
    .slice(0, 12);

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            INFRASTRUCTURE
          </div>

          <h1>Transport</h1>

          <p>
            Live Raft message delivery, peer queues,
            retries and network health.
          </p>
        </div>

        <div className="live-indicator connected">
          <Radio size={14} />
          LIVE
        </div>
      </header>

      <section className="metrics-grid">
        <Metric
          icon={CheckCircle2}
          label="DELIVERY"
          value={`${delivery.toFixed(1)}%`}
          detail={`${transport?.delivered ?? 0} delivered`}
        />

        <Metric
          icon={Activity}
          label="MESSAGES"
          value={String(transport?.sent ?? 0)}
          detail="Delivery attempts"
        />

        <Metric
          icon={RefreshCw}
          label="RETRIES"
          value={String(transport?.retries ?? 0)}
          detail="Bounded retry attempts"
        />

        <Metric
          icon={AlertTriangle}
          label="FAILURES"
          value={String(transport?.failures ?? 0)}
          detail={`${transport?.backpressure ?? 0} backpressure`}
        />
      </section>

      <section className="transport-network-panel">
        <div className="panel-heading">
          <div>
            <h2>Live peer transport</h2>
            <p>
              Independent delivery channels from the
              current leader.
            </p>
          </div>

          <Network size={17} />
        </div>

        {leader ? (
          <div className="transport-topology">
            <div className="transport-leader">
              <Server size={18} />
              <span>NODE {leader.id}</span>
              <strong>LEADER</strong>
            </div>

            <div className="transport-peer-grid">
              {(transport?.peers ?? []).map(
                (peer) => {
                  const node = nodes.find(
                    (candidate) =>
                      candidate.id === peer.peer_id,
                  );

                  const online =
                    node?.reachable !== false;

                  const queuePercent =
                    peer.queue_capacity === 0
                      ? 0
                      : Math.min(
                          100,
                          (peer.queue_depth /
                            peer.queue_capacity) *
                            100,
                        );

                  const peerDelivery =
                    peer.sent === 0
                      ? 100
                      : Math.min(
                          100,
                          (peer.delivered /
                            peer.sent) *
                            100,
                        );

                  return (
                    <div
                      className="transport-peer"
                      key={peer.peer_id}
                    >
                      <div
                        className={
                          online
                            ? "transport-path healthy"
                            : "transport-path failed"
                        }
                      >
                        <span className="transport-line" />

                        {online && (
                          <span className="transport-packet">
                            ›
                          </span>
                        )}
                      </div>

                      <div className="transport-peer-card">
                        <div className="transport-peer-header">
                          <div>
                            <span>
                              NODE {peer.peer_id}
                            </span>

                            <strong>
                              {online
                                ? "CONNECTED"
                                : "UNREACHABLE"}
                            </strong>
                          </div>

                          <span
                            className={
                              online
                                ? "transport-health healthy"
                                : "transport-health failed"
                            }
                          >
                            {online
                              ? "HEALTHY"
                              : "FAILED"}
                          </span>
                        </div>

                        <div className="transport-latency">
                          <span>
                            AVG LATENCY
                          </span>

                          <strong>
                            {peer.average_latency_ms.toFixed(
                              2,
                            )}
                            ms
                          </strong>
                        </div>

                        <div className="transport-queue">
                          <div>
                            <span>QUEUE</span>

                            <strong>
                              {peer.queue_depth} /{" "}
                              {peer.queue_capacity}
                            </strong>
                          </div>

                          <div className="transport-queue-track">
                            <span
                              style={{
                                width: `${queuePercent}%`,
                              }}
                            />
                          </div>
                        </div>

                        <div className="transport-peer-metrics">
                          <PeerMetric
                            label="DELIVERY"
                            value={`${peerDelivery.toFixed(
                              1,
                            )}%`}
                          />

                          <PeerMetric
                            label="RETRIES"
                            value={String(
                              peer.retries,
                            )}
                          />

                          <PeerMetric
                            label="FAILURES"
                            value={String(
                              peer.failures,
                            )}
                          />
                        </div>

                        {peer.last_error && (
                          <div className="transport-error">
                            {peer.last_error}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                },
              )}
            </div>
          </div>
        ) : (
          <div className="empty-state">
            Waiting for an elected leader.
          </div>
        )}
      </section>

      <section className="panel transport-events-panel">
        <div className="panel-heading">
          <div>
            <h2>Transport activity</h2>
            <p>
              Latest queue, delivery, retry and failure
              events.
            </p>
          </div>
        </div>

        <div className="transport-event-stream">
          {recent.length === 0 && (
            <div className="empty-state">
              Waiting for transport events…
            </div>
          )}

          {recent.map((event) => (
            <div
              className="transport-event-row"
              key={`${event.node_id}-${event.id}`}
            >
              <div
                className={
                  event.type.includes("failure")
                    ? "transport-event-icon failure"
                    : event.type.includes("retry")
                      ? "transport-event-icon retry"
                      : "transport-event-icon"
                }
              >
                <Activity size={12} />
              </div>

              <div>
                <strong>
                  {formatTransportEvent(
                    event.type,
                  )}
                </strong>

                <span>
                  Node {event.node_id}
                  {event.data?.peer_id
                    ? ` → Node ${String(
                        event.data.peer_id,
                      )}`
                    : ""}
                </span>
              </div>

              <code>
                {event.data?.latency_ms !==
                undefined
                  ? `${Number(
                      event.data.latency_ms,
                    ).toFixed(2)} ms`
                  : event.data?.delay_ms !==
                      undefined
                    ? `retry in ${String(
                        event.data.delay_ms,
                      )} ms`
                    : ""}
              </code>
            </div>
          ))}
        </div>
      </section>
    </>
  );
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

function PeerMetric({
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

function formatTransportEvent(
  value: string,
) {
  return value
    .replace(/^transport\./, "")
    .split(".")
    .map(
      (part) =>
        part.charAt(0).toUpperCase() +
        part.slice(1),
    )
    .join(" ");
}

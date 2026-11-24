import {
  Activity,
  Radio,
  Server,
  WifiOff,
} from "lucide-react";

import type { NodeStatus, RaftEvent } from "../types";

import {
  buildHeartbeatTelemetry,
  formatAge,
} from "../telemetry/events";

interface Props {
  events: RaftEvent[];
  nodes: NodeStatus[];
}

export function HeartbeatTelemetry({
  events,
  nodes,
}: Props) {
  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const telemetry = buildHeartbeatTelemetry(
    events,
    leader?.id,
  );

  const totalSent = telemetry.reduce(
    (sum, peer) => sum + peer.sent,
    0,
  );

  const totalAccepted = telemetry.reduce(
    (sum, peer) => sum + peer.accepted,
    0,
  );

  const delivery =
    totalSent === 0
      ? 0
      : (totalAccepted / totalSent) * 100;

  const healthyPeers = telemetry.filter(
    (peer) => peer.status === "healthy",
  ).length;

  return (
    <div className="heartbeat-dashboard">
      <section className="heartbeat-summary-grid">
        <TelemetryMetric
          label="LEADER"
          value={
            leader
              ? `Node ${leader.id}`
              : "Election"
          }
          detail={
            leader
              ? `Term ${leader.term}`
              : "No leader"
          }
        />

        <TelemetryMetric
          label="DELIVERY"
          value={`${delivery.toFixed(1)}%`}
          detail="Accepted / sent"
        />

        <TelemetryMetric
          label="HEALTHY PEERS"
          value={`${healthyPeers} / 2`}
          detail="Heartbeat status"
        />

        <TelemetryMetric
          label="HEARTBEATS"
          value={String(totalSent)}
          detail="Buffered events"
        />
      </section>

      <section className="heartbeat-network-panel">
        <div className="heartbeat-panel-heading">
          <div>
            <span className="eyebrow">
              LIVE RAFT TELEMETRY
            </span>
            <h2>Heartbeat network</h2>
            <p>
              Actual heartbeat traffic derived from
              ShardRAFT SSE events.
            </p>
          </div>

          <div className="telemetry-streaming">
            <Radio size={13} />
            STREAMING
          </div>
        </div>

        {leader ? (
          <div className="heartbeat-network">
            <div className="heartbeat-leader">
              <Server size={18} />
              <span>NODE {leader.id}</span>
              <strong>LEADER</strong>
              <small>Term {leader.term}</small>
            </div>

            <div className="heartbeat-peer-grid">
              {telemetry.map((peer) => {
                const node = nodes.find(
                  (candidate) =>
                    candidate.id === peer.nodeId,
                );

                const online =
                  node?.reachable !== false;

                return (
                  <div
                    className="heartbeat-peer-card"
                    key={peer.nodeId}
                  >
                    <div
                      className={[
                        "heartbeat-path",
                        peer.status,
                      ].join(" ")}
                    >
                      <span className="heartbeat-line" />

                      {peer.pulses
                        .slice(-3)
                        .map((pulse) => (
                          <span
                            key={pulse}
                            className="heartbeat-packet"
                          />
                        ))}

                      <span className="heartbeat-arrow">
                        →
                      </span>
                    </div>

                    <div className="heartbeat-peer-node">
                      {online ? (
                        <Server size={17} />
                      ) : (
                        <WifiOff size={17} />
                      )}

                      <div>
                        <span>
                          NODE {peer.nodeId}
                        </span>

                        <strong>
                          {online
                            ? "FOLLOWER"
                            : "OFFLINE"}
                        </strong>
                      </div>

                      <span
                        className={`telemetry-health ${peer.status}`}
                      >
                        {peer.status}
                      </span>
                    </div>

                    <div className="heartbeat-peer-stats">
                      <div>
                        <span>LAST ACK</span>
                        <strong>
                          {formatAge(peer.ageMs)}
                        </strong>
                      </div>

                      <div>
                        <span>DELIVERY</span>
                        <strong>
                          {peer.deliveryPercent.toFixed(
                            1,
                          )}
                          %
                        </strong>
                      </div>

                      <div>
                        <span>ACCEPTED</span>
                        <strong>
                          {peer.accepted}
                        </strong>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : (
          <div className="heartbeat-no-leader">
            <Activity size={22} />
            <strong>
              Waiting for leader election
            </strong>
            <span>
              Heartbeat telemetry resumes when a
              leader is observed.
            </span>
          </div>
        )}
      </section>

      <section className="heartbeat-timeline-panel">
        <div className="heartbeat-panel-heading">
          <div>
            <h2>Heartbeat timeline</h2>
            <p>
              Recent leader-to-follower pulse activity.
            </p>
          </div>

          <span className="section-chip">
            LAST 12 SECONDS
          </span>
        </div>

        {telemetry.map((peer) => (
          <HeartbeatTimeline
            key={peer.nodeId}
            peer={peer}
          />
        ))}
      </section>
    </div>
  );
}

function TelemetryMetric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function HeartbeatTimeline({
  peer,
}: {
  peer: ReturnType<
    typeof buildHeartbeatTelemetry
  >[number];
}) {
  const now = Date.now();
  const windowMs = 12_000;

  const pulses = peer.pulses.filter(
    (timestamp) => now - timestamp <= windowMs,
  );

  return (
    <div className="heartbeat-timeline-row">
      <div className="heartbeat-timeline-label">
        <strong>Node {peer.nodeId}</strong>
        <span>
          {peer.status === "healthy"
            ? "Receiving"
            : peer.status === "delayed"
              ? "Delayed"
              : peer.status === "lost"
                ? "Heartbeat lost"
                : "Waiting"}
        </span>
      </div>

      <div className="heartbeat-track">
        <span className="heartbeat-track-line" />

        {pulses.map((timestamp) => {
          const age = now - timestamp;

          const left =
            100 -
            Math.min(
              100,
              (age / windowMs) * 100,
            );

          return (
            <span
              key={timestamp}
              className="heartbeat-tick"
              style={{ left: `${left}%` }}
            />
          );
        })}

        {peer.status === "lost" && (
          <span className="heartbeat-lost-marker">
            ×
          </span>
        )}
      </div>

      <div className="heartbeat-timeline-age">
        {formatAge(peer.ageMs)} ago
      </div>
    </div>
  );
}

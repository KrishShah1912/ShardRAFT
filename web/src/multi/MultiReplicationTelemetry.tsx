import { useMemo, useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import {
  correlateReplication,
  replicationObservation,
  replicationIdentity,
  commitObservation,
  type ReplicationObservation,
  type CommitObservation,
} from "./replicationTelemetry";

import {
  eventIdentity,
  isRecentEvent,
} from "./liveTelemetry";

import "./multiReplicationTelemetry.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
  selectedShard: number;
}

const COLORS = {
  sent: "#38bdf8",
  received: "#a78bfa",
  accepted: "#34d399",
  acknowledged: "#22c55e",
  rejected: "#f87171",
};

function stageLabel(
  stage: ReplicationObservation["stage"],
) {
  switch (stage) {
    case "sent":
      return "AppendEntries sent";
    case "received":
      return "Entries received";
    case "accepted":
      return "Follower accepted";
    case "acknowledged":
      return "Leader acknowledged";
    case "rejected":
      return "Request rejected";
  }
}

export function MultiReplicationTelemetry({
  cluster,
  telemetry,
  selectedShard,
}: Props) {
  const [animate, setAnimate] = useState(true);

  const shard = cluster.shards.find(
    item => item.shardId === selectedShard,
  );

  const leader = shard?.replicas.find(
    item => item.nodeId === shard.leaderId,
  );

  const followers = shard?.replicas.filter(
    item => item.nodeId !== shard.leaderId,
  ) ?? [];

  const events = useMemo(
    () => telemetry.replicationEvents.filter(
      event => event.shard_id === selectedShard,
    ),
    [telemetry.replicationEvents, selectedShard],
  );

  const requests = useMemo(
    () => correlateReplication(events),
    [events],
  );

  const commits = useMemo(
    () => events
      .map(commitObservation)
      .filter(
        (item): item is CommitObservation =>
          item !== null,
      ),
    [events],
  );

  const quorumCommit = commits.find(
    item => item.source === "leader_quorum",
  );

  const currentRequests = requests.filter(
    item =>
      item.leaderId === shard?.leaderId &&
      item.term === leader?.term,
  );

  const active = currentRequests.filter(
    item => isRecentEvent(
      item.timestamp,
      telemetry.now,
      6500,
    ),
  );

  const packetEvents = useMemo(() => {
    const known = new Set(
      requests.map(replicationIdentity),
    );

    return events.flatMap(event => {
      const observation = replicationObservation(event);

      if (
        !observation ||
        !known.has(replicationIdentity(observation))
      ) {
        return [];
      }

      // Only actual leader-side acknowledgment or rejection
      // events represent an observed response at the leader.
      const returning =
        observation.stage === "acknowledged" ||
        (
          observation.stage === "rejected" &&
          event.data.reason === "follower_rejected"
        );

      if (
        observation.stage !== "sent" &&
        !returning
      ) {
        return [];
      }

      return [{
        ...observation,
        packetKey: eventIdentity(event),
        returning,
      }];
    });
  }, [events, requests]);

  const committedRecently =
    quorumCommit !== undefined &&
    quorumCommit.nodeId === leader?.nodeId &&
    quorumCommit.term === leader?.term &&
    isRecentEvent(
      quorumCommit.timestamp,
      telemetry.now,
      6500,
    );

  const connected = telemetry.streams.filter(
    stream => stream.status === "connected",
  ).length;

  return (
    <article className="ma-panel mrt-panel">
      <header className="mrt-header">
        <div>
          <div className="mrt-eyebrow">
            REAL-TIME CONSENSUS TELEMETRY
          </div>

          <h2>Live Replication Engine</h2>

          <p>
            Shard {selectedShard} · Observed log
            transmission, acknowledgments and commits
          </p>
        </div>

        <div className="mrt-connection">
          <i className={connected === 5 ? "online" : ""} />
          {connected}/5 SSE connected
        </div>
      </header>

      <div className="mrt-toolbar">
        <span>
          {active.length > 0
            ? "Replication activity observed"
            : "Waiting for new log replication"}
        </span>

        <label>
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Animate replication
        </label>
      </div>

      <div className="mrt-stats">
        {[
          ["Leader", leader
            ? `Node ${leader.nodeId}` : "Unknown"],
          ["Current term", leader?.term ?? "—"],
          ["Observed requests", currentRequests.length],
          ["Acknowledged", currentRequests.filter(
            item => item.stage === "acknowledged",
          ).length],
          ["Rejected", currentRequests.filter(
            item => item.stage === "rejected",
          ).length],
          ["Leader commit", leader?.commitIndex ?? "—"],
        ].map(([label, value]) => (
          <div className="mrt-stat" key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </div>

      <div className="mrt-canvas">
        <svg
          viewBox="0 0 960 480"
          role="img"
          aria-label={`Shard ${selectedShard} replication topology`}
        >
          <defs>
            <pattern
              id="mrt-grid"
              width="30"
              height="30"
              patternUnits="userSpaceOnUse"
            >
              <path
                d="M30 0H0V30"
                fill="none"
                stroke="#263449"
                strokeWidth=".6"
              />
            </pattern>

            <filter id="mrt-glow">
              <feGaussianBlur stdDeviation="4" />
            </filter>
          </defs>

          <rect
            width="960"
            height="480"
            fill="url(#mrt-grid)"
            opacity=".45"
          />

          {leader ? (
            <>
              {followers.map((follower, index) => {
                const x = index === 0 ? 240 : 720;

                const forward =
                  `M480 140 Q480 230 ${x} 330`;

                const backward =
                  `M${x + (index === 0 ? 18 : -18)} 330 ` +
                  `Q480 250 ${480 + (index === 0 ? -18 : 18)} 140`;

                const observed = currentRequests.find(
                  item =>
                    item.followerId === follower.nodeId,
                );

                const recent = packetEvents.filter(
                  item =>
                    item.followerId === follower.nodeId &&
                    item.leaderId === leader.nodeId &&
                    item.term === leader.term &&
                    isRecentEvent(
                      item.timestamp,
                      telemetry.now,
                      6500,
                    ),
                ).slice(0, 6);

                const color = !follower.reachable
                  ? "#f87171"
                  : observed
                    ? COLORS[observed.stage]
                    : "#475569";

                return (
                  <g key={follower.nodeId}>
                    <path
                      d={forward}
                      fill="none"
                      stroke={color}
                      strokeWidth="2.5"
                      strokeDasharray="9 8"
                      opacity=".75"
                    />

                    <path
                      d={backward}
                      fill="none"
                      stroke="#34d399"
                      strokeWidth="1.5"
                      strokeDasharray="4 9"
                      opacity={
                        follower.reachable ? ".35" : ".08"
                      }
                    />

                    <text
                      x={index === 0 ? 250 : 710}
                      y="220"
                      fill={color}
                      textAnchor="middle"
                      fontSize="13"
                    >
                      {!follower.reachable
                        ? "Replica unavailable"
                        : observed
                          ? stageLabel(observed.stage)
                          : "No append observed"}
                    </text>

                    <text
                      x={index === 0 ? 250 : 710}
                      y="241"
                      fill="#94a3b8"
                      textAnchor="middle"
                      fontSize="12"
                    >
                      {observed
                        ? `Request #${observed.requestId}`
                        : "Awaiting log entries"}
                    </text>

                    {animate && follower.reachable &&
                      recent.map((item, packetIndex) => {
                        const returning = item.returning;

                        const path = returning
                          ? backward : forward;

                        const packetColor =
                          COLORS[item.stage];

                        return (
                          <g
                            key={item.packetKey}
                            className="mrt-motion-packet"
                            style={{
                              offsetPath: `path('${path}')`,
                              offsetRotate: "0deg",
                              animationDelay:
                                `${packetIndex * 100}ms`,
                            }}
                          >
                            <circle
                              r="16"
                              fill={packetColor}
                              opacity=".45"
                              filter="url(#mrt-glow)"
                            />

                            <rect
                              x="-10"
                              y="-7"
                              width="20"
                              height="14"
                              rx="3"
                              fill={packetColor}
                            />

                            <text
                              y="3"
                              textAnchor="middle"
                              fontSize="8"
                              fontWeight="bold"
                              fill="#07111f"
                            >
                              {returning
                                ? item.stage === "rejected"
                                  ? "ERR"
                                  : "ACK"
                                : item.entries}
                            </text>


                          </g>
                        );
                      })}

                    <rect
                      x={x - 125}
                      y="330"
                      width="250"
                      height="120"
                      rx="14"
                      fill="#111c2b"
                      stroke={
                        follower.reachable
                          ? "#34d399"
                          : "#f87171"
                      }
                      strokeWidth="1.7"
                    />

                    <text
                      x={x}
                      y="354"
                      textAnchor="middle"
                      fill={
                        follower.reachable
                          ? "#34d399"
                          : "#f87171"
                      }
                      fontSize="12"
                    >
                      {follower.reachable
                        ? follower.role.toUpperCase()
                        : "UNREACHABLE"}
                    </text>

                    <text
                      x={x}
                      y="382"
                      textAnchor="middle"
                      fill="#e2e8f0"
                      fontSize="23"
                      fontWeight="bold"
                    >
                      Node {follower.nodeId}
                    </text>

                    <text
                      x={x}
                      y="406"
                      textAnchor="middle"
                      fill="#94a3b8"
                      fontSize="13"
                    >
                      Commit {follower.commitIndex ?? "—"}
                    </text>

                    <text
                      x={x}
                      y="429"
                      textAnchor="middle"
                      fill="#94a3b8"
                      fontSize="13"
                    >
                      Applied {follower.appliedIndex ?? "—"}
                    </text>
                  </g>
                );
              })}

              <rect
                x="345"
                y="25"
                width="270"
                height="115"
                rx="15"
                fill="#102238"
                stroke="#38bdf8"
                strokeWidth="2"
              />

              <text
                x="480"
                y="51"
                textAnchor="middle"
                fontSize="12"
                fill="#38bdf8"
              >
                CURRENT LEADER
              </text>

              <text
                x="480"
                y="82"
                textAnchor="middle"
                fontSize="27"
                fontWeight="bold"
                fill="#f1f5f9"
              >
                Node {leader.nodeId}
              </text>

              <text
                x="480"
                y="108"
                textAnchor="middle"
                fontSize="14"
                fill="#94a3b8"
              >
                Term {leader.term} · Commit {leader.commitIndex}
              </text>

              <g className={
                committedRecently && animate
                  ? "mrt-quorum-flash"
                  : ""
              }>
                <rect
                  x="370"
                  y="270"
                  width="220"
                  height="42"
                  rx="10"
                  fill={
                    committedRecently
                      ? "#064e3b"
                      : "#142033"
                  }
                  stroke={
                    committedRecently
                      ? "#34d399"
                      : "#334155"
                  }
                />

                <text
                  x="480"
                  y="296"
                  textAnchor="middle"
                  fontSize="13"
                  fontWeight="bold"
                  fill={
                    committedRecently
                      ? "#6ee7b7"
                      : "#94a3b8"
                  }
                >
                  {committedRecently
                    ? `QUORUM COMMIT #${quorumCommit.commitIndex}`
                    : "AWAITING NEW COMMIT"}
                </text>
              </g>
            </>
          ) : (
            <g>
              <text
                x="480"
                y="220"
                textAnchor="middle"
                fill="#fbbf24"
                fontSize="24"
              >
                No unique leader observed
              </text>
              <text
                x="480"
                y="252"
                textAnchor="middle"
                fill="#94a3b8"
                fontSize="15"
              >
                Waiting for consensus observations
              </text>
            </g>
          )}
        </svg>
      </div>

      <div className="mrt-legend">
        {Object.entries(COLORS).map(([stage, color]) => (
          <span key={stage}>
            <i style={{ background: color }} />
            {stage}
          </span>
        ))}
      </div>

      <h3>Replica synchronization</h3>

      <div className="mrt-replicas">
        {shard?.replicas.map(replica => {
          const commit = replica.commitIndex;
          const applied = replica.appliedIndex;

          const gap =
            leader?.commitIndex != null &&
            commit != null &&
            replica.reachable
              ? Math.max(0, leader.commitIndex - commit)
              : null;

          const ratio =
            commit != null && applied != null
              ? commit === 0
                ? 100
                : Math.min(100, applied / commit * 100)
              : 0;

          return (
            <div className="mrt-replica" key={replica.nodeId}>
              <div className="mrt-replica-heading">
                <strong>Node {replica.nodeId}</strong>
                <span>{replica.role}</span>
              </div>

              <small>Applied / committed</small>

              <div className="mrt-progress">
                <div style={{ width: `${ratio}%` }} />
              </div>

              <div className="mrt-replica-heading">
                <span>
                  {replica.reachable
                    ? `${applied ?? "—"} / ${commit ?? "—"}`
                    : "Unavailable"}
                </span>
                <span>
                  Commit gap: {gap ?? "—"}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <h3>Latest observed majority commit</h3>

      <div className="mrt-commit">
        {quorumCommit ? (
          <>
            <div>
              <small>Commit advancement</small>
              <strong>
                {quorumCommit.previousCommit}
                {" → "}
                {quorumCommit.commitIndex}
              </strong>
            </div>
            <div>
              <small>Replicas / majority</small>
              <strong>
                {quorumCommit.replicas ?? "—"}
                {" / "}
                {quorumCommit.majority ?? "—"}
              </strong>
            </div>
            <div>
              <small>Entries committed</small>
              <strong>+{quorumCommit.advancedBy}</strong>
            </div>
            <div>
              <small>Observed leader</small>
              <strong>Node {quorumCommit.nodeId}</strong>
            </div>
          </>
        ) : (
          <p>No majority commit event retained yet.</p>
        )}
      </div>

      <h3>Replication request monitor</h3>

      <div className="ma-table-wrap mrt-request-table">
        <table>
          <thead>
            <tr>
              <th>Request</th>
              <th>Leader → Follower</th>
              <th>Term</th>
              <th>Entries</th>
              <th>Log range</th>
              <th>Observed stage</th>
            </tr>
          </thead>

          <tbody>
            {requests.slice(0, 20).map(item => (
              <tr key={replicationIdentity(item)}>
                <td>#{item.requestId}</td>
                <td>
                  N{item.leaderId} → N{item.followerId}
                </td>
                <td>{item.term}</td>
                <td>{item.entries}</td>
                <td>
                  {item.firstIndex ?? "—"}
                  {" – "}
                  {item.lastIndex ?? "—"}
                </td>
                <td>
                  <span
                    className="mrt-stage"
                    style={{ color: COLORS[item.stage] }}
                    title={item.reason ?? undefined}
                  >
                    {stageLabel(item.stage)}
                  </span>
                </td>
              </tr>
            ))}

            {requests.length === 0 && (
              <tr>
                <td colSpan={6}>
                  Execute a mutation to observe replication.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <h3>Live replication event timeline</h3>

      <div className="mrt-timeline">
        {events.slice(0, 30).map(event => (
          <div
            className="mrt-event"
            key={eventIdentity(event)}
          >
            <i className={
              event.type === "raft.commit.advanced"
                ? "commit"
                : event.type === "raft.append.rejected"
                  ? "rejected"
                  : ""
            } />

            <time>
              {new Date(event.timestamp)
                .toLocaleTimeString()}
            </time>

            <strong>Node {event.node_id}</strong>

            <span>
              {event.type.replace("raft.", "")}
            </span>

            <code>
              {JSON.stringify(event.data)}
            </code>
          </div>
        ))}

        {events.length === 0 && (
          <p>Waiting for replication events.</p>
        )}
      </div>

      <p className="mrt-note">
        Animations represent observed events at a
        deliberately slowed presentation speed.
        They do not measure network latency.
        History is bounded and SSE delivery is best-effort.
        Follower acceptance alone does not establish
        majority commitment.
      </p>
    </article>
  );
}

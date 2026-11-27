import { useMemo, useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import type {
  DataShardID,
} from "../api/multiDiagnostics";

import {
  DATA_SHARDS,
} from "./clusterState";

import {
  electionEventLabel,
  electionEventColor,
  electionTerms,
  electionTimeline,
} from "./electionTelemetry";

import {
  eventIdentity,
  type LiveEvent,
} from "./liveTelemetry";

import { MultiElectionControls } from "./MultiElectionControls";

import "./multiElectionTelemetry.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

function describe(event: LiveEvent): string {
  switch (event.type) {
    case "raft.vote.requested":
      return `Node ${event.node_id} → Node ${event.data.to}`;

    case "raft.vote.granted":
    case "raft.vote.rejected":
      return `Node ${event.node_id} → Node ${event.data.candidate_id}`;

    case "raft.term.changed":
      return `Term ${event.data.previous_term} → ${event.term}`;

    case "raft.leader.elected":
      return `Node ${event.node_id} elected leader`;

    case "raft.election.started":
      return `Node ${event.node_id} started election`;

    case "raft.role.changed":
      return `Node ${event.node_id}: role ${event.data.previous_role} → ${event.data.role}`;

    default:
      return `Node ${event.node_id}`;
  }
}

export function MultiElectionTelemetry({
  cluster,
  telemetry,
}: Props) {
  const [selectedShard, setSelectedShard] =
    useState<DataShardID>(1);

  const [selectedTerm, setSelectedTerm] =
    useState("latest");

  const [animate, setAnimate] = useState(true);

  const shard = cluster.shards.find(
    item => item.shardId === selectedShard,
  );

  const terms = useMemo(
    () => electionTerms(
      telemetry.electionEvents,
      selectedShard,
    ),
    [telemetry.electionEvents, selectedShard],
  );

  const term = selectedTerm === "latest"
    ? terms[0]
    : Number(selectedTerm);

  const timeline = useMemo(
    () => electionTimeline(
      telemetry.electionEvents,
      selectedShard,
      term,
    ),
    [telemetry.electionEvents, selectedShard, term],
  );

  const elected = [...timeline].reverse().find(
    event => event.type === "raft.leader.elected",
  );

  const started = [...timeline].reverse().find(
    event => event.type === "raft.election.started",
  );

  const candidate = elected?.node_id ?? started?.node_id;

  const requests = timeline.filter(
    event =>
      event.type === "raft.vote.requested" &&
      event.node_id === candidate,
  );

  const grants = timeline.filter(
    event =>
      event.type === "raft.vote.granted" &&
      event.data.candidate_id === candidate,
  );

  const rejections = timeline.filter(
    event =>
      event.type === "raft.vote.rejected" &&
      event.data.candidate_id === candidate,
  );

  const leader = shard?.replicas.find(
    replica => replica.nodeId === shard.leaderId,
  );

  const focusNode = candidate ?? shard?.leaderId;

  const peers = shard?.replicas.filter(
    replica => replica.nodeId !== focusNode,
  ) ?? [];

  const liveElection = timeline.some(event => {
    const age = telemetry.now - Date.parse(event.timestamp);
    return age >= 0 && age <= 6000;
  });

  return (
    <article className="ma-panel me-panel">
      <div className="me-header">
        <div>
          <div className="eyebrow">
            LIVE CONSENSUS TELEMETRY
          </div>

          <h2>Raft Leader Elections</h2>

          <p>
            Observed election events, voting and
            leadership transitions.
          </p>
        </div>

        <span className="me-badge">
          {telemetry.connectedNodes}/5 SSE connected
        </span>
      </div>

      <div className="me-toolbar">
        <div className="mc-shard-selector">
          {DATA_SHARDS.map(id => (
            <button
              type="button"
              key={id}
              className={
                selectedShard === id ? "mc-selected" : ""
              }
              onClick={() => {
                setSelectedShard(id);
                setSelectedTerm("latest");
              }}
            >
              Shard {id}
            </button>
          ))}
        </div>

        <label>
          Election term{" "}
          <select
            value={selectedTerm}
            onChange={event =>
              setSelectedTerm(event.target.value)
            }
          >
            <option value="latest">Latest observed</option>

            {terms.map(value => (
              <option key={value} value={value}>
                Term {value}
              </option>
            ))}
          </select>
        </label>

        <label>
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          {" "}Animate votes
        </label>
      </div>

      <MultiElectionControls
        cluster={cluster}
        shardId={selectedShard}
        onElectionTrigger={() => {
          setSelectedTerm("latest");
          setAnimate(true);
        }}
      />

      <div className="me-stats">
        {[
          ["Current leader", shard?.leaderId
            ? `Node ${shard.leaderId}` : "Unknown"],
          ["Current leader term", leader?.term ?? "—"],
          ["Selected election term", term ?? "—"],
          ["Votes granted (observed)",
            new Set(grants.map(event => event.node_id)).size],
          ["Votes rejected (observed)",
            new Set(rejections.map(event => event.node_id)).size],
        ].map(([label, value]) => (
          <div className="me-stat" key={label}>
            <small>{label}</small>
            <strong>{value}</strong>
          </div>
        ))}
      </div>

      <div className="me-status">
        {liveElection
          ? "Recent election activity"
          : elected
            ? "Recorded election — historical observation"
            : timeline.length
              ? "Partial election history"
              : "Waiting for election events"}
      </div>

      {focusNode ? (
        <div className="me-diagram">
          <svg
            viewBox="0 0 800 320"
            role="img"
            aria-label="Observed Raft election topology"
          >
            {peers.map((peer, index) => {
              const x = index === 0 ? 190 : 610;

              const requested = requests.some(
                event => event.data.to === peer.nodeId,
              );

              const responses = timeline.filter(
                event =>
                  event.node_id === peer.nodeId &&
                  event.data.candidate_id === candidate &&
                  (
                    event.type === "raft.vote.granted" ||
                    event.type === "raft.vote.rejected"
                  ),
              );

              const response = responses[responses.length - 1];

              const color = response
                ? electionEventColor(response)
                : requested ? "#60a5fa" : "#475569";

              const outgoing =
                `M400 90 Q400 155 ${x} 230`;

              const incoming =
                `M${x} 230 Q${x} 130 400 90`;

              const traffic = timeline.filter(event =>
                (
                  event.type === "raft.vote.requested" &&
                  event.node_id === candidate &&
                  event.data.to === peer.nodeId
                ) ||
                (
                  event.node_id === peer.nodeId &&
                  event.data.candidate_id === candidate &&
                  (
                    event.type === "raft.vote.granted" ||
                    event.type === "raft.vote.rejected"
                  )
                ),
              ).filter(event => {
                const age =
                  telemetry.now - Date.parse(event.timestamp);

                return age >= 0 && age <= 6000;
              }).slice(-6);

              return (
                <g key={peer.nodeId}>
                  <path
                    d={outgoing}
                    stroke={requested ? "#60a5fa" : "#475569"}
                    strokeWidth="2"
                    strokeDasharray="7 7"
                    fill="none"
                  />

                  {response && (
                    <path
                      d={incoming}
                      stroke={color}
                      strokeWidth="2"
                      strokeDasharray="7 7"
                      fill="none"
                    />
                  )}

                  {animate && traffic.map(event => (
                    <circle
                      key={eventIdentity(event)}
                      r="5"
                      fill={electionEventColor(event)}
                      className="me-packet"
                      style={{
                        offsetPath: `path("${
                          event.type === "raft.vote.requested"
                            ? outgoing
                            : incoming
                        }")`,
                      }}
                    />
                  ))}

                  <text
                    x={index === 0 ? 235 : 565}
                    y="180"
                    textAnchor="middle"
                    fill={color}
                    fontSize="12"
                  >
                    {response
                      ? electionEventLabel(response)
                      : requested
                        ? "Vote requested"
                        : "No vote observed"}
                  </text>

                  <rect
                    x={x - 85}
                    y="230"
                    width="170"
                    height="68"
                    rx="12"
                    fill="#111827"
                    stroke={color}
                  />

                  <text
                    x={x}
                    y="256"
                    textAnchor="middle"
                    fill="#e2e8f0"
                    fontSize="18"
                    fontWeight="600"
                  >
                    Node {peer.nodeId}
                  </text>

                  <text
                    x={x}
                    y="278"
                    textAnchor="middle"
                    fill="#94a3b8"
                    fontSize="12"
                  >
                    Current role: {peer.role}
                  </text>
                </g>
              );
            })}

            <rect
              x="310"
              y="15"
              width="180"
              height="75"
              rx="12"
              fill="#111827"
              stroke={elected || !candidate
                ? "#34d399" : "#fbbf24"}
              strokeWidth="2"
            />

            <text
              x="400"
              y="37"
              textAnchor="middle"
              fontSize="11"
              fill={elected || !candidate
                ? "#34d399" : "#fbbf24"}
            >
              {elected
                ? "ELECTED LEADER"
                : candidate
                  ? "OBSERVED CANDIDATE"
                  : "CURRENT LEADER"}
            </text>

            <text
              x="400"
              y="60"
              textAnchor="middle"
              fontSize="20"
              fontWeight="600"
              fill="#e2e8f0"
            >
              Node {focusNode}
            </text>

            <text
              x="400"
              y="78"
              textAnchor="middle"
              fontSize="12"
              fill="#94a3b8"
            >
              Term {term ?? leader?.term ?? "—"}
            </text>
          </svg>
        </div>
      ) : (
        <div className="me-empty">
          No leader or election candidate currently observed.
        </div>
      )}

      <div className="me-legend">
        <span style={{color: "#60a5fa"}}>
          ● Vote requested
        </span>
        <span style={{color: "#34d399"}}>
          ● Vote granted
        </span>
        <span style={{color: "#f87171"}}>
          ● Vote rejected
        </span>
        <span style={{color: "#fbbf24"}}>
          ● Candidate
        </span>
      </div>

      <h3>Election event timeline</h3>

      {timeline.length === 0 ? (
        <div className="me-empty">
          No election events retained for this shard.
          A stable cluster may remain idle here.
          New elections will appear automatically.
        </div>
      ) : (
        <div className="me-timeline">
          {timeline.slice(-100).map(event => (
            <div
              className="me-event"
              key={eventIdentity(event)}
            >
              <span
                className="me-dot"
                style={{
                  background: electionEventColor(event),
                }}
              />

              <div>
                <strong>{electionEventLabel(event)}</strong>
                <small>{describe(event)}</small>
              </div>

              <span>Term {event.term ?? "—"}</span>

              <time>
                {new Date(event.timestamp).toLocaleTimeString()}
              </time>
            </div>
          ))}
        </div>
      )}

      <p className="me-note">
        Three replicas require two votes for a majority.
        Vote counts show observed remote decisions, not
        confirmed delivery to the candidate. Self-votes
        are not included. Historical election events
        and current replica roles are separate observations.
        Metadata election events are not available.
      </p>
    </article>
  );
}

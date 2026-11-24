import {
  Activity,
  ArrowRight,
  Boxes,
  Braces,
  ChevronRight,
  CircleDot,
  Crown,
  Database,
  FileStack,
  GitBranch,
  HardDrive,
  Network,
  Radio,
  Server,
  ShieldCheck,
  Workflow,
  X,
  Zap,
} from "lucide-react";

import {
  useMemo,
  useState,
} from "react";

import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

type ArchitectureView =
  | "system"
  | "write"
  | "consensus"
  | "recovery";

type ComponentKey =
  | "frontend"
  | "supervisor"
  | "api"
  | "shard"
  | "raft"
  | "transport"
  | "wal"
  | "snapshot"
  | "kv"
  | "pebble"
  | "observability";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  transport?: TransportStats;
  connected: boolean;
}

interface ComponentDetail {
  title: string;
  subtitle: string;
  description: string;
  responsibilities: string[];
  files: string[];
  communicatesWith: string[];
}

const details: Record<
  ComponentKey,
  ComponentDetail
> = {
  frontend: {
    title: "ShardRAFT Console",
    subtitle: "React / Vite control plane",
    description:
      "Interactive browser interface for observing consensus, controlling the local development cluster, issuing KV operations, inspecting storage and visualizing live telemetry.",
    responsibilities: [
      "Poll node status and administrative debug endpoints.",
      "Consume the server-sent event stream.",
      "Visualize leader election, replication, transport and storage state.",
      "Issue PUT, DELETE and CAS operations.",
      "Control local node lifecycle through the development supervisor.",
    ],
    files: [
      "web/src/App.tsx",
      "web/src/api/client.ts",
      "web/src/realtime/events.ts",
      "web/src/features/*",
    ],
    communicatesWith: [
      "shardraftd HTTP APIs",
      "SSE observability stream",
      "Development supervisor",
    ],
  },

  supervisor: {
    title: "Development Supervisor",
    subtitle: "Local process control plane",
    description:
      "Development-only service that owns the three shardraftd processes and exposes restricted start, stop and restart operations to the frontend.",
    responsibilities: [
      "Start Node 1, Node 2 and Node 3.",
      "Stop or restart individual node processes.",
      "Report process PID, reachability and startup state.",
      "Keep arbitrary shell execution outside the browser API.",
    ],
    files: [
      "cmd/shardraft-supervisor/main.go",
      "scripts/dev.sh",
    ],
    communicatesWith: [
      "Frontend control API",
      "Node 1 process",
      "Node 2 process",
      "Node 3 process",
    ],
  },

  api: {
    title: "HTTP API Layer",
    subtitle: "Client, admin and Raft endpoints",
    description:
      "Each shardraftd process exposes client KV operations, Raft peer messaging, node status, administrative inspection and event streaming.",
    responsibilities: [
      "Accept client KV requests.",
      "Receive peer Raft messages.",
      "Expose node and Raft debug state.",
      "Expose KV, storage and transport snapshots.",
      "Stream observability events.",
    ],
    files: [
      "cmd/shardraftd/http.go",
      "cmd/shardraftd/kv_http.go",
      "cmd/shardraftd/raft_http.go",
      "cmd/shardraftd/admin_http.go",
    ],
    communicatesWith: [
      "Shard",
      "HTTPTransport",
      "Observability Bus",
    ],
  },

  shard: {
    title: "Shard Runtime",
    subtitle: "Consensus + state-machine orchestration",
    description:
      "The shard layer coordinates Raft consensus with durable state-machine application and recovery. It is the runtime boundary around one replicated Raft group.",
    responsibilities: [
      "Drive the Raft state machine.",
      "Track pending client requests.",
      "Apply committed entries.",
      "Coordinate recovery and compaction.",
      "Expose debug state to administrative APIs.",
    ],
    files: [
      "internal/shard/shard.go",
      "internal/shard/pending_requests.go",
      "internal/shard/compaction.go",
      "internal/shard/debug.go",
    ],
    communicatesWith: [
      "Raft",
      "KV engine",
      "WAL / snapshot",
      "Transport send callback",
    ],
  },

  raft: {
    title: "Raft Consensus",
    subtitle: "Election, replication and commit",
    description:
      "Raft provides leader election, replicated-log consistency, quorum commit advancement and follower catch-up for the shard.",
    responsibilities: [
      "Follower / Candidate / Leader transitions.",
      "Election timeout and voting.",
      "AppendEntries replication.",
      "Maintain nextIndex and matchIndex.",
      "Advance commit index after quorum replication.",
      "Handle log truncation and compaction boundaries.",
    ],
    files: [
      "internal/raft/raft.go",
      "internal/raft/election.go",
      "internal/raft/replication.go",
      "internal/raft/compaction.go",
    ],
    communicatesWith: [
      "Shard runtime",
      "HTTP transport",
      "Observability events",
    ],
  },

  transport: {
    title: "HTTP Transport",
    subtitle: "Independent per-peer delivery",
    description:
      "Outbound Raft messages are delivered through independent bounded queues and workers for each peer, preventing one slow peer from directly blocking another peer's delivery path.",
    responsibilities: [
      "Maintain one bounded queue per peer.",
      "Preserve per-peer message ordering.",
      "POST Raft messages to /raft/message.",
      "Retry failed delivery with bounded exponential backoff.",
      "Report queue backpressure.",
      "Record delivery latency and transport counters.",
    ],
    files: [
      "internal/transport/http.go",
      "internal/transport/retry.go",
      "internal/transport/stats.go",
      "cmd/shardraftd/transport_send.go",
    ],
    communicatesWith: [
      "Local Raft node",
      "Remote shardraftd peers",
      "Observability Bus",
    ],
  },

  wal: {
    title: "Segmented WAL",
    subtitle: "Durable Raft history",
    description:
      "The write-ahead log persists Raft history required for crash recovery and supports segmented storage, checkpoint publication and compaction.",
    responsibilities: [
      "Persist Raft log state.",
      "Rotate and manage WAL segments.",
      "Recover after crashes and partial writes.",
      "Support physical/logical compaction.",
      "Coordinate checkpoint generations.",
    ],
    files: [
      "internal/wal/segmented.go",
      "internal/wal/segmented_write.go",
      "internal/wal/segmented_recovery.go",
      "internal/wal/compaction.go",
    ],
    communicatesWith: [
      "Raft driver",
      "Snapshot recovery",
      "Filesystem",
    ],
  },

  snapshot: {
    title: "Snapshot Layer",
    subtitle: "Compacted recovery state",
    description:
      "Snapshots provide a compact durable representation of applied state so recovery does not require replaying an indefinitely growing log.",
    responsibilities: [
      "Persist applied state.",
      "Track applied index and term.",
      "Persist deduplication metadata.",
      "Restore compacted state during recovery.",
    ],
    files: [
      "internal/snapshot/snapshot.go",
      "internal/snapshot/dedup.go",
      "internal/raftdriver/snapshot_recovery.go",
    ],
    communicatesWith: [
      "Shard",
      "KV engine",
      "WAL recovery",
    ],
  },

  kv: {
    title: "KV State Machine",
    subtitle: "Replicated application state",
    description:
      "Committed Raft commands are applied to the key-value state machine, which implements mutation semantics, CAS behavior and request deduplication.",
    responsibilities: [
      "Apply committed PUT operations.",
      "Apply DELETE operations.",
      "Perform compare-and-swap.",
      "Deduplicate repeated client requests.",
      "Maintain applied-index consistency.",
    ],
    files: [
      "internal/kvstore/engine.go",
      "internal/kvstore/command.go",
      "internal/kvstore/dedup.go",
      "internal/kvstore/store.go",
    ],
    communicatesWith: [
      "Shard runtime",
      "Pebble store",
      "Snapshot layer",
    ],
  },

  pebble: {
    title: "Pebble",
    subtitle: "Persistent materialized KV state",
    description:
      "Pebble backs the durable materialized key-value state exposed by the replicated state machine.",
    responsibilities: [
      "Persist key-value records.",
      "Serve recovered state.",
      "Provide durable local storage beneath the KV engine.",
    ],
    files: [
      "internal/kvstore/pebble.go",
      "internal/kvstore/engine_store.go",
    ],
    communicatesWith: [
      "KV engine",
      "Local disk",
    ],
  },

  observability: {
    title: "Observability Bus",
    subtitle: "Real-time event backbone",
    description:
      "A bounded in-memory event bus receives Raft and transport telemetry and distributes it to history queries and live SSE subscribers.",
    responsibilities: [
      "Assign ordered event IDs.",
      "Retain bounded event history.",
      "Publish Raft events.",
      "Publish transport events.",
      "Fan out live events to SSE subscribers.",
    ],
    files: [
      "internal/observability/event.go",
      "internal/observability/bus.go",
      "cmd/shardraftd/transport_observability.go",
    ],
    communicatesWith: [
      "Raft",
      "Transport",
      "Admin HTTP",
      "Frontend SSE client",
    ],
  },
};

export function Architecture({
  nodes,
  events,
  transport,
  connected,
}: Props) {
  const [view, setView] =
    useState<ArchitectureView>(
      "system",
    );

  const [selected, setSelected] =
    useState<ComponentKey>();

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const reachable = nodes.filter(
    (node) =>
      node.reachable !== false,
  ).length;

  const recentReplication =
    events.filter(
      (event) =>
        event.type ===
          "raft.append.sent" ||
        event.type ===
          "raft.commit.advanced",
    ).length;

  const transportDelivery =
    (transport?.sent ?? 0) === 0
      ? 100
      : Math.min(
          100,
          ((transport?.delivered ??
            0) /
            (transport?.sent ??
              1)) *
            100,
        );

  return (
    <>
      <header className="page-header architecture-header">
        <div>
          <div className="eyebrow">
            SYSTEM DESIGN
          </div>

          <h1>
            ShardRAFT Architecture
          </h1>

          <p>
            Explore the complete request,
            consensus, persistence, transport
            and observability architecture.
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
            ? "LIVE SYSTEM"
            : "RECONNECTING"}
        </div>
      </header>

      <section className="architecture-kpis">
        <ArchKPI
          label="NODES"
          value={`${reachable} / 3`}
          detail="reachable"
        />

        <ArchKPI
          label="LEADER"
          value={
            leader
              ? `Node ${leader.id}`
              : "Election"
          }
          detail={
            leader
              ? `term ${leader.term}`
              : "pending"
          }
        />

        <ArchKPI
          label="COMMIT"
          value={String(
            leader?.commit_index ??
              "—",
          )}
          detail="Raft index"
        />

        <ArchKPI
          label="TRANSPORT"
          value={`${transportDelivery.toFixed(
            1,
          )}%`}
          detail="delivery"
        />

        <ArchKPI
          label="EVENTS"
          value={String(events.length)}
          detail="buffered"
        />
      </section>

      <section className="architecture-view-switcher">
        <ViewButton
          icon={Boxes}
          title="System"
          description="Complete component map"
          active={view === "system"}
          onClick={() =>
            setView("system")
          }
        />

        <ViewButton
          icon={Zap}
          title="Write Path"
          description="Client → durable commit"
          active={view === "write"}
          onClick={() =>
            setView("write")
          }
        />

        <ViewButton
          icon={GitBranch}
          title="Consensus"
          description="Election + replication"
          active={
            view === "consensus"
          }
          onClick={() =>
            setView("consensus")
          }
        />

        <ViewButton
          icon={HardDrive}
          title="Recovery"
          description="WAL + snapshot restore"
          active={
            view === "recovery"
          }
          onClick={() =>
            setView("recovery")
          }
        />
      </section>

      {view === "system" && (
        <SystemArchitecture
          nodes={nodes}
          leaderId={leader?.id}
          onSelect={setSelected}
        />
      )}

      {view === "write" && (
        <WriteArchitecture
          leaderId={leader?.id}
          onSelect={setSelected}
        />
      )}

      {view ===
        "consensus" && (
        <ConsensusArchitecture
          nodes={nodes}
          leaderId={leader?.id}
          replicationEvents={
            recentReplication
          }
          onSelect={setSelected}
        />
      )}

      {view === "recovery" && (
        <RecoveryArchitecture
          onSelect={setSelected}
        />
      )}

      <section className="architecture-principles">
        <div className="section-heading">
          <div>
            <h2>
              Design properties
            </h2>

            <p>
              Core properties expressed by
              the current implementation.
            </p>
          </div>
        </div>

        <div className="architecture-principle-grid">
          <Principle
            icon={Crown}
            title="Single Raft leader"
            text="Client mutations are ordered through the elected leader of the replicated group."
          />

          <Principle
            icon={Network}
            title="Peer isolation"
            text="Each transport peer has its own bounded queue and delivery worker."
          />

          <Principle
            icon={HardDrive}
            title="Crash recovery"
            text="WAL and snapshot state reconstruct durable consensus and application state."
          />

          <Principle
            icon={ShieldCheck}
            title="Request safety"
            text="Deduplication and CAS semantics protect application-level mutations."
          />

          <Principle
            icon={Activity}
            title="Observable"
            text="Raft and transport transitions flow through a bounded event bus into live SSE telemetry."
          />

          <Principle
            icon={Workflow}
            title="Layered runtime"
            text="HTTP, Shard, Raft, storage and transport remain separated behind explicit boundaries."
          />
        </div>
      </section>

      {selected && (
        <ArchitectureInspector
          component={selected}
          onClose={() =>
            setSelected(undefined)
          }
        />
      )}
    </>
  );
}

function SystemArchitecture({
  nodes,
  leaderId,
  onSelect,
}: {
  nodes: NodeStatus[];
  leaderId?: number;
  onSelect: (
    key: ComponentKey,
  ) => void;
}) {
  return (
    <section className="architecture-canvas">
      <ArchitectureSectionLabel
        number="01"
        title="Control & Client Plane"
      />

      <div className="architecture-control-plane">
        <ArchitectureNode
          icon={Braces}
          title="React Console"
          subtitle="localhost:5173"
          tag="CONTROL PLANE"
          onClick={() =>
            onSelect("frontend")
          }
        />

        <FlowArrow
          label="HTTP / SSE"
          animated
        />

        <ArchitectureNode
          icon={Workflow}
          title="Dev Supervisor"
          subtitle="localhost:8090"
          tag="PROCESS CONTROL"
          onClick={() =>
            onSelect("supervisor")
          }
        />
      </div>

      <ArchitectureSectionLabel
        number="02"
        title="Replicated Raft Group"
      />

      <div className="architecture-cluster-shell">
        <div className="architecture-cluster-top">
          <span>
            RAFT GROUP 01
          </span>

          <strong>
            3 voting members · quorum 2
          </strong>
        </div>

        <div className="architecture-node-grid">
          {[1, 2, 3].map((id) => {
            const node =
              nodes.find(
                (candidate) =>
                  candidate.id === id,
              );

            return (
              <RaftArchitectureNode
                key={id}
                id={id}
                node={node}
                leader={
                  leaderId === id
                }
                onSelect={onSelect}
              />
            );
          })}
        </div>

        <div className="architecture-peer-network">
          <span className="peer-network-line" />

          <div>
            <Radio size={12} />
            HTTP RAFT TRANSPORT
          </div>

          <span className="peer-network-line" />
        </div>
      </div>

      <ArchitectureSectionLabel
        number="03"
        title="Durability & State Machine"
      />

      <div className="architecture-storage-row">
        <ArchitectureNode
          icon={FileStack}
          title="Segmented WAL"
          subtitle="Raft durability"
          tag="RECOVERY"
          onClick={() =>
            onSelect("wal")
          }
        />

        <FlowArrow label="checkpoint" />

        <ArchitectureNode
          icon={HardDrive}
          title="Snapshot"
          subtitle="Compacted state"
          tag="RECOVERY"
          onClick={() =>
            onSelect("snapshot")
          }
        />

        <FlowArrow label="restore/apply" />

        <ArchitectureNode
          icon={Database}
          title="KV Engine"
          subtitle="Replicated state"
          tag="STATE MACHINE"
          onClick={() =>
            onSelect("kv")
          }
        />

        <FlowArrow label="persist" />

        <ArchitectureNode
          icon={CircleDot}
          title="Pebble"
          subtitle="Materialized KV"
          tag="STORAGE"
          onClick={() =>
            onSelect("pebble")
          }
        />
      </div>

      <div className="architecture-observability-strip">
        <ArchitectureNode
          icon={Activity}
          title="Observability Bus"
          subtitle="Raft + transport events"
          tag="REAL-TIME"
          wide
          onClick={() =>
            onSelect(
              "observability",
            )
          }
        />

        <div className="architecture-observability-flow">
          <span>
            Raft events
          </span>
          <ChevronRight
            size={12}
          />
          <span>
            Transport events
          </span>
          <ChevronRight
            size={12}
          />
          <span>
            History
          </span>
          <ChevronRight
            size={12}
          />
          <span>SSE</span>
          <ChevronRight
            size={12}
          />
          <span>Frontend</span>
        </div>
      </div>
    </section>
  );
}

function RaftArchitectureNode({
  id,
  node,
  leader,
  onSelect,
}: {
  id: number;
  node?: NodeStatus;
  leader: boolean;
  onSelect: (
    key: ComponentKey,
  ) => void;
}) {
  const online =
    node?.reachable !== false;

  return (
    <div
      className={[
        "architecture-raft-node",
        leader ? "leader" : "",
        !online ? "offline" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className="architecture-raft-header">
        <Server size={15} />

        <div>
          <strong>
            Node {id}
          </strong>

          <span>
            :808{id}
          </span>
        </div>

        <em>
          {!online
            ? "OFFLINE"
            : leader
              ? "LEADER"
              : node?.role === 1
                ? "CANDIDATE"
                : "FOLLOWER"}
        </em>
      </div>

      <div className="architecture-live-stats">
        <div>
          <span>TERM</span>
          <strong>
            {online
              ? node?.term ?? 0
              : "—"}
          </strong>
        </div>

        <div>
          <span>COMMIT</span>
          <strong>
            {online
              ? node?.commit_index ??
                0
              : "—"}
          </strong>
        </div>

        <div>
          <span>APPLIED</span>
          <strong>
            {online
              ? node?.applied_index ??
                0
              : "—"}
          </strong>
        </div>
      </div>

      <div className="architecture-node-layers">
        <button
          onClick={() =>
            onSelect("api")
          }
        >
          HTTP API
        </button>

        <button
          onClick={() =>
            onSelect("shard")
          }
        >
          Shard
        </button>

        <button
          onClick={() =>
            onSelect("raft")
          }
        >
          Raft
        </button>

        <button
          onClick={() =>
            onSelect(
              "transport",
            )
          }
        >
          Transport
        </button>
      </div>
    </div>
  );
}

function WriteArchitecture({
  leaderId,
  onSelect,
}: {
  leaderId?: number;
  onSelect: (
    key: ComponentKey,
  ) => void;
}) {
  const steps: {
    number: string;
    key: ComponentKey;
    title: string;
    subtitle: string;
    detail: string;
  }[] = [
    {
      number: "01",
      key: "frontend",
      title: "Client Request",
      subtitle: "PUT / DELETE / CAS",
      detail:
        "The browser or external client sends a mutation to the current leader.",
    },
    {
      number: "02",
      key: "api",
      title: "HTTP Handler",
      subtitle: `Node ${leaderId ?? "?"}`,
      detail:
        "The daemon validates and converts the request into a replicated command.",
    },
    {
      number: "03",
      key: "shard",
      title: "Shard Proposal",
      subtitle: "Pending request",
      detail:
        "The shard tracks the request while proposing its command into Raft.",
    },
    {
      number: "04",
      key: "raft",
      title: "Raft Log",
      subtitle: "Append locally",
      detail:
        "The leader appends the command at the next log index.",
    },
    {
      number: "05",
      key: "transport",
      title: "Replication",
      subtitle: "AppendEntries",
      detail:
        "Independent transport workers deliver the entry to followers.",
    },
    {
      number: "06",
      key: "raft",
      title: "Quorum",
      subtitle: "2 / 3 replicas",
      detail:
        "matchIndex progress allows the leader to advance commitIndex.",
    },
    {
      number: "07",
      key: "kv",
      title: "Apply",
      subtitle: "State machine",
      detail:
        "Committed commands are applied deterministically to the KV engine.",
    },
    {
      number: "08",
      key: "pebble",
      title: "Persist",
      subtitle: "Pebble",
      detail:
        "The materialized key-value state is persisted locally.",
    },
  ];

  return (
    <section className="architecture-canvas">
      <ArchitectureSectionLabel
        number="WRITE"
        title="Replicated Mutation Lifecycle"
      />

      <div className="write-path">
        {steps.map(
          (step, index) => (
            <div
              className="write-path-wrapper"
              key={`${step.number}-${step.title}`}
            >
              <button
                className="write-path-step"
                onClick={() =>
                  onSelect(step.key)
                }
              >
                <span className="write-step-number">
                  {step.number}
                </span>

                <div>
                  <strong>
                    {step.title}
                  </strong>

                  <em>
                    {step.subtitle}
                  </em>

                  <p>
                    {step.detail}
                  </p>
                </div>
              </button>

              {index <
                steps.length - 1 && (
                <div className="write-path-arrow">
                  <span />
                  <ArrowRight
                    size={13}
                  />
                </div>
              )}
            </div>
          ),
        )}
      </div>

      <div className="architecture-guarantee">
        <ShieldCheck size={18} />

        <div>
          <strong>
            Response follows replicated
            state-machine progress
          </strong>

          <span>
            A mutation travels through
            proposal, replication, quorum
            commit and application rather
            than directly modifying follower
            storage.
          </span>
        </div>
      </div>
    </section>
  );
}

function ConsensusArchitecture({
  nodes,
  leaderId,
  replicationEvents,
  onSelect,
}: {
  nodes: NodeStatus[];
  leaderId?: number;
  replicationEvents: number;
  onSelect: (
    key: ComponentKey,
  ) => void;
}) {
  return (
    <section className="architecture-canvas">
      <ArchitectureSectionLabel
        number="RAFT"
        title="Consensus Protocol"
      />

      <div className="consensus-architecture">
        <div className="consensus-election-flow">
          <ProtocolStage
            number="1"
            title="Follower"
            text="Receives periodic leader heartbeats."
          />

          <ProtocolArrow
            text="election timeout"
          />

          <ProtocolStage
            number="2"
            title="Candidate"
            text="Increments term and requests votes."
          />

          <ProtocolArrow
            text="majority vote"
          />

          <ProtocolStage
            number="3"
            title="Leader"
            text="Replicates log entries and heartbeats."
          />
        </div>

        <div className="consensus-live-cluster">
          {[1, 2, 3].map((id) => {
            const node =
              nodes.find(
                (candidate) =>
                  candidate.id === id,
              );

            return (
              <button
                key={id}
                className={[
                  "consensus-live-node",
                  leaderId === id
                    ? "leader"
                    : "",
                  node?.reachable ===
                  false
                    ? "offline"
                    : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
                onClick={() =>
                  onSelect("raft")
                }
              >
                <Server
                  size={16}
                />

                <strong>
                  Node {id}
                </strong>

                <span>
                  {node?.reachable ===
                  false
                    ? "OFFLINE"
                    : leaderId === id
                      ? "LEADER"
                      : node?.role ===
                          1
                        ? "CANDIDATE"
                        : "FOLLOWER"}
                </span>

                <small>
                  term{" "}
                  {node?.term ?? "—"}
                </small>
              </button>
            );
          })}
        </div>

        <div className="consensus-replication-flow">
          <div>
            <span>LEADER</span>
            <strong>
              AppendEntries
            </strong>
          </div>

          <div className="replication-animation">
            <span className="replication-flow-line" />
            <span className="replication-flow-packet">
              LOG
            </span>
          </div>

          <div>
            <span>FOLLOWERS</span>
            <strong>
              Append + ACK
            </strong>
          </div>

          <div className="replication-animation reverse">
            <span className="replication-flow-line" />
            <span className="replication-flow-packet">
              ACK
            </span>
          </div>

          <div>
            <span>QUORUM</span>
            <strong>
              Commit advance
            </strong>
          </div>
        </div>

        <div className="architecture-consensus-stats">
          <ArchKPI
            label="LEADER"
            value={
              leaderId
                ? `Node ${leaderId}`
                : "Election"
            }
            detail="live"
          />

          <ArchKPI
            label="QUORUM"
            value="2 / 3"
            detail="majority"
          />

          <ArchKPI
            label="REPLICATION EVENTS"
            value={String(
              replicationEvents,
            )}
            detail="buffered"
          />
        </div>
      </div>
    </section>
  );
}

function RecoveryArchitecture({
  onSelect,
}: {
  onSelect: (
    key: ComponentKey,
  ) => void;
}) {
  return (
    <section className="architecture-canvas">
      <ArchitectureSectionLabel
        number="RECOVERY"
        title="Crash & Restart Path"
      />

      <div className="recovery-flow">
        <RecoveryStage
          icon={CircleDot}
          title="Process Stops"
          text="In-memory Raft state disappears."
        />

        <RecoveryConnector />

        <RecoveryStage
          icon={FileStack}
          title="Open WAL"
          text="Discover durable segments and checkpoint metadata."
          onClick={() =>
            onSelect("wal")
          }
        />

        <RecoveryConnector />

        <RecoveryStage
          icon={HardDrive}
          title="Load Snapshot"
          text="Restore compacted applied state when available."
          onClick={() =>
            onSelect("snapshot")
          }
        />

        <RecoveryConnector />

        <RecoveryStage
          icon={GitBranch}
          title="Replay"
          text="Recover remaining durable Raft history."
          onClick={() =>
            onSelect("raft")
          }
        />

        <RecoveryConnector />

        <RecoveryStage
          icon={Database}
          title="Restore KV"
          text="Reconstruct the materialized state machine."
          onClick={() =>
            onSelect("kv")
          }
        />

        <RecoveryConnector />

        <RecoveryStage
          icon={Network}
          title="Rejoin"
          text="Follower receives missing entries and catches up."
          onClick={() =>
            onSelect(
              "transport",
            )
          }
        />
      </div>

      <div className="recovery-layer-grid">
        <RecoveryLayer
          title="WAL safety"
          items={[
            "Segment discovery",
            "Partial-write recovery",
            "Checkpoint publication",
            "Compaction boundaries",
          ]}
        />

        <RecoveryLayer
          title="Snapshot safety"
          items={[
            "Applied index",
            "Applied term",
            "KV records",
            "Deduplication records",
          ]}
        />

        <RecoveryLayer
          title="Raft recovery"
          items={[
            "Hard state",
            "Log reconstruction",
            "Compacted boundary",
            "Follower catch-up",
          ]}
        />
      </div>
    </section>
  );
}

function ArchitectureNode({
  icon: Icon,
  title,
  subtitle,
  tag,
  wide = false,
  onClick,
}: {
  icon: typeof Server;
  title: string;
  subtitle: string;
  tag: string;
  wide?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      className={
        wide
          ? "architecture-component wide"
          : "architecture-component"
      }
      onClick={onClick}
    >
      <div className="architecture-component-icon">
        <Icon size={17} />
      </div>

      <div>
        <span>{tag}</span>
        <strong>{title}</strong>
        <small>{subtitle}</small>
      </div>

      <ChevronRight
        size={13}
      />
    </button>
  );
}

function FlowArrow({
  label,
  animated = false,
}: {
  label: string;
  animated?: boolean;
}) {
  return (
    <div className="architecture-flow-arrow">
      <span>{label}</span>

      <div>
        <i />

        {animated && (
          <b />
        )}

        <ArrowRight
          size={12}
        />
      </div>
    </div>
  );
}

function ArchitectureSectionLabel({
  number,
  title,
}: {
  number: string;
  title: string;
}) {
  return (
    <div className="architecture-section-label">
      <span>{number}</span>
      <strong>{title}</strong>
    </div>
  );
}

function ArchKPI({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="architecture-kpi">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function ViewButton({
  icon: Icon,
  title,
  description,
  active,
  onClick,
}: {
  icon: typeof Server;
  title: string;
  description: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      className={
        active
          ? "architecture-view active"
          : "architecture-view"
      }
      onClick={onClick}
    >
      <Icon size={15} />

      <div>
        <strong>{title}</strong>
        <span>{description}</span>
      </div>
    </button>
  );
}

function Principle({
  icon: Icon,
  title,
  text,
}: {
  icon: typeof Server;
  title: string;
  text: string;
}) {
  return (
    <div className="architecture-principle">
      <Icon size={17} />

      <div>
        <strong>{title}</strong>
        <p>{text}</p>
      </div>
    </div>
  );
}

function ProtocolStage({
  number,
  title,
  text,
}: {
  number: string;
  title: string;
  text: string;
}) {
  return (
    <div className="protocol-stage">
      <span>{number}</span>
      <strong>{title}</strong>
      <p>{text}</p>
    </div>
  );
}

function ProtocolArrow({
  text,
}: {
  text: string;
}) {
  return (
    <div className="protocol-arrow">
      <span>{text}</span>
      <ArrowRight
        size={16}
      />
    </div>
  );
}

function RecoveryStage({
  icon: Icon,
  title,
  text,
  onClick,
}: {
  icon: typeof Server;
  title: string;
  text: string;
  onClick?: () => void;
}) {
  return (
    <button
      className="recovery-stage"
      onClick={onClick}
      disabled={!onClick}
    >
      <Icon size={18} />
      <strong>{title}</strong>
      <span>{text}</span>
    </button>
  );
}

function RecoveryConnector() {
  return (
    <div className="recovery-connector">
      <span />
      <ArrowRight
        size={13}
      />
    </div>
  );
}

function RecoveryLayer({
  title,
  items,
}: {
  title: string;
  items: string[];
}) {
  return (
    <div className="recovery-layer">
      <strong>{title}</strong>

      {items.map((item) => (
        <span key={item}>
          <CheckDot />
          {item}
        </span>
      ))}
    </div>
  );
}

function CheckDot() {
  return (
    <i className="architecture-check-dot" />
  );
}

function ArchitectureInspector({
  component,
  onClose,
}: {
  component: ComponentKey;
  onClose: () => void;
}) {
  const detail =
    details[component];

  return (
    <div
      className="architecture-inspector-backdrop"
      onMouseDown={onClose}
    >
      <aside
        className="architecture-inspector"
        onMouseDown={(event) =>
          event.stopPropagation()
        }
      >
        <div className="architecture-inspector-header">
          <div>
            <span className="eyebrow">
              COMPONENT
            </span>

            <h2>
              {detail.title}
            </h2>

            <p>
              {detail.subtitle}
            </p>
          </div>

          <button
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </div>

        <section>
          <h3>Purpose</h3>
          <p>
            {detail.description}
          </p>
        </section>

        <section>
          <h3>
            Responsibilities
          </h3>

          <div className="architecture-detail-list">
            {detail.responsibilities.map(
              (item) => (
                <div key={item}>
                  <i />
                  <span>{item}</span>
                </div>
              ),
            )}
          </div>
        </section>

        <section>
          <h3>
            Implementation
          </h3>

          <div className="architecture-file-list">
            {detail.files.map(
              (file) => (
                <code key={file}>
                  {file}
                </code>
              ),
            )}
          </div>
        </section>

        <section>
          <h3>
            Communicates with
          </h3>

          <div className="architecture-communication-list">
            {detail.communicatesWith.map(
              (item) => (
                <span key={item}>
                  {item}
                </span>
              ),
            )}
          </div>
        </section>
      </aside>
    </div>
  );
}

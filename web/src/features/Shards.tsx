import {
  Activity,
  ArrowRight,
  Boxes,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  Crown,
  Database,
  FileStack,
  GitBranch,
  HardDrive,
  Layers3,
  Radio,
  Server,
  ShieldCheck,
  X,
} from "lucide-react";

import {
  useMemo,
  useState,
  type ReactNode,
} from "react";

import type {
  KVSnapshot,
  NodeStatus,
  Page,
  RaftDebugState,
  RaftLog,
  StorageDebug,
} from "../types";

interface Props {
  nodes: NodeStatus[];
  raft?: RaftDebugState;
  raftLog?: RaftLog;
  kv?: KVSnapshot;
  storage?: StorageDebug;
  connected: boolean;
  onNavigate: (page: Page) => void;
}

export function Shards({
  nodes,
  raft,
  raftLog,
  kv,
  storage,
  connected,
  onNavigate,
}: Props) {
  const [inspectorOpen, setInspectorOpen] =
    useState(false);

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const reachable = nodes.filter(
    (node) => node.reachable !== false,
  ).length;

  const quorum = reachable >= 2;

  const health =
    reachable === 3
      ? "HEALTHY"
      : quorum
        ? "DEGRADED"
        : "UNAVAILABLE";

  const replicaRows = useMemo(
    () =>
      [1, 2, 3].map((id) => {
        const node = nodes.find(
          (candidate) =>
            candidate.id === id,
        );

        const online =
          node?.reachable !== false;

        const isLeader =
          online &&
          node?.role === 2;

        const leaderCommit =
          leader?.commit_index ?? 0;

        const lag = online
          ? Math.max(
              0,
              leaderCommit -
                (node?.applied_index ?? 0),
            )
          : undefined;

        const replicationPeer =
          raft?.peers?.find(
            (peer) =>
              peer.node_id === id,
          );

        return {
          id,
          node,
          online,
          isLeader,
          lag,
          nextIndex:
            replicationPeer?.next_index,
          matchIndex:
            replicationPeer?.match_index,
        };
      }),
    [nodes, leader, raft],
  );

  const maxLag = Math.max(
    0,
    ...replicaRows
      .filter(
        (row) =>
          row.online &&
          !row.isLeader,
      )
      .map(
        (row) => row.lag ?? 0,
      ),
  );

  const records =
    kv?.count ??
    storage?.records ??
    0;

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            DATA DISTRIBUTION
          </div>

          <h1>Shards</h1>

          <p>
            Replicated shard ownership,
            Raft membership, replica health
            and durable state.
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
            ? "LIVE"
            : "RECONNECTING"}
        </div>
      </header>

      <section className="metrics-grid">
        <ShardMetric
          icon={Boxes}
          label="ACTIVE SHARDS"
          value="1"
          detail="Current topology"
        />

        <ShardMetric
          icon={Server}
          label="REPLICAS"
          value={`${reachable} / 3`}
          detail="Voting members reachable"
        />

        <ShardMetric
          icon={Crown}
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

        <ShardMetric
          icon={GitBranch}
          label="MAX LAG"
          value={String(maxLag)}
          detail="Applied index"
        />
      </section>

      <section
        className={[
          "shard-hero",
          health.toLowerCase(),
        ].join(" ")}
      >
        <div className="shard-hero-heading">
          <div>
            <div className="shard-title-line">
              <div className="shard-icon">
                <Boxes size={18} />
              </div>

              <div>
                <span className="eyebrow">
                  SHARD 01
                </span>

                <h2>
                  Primary replicated shard
                </h2>
              </div>
            </div>

            <p>
              One logical shard replicated
              across three voting Raft
              members.
            </p>
          </div>

          <div
            className={`shard-health ${health.toLowerCase()}`}
          >
            <CircleDot size={11} />
            {health}
          </div>
        </div>

        <div className="shard-identity-grid">
          <ShardIdentity
            label="KEYSPACE"
            value="Default / Full"
            detail="No hash partitioning configured"
          />

          <ShardIdentity
            label="RAFT GROUP"
            value="Group 01"
            detail="Single consensus group"
          />

          <ShardIdentity
            label="REPLICATION"
            value="3"
            detail="Voting replicas"
          />

          <ShardIdentity
            label="QUORUM"
            value="2"
            detail={
              quorum
                ? "Currently available"
                : "Currently unavailable"
            }
          />

          <ShardIdentity
            label="RECORDS"
            value={String(records)}
            detail="Materialized KV records"
          />
        </div>

        <div className="shard-topology">
          <div className="shard-topology-leader">
            {leader ? (
              <ReplicaNode
                id={leader.id}
                role="LEADER"
                term={leader.term}
                commit={
                  leader.commit_index
                }
                applied={
                  leader.applied_index
                }
                online
                leader
              />
            ) : (
              <div className="shard-election-card">
                <Activity size={18} />
                <strong>
                  Leader election
                </strong>
                <span>
                  Waiting for quorum to
                  elect a leader.
                </span>
              </div>
            )}
          </div>

          <div className="shard-replication-links">
            <div className="shard-link">
              <span />
              <b />
            </div>

            <div className="shard-link-label">
              AppendEntries
              <small>
                replicated log
              </small>
            </div>

            <div className="shard-link reverse">
              <span />
              <b />
            </div>
          </div>

          <div className="shard-follower-grid">
            {replicaRows
              .filter(
                (row) =>
                  row.id !== leader?.id,
              )
              .map((row) => (
                <ReplicaNode
                  key={row.id}
                  id={row.id}
                  role={
                    !row.online
                      ? "OFFLINE"
                      : row.node?.role === 1
                        ? "CANDIDATE"
                        : "FOLLOWER"
                  }
                  term={
                    row.node?.term
                  }
                  commit={
                    row.node
                      ?.commit_index
                  }
                  applied={
                    row.node
                      ?.applied_index
                  }
                  lag={row.lag}
                  online={row.online}
                />
              ))}
          </div>
        </div>

        <div className="shard-footer-stats">
          <ShardFooterStat
            label="COMMIT INDEX"
            value={String(
              leader?.commit_index ??
                "—",
            )}
          />

          <ShardFooterStat
            label="APPLIED INDEX"
            value={String(
              leader?.applied_index ??
                "—",
            )}
          />

          <ShardFooterStat
            label="SNAPSHOT"
            value={String(
              raft?.snapshot_index ??
                storage?.snapshot
                  ?.applied_index ??
                0,
            )}
          />

          <ShardFooterStat
            label="LOG RANGE"
            value={
              raft
                ? `${raft.first_log_index} → ${raft.last_log_index}`
                : raftLog
                  ? `${raftLog.first_index} → ${raftLog.last_index}`
                  : "—"
            }
          />

          <button
            className="shard-inspect-button"
            onClick={() =>
              setInspectorOpen(true)
            }
          >
            Inspect shard
            <ChevronRight
              size={12}
            />
          </button>
        </div>
      </section>

      <section className="panel shard-replica-panel">
        <div className="panel-heading">
          <div>
            <h2>Replica state</h2>

            <p>
              Live consensus and
              replication position for
              every voting member.
            </p>
          </div>

          <span className="section-chip">
            RF 3 · QUORUM 2
          </span>
        </div>

        <div className="shard-replica-table">
          <div className="shard-table-header">
            <span>REPLICA</span>
            <span>ROLE</span>
            <span>TERM</span>
            <span>COMMIT</span>
            <span>APPLIED</span>
            <span>MATCH</span>
            <span>NEXT</span>
            <span>LAG</span>
            <span>HEALTH</span>
          </div>

          {replicaRows.map(
            (row) => (
              <div
                className="shard-table-row"
                key={row.id}
              >
                <div className="shard-table-node">
                  <Server size={12} />

                  <strong>
                    Node {row.id}
                  </strong>
                </div>

                <span
                  className={[
                    "shard-role-pill",
                    row.isLeader
                      ? "leader"
                      : !row.online
                        ? "offline"
                        : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                >
                  {!row.online
                    ? "OFFLINE"
                    : row.isLeader
                      ? "LEADER"
                      : row.node?.role ===
                          1
                        ? "CANDIDATE"
                        : "FOLLOWER"}
                </span>

                <code>
                  {row.online
                    ? row.node?.term ??
                      "—"
                    : "—"}
                </code>

                <code>
                  {row.online
                    ? row.node
                        ?.commit_index ??
                      "—"
                    : "—"}
                </code>

                <code>
                  {row.online
                    ? row.node
                        ?.applied_index ??
                      "—"
                    : "—"}
                </code>

                <code>
                  {row.isLeader
                    ? "—"
                    : row.matchIndex ??
                      "—"}
                </code>

                <code>
                  {row.isLeader
                    ? "—"
                    : row.nextIndex ??
                      "—"}
                </code>

                <code>
                  {row.lag ??
                    "—"}
                </code>

                <div
                  className={
                    row.online
                      ? "shard-row-health online"
                      : "shard-row-health offline"
                  }
                >
                  <i />
                  {row.online
                    ? "HEALTHY"
                    : "DOWN"}
                </div>
              </div>
            ),
          )}
        </div>
      </section>

      <section className="panel shard-keyspace-panel">
        <div className="panel-heading">
          <div>
            <h2>
              Keyspace ownership
            </h2>

            <p>
              Current implementation uses
              one replicated shard rather
              than multiple hash ranges.
            </p>
          </div>

          <Database size={17} />
        </div>

        <div className="shard-keyspace-content">
          <div className="shard-keyspace-axis">
            <span>0%</span>
            <span>
              LOGICAL KEYSPACE
            </span>
            <span>100%</span>
          </div>

          <button
            className="shard-keyspace-bar"
            onClick={() =>
              setInspectorOpen(true)
            }
          >
            <div>
              <Boxes size={14} />

              <strong>
                SHARD 01
              </strong>

              <span>
                DEFAULT / FULL KEYSPACE
              </span>
            </div>

            <em>
              RF 3
            </em>
          </button>

          <div className="shard-keyspace-note">
            <ShieldCheck
              size={15}
            />

            <div>
              <strong>
                Accurate to the current
                implementation
              </strong>

              <span>
                No fabricated hash slots or
                multiple Raft groups are
                shown. The architecture is
                ready to visualize those if
                routing and additional shard
                groups are introduced later.
              </span>
            </div>
          </div>
        </div>
      </section>

      <section className="shard-layer-grid">
        <ShardLayer
          icon={GitBranch}
          title="Consensus"
          value="Raft"
          detail="Leader election + replicated log"
          onClick={() =>
            onNavigate(
              "replication",
            )
          }
        />

        <ShardLayer
          icon={Database}
          title="State Machine"
          value="KV Store"
          detail={`${records} materialized records`}
          onClick={() =>
            onNavigate("database")
          }
        />

        <ShardLayer
          icon={FileStack}
          title="Durability"
          value="Segmented WAL"
          detail={
            storage?.snapshot?.exists
              ? `Snapshot @ ${storage.snapshot.applied_index}`
              : "Snapshot not published"
          }
          onClick={() =>
            onNavigate("storage")
          }
        />

        <ShardLayer
          icon={HardDrive}
          title="Engine"
          value={
            storage?.engine ??
            "Pebble"
          }
          detail="Persistent local state"
          onClick={() =>
            onNavigate("storage")
          }
        />
      </section>

      {inspectorOpen && (
        <ShardInspector
          leader={leader}
          reachable={reachable}
          quorum={quorum}
          raft={raft}
          raftLog={raftLog}
          storage={storage}
          records={records}
          replicaRows={replicaRows}
          onClose={() =>
            setInspectorOpen(false)
          }
          onNavigate={(page) => {
            setInspectorOpen(
              false,
            );
            onNavigate(page);
          }}
        />
      )}
    </>
  );
}

function ReplicaNode({
  id,
  role,
  term,
  commit,
  applied,
  lag,
  online,
  leader = false,
}: {
  id: number;
  role: string;
  term?: number;
  commit?: number;
  applied?: number;
  lag?: number;
  online: boolean;
  leader?: boolean;
}) {
  return (
    <div
      className={[
        "shard-replica-node",
        leader ? "leader" : "",
        !online ? "offline" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className="shard-replica-header">
        <Server size={15} />

        <div>
          <strong>
            Node {id}
          </strong>

          <span>
            127.0.0.1:808{id}
          </span>
        </div>

        {leader && (
          <Crown size={13} />
        )}
      </div>

      <div className="shard-replica-role">
        {role}
      </div>

      <div className="shard-replica-stats">
        <div>
          <span>TERM</span>
          <strong>
            {online
              ? term ?? "—"
              : "—"}
          </strong>
        </div>

        <div>
          <span>COMMIT</span>
          <strong>
            {online
              ? commit ?? "—"
              : "—"}
          </strong>
        </div>

        <div>
          <span>APPLIED</span>
          <strong>
            {online
              ? applied ?? "—"
              : "—"}
          </strong>
        </div>

        {!leader && (
          <div>
            <span>LAG</span>
            <strong>
              {online
                ? lag ?? 0
                : "—"}
            </strong>
          </div>
        )}
      </div>
    </div>
  );
}

function ShardMetric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Boxes;
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

function ShardIdentity({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function ShardFooterStat({
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

function ShardLayer({
  icon: Icon,
  title,
  value,
  detail,
  onClick,
}: {
  icon: typeof Boxes;
  title: string;
  value: string;
  detail: string;
  onClick: () => void;
}) {
  return (
    <button
      className="shard-layer-card"
      onClick={onClick}
    >
      <div className="shard-layer-icon">
        <Icon size={16} />
      </div>

      <div>
        <span>{title}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>

      <ArrowRight size={13} />
    </button>
  );
}

function ShardInspector({
  leader,
  reachable,
  quorum,
  raft,
  raftLog,
  storage,
  records,
  replicaRows,
  onClose,
  onNavigate,
}: {
  leader?: NodeStatus;
  reachable: number;
  quorum: boolean;
  raft?: RaftDebugState;
  raftLog?: RaftLog;
  storage?: StorageDebug;
  records: number;
  replicaRows: {
    id: number;
    online: boolean;
    isLeader: boolean;
    lag?: number;
    nextIndex?: number;
    matchIndex?: number;
  }[];
  onClose: () => void;
  onNavigate: (page: Page) => void;
}) {
  return (
    <div
      className="shard-inspector-backdrop"
      onMouseDown={onClose}
    >
      <aside
        className="shard-inspector"
        onMouseDown={(event) =>
          event.stopPropagation()
        }
      >
        <div className="shard-inspector-header">
          <div>
            <span className="eyebrow">
              SHARD 01
            </span>

            <h2>
              Replicated shard
            </h2>

            <p>
              Raft Group 01 · RF 3 ·
              quorum 2
            </p>
          </div>

          <button
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </div>

        <InspectorSection
          title="Consensus"
          icon={GitBranch}
        >
          <InspectorGrid>
            <InspectorValue
              label="LEADER"
              value={
                leader
                  ? `Node ${leader.id}`
                  : "None"
              }
            />

            <InspectorValue
              label="TERM"
              value={String(
                leader?.term ??
                  raft?.term ??
                  "—",
              )}
            />

            <InspectorValue
              label="MEMBERS"
              value="3"
            />

            <InspectorValue
              label="REACHABLE"
              value={`${reachable} / 3`}
            />

            <InspectorValue
              label="QUORUM"
              value={
                quorum
                  ? "Available"
                  : "Lost"
              }
            />

            <InspectorValue
              label="COMMIT"
              value={String(
                leader
                  ?.commit_index ??
                  raft
                    ?.commit_index ??
                  "—",
              )}
            />
          </InspectorGrid>
        </InspectorSection>

        <InspectorSection
          title="State machine"
          icon={Database}
        >
          <InspectorGrid>
            <InspectorValue
              label="ENGINE"
              value={
                storage?.engine ??
                "pebble"
              }
            />

            <InspectorValue
              label="RECORDS"
              value={String(records)}
            />

            <InspectorValue
              label="APPLIED"
              value={String(
                leader
                  ?.applied_index ??
                  storage
                    ?.applied_index ??
                  "—",
              )}
            />

            <InspectorValue
              label="KEYSPACE"
              value="Default"
            />
          </InspectorGrid>
        </InspectorSection>

        <InspectorSection
          title="Durability"
          icon={HardDrive}
        >
          <InspectorGrid>
            <InspectorValue
              label="SNAPSHOT"
              value={
                storage?.snapshot
                  ?.exists
                  ? `Index ${storage.snapshot.applied_index}`
                  : "None"
              }
            />

            <InspectorValue
              label="SNAPSHOT TERM"
              value={String(
                storage?.snapshot
                  ?.applied_term ??
                  raft?.snapshot_term ??
                  0,
              )}
            />

            <InspectorValue
              label="FIRST LOG"
              value={String(
                raft
                  ?.first_log_index ??
                  raftLog
                    ?.first_index ??
                  "—",
              )}
            />

            <InspectorValue
              label="LAST LOG"
              value={String(
                raft
                  ?.last_log_index ??
                  raftLog
                    ?.last_index ??
                  "—",
              )}
            />
          </InspectorGrid>
        </InspectorSection>

        <InspectorSection
          title="Replication"
          icon={Layers3}
        >
          <div className="shard-inspector-replicas">
            {replicaRows.map(
              (row) => (
                <div key={row.id}>
                  <div>
                    <i
                      className={
                        row.online
                          ? "online"
                          : "offline"
                      }
                    />

                    <strong>
                      Node {row.id}
                    </strong>

                    <span>
                      {row.isLeader
                        ? "LEADER"
                        : row.online
                          ? "FOLLOWER"
                          : "OFFLINE"}
                    </span>
                  </div>

                  <code>
                    lag{" "}
                    {row.lag ??
                      "—"}
                  </code>
                </div>
              ),
            )}
          </div>
        </InspectorSection>

        <div className="shard-inspector-actions">
          <button
            onClick={() =>
              onNavigate(
                "raft-log",
              )
            }
          >
            <GitBranch size={12} />
            Raft Log
          </button>

          <button
            onClick={() =>
              onNavigate(
                "database",
              )
            }
          >
            <Database size={12} />
            Database
          </button>

          <button
            onClick={() =>
              onNavigate(
                "storage",
              )
            }
          >
            <HardDrive size={12} />
            Storage
          </button>

          <button
            onClick={() =>
              onNavigate(
                "replication",
              )
            }
          >
            <Layers3 size={12} />
            Replication
          </button>
        </div>
      </aside>
    </div>
  );
}

function InspectorSection({
  title,
  icon: Icon,
  children,
}: {
  title: string;
  icon: typeof Boxes;
  children: ReactNode;
}) {
  return (
    <section className="shard-inspector-section">
      <div className="shard-inspector-section-heading">
        <Icon size={13} />
        <h3>{title}</h3>
      </div>

      {children}
    </section>
  );
}

function InspectorGrid({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <div className="shard-inspector-grid">
      {children}
    </div>
  );
}

function InspectorValue({
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

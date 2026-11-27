import type { DataShardID } from "../api/multiDiagnostics";

import {
  Activity,
  GitBranch,
  Radio,
} from "lucide-react";

import { MultiReplicationTelemetry } from "./MultiReplicationTelemetry";

import type { MultiRaftClusterState } from "./useMultiRaftCluster";
import type { MultiLiveTelemetry } from "./useMultiLiveTelemetry";

import "./multiArchitectureConsensus.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
  selectedShard: DataShardID;
  onShardChange: (shardId: DataShardID) => void;
  selectionLocked?: boolean;
}

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

export function MultiArchitectureConsensus({
  cluster,
  telemetry,
  selectedShard: selected,
  onShardChange,
  selectionLocked = false,
}: Props) {

  return (
    <section className="mac-consensus">
      <div className="ma-panel mac-heading">
        <div className="mac-title">
          <div className="mac-icon">
            <GitBranch size={23} />
          </div>

          <div>
            <div className="mac-eyebrow">
              DISTRIBUTED CONSENSUS
            </div>

            <h2>Live Consensus Control Plane</h2>

            <p>
              Explore independent Raft groups,
              observed replication and majority commits.
            </p>
          </div>

          <div className="mac-streams">
            <Radio size={15} />
            {telemetry.connectedNodes}/5 SSE connected
          </div>
        </div>

        {selectionLocked && (
          <div className="mac-demo-lock">
            Live demonstration active ·
            Following Shard {selected}
          </div>
        )}

        <div className="mac-shards">
          {([1, 2, 3, 4] as const).map(id => {
            const shard = cluster.shards.find(
              item => item.shardId === id,
            );

            const color = COLORS[id - 1];

            return (
              <button
                key={id}
                type="button"
                aria-pressed={selected === id}
                className={
                  selected === id
                    ? "mac-shard active"
                    : "mac-shard"
                }
                style={{
                  borderColor:
                    selected === id ? color : undefined,
                  boxShadow:
                    selected === id
                      ? `0 0 24px ${color}18`
                      : undefined,
                }}
                disabled={selectionLocked}
                onClick={() => onShardChange(id)}
              >
                <div className="mac-shard-top">
                  <Activity size={17} color={color} />

                  <strong style={{ color }}>
                    Shard {id}
                  </strong>

                  <i
                    style={{
                      background:
                        cluster.loading
                          ? "#64748b"
                          : shard?.state === "healthy"
                            ? "#34d399"
                            : shard?.majorityReachable
                              ? "#fbbf24"
                              : "#f87171",
                    }}
                  />
                </div>

                <div className="mac-shard-details">
                  <span>
                    Observed leader
                    <b>
                      {shard?.leaderId
                        ? `Node ${shard.leaderId}`
                        : "Unknown"}
                    </b>
                  </span>

                  <span>
                    Reachable
                    <b>
                      {cluster.loading
                        ? "—"
                        : `${shard?.reachableReplicas ?? 0}/3`}
                    </b>
                  </span>
                </div>

                <div className="mac-shard-progress">
                  <div
                    style={{
                      background: color,
                      width: `${
                        (shard?.reachableReplicas ?? 0)
                          / 3 * 100
                      }%`,
                    }}
                  />
                </div>

                <small>
                  {cluster.loading
                    ? "Connecting"
                    : shard?.state ?? "Unknown"}
                </small>
              </button>
            );
          })}
        </div>
      </div>

      <MultiReplicationTelemetry
        key={selected}
        cluster={cluster}
        telemetry={telemetry}
        selectedShard={selected}
      />
    </section>
  );
}

import {
  Play,
  RefreshCw,
  Server,
  Square,
} from "lucide-react";

import type { ControlledNode } from "../api/control";
import type { NodeStatus } from "../types";

interface Props {
  node: NodeStatus;
  control?: ControlledNode;
  busy?: boolean;

  onStart: () => void;
  onStop: () => void;
  onRestart: () => void;
}

export function TopologyNode({
  node,
  control,
  busy = false,
  onStart,
  onStop,
  onRestart,
}: Props) {
  const reachable = node.reachable !== false;
  const leader = reachable && node.role === 2;

  const running =
    control?.running === true ||
    control?.reachable === true ||
    reachable;

  const role = !reachable
    ? "OFFLINE"
    : leader
      ? "LEADER"
      : node.role === 1
        ? "CANDIDATE"
        : "FOLLOWER";

  return (
    <div
      className={[
        "topology-control-node",
        leader ? "leader" : "",
        !reachable ? "offline" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <div className="topology-control-header">
        <div className="topology-node-identity">
          <div className="topology-server-icon">
            <Server size={15} />
          </div>

          <div>
            <strong>Node {node.id}</strong>
            <span>127.0.0.1:808{node.id}</span>
          </div>
        </div>

        <span
          className={[
            "topology-role",
            leader ? "leader" : "",
            !reachable ? "offline" : "",
          ]
            .filter(Boolean)
            .join(" ")}
        >
          {role}
        </span>
      </div>

      <div className="topology-node-stats">
        <div>
          <span>TERM</span>
          <strong>{reachable ? node.term : "—"}</strong>
        </div>

        <div>
          <span>COMMIT</span>
          <strong>
            {reachable ? node.commit_index : "—"}
          </strong>
        </div>

        <div>
          <span>APPLIED</span>
          <strong>
            {reachable ? node.applied_index : "—"}
          </strong>
        </div>
      </div>

      <div className="topology-node-actions">
        {!running ? (
          <button
            className="topology-action start"
            disabled={busy}
            onClick={onStart}
          >
            <Play size={11} />
            Start
          </button>
        ) : (
          <>
            <button
              className="topology-action"
              disabled={busy}
              onClick={onRestart}
            >
              <RefreshCw size={11} />
              Restart
            </button>

            <button
              className={
                leader
                  ? "topology-action stop leader-stop"
                  : "topology-action stop"
              }
              disabled={busy}
              onClick={onStop}
            >
              <Square size={10} />
              {leader ? "Stop leader" : "Stop"}
            </button>
          </>
        )}
      </div>
    </div>
  );
}

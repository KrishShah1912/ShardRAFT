import { Server } from "lucide-react";
import { roleName } from "../lib/format";
import type { NodeStatus } from "../types";

export function NodeCard({
  node,
}: {
  node: NodeStatus;
}) {
  const role = roleName(node.role);

  return (
    <div
      className={`node-card ${role.toLowerCase()} ${
        node.reachable === false ? "offline" : ""
      }`}
    >
      <div className="node-card-header">
        <div className="node-icon">
          <Server size={18} />
        </div>

        <div>
          <div className="node-name">Node {node.id}</div>
          <div className="node-address">
            127.0.0.1:{8080 + node.id}
          </div>
        </div>

        <span className={`role-badge ${role.toLowerCase()}`}>
          {node.reachable === false ? "Offline" : role}
        </span>
      </div>

      <div className="node-stats">
        <div>
          <span>TERM</span>
          <strong>{node.term}</strong>
        </div>
        <div>
          <span>COMMIT</span>
          <strong>{node.commit_index}</strong>
        </div>
        <div>
          <span>APPLIED</span>
          <strong>{node.applied_index}</strong>
        </div>
      </div>
    </div>
  );
}

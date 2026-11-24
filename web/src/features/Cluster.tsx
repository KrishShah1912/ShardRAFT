import { useState } from "react";

import type { ControlledNode } from "../api/control";
import { NodeCard } from "../components/NodeCard";
import { NodeControls } from "../components/NodeControls";
import type { NodeStatus } from "../types";

interface Props {
  nodes: NodeStatus[];
  controlledNodes: ControlledNode[];

  onStart: (id: number) => Promise<void>;
  onStop: (id: number) => Promise<void>;
  onRestart: (id: number) => Promise<void>;
}

export function Cluster({
  nodes,
  controlledNodes,
  onStart,
  onStop,
  onRestart,
}: Props) {
  const [busyNode, setBusyNode] = useState<number>();
  const [confirmLeader, setConfirmLeader] =
    useState<number>();

  function controlled(id: number) {
    return controlledNodes.find(
      (node) => node.id === id,
    );
  }

  async function execute(
    id: number,
    action: () => Promise<void>,
  ) {
    setBusyNode(id);

    try {
      await action();
    } finally {
      setBusyNode(undefined);
      setConfirmLeader(undefined);
    }
  }

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">CONSENSUS</div>
          <h1>Cluster</h1>
          <p>
            Inspect and control the local three-node
            ShardRAFT cluster.
          </p>
        </div>
      </header>

      <div className="cluster-grid">
        {nodes.map((node) => {
          const control = controlled(node.id);
          const isLeader =
            node.role === 2 &&
            node.reachable !== false;

          return (
            <div
              className="controlled-node"
              key={node.id}
            >
              <NodeCard node={node} />

              <NodeControls
                node={control}
                isLeader={isLeader}
                busy={busyNode === node.id}
                onStart={() =>
                  execute(
                    node.id,
                    () => onStart(node.id),
                  )
                }
                onRestart={() =>
                  execute(
                    node.id,
                    () => onRestart(node.id),
                  )
                }
                onStop={() => {
                  if (isLeader) {
                    setConfirmLeader(node.id);
                    return;
                  }

                  void execute(
                    node.id,
                    () => onStop(node.id),
                  );
                }}
              />

              {confirmLeader === node.id && (
                <div className="leader-warning">
                  <strong>
                    Stop the current leader?
                  </strong>

                  <p>
                    Raft will need to elect a replacement
                    leader. The cluster should remain
                    available with the other two nodes.
                  </p>

                  <div>
                    <button
                      onClick={() =>
                        setConfirmLeader(undefined)
                      }
                    >
                      Cancel
                    </button>

                    <button
                      className="danger-button"
                      onClick={() =>
                        void execute(
                          node.id,
                          () => onStop(node.id),
                        )
                      }
                    >
                      Stop leader
                    </button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="panel">
        <div className="panel-heading">
          <div>
            <h2>Node state</h2>
            <p>
              Authoritative state reported by each process.
            </p>
          </div>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>NODE</th>
                <th>ROLE</th>
                <th>TERM</th>
                <th>LEADER</th>
                <th>COMMIT</th>
                <th>APPLIED</th>
                <th>PROCESS</th>
                <th>STATUS</th>
              </tr>
            </thead>

            <tbody>
              {nodes.map((node) => {
                const control = controlled(node.id);

                return (
                  <tr key={node.id}>
                    <td>Node {node.id}</td>

                    <td>
                      {node.reachable === false
                        ? "—"
                        : node.role === 2
                          ? "Leader"
                          : node.role === 1
                            ? "Candidate"
                            : "Follower"}
                    </td>

                    <td>
                      {node.reachable === false
                        ? "—"
                        : node.term}
                    </td>

                    <td>
                      {node.reachable === false
                        ? "—"
                        : node.leader_id
                          ? `Node ${node.leader_id}`
                          : "—"}
                    </td>

                    <td>
                      {node.reachable === false
                        ? "—"
                        : node.commit_index}
                    </td>

                    <td>
                      {node.reachable === false
                        ? "—"
                        : node.applied_index}
                    </td>

                    <td>
                      {control?.running
                        ? `PID ${control.pid ?? "—"}`
                        : "Stopped"}
                    </td>

                    <td>
                      <span
                        className={
                          node.reachable === false
                            ? "status-pill offline"
                            : "status-pill"
                        }
                      >
                        {node.reachable === false
                          ? "Offline"
                          : "Online"}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

import { NodeCard } from "../components/NodeCard";
import type { NodeStatus } from "../types";

export function Cluster({
  nodes,
}: {
  nodes: NodeStatus[];
}) {
  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">CONSENSUS</div>
          <h1>Cluster</h1>
          <p>
            Inspect node roles, terms and replicated progress.
          </p>
        </div>
      </header>

      <div className="cluster-grid">
        {nodes.map((node) => (
          <NodeCard node={node} key={node.id} />
        ))}
      </div>

      <div className="panel">
        <div className="panel-heading">
          <div>
            <h2>Node state</h2>
            <p>Authoritative state reported by each process.</p>
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
                <th>STATUS</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((node) => (
                <tr key={node.id}>
                  <td>Node {node.id}</td>
                  <td>
                    {node.role === 2
                      ? "Leader"
                      : node.role === 1
                        ? "Candidate"
                        : "Follower"}
                  </td>
                  <td>{node.term}</td>
                  <td>
                    {node.leader_id
                      ? `Node ${node.leader_id}`
                      : "—"}
                  </td>
                  <td>{node.commit_index}</td>
                  <td>{node.applied_index}</td>
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
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

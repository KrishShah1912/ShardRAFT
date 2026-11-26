import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import "./multiArchitecture.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const PLACEMENT = [
  { id: 1, nodes: [1, 2, 3], start: 0, end: 16383 },
  { id: 2, nodes: [2, 3, 4], start: 16384, end: 32767 },
  { id: 3, nodes: [3, 4, 5], start: 32768, end: 49151 },
  { id: 4, nodes: [1, 4, 5], start: 49152, end: 65535 },
];

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

export function MultiArchitecture({ cluster }: Props) {
  const map = cluster.shardMap?.shard_map;

  return (
    <section className="mx-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            SYSTEM ARCHITECTURE
          </div>

          <h1>Multi-Raft Architecture</h1>

          <p>
            Distributed routing, independent consensus
            groups and replicated persistent storage.
          </p>
        </div>
      </header>

      <article className="ma-panel">
        <h2>End-to-end request path</h2>

        <div className="mx-flow">
          <div className="mx-box mx-client">
            <strong>Client / Query Console</strong>
            <span>GET · PUT · DELETE · CAS</span>
          </div>

          <div className="mx-arrow">↓</div>

          <div className="mx-gateways">
            {[1, 2].map(id => (
              <div className="mx-box" key={id}>
                <strong>Gateway {id}</strong>

                <span>
                  HTTP :{18090 + id}
                </span>

                <small>
                  Request routing and retry
                </small>
              </div>
            ))}
          </div>

          <div className="mx-arrow">↓</div>

          <div className="mx-box mx-metadata">
            <strong>
              Metadata discovery and key routing
            </strong>

            <span>
              CRC32 → hash slot → shard ownership
            </span>

            <small>
              Metadata epoch: {map?.epoch ?? "Unknown"}
            </small>
          </div>

          <div className="mx-arrow">↓</div>

          <div className="mx-shards">
            {PLACEMENT.map((placement, index) => {
              const shard = cluster.shards.find(
                item => item.shardId === placement.id,
              );

              return (
                <div
                  className="mx-box"
                  key={placement.id}
                  style={{
                    borderColor: COLORS[index],
                  }}
                >
                  <strong
                    style={{ color: COLORS[index] }}
                  >
                    Shard {placement.id}
                  </strong>

                  <span>
                    {placement.start.toLocaleString()}
                    {" – "}
                    {placement.end.toLocaleString()}
                  </span>

                  <small>
                    {shard?.leaderId
                      ? `Leader: Node ${shard.leaderId}`
                      : "Leader unknown"}
                  </small>

                  <small>
                    {shard?.reachableReplicas ?? 0}/3
                    replicas observed
                  </small>
                </div>
              );
            })}
          </div>

          <div className="mx-arrow">↓</div>

          <div className="mx-box">
            <strong>gRPC Raft transport</strong>

            <span>
              Elections · AppendEntries · Replication
            </span>
          </div>

          <div className="mx-arrow">↓</div>

          <div className="mx-box">
            <strong>Persistent storage</strong>

            <span>
              Segmented WAL · Pebble · Snapshots
            </span>
          </div>
        </div>

        <p>
          This diagram shows the configured request
          path. It does not claim to trace individual
          requests or verify gateway health.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Physical node placement</h2>

        <div className="mx-nodes">
          {[1, 2, 3, 4, 5].map(nodeId => {
            const hosted = PLACEMENT.filter(
              item => item.nodes.includes(nodeId),
            );

            return (
              <div
                className="mx-node"
                key={nodeId}
              >
                <strong>Node {nodeId}</strong>

                <small>
                  HTTP :{18080 + nodeId}
                </small>

                {hosted.map(placement => {
                  const shard = cluster.shards.find(
                    item =>
                      item.shardId === placement.id,
                  );

                  const replica = shard?.replicas.find(
                    item => item.nodeId === nodeId,
                  );

                  return (
                    <div
                      className="mx-replica"
                      key={placement.id}
                      style={{
                        borderColor:
                          COLORS[placement.id - 1],
                      }}
                    >
                      <strong>
                        Shard {placement.id}
                      </strong>

                      <small>
                        {replica?.reachable
                          ? replica.role
                          : "Not observed"}
                      </small>
                    </div>
                  );
                })}

                {nodeId <= 3 && (
                  <div className="mx-replica mx-meta">
                    <strong>Metadata</strong>

                    <small>
                      {cluster.metadataLeader === nodeId
                        ? "Observed leader"
                        : "Configured replica"}
                    </small>
                  </div>
                )}

                <div className="mx-disk">
                  Persistent Docker volume
                </div>
              </div>
            );
          })}
        </div>

        <p>
          Fixed deployment placement with live data
          replica observations. Metadata group 5
          is independent of the four data shards.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Hash slot distribution</h2>

        <div className="mx-slotbar">
          {PLACEMENT.map((shard, index) => (
            <div
              key={shard.id}
              style={{
                background: `${COLORS[index]}20`,
                borderColor: COLORS[index],
              }}
            >
              <strong>Shard {shard.id}</strong>

              <span>16,384 slots</span>

              <small>
                {shard.start.toLocaleString()}
                {" – "}
                {shard.end.toLocaleString()}
              </small>
            </div>
          ))}
        </div>

        <p>
          Configured layout: 65,536 slots distributed
          equally across four data shards.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Metadata discovery</h2>

        {map ? (
          <>
            <div className="ma-stat-grid">
              <div className="ma-stat">
                <span>Metadata epoch</span>
                <strong>{map.epoch}</strong>
              </div>

              <div className="ma-stat">
                <span>Total slots</span>
                <strong>
                  {map.total_slots.toLocaleString()}
                </strong>
              </div>

              <div className="ma-stat">
                <span>Discovered shards</span>
                <strong>{map.shards.length}</strong>
              </div>
            </div>

            <pre className="mx-json">
              {JSON.stringify(map, null, 2)}
            </pre>
          </>
        ) : (
          <p>No metadata map is currently available.</p>
        )}
      </article>
    </section>
  );
}

export default MultiArchitecture;

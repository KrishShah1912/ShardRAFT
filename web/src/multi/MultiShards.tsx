import { useMemo, useState } from "react";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import {
  crc32,
  keySlot,
  previewKeyRouting,
} from "./keyRouting";

import {
  METADATA_REPLICAS,
} from "./clusterState";

import "./multiShards.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

export function MultiShards({ cluster }: Props) {
  const [key, setKey] = useState("demo:user");

  const map = cluster.shardMap?.shard_map;

  const routing = useMemo(() => {
    if (!map || !key.trim()) return null;

    return previewKeyRouting(key, map);
  }, [key, map]);

  const coverage = useMemo(() => {
    if (!map || map.total_slots !== 65536) {
      return null;
    }

    const owners = new Uint32Array(map.total_slots);

    let invalidRanges = 0;

    for (const shard of map.shards) {
      for (const range of shard.slot_ranges) {
        const start = range.start_slot;
        const end = range.end_slot;

        if (
          !Number.isInteger(start) ||
          !Number.isInteger(end) ||
          start < 0 ||
          end >= map.total_slots ||
          end < start
        ) {
          invalidRanges++;
          continue;
        }

        for (let slot = start; slot <= end; slot++) {
          owners[slot]++;
        }
      }
    }

    let assigned = 0;
    let missing = 0;
    let overlapping = 0;

    for (const count of owners) {
      if (count === 0) missing++;
      else if (count === 1) assigned++;
      else overlapping++;
    }

    return {
      assigned,
      missing,
      overlapping,
      invalidRanges,
    };
  }, [map]);

  const metadataLeader = cluster.metadata.find(
    item => item.node_id === cluster.metadataLeader,
  );

  return (
    <section className="ms-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            PHYSICAL SHARDING
          </div>

          <h1>Sharding & Metadata</h1>

          <p>
            Inspect discovered slot ownership,
            replica placement and metadata consensus.
          </p>
        </div>
      </header>

      <div className="ma-stat-grid">
        {[
          {
            label: "Discovered data shards",
            value: map?.shards.length ?? "—",
          },
          {
            label: "Metadata epoch",
            value: map?.epoch ?? "—",
          },
          {
            label: "Total hash slots",
            value:
              map?.total_slots.toLocaleString()
              ?? "—",
          },
          {
            label: "Uniquely assigned slots",
            value:
              coverage?.assigned.toLocaleString()
              ?? "—",
          },
          {
            label: "Unassigned slots",
            value: coverage?.missing ?? "—",
          },
          {
            label: "Overlapping slots",
            value: coverage?.overlapping ?? "—",
          },
        ].map(stat => (
          <article
            className="ma-stat"
            key={stat.label}
          >
            <span>{stat.label}</span>
            <strong>{stat.value}</strong>
          </article>
        ))}
      </div>

      <article className="ma-panel">
        <h2>Discovered shard map</h2>

        {map ? (
          <>
            <p>
              Source: Node {cluster.shardMap?.node_id}
              {" · "}
              Locally committed metadata
              {" · "}
              Epoch {map.epoch}
            </p>

            {coverage && (
              <div
                className={
                  coverage.missing === 0 &&
                  coverage.overlapping === 0 &&
                  coverage.invalidRanges === 0
                    ? "ms-valid"
                    : "ms-warning"
                }
              >
                {coverage.missing === 0 &&
                coverage.overlapping === 0 &&
                coverage.invalidRanges === 0
                  ? "All 65,536 slots have exactly one owner."
                  : "Slot ownership validation failed."}

                {" Invalid ranges: "}
                {coverage.invalidRanges}
              </div>
            )}

            {coverage &&
              coverage.missing === 0 &&
              coverage.overlapping === 0 &&
              coverage.invalidRanges === 0 && (
                <div className="ms-slot-track">
                  {map.shards.flatMap(shard =>
                    shard.slot_ranges.map(range => (
                      <div
                        key={`${shard.shard_id}-${range.start_slot}`}
                        title={
                          `Shard ${shard.shard_id}: ` +
                          `${range.start_slot}–${range.end_slot}`
                        }
                        style={{
                          left: `${
                            range.start_slot /
                            map.total_slots * 100
                          }%`,
                          width: `${
                            (range.end_slot -
                              range.start_slot + 1) /
                            map.total_slots * 100
                          }%`,
                          background:
                            COLORS[
                              (shard.shard_id - 1) % 4
                            ],
                        }}
                      >
                        S{shard.shard_id}
                      </div>
                    )),
                  )}
                </div>
              )}

            <div className="ms-slot-labels">
              <span>0</span>
              <span>
                {(map.total_slots - 1).toLocaleString()}
              </span>
            </div>

            <div className="ms-shard-grid">
              {map.shards.map(shard => {
                const observed = cluster.shards.find(
                  item =>
                    item.shardId === shard.shard_id,
                );

                return (
                  <div
                    className="ms-shard-card"
                    key={shard.shard_id}
                    style={{
                      borderTopColor:
                        COLORS[
                          (shard.shard_id - 1) % 4
                        ],
                    }}
                  >
                    <div className="ms-heading">
                      <h3>Shard {shard.shard_id}</h3>

                      <span>
                        Epoch {shard.epoch}
                      </span>
                    </div>

                    {shard.slot_ranges.map(range => (
                      <div
                        className="ms-range"
                        key={`${range.start_slot}-${range.end_slot}`}
                      >
                        <strong>
                          {range.start_slot.toLocaleString()}
                          {" – "}
                          {range.end_slot.toLocaleString()}
                        </strong>

                        <small>
                          {(range.end_slot -
                            range.start_slot + 1)
                            .toLocaleString()} slots
                        </small>
                      </div>
                    ))}

                    <div className="ms-detail">
                      <span>Observed leader</span>

                      <strong>
                        {observed?.leaderId
                          ? `Node ${observed.leaderId}`
                          : "Unknown"}
                      </strong>

                      <span>Metadata leader hint</span>

                      <strong>
                        {shard.leader_id
                          ? `Node ${shard.leader_id}`
                          : "Unknown"}
                      </strong>

                      <span>Reachable replicas</span>

                      <strong>
                        {observed?.reachableReplicas ?? "—"}
                        /{shard.replicas.length}
                      </strong>
                    </div>

                    {shard.replicas.map(replica => {
                      const status =
                        observed?.replicas.find(
                          item =>
                            item.nodeId === replica.node_id,
                        );

                      return (
                        <div
                          className="ms-replica"
                          key={replica.node_id}
                        >
                          <div>
                            <strong>
                              Node {replica.node_id}
                            </strong>

                            <small>
                              {replica.address}
                            </small>
                          </div>

                          <span>
                            {status?.reachable
                              ? status.role
                              : "Not observed"}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          </>
        ) : (
          <div className="ms-empty">
            {cluster.loading
              ? "Discovering metadata..."
              : "No shard map is currently available."}
          </div>
        )}
      </article>

      <article className="ma-panel">
        <h2>CRC32 key routing explorer</h2>

        <p>
          Calculate the expected destination shard
          without executing a database operation.
        </p>

        <label className="ms-key-input">
          Database key

          <input
            value={key}
            onChange={event =>
              setKey(event.target.value)
            }
            placeholder="Enter a database key"
          />
        </label>

        <div className="ms-examples">
          {[
            "demo:user",
            "user:101",
            "orders:2026",
            "café",
            "你好",
          ].map(example => (
            <button
              key={example}
              type="button"
              onClick={() => setKey(example)}
            >
              {example}
            </button>
          ))}
        </div>

        {key.trim() && (
          <div className="ma-stat-grid">
            <div className="ma-stat">
              <span>CRC32 hash</span>
              <strong>{crc32(key)}</strong>
            </div>

            <div className="ma-stat">
              <span>Hash slot</span>
              <strong>{keySlot(key)}</strong>
            </div>

            <div className="ma-stat">
              <span>Expected destination</span>

              <strong>
                {routing?.shardId
                  ? `Shard ${routing.shardId}`
                  : "Unknown"}
              </strong>
            </div>
          </div>
        )}

        {routing && (
          <div className="ms-routing-result">
            <span>
              Metadata epoch {routing.epoch}
            </span>

            <span>
              Replicas:{" "}
              {routing.replicaIds.length
                ? routing.replicaIds
                    .map(id => `Node ${id}`)
                    .join(", ")
                : "Unknown"}
            </span>
          </div>
        )}

        <p>
          Routing is calculated from the observed
          metadata map. It is not a live request trace.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Metadata Raft group — Group 5</h2>

        <p>
          Independent consensus group hosted by
          Nodes 1, 2 and 3.
        </p>

        <div className="ma-stat-grid">
          <div className="ma-stat">
            <span>Observed metadata leader</span>

            <strong>
              {cluster.metadataLeader
                ? `Node ${cluster.metadataLeader}`
                : "Unknown"}
            </strong>
          </div>

          <div className="ma-stat">
            <span>Observed leader term</span>

            <strong>
              {metadataLeader?.term ?? "—"}
            </strong>
          </div>

          <div className="ma-stat">
            <span>Observed metadata replicas</span>

            <strong>
              {cluster.metadata.length}/3
            </strong>
          </div>
        </div>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Term</th>
                <th>Reported leader</th>
                <th>Commit index</th>
                <th>Applied index</th>
              </tr>
            </thead>

            <tbody>
              {METADATA_REPLICAS.map(id => {
                const status = cluster.metadata.find(
                  item => item.node_id === id,
                );

                return (
                  <tr key={id}>
                    <td>Node {id}</td>

                    <td>
                      {status?.role ?? "Not observed"}
                    </td>

                    <td>{status?.term ?? "—"}</td>

                    <td>
                      {status?.leader_id || "—"}
                    </td>

                    <td>
                      {status?.commit_index ?? "—"}
                    </td>

                    <td>
                      {status?.applied_index ?? "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <p>
          Metadata observations are local committed
          state, not linearizable metadata reads.
          Group 5 is excluded from data key routing.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Raw metadata response</h2>

        {cluster.shardMap ? (
          <pre className="ms-json">
            {JSON.stringify(
              cluster.shardMap,
              null,
              2,
            )}
          </pre>
        ) : (
          <div className="ms-empty">
            No metadata response available.
          </div>
        )}
      </article>
    </section>
  );
}

export default MultiShards;

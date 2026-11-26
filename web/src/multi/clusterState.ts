import type {
  MultiRaftNodeObservation,
  StorageNodeID,
} from "../api/multiRaft";

import type {
  DataShardID,
} from "../api/multiDiagnostics";

export const DATA_SHARDS = [1, 2, 3, 4] as const;

export const SHARD_PLACEMENT:
  Record<DataShardID, readonly StorageNodeID[]> = {
    1: [1, 2, 3],
    2: [2, 3, 4],
    3: [3, 4, 5],
    4: [1, 4, 5],
  };

export const METADATA_REPLICAS = [1, 2, 3] as const;

export interface ReplicaState {
  nodeId: StorageNodeID;
  reachable: boolean;
  role: string;
  term: number | null;
  leaderId: number | null;
  commitIndex: number | null;
  appliedIndex: number | null;
}

export interface ShardState {
  shardId: DataShardID;
  replicas: ReplicaState[];
  leaderId: StorageNodeID | null;
  reachableReplicas: number;
  majorityReachable: boolean;
  replicationLag: number | null;
  state: "healthy" | "degraded" | "unavailable" | "electing";
}

export function deriveShardStates(
  observations: MultiRaftNodeObservation[],
): ShardState[] {
  return DATA_SHARDS.map(shardId => {
    const replicas: ReplicaState[] =
      SHARD_PLACEMENT[shardId].map(nodeId => {
        const observation = observations.find(
          node => node.id === nodeId,
        );

        const group = observation?.reachable
          ? observation.status.groups.find(
              group => group.shard_id === shardId,
            )
          : undefined;

        return {
          nodeId,
          reachable: group !== undefined,
          role: group?.role ?? "unknown",
          term: group?.term ?? null,
          leaderId: group?.leader_id ?? null,
          commitIndex: group?.commit_index ?? null,
          appliedIndex: group?.applied_index ?? null,
        };
      });

    const reachable = replicas.filter(
      replica => replica.reachable,
    );

    const leaders = reachable.filter(
      replica => replica.role === "leader",
    );

    const leader = leaders.length === 1
      ? leaders[0]
      : undefined;

    const majorityReachable = reachable.length >= 2;

    const commits = reachable
      .map(replica => replica.commitIndex)
      .filter((value): value is number => value !== null);

    const replicationLag = commits.length >= 2
      ? Math.max(...commits) - Math.min(...commits)
      : null;

    const state: ShardState["state"] =
      !majorityReachable
        ? "unavailable"
        : !leader
          ? "electing"
          : reachable.length < 3
            ? "degraded"
            : "healthy";

    return {
      shardId,
      replicas,
      leaderId: leader?.nodeId ?? null,
      reachableReplicas: reachable.length,
      majorityReachable,
      replicationLag,
      state,
    };
  });
}

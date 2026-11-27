import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
  METADATA_REPLICAS,
} from "./clusterState";

import type {
  StorageNodeID,
} from "../api/multiRaft";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

export const MAX_OBSERVATION_AGE_MS = 10000;

export interface QuorumGroupImpact {
  id: string;
  label: string;
  members: readonly StorageNodeID[];
  leaderId: number | null;
  current: number;
  remaining: number;
  required: number;
  affected: boolean;
  majorityRetained: boolean;
}

export interface QuorumAssessment {
  nodeId: StorageNodeID;
  observationsAvailable: boolean;
  observationAgeMs: number | null;
  groups: QuorumGroupImpact[];
  affected: QuorumGroupImpact[];
  risks: QuorumGroupImpact[];
  allowed: boolean;
  reason: string;
}

type QuorumCluster = Pick<
  MultiRaftClusterState,
  | "loading"
  | "lastUpdated"
  | "observations"
  | "shards"
  | "metadata"
  | "metadataLeader"
>;

export function assessNodeQuorum(
  cluster: QuorumCluster,
  nodeId: StorageNodeID,
  now = Date.now(),
): QuorumAssessment {
  const observationAgeMs =
    cluster.lastUpdated === null
      ? null
      : now - cluster.lastUpdated;

  const observationsAvailable =
    !cluster.loading &&
    observationAgeMs !== null &&
    Number.isFinite(observationAgeMs) &&
    observationAgeMs >= 0 &&
    observationAgeMs <= MAX_OBSERVATION_AGE_MS &&
    [1, 2, 3, 4, 5].every(id =>
      cluster.observations.some(
        observation => observation.id === id,
      ),
    );

  function groupImpact(
    id: string,
    label: string,
    members: readonly StorageNodeID[],
    reachable: ReadonlySet<number>,
    leaderId: number | null,
  ): QuorumGroupImpact {
    const required =
      Math.floor(members.length / 2) + 1;

    const current = members.filter(
      member => reachable.has(member),
    ).length;

    const remaining = members.filter(
      member =>
        member !== nodeId &&
        reachable.has(member),
    ).length;

    return {
      id,
      label,
      members,
      leaderId,
      current,
      remaining,
      required,
      affected: members.includes(nodeId),
      majorityRetained: remaining >= required,
    };
  }

  const groups = DATA_SHARDS.map(shardId => {
    const shard = cluster.shards.find(
      item => item.shardId === shardId,
    );

    const reachable = new Set<number>(
      (shard?.replicas ?? [])
        .filter(replica => replica.reachable)
        .map(replica => replica.nodeId),
    );

    return groupImpact(
      `shard-${shardId}`,
      `Shard ${shardId}`,
      SHARD_PLACEMENT[shardId],
      reachable,
      shard?.leaderId ?? null,
    );
  });

  const metadataReachable = new Set<number>(
    cluster.metadata.map(
      replica => replica.node_id,
    ),
  );

  groups.push(
    groupImpact(
      "metadata",
      "Metadata",
      METADATA_REPLICAS,
      metadataReachable,
      cluster.metadataLeader,
    ),
  );

  const affected = groups.filter(
    group => group.affected,
  );

  const risks = groups.filter(
    group => !group.majorityRetained,
  );

  const allowed =
    observationsAvailable &&
    affected.length > 0 &&
    risks.length === 0;

  const reason = !observationsAvailable
    ? "Cluster observations are unavailable or stale."
    : affected.length === 0
      ? "Configured node membership is unavailable."
      : risks.length > 0
        ? "Reachable majority at risk: " +
          risks.map(group => group.label).join(", ")
        : "All configured groups retain a reachable majority.";

  return {
    nodeId,
    observationsAvailable,
    observationAgeMs,
    groups,
    affected,
    risks,
    allowed,
    reason,
  };
}

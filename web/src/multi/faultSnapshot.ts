import {
  DATA_SHARDS,
  SHARD_PLACEMENT,
  METADATA_REPLICAS,
} from "./clusterState";

import {
  STORAGE_NODE_IDS,
  type StorageNodeID,
} from "../api/multiRaft";

import {
  MAX_OBSERVATION_AGE_MS,
} from "./quorumProtection";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

export type SnapshotGroupID = 1 | 2 | 3 | 4 | 5;

export interface ReplicaSnapshot {
  readonly nodeId: StorageNodeID;
  readonly reachable: boolean | null;
  readonly role: string;
  readonly term: number | null;
  readonly leaderId: number | null;
  readonly commitIndex: number | null;
  readonly appliedIndex: number | null;
}

export interface GroupSnapshot {
  readonly groupId: SnapshotGroupID;
  readonly label: string;
  readonly leaderId: number | null;
  readonly term: number | null;
  readonly commitIndex: number | null;
  readonly appliedIndex: number | null;
  readonly reachable: number | null;
  readonly required: number;
  readonly majority: boolean | null;
  readonly commitSpread: number | null;
  readonly replicas: readonly ReplicaSnapshot[];
}

export interface NodeSnapshot {
  readonly nodeId: StorageNodeID;
  readonly reachable: boolean | null;
  readonly observedAt: number | null;
}

export interface ClusterSnapshot {
  readonly capturedAt: number;
  readonly observedAt: number | null;
  readonly observationAgeMs: number | null;
  readonly available: boolean;
  readonly reachableNodes: number | null;
  readonly reachableMajorities: number | null;
  readonly groups: readonly GroupSnapshot[];
  readonly nodes: readonly NodeSnapshot[];
}

export type QuorumTransition =
  | "retained"
  | "lost"
  | "restored"
  | "unavailable"
  | "unknown";

export type ReachabilityTransition =
  | "unchanged"
  | "lost"
  | "restored"
  | "unknown";

export type CatchUpObservation =
  | "at-observed-leader-index"
  | "behind-observed-leader"
  | "ahead-of-observed-leader"
  | "unavailable"
  | "unknown";

export interface ReplicaComparison {
  readonly nodeId: StorageNodeID;
  readonly before: ReplicaSnapshot;
  readonly after: ReplicaSnapshot;
  readonly reachability: ReachabilityTransition;
  readonly termDelta: number | null;
  readonly commitDelta: number | null;
  readonly appliedDelta: number | null;
  readonly catchUp: CatchUpObservation;
}

export interface GroupComparison {
  readonly groupId: SnapshotGroupID;
  readonly label: string;
  readonly before: GroupSnapshot;
  readonly after: GroupSnapshot;
  readonly leaderChanged: boolean | null;
  readonly termDelta: number | null;
  readonly commitDelta: number | null;
  readonly appliedDelta: number | null;
  readonly reachableDelta: number | null;
  readonly quorum: QuorumTransition;
  readonly replicas: readonly ReplicaComparison[];
}

export interface ClusterComparison {
  readonly before: ClusterSnapshot;
  readonly after: ClusterSnapshot;
  readonly comparable: boolean;
  readonly observationIntervalMs: number | null;
  readonly reachableNodeDelta: number | null;
  readonly majorityDelta: number | null;
  readonly leadershipChanges: number;
  readonly groups: readonly GroupComparison[];
}

type SnapshotCluster = Pick<
  MultiRaftClusterState,
  | "loading"
  | "lastUpdated"
  | "observations"
  | "shards"
  | "metadata"
  | "metadataLeader"
>;

function counter(value: unknown): number | null {
  return typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0
    ? value
    : null;
}

function validLeader(
  value: unknown,
  members: readonly StorageNodeID[],
): number | null {
  return typeof value === "number" &&
    members.some(member => member === value)
    ? value
    : null;
}

function delta(
  before: number | null,
  after: number | null,
): number | null {
  return before === null || after === null
    ? null
    : after - before;
}

function unknownReplica(
  nodeId: StorageNodeID,
  reachable: boolean | null = null,
): ReplicaSnapshot {
  return Object.freeze({
    nodeId,
    reachable,
    role: "unknown",
    term: null,
    leaderId: null,
    commitIndex: null,
    appliedIndex: null,
  });
}

export function captureClusterSnapshot(
  cluster: SnapshotCluster,
  now = Date.now(),
): ClusterSnapshot {
  const observedAt = cluster.lastUpdated;

  const observationAgeMs =
    observedAt === null
      ? null
      : now - observedAt;

  const available =
    !cluster.loading &&
    observationAgeMs !== null &&
    Number.isFinite(observationAgeMs) &&
    observationAgeMs >= 0 &&
    observationAgeMs <= MAX_OBSERVATION_AGE_MS &&
    STORAGE_NODE_IDS.every(id =>
      cluster.observations.filter(
        observation => observation.id === id,
      ).length === 1,
    );

  const nodes = STORAGE_NODE_IDS.map(nodeId => {
    const observation = cluster.observations.find(
      item => item.id === nodeId,
    );

    return Object.freeze({
      nodeId,
      reachable: available
        ? observation?.reachable ?? null
        : null,
      observedAt: available
        ? observation?.observedAt ?? null
        : null,
    });
  });

  function captureGroup(
    groupId: SnapshotGroupID,
    members: readonly StorageNodeID[],
  ): GroupSnapshot {
    const label = groupId === 5
      ? "Metadata"
      : `Shard ${groupId}`;

    const shard = cluster.shards.find(
      item => item.shardId === groupId,
    );

    const replicas = members.map(nodeId => {
      if (!available) {
        return unknownReplica(nodeId);
      }

      if (groupId === 5) {
        const matches = cluster.metadata.filter(
          item => item.node_id === nodeId,
        );

        if (matches.length !== 1) {
          return unknownReplica(
            nodeId,
            matches.length === 0 ? false : null,
          );
        }

        const metadata = matches[0];

        // Use only fields supported by the API.
        // Missing numeric counters remain unknown.
        const raw = metadata as unknown as
          Record<string, unknown>;

        return Object.freeze({
          nodeId,
          reachable: true,
          role: metadata.role,
          term: counter(raw.term),
          leaderId: validLeader(
            raw.leader_id,
            members,
          ),
          commitIndex: counter(raw.commit_index),
          appliedIndex: counter(raw.applied_index),
        });
      }

      const matches = shard?.replicas.filter(
        item => item.nodeId === nodeId,
      ) ?? [];

      if (matches.length !== 1) {
        return unknownReplica(nodeId);
      }

      const replica = matches[0];

      if (!replica.reachable) {
        return unknownReplica(nodeId, false);
      }

      return Object.freeze({
        nodeId,
        reachable: true,
        role: replica.role,
        term: counter(replica.term),
        leaderId: validLeader(
          replica.leaderId,
          members,
        ),
        commitIndex: counter(replica.commitIndex),
        appliedIndex: counter(replica.appliedIndex),
      });
    });

    const complete = available &&
      replicas.every(
        replica => replica.reachable !== null,
      );

    const reachable = complete
      ? replicas.filter(
          replica => replica.reachable === true,
        ).length
      : null;

    const required =
      Math.floor(members.length / 2) + 1;

    const leaders = replicas.filter(
      replica =>
        replica.reachable === true &&
        replica.role === "leader",
    );

    const leader = leaders.length === 1
      ? leaders[0]
      : undefined;

    const reachableReplicas = replicas.filter(
      replica => replica.reachable === true,
    );

    const commits = reachableReplicas.map(
      replica => replica.commitIndex,
    );

    const knownCommits = commits.filter(
      (value): value is number => value !== null,
    );

    const commitSpread =
      complete &&
      knownCommits.length >= 2 &&
      knownCommits.length === commits.length
        ? Math.max(...knownCommits) -
          Math.min(...knownCommits)
        : null;

    return Object.freeze({
      groupId,
      label,
      leaderId: leader?.nodeId ?? null,
      term: leader?.term ?? null,
      commitIndex: leader?.commitIndex ?? null,
      appliedIndex: leader?.appliedIndex ?? null,
      reachable,
      required,
      majority: reachable === null
        ? null
        : reachable >= required,
      commitSpread,
      replicas: Object.freeze(replicas),
    });
  }

  const groups = DATA_SHARDS.map(groupId =>
    captureGroup(
      groupId,
      SHARD_PLACEMENT[groupId],
    ),
  );

  groups.push(
    captureGroup(5, METADATA_REPLICAS),
  );

  return Object.freeze({
    capturedAt: now,
    observedAt,
    observationAgeMs,
    available,
    reachableNodes: available
      ? nodes.filter(
          node => node.reachable === true,
        ).length
      : null,
    reachableMajorities:
      groups.every(group => group.majority !== null)
        ? groups.filter(
            group => group.majority === true,
          ).length
        : null,
    groups: Object.freeze(groups),
    nodes: Object.freeze(nodes),
  });
}

export function quorumTransition(
  before: boolean | null,
  after: boolean | null,
): QuorumTransition {
  if (before === null || after === null) {
    return "unknown";
  }

  if (before && after) return "retained";
  if (before && !after) return "lost";
  if (!before && after) return "restored";

  return "unavailable";
}

export function reachabilityTransition(
  before: boolean | null,
  after: boolean | null,
): ReachabilityTransition {
  if (before === null || after === null) {
    return "unknown";
  }

  if (before === after) return "unchanged";

  return after ? "restored" : "lost";
}

export function observeReplicaCatchUp(
  replica: ReplicaSnapshot,
  group: GroupSnapshot,
): CatchUpObservation {
  if (replica.reachable === false) {
    return "unavailable";
  }

  if (
    replica.reachable !== true ||
    group.leaderId === null ||
    group.term === null ||
    replica.term !== group.term ||
    replica.commitIndex === null ||
    replica.appliedIndex === null ||
    group.commitIndex === null ||
    group.appliedIndex === null
  ) {
    return "unknown";
  }

  if (
    replica.commitIndex > group.commitIndex ||
    replica.appliedIndex > group.appliedIndex
  ) {
    return "ahead-of-observed-leader";
  }

  if (
    replica.commitIndex === group.commitIndex &&
    replica.appliedIndex === group.appliedIndex
  ) {
    return "at-observed-leader-index";
  }

  return "behind-observed-leader";
}

export function compareClusterSnapshots(
  before: ClusterSnapshot,
  after: ClusterSnapshot,
): ClusterComparison {
  const comparable =
    before.available &&
    after.available &&
    before.observedAt !== null &&
    after.observedAt !== null &&
    after.observedAt > before.observedAt;

  const difference = (
    previous: number | null,
    current: number | null,
  ) => comparable
    ? delta(previous, current)
    : null;

  const groups = before.groups.map(previous => {
    const current = after.groups.find(
      group => group.groupId === previous.groupId,
    );

    if (!current) {
      throw new Error(
        `Missing snapshot group ${previous.groupId}`,
      );
    }

    const replicas = previous.replicas.map(replica => {
      const next = current.replicas.find(
        item => item.nodeId === replica.nodeId,
      ) ?? unknownReplica(replica.nodeId);

      return Object.freeze({
        nodeId: replica.nodeId,
        before: replica,
        after: next,
        reachability: comparable
          ? reachabilityTransition(
              replica.reachable,
              next.reachable,
            )
          : "unknown" as const,
        termDelta: difference(
          replica.term,
          next.term,
        ),
        commitDelta: difference(
          replica.commitIndex,
          next.commitIndex,
        ),
        appliedDelta: difference(
          replica.appliedIndex,
          next.appliedIndex,
        ),
        catchUp: after.available
          ? observeReplicaCatchUp(next, current)
          : "unknown" as const,
      });
    });

    return Object.freeze({
      groupId: previous.groupId,
      label: previous.label,
      before: previous,
      after: current,
      leaderChanged:
        comparable &&
        previous.leaderId !== null &&
        current.leaderId !== null
          ? previous.leaderId !== current.leaderId
          : null,
      termDelta: difference(
        previous.term,
        current.term,
      ),
      commitDelta: difference(
        previous.commitIndex,
        current.commitIndex,
      ),
      appliedDelta: difference(
        previous.appliedIndex,
        current.appliedIndex,
      ),
      reachableDelta: difference(
        previous.reachable,
        current.reachable,
      ),
      quorum: comparable
        ? quorumTransition(
            previous.majority,
            current.majority,
          )
        : "unknown" as const,
      replicas: Object.freeze(replicas),
    });
  });

  return Object.freeze({
    before,
    after,
    comparable,
    observationIntervalMs: comparable
      ? delta(before.observedAt, after.observedAt)
      : null,
    reachableNodeDelta: difference(
      before.reachableNodes,
      after.reachableNodes,
    ),
    majorityDelta: difference(
      before.reachableMajorities,
      after.reachableMajorities,
    ),
    leadershipChanges: groups.filter(
      group => group.leaderChanged === true,
    ).length,
    groups: Object.freeze(groups),
  });
}

export const STORAGE_NODE_IDS = [1, 2, 3, 4, 5] as const;
export const GATEWAY_IDS = [1, 2] as const;

export type StorageNodeID =
  (typeof STORAGE_NODE_IDS)[number];

export type GatewayID =
  (typeof GATEWAY_IDS)[number];

export type MultiRaftRole =
  | "leader"
  | "follower"
  | "candidate"
  | "unknown";

export interface MultiRaftGroupStatus {
  shard_id: number;
  role: MultiRaftRole;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
}

export interface MultiRaftNodeStatus {
  id: StorageNodeID;
  mode: "multi-raft";
  groups: MultiRaftGroupStatus[];
}

export type MultiRaftNodeObservation =
  | {
      id: StorageNodeID;
      reachable: true;
      status: MultiRaftNodeStatus;
      observedAt: number;
    }
  | {
      id: StorageNodeID;
      reachable: false;
      error: string;
      observedAt: number;
    };

export const multiNodeBase = (id: StorageNodeID) =>
  `/multi/node${id}`;

export const multiGatewayBase = (id: GatewayID) =>
  `/multi/gateway${id}`;

function isCounter(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0
  );
}

function isGroupStatus(
  value: unknown,
): value is MultiRaftGroupStatus {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const group = value as Record<string, unknown>;

  return (
    isCounter(group.shard_id) &&
    typeof group.role === "string" &&
    ["leader", "follower", "candidate", "unknown"]
      .includes(group.role) &&
    isCounter(group.term) &&
    isCounter(group.leader_id) &&
    isCounter(group.commit_index) &&
    isCounter(group.applied_index)
  );
}

export function parseMultiRaftStatus(
  value: unknown,
  expectedID: StorageNodeID,
): MultiRaftNodeStatus {
  if (typeof value !== "object" || value === null) {
    throw new Error("Invalid multi-Raft status response.");
  }

  const status = value as Record<string, unknown>;

  if (
    status.id !== expectedID ||
    status.mode !== "multi-raft" ||
    !Array.isArray(status.groups) ||
    status.groups.length === 0 ||
    !status.groups.every(isGroupStatus)
  ) {
    throw new Error("Invalid multi-Raft node status.");
  }

  const groupIDs = status.groups.map(
    (group) => group.shard_id,
  );

  if (new Set(groupIDs).size !== groupIDs.length) {
    throw new Error("Duplicate Raft group identifiers.");
  }

  return {
    id: expectedID,
    mode: "multi-raft",
    groups: status.groups,
  };
}

export async function getMultiRaftNodeStatus(
  id: StorageNodeID,
): Promise<MultiRaftNodeStatus> {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    2500,
  );

  try {
    const response = await fetch(
      `${multiNodeBase(id)}/status`,
      {
        cache: "no-store",
        signal: controller.signal,
      },
    );

    if (!response.ok) {
      throw new Error(
        `Node ${id} returned HTTP ${response.status}`,
      );
    }

    const value: unknown = await response.json();

    return parseMultiRaftStatus(value, id);
  } finally {
    clearTimeout(timeout);
  }
}

export async function getMultiRaftClusterStatuses():
  Promise<MultiRaftNodeObservation[]> {
  return Promise.all(
    STORAGE_NODE_IDS.map(
      async (id): Promise<MultiRaftNodeObservation> => {
        try {
          const status = await getMultiRaftNodeStatus(id);

          return {
            id,
            reachable: true,
            status,
            observedAt: Date.now(),
          };
        } catch (error) {
          return {
            id,
            reachable: false,
            error:
              error instanceof Error
                ? error.message
                : String(error),
            observedAt: Date.now(),
          };
        }
      },
    ),
  );
}

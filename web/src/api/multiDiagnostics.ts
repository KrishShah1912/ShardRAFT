import {
  multiNodeBase,
  type StorageNodeID,
} from "./multiRaft";

export type DataShardID = 1 | 2 | 3 | 4;
export type MetadataNodeID = 1 | 2 | 3;

export type DiagnosticEndpoint =
  | "raft"
  | "raft/log"
  | "kv"
  | "storage"
  | "transport";

export interface MetadataStatus {
  node_id: MetadataNodeID;
  group_id: 5;
  role: string;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
  consistency: "local";
}

export interface SlotRange {
  start_slot: number;
  end_slot: number;
}

export interface ReplicaInfo {
  node_id: StorageNodeID;
  address: string;
}

export interface ShardInfo {
  shard_id: DataShardID;
  epoch: number;
  slot_ranges: SlotRange[];
  replicas: ReplicaInfo[];
  leader_id: number;
}

export interface ShardMap {
  epoch: number;
  total_slots: number;
  shards: ShardInfo[];
}

export interface ShardMapResponse {
  node_id: MetadataNodeID;
  consistency: "local";
  shard_map: ShardMap;
}

export interface ShardDiagnostic<T = unknown> {
  node_id: StorageNodeID;
  shard_id: DataShardID;
  consistency?: "local";
  protocol?: "grpc";
  data: T;
}

export interface MultiRaftEvent {
  id: number;
  timestamp: string;
  node_id: StorageNodeID;
  shard_id: DataShardID;
  category: string;
  type: string;
  term?: number;
  data?: Record<string, unknown>;
}

export interface EventHistory {
  events: MultiRaftEvent[];
  latest_event_id: number;
}

export interface DockerService {
  service: string;
  state: string;
  health: string;
  status: string;
  containerId: string;
}

export type DockerAction =
  | "start"
  | "stop"
  | "restart";

export interface DockerActionResult {
  node_id: StorageNodeID;
  action: DockerAction;
  success: boolean;
  output: string;
}

export async function requestJSON<T>(
  url: string,
  init: RequestInit = {},
  timeout = 5000,
): Promise<T> {
  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    timeout,
  );

  try {
    const response = await fetch(url, {
      ...init,
      cache: "no-store",
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text();

      throw new Error(
        `HTTP ${response.status}: ${body}`,
      );
    }

    return await response.json() as T;
  } finally {
    clearTimeout(timer);
  }
}

export function getMetadataStatus(
  node: MetadataNodeID,
): Promise<MetadataStatus> {
  return requestJSON(
    `${multiNodeBase(node)}/api/v1/metadata/status`,
  );
}

export function normalizeShardMap(
  response: ShardMapResponse,
): ShardMapResponse {
  return {
    ...response,
    shard_map: {
      ...response.shard_map,
      shards: response.shard_map.shards.map(
        shard => ({
          ...shard,
          leader_id: shard.leader_id ?? 0,
          slot_ranges: shard.slot_ranges.map(
            range => ({
              start_slot: range.start_slot ?? 0,
              end_slot: range.end_slot ?? 0,
            }),
          ),
        }),
      ),
    },
  };
}

export async function getShardMap(
  node: MetadataNodeID,
): Promise<ShardMapResponse> {
  const result = await requestJSON<ShardMapResponse>(
    `${multiNodeBase(node)}/api/v1/metadata/shard-map`,
  );

  return normalizeShardMap(result);
}

export async function getShardDiagnostic<T = unknown>(
  node: StorageNodeID,
  shard: DataShardID,
  endpoint: DiagnosticEndpoint,
): Promise<ShardDiagnostic<T>> {
  const result = await requestJSON<ShardDiagnostic<T>>(
    `${multiNodeBase(node)}/api/v1/shards/${shard}/${endpoint}`,
  );

  if (
    result.node_id !== node ||
    result.shard_id !== shard
  ) {
    throw new Error("Diagnostic identity mismatch");
  }

  return result;
}

export function getMultiEventHistory(
  node: StorageNodeID,
  after = 0,
): Promise<EventHistory> {
  return requestJSON(
    `${multiNodeBase(node)}/api/v1/events/history?after=${after}`,
  );
}

export function multiEventStreamURL(
  node: StorageNodeID,
  after = 0,
): string {
  return (
    `${multiNodeBase(node)}/api/v1/events?after=${after}`
  );
}

export function parseDockerServices(
  output: string,
): DockerService[] {
  const text = output.trim();

  if (!text) {
    return [];
  }

  const records = text.startsWith("[")
    ? JSON.parse(text)
    : text.split(/\r?\n/).map(
        line => JSON.parse(line),
      );

  if (!Array.isArray(records)) {
    throw new Error("Invalid Docker service response");
  }

  return records.map(record => ({
    service: String(record.Service ?? ""),
    state: String(record.State ?? "unknown"),
    health: String(record.Health ?? ""),
    status: String(record.Status ?? ""),
    containerId: String(record.ID ?? ""),
  })).filter(service => service.service !== "");
}

export async function getDockerServices():
  Promise<DockerService[]> {
  const result = await requestJSON<{
    mode: string;
    output: string;
  }>(
    "/multi/control/api/v1/control/cluster",
    {},
    12000,
  );

  return parseDockerServices(result.output);
}

export function runDockerAction(
  node: StorageNodeID,
  action: DockerAction,
): Promise<DockerActionResult> {
  return requestJSON(
    `/multi/control/api/v1/control/nodes/${node}/${action}`,
    { method: "POST" },
    70000,
  );
}

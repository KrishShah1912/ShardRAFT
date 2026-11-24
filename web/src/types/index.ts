export type Role = 0 | 1 | 2;

export interface NodeStatus {
  id: number;
  role: Role;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
  latest_event_id?: number;
  reachable?: boolean;
}

export interface RaftEvent {
  id: number;
  timestamp: string;
  node_id: number;
  category: string;
  type: string;
  term?: number;
  data?: Record<string, unknown>;
}

export interface EventHistory {
  events: RaftEvent[];
  latest_event_id: number;
}

export interface PeerReplication {
  node_id: number;
  next_index: number;
  match_index: number;
  pending_append: Record<string, number>;
}

export interface RaftDebugState {
  id: number;
  role: Role;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
  first_log_index: number;
  last_log_index: number;
  snapshot_index: number;
  snapshot_term: number;
  election_elapsed: number;
  election_timeout: number;
  heartbeat_elapsed: number;
  heartbeat_timeout: number;
  peers: PeerReplication[];
}

export interface RaftLogEntry {
  index: number;
  term: number;
  type: number;
  data?: string;
}

export interface RaftLog {
  snapshot_index: number;
  snapshot_term: number;
  first_index: number;
  last_index: number;
  commit_index: number;
  applied_index: number;
  entries: RaftLogEntry[];
}

export interface KVItem {
  key: string;
  value: string;
}

export interface KVSnapshot {
  count: number;
  applied_index: number;
  items: KVItem[];
}

export interface WALStats {
  active_segment: number;
  active_size: number;
  max_segment_size: number;
  recovered: boolean;
  closed: boolean;
  unusable: boolean;
}

export interface SnapshotStats {
  exists: boolean;
  version?: number;
  applied_index?: number;
  applied_term?: number;
  records?: number;
  dedup_records?: number;
  valid: boolean;
  error?: string;
}

export interface StorageDebug {
  engine: string;
  wal: WALStats;
  snapshot: SnapshotStats;
  records: number;
  applied_index: number;
}

export type Page =
  | "overview"
  | "architecture"
  | "cluster"
  | "replication"
  | "database"
  | "console"
  | "raft-log"
  | "storage"
  | "transport"
  | "metrics"
  | "events"
  | "faults"
  | "performance"
  | "test-lab"
  | "shards";

export interface TransportPeerStats {
  peer_id: number;
  address: string;
  queue_depth: number;
  queue_capacity: number;
  enqueued: number;
  sent: number;
  delivered: number;
  retries: number;
  failures: number;
  backpressure: number;
  last_latency_ms: number;
  average_latency_ms: number;
  last_success_at?: string;
  last_failure_at?: string;
  last_error?: string;
}

export interface TransportStats {
  closed: boolean;
  peers: TransportPeerStats[];
  enqueued: number;
  sent: number;
  delivered: number;
  retries: number;
  failures: number;
  backpressure: number;
}

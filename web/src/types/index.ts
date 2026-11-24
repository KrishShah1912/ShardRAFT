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

export type Page =
  | "overview"
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
  | "shards";

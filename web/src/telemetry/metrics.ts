import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

export interface TimePoint {
  timestamp: number;
  value: number;
}

export interface NodeSeriesPoint {
  timestamp: number;
  node1: number;
  node2: number;
  node3: number;
}

export interface AvailabilitySample {
  timestamp: number;
  node1: boolean;
  node2: boolean;
  node3: boolean;
  leaderId?: number;
}

export interface MetricsSnapshot {
  timestamp: number;
  commitIndex: number;
  appliedIndex: number;
  replicationLag: number;
  transportSent: number;
  transportDelivered: number;
  transportRetries: number;
  transportFailures: number;
  averageLatency: number;
  heartbeatEvents: number;
}

export function createMetricsSnapshot(
  nodes: NodeStatus[],
  events: RaftEvent[],
  transport?: TransportStats,
): MetricsSnapshot {
  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  const replicationLag = leader
    ? Math.max(
        0,
        ...nodes
          .filter(
            (node) =>
              node.id !== leader.id &&
              node.reachable !== false,
          )
          .map((node) =>
            Math.max(
              0,
              leader.commit_index -
                node.applied_index,
            ),
          ),
      )
    : 0;

  const peers =
    transport?.peers ?? [];

  const latencyPeers = peers.filter(
    (peer) =>
      Number.isFinite(
        peer.average_latency_ms,
      ) &&
      peer.average_latency_ms > 0,
  );

  const averageLatency =
    latencyPeers.length === 0
      ? 0
      : latencyPeers.reduce(
          (sum, peer) =>
            sum +
            peer.average_latency_ms,
          0,
        ) / latencyPeers.length;

  const heartbeatEvents = events.filter(
    (event) =>
      event.type ===
        "raft.heartbeat.sent" ||
      event.type ===
        "raft.heartbeat.received" ||
      event.type ===
        "raft.heartbeat.accepted",
  ).length;

  return {
    timestamp: Date.now(),
    commitIndex:
      leader?.commit_index ?? 0,
    appliedIndex:
      leader?.applied_index ?? 0,
    replicationLag,
    transportSent:
      transport?.sent ?? 0,
    transportDelivered:
      transport?.delivered ?? 0,
    transportRetries:
      transport?.retries ?? 0,
    transportFailures:
      transport?.failures ?? 0,
    averageLatency,
    heartbeatEvents,
  };
}

export function availabilitySample(
  nodes: NodeStatus[],
): AvailabilitySample {
  const reachable = (id: number) =>
    nodes.find((node) => node.id === id)
      ?.reachable !== false;

  const leader = nodes.find(
    (node) =>
      node.role === 2 &&
      node.reachable !== false,
  );

  return {
    timestamp: Date.now(),
    node1: reachable(1),
    node2: reachable(2),
    node3: reachable(3),
    leaderId: leader?.id,
  };
}

export function calculateRate(
  current: number,
  previous: number,
  elapsedMs: number,
): number {
  if (elapsedMs <= 0) {
    return 0;
  }

  return (
    Math.max(0, current - previous) /
    (elapsedMs / 1000)
  );
}

export function availabilityPercent(
  samples: AvailabilitySample[],
): number {
  if (samples.length === 0) {
    return 100;
  }

  let healthy = 0;

  for (const sample of samples) {
    const reachable =
      Number(sample.node1) +
      Number(sample.node2) +
      Number(sample.node3);

    if (reachable >= 2) {
      healthy += 1;
    }
  }

  return (
    (healthy / samples.length) * 100
  );
}

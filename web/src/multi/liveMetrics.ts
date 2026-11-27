import type { LiveEvent } from "./liveTelemetry";

export const METRIC_SERIES = [
  {
    key: "heartbeats",
    label: "Heartbeats sent",
    color: "#60a5fa",
  },
  {
    key: "appends",
    label: "AppendEntries sent",
    color: "#a78bfa",
  },
  {
    key: "acknowledgements",
    label: "Replication progress",
    color: "#2dd4bf",
  },
  {
    key: "commits",
    label: "Quorum commit advances",
    color: "#22c55e",
  },
  {
    key: "transport",
    label: "Transport deliveries",
    color: "#fbbf24",
  },
] as const;

export type MetricKey =
  typeof METRIC_SERIES[number]["key"];

export type LiveMetricSample = {
  timestamp: number;
  complete: boolean;
} & Record<MetricKey, number>;

export function sampleLiveMetrics(
  events: readonly LiveEvent[],
  timestamp: number,
  shard: number | "all",
  connectedNodes: number,
): LiveMetricSample {
  const sample: LiveMetricSample = {
    timestamp,
    complete: connectedNodes === 5,
    heartbeats: 0,
    appends: 0,
    acknowledgements: 0,
    commits: 0,
    transport: 0,
  };

  const start = timestamp - 1000;

  const seen = new Set<string>();

  for (const event of events) {
    const time = Date.parse(event.timestamp);

    if (
      !Number.isFinite(time) ||
      time < start ||
      time >= timestamp
    ) continue;

    if (
      shard !== "all" &&
      event.shard_id !== shard
    ) continue;

    const identity =
      `${event.node_id}:${event.id}:${event.timestamp}`;

    if (seen.has(identity)) continue;
    seen.add(identity);

    if (event.category === "raft") {
      switch (event.type) {
        case "raft.heartbeat.sent":
          sample.heartbeats++;
          break;

        case "raft.append.sent":
          if (
            typeof event.data.entries === "number" &&
            event.data.entries > 0
          ) {
            sample.appends++;
          }
          break;

        case "raft.replication.progress":
          sample.acknowledgements++;
          break;

        case "raft.commit.advanced":
          if (
            event.data.source === "leader_quorum" &&
            typeof event.data.commit_index === "number" &&
            typeof event.data.previous_commit === "number" &&
            event.data.commit_index >
              event.data.previous_commit
          ) {
            sample.commits++;
          }
          break;
      }
    }

    if (
      event.category === "transport" &&
      event.type === "transport.message.delivered"
    ) {
      sample.transport++;
    }
  }

  return sample;
}

export function appendMetricSample(
  previous: readonly LiveMetricSample[],
  incoming: LiveMetricSample,
  limit = 60,
): LiveMetricSample[] {
  const last = previous[previous.length - 1];

  if (
    last &&
    incoming.timestamp <= last.timestamp
  ) {
    return [...previous];
  }

  return [...previous, incoming].slice(
    -Math.max(1, limit),
  );
}

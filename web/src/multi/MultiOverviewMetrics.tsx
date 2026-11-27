import {
  Server,
  GitBranch,
  ShieldCheck,
  Network,
  Layers,
  Container,
} from "lucide-react";

import { Metric } from "../design-system";

import type {
  Tone,
} from "../design-system";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import "./multiOverview.css";

interface Props {
  cluster: MultiRaftClusterState;
}

function countTone(
  count: number,
  total: number,
): Tone {
  if (count === total) return "success";
  if (count === 0) return "danger";
  return "warning";
}

export function MultiOverviewMetrics({
  cluster,
}: Props) {
  const leaders = cluster.shards.filter(
    shard => shard.leaderId !== null,
  ).length;

  const majorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  const metadataLeader = cluster.metadataLeader;

  const epoch =
    cluster.shardMap?.shard_map.epoch;

  const awaiting =
    cluster.loading || !cluster.lastUpdated;

  function observedTone(tone: Tone): Tone {
    return awaiting ? "neutral" : tone;
  }

  return (
    <div
      className="sr-overview-metrics"
      aria-label="Cluster overview metrics"
    >
      <Metric
        label="Storage nodes"
        value={
          awaiting
            ? "—"
            : `${cluster.reachableNodes}/5`
        }
        detail={
          awaiting
            ? "Awaiting observations"
            : "HTTP reachable"
        }
        tone={observedTone(
          countTone(cluster.reachableNodes, 5),
        )}
        icon={Server}
      />

      <Metric
        label="Shard leaders"
        value={
          awaiting ? "—" : `${leaders}/4`
        }
        detail="Observed data shard leaders"
        tone={observedTone(
          countTone(leaders, 4),
        )}
        icon={GitBranch}
      />

      <Metric
        label="Shard majorities"
        value={
          awaiting ? "—" : `${majorities}/4`
        }
        detail="Reachability based"
        tone={observedTone(
          countTone(majorities, 4),
        )}
        icon={ShieldCheck}
      />

      <Metric
        label="Metadata leader"
        value={
          awaiting
            ? "—"
            : metadataLeader
              ? `Node ${metadataLeader}`
              : "Unknown"
        }
        detail="Observed metadata leader"
        tone={observedTone(
          metadataLeader
            ? "primary"
            : "warning",
        )}
        icon={Network}
      />

      <Metric
        label="Metadata epoch"
        value={
          awaiting
            ? "—"
            : epoch ?? "Unknown"
        }
        detail="Discovered shard map version"
        tone={observedTone(
          epoch !== undefined
            ? "neutral"
            : "warning",
        )}
        icon={Layers}
      />

      <Metric
        label="Docker control"
        value={
          cluster.loading
            ? "—"
            : cluster.controlAvailable
              ? "Available"
              : "Unavailable"
        }
        detail="Local lifecycle controller"
        tone={
          cluster.loading
            ? "neutral"
            : cluster.controlAvailable
              ? "success"
              : "warning"
        }
        icon={Container}
      />
    </div>
  );
}

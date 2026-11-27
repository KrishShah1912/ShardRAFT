import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  getMultiRaftClusterStatuses,
  type MultiRaftNodeObservation,
} from "../api/multiRaft";

import {
  getMetadataStatus,
  getShardMap,
  getDockerServices,
  type MetadataStatus,
  type ShardMapResponse,
  type DockerService,
} from "../api/multiDiagnostics";

import {
  deriveShardStates,
  METADATA_REPLICAS,
} from "./clusterState";

export function useMultiRaftCluster(
  intervalMs = 1500,
) {
  const [observations, setObservations] =
    useState<MultiRaftNodeObservation[]>([]);

  const [metadata, setMetadata] =
    useState<MetadataStatus[]>([]);

  const [shardMap, setShardMap] =
    useState<ShardMapResponse | null>(null);

  const [services, setServices] =
    useState<DockerService[]>([]);

  const [controlAvailable, setControlAvailable] =
    useState(false);

  const [loading, setLoading] = useState(true);

  const [lastUpdated, setLastUpdated] =
    useState<number | null>(null);

  const [lastPollStartedAt, setLastPollStartedAt] =
    useState<number | null>(null);

  const [clusterErrors, setClusterErrors] =
    useState<string[]>([]);

  const [controlError, setControlError] =
    useState("");

  const actionOwner = useRef<string | null>(null);

  const [actionLocked, setActionLocked] =
    useState(false);

  const [actionCompletedAt, setActionCompletedAt] =
    useState<number | null>(null);

  const [lastControlPollStartedAt, setLastControlPollStartedAt] =
    useState<number | null>(null);

  const acquireAction = useCallback(() => {
    if (actionOwner.current !== null) {
      return null;
    }

    const token = crypto.randomUUID();

    actionOwner.current = token;
    setActionCompletedAt(null);
    setActionLocked(true);

    return token;
  }, []);

  const finishAction = useCallback((token: string) => {
    if (actionOwner.current !== token) return;

    setActionCompletedAt(Date.now());
  }, []);

  useEffect(() => {
    if (actionCompletedAt === null) return;

    if (
      lastPollStartedAt === null ||
      lastControlPollStartedAt === null ||
      lastPollStartedAt <= actionCompletedAt ||
      lastControlPollStartedAt <= actionCompletedAt ||
      lastUpdated === null ||
      !controlAvailable ||
      loading
    ) {
      return;
    }

    actionOwner.current = null;
    setActionCompletedAt(null);
    setActionLocked(false);
  }, [
    actionCompletedAt,
    lastPollStartedAt,
    lastControlPollStartedAt,
    lastUpdated,
    controlAvailable,
    loading,
  ]);

  const generation = useRef(0);
  const active = useRef(false);

  const clusterFlight =
    useRef<number | null>(null);

  const controlFlight =
    useRef<number | null>(null);

  const refreshCluster = useCallback(async () => {
    const token = generation.current;

    if (
      !active.current ||
      clusterFlight.current === token
    ) {
      return;
    }

    clusterFlight.current = token;

    const pollStartedAt = Date.now();

    const current = () =>
      active.current &&
      generation.current === token;

    try {
      const [
        nodes,
        metadataResults,
        mapResults,
      ] = await Promise.all([
        getMultiRaftClusterStatuses(),

        Promise.allSettled(
          METADATA_REPLICAS.map(
            node => getMetadataStatus(node),
          ),
        ),

        Promise.allSettled(
          METADATA_REPLICAS.map(
            node => getShardMap(node),
          ),
        ),
      ]);

      if (!current()) return;

      const availableMetadata =
        metadataResults.flatMap(result =>
          result.status === "fulfilled"
            ? [result.value]
            : [],
        );

      const availableMaps =
        mapResults.flatMap(result =>
          result.status === "fulfilled"
            ? [result.value]
            : [],
        ).sort(
          (a, b) =>
            b.shard_map.epoch -
            a.shard_map.epoch,
        );

      const nextErrors: string[] = [];

      for (const node of nodes) {
        if (!node.reachable) {
          nextErrors.push(
            `Node ${node.id}: ${node.error}`,
          );
        }
      }

      if (!availableMetadata.length) {
        nextErrors.push(
          "Metadata status unavailable",
        );
      }

      if (!availableMaps.length) {
        nextErrors.push(
          "Shard map unavailable",
        );
      }

      setObservations(nodes);
      setMetadata(availableMetadata);
      setShardMap(availableMaps[0] ?? null);
      setClusterErrors(nextErrors);
      setLastPollStartedAt(pollStartedAt);
      setLastUpdated(Date.now());

    } catch (error) {
      if (current()) {
        setObservations([]);
        setMetadata([]);
        setShardMap(null);
        setLastUpdated(null);
        setLastPollStartedAt(null);

        setClusterErrors([
          "Cluster observations unavailable: " +
          String(error),
        ]);
      }
    } finally {
      if (clusterFlight.current === token) {
        clusterFlight.current = null;
      }

      if (current()) {
        setLoading(false);
      }
    }
  }, []);

  const refreshControl = useCallback(async () => {
    const token = generation.current;

    if (
      !active.current ||
      controlFlight.current === token
    ) {
      return;
    }

    controlFlight.current = token;

    const controlPollStartedAt = Date.now();

    const current = () =>
      active.current &&
      generation.current === token;

    try {
      const result = await getDockerServices();

      if (!current()) return;

      setServices(result);
      setLastControlPollStartedAt(controlPollStartedAt);
      setControlAvailable(true);
      setControlError("");

    } catch (error) {
      if (!current()) return;

      setServices([]);
      setLastControlPollStartedAt(null);
      setControlAvailable(false);

      setControlError(
        "Docker control unavailable: " +
        String(error),
      );
    } finally {
      if (controlFlight.current === token) {
        controlFlight.current = null;
      }
    }
  }, []);

  const refresh = useCallback(async () => {
    await Promise.all([
      refreshCluster(),
      refreshControl(),
    ]);
  }, [refreshCluster, refreshControl]);

  useEffect(() => {
    generation.current += 1;
    active.current = true;

    void refreshCluster();
    void refreshControl();

    const clusterTimer = window.setInterval(
      () => void refreshCluster(),
      intervalMs,
    );

    const controlTimer = window.setInterval(
      () => void refreshControl(),
      Math.max(intervalMs, 3000),
    );

    return () => {
      active.current = false;
      generation.current += 1;

      window.clearInterval(clusterTimer);
      window.clearInterval(controlTimer);
    };
  }, [
    intervalMs,
    refreshCluster,
    refreshControl,
  ]);

  const shards = useMemo(
    () => deriveShardStates(observations),
    [observations],
  );

  const reachableNodes = observations.filter(
    node => node.reachable,
  ).length;

  const metadataLeaders = metadata.filter(
    node => node.role === "leader",
  );

  const errors = useMemo(
    () => controlError
      ? [...clusterErrors, controlError]
      : clusterErrors,
    [clusterErrors, controlError],
  );

  return {
    observations,
    shards,
    metadata,
    shardMap,
    services,
    controlAvailable,
    reachableNodes,

    metadataLeader:
      metadataLeaders.length === 1
        ? metadataLeaders[0].node_id
        : null,

    loading,
    lastUpdated,
    lastPollStartedAt,
    actionLocked,
    acquireAction,
    finishAction,
    errors,
    refresh,
  };
}

export type MultiRaftClusterState =
  ReturnType<typeof useMultiRaftCluster>;

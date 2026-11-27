import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  connectMultiEventStream,
  type StreamStatus,
} from "./liveEventStream";

import {
  eventIdentity,
  heartbeatIdentity,
  heartbeatObservation,
  mergeHeartbeat,
  mergeLiveEvents,
  isRecentEvent,
  type LiveEvent,
  type HeartbeatObservation,
} from "./liveTelemetry";

import {
  isElectionEvent,
  mergeElectionEvents,
} from "./electionTelemetry";

import {
  isReplicationEvent,
  mergeReplicationEvents,
} from "./replicationTelemetry";

const NODES = [1, 2, 3, 4, 5];

const HISTORY_LIMIT = 6000;
const BUFFER_LIMIT = 16384;
const REPLICATION_BUFFER_LIMIT = 16384;
const ELECTION_BUFFER_LIMIT = 16384;
const UPDATE_INTERVAL_MS = 200;

export type NodeStreamState = {
  nodeId: number;
  status: StreamStatus;
  lastEventAt: string | null;
};

function initialStreams(): NodeStreamState[] {
  return NODES.map(nodeId => ({
    nodeId,
    status: "connecting",
    lastEventAt: null,
  }));
}

export function useMultiLiveTelemetry() {
  const [events, setEvents] =
    useState<LiveEvent[]>([]);

  const [streams, setStreams] =
    useState<NodeStreamState[]>(initialStreams);

  const [electionEvents, setElectionEvents] =
    useState<LiveEvent[]>([]);

  const [replicationEvents, setReplicationEvents] =
    useState<LiveEvent[]>([]);

  const [now, setNow] = useState(Date.now);

  const [droppedEvents, setDroppedEvents] =
    useState(0);

  const [bufferDrops, setBufferDrops] = useState({
    general: 0,
    replication: 0,
    election: 0,
  });

  useEffect(() => {
    let active = true;

    const pending = new Map<string, LiveEvent>();

    const pendingElections =
      new Map<string, LiveEvent>();

    const pendingReplication =
      new Map<string, LiveEvent>();

    const latestByNode = new Map<number, string>();

    let droppedGeneral = 0;
    let droppedReplication = 0;
    let droppedElection = 0;

    function flushPending() {
      if (!active) return;

      setNow(Date.now());

      if (pendingReplication.size > 0) {
        const batch = [...pendingReplication.values()];

        pendingReplication.clear();

        setReplicationEvents(previous =>
          mergeReplicationEvents(previous, batch),
        );
      }

      if (pendingElections.size > 0) {
        const batch = [...pendingElections.values()];

        pendingElections.clear();

        setElectionEvents(previous =>
          mergeElectionEvents(previous, batch),
        );
      }

      if (pending.size > 0) {
        const batch = [...pending.values()];

        pending.clear();

        setEvents(previous =>
          mergeLiveEvents(
            previous,
            batch,
            HISTORY_LIMIT,
          ),
        );

        const timestamps = new Map(latestByNode);
        latestByNode.clear();

        setStreams(previous =>
          previous.map(stream => {
            const timestamp = timestamps.get(
              stream.nodeId,
            );

            if (!timestamp) return stream;

            if (
              stream.lastEventAt &&
              Date.parse(timestamp) <
                Date.parse(stream.lastEventAt)
            ) {
              return stream;
            }

            return {
              ...stream,
              lastEventAt: timestamp,
            };
          }),
        );

      }

      if (
        droppedGeneral > 0 ||
        droppedReplication > 0 ||
        droppedElection > 0
      ) {
        const general = droppedGeneral;
        const replication = droppedReplication;
        const election = droppedElection;

        droppedGeneral = 0;
        droppedReplication = 0;
        droppedElection = 0;

        setDroppedEvents(previous =>
          previous + general + replication + election,
        );

        setBufferDrops(previous => ({
          general: previous.general + general,
          replication: previous.replication + replication,
          election: previous.election + election,
        }));
      }
    }

    const disconnect = NODES.map(nodeId =>
      connectMultiEventStream(
        nodeId,

        event => {
          if (!active) return;

          const key = eventIdentity(event);

          // Flush pending batches before any queue reaches
          // its eviction boundary, even if browser timers
          // are delayed or SSE history arrives in bursts.
          if (
            (!pending.has(key) &&
              pending.size >= BUFFER_LIMIT) ||
            (isReplicationEvent(event) &&
              !pendingReplication.has(key) &&
              pendingReplication.size >=
                REPLICATION_BUFFER_LIMIT) ||
            (isElectionEvent(event) &&
              !pendingElections.has(key) &&
              pendingElections.size >=
                ELECTION_BUFFER_LIMIT)
          ) {
            flushPending();
          }

          if (isReplicationEvent(event)) {
            if (
              !pendingReplication.has(key) &&
              pendingReplication.size >= REPLICATION_BUFFER_LIMIT
            ) {
              const oldest =
                pendingReplication.keys().next().value;

              if (oldest !== undefined) {
                pendingReplication.delete(oldest);
                droppedReplication += 1;
              }
            }

            pendingReplication.set(key, event);
          }

          if (isElectionEvent(event)) {
            if (
              !pendingElections.has(key) &&
              pendingElections.size >= ELECTION_BUFFER_LIMIT
            ) {
              const oldest =
                pendingElections.keys().next().value;

              if (oldest !== undefined) {
                pendingElections.delete(oldest);
                droppedElection += 1;
              }
            }

            pendingElections.set(key, event);
          }

          if (
            !pending.has(key) &&
            pending.size >= BUFFER_LIMIT
          ) {
            const oldest = pending.keys().next().value;

            if (oldest !== undefined) {
              pending.delete(oldest);
              droppedGeneral += 1;
            }
          }

          pending.set(key, event);

          const previous = latestByNode.get(nodeId);

          if (
            !previous ||
            Date.parse(event.timestamp) >=
              Date.parse(previous)
          ) {
            latestByNode.set(
              nodeId,
              event.timestamp,
            );
          }
        },

        status => {
          if (!active) return;

          setStreams(previous =>
            previous.map(stream =>
              stream.nodeId === nodeId
                ? { ...stream, status }
                : stream,
            ),
          );
        },
      ),
    );

    const timer = window.setInterval(
      flushPending,
      UPDATE_INTERVAL_MS,
    );

    return () => {
      active = false;

      window.clearInterval(timer);

      for (const close of disconnect) {
        close();
      }

      pending.clear();
      pendingElections.clear();
      pendingReplication.clear();
      latestByNode.clear();
    };
  }, []);

  const recentEvents = useMemo(
    () => events.filter(event =>
      isRecentEvent(event.timestamp, now),
    ),
    [events, now],
  );

  const heartbeats = useMemo(() => {
    const observations =
      new Map<string, HeartbeatObservation>();

    for (const event of recentEvents) {
      const heartbeat = heartbeatObservation(event);

      if (!heartbeat) continue;

      const key = heartbeatIdentity(heartbeat);

      observations.set(
        key,
        mergeHeartbeat(
          observations.get(key),
          heartbeat,
        ),
      );
    }

    return [...observations.values()].sort(
      (a, b) =>
        Date.parse(b.timestamp) -
        Date.parse(a.timestamp),
    );
  }, [recentEvents]);

  const connectedNodes = streams.filter(
    stream => stream.status === "connected",
  ).length;

  return {
    events,
    electionEvents,
    replicationEvents,
    recentEvents,
    heartbeats,
    streams,
    connectedNodes,
    droppedEvents,
    bufferDrops,
    now,
  };
}

export type MultiLiveTelemetry =
  ReturnType<typeof useMultiLiveTelemetry>;

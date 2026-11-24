import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getControlledCluster,
  restartControlledNode,
  startControlledNode,
  stopControlledNode,
  type ControlledNode,
} from "./api/control";

import {
  getClusterStatuses,
  getEventHistory,
  getKVSnapshot,
  getRaftDebug,
  getRaftLog,
  getStorageDebug,
  getTransportStats,
} from "./api/client";

import { Sidebar } from "./components/Sidebar";
import { Architecture } from "./features/Architecture";
import { Cluster } from "./features/Cluster";
import { Database } from "./features/Database";
import { Events } from "./features/Events";
import { FaultLab } from "./features/FaultLab";
import { Metrics } from "./features/Metrics";
import { Overview } from "./features/Overview";
import { QueryConsole } from "./features/QueryConsole";
import { RaftLogView } from "./features/RaftLog";
import { Replication } from "./features/Replication";
import { Shards } from "./features/Shards";
import { Storage } from "./features/Storage";
import { Transport } from "./features/Transport";
import { connectEventStream } from "./realtime/events";

import type {
  KVSnapshot,
  NodeStatus,
  Page,
  RaftDebugState,
  RaftEvent,
  RaftLog,
  StorageDebug,
  TransportStats,
} from "./types";

const MAX_EVENTS = 500;

export default function App() {
  const [page, setPage] = useState<Page>("overview");
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [events, setEvents] = useState<RaftEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [controlledNodes, setControlledNodes] =
    useState<ControlledNode[]>([]);

  const [raft, setRaft] = useState<RaftDebugState>();
  const [raftLog, setRaftLog] = useState<RaftLog>();
  const [kv, setKV] = useState<KVSnapshot>();
  const [storage, setStorage] = useState<StorageDebug>();
  const [transport, setTransport] =
    useState<TransportStats>();

  const leader = useMemo(
    () =>
      nodes.find(
        (node) =>
          node.role === 2 && node.reachable !== false,
      ),
    [nodes],
  );

  const refreshNodes = useCallback(async () => {
    setNodes(await getClusterStatuses());
  }, []);

  const refreshControl = useCallback(async () => {
    try {
      const cluster = await getControlledCluster();
      setControlledNodes(cluster.nodes);
    } catch {
      setControlledNodes([]);
    }
  }, []);

  const refreshAllClusterState = useCallback(async () => {
    await Promise.all([
      refreshNodes(),
      refreshControl(),
    ]);
  }, [refreshNodes, refreshControl]);

  const startNode = useCallback(
    async (id: number) => {
      await startControlledNode(id);
      await new Promise((resolve) =>
        window.setTimeout(resolve, 500),
      );
      await refreshAllClusterState();
    },
    [refreshAllClusterState],
  );

  const stopNode = useCallback(
    async (id: number) => {
      await stopControlledNode(id);
      await refreshAllClusterState();
    },
    [refreshAllClusterState],
  );

  const restartNode = useCallback(
    async (id: number) => {
      await restartControlledNode(id);
      await new Promise((resolve) =>
        window.setTimeout(resolve, 500),
      );
      await refreshAllClusterState();
    },
    [refreshAllClusterState],
  );

  const refreshLeaderData = useCallback(async () => {
    if (!leader) return;

    const results = await Promise.allSettled([
      getRaftDebug(leader.id),
      getRaftLog(leader.id),
      getKVSnapshot(leader.id),
      getStorageDebug(leader.id),
      getTransportStats(leader.id),
    ]);

    if (results[0].status === "fulfilled") {
      setRaft(results[0].value);
    }

    if (results[1].status === "fulfilled") {
      setRaftLog(results[1].value);
    }

    if (results[2].status === "fulfilled") {
      setKV(results[2].value);
    }

    if (results[3].status === "fulfilled") {
      setStorage(results[3].value);
    }

    if (results[4].status === "fulfilled") {
      setTransport(results[4].value);
    }
  }, [leader]);

  useEffect(() => {
    void refreshAllClusterState();

    const interval = window.setInterval(
      () => void refreshAllClusterState(),
      1000,
    );

    return () => window.clearInterval(interval);
  }, [refreshAllClusterState]);

  useEffect(() => {
    void refreshLeaderData();

    const interval = window.setInterval(
      () => void refreshLeaderData(),
      1500,
    );

    return () => window.clearInterval(interval);
  }, [refreshLeaderData]);

  useEffect(() => {
    let cancelled = false;

    async function loadHistory() {
      const histories = await Promise.all(
        [1, 2, 3].map(async (id) => {
          try {
            return await getEventHistory(id);
          } catch {
            return {
              events: [],
              latest_event_id: 0,
            };
          }
        }),
      );

      if (cancelled) return;

      setEvents(
        histories
          .flatMap((history) => history.events)
          .sort(
            (a, b) =>
              new Date(b.timestamp).getTime() -
              new Date(a.timestamp).getTime(),
          )
          .slice(0, MAX_EVENTS),
      );
    }

    void loadHistory();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const states = new Map<number, boolean>();

    const disconnectors = [1, 2, 3].map((nodeId) =>
      connectEventStream(
        nodeId,
        (event) => {
          setEvents((current) => {
            const duplicate = current.some(
              (existing) =>
                existing.node_id === event.node_id &&
                existing.id === event.id,
            );

            if (duplicate) return current;

            return [event, ...current].slice(
              0,
              MAX_EVENTS,
            );
          });

          if (
            event.type === "raft.role.changed" ||
            event.type === "raft.term.changed" ||
            event.type === "raft.leader.elected" ||
            event.type === "raft.commit.advanced" ||
            event.type === "raft.replication.progress"
          ) {
            void refreshNodes();
          }
        },
        (state) => {
          states.set(nodeId, state);
          setConnected(
            [...states.values()].some(Boolean),
          );
        },
      ),
    );

    return () => {
      disconnectors.forEach((disconnect) =>
        disconnect(),
      );
    };
  }, [refreshNodes]);

  function refreshEverything() {
    void refreshNodes();
    void refreshLeaderData();
  }

  function renderPage() {
    switch (page) {
      case "overview":
        return (
          <Overview
            nodes={nodes}
            events={events}
            connected={connected}
            controlledNodes={controlledNodes}
            onStart={startNode}
            onStop={stopNode}
            onRestart={restartNode}
          />
        );

      case "architecture":
        return (
          <Architecture
            nodes={nodes}
            events={events}
            transport={transport}
            connected={connected}
          />
        );

      case "cluster":
        return (
          <Cluster
            nodes={nodes}
            controlledNodes={controlledNodes}
            onStart={startNode}
            onStop={stopNode}
            onRestart={restartNode}
          />
        );

      case "replication":
        return (
          <Replication
            nodes={nodes}
            raft={raft}
            events={events}
          />
        );

      case "raft-log":
        return <RaftLogView log={raftLog} />;

      case "database":
        return <Database snapshot={kv} />;

      case "console":
        return (
          <QueryConsole
            leaderId={leader?.id}
            onComplete={refreshEverything}
          />
        );

      case "storage":
        return <Storage storage={storage} />;

      case "events":
        return (
          <Events
            events={events}
            nodes={nodes}
            connected={connected}
          />
        );

      case "transport":
        return (
          <Transport
            nodes={nodes}
            events={events}
            transport={transport}
          />
        );

      case "metrics":
        return (
          <Metrics
            nodes={nodes}
            events={events}
            transport={transport}
            connected={connected}
          />
        );

      case "faults":
        return (
          <FaultLab
            nodes={nodes}
            events={events}
            transport={transport}
            controlledNodes={controlledNodes}
            connected={connected}
            onStart={startNode}
            onStop={stopNode}
            onRestart={restartNode}
          />
        );

      case "shards":
        return (
          <Shards
            nodes={nodes}
            raft={raft}
            raftLog={raftLog}
            kv={kv}
            storage={storage}
            connected={connected}
            onNavigate={setPage}
          />
        );
    }
  }

  return (
    <div className="app-shell">
      <Sidebar page={page} onChange={setPage} />
      <main className="main-content">{renderPage()}</main>
    </div>
  );
}

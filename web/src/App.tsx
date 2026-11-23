import { useCallback, useEffect, useMemo, useState } from "react";
import {
  getClusterStatuses,
  getEventHistory,
} from "./api/client";
import { Sidebar } from "./components/Sidebar";
import { Cluster } from "./features/Cluster";
import { Events } from "./features/Events";
import { Overview } from "./features/Overview";
import { Placeholder } from "./features/Placeholder";
import { QueryConsole } from "./features/QueryConsole";
import { connectEventStream } from "./realtime/events";
import type {
  NodeStatus,
  Page,
  RaftEvent,
} from "./types";

const MAX_EVENTS = 500;

export default function App() {
  const [page, setPage] = useState<Page>("overview");
  const [nodes, setNodes] = useState<NodeStatus[]>([]);
  const [events, setEvents] = useState<RaftEvent[]>([]);
  const [connected, setConnected] = useState(false);

  const leader = useMemo(
    () =>
      nodes.find(
        (node) =>
          node.role === 2 && node.reachable !== false,
      ),
    [nodes],
  );

  const refreshNodes = useCallback(async () => {
    const statuses = await getClusterStatuses();
    setNodes(statuses);
  }, []);

  useEffect(() => {
    void refreshNodes();

    const interval = window.setInterval(
      () => void refreshNodes(),
      1000,
    );

    return () => window.clearInterval(interval);
  }, [refreshNodes]);

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

      if (cancelled) {
        return;
      }

      const combined = histories
        .flatMap((history) => history.events)
        .sort(
          (a, b) =>
            new Date(b.timestamp).getTime() -
            new Date(a.timestamp).getTime(),
        )
        .slice(0, MAX_EVENTS);

      setEvents(combined);
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

            if (duplicate) {
              return current;
            }

            return [event, ...current].slice(
              0,
              MAX_EVENTS,
            );
          });

          if (
            event.type === "raft.role.changed" ||
            event.type === "raft.term.changed" ||
            event.type === "raft.leader.elected" ||
            event.type === "raft.commit.advanced"
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
      disconnectors.forEach((disconnect) => disconnect());
    };
  }, [refreshNodes]);

  function renderPage() {
    switch (page) {
      case "overview":
        return (
          <Overview
            nodes={nodes}
            events={events}
            connected={connected}
          />
        );

      case "cluster":
        return <Cluster nodes={nodes} />;

      case "events":
        return <Events events={events} />;

      case "console":
      case "database":
        return (
          <QueryConsole
            leaderId={leader?.id}
            onComplete={refreshNodes}
          />
        );

      case "replication":
        return (
          <Placeholder
            title="Replication"
            description="Interactive AppendEntries and quorum visualization."
          />
        );

      case "raft-log":
        return (
          <Placeholder
            title="Raft Log"
            description="Committed, applied and pending Raft entries."
          />
        );

      case "storage":
        return (
          <Placeholder
            title="Storage"
            description="WAL, snapshots, Pebble and recovery state."
          />
        );

      case "transport":
        return (
          <Placeholder
            title="Transport"
            description="Peer queues, delivery and retry observability."
          />
        );

      case "metrics":
        return (
          <Placeholder
            title="Metrics"
            description="Throughput, latency and consensus metrics."
          />
        );

      case "faults":
        return (
          <Placeholder
            title="Fault Lab"
            description="Controlled failure and recovery experiments."
          />
        );

      case "shards":
        return (
          <Placeholder
            title="Shards"
            description="Hash-slot and multi-Raft-group architecture."
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

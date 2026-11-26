import { useState } from "react";

import { Sidebar } from "../components/Sidebar";
import type { Page } from "../types";

import { useMultiRaftCluster } from "./useMultiRaftCluster";
import { MultiQueryConsole } from "./MultiQueryConsole";

import { MultiArchitecture } from "./MultiArchitecture";

import { MultiCluster, MultiReplication } from "./MultiConsensus";

import { MultiShards } from "./MultiShards";

import { MultiDataDiagnostics } from "./MultiDataDiagnostics";

import { MultiTransport, MultiMetrics, MultiEvents } from "./MultiTelemetry";

import { MultiFaultLab } from "./MultiFaultLab";

import {
  MultiPerformanceLab,
  MultiTestLab,
} from "./MultiEvaluation";

import "./multiApp.css";

const PAGE_NAMES: Record<Page, string> = {
  overview: "Overview",
  architecture: "Architecture",
  cluster: "Cluster",
  replication: "Replication",
  database: "Database",
  console: "Query Console",
  "raft-log": "Raft Log",
  storage: "Storage",
  transport: "Transport",
  metrics: "Metrics",
  events: "Events",
  faults: "Fault Lab",
  performance: "Performance Lab",
  "test-lab": "Test Lab",
  shards: "Shards",
};

export default function MultiRaftApp() {
  const [page, setPage] =
    useState<Page>("overview");

  const cluster = useMultiRaftCluster();

  const observedLeaders = cluster.shards.filter(
    shard => shard.leaderId !== null,
  ).length;

  const observedMajorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  return (
    <div className="app-shell multi-app">
      <Sidebar
        page={page}
        onChange={setPage}
        mode="multi"
        reachableNodes={cluster.reachableNodes}
        loading={cluster.loading}
      />

      <main className="main-content">
        <div className="ma-toolbar">
          <div className="ma-mode">
            MULTI-RAFT DEPLOYMENT
          </div>

          <div className="ma-toolbar-right">
            <span>
              {cluster.loading
                ? "Connecting..."
                : cluster.lastUpdated
                  ? `Observed ${new Date(
                      cluster.lastUpdated,
                    ).toLocaleTimeString()}`
                  : "Awaiting observations"}
            </span>

            <button
              type="button"
              onClick={() => void cluster.refresh()}
            >
              Refresh
            </button>
          </div>
        </div>

        {cluster.errors.length > 0 && (
          <details className="ma-errors">
            <summary>
              {cluster.errors.length} observation warning(s)
            </summary>

            <ul>
              {cluster.errors.map(error => (
                <li key={error}>{error}</li>
              ))}
            </ul>
          </details>
        )}

        {page === "overview" && (
          <section className="ma-overview">
            <header className="page-header">
              <div>
                <div className="eyebrow">
                  DISTRIBUTED DATABASE
                </div>

                <h1>Multi-Raft Overview</h1>

                <p>
                  Live observations from the five-node
                  Docker deployment.
                </p>
              </div>
            </header>

            <div className="ma-stat-grid">
              {[
                {
                  label: "Reachable storage nodes",
                  value: `${cluster.reachableNodes}/5`,
                },
                {
                  label: "Observed shard leaders",
                  value: `${observedLeaders}/4`,
                },
                {
                  label: "Reachable shard majorities",
                  value: `${observedMajorities}/4`,
                },
                {
                  label: "Metadata leader",
                  value: cluster.metadataLeader
                    ? `Node ${cluster.metadataLeader}`
                    : "Unknown",
                },
                {
                  label: "Metadata epoch",
                  value:
                    cluster.shardMap?.shard_map.epoch
                    ?? "Unknown",
                },
                {
                  label: "Docker control",
                  value: cluster.controlAvailable
                    ? "Available"
                    : "Unavailable",
                },
              ].map(stat => (
                <article
                  className="ma-stat"
                  key={stat.label}
                >
                  <span>{stat.label}</span>

                  <strong>
                    {cluster.loading ? "—" : stat.value}
                  </strong>
                </article>
              ))}
            </div>

            <article className="ma-panel">
              <h2>Data shard observations</h2>

              <p>
                Reachability is an observation, not proof
                of consensus availability.
              </p>

              <div className="ma-table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Shard</th>
                      <th>Observed leader</th>
                      <th>Reachable replicas</th>
                      <th>Commit index spread</th>
                      <th>Observed state</th>
                    </tr>
                  </thead>

                  <tbody>
                    {cluster.shards.map(shard => (
                      <tr key={shard.shardId}>
                        <td>Shard {shard.shardId}</td>

                        <td>
                          {shard.leaderId
                            ? `Node ${shard.leaderId}`
                            : "Unknown"}
                        </td>

                        <td>
                          {shard.reachableReplicas}/3
                        </td>

                        <td>
                          {shard.replicationLag ?? "—"}
                        </td>

                        <td>
                          {cluster.loading
                            ? "Loading"
                            : shard.state}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="ma-panel">
              <h2>Execute database operations</h2>

              <p>
                Send requests through either HTTP gateway
                and inspect metadata-based key routing.
              </p>

              <button
                className="ma-primary"
                onClick={() => setPage("console")}
              >
                Open Query Console
              </button>
            </article>
          </section>
        )}

        <div hidden={page !== "console"}>
          <MultiQueryConsole cluster={cluster} />
        </div>

        {page === "architecture" && (
          <MultiArchitecture cluster={cluster} />
        )}

        {page === "cluster" && (
          <MultiCluster cluster={cluster} />
        )}

        {page === "replication" && (
          <MultiReplication cluster={cluster} />
        )}

        {page === "shards" && (
          <MultiShards cluster={cluster} />
        )}

        {(page === "database" ||
          page === "raft-log" ||
          page === "storage") && (
          <MultiDataDiagnostics
            key={page}
            cluster={cluster}
            page={page}
            onOpenConsole={() => setPage("console")}
          />
        )}

        {page === "transport" && (
          <MultiTransport cluster={cluster} />
        )}

        {page === "metrics" && (
          <MultiMetrics cluster={cluster} />
        )}

        {page === "events" && (
          <MultiEvents />
        )}

        <div hidden={page !== "faults"}>
          <MultiFaultLab
            cluster={cluster}
            onOpenConsole={() => setPage("console")}
          />
        </div>

        {page === "performance" && (
          <MultiPerformanceLab />
        )}

        {page === "test-lab" && (
          <MultiTestLab />
        )}

        {page !== "overview" &&
          page !== "console" &&
          page !== "architecture" &&
          page !== "cluster" &&
          page !== "replication" &&
          page !== "shards" &&
          page !== "database" &&
          page !== "raft-log" &&
          page !== "storage" &&
          page !== "transport" &&
          page !== "metrics" &&
          page !== "events" &&
          page !== "faults" &&
          page !== "performance" &&
          page !== "test-lab" && (
          <article className="ma-panel">
            <h1>{PAGE_NAMES[page]}</h1>

            <p>
              Multi-Raft dashboard integration pending.
              No simulated telemetry is displayed.
            </p>
          </article>
        )}
      </main>
    </div>
  );
}

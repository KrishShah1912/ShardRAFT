import { lazy, Suspense, useState } from "react";

import { MultiSidebar } from "./MultiSidebar";
import { MultiWorkspaceToolbar } from "./MultiWorkspaceToolbar";
import { MultiOverviewMetrics } from "./MultiOverviewMetrics";
import { MultiOverviewShardHealth } from "./MultiOverviewShardHealth";
import { MultiLiveTopology } from "./MultiLiveTopology";
import type { Page } from "../types";

import { useMultiRaftCluster } from "./useMultiRaftCluster";
import { MultiQueryConsole } from "./MultiQueryConsole";


import { MultiFaultLab } from "./MultiFaultLab";


import { useMultiLiveTelemetry } from "./useMultiLiveTelemetry";
import { MultiHeartbeatTelemetry } from "./MultiHeartbeatTelemetry";

import "./multiApp.css";
import "../design-system/tokens.css";
import "../design-system/foundation.css";
import "../design-system/components.css";

const MultiArchitecture = lazy(() =>
  import("./MultiArchitecture").then(module => ({
    default: module.MultiArchitecture,
  })),
);

const MultiCluster = lazy(() =>
  import("./MultiConsensus").then(module => ({
    default: module.MultiCluster,
  })),
);

const MultiReplication = lazy(() =>
  import("./MultiConsensus").then(module => ({
    default: module.MultiReplication,
  })),
);

const MultiShards = lazy(() =>
  import("./MultiShards").then(module => ({
    default: module.MultiShards,
  })),
);

const MultiDataDiagnostics = lazy(() =>
  import("./MultiDataDiagnostics").then(module => ({
    default: module.MultiDataDiagnostics,
  })),
);

const MultiTransport = lazy(() =>
  import("./MultiTelemetry").then(module => ({
    default: module.MultiTransport,
  })),
);

const MultiMetrics = lazy(() =>
  import("./MultiTelemetry").then(module => ({
    default: module.MultiMetrics,
  })),
);

const MultiEvents = lazy(() =>
  import("./MultiTelemetry").then(module => ({
    default: module.MultiEvents,
  })),
);

const MultiPerformanceLab = lazy(() =>
  import("./MultiEvaluation").then(module => ({
    default: module.MultiPerformanceLab,
  })),
);

const MultiTestLab = lazy(() =>
  import("./MultiEvaluation").then(module => ({
    default: module.MultiTestLab,
  })),
);

function PageLoading() {
  return (
    <article
      className="ma-panel"
      role="status"
      aria-live="polite"
      aria-busy="true"
    >
      <h2>Loading workspace</h2>
      <p>Preparing the requested dashboard page.</p>
    </article>
  );
}

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
  const telemetry = useMultiLiveTelemetry();


  return (
    <div className="app-shell multi-app">
      <MultiSidebar
        page={page}
        onChange={setPage}
        reachableNodes={cluster.reachableNodes}
        loading={cluster.loading}
      />

      <main className="main-content">
        <MultiWorkspaceToolbar
          page={page}
          cluster={cluster}
          telemetry={telemetry}
        />

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

            <MultiOverviewMetrics cluster={cluster} />

            <div className="sr-overview-topology">
              <MultiLiveTopology
                cluster={cluster}
                telemetry={telemetry}
              />
            </div>

            <MultiHeartbeatTelemetry
              cluster={cluster}
              telemetry={telemetry}
            />

            <MultiOverviewShardHealth cluster={cluster} />

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

        <Suspense fallback={<PageLoading />}>
        {page === "architecture" && (
          <MultiArchitecture
            cluster={cluster}
            telemetry={telemetry}
          />
        )}

        {page === "cluster" && (
          <MultiCluster
            cluster={cluster}
            telemetry={telemetry}
          />
        )}

        {page === "replication" && (
          <MultiReplication cluster={cluster}
        telemetry={telemetry}
      />
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
          <MultiMetrics cluster={cluster} telemetry={telemetry} />
        )}

        {page === "events" && (
          <MultiEvents telemetry={telemetry} />
        )}

        </Suspense>

        <div hidden={page !== "faults"}>
          <MultiFaultLab telemetry={telemetry}
            cluster={cluster}
            onOpenConsole={() => setPage("console")}
          />
        </div>

        <Suspense fallback={<PageLoading />}>
        {page === "performance" && (
          <MultiPerformanceLab />
        )}

        {page === "test-lab" && (
          <MultiTestLab />
        )}

        </Suspense>

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

import { useState } from "react";
import type { DataShardID } from "../api/multiDiagnostics";

import { MultiArchitectureNavigation } from "./MultiArchitectureNavigation";
import { MultiArchitectureStorage } from "./MultiArchitectureStorage";
import { MultiArchitectureConsensus } from "./MultiArchitectureConsensus";
import { MultiLiveWorkloadDemo } from "./MultiLiveWorkloadDemo";
import { MultiRequestJourney } from "./MultiRequestJourney";
import { MultiPhysicalDeployment } from "./MultiPhysicalDeployment";
import { MultiArchitectureCanvas } from "./MultiArchitectureCanvas";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import "./multiArchitecture.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

export function MultiArchitecture({
  cluster,
  telemetry,
}: Props) {
  const [demoShard, setDemoShard] =
    useState<DataShardID>(1);

  const [demoRunning, setDemoRunning] =
    useState(false);

  function selectDemoShard(shardId: DataShardID) {
    if (!demoRunning) {
      setDemoShard(shardId);
    }
  }

  const map = cluster.shardMap?.shard_map;

  return (
    <section className="mx-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            SYSTEM ARCHITECTURE
          </div>

          <h1>Multi-Raft Architecture</h1>

          <p>
            Distributed routing, independent consensus
            groups and replicated persistent storage.
          </p>
        </div>
      </header>

      <div className="sr-architecture-page">
        <MultiArchitectureNavigation />

        <div
          id="sr-arch-logical"
          className="sr-architecture-section"
        >
          <MultiArchitectureCanvas
            cluster={cluster}
            telemetry={telemetry}
          />
        </div>

        <div
          id="sr-arch-physical"
          className="sr-architecture-section"
        >
          <MultiPhysicalDeployment
            cluster={cluster}
          />
        </div>

        <div
          id="sr-arch-request"
          className="sr-architecture-section"
        >
          <MultiRequestJourney
            cluster={cluster}
          />
        </div>

        <div
          id="sr-arch-consensus"
          className="sr-architecture-section"
        >
          <div className="mx-live-consensus-stack">
            <MultiLiveWorkloadDemo
              cluster={cluster}
              selectedShard={demoShard}
              onShardChange={selectDemoShard}
              onRunningChange={setDemoRunning}
            />

            <MultiArchitectureConsensus
              cluster={cluster}
              telemetry={telemetry}
              selectedShard={demoShard}
              onShardChange={selectDemoShard}
              selectionLocked={demoRunning}
            />
          </div>
        </div>

        <div
          id="sr-arch-storage"
          className="sr-architecture-section"
        >
          <MultiArchitectureStorage
            cluster={cluster}
          />
        </div>

        <article className="ma-panel mx-discovery">
          <div className="mx-discovery-header">
            <div>
              <div className="eyebrow">
                CONTROL PLANE DIAGNOSTICS
              </div>

              <h2>Metadata discovery</h2>

              <p>
                Current shard map observed from
                the metadata Raft group.
              </p>
            </div>

            <span
              className={
                map
                  ? "mx-discovery-status available"
                  : "mx-discovery-status"
              }
            >
              <i />
              {map ? "Map observed" : "Map unavailable"}
            </span>
          </div>

          {map ? (
            <>
              <div className="mx-discovery-metrics">
                <div>
                  <span>Metadata epoch</span>
                  <strong>{map.epoch}</strong>
                </div>

                <div>
                  <span>Total hash slots</span>
                  <strong>
                    {map.total_slots.toLocaleString()}
                  </strong>
                </div>

                <div>
                  <span>Discovered shards</span>
                  <strong>{map.shards.length}</strong>
                </div>
              </div>

              <details className="mx-discovery-details">
                <summary>
                  Inspect raw metadata response
                </summary>

                <pre className="mx-json">
                  {JSON.stringify(map, null, 2)}
                </pre>
              </details>
            </>
          ) : (
            <p>No metadata map is currently available.</p>
          )}
        </article>
      </div>
    </section>
  );
}

export default MultiArchitecture;

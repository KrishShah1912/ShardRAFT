import {
  useEffect,
  useRef,
  useState,
} from "react";

import {
  runDockerAction,
  type DockerAction,
} from "../api/multiDiagnostics";

import {
  STORAGE_NODE_IDS,
  type StorageNodeID,
} from "../api/multiRaft";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import { MultiFaultTopology } from "./MultiFaultTopology";

import { MultiFaultTimeline } from "./MultiFaultTimeline";

import { MultiFaultComparison } from "./MultiFaultComparison";

import {
  captureClusterSnapshot,
  type ClusterSnapshot,
} from "./faultSnapshot";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import type {
  FaultReachabilityObservation,
} from "./faultTimeline";

import { assessNodeQuorum } from "./quorumProtection";

import "./multiFaultLab.css";

interface Props {
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
  onOpenConsole: () => void;
}

interface ActionRecord {
  id: string;
  node: StorageNodeID;
  action: DockerAction;
  startedAt: string;
  completedAt: string;
  durationMs: number;
  success: boolean;
  output: string;
  before: ClusterSnapshot;
  after: ClusterSnapshot | null;
  completedAtMs: number;
  snapshotDeadline: number;
  snapshotStatus: "pending" | "captured" | "unavailable";
}

interface LeaderChange {
  id: string;
  timestamp: string;
  group: string;
  previous: number | null;
  current: number | null;
}

function leaderLabel(id: number | null) {
  return id ? `Node ${id}` : "Unknown";
}

export function MultiFaultLab({
  cluster, telemetry,
  onOpenConsole,
}: Props) {
  const [busy, setBusy] = useState(false);

  const [reachabilityChanges, setReachabilityChanges] =
    useState<FaultReachabilityObservation[]>([]);

  const previousReachability = useRef(
    new Map<number, boolean>(),
  );

  const lastReachabilityPoll =
    useRef<number | null>(null);

  useEffect(() => {
    if (
      cluster.loading ||
      cluster.lastUpdated === null ||
      cluster.lastUpdated === lastReachabilityPoll.current
    ) {
      return;
    }

    const observedAt = cluster.lastUpdated;

    lastReachabilityPoll.current = observedAt;

    const current = new Map<number, boolean>();

    const transitions: FaultReachabilityObservation[] = [];

    for (const observation of cluster.observations) {
      const nodeId = observation.id;

      current.set(
        nodeId,
        observation.reachable,
      );

      const previous =
        previousReachability.current.get(nodeId);

      if (
        previous !== undefined &&
        previous !== observation.reachable
      ) {
        transitions.push({
          id: crypto.randomUUID(),
          timestamp: new Date(observedAt).toISOString(),
          nodeId,
          reachable: observation.reachable,
        });
      }
    }

    previousReachability.current = current;

    if (transitions.length > 0) {
      setReachabilityChanges(existing =>
        [...transitions, ...existing].slice(0, 200),
      );
    }
  }, [
    cluster.loading,
    cluster.lastUpdated,
    cluster.observations,
  ]);

  const [protectionMessage, setProtectionMessage] =
    useState("");

  const [activeAction, setActiveAction] =
    useState("");

  const [history, setHistory] =
    useState<ActionRecord[]>([]);

  const [selectedId, setSelectedId] =
    useState<string | null>(null);

  const selected = history.find(
    item => item.id === selectedId,
  ) ?? null;

  const snapshotPending = history.some(
    item => item.snapshotStatus === "pending",
  );

  useEffect(() => {
    if (!snapshotPending) return;

    const now = Date.now();

    const snapshot = captureClusterSnapshot(
      cluster,
      now,
    );

    const pollStartedAt =
      cluster.lastPollStartedAt;

    setHistory(items => {
      let changed = false;

      const next = items.map(record => {
        if (record.snapshotStatus !== "pending") {
          return record;
        }

        if (now >= record.snapshotDeadline) {
          changed = true;

          return {
            ...record,
            snapshotStatus: "unavailable" as const,
          };
        }

        if (
          !snapshot.available ||
          snapshot.observedAt === null ||
          pollStartedAt === null ||
          pollStartedAt <= record.completedAtMs ||
          snapshot.observedAt <= record.completedAtMs
        ) {
          return record;
        }

        changed = true;

        return {
          ...record,
          after: snapshot,
          snapshotStatus: "captured" as const,
        };
      });

      return changed ? next : items;
    });
  }, [
    snapshotPending,
    history,
    cluster.loading,
    cluster.lastUpdated,
    cluster.lastPollStartedAt,
    cluster.observations,
    cluster.shards,
    cluster.metadata,
    cluster.metadataLeader,
  ]);

  useEffect(() => {
    if (!snapshotPending) return;

    const timer = window.setInterval(() => {
      const now = Date.now();

      setHistory(items => {
        let changed = false;

        const next = items.map(record => {
          if (
            record.snapshotStatus !== "pending" ||
            now < record.snapshotDeadline
          ) {
            return record;
          }

          changed = true;

          return {
            ...record,
            snapshotStatus: "unavailable" as const,
          };
        });

        return changed ? next : items;
      });
    }, 500);

    return () => window.clearInterval(timer);
  }, [snapshotPending]);

  const [changes, setChanges] =
    useState<LeaderChange[]>([]);

  const executing = useRef(false);

  const sharedActionToken =
    useRef<string | null>(null);

  useEffect(() => {
    if (busy || snapshotPending) return;

    const token = sharedActionToken.current;

    if (token === null) return;

    sharedActionToken.current = null;
    cluster.finishAction(token);

    void cluster.refresh();
  }, [
    busy,
    snapshotPending,
    cluster.finishAction,
    cluster.refresh,
  ]);

  const previousLeaders = useRef<
    Record<string, number | null> | null
  >(null);

  useEffect(() => {
    if (!cluster.lastUpdated) return;

    const current: Record<
      string,
      number | null
    > = {};

    for (const shard of cluster.shards) {
      current[`Shard ${shard.shardId}`] =
        shard.leaderId;
    }

    current["Metadata group"] =
      cluster.metadataLeader;

    const previous = previousLeaders.current;

    previousLeaders.current = current;

    if (!previous) return;

    const detected: LeaderChange[] = [];

    for (const [group, leader] of
      Object.entries(current)) {
      if (previous[group] !== leader) {
        detected.push({
          id: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          group,
          previous: previous[group] ?? null,
          current: leader,
        });
      }
    }

    if (detected.length) {
      setChanges(items =>
        [...detected, ...items].slice(0, 100),
      );
    }
  }, [
    cluster.lastUpdated,
    cluster.shards,
    cluster.metadataLeader,
  ]);

  async function execute(
    node: StorageNodeID,
    action: DockerAction,
  ) {
    if (
      executing.current ||
      cluster.actionLocked ||
      snapshotPending ||
      !cluster.controlAvailable
    ) {
      return;
    }

    setProtectionMessage("");

    const service = cluster.services.find(
      item => item.service === `n${node}`,
    );

    if (!service) {
      setProtectionMessage(
        "Container observations are unavailable.",
      );
      return;
    }

    if (
      action === "start" &&
      service.state !== "exited" &&
      service.state !== "created"
    ) {
      setProtectionMessage(
        "Start requires a stopped container.",
      );
      return;
    }

    if (
      action !== "start" &&
      service.state !== "running"
    ) {
      setProtectionMessage(
        "Stop and Restart require a running container.",
      );
      return;
    }

    if (action !== "start") {
      const assessment = assessNodeQuorum(
        cluster,
        node,
      );

      if (!assessment.allowed) {
        setProtectionMessage(
          `Action blocked: ${assessment.reason}`,
        );
        return;
      }

      const warning = [
        `${action.toUpperCase()} Node ${node}?`,
        "",
        "This changes the local Docker deployment.",
        "",
        "Projected reachable replicas:",
        ...assessment.affected.map(group =>
          `${group.label}: ${group.current}/3 -> ${group.remaining}/3`,
        ),
        "",
        "All configured groups retain an observed majority.",
        "This is an observation-based assessment, not a consensus guarantee.",
      ].join("\\n");

      if (!window.confirm(warning)) return;

      const finalAssessment = assessNodeQuorum(
        cluster,
        node,
      );

      if (!finalAssessment.allowed) {
        setProtectionMessage(
          `Action blocked: ${finalAssessment.reason}`,
        );
        return;
      }
    }

    const token = cluster.acquireAction();

    if (token === null) {
      setProtectionMessage(
        "Another cluster action is active.",
      );
      return;
    }

    sharedActionToken.current = token;

    executing.current = true;
    setBusy(true);
    setActiveAction(`${action} Node ${node}`);

    const before = captureClusterSnapshot(cluster);

    const startedAt = new Date().toISOString();
    const start = performance.now();

    let success = false;
    let output = "";

    try {
      const result = await runDockerAction(
        node,
        action,
      );

      success =
        result.success &&
        result.node_id === node &&
        result.action === action;

      output = result.output;

      if (!success) {
        output =
          "Docker action was not confirmed.\n" +
          output;
      }
    } catch (failure) {
      output =
        "Action outcome not confirmed: " +
        String(failure);
    } finally {
      const completedAtMs = Date.now();

      const record: ActionRecord = {
        id: crypto.randomUUID(),
        node,
        action,
        startedAt,
        completedAt:
          new Date(completedAtMs).toISOString(),
        completedAtMs,
        snapshotDeadline: completedAtMs + 15000,
        snapshotStatus: "pending",
        after: null,
        durationMs: performance.now() - start,
        success,
        output,
        before,
      };

      setHistory(items =>
        [record, ...items].slice(0, 50),
      );

      setSelectedId(record.id);

      try {
        await cluster.refresh();
      } catch (error) {
        setProtectionMessage(
          "Unable to refresh cluster observations: " +
          String(error),
        );
      } finally {
        executing.current = false;
        setBusy(false);
        setActiveAction("");
      }
    }
  }

  const observedLeaders = cluster.shards.filter(
    shard => shard.leaderId !== null,
  ).length;

  const observedMajorities = cluster.shards.filter(
    shard => shard.majorityReachable,
  ).length;

  return (
    <section className="mf-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">
            RESILIENCE AND RECOVERY
          </div>

          <h1>Fault Injection Lab</h1>

          <p>
            Control the local Docker deployment and
            observe independent Raft group recovery.
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
            label: "Observed data leaders",
            value: `${observedLeaders}/4`,
          },
          {
            label: "Reachable data majorities",
            value: `${observedMajorities}/4`,
          },
          {
            label: "Docker control API",
            value: cluster.controlAvailable
              ? "Available"
              : "Unavailable",
          },
          {
            label: "Observed metadata leader",
            value: leaderLabel(
              cluster.metadataLeader,
            ),
          },
          {
            label: "Recorded control actions",
            value: history.length,
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

      {!cluster.controlAvailable && (
        <div className="mf-warning">
          Docker control is unavailable. Node actions
          are disabled until the local control API
          becomes reachable.
        </div>
      )}

      {busy && (
        <div className="mf-notice" role="status">
          Executing {activeAction}...
        </div>
      )}

      {cluster.actionLocked && !busy && !snapshotPending && (
        <div className="mf-notice" role="status">
          Cluster action coordination is active.
          Waiting for fresh cluster and Docker observations.
        </div>
      )}

      {snapshotPending && (
        <div className="mf-notice" role="status">
          Waiting for a fresh post-action observation.
          Node controls remain disabled until the
          snapshot is captured or the wait expires.
        </div>
      )}

      {protectionMessage && (
        <div className="mf-warning" role="alert">
          {protectionMessage}
        </div>
      )}

      <MultiFaultTopology cluster={cluster} />

      <MultiFaultTimeline
        telemetry={telemetry}
        actions={history}
        leadership={changes}
        reachability={reachabilityChanges}
      />

      <article className="ma-panel">
        <div className="mf-heading">
          <h2>Physical node controls</h2>

          <button
            onClick={() => void cluster.refresh()}
          >
            Refresh observations
          </button>
        </div>

        <div className="mf-node-grid">
          {STORAGE_NODE_IDS.map(id => {
            const service = cluster.services.find(
              item => item.service === `n${id}`,
            );

            const observation =
              cluster.observations.find(
                item => item.id === id,
              );

            const hosted = cluster.shards.filter(
              shard => shard.replicas.some(
                replica => replica.nodeId === id,
              ),
            );

            const running =
              service?.state === "running";

            const stopped =
              service?.state === "exited" ||
              service?.state === "created";

            const disabled =
              busy ||
              cluster.actionLocked ||
              snapshotPending ||
              !cluster.controlAvailable ||
              !service;

            const assessment = assessNodeQuorum(
              cluster,
              id,
            );

            const disruptiveDisabled =
              disabled ||
              !running ||
              !assessment.allowed;

            return (
              <div
                className="mf-node"
                key={id}
              >
                <div className="mf-heading">
                  <h3>Node {id}</h3>

                  <span
                    className={
                      observation?.reachable
                        ? "mf-online"
                        : "mf-offline"
                    }
                  >
                    {observation?.reachable
                      ? "Reachable"
                      : "Not observed"}
                  </span>
                </div>

                <div className="mf-fields">
                  <span>Container state</span>

                  <strong>
                    {service?.state ?? "Unknown"}
                  </strong>

                  <span>Container health</span>

                  <strong>
                    {service?.health || "Unknown"}
                  </strong>

                  <span>HTTP endpoint</span>

                  <strong>:{18080 + id}</strong>
                </div>

                <div className="mf-groups">
                  {hosted.map(shard => (
                    <span key={shard.shardId}>
                      S{shard.shardId}
                      {shard.leaderId === id
                        ? " · Leader"
                        : ""}
                    </span>
                  ))}

                  {id <= 3 && (
                    <span>
                      Metadata
                      {cluster.metadataLeader === id
                        ? " · Leader"
                        : ""}
                    </span>
                  )}
                </div>

                <div className="mf-actions">
                  <button
                    disabled={disabled || !stopped}
                    onClick={() =>
                      void execute(id, "start")
                    }
                  >
                    Start
                  </button>

                  <button
                    className="mf-stop"
                    disabled={disruptiveDisabled}
                    title={assessment.reason}
                    onClick={() =>
                      void execute(id, "stop")
                    }
                  >
                    Stop
                  </button>

                  <button
                    disabled={disruptiveDisabled}
                    title={assessment.reason}
                    onClick={() =>
                      void execute(id, "restart")
                    }
                  >
                    Restart
                  </button>
                </div>

                {running && (
                  <div
                    className={
                      "mf-quorum-status " +
                      (assessment.allowed
                        ? "mf-quorum-allowed"
                        : "mf-quorum-blocked")
                    }
                  >
                    <strong>
                      {assessment.allowed
                        ? "Majority retained"
                        : "Quorum protection active"}
                    </strong>

                    <small>{assessment.reason}</small>
                  </div>
                )}
              </div>
            );
          })}
        </div>

        <p>
          Actions affect physical nodes, including all
          Raft groups hosted by those nodes. Persistent
          Docker volumes are not deleted.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Current shard availability</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Group</th>
                <th>Observed leader</th>
                <th>Reachable replicas</th>
                <th>Majority reachable</th>
                <th>Commit spread</th>
              </tr>
            </thead>

            <tbody>
              {cluster.shards.map(shard => (
                <tr key={shard.shardId}>
                  <td>Shard {shard.shardId}</td>

                  <td>
                    {leaderLabel(shard.leaderId)}
                  </td>

                  <td>
                    {shard.reachableReplicas}/3
                  </td>

                  <td
                    className={
                      shard.majorityReachable
                        ? "mf-online"
                        : "mf-offline"
                    }
                  >
                    {cluster.loading
                      ? "Unknown"
                      : shard.majorityReachable
                        ? "Yes"
                        : "No"}
                  </td>

                  <td>
                    {shard.replicationLag ?? "—"}
                  </td>
                </tr>
              ))}

              <tr>
                <td>Metadata group</td>

                <td>
                  {leaderLabel(
                    cluster.metadataLeader,
                  )}
                </td>

                <td>{cluster.metadata.length}/3</td>

                <td>
                  {cluster.loading
                    ? "Unknown"
                    : cluster.metadata.length >= 2
                      ? "Yes"
                      : "No"}
                </td>

                <td>—</td>
              </tr>
            </tbody>
          </table>
        </div>

        <p>
          Reachability is an observation, not proof
          of consensus or successful writes.
        </p>
      </article>

      <article className="ma-panel">
        <div className="mf-heading">
          <h2>Observed leadership transitions</h2>

          <button onClick={() => setChanges([])}>
            Clear timeline
          </button>
        </div>

        {changes.length === 0 ? (
          <div className="mf-empty">
            No leadership changes observed since
            this monitoring session began.
          </div>
        ) : (
          <div className="ma-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Observed at</th>
                  <th>Group</th>
                  <th>Previous leader</th>
                  <th>Current leader</th>
                </tr>
              </thead>

              <tbody>
                {changes.map(change => (
                  <tr key={change.id}>
                    <td>
                      {new Date(change.timestamp)
                        .toLocaleTimeString()}
                    </td>

                    <td>{change.group}</td>

                    <td>
                      {leaderLabel(change.previous)}
                    </td>

                    <td>
                      {leaderLabel(change.current)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <p>
          Transitions are detected through polling.
          Exact election duration is not measured
          by this timeline.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Docker action history</h2>

        {history.length === 0 ? (
          <div className="mf-empty">
            No Docker actions recorded.
          </div>
        ) : (
          <div className="ma-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Started</th>
                  <th>Node</th>
                  <th>Action</th>
                  <th>Result</th>
                  <th>API duration</th>
                  <th>Snapshot</th>
                  <th>Details</th>
                </tr>
              </thead>

              <tbody>
                {history.map(record => (
                  <tr key={record.id}>
                    <td>
                      {new Date(record.startedAt)
                        .toLocaleTimeString()}
                    </td>

                    <td>Node {record.node}</td>

                    <td>{record.action}</td>

                    <td
                      className={
                        record.success
                          ? "mf-online"
                          : "mf-offline"
                      }
                    >
                      {record.success
                        ? "Confirmed"
                        : "Unconfirmed"}
                    </td>

                    <td>
                      {record.durationMs.toFixed(1)} ms
                    </td>

                    <td>
                      {record.snapshotStatus === "captured"
                        ? "Captured"
                        : record.snapshotStatus === "pending"
                          ? "Pending"
                          : "Unavailable"}
                    </td>

                    <td>
                      <button
                        onClick={() =>
                          setSelectedId(record.id)
                        }
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </article>

      {selected && (
        <article className="ma-panel">
          <h2>
            {selected.action.toUpperCase()}
            {" — "}
            Node {selected.node}
          </h2>

          <pre className="mf-output">
            {selected.output || "No command output"}
          </pre>

        </article>
      )}

      <MultiFaultComparison
        before={selected?.before ?? null}
        after={selected?.after ?? null}
        actionLabel={
          selected
            ? `${selected.action.toUpperCase()} Node ${selected.node}`
            : undefined
        }
        pending={
          selected?.snapshotStatus === "pending"
        }
        message={
          selected?.snapshotStatus === "unavailable"
            ? "No eligible post-action observation was captured within 15 seconds."
            : selected && !selected.success
              ? "Docker action outcome was not confirmed. Snapshot differences do not establish that the action succeeded."
              : undefined
        }
      />

      <article className="ma-panel">
        <h2>Demonstration workflow</h2>

        <div className="mf-workflow">
          {[
            "Write a test record in Query Console.",
            "Identify its destination shard and leader.",
            "Stop one storage node hosting that shard.",
            "Observe leadership and replica changes.",
            "Verify the record through Query Console.",
            "Restart the stopped node.",
            "Observe replica catch-up and verify the record.",
          ].map((step, index) => (
            <div key={step}>
              <span>{index + 1}</span>
              <p>{step}</p>
            </div>
          ))}
        </div>

        <button onClick={onOpenConsole}>
          Open Query Console
        </button>

        <p>
          Restore the local cluster after the
          demonstration. A successful Docker action
          alone does not establish data correctness.
        </p>
      </article>
    </section>
  );
}

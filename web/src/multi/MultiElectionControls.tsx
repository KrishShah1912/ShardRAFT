import { useRef, useState } from "react";
import { Play, Square, RefreshCw } from "lucide-react";

import {
  runDockerAction,
  type DockerAction,
  type DataShardID,
} from "../api/multiDiagnostics";

import type {
  StorageNodeID,
} from "../api/multiRaft";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import { assessNodeQuorum } from "./quorumProtection";

import "./multiElectionControls.css";

interface Props {
  cluster: MultiRaftClusterState;
  shardId: DataShardID;
  onElectionTrigger: () => void;
}

export function MultiElectionControls({
  cluster,
  shardId,
  onElectionTrigger,
}: Props) {
  const executing = useRef(false);

  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [failed, setFailed] = useState(false);

  const shard = cluster.shards.find(
    item => item.shardId === shardId,
  );

  function quorumRisk(node: StorageNodeID) {
    const assessment = assessNodeQuorum(
      cluster,
      node,
    );

    return assessment.allowed
      ? []
      : [assessment.reason];
  }

  async function execute(
    node: StorageNodeID,
    action: DockerAction,
  ) {
    if (
      executing.current ||
      cluster.actionLocked ||
      !cluster.controlAvailable
    ) return;

    const service = cluster.services.find(
      item => item.service === `n${node}`,
    );

    if (!service) return;

    if (
      action === "start" &&
      service.state !== "exited" &&
      service.state !== "created"
    ) return;

    if (
      action !== "start" &&
      service.state !== "running"
    ) return;

    if (action !== "start") {
      const risks = quorumRisk(node);

      if (risks.length) {
        setFailed(true);
        setMessage(
          `Action blocked: ${risks.join(", ")}`,
        );
        return;
      }

      const hosted = cluster.shards.filter(
        group => group.replicas.some(
          replica => replica.nodeId === node,
        ),
      ).map(group => `Shard ${group.shardId}`);

      if (node <= 3) hosted.push("Metadata");

      const confirmed = window.confirm(
        `${action.toUpperCase()} Node ${node}?\n\n` +
        `Affected groups: ${hosted.join(", ")}\n\n` +
        "This changes the local Docker cluster.",
      );

      if (!confirmed) return;

      const finalAssessment = assessNodeQuorum(
        cluster,
        node,
      );

      if (!finalAssessment.allowed) {
        setFailed(true);
        setMessage(
          `Action blocked: ${finalAssessment.reason}`,
        );
        return;
      }

    }

    const token = cluster.acquireAction();

    if (token === null) {
      setFailed(true);
      setMessage("Another cluster action is active.");
      return;
    }

    executing.current = true;
    setBusy(true);
    setFailed(false);
    setMessage(`Executing ${action} on Node ${node}...`);

    try {
      if (action !== "start") {
        onElectionTrigger();
      }

      const result = await runDockerAction(node, action);

      if (
        !result.success ||
        result.node_id !== node ||
        result.action !== action
      ) {
        throw new Error("Docker action not confirmed");
      }

      setMessage(
        action === "stop"
          ? `Node ${node} stop confirmed. Watching for election events.`
          : action === "start"
            ? `Node ${node} start confirmed. Watching for recovery.`
            : `Node ${node} restart confirmed. Watching cluster state.`,
      );
    } catch (error) {
      setFailed(true);
      setMessage(
        `Action outcome not confirmed: ${String(error)}`,
      );
    } finally {
      cluster.finishAction(token);

      try {
        await cluster.refresh();
      } catch (error) {
        setFailed(true);
        setMessage(
          "Unable to refresh observations: " +
          String(error),
        );
      } finally {
        executing.current = false;
        setBusy(false);
      }
    }
  }

  if (!shard) return null;

  const leader = shard.replicas.find(
    replica => replica.nodeId === shard.leaderId,
  );

  const leaderService = cluster.services.find(
    item => item.service === `n${shard.leaderId}`,
  );

  return (
    <section className="mec-panel">
      <div className="mec-header">
        <div>
          <h3>Interactive election controls</h3>
          <p>
            Control physical replicas and observe
            real Raft election events.
          </p>
        </div>

        <button
          type="button"
          className="mec-stop"
          disabled={
            busy ||
            cluster.actionLocked ||
            !cluster.controlAvailable ||
            !leader ||
            !leader.reachable ||
            leaderService?.state !== "running" ||
            quorumRisk(leader.nodeId).length > 0
          }
          onClick={() => {
            if (leader) {
              void execute(leader.nodeId, "stop");
            }
          }}
        >
          <Square size={13} />
          Stop current leader
        </button>
      </div>

      <div className="mec-grid">
        {shard.replicas.map(replica => {
          const id = replica.nodeId;

          const service = cluster.services.find(
            item => item.service === `n${id}`,
          );

          const running = service?.state === "running";

          const stopped =
            service?.state === "exited" ||
            service?.state === "created";

          const isLeader = shard.leaderId === id;

          const risks = quorumRisk(id);

          const disabled =
            busy ||
            cluster.actionLocked ||
            !cluster.controlAvailable ||
            !service;

          const state = stopped
            ? "Stopped"
            : !replica.reachable
              ? running
                ? "Awaiting node response"
                : "Unreachable"
              : isLeader
                ? "Leader"
                : replica.role;

          return (
            <div
              key={id}
              className={
                "mec-node " +
                (stopped
                  ? "mec-offline"
                  : isLeader && replica.reachable
                    ? "mec-leader"
                    : "")
              }
            >
              <div className="mec-node-header">
                <strong>Node {id}</strong>

                <span className={
                  replica.reachable
                    ? "mec-online-text"
                    : "mec-offline-text"
                }>
                  {state}
                </span>
              </div>

              <div className="mec-detail">
                Container: {service?.state ?? "Unknown"}
              </div>

              <div className="mec-detail">
                Term: {replica.term ?? "—"}
              </div>

              <div className="mec-actions">
                {stopped ? (
                  <button
                    type="button"
                    className="mec-start"
                    disabled={disabled}
                    onClick={() => void execute(id, "start")}
                  >
                    <Play size={13} />
                    Start
                  </button>
                ) : (
                  <>
                    <button
                      type="button"
                      disabled={
                        disabled ||
                        !running ||
                        risks.length > 0
                      }
                      onClick={() =>
                        void execute(id, "restart")
                      }
                    >
                      <RefreshCw size={13} />
                      Restart
                    </button>

                    <button
                      type="button"
                      className="mec-stop"
                      disabled={
                        disabled ||
                        !running ||
                        risks.length > 0
                      }
                      onClick={() =>
                        void execute(id, "stop")
                      }
                    >
                      <Square size={13} />
                      {isLeader ? "Stop leader" : "Stop"}
                    </button>
                  </>
                )}
              </div>

              {running && risks.length > 0 && (
                <small className="mec-risk">
                  Quorum protection: {risks.join(", ")}
                </small>
              )}
            </div>
          );
        })}
      </div>

      {cluster.actionLocked && !busy && (
        <div className="mec-message" role="status">
          Another action is active or fresh cluster
          observations are pending.
        </div>
      )}

      {!cluster.controlAvailable && (
        <div className="mec-message mec-error">
          Docker control unavailable.
        </div>
      )}

      {message && (
        <div
          role="status"
          className={
            "mec-message " +
            (failed ? "mec-error" : "")
          }
        >
          {message}
        </div>
      )}

      <p className="mec-note">
        Node actions affect all hosted Raft groups.
        Election and recovery observations are live,
        not simulated. Restore stopped nodes after
        the demonstration.
      </p>
    </section>
  );
}

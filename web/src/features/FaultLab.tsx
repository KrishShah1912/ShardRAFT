import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Crown,
  Play,
  Power,
  Radio,
  RefreshCw,
  RotateCcw,
  Server,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";

import {
  useEffect,
  useMemo,
  useState,
} from "react";

import type {
  ControlledNode,
} from "../api/control";

import {
  currentLeader,
  eventToTimeline,
  experimentDuration,
  failoverDuration,
  faultRelevantEvent,
  hasQuorum,
  reachableCount,
  startFaultExperiment,
  type FaultExperiment,
  type FaultTimelineEvent,
} from "../telemetry/faults";

import type {
  NodeStatus,
  RaftEvent,
  TransportStats,
} from "../types";

interface Props {
  nodes: NodeStatus[];
  events: RaftEvent[];
  transport?: TransportStats;
  controlledNodes: ControlledNode[];
  connected: boolean;

  onStart: (
    id: number,
  ) => Promise<void>;

  onStop: (
    id: number,
  ) => Promise<void>;

  onRestart: (
    id: number,
  ) => Promise<void>;
}

export function FaultLab({
  nodes,
  events,
  transport,
  controlledNodes,
  connected,
  onStart,
  onStop,
  onRestart,
}: Props) {
  const [experiment, setExperiment] =
    useState<FaultExperiment>();

  const [timeline, setTimeline] =
    useState<FaultTimelineEvent[]>(
      [],
    );

  const [busyNode, setBusyNode] =
    useState<number>();

  const [error, setError] =
    useState<string>();

  const leader = currentLeader(nodes);

  const quorum = hasQuorum(nodes);

  const reachable =
    reachableCount(nodes);

  const relevantEvents = useMemo(
    () =>
      events
        .filter(faultRelevantEvent)
        .sort(
          (a, b) =>
            new Date(a.timestamp).getTime() -
            new Date(b.timestamp).getTime(),
        ),
    [events],
  );

  useEffect(() => {
    if (!experiment) {
      return;
    }

    const experimentEvents =
      relevantEvents.filter(
        (event) =>
          new Date(
            event.timestamp,
          ).getTime() >=
          experiment.startedAt,
      );

    const converted =
      experimentEvents.map(
        eventToTimeline,
      );

    setTimeline((current) => {
      const manual = current.filter(
        (item) =>
          !item.id.startsWith(
            "event-",
          ),
      );

      return [
        ...manual,
        ...converted,
      ].sort(
        (a, b) =>
          a.timestamp -
          b.timestamp,
      );
    });
  }, [
    relevantEvents,
    experiment,
  ]);

  useEffect(() => {
    if (!experiment) {
      return;
    }

    const target = nodes.find(
      (node) =>
        node.id ===
        experiment.targetNodeId,
    );

    const current =
      currentLeader(nodes);

    setExperiment((existing) => {
      if (!existing) {
        return existing;
      }

      let next = existing;

      if (
        target?.reachable === false &&
        !next.leaderLostAt &&
        existing.previousLeaderId ===
          existing.targetNodeId
      ) {
        next = {
          ...next,
          leaderLostAt: Date.now(),
        };
      }

      if (
        current &&
        current.id !==
          existing.previousLeaderId &&
        !next.newLeaderAt
      ) {
        next = {
          ...next,
          newLeaderId: current.id,
          newLeaderAt: Date.now(),
          endingTerm: current.term,
          endingCommit:
            current.commit_index,
          endingRetries:
            transport?.retries ?? 0,
          endingFailures:
            transport?.failures ??
            0,
          status: "recovering",
        };
      }

      if (
        target?.reachable !== false &&
        next.newLeaderAt &&
        !next.recoveredAt
      ) {
        next = {
          ...next,
          recoveredAt: Date.now(),
          status: "completed",
        };
      }

      return next;
    });
  }, [
    nodes,
    transport,
    experiment?.targetNodeId,
    experiment?.previousLeaderId,
  ]);

  function controlFor(
    id: number,
  ) {
    return controlledNodes.find(
      (node) => node.id === id,
    );
  }

  function addTimeline(
    event: Omit<
      FaultTimelineEvent,
      "id" | "timestamp"
    >,
  ) {
    setTimeline((current) => [
      ...current,
      {
        ...event,
        id: `manual-${Date.now()}-${Math.random()}`,
        timestamp: Date.now(),
      },
    ]);
  }

  async function runNodeAction(
    id: number,
    action:
      | "start"
      | "stop"
      | "restart",
  ) {
    setBusyNode(id);
    setError(undefined);

    try {
      if (action === "start") {
        addTimeline({
          type: "recovery",
          title: `Starting Node ${id}`,
          detail:
            "Supervisor received a node start request.",
          nodeId: id,
        });

        await onStart(id);
      }

      if (action === "stop") {
        addTimeline({
          type: "node",
          title: `Stopping Node ${id}`,
          detail:
            "Supervisor received a node stop request.",
          nodeId: id,
        });

        await onStop(id);
      }

      if (action === "restart") {
        addTimeline({
          type: "recovery",
          title: `Restarting Node ${id}`,
          detail:
            "Supervisor is cycling the node process.",
          nodeId: id,
        });

        await onRestart(id);
      }
    } catch (caught) {
      const message =
        caught instanceof Error
          ? caught.message
          : String(caught);

      setError(message);

      addTimeline({
        type: "experiment",
        title: "Control action failed",
        detail: message,
        nodeId: id,
      });
    } finally {
      setBusyNode(undefined);
    }
  }

  async function killLeader() {
    if (!leader) {
      setError(
        "No current leader is available.",
      );
      return;
    }

    if (reachable < 3) {
      setError(
        "Leader failover experiment requires all three nodes online first.",
      );
      return;
    }

    const next =
      startFaultExperiment(
        "Leader failover",
        leader.id,
        nodes,
        transport,
      );

    setExperiment(next);
    setTimeline([]);
    setError(undefined);

    addTimeline({
      type: "experiment",
      title:
        "Leader failover experiment started",
      detail: `Node ${leader.id} will be stopped. The remaining nodes should preserve quorum and elect a replacement.`,
      nodeId: leader.id,
    });

    await runNodeAction(
      leader.id,
      "stop",
    );
  }

  async function recoverTarget() {
    if (!experiment) {
      return;
    }

    await runNodeAction(
      experiment.targetNodeId,
      "start",
    );
  }

  function resetExperiment() {
    setExperiment(undefined);
    setTimeline([]);
    setError(undefined);
  }

  const failoverMs =
    failoverDuration(experiment);

  const totalMs =
    experimentDuration(experiment);

  const retryIncrease =
    experiment?.endingRetries ===
      undefined
      ? undefined
      : Math.max(
          0,
          experiment.endingRetries -
            experiment.startingRetries,
        );

  const failureIncrease =
    experiment?.endingFailures ===
      undefined
      ? undefined
      : Math.max(
          0,
          experiment.endingFailures -
            experiment.startingFailures,
        );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            RESILIENCE
          </div>

          <h1>Fault Lab</h1>

          <p>
            Run controlled node failures and
            observe Raft election, availability,
            transport and recovery in real time.
          </p>
        </div>

        <div
          className={
            connected
              ? "live-indicator connected"
              : "live-indicator"
          }
        >
          <Radio size={14} />

          {connected
            ? "CONTROL READY"
            : "RECONNECTING"}
        </div>
      </header>

      <section className="fault-safety-banner">
        <ShieldCheck size={19} />

        <div>
          <strong>
            Development cluster controls
          </strong>

          <span>
            Actions are restricted to the
            local ShardRAFT supervisor. No
            arbitrary shell execution is
            exposed.
          </span>
        </div>
      </section>

      <section className="metrics-grid">
        <Metric
          icon={Server}
          label="REACHABLE"
          value={`${reachable} / 3`}
          detail={
            reachable === 3
              ? "All nodes online"
              : "Degraded cluster"
          }
        />

        <Metric
          icon={Crown}
          label="LEADER"
          value={
            leader
              ? `Node ${leader.id}`
              : "Election"
          }
          detail={
            leader
              ? `Term ${leader.term}`
              : "No leader observed"
          }
        />

        <Metric
          icon={ShieldCheck}
          label="QUORUM"
          value={
            quorum
              ? "AVAILABLE"
              : "LOST"
          }
          detail={`${reachable} voting nodes reachable`}
        />

        <Metric
          icon={Activity}
          label="COMMIT"
          value={String(
            leader?.commit_index ?? "—",
          )}
          detail={`Applied ${
            leader?.applied_index ?? "—"
          }`}
        />
      </section>

      <section className="fault-layout">
        <div className="fault-control-column">
          <section className="panel fault-experiment-card">
            <div className="panel-heading">
              <div>
                <h2>
                  Preset experiment
                </h2>

                <p>
                  Demonstrate automatic Raft
                  leader failover.
                </p>
              </div>

              <TriangleAlert
                size={17}
              />
            </div>

            <div className="fault-preset">
              <div className="fault-preset-icon">
                <Crown size={20} />
              </div>

              <div className="fault-preset-copy">
                <strong>
                  Kill Current Leader
                </strong>

                <p>
                  Stop the elected leader,
                  preserve 2/3 quorum, observe
                  the election, then recover
                  the failed node.
                </p>
              </div>

              <button
                className="fault-primary-button"
                disabled={
                  !leader ||
                  reachable < 3 ||
                  experiment?.status ===
                    "running"
                }
                onClick={() =>
                  void killLeader()
                }
              >
                <Play size={12} />
                Run experiment
              </button>
            </div>

            <div className="fault-expectations">
              <Expectation
                label="Expected availability"
                value="Maintained"
              />

              <Expectation
                label="Required quorum"
                value="2 / 3"
              />

              <Expectation
                label="Expected action"
                value="New election"
              />
            </div>
          </section>

          <section className="panel">
            <div className="panel-heading">
              <div>
                <h2>
                  Manual node controls
                </h2>

                <p>
                  Start, stop or restart any
                  local node.
                </p>
              </div>
            </div>

            <div className="fault-node-list">
              {[1, 2, 3].map(
                (id) => {
                  const node =
                    nodes.find(
                      (candidate) =>
                        candidate.id ===
                        id,
                    );

                  const control =
                    controlFor(id);

                  const online =
                    node?.reachable !==
                      false &&
                    control?.reachable !==
                      false;

                  const isLeader =
                    node?.role === 2 &&
                    node?.reachable !==
                      false;

                  return (
                    <div
                      className="fault-node-row"
                      key={id}
                    >
                      <div className="fault-node-identity">
                        <div
                          className={[
                            "fault-node-status",
                            online
                              ? "online"
                              : "offline",
                          ].join(" ")}
                        />

                        <div>
                          <strong>
                            Node {id}
                          </strong>

                          <span>
                            {!online
                              ? "Offline"
                              : isLeader
                                ? "Leader"
                                : node?.role ===
                                    1
                                  ? "Candidate"
                                  : "Follower"}
                          </span>
                        </div>
                      </div>

                      <div className="fault-node-meta">
                        <span>
                          TERM{" "}
                          {online
                            ? node?.term
                            : "—"}
                        </span>

                        <span>
                          PID{" "}
                          {control?.pid ??
                            "—"}
                        </span>
                      </div>

                      <div className="fault-node-actions">
                        {!online ? (
                          <button
                            className="fault-node-action start"
                            disabled={
                              busyNode === id
                            }
                            onClick={() =>
                              void runNodeAction(
                                id,
                                "start",
                              )
                            }
                          >
                            <Power
                              size={11}
                            />
                            Start
                          </button>
                        ) : (
                          <>
                            <button
                              className="fault-node-action"
                              disabled={
                                busyNode ===
                                id
                              }
                              onClick={() =>
                                void runNodeAction(
                                  id,
                                  "restart",
                                )
                              }
                            >
                              <RefreshCw
                                size={11}
                              />
                              Restart
                            </button>

                            <button
                              className={
                                isLeader
                                  ? "fault-node-action stop leader"
                                  : "fault-node-action stop"
                              }
                              disabled={
                                busyNode ===
                                id
                              }
                              onClick={() =>
                                void runNodeAction(
                                  id,
                                  "stop",
                                )
                              }
                            >
                              <Power
                                size={11}
                              />
                              Stop
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  );
                },
              )}
            </div>
          </section>
        </div>

        <div className="fault-result-column">
          <section className="panel fault-result-card">
            <div className="panel-heading">
              <div>
                <h2>
                  Experiment status
                </h2>

                <p>
                  Automatically measured from
                  live cluster telemetry.
                </p>
              </div>

              {experiment && (
                <span
                  className={`fault-status ${experiment.status}`}
                >
                  {experiment.status}
                </span>
              )}
            </div>

            {!experiment ? (
              <div className="fault-empty-result">
                <Activity size={24} />

                <strong>
                  No active experiment
                </strong>

                <span>
                  Run the leader failover
                  preset or manually control
                  nodes to begin.
                </span>
              </div>
            ) : (
              <>
                <div className="fault-result-summary">
                  <ResultMetric
                    label="TARGET"
                    value={`Node ${experiment.targetNodeId}`}
                  />

                  <ResultMetric
                    label="OLD LEADER"
                    value={
                      experiment.previousLeaderId
                        ? `Node ${experiment.previousLeaderId}`
                        : "—"
                    }
                  />

                  <ResultMetric
                    label="NEW LEADER"
                    value={
                      experiment.newLeaderId
                        ? `Node ${experiment.newLeaderId}`
                        : "Waiting"
                    }
                  />

                  <ResultMetric
                    label="FAILOVER"
                    value={
                      failoverMs ===
                      undefined
                        ? "—"
                        : `${failoverMs} ms`
                    }
                  />

                  <ResultMetric
                    label="TERM"
                    value={`${experiment.startingTerm} → ${
                      experiment.endingTerm ??
                      "…"
                    }`}
                  />

                  <ResultMetric
                    label="RETRIES"
                    value={
                      retryIncrease ===
                      undefined
                        ? "—"
                        : `+${retryIncrease}`
                    }
                  />

                  <ResultMetric
                    label="FAILURES"
                    value={
                      failureIncrease ===
                      undefined
                        ? "—"
                        : `+${failureIncrease}`
                    }
                  />

                  <ResultMetric
                    label="TOTAL TIME"
                    value={
                      totalMs ===
                      undefined
                        ? "—"
                        : `${totalMs} ms`
                    }
                  />
                </div>

                <div className="fault-progress">
                  <FaultStage
                    title="Leader stopped"
                    active={
                      experiment.leaderLostAt !==
                      undefined
                    }
                  />

                  <FaultStage
                    title="Election"
                    active={
                      experiment.newLeaderAt !==
                      undefined
                    }
                  />

                  <FaultStage
                    title="New leader"
                    active={
                      experiment.newLeaderId !==
                      undefined
                    }
                  />

                  <FaultStage
                    title="Recovered"
                    active={
                      experiment.recoveredAt !==
                      undefined
                    }
                  />
                </div>

                <div className="fault-result-actions">
                  {experiment.newLeaderAt &&
                    !experiment.recoveredAt && (
                      <button
                        className="fault-primary-button"
                        disabled={
                          busyNode ===
                          experiment.targetNodeId
                        }
                        onClick={() =>
                          void recoverTarget()
                        }
                      >
                        <RotateCcw
                          size={12}
                        />
                        Recover Node{" "}
                        {
                          experiment.targetNodeId
                        }
                      </button>
                    )}

                  <button
                    className="fault-secondary-button"
                    onClick={
                      resetExperiment
                    }
                  >
                    Reset
                  </button>
                </div>
              </>
            )}
          </section>

          {error && (
            <div className="fault-error-banner">
              <AlertTriangle
                size={15}
              />
              {error}
            </div>
          )}
        </div>
      </section>

      <section className="panel fault-timeline-panel">
        <div className="panel-heading">
          <div>
            <h2>
              Experiment timeline
            </h2>

            <p>
              Node control, election,
              transport and recovery events.
            </p>
          </div>

          <Clock3 size={17} />
        </div>

        {timeline.length === 0 ? (
          <div className="empty-state">
            Timeline events will appear
            during an experiment.
          </div>
        ) : (
          <div className="fault-timeline">
            {timeline.map(
              (item, index) => (
                <TimelineRow
                  key={item.id}
                  item={item}
                  origin={
                    experiment?.startedAt ??
                    timeline[0].timestamp
                  }
                  last={
                    index ===
                    timeline.length - 1
                  }
                />
              ),
            )}
          </div>
        )}
      </section>
    </>
  );
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="metric-card">
      <div className="metric-card-top">
        <span>{label}</span>
        <Icon size={17} />
      </div>

      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function Expectation({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ResultMetric({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="fault-result-metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function FaultStage({
  title,
  active,
}: {
  title: string;
  active: boolean;
}) {
  return (
    <div
      className={
        active
          ? "fault-stage complete"
          : "fault-stage"
      }
    >
      <div className="fault-stage-marker">
        {active ? (
          <CheckCircle2 size={13} />
        ) : (
          <span />
        )}
      </div>

      <strong>{title}</strong>
    </div>
  );
}

function TimelineRow({
  item,
  origin,
  last,
}: {
  item: FaultTimelineEvent;
  origin: number;
  last: boolean;
}) {
  return (
    <div className="fault-timeline-row">
      <div className="fault-timeline-time">
        +
        {Math.max(
          0,
          item.timestamp - origin,
        )}
        ms
      </div>

      <div className="fault-timeline-rail">
        <div
          className={`fault-timeline-marker ${item.type}`}
        />

        {!last && (
          <div className="fault-timeline-line" />
        )}
      </div>

      <div className="fault-timeline-content">
        <strong>
          {item.title}
        </strong>

        <span>
          {item.detail}
        </span>
      </div>
    </div>
  );
}

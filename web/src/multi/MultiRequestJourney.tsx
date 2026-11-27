import { useEffect, useState } from "react";
import {
  ArrowDown,
  ArrowRight,
  Database,
  Fingerprint,
  Network,
  Play,
  Radio,
  Server,
  Workflow,
} from "lucide-react";

import type { MultiRaftClusterState } from "./useMultiRaftCluster";
import type { KVOperation } from "../api/routing";

import {
  crc32,
  keySlot,
  previewKeyRouting,
} from "./keyRouting";

import "./multiRequestJourney.css";

interface Props {
  cluster: MultiRaftClusterState;
}

const COLORS = [
  "#60a5fa",
  "#2dd4bf",
  "#c084fc",
  "#fbbf24",
];

const OPERATIONS: KVOperation[] = [
  "GET", "PUT", "DELETE", "CAS",
];

export function MultiRequestJourney({ cluster }: Props) {
  const [key, setKey] = useState("demo:user");
  const [operation, setOperation] =
    useState<KVOperation>("PUT");
  const [gateway, setGateway] = useState(1);
  const [animate, setAnimate] = useState(true);
  const [step, setStep] = useState(-1);
  const [run, setRun] = useState(0);
  const [running, setRunning] = useState(false);

  const map = cluster.shardMap?.shard_map;

  const routing = map && key.trim()
    ? previewKeyRouting(key, map)
    : null;

  const shard = cluster.shards.find(
    item => item.shardId === routing?.shardId,
  );

  const leader = shard?.replicas.find(
    item => item.nodeId === shard.leaderId,
  );

  const color = routing?.shardId
    ? COLORS[routing.shardId - 1] ?? "#60a5fa"
    : "#64748b";

  useEffect(() => {
    if (!running) return;

    if (step >= 4) {
      setRunning(false);
      return;
    }

    const timer = window.setTimeout(
      () => setStep(previous => previous + 1),
      animate ? 850 : 0,
    );

    return () => window.clearTimeout(timer);
  }, [running, step, animate, run]);

  function reset() {
    setRunning(false);
    setStep(-1);
  }

  function start() {
    if (!routing?.shardId || running) return;
    setRun(previous => previous + 1);
    setStep(0);
    setRunning(true);
  }

  const stages = [
    {
      icon: Radio,
      label: "Client request",
      value: operation,
      detail: key || "Enter a key",
    },
    {
      icon: Network,
      label: "HTTP gateway",
      value: `Gateway ${gateway}`,
      detail: `Port ${18090 + gateway}`,
    },
    {
      icon: Fingerprint,
      label: "CRC32 hashing",
      value: key.trim()
        ? crc32(key).toLocaleString()
        : "Unknown",
      detail: "IEEE CRC32 · UTF-8",
    },
    {
      icon: Workflow,
      label: "Slot routing",
      value: key.trim()
        ? keySlot(key).toLocaleString()
        : "Unknown",
      detail: `Metadata epoch ${routing?.epoch ?? "—"}`,
    },
    {
      icon: Database,
      label: "Destination",
      value: routing?.shardId
        ? `Shard ${routing.shardId}`
        : "Unknown",
      detail: leader
        ? `Observed leader N${leader.nodeId}`
        : "No observed leader",
    },
  ];

  return (
    <article className="ma-panel mrj-panel">
      <div className="mrj-header">
        <div>
          <div className="mrj-eyebrow">
            INTERACTIVE ROUTING VISUALIZER
          </div>
          <h2>Request Journey</h2>
          <p>
            Explore how a database key maps to an
            independent Raft consensus group.
          </p>
        </div>

        <label className="mrj-toggle">
          <input
            type="checkbox"
            checked={animate}
            onChange={event =>
              setAnimate(event.target.checked)
            }
          />
          Enable motion
        </label>
      </div>

      <div className="mrj-mode">
        <Fingerprint size={15} />
        Routing preview — no database requests dispatched
      </div>

      <div className="mrj-builder">
        <label>
          Database key
          <input
            value={key}
            disabled={running}
            onChange={event => {
              setKey(event.target.value);
              reset();
            }}
          />
        </label>

        <label>
          Operation
          <select
            value={operation}
            disabled={running}
            onChange={event => {
              setOperation(
                event.target.value as KVOperation,
              );
              reset();
            }}
          >
            {OPERATIONS.map(item => (
              <option key={item}>{item}</option>
            ))}
          </select>
        </label>

        <label>
          Gateway
          <select
            value={gateway}
            disabled={running}
            onChange={event => {
              setGateway(Number(event.target.value));
              reset();
            }}
          >
            <option value={1}>Gateway 1</option>
            <option value={2}>Gateway 2</option>
          </select>
        </label>

        <button
          type="button"
          className="mrj-run"
          disabled={running || !routing?.shardId}
          onClick={start}
        >
          <Play size={16} />
          {running ? "Visualizing..." : "Visualize route"}
        </button>
      </div>

      <div className={
        `mrj-canvas ${animate ? "" : "mrj-static"}`
      }>
        <div className="mrj-pipeline">
          {stages.map((stage, index) => (
            <div
              key={stage.label}
              className="mrj-stage-column"
            >
              <div
                className={[
                  "mrj-stage",
                  step === index ? "mrj-current" : "",
                  step > index ? "mrj-completed" : "",
                ].join(" ")}
              >
                <div className="mrj-stage-top">
                  <stage.icon size={22} />
                  <small>0{index + 1}</small>
                </div>
                <span>{stage.label}</span>
                <strong>{stage.value}</strong>
                <small>{stage.detail}</small>
              </div>

              {index < stages.length - 1 && (
                <div className={[
                  "mrj-connector",
                  step > index ? "mrj-passed" : "",
                ].join(" ")}>
                  <ArrowRight size={18} />
                  {animate && step === index && running && (
                    <i key={`${run}-${index}`} />
                  )}
                </div>
              )}
            </div>
          ))}
        </div>

        <div className="mrj-slot-heading">
          <span>Configured hash space</span>
          <strong>65,536 slots</strong>
        </div>

        <div className="mrj-slotbar">
          {[1, 2, 3, 4].map(id => (
            <div
              key={id}
              className={
                routing?.shardId === id
                  ? "mrj-slot mrj-selected-slot"
                  : "mrj-slot"
              }
              style={{
                borderColor: COLORS[id - 1],
                color: COLORS[id - 1],
                background: `${COLORS[id - 1]}12`,
              }}
            >
              <strong>Shard {id}</strong>
              <small>
                {((id - 1) * 16384).toLocaleString()}
                {" – "}
                {(id * 16384 - 1).toLocaleString()}
              </small>
            </div>
          ))}

          {key.trim() && (
            <div
              className="mrj-slot-marker"
              style={{
                left: `${keySlot(key) / 65536 * 100}%`,
                color,
              }}
              tabIndex={0}
              aria-label={`Hash slot ${keySlot(key)}, ${
                routing?.shardId
                  ? `Shard ${routing.shardId}`
                  : "Destination unknown"
              }`}
            >
              <div className="mrj-marker-line" />
              <div className="mrj-marker-dot" />

              <div
                role="tooltip"
                className={[
                  "mrj-slot-tooltip",
                  keySlot(key) < 12000
                    ? "mrj-tooltip-left"
                    : keySlot(key) > 53536
                      ? "mrj-tooltip-right"
                      : "",
                ].join(" ")}
              >
                <div className="mrj-tooltip-heading">
                  <Fingerprint size={14} />
                  HASH SLOT DETAILS
                </div>

                <div className="mrj-tooltip-value">
                  {keySlot(key).toLocaleString()}
                </div>

                <div className="mrj-tooltip-divider" />

                <div className="mrj-tooltip-row">
                  <span>Database key</span>
                  <strong>{key}</strong>
                </div>

                <div className="mrj-tooltip-row">
                  <span>CRC32 hash</span>
                  <strong>
                    {crc32(key).toLocaleString()}
                  </strong>
                </div>

                <div className="mrj-tooltip-row">
                  <span>Destination</span>
                  <strong style={{ color }}>
                    {routing?.shardId
                      ? `Shard ${routing.shardId}`
                      : "Unknown"}
                  </strong>
                </div>

                <div className="mrj-tooltip-row">
                  <span>Configured slot range</span>
                  <strong>
                    {routing?.shardId
                      ? `${((routing.shardId - 1) * 16384)
                          .toLocaleString()} – ${
                          (routing.shardId * 16384 - 1)
                            .toLocaleString()}`
                      : "Unknown"}
                  </strong>
                </div>

                <div className="mrj-tooltip-row">
                  <span>Metadata epoch</span>
                  <strong>
                    {routing?.epoch ?? "Unknown"}
                  </strong>
                </div>
              </div>
            </div>
          )}
        </div>

        <div className="mrj-down">
          <ArrowDown size={23} color={color} />
        </div>

        {shard ? (
          <div
            className="mrj-consensus"
            style={{ borderColor: color }}
          >
            <div className="mrj-group-heading">
              <strong style={{ color }}>
                Shard {shard.shardId} Consensus Group
              </strong>
              <span>{shard.state}</span>
            </div>

            <div className="mrj-leader">
              <Server size={25} color={color} />
              <strong>
                {leader
                  ? `Node ${leader.nodeId}`
                  : "Leader unknown"}
              </strong>
              <small>Observed leader</small>
              <span>Term {leader?.term ?? "—"}</span>
            </div>

            <div className="mrj-down">
              <ArrowDown size={20} color={color} />
            </div>

            <div className="mrj-replicas">
              {shard.replicas.map(replica => (
                <div
                  key={replica.nodeId}
                  className={[
                    "mrj-replica",
                    replica.reachable
                      ? "mrj-online"
                      : "mrj-offline",
                  ].join(" ")}
                >
                  <div className="mrj-replica-title">
                    <Server size={18} />
                    <strong>Node {replica.nodeId}</strong>
                    <i />
                  </div>

                  <small>
                    {replica.reachable
                      ? replica.role
                      : "Not observed"}
                  </small>

                  <div className="mrj-counters">
                    <div>
                      <small>Term</small>
                      <b>{replica.term ?? "—"}</b>
                    </div>
                    <div>
                      <small>Commit</small>
                      <b>{replica.commitIndex ?? "—"}</b>
                    </div>
                    <div>
                      <small>Applied</small>
                      <b>{replica.appliedIndex ?? "—"}</b>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            <div className="mrj-quorum">
              <span>
                Reachable replicas: {shard.reachableReplicas}/3
              </span>
              <strong>
                {shard.majorityReachable
                  ? "Reachable majority"
                  : "Majority not observed"}
              </strong>
            </div>
          </div>
        ) : (
          <div className="mrj-empty">
            No destination available from the current
            metadata map.
          </div>
        )}

        <div className="mrj-result" aria-live="polite">
          {running
            ? `Preview stage ${step + 1} of 5`
            : step === 4
              ? "Routing preview complete — no request sent"
              : "Select a key and visualize its route"}
        </div>
      </div>

      <p className="mrj-note">
        Hashing and destination selection use the current
        metadata map. Replica counters are live observations.
        Animated stages illustrate predicted routing, not
        measured request latency or confirmed replication.
        Reachability alone does not prove quorum commitment.
      </p>
    </article>
  );
}

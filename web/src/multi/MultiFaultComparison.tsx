import { useMemo, useState } from "react";

import {
  GitCompare,
  ArrowRight,
  ShieldCheck,
  AlertTriangle,
  Clock,
  Database,
  Crown,
  Server,
} from "lucide-react";

import {
  compareClusterSnapshots,
  type ClusterSnapshot,
  type GroupSnapshot,
  type SnapshotGroupID,
  type CatchUpObservation,
  type QuorumTransition,
} from "./faultSnapshot";

import "./multiFaultComparison.css";

interface Props {
  before: ClusterSnapshot | null;
  after: ClusterSnapshot | null;
  actionLabel?: string;
  pending?: boolean;
  message?: string;
}

const QUORUM_LABELS: Record<QuorumTransition, string> = {
  retained: "Majority retained",
  lost: "Majority lost",
  restored: "Majority restored",
  unavailable: "Majority unavailable",
  unknown: "Unknown",
};

const CATCH_UP_LABELS: Record<CatchUpObservation, string> = {
  "at-observed-leader-index": "Indexes aligned",
  "behind-observed-leader": "Behind observed leader",
  "ahead-of-observed-leader": "Ahead of observed leader",
  unavailable: "Unavailable",
  unknown: "Unknown",
};

function numberLabel(value: number | null): string {
  return value === null
    ? "Unknown"
    : value.toLocaleString();
}

function leaderLabel(value: number | null): string {
  return value === null
    ? "Unknown"
    : `Node ${value}`;
}

function deltaLabel(value: number | null): string {
  if (value === null) return "—";
  if (value > 0) return `+${value.toLocaleString()}`;
  return value.toLocaleString();
}

function reachableLabel(value: boolean | null): string {
  if (value === null) return "Unknown";
  return value ? "Reachable" : "Not observed";
}

function majorityLabel(value: boolean | null): string {
  if (value === null) return "Unknown";
  return value ? "Reachable" : "Unavailable";
}

function timeLabel(value: number | null): string {
  return value === null
    ? "Unknown"
    : new Date(value).toLocaleString();
}

function tone(value: boolean | null): string {
  return value === null
    ? "mfc-neutral"
    : value
      ? "mfc-good"
      : "mfc-bad";
}

function SnapshotCard({
  title,
  snapshot,
  group,
}: {
  title: string;
  snapshot: ClusterSnapshot;
  group: GroupSnapshot;
}) {
  const fields = [
    ["Raft term", numberLabel(group.term)],
    ["Commit index", numberLabel(group.commitIndex)],
    ["Applied index", numberLabel(group.appliedIndex)],
    [
      "Reachable replicas",
      group.reachable === null
        ? "Unknown"
        : `${group.reachable}/${group.replicas.length}`,
    ],
    ["Required majority", String(group.required)],
    ["Commit spread", numberLabel(group.commitSpread)],
  ];

  return (
    <section className="mfc-snapshot">
      <div className="mfc-snapshot-heading">
        <span>{title}</span>

        <small className={tone(snapshot.available)}>
          {snapshot.available
            ? "Observation available"
            : "Observation unavailable"}
        </small>
      </div>

      <div className="mfc-leader">
        <Crown size={22} />

        <div>
          <small>Observed leader</small>
          <strong>{leaderLabel(group.leaderId)}</strong>
        </div>
      </div>

      <dl className="mfc-fields">
        {fields.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}

        <div>
          <dt>Reachable majority</dt>

          <dd className={tone(group.majority)}>
            {majorityLabel(group.majority)}
          </dd>
        </div>
      </dl>

      <footer className="mfc-snapshot-time">
        <Clock size={13} />

        <span>
          Observed: {timeLabel(snapshot.observedAt)}
        </span>
      </footer>
    </section>
  );
}

export function MultiFaultComparison({
  before,
  after,
  actionLabel,
  pending = false,
  message,
}: Props) {
  const [selectedGroup, setSelectedGroup] =
    useState<SnapshotGroupID>(1);

  const comparison = useMemo(
    () => before && after
      ? compareClusterSnapshots(before, after)
      : null,
    [before, after],
  );

  const group = comparison?.groups.find(
    item => item.groupId === selectedGroup,
  );

  const beforeGroup = before?.groups.find(
    item => item.groupId === selectedGroup,
  );

  const quorumClass =
    group?.quorum === "retained" ||
    group?.quorum === "restored"
      ? "mfc-good"
      : group?.quorum === "lost" ||
          group?.quorum === "unavailable"
        ? "mfc-bad"
        : "mfc-neutral";

  return (
    <article className="ma-panel mfc-panel">
      <header className="mfc-header">
        <div>
          <div className="eyebrow">
            CLUSTER STATE COMPARISON
          </div>

          <h2>
            <GitCompare size={21} />
            Before &amp; After Cluster State
          </h2>

          <p>
            Compare recorded observations across Raft
            groups and individual replicas.
          </p>
        </div>

        <span className="mfc-fixed-label">
          Immutable snapshots
        </span>
      </header>

      {message && (
        <div className="mfc-notice">
          <AlertTriangle size={16} />
          {message}
        </div>
      )}

      {!before ? (
        <div className="mfc-empty">
          <GitCompare size={32} />

          <strong>No action snapshot recorded</strong>

          <span>
            Select a recorded Docker action to inspect
            its before-and-after observations.
          </span>
        </div>
      ) : (
        <>
          <div className="mfc-action">
            <div>
              <small>Selected Docker action</small>

              <strong>
                {actionLabel ?? "Recorded action"}
              </strong>
            </div>

            <div>
              <small>Before capture</small>
              <strong>{timeLabel(before.capturedAt)}</strong>
            </div>

            <div>
              <small>After capture</small>

              <strong>
                {after
                  ? timeLabel(after.capturedAt)
                  : pending
                    ? "Waiting for observation"
                    : "Not captured"}
              </strong>
            </div>
          </div>

          <div className="mfc-group-selector">
            {before.groups.map(item => (
              <button
                type="button"
                key={item.groupId}
                aria-pressed={
                  selectedGroup === item.groupId
                }
                className={
                  selectedGroup === item.groupId
                    ? "mfc-active-group"
                    : ""
                }
                onClick={() =>
                  setSelectedGroup(item.groupId)
                }
              >
                {item.label}
              </button>
            ))}
          </div>

          {!after && (
            <div className="mfc-notice">
              <Clock size={16} />

              {pending
                ? "Waiting for a fresh post-action observation."
                : "No post-action snapshot is available."}
            </div>
          )}

          {comparison && !comparison.comparable && (
            <div className="mfc-notice">
              <AlertTriangle size={16} />

              These observations are unavailable or
              not chronologically comparable.
              Differences remain unknown.
            </div>
          )}

          {comparison && (
            <div className="mfc-metrics">
              {[
                {
                  label: "Reachable nodes",
                  before: comparison.before.reachableNodes,
                  after: comparison.after.reachableNodes,
                  delta: comparison.reachableNodeDelta,
                },
                {
                  label: "Reachable majorities",
                  before: comparison.before.reachableMajorities,
                  after: comparison.after.reachableMajorities,
                  delta: comparison.majorityDelta,
                },
              ].map(metric => (
                <div
                  className="mfc-metric"
                  key={metric.label}
                >
                  <small>{metric.label}</small>

                  <strong>
                    {numberLabel(metric.before)}
                    <ArrowRight size={16} />
                    {numberLabel(metric.after)}
                  </strong>

                  <span>
                    Difference: {deltaLabel(metric.delta)}
                  </span>
                </div>
              ))}

              <div className="mfc-metric">
                <small>Known leadership changes</small>

                <strong>
                  {comparison.comparable
                    ? comparison.leadershipChanges
                    : "Unknown"}
                </strong>

                <span>Between recorded observations</span>
              </div>

              <div className="mfc-metric">
                <small>Observation interval</small>

                <strong>
                  {comparison.observationIntervalMs === null
                    ? "Unknown"
                    : `${(
                        comparison.observationIntervalMs /
                        1000
                      ).toFixed(2)} s`}
                </strong>

                <span>Not recovery duration</span>
              </div>
            </div>
          )}

          <div className="mfc-snapshot-grid">
            {beforeGroup && (
              <SnapshotCard
                title="BEFORE ACTION"
                snapshot={before}
                group={beforeGroup}
              />
            )}

            {after && group ? (
              <SnapshotCard
                title="AFTER ACTION"
                snapshot={after}
                group={group.after}
              />
            ) : (
              <div className="mfc-empty">
                <Clock size={28} />

                <strong>After snapshot pending</strong>

                <span>
                  No values will be fabricated while
                  observations are unavailable.
                </span>
              </div>
            )}
          </div>

          {group && comparison && (
            <>
              <section className="mfc-section">
                <div className="mfc-section-heading">
                  <h3>
                    <Database size={17} />
                    {group.label} state changes
                  </h3>

                  <span className={quorumClass}>
                    <ShieldCheck size={15} />
                    {QUORUM_LABELS[group.quorum]}
                  </span>
                </div>

                <div className="ma-table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Metric</th>
                        <th>Before</th>
                        <th>After</th>
                        <th>Difference</th>
                      </tr>
                    </thead>

                    <tbody>
                      <tr>
                        <td>Observed leader</td>

                        <td>
                          {leaderLabel(group.before.leaderId)}
                        </td>

                        <td>
                          {leaderLabel(group.after.leaderId)}
                        </td>

                        <td>
                          {group.leaderChanged === null
                            ? "Unknown"
                            : group.leaderChanged
                              ? "Changed"
                              : "Unchanged"}
                        </td>
                      </tr>

                      {[
                        [
                          "Raft term",
                          group.before.term,
                          group.after.term,
                          group.termDelta,
                        ],
                        [
                          "Commit index",
                          group.before.commitIndex,
                          group.after.commitIndex,
                          group.commitDelta,
                        ],
                        [
                          "Applied index",
                          group.before.appliedIndex,
                          group.after.appliedIndex,
                          group.appliedDelta,
                        ],
                        [
                          "Reachable replicas",
                          group.before.reachable,
                          group.after.reachable,
                          group.reachableDelta,
                        ],
                      ].map(([label, previous, current, change]) => (
                        <tr key={String(label)}>
                          <td>{label}</td>

                          <td>
                            {numberLabel(previous as number | null)}
                          </td>

                          <td>
                            {numberLabel(current as number | null)}
                          </td>

                          <td className="mfc-delta">
                            {deltaLabel(change as number | null)}
                          </td>
                        </tr>
                      ))}

                      <tr>
                        <td>Reachable majority</td>

                        <td>
                          {majorityLabel(group.before.majority)}
                        </td>

                        <td>
                          {majorityLabel(group.after.majority)}
                        </td>

                        <td className={quorumClass}>
                          {QUORUM_LABELS[group.quorum]}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="mfc-section">
                <div className="mfc-section-heading">
                  <h3>
                    <Server size={17} />
                    Individual replica comparison
                  </h3>
                </div>

                <div className="mfc-replica-grid">
                  {group.replicas.map(item => (
                    <article
                      className="mfc-replica"
                      key={item.nodeId}
                    >
                      <header>
                        <strong>Node {item.nodeId}</strong>

                        <span className={
                          tone(item.after.reachable)
                        }>
                          {reachableLabel(item.after.reachable)}
                        </span>
                      </header>

                      <div className="mfc-role-change">
                        <span>{item.before.role}</span>
                        <ArrowRight size={14} />
                        <span>{item.after.role}</span>
                      </div>

                      <dl className="mfc-fields">
                        <div>
                          <dt>Reachability transition</dt>
                          <dd>{item.reachability}</dd>
                        </div>

                        <div>
                          <dt>Term</dt>

                          <dd>
                            {numberLabel(item.before.term)}
                            {" → "}
                            {numberLabel(item.after.term)}
                          </dd>
                        </div>

                        <div>
                          <dt>Commit index</dt>

                          <dd>
                            {numberLabel(item.before.commitIndex)}
                            {" → "}
                            {numberLabel(item.after.commitIndex)}
                          </dd>
                        </div>

                        <div>
                          <dt>Applied index</dt>

                          <dd>
                            {numberLabel(item.before.appliedIndex)}
                            {" → "}
                            {numberLabel(item.after.appliedIndex)}
                          </dd>
                        </div>

                        <div>
                          <dt>Commit difference</dt>
                          <dd>{deltaLabel(item.commitDelta)}</dd>
                        </div>

                        <div>
                          <dt>Applied difference</dt>
                          <dd>{deltaLabel(item.appliedDelta)}</dd>
                        </div>
                      </dl>

                      <div className={
                        "mfc-catch-up " +
                        (
                          item.catchUp === "at-observed-leader-index"
                            ? "mfc-good"
                            : item.catchUp === "unavailable"
                              ? "mfc-bad"
                              : "mfc-neutral"
                        )
                      }>
                        <small>After-snapshot index alignment</small>

                        <strong>
                          {CATCH_UP_LABELS[item.catchUp]}
                        </strong>
                      </div>
                    </article>
                  ))}
                </div>
              </section>
            </>
          )}

          <details className="mfc-raw">
            <summary>Inspect recorded snapshot JSON</summary>

            <pre>
              {JSON.stringify(
                { before, after },
                null,
                2,
              )}
            </pre>
          </details>
        </>
      )}

      <footer className="mfc-footer">
        Snapshots represent independently polled
        observations, not atomic distributed state.
        Reachable majority does not prove consensus.
        Matching indexes do not prove data equality.
        Differences may include unrelated concurrent
        activity. Observation interval is not measured
        failover or recovery duration.
      </footer>
    </article>
  );
}

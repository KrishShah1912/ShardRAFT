import {
  useEffect,
  useState,
} from "react";

import {
  loadExperimentReport,
  type ExperimentReport,
  type ExperimentDataset,
  type ExperimentRow,
} from "./experimentReport";

import "./multiEvaluation.css";

function useExperimentReport() {
  const [report, setReport] =
    useState<ExperimentReport | null>(null);

  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    loadExperimentReport(controller.signal)
      .then(setReport)
      .catch(failure => {
        if (!controller.signal.aborted) {
          setError(String(failure));
        }
      });

    return () => controller.abort();
  }, []);

  return { report, error };
}

function numeric(value: string | undefined) {
  if (!value?.trim()) return null;

  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : null;
}

function format(
  value: string | number | undefined,
  digits = 2,
) {
  const number = numeric(
    value === undefined
      ? undefined
      : String(value),
  );

  return number === null
    ? "—"
    : number.toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <article className="ma-stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function Source({
  dataset,
}: {
  dataset: ExperimentDataset;
}) {
  return (
    <div className="me-source">
      Source: <code>{dataset.source}</code>
    </div>
  );
}

function BarChart({
  rows,
  label,
  metric,
  unit,
}: {
  rows: ExperimentRow[];
  label: string;
  metric: string;
  unit: string;
}) {
  const values = rows.map(
    row => numeric(row[metric]) ?? 0,
  );

  const max = Math.max(1, ...values);

  return (
    <div className="me-chart">
      {rows.map((row, index) => {
        const value = numeric(row[metric]);

        return (
          <div
            className="me-chart-row"
            key={`${row[label]}-${index}`}
          >
            <span>{row[label]}</span>

            <div className="me-track">
              <div
                className="me-bar"
                style={{
                  width: `${
                    Math.max(0, value ?? 0)
                    / max * 100
                  }%`,
                }}
              />
            </div>

            <strong>
              {format(row[metric])} {unit}
            </strong>
          </div>
        );
      })}
    </div>
  );
}

function DataTable({
  dataset,
  fields,
}: {
  dataset: ExperimentDataset;
  fields: Array<[string, string]>;
}) {
  return (
    <div className="ma-table-wrap">
      <table>
        <thead>
          <tr>
            {fields.map(([key, title]) => (
              <th key={key}>{title}</th>
            ))}
          </tr>
        </thead>

        <tbody>
          {dataset.rows.map((row, index) => (
            <tr key={index}>
              {fields.map(([key]) => (
                <td key={key}>
                  {row[key] || "—"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ReportHeader({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <header className="page-header">
      <div>
        <div className="eyebrow">
          CORRECTNESS AND PERFORMANCE
        </div>

        <h1>{title}</h1>

        <p>{description}</p>
      </div>
    </header>
  );
}

function HistoricalNotice({
  report,
}: {
  report: ExperimentReport;
}) {
  return (
    <div className="me-notice">
      <strong>Historical experiment evidence</strong>

      <p>
        These results were imported from previously
        recorded experiments. They are not measurements
        of the current cluster.
      </p>

      <span>
        Report generated: {
          new Date(report.generated_at)
            .toLocaleString()
        }
      </span>

      <a
        href="/generated/multi-raft-experiments.json"
        download
      >
        Download experiment report
      </a>
    </div>
  );
}

export function MultiPerformanceLab() {
  const { report, error } = useExperimentReport();

  const [metric, setMetric] =
    useState("throughput_ops");

  if (!report) {
    return (
      <section className="me-page">
        <ReportHeader
          title="Performance Lab"
          description="Recorded workload measurements."
        />

        <article className="ma-panel">
          {error || "Loading experiment report..."}
        </article>
      </section>
    );
  }

  const {
    performance,
    faults,
    wal,
  } = report.datasets;

  const summary = report.summary;

  const peak = Math.max(
    ...performance.rows.map(
      row => numeric(row.throughput_ops) ?? 0,
    ),
  );

  const faultValue = (name: string) =>
    faults.rows.find(
      row => row.measurement === name,
    )?.duration_ms;

  const fault = faults.rows[0];

  const ordered = [...performance.rows].sort(
    (a, b) =>
      (numeric(b[metric]) ?? 0) -
      (numeric(a[metric]) ?? 0),
  );

  return (
    <section className="me-page">
      <ReportHeader
        title="Performance Lab"
        description="Compare distributed workloads, latency, recovery and WAL synchronization."
      />

      <HistoricalNotice report={report} />

      <div className="ma-stat-grid">
        <Stat
          label="Workload experiments"
          value={summary.experiments}
        />

        <Stat
          label="Successful operations"
          value={
            summary.successful.toLocaleString()
          }
        />

        <Stat
          label="Peak recorded throughput"
          value={`${format(peak)} ops/s`}
        />

        <Stat
          label="Failed operations"
          value={summary.failed}
        />

        <Stat
          label="End-to-end failover"
          value={`${
            format(faultValue(
              "end_to_end_failover",
            ))
          } ms`}
        />

        <Stat
          label="Replica recovery"
          value={`${
            format(faultValue(
              "replica_recovery",
            ))
          } ms`}
        />
      </div>

      <article className="ma-panel">
        <div className="me-heading">
          <h2>Workload comparison</h2>

          <select
            aria-label="Performance metric"
            value={metric}
            onChange={event =>
              setMetric(event.target.value)
            }
          >
            <option value="throughput_ops">
              Throughput
            </option>

            <option value="p50_ms">
              p50 latency
            </option>

            <option value="p95_ms">
              p95 latency
            </option>

            <option value="p99_ms">
              p99 latency
            </option>
          </select>
        </div>

        <BarChart
          rows={ordered}
          label="experiment"
          metric={metric}
          unit={
            metric === "throughput_ops"
              ? "ops/s"
              : "ms"
          }
        />

        <p>
          Throughput counts successful operations
          per elapsed second. Latency percentiles
          include successful operations only.
        </p>

        <Source dataset={performance} />
      </article>

      <article className="ma-panel">
        <h2>Distributed workload results</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Experiment</th>
                <th>Distribution</th>
                <th>Workers</th>
                <th>Read ratio</th>
                <th>Value bytes</th>
                <th>Ops/s</th>
                <th>p50 ms</th>
                <th>p95 ms</th>
                <th>p99 ms</th>
              </tr>
            </thead>

            <tbody>
              {performance.rows.map(row => (
                <tr key={row.run_id}>
                  <td>{row.experiment}</td>
                  <td>{row.distribution}</td>
                  <td>{row.concurrency}</td>
                  <td>{row.read_ratio}</td>
                  <td>{row.value_size}</td>
                  <td>
                    {format(row.throughput_ops)}
                  </td>
                  <td>{format(row.p50_ms, 3)}</td>
                  <td>{format(row.p95_ms, 3)}</td>
                  <td>{format(row.p99_ms, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p>
          Each workload has one recorded repetition.
          Results are specific to the original local
          execution environment.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Failover and replica recovery</h2>

        <div className="me-transition">
          <div>
            <span>Original leader</span>
            <strong>
              Node {fault.original_leader}
            </strong>
            <span>Term {fault.original_term}</span>
          </div>

          <strong>→</strong>

          <div>
            <span>New leader</span>
            <strong>
              Node {fault.new_leader}
            </strong>
            <span>Term {fault.new_term}</span>
          </div>
        </div>

        <p>
          Data shard {fault.shard} ·
          {" "}{fault.failure_type}
        </p>

        <BarChart
          rows={faults.rows}
          label="measurement"
          metric="duration_ms"
          unit="ms"
        />

        <p>
          Failover includes stop initiation through
          successful read/write verification.
          Recovery includes restart through observed
          replica catch-up. Measurements overlap
          and must not be added together.
        </p>

        <Source dataset={faults} />
      </article>

      <article className="ma-panel">
        <h2>WAL synchronization microbenchmark</h2>

        <div className="me-notice">
          Isolated file synchronization measurements.
          These are not production distributed
          ShardRAFT throughput results.
        </div>

        <BarChart
          rows={wal.rows}
          label="mode"
          metric="throughput_records_sec"
          unit="records/s"
        />

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Mode</th>
                <th>Durability</th>
                <th>Records</th>
                <th>Sync calls</th>
                <th>Records/s</th>
                <th>p50 ms</th>
                <th>p95 ms</th>
                <th>p99 ms</th>
              </tr>
            </thead>

            <tbody>
              {wal.rows.map(row => (
                <tr key={row.mode}>
                  <td>{row.mode}</td>

                  <td
                    className={
                      row.durability === "weaker"
                        ? "me-warning"
                        : ""
                    }
                  >
                    {row.durability}
                  </td>

                  <td>{row.records}</td>
                  <td>{row.sync_calls}</td>

                  <td>
                    {format(
                      row.throughput_records_sec,
                    )}
                  </td>

                  <td>{format(row.p50_ms, 6)}</td>
                  <td>{format(row.p95_ms, 6)}</td>
                  <td>{format(row.p99_ms, 6)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p>
          Timed-sync and no-sync provide weaker
          durability. Production WAL configuration
          is unchanged. Power-loss durability was
          not established by this experiment.
        </p>

        <Source dataset={wal} />
      </article>
    </section>
  );
}

export function MultiTestLab() {
  const { report, error } = useExperimentReport();

  const [
    validationReport,
    setValidationReport,
  ] = useState("Checking availability...");

  useEffect(() => {
    const controller = new AbortController();

    fetch(
      "/generated/validation-report.json",
      {
        cache: "no-store",
        signal: controller.signal,
      },
    )
      .then(async response => {
        if (!response.ok) {
          throw new Error("Report unavailable");
        }

        const value = await response.json();

        if (
          value.schema_version !== 1 ||
          !Array.isArray(value.suites)
        ) {
          throw new Error("Invalid report");
        }

        setValidationReport(
          "A separate validation report exists. " +
          "Its scope and revision must be checked " +
          "before attributing results to Multi-Raft.",
        );
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setValidationReport(
            "No general validation report is available. " +
            "No test suite has been started.",
          );
        }
      });

    return () => controller.abort();
  }, []);

  if (!report) {
    return (
      <section className="me-page">
        <ReportHeader
          title="Test Lab"
          description="Recorded correctness evidence."
        />

        <article className="ma-panel">
          {error || "Loading correctness evidence..."}
        </article>
      </section>
    );
  }

  const summary = report.summary;
  const performance = report.datasets.performance;

  return (
    <section className="me-page">
      <ReportHeader
        title="Test Lab"
        description="Inspect recorded correctness histories and evaluation coverage."
      />

      <HistoricalNotice report={report} />

      <div className="ma-stat-grid">
        <Stat
          label="Recorded workloads"
          value={summary.experiments}
        />

        <Stat
          label="Linearizable histories"
          value={
            `${summary.linearizable_runs}` +
            `/${summary.experiments}`
          }
        />

        <Stat
          label="Successful operations"
          value={
            summary.successful.toLocaleString()
          }
        />

        <Stat
          label="Failed operations"
          value={summary.failed}
        />

        <Stat
          label="Unknown outcomes"
          value={summary.unknown}
        />

        <Stat
          label="Pending operations"
          value={summary.pending}
        />
      </div>

      <article className="ma-panel">
        <h2>Correctness verification pipeline</h2>

        <div className="me-pipeline">
          {[
            [
              "Workload generator",
              "Concurrent gateway requests",
            ],
            [
              "History recorder",
              "Inputs, outputs and timestamps",
            ],
            [
              "Porcupine checker",
              "Single-key state machine",
            ],
            [
              "Correctness verdict",
              "Recorded history classification",
            ],
          ].map(([title, detail], index) => (
            <div key={title}>
              <span>0{index + 1}</span>
              <strong>{title}</strong>
              <p>{detail}</p>
            </div>
          ))}
        </div>

        <p>
          The checker evaluates single-key histories
          using fresh key namespaces, initially empty
          state and no external writes to those
          namespaces.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Recorded Porcupine verdicts</h2>

        <div className="ma-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Experiment</th>
                <th>Recorded</th>
                <th>Successful</th>
                <th>Failed</th>
                <th>Unknown</th>
                <th>Pending</th>
                <th>Verdict</th>
              </tr>
            </thead>

            <tbody>
              {performance.rows.map(row => (
                <tr key={row.run_id}>
                  <td>{row.experiment}</td>
                  <td>{row.recorded}</td>
                  <td>{row.successful}</td>
                  <td>{row.failed}</td>
                  <td>{row.unknown}</td>
                  <td>{row.pending}</td>

                  <td>
                    <span
                      className={
                        row.checker_verdict
                          === "LINEARIZABLE"
                          ? "me-passed"
                          : "me-warning"
                      }
                    >
                      {row.checker_verdict}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <Source dataset={performance} />

        <p>
          Positive recorded verdicts are not a
          formal proof of implementation correctness
          or multi-key transaction guarantees.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Recorded evaluation coverage</h2>

        <div className="me-coverage">
          {[
            [
              "Uniform workload",
              "Baseline and workload variations",
              "Recorded",
            ],
            [
              "Zipfian workload",
              "Skewed key distribution",
              "Recorded",
            ],
            [
              "Read/write ratio",
              "Read-heavy and write-heavy",
              "Recorded",
            ],
            [
              "Record size",
              "128 B, 1 KB and 4 KB",
              "Recorded",
            ],
            [
              "Concurrency",
              "1, 4 and 16 workers",
              "Recorded",
            ],
            [
              "Leader failover",
              "Graceful Docker stop",
              "Measured",
            ],
            [
              "Replica recovery",
              "Observed catch-up",
              "Measured",
            ],
            [
              "WAL synchronization",
              "Isolated file microbenchmark",
              "Measured",
            ],
          ].map(([title, detail, status]) => (
            <div key={title}>
              <div>
                <strong>{title}</strong>
                <span>{detail}</span>
              </div>

              <span className="me-passed">
                {status}
              </span>
            </div>
          ))}
        </div>
      </article>

      <article className="ma-panel">
        <h2>Fault measurement evidence</h2>

        <DataTable
          dataset={report.datasets.faults}
          fields={[
            ["measurement", "Measurement"],
            ["duration_ms", "Duration (ms)"],
            ["shard", "Shard"],
            ["failure_type", "Failure type"],
          ]}
        />

        <Source dataset={report.datasets.faults} />
      </article>

      <article className="ma-panel">
        <h2>General validation report</h2>

        <div className="me-notice">
          {validationReport}
        </div>

        <p>
          Full Go, frontend and browser test
          results are separate from the historical
          experiment evidence shown above.
        </p>
      </article>

      <article className="ma-panel">
        <h2>Evaluation limitations</h2>

        <ul className="me-limitations">
          <li>
            One repetition per recorded workload.
          </li>
          <li>
            Single-key correctness checking only.
          </li>
          <li>
            No formal correctness proof.
          </li>
          <li>
            Graceful stops, not abrupt power loss.
          </li>
          <li>
            Recovery includes observed catch-up.
          </li>
          <li>
            WAL microbenchmark is isolated from
            distributed replication.
          </li>
          <li>
            Distributed WAL mode was recorded as
            default-unverified.
          </li>
        </ul>
      </article>
    </section>
  );
}

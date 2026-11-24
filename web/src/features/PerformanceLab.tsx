import {
  Activity,
  CheckCircle2,
  Clock3,
  Gauge,
  Play,
  RefreshCw,
  Server,
  Timer,
  TrendingUp,
  XCircle,
} from "lucide-react";
import {
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getBenchmarkHistory,
  getCurrentBenchmark,
  startBenchmark,
  type BenchmarkConfig,
  type BenchmarkJob,
  type BenchmarkLatency,
} from "../api/benchmarks";

const DEFAULT_CONFIG: BenchmarkConfig = {
  requests: 1000,
  concurrency: 8,
  read_ratio: 0.8,
  value_size: 128,
  mode: "normal",
  failure_delay_ms: 1500,
};

function number(
  value: number,
  digits = 1,
) {
  return value.toLocaleString(
    undefined,
    {
      maximumFractionDigits:
        digits,
    },
  );
}

function latencyRows(
  latency: BenchmarkLatency,
) {
  return [
    ["Average", latency.average_ms],
    ["p50", latency.p50_ms],
    ["p95", latency.p95_ms],
    ["p99", latency.p99_ms],
    ["Max", latency.max_ms],
  ] as const;
}

function percentChange(
  baseline: number,
  candidate: number,
) {
  if (baseline === 0) {
    return 0;
  }

  return (
    ((candidate - baseline) /
      baseline) *
    100
  );
}

function successRateFor(
  job: BenchmarkJob,
) {
  const result = job.result;

  if (
    !result ||
    result.requests === 0
  ) {
    return 0;
  }

  return (
    (result.successful /
      result.requests) *
    100
  );
}


interface ScalingPoint {
  concurrency: number;
  throughput: number;
  readP95: number;
  writeP95: number;
  runs: number;
}

function buildScalingPoints(
  history: BenchmarkJob[],
): ScalingPoint[] {
  const groups =
    new Map<
      number,
      {
        throughput: number[];
        readP95: number[];
        writeP95: number[];
      }
    >();

  for (const job of history) {
    if (
      job.state !== "passed" ||
      !job.result ||
      !job.config
    ) {
      continue;
    }

    /*
     * Keep the scaling comparison fair:
     * same workload shape and value size.
     */
    if (
      job.config.read_ratio !== 0.8 ||
      job.config.value_size !== 128
    ) {
      continue;
    }

    const concurrency =
      job.config.concurrency;

    const group =
      groups.get(concurrency) ?? {
        throughput: [],
        readP95: [],
        writeP95: [],
      };

    group.throughput.push(
      job.result.throughput_ops,
    );

    group.readP95.push(
      job.result.read_latency.p95_ms,
    );

    group.writeP95.push(
      job.result.write_latency.p95_ms,
    );

    groups.set(
      concurrency,
      group,
    );
  }

  return Array.from(
    groups.entries(),
  )
    .map(
      ([
        concurrency,
        group,
      ]) => ({
        concurrency,

        throughput:
          average(group.throughput),

        readP95:
          average(group.readP95),

        writeP95:
          average(group.writeP95),

        runs:
          group.throughput.length,
      }),
    )
    .sort(
      (a, b) =>
        a.concurrency -
        b.concurrency,
    );
}

function average(
  values: number[],
) {
  if (values.length === 0) {
    return 0;
  }

  return (
    values.reduce(
      (sum, value) =>
        sum + value,
      0,
    ) / values.length
  );
}


function sameWorkload(
  a: BenchmarkJob,
  b: BenchmarkJob,
) {
  if (!a.config || !b.config) {
    return false;
  }

  return (
    a.config.requests ===
      b.config.requests &&
    a.config.concurrency ===
      b.config.concurrency &&
    a.config.read_ratio ===
      b.config.read_ratio &&
    a.config.value_size ===
      b.config.value_size
  );
}

function isNormalRun(
  job: BenchmarkJob,
) {
  return (
    !job.experiment &&
    (
      !job.config?.mode ||
      job.config.mode === "normal"
    )
  );
}

function findNormalBaseline(
  failure: BenchmarkJob,
  history: BenchmarkJob[],
) {
  return history.find(
    (candidate) =>
      candidate.id !== failure.id &&
      candidate.state === "passed" &&
      Boolean(candidate.result) &&
      isNormalRun(candidate) &&
      sameWorkload(
        failure,
        candidate,
      ),
  );
}

export function PerformanceLab() {
  const [config, setConfig] =
    useState(DEFAULT_CONFIG);

  const [current, setCurrent] =
    useState<BenchmarkJob>({
      state: "idle",
    });

  const [history, setHistory] =
    useState<BenchmarkJob[]>([]);

  const [comparisonIds, setComparisonIds] =
    useState<string[]>([]);

  const [error, setError] =
    useState("");

  const [starting, setStarting] =
    useState(false);

  const refresh = useCallback(
    async () => {
      try {
        const [
          currentJob,
          historyResponse,
        ] = await Promise.all([
          getCurrentBenchmark(),
          getBenchmarkHistory(),
        ]);

        setCurrent(currentJob);
        setHistory(
          historyResponse.runs,
        );
        setError("");
      } catch (value) {
        setError(
          value instanceof Error
            ? value.message
            : "Unable to reach benchmark service",
        );
      }
    },
    [],
  );

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (
      current.state !== "running"
    ) {
      return;
    }

    const timer =
      window.setInterval(
        () => {
          void refresh();
        },
        800,
      );

    return () =>
      window.clearInterval(timer);
  }, [
    current.state,
    refresh,
  ]);

  function toggleComparison(
    id: string,
  ) {
    setComparisonIds(
      (currentIds) => {
        if (
          currentIds.includes(id)
        ) {
          return currentIds.filter(
            (value) =>
              value !== id,
          );
        }

        if (
          currentIds.length >= 2
        ) {
          return [
            currentIds[1],
            id,
          ];
        }

        return [
          ...currentIds,
          id,
        ];
      },
    );
  }

  async function run() {
    setStarting(true);
    setError("");

    try {
      const job =
        await startBenchmark(config);

      setCurrent(job);
    } catch (value) {
      setError(
        value instanceof Error
          ? value.message
          : "Benchmark could not start",
      );
    } finally {
      setStarting(false);
    }
  }

  const comparisonRuns =
    comparisonIds
      .map((id) =>
        history.find(
          (job) =>
            job.id === id,
        ),
      )
      .filter(
        (
          job,
        ): job is BenchmarkJob =>
          Boolean(
            job?.result,
          ),
      );

  const comparisonA =
    comparisonRuns[0];

  const comparisonB =
    comparisonRuns[1];

  const scalingPoints =
    useMemo(
      () =>
        buildScalingPoints(
          history,
        ),
      [history],
    );

  const normalBaseline =
    current.experiment?.mode ===
      "leader-failure"
      ? findNormalBaseline(
          current,
          history,
        )
      : undefined;

  const result =
    current.result;

  const successRate =
    result && result.requests > 0
      ? (result.successful /
          result.requests) *
        100
      : 0;

  const commitAdvance =
    result
      ? result.final_leader
          .commit_index -
        result.initial_leader
          .commit_index
      : 0;

  const maxLatency = useMemo(
    () => {
      if (!result) {
        return 1;
      }

      return Math.max(
        result.read_latency.max_ms,
        result.write_latency.max_ms,
        1,
      );
    },
    [result],
  );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            EXPERIMENTS
          </div>

          <h1>Performance Lab</h1>

          <p>
            Run real workloads against
            the live Raft cluster and
            inspect throughput, latency,
            replication progress, and
            benchmark history.
          </p>
        </div>

        <div
          className={`performance-state ${current.state}`}
        >
          <Activity size={15} />

          {current.state.toUpperCase()}
        </div>
      </header>

      <div className="performance-layout">
        <section className="panel performance-config">
          <div className="panel-heading">
            <div>
              <h2>Workload</h2>
              <p>
                Configure a bounded
                benchmark run
              </p>
            </div>
          </div>

          <label>
            <span>Requests</span>

            <input
              type="number"
              min={100}
              max={50000}
              value={config.requests}
              onChange={(event) =>
                setConfig({
                  ...config,
                  requests:
                    Number(
                      event.target.value,
                    ),
                })
              }
            />

            <small>
              100 – 50,000 operations
            </small>
          </label>

          <label>
            <span>Concurrency</span>

            <input
              type="number"
              min={1}
              max={64}
              value={
                config.concurrency
              }
              onChange={(event) =>
                setConfig({
                  ...config,
                  concurrency:
                    Number(
                      event.target.value,
                    ),
                })
              }
            />

            <small>
              1 – 64 workers
            </small>
          </label>

          <label>
            <span>Read ratio</span>

            <div className="performance-range">
              <input
                type="range"
                min={0}
                max={100}
                value={
                  config.read_ratio *
                  100
                }
                onChange={(event) =>
                  setConfig({
                    ...config,
                    read_ratio:
                      Number(
                        event.target
                          .value,
                      ) / 100,
                  })
                }
              />

              <strong>
                {Math.round(
                  config.read_ratio *
                    100,
                )}
                %
              </strong>
            </div>

            <small>
              {Math.round(
                config.read_ratio *
                  100,
              )}
              % reads ·{" "}
              {Math.round(
                (1 -
                  config.read_ratio) *
                  100,
              )}
              % writes
            </small>
          </label>

          <label>
            <span>Value size</span>

            <select
              value={config.value_size}
              onChange={(event) =>
                setConfig({
                  ...config,
                  value_size:
                    Number(
                      event.target.value,
                    ),
                })
              }
            >
              <option value={64}>
                64 B
              </option>
              <option value={128}>
                128 B
              </option>
              <option value={256}>
                256 B
              </option>
              <option value={512}>
                512 B
              </option>
              <option value={1024}>
                1 KB
              </option>
              <option value={4096}>
                4 KB
              </option>
            </select>
          </label>

          <div className="performance-experiment-control">
            <span>Experiment</span>

            <div className="performance-mode-options">
              <button
                type="button"
                className={
                  config.mode === "normal"
                    ? "active"
                    : ""
                }
                onClick={() =>
                  setConfig({
                    ...config,
                    mode: "normal",
                  })
                }
              >
                Normal
              </button>

              <button
                type="button"
                className={
                  config.mode ===
                  "leader-failure"
                    ? "active"
                    : ""
                }
                onClick={() =>
                  setConfig({
                    ...config,
                    mode:
                      "leader-failure",
                  })
                }
              >
                Leader Failure
              </button>
            </div>

            {config.mode ===
              "leader-failure" && (
              <>
                <span className="performance-delay-label">
                  Inject failure after
                </span>

                <select
                  value={
                    config.failure_delay_ms ??
                    1500
                  }
                  onChange={(event) =>
                    setConfig({
                      ...config,
                      failure_delay_ms:
                        Number(
                          event.target.value,
                        ),
                    })
                  }
                >
                  <option value={500}>
                    0.5 seconds
                  </option>

                  <option value={1000}>
                    1 second
                  </option>

                  <option value={1500}>
                    1.5 seconds
                  </option>

                  <option value={2000}>
                    2 seconds
                  </option>

                  <option value={3000}>
                    3 seconds
                  </option>
                </select>

                <small>
                  Stop the current leader
                  during the workload,
                  elect a replacement,
                  then recover the failed
                  replica.
                </small>
              </>
            )}
          </div>

          <button
            className="performance-run"
            disabled={
              starting ||
              current.state ===
                "running"
            }
            onClick={() =>
              void run()
            }
          >
            {current.state ===
            "running" ? (
              <RefreshCw
                size={16}
                className="spin"
              />
            ) : (
              <Play size={16} />
            )}

            {current.state ===
            "running"
              ? "Benchmark running"
              : "Run benchmark"}
          </button>

          {error && (
            <div className="performance-error">
              <XCircle size={15} />
              {error}
            </div>
          )}
        </section>

        <main className="performance-main">
          {current.state ===
            "running" && (
            <section className="panel performance-running">
              <RefreshCw
                size={22}
                className="spin"
              />

              <div>
                <strong>
                  Benchmark in progress
                </strong>

                <span>
                  {current.config
                    ?.requests.toLocaleString()}{" "}
                  operations ·{" "}
                  {
                    current.config
                      ?.concurrency
                  }{" "}
                  workers
                </span>
              </div>
            </section>
          )}

          {result ? (
            <>
              <div className="performance-kpis">
                <div className="panel">
                  <TrendingUp size={17} />
                  <span>THROUGHPUT</span>
                  <strong>
                    {number(
                      result.throughput_ops,
                      2,
                    )}
                  </strong>
                  <small>ops/sec</small>
                </div>

                <div className="panel">
                  <CheckCircle2 size={17} />
                  <span>SUCCESS</span>
                  <strong>
                    {number(
                      successRate,
                      1,
                    )}
                    %
                  </strong>
                  <small>
                    {result.successful.toLocaleString()}
                    /
                    {result.requests.toLocaleString()}
                  </small>
                </div>

                <div className="panel">
                  <Clock3 size={17} />
                  <span>READ p95</span>
                  <strong>
                    {number(
                      result
                        .read_latency
                        .p95_ms,
                      2,
                    )}
                  </strong>
                  <small>milliseconds</small>
                </div>

                <div className="panel">
                  <Timer size={17} />
                  <span>WRITE p95</span>
                  <strong>
                    {number(
                      result
                        .write_latency
                        .p95_ms,
                      2,
                    )}
                  </strong>
                  <small>milliseconds</small>
                </div>
              </div>

              <section className="panel performance-latency">
                <div className="panel-heading">
                  <div>
                    <h2>
                      Latency Distribution
                    </h2>
                    <p>
                      Read versus replicated
                      write latency
                    </p>
                  </div>
                </div>

                <div className="performance-latency-grid">
                  <LatencyColumn
                    title="READ"
                    latency={
                      result.read_latency
                    }
                    max={maxLatency}
                  />

                  <LatencyColumn
                    title="WRITE"
                    latency={
                      result.write_latency
                    }
                    max={maxLatency}
                  />
                </div>
              </section>

              <section className="panel performance-cluster-result">
                <div className="panel-heading">
                  <div>
                    <h2>
                      Cluster Impact
                    </h2>
                    <p>
                      Consensus state before
                      and after the workload
                    </p>
                  </div>
                </div>

                <div className="performance-cluster-grid">
                  <div>
                    <Server size={17} />
                    <span>
                      INITIAL LEADER
                    </span>
                    <strong>
                      Node{" "}
                      {
                        result
                          .initial_leader
                          .id
                      }
                    </strong>
                    <small>
                      Term{" "}
                      {
                        result
                          .initial_leader
                          .term
                      }
                    </small>
                  </div>

                  <div>
                    <Server size={17} />
                    <span>
                      FINAL LEADER
                    </span>
                    <strong>
                      Node{" "}
                      {
                        result
                          .final_leader
                          .id
                      }
                    </strong>
                    <small>
                      Term{" "}
                      {
                        result
                          .final_leader
                          .term
                      }
                    </small>
                  </div>

                  <div>
                    <Gauge size={17} />
                    <span>
                      COMMIT ADVANCE
                    </span>
                    <strong>
                      +{commitAdvance}
                    </strong>
                    <small>
                      committed entries
                    </small>
                  </div>

                  <div>
                    <Activity size={17} />
                    <span>
                      DURATION
                    </span>
                    <strong>
                      {number(
                        result.duration_ms /
                          1000,
                        2,
                      )}
                      s
                    </strong>
                    <small>
                      benchmark workload
                    </small>
                  </div>
                </div>
              </section>
            </>
          ) : (
            current.state !==
              "running" && (
              <section className="panel performance-empty">
                <Gauge size={30} />

                <h2>
                  Ready to benchmark
                </h2>

                <p>
                  Configure a workload and
                  run it against the live
                  ShardRAFT cluster.
                </p>
              </section>
            )
          )}
        </main>
      </div>

      {comparisonA &&
        comparisonB && (
          <PerformanceComparison
            a={comparisonA}
            b={comparisonB}
          />
        )}

      {current.experiment &&
        current.experiment.mode ===
          "leader-failure" && (
          <FailoverImpact
            experiment={
              current.experiment
            }
          />
        )}

      {current.experiment?.mode ===
          "leader-failure" &&
        normalBaseline && (
          <FaultToleranceComparison
            normal={normalBaseline}
            failure={current}
          />
        )}

      <ScalingAnalysis
        points={scalingPoints}
      />

      <section className="panel performance-history">
        <div className="panel-heading">
          <div>
            <h2>Benchmark History</h2>
            <p>
              Recent runs from this
              supervisor session
            </p>
          </div>

          <button
            onClick={() =>
              void refresh()
            }
          >
            <RefreshCw size={14} />
            Refresh
          </button>
        </div>

        {history.length === 0 ? (
          <div className="performance-history-empty">
            No benchmark history yet.
          </div>
        ) : (
          <div className="performance-history-table">
            <div className="performance-history-head">
              <span>COMPARE</span>
              <span>RUN</span>
              <span>WORKLOAD</span>
              <span>CONCURRENCY</span>
              <span>THROUGHPUT</span>
              <span>READ p95</span>
              <span>WRITE p95</span>
              <span>RESULT</span>
            </div>

            {history.map(
              (job, index) => (
                <div
                  className="performance-history-row"
                  key={job.id}
                >
                  <button
                    className={`performance-compare-selector ${
                      comparisonIds.includes(
                        job.id ?? "",
                      )
                        ? "selected"
                        : ""
                    }`}
                    disabled={
                      !job.id ||
                      !job.result
                    }
                    onClick={() => {
                      if (job.id) {
                        toggleComparison(
                          job.id,
                        );
                      }
                    }}
                  >
                    {comparisonIds.indexOf(
                      job.id ?? "",
                    ) === 0
                      ? "A"
                      : comparisonIds.indexOf(
                            job.id ?? "",
                          ) === 1
                        ? "B"
                        : "+"}
                  </button>

                  <span>
                    #{history.length -
                      index}
                  </span>

                  <span>
                    {Math.round(
                      (job.config
                        ?.read_ratio ??
                        0) * 100,
                    )}
                    R /{" "}
                    {Math.round(
                      (1 -
                        (job.config
                          ?.read_ratio ??
                          0)) *
                        100,
                    )}
                    W
                  </span>

                  <span>
                    {
                      job.config
                        ?.concurrency
                    }
                  </span>

                  <strong>
                    {job.result
                      ? number(
                          job.result
                            .throughput_ops,
                          1,
                        )
                      : "—"}
                  </strong>

                  <span>
                    {job.result
                      ? `${number(
                          job.result
                            .read_latency
                            .p95_ms,
                          1,
                        )} ms`
                      : "—"}
                  </span>

                  <span>
                    {job.result
                      ? `${number(
                          job.result
                            .write_latency
                            .p95_ms,
                          1,
                        )} ms`
                      : "—"}
                  </span>

                  <span
                    className={`performance-history-state ${job.state}`}
                  >
                    {job.state}
                  </span>
                </div>
              ),
            )}
          </div>
        )}
      </section>
    </>
  );
}

function LatencyColumn({
  title,
  latency,
  max,
}: {
  title: string;
  latency: BenchmarkLatency;
  max: number;
}) {
  return (
    <div className="performance-latency-column">
      <h3>{title}</h3>

      {latencyRows(
        latency,
      ).map(([label, value]) => (
        <div
          className="performance-latency-row"
          key={label}
        >
          <span>{label}</span>

          <div>
            <i
              style={{
                width: `${Math.max(
                  2,
                  (value / max) *
                    100,
                )}%`,
              }}
            />
          </div>

          <strong>
            {number(value, 2)} ms
          </strong>
        </div>
      ))}
    </div>
  );
}

function ComparisonMetric({
  label,
  a,
  b,
  unit,
  higherIsBetter,
}: {
  label: string;
  a: number;
  b: number;
  unit: string;
  higherIsBetter: boolean;
}) {
  const change =
    percentChange(a, b);

  const improved =
    higherIsBetter
      ? change > 0
      : change < 0;

  const regressed =
    higherIsBetter
      ? change < 0
      : change > 0;

  return (
    <div className="performance-comparison-row">
      <strong>{label}</strong>

      <span>
        {number(a, 2)}
        {unit}
      </span>

      <span>
        {number(b, 2)}
        {unit}
      </span>

      <span
        className={
          improved
            ? "comparison-good"
            : regressed
              ? "comparison-bad"
              : "comparison-neutral"
        }
      >
        {change > 0 ? "+" : ""}
        {number(change, 1)}%
      </span>
    </div>
  );
}

function PerformanceComparison({
  a,
  b,
}: {
  a: BenchmarkJob;
  b: BenchmarkJob;
}) {
  if (
    !a.result ||
    !b.result
  ) {
    return null;
  }

  const ar = a.result;
  const br = b.result;

  return (
    <section className="panel performance-comparison">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">
            RUN COMPARISON
          </div>

          <h2>
            A vs B
          </h2>

          <p>
            Compare throughput,
            latency, workload pressure,
            and success rate.
          </p>
        </div>

        <div className="comparison-run-labels">
          <span>
            A · {a.config?.concurrency}
            {" "}workers
          </span>

          <span>
            B · {b.config?.concurrency}
            {" "}workers
          </span>
        </div>
      </div>

      <div className="performance-comparison-table">
        <div className="performance-comparison-head">
          <span>METRIC</span>
          <span>RUN A</span>
          <span>RUN B</span>
          <span>CHANGE</span>
        </div>

        <ComparisonMetric
          label="Throughput"
          a={ar.throughput_ops}
          b={br.throughput_ops}
          unit=" ops/s"
          higherIsBetter
        />

        <ComparisonMetric
          label="Read p95"
          a={ar.read_latency.p95_ms}
          b={br.read_latency.p95_ms}
          unit=" ms"
          higherIsBetter={false}
        />

        <ComparisonMetric
          label="Write p95"
          a={ar.write_latency.p95_ms}
          b={br.write_latency.p95_ms}
          unit=" ms"
          higherIsBetter={false}
        />

        <ComparisonMetric
          label="Overall p99"
          a={ar.overall_latency.p99_ms}
          b={br.overall_latency.p99_ms}
          unit=" ms"
          higherIsBetter={false}
        />

        <ComparisonMetric
          label="Success"
          a={successRateFor(a)}
          b={successRateFor(b)}
          unit="%"
          higherIsBetter
        />

        <ComparisonMetric
          label="Concurrency"
          a={a.config?.concurrency ?? 0}
          b={b.config?.concurrency ?? 0}
          unit=""
          higherIsBetter
        />
      </div>
    </section>
  );
}

function ScalingMetric({
  label,
  value,
  max,
  unit,
}: {
  label: string;
  value: number;
  max: number;
  unit: string;
}) {
  return (
    <div className="scaling-metric">
      <span>{label}</span>

      <div>
        <i
          style={{
            width: `${Math.max(
              3,
              (value / max) * 100,
            )}%`,
          }}
        />
      </div>

      <strong>
        {number(value, 1)}
        {unit}
      </strong>
    </div>
  );
}

function ScalingAnalysis({
  points,
}: {
  points: ScalingPoint[];
}) {
  if (points.length < 2) {
    return null;
  }

  const maxThroughput =
    Math.max(
      ...points.map(
        (point) =>
          point.throughput,
      ),
      1,
    );

  const maxLatency =
    Math.max(
      ...points.flatMap(
        (point) => [
          point.readP95,
          point.writeP95,
        ],
      ),
      1,
    );

  const bestThroughput =
    points.reduce(
      (best, point) =>
        point.throughput >
        best.throughput
          ? point
          : best,
    );

  return (
    <section className="panel scaling-panel">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">
            SCALING ANALYSIS
          </div>

          <h2>
            Concurrency Scaling
          </h2>

          <p>
            Average results for comparable
            80R / 20W, 128-byte workloads.
          </p>
        </div>

        <div className="scaling-best">
          <span>
            PEAK THROUGHPUT
          </span>

          <strong>
            {number(
              bestThroughput
                .throughput,
              1,
            )}{" "}
            ops/s
          </strong>

          <small>
            {
              bestThroughput
                .concurrency
            }{" "}
            workers
          </small>
        </div>
      </div>

      <div className="scaling-grid">
        {points.map(
          (point) => (
            <article
              className="scaling-card"
              key={
                point.concurrency
              }
            >
              <div className="scaling-card-head">
                <div>
                  <span>
                    CONCURRENCY
                  </span>

                  <strong>
                    {
                      point.concurrency
                    }
                  </strong>
                </div>

                <small>
                  {point.runs}{" "}
                  {point.runs === 1
                    ? "run"
                    : "runs"}
                </small>
              </div>

              <ScalingMetric
                label="Throughput"
                value={
                  point.throughput
                }
                max={maxThroughput}
                unit=" ops/s"
              />

              <ScalingMetric
                label="Read p95"
                value={
                  point.readP95
                }
                max={maxLatency}
                unit=" ms"
              />

              <ScalingMetric
                label="Write p95"
                value={
                  point.writeP95
                }
                max={maxLatency}
                unit=" ms"
              />
            </article>
          ),
        )}
      </div>

      <div className="scaling-tradeoff">
        <strong>
          Scaling tradeoff
        </strong>

        <span>
          Higher concurrency can improve
          throughput while increasing
          tail latency. Compare throughput
          gain against read and write p95
          before choosing a workload
          operating point.
        </span>
      </div>
    </section>
  );
}

function FailoverImpact({
  experiment,
}: {
  experiment: {
    failed_node_id?: number;
    leader_before?: number;
    leader_after?: number;
    term_before?: number;
    term_after?: number;
    failover_ms?: number;
    recovered: boolean;
  };
}) {
  return (
    <section className="panel failover-impact">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">
            FAILURE EXPERIMENT
          </div>

          <h2>
            Leader Failover Impact
          </h2>

          <p>
            Observed Raft behavior while
            the benchmark continued
            through leader failure.
          </p>
        </div>

        <span
          className={`failover-recovery ${
            experiment.recovered
              ? "recovered"
              : "failed"
          }`}
        >
          {experiment.recovered
            ? "RECOVERED"
            : "NOT RECOVERED"}
        </span>
      </div>

      <div className="failover-flow">
        <div>
          <span>INITIAL LEADER</span>
          <strong>
            Node{" "}
            {experiment.leader_before ??
              "—"}
          </strong>
          <small>
            Term{" "}
            {experiment.term_before ??
              "—"}
          </small>
        </div>

        <div className="failover-arrow">
          →
        </div>

        <div className="failover-failed-node">
          <span>FAILED NODE</span>
          <strong>
            Node{" "}
            {experiment.failed_node_id ??
              "—"}
          </strong>
          <small>
            intentionally stopped
          </small>
        </div>

        <div className="failover-arrow">
          →
        </div>

        <div>
          <span>NEW LEADER</span>
          <strong>
            Node{" "}
            {experiment.leader_after ??
              "—"}
          </strong>
          <small>
            Term{" "}
            {experiment.term_after ??
              "—"}
          </small>
        </div>

        <div className="failover-arrow">
          →
        </div>

        <div>
          <span>FAILOVER</span>
          <strong>
            {experiment.failover_ms
              ? `${number(
                  experiment.failover_ms /
                    1000,
                  2,
                )} s`
              : "—"}
          </strong>
          <small>
            election duration
          </small>
        </div>
      </div>
    </section>
  );
}

function ImpactMetric({
  label,
  normal,
  failure,
  unit,
  higherIsBetter,
}: {
  label: string;
  normal: number;
  failure: number;
  unit: string;
  higherIsBetter: boolean;
}) {
  const change =
    percentChange(
      normal,
      failure,
    );

  const degraded =
    higherIsBetter
      ? change < 0
      : change > 0;

  const improved =
    higherIsBetter
      ? change > 0
      : change < 0;

  return (
    <div className="fault-impact-row">
      <strong>{label}</strong>

      <span>
        {number(normal, 2)}
        {unit}
      </span>

      <span>
        {number(failure, 2)}
        {unit}
      </span>

      <span
        className={
          degraded
            ? "comparison-bad"
            : improved
              ? "comparison-good"
              : "comparison-neutral"
        }
      >
        {change > 0 ? "+" : ""}
        {number(change, 1)}%
      </span>
    </div>
  );
}

function FaultToleranceComparison({
  normal,
  failure,
}: {
  normal: BenchmarkJob;
  failure: BenchmarkJob;
}) {
  if (
    !normal.result ||
    !failure.result ||
    !failure.experiment
  ) {
    return null;
  }

  const baseline =
    normal.result;

  const failover =
    failure.result;

  const experiment =
    failure.experiment;

  return (
    <section className="panel fault-comparison">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">
            FAULT-TOLERANCE COST
          </div>

          <h2>
            Normal vs Leader Failure
          </h2>

          <p>
            Quantifies the availability
            and performance cost of a
            Raft leader failure.
          </p>
        </div>

        <div className="fault-comparison-summary">
          <span>
            FAILOVER
          </span>

          <strong>
            {number(
              (experiment.failover_ms ??
                0) / 1000,
              2,
            )} s
          </strong>
        </div>
      </div>

      <div className="fault-impact-table">
        <div className="fault-impact-head">
          <span>METRIC</span>
          <span>NORMAL</span>
          <span>FAILOVER</span>
          <span>IMPACT</span>
        </div>

        <ImpactMetric
          label="Throughput"
          normal={
            baseline.throughput_ops
          }
          failure={
            failover.throughput_ops
          }
          unit=" ops/s"
          higherIsBetter
        />

        <ImpactMetric
          label="Read p95"
          normal={
            baseline.read_latency
              .p95_ms
          }
          failure={
            failover.read_latency
              .p95_ms
          }
          unit=" ms"
          higherIsBetter={false}
        />

        <ImpactMetric
          label="Write p95"
          normal={
            baseline.write_latency
              .p95_ms
          }
          failure={
            failover.write_latency
              .p95_ms
          }
          unit=" ms"
          higherIsBetter={false}
        />

        <ImpactMetric
          label="Overall p99"
          normal={
            baseline.overall_latency
              .p99_ms
          }
          failure={
            failover.overall_latency
              .p99_ms
          }
          unit=" ms"
          higherIsBetter={false}
        />

        <ImpactMetric
          label="Success rate"
          normal={
            successRateFor(normal)
          }
          failure={
            successRateFor(failure)
          }
          unit="%"
          higherIsBetter
        />
      </div>

      <div className="fault-transition">
        <div>
          <span>
            LEADER TRANSITION
          </span>

          <strong>
            Node{" "}
            {experiment.leader_before ??
              "—"}
            {" → "}
            Node{" "}
            {experiment.leader_after ??
              "—"}
          </strong>
        </div>

        <div>
          <span>
            TERM
          </span>

          <strong>
            {experiment.term_before ??
              "—"}
            {" → "}
            {experiment.term_after ??
              "—"}
          </strong>
        </div>

        <div>
          <span>
            RECOVERY
          </span>

          <strong
            className={
              experiment.recovered
                ? "comparison-good"
                : "comparison-bad"
            }
          >
            {experiment.recovered
              ? "Successful"
              : "Failed"}
          </strong>
        </div>

        <div>
          <span>
            FAILED OPERATIONS
          </span>

          <strong>
            {failover.failed}
          </strong>
        </div>
      </div>
    </section>
  );
}

import {
  CheckCircle2,
  ChevronRight,
  CircleDot,
  FastForward,
  Pause,
  Play,
  RotateCcw,
  ShieldCheck,
  SkipBack,
  StepForward,
  Vote,
  Waypoints,
  WifiOff,
} from "lucide-react";
import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  CoverageMatrix,
} from "./CoverageMatrix";

import {
  TestExplorer,
} from "./TestExplorer";

import {
  getValidationRun,
  startValidationRun,
  type ValidationRun,
} from "../api/validation";

import {
  loadValidationReport,
} from "../validation/report";
import type {
  ValidationReport,
  ValidationSuite,
} from "../validation/types";

import {
  TEST_CATALOG,
  categoryLabel,
  type TestCategory,
  type TestTraceEvent,
  type VisualTestCase,
} from "../telemetry/testCatalog";

const ALL = "all";

type Filter = TestCategory | typeof ALL;

function eventIcon(
  event: TestTraceEvent,
) {
  switch (event.kind) {
    case "election":
    case "vote":
      return Vote;
    case "failure":
      return WifiOff;
    case "assertion":
      return ShieldCheck;
    default:
      return Waypoints;
  }
}

function eventTone(
  event: TestTraceEvent,
) {
  if (event.kind === "failure") {
    return "danger";
  }

  if (
    event.kind === "commit" ||
    event.kind === "apply" ||
    event.kind === "recovery" ||
    event.kind === "assertion"
  ) {
    return "success";
  }

  return "active";
}

function Node({
  id,
  test,
  current,
}: {
  id: number;
  test: VisualTestCase;
  current?: TestTraceEvent;
}) {
  const failed =
    current?.kind === "failure" &&
    current.nodeId === id;

  const leader =
    current?.nodeId === id &&
    (
      current.kind === "assertion" ||
      current.kind === "commit"
    );

  const involved =
    current?.nodeId === id ||
    current?.from === id ||
    current?.to === id;

  return (
    <div
      className={[
        "test-node",
        involved ? "active" : "",
        failed ? "failed" : "",
        leader ? "leader" : "",
      ].join(" ")}
    >
      <div className="test-node-orbit">
        <CircleDot size={18} />
      </div>

      <strong>Node {id}</strong>

      <span>
        {failed
          ? "OFFLINE"
          : leader
            ? "LEADER"
            : "RAFT NODE"}
      </span>

      <small>
        {test.category === "routing" && id === 2
          ? "Entry node"
          : "Replica"}
      </small>
    </div>
  );
}

export function TestLab() {
  const [validation, setValidation] =
    useState<ValidationReport | null>(null);

  const [validationLoading, setValidationLoading] =
    useState(true);

  const [resultSuite, setResultSuite] =
    useState<string>("all");

  const [filter, setFilter] =
    useState<Filter>(ALL);

  const [selectedId, setSelectedId] =
    useState(TEST_CATALOG[0].id);

  const [
    reportAvailable,
    setReportAvailable,
  ] = useState(false);

  const [
    reportLoadError,
    setReportLoadError,
  ] = useState("");

  const [
    validationRun,
    setValidationRun,
  ] = useState<ValidationRun>({
    state: "idle",
  });

  const [
    validationActionError,
    setValidationActionError,
  ] = useState("");

  const [step, setStep] = useState(0);
  const [playing, setPlaying] =
    useState(false);
  const [speed, setSpeed] = useState(1);

  const tests = useMemo(
    () =>
      filter === ALL
        ? TEST_CATALOG
        : TEST_CATALOG.filter(
            (test) =>
              test.category === filter,
          ),
    [filter],
  );

  const selected =
    TEST_CATALOG.find(
      (test) => test.id === selectedId,
    ) ?? TEST_CATALOG[0];

  const trace = selected.trace;

  const current =
    trace[
      Math.min(
        step,
        trace.length - 1,
      )
    ];

  const refreshValidationReport =
    async () => {
      const loaded =
        await loadValidationReport();

      setValidation(
        loaded.report,
      );

      setReportAvailable(
        loaded.reportAvailable,
      );

      setReportLoadError(
        loaded.error ?? "",
      );
    };

  useEffect(() => {
    const controller =
      new AbortController();

    setValidationLoading(true);

    loadValidationReport(
      controller.signal,
    )
      .then((loaded) => {
        setValidation(
          loaded.report,
        );

        setReportAvailable(
          loaded.reportAvailable,
        );

        setReportLoadError(
          loaded.error ?? "",
        );
      })
      .finally(() => {
        setValidationLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (
      validationRun.state !==
      "running"
    ) {
      return;
    }

    let cancelled = false;
    let timer: number | undefined;

    const pollValidationRun =
      async () => {
        try {
          const run =
            await getValidationRun();

          if (cancelled) {
            return;
          }

          setValidationRun(
            run,
          );

          if (
            run.state === "passed"
          ) {
            await refreshValidationReport();
            return;
          }

          if (
            run.state === "failed"
          ) {
            setValidationActionError(
              run.error ??
                "Validation failed.",
            );
            return;
          }

          timer =
            window.setTimeout(
              pollValidationRun,
              1000,
            );
        } catch (error) {
          if (cancelled) {
            return;
          }

          setValidationActionError(
            error instanceof Error
              ? error.message
              : "Unable to check validation status.",
          );

          timer =
            window.setTimeout(
              pollValidationRun,
              2000,
            );
        }
      };

    void pollValidationRun();

    return () => {
      cancelled = true;

      if (timer !== undefined) {
        window.clearTimeout(
          timer,
        );
      }
    };
  }, [validationRun.state]);

  useEffect(() => {
    setStep(0);
    setPlaying(false);
  }, [selectedId]);

  useEffect(() => {
    if (!playing) return;

    if (step >= trace.length - 1) {
      setPlaying(false);
      return;
    }

    const timer = window.setTimeout(
      () => {
        setStep((value) =>
          Math.min(
            value + 1,
            trace.length - 1,
          ),
        );
      },
      900 / speed,
    );

    return () =>
      window.clearTimeout(timer);
  }, [
    playing,
    speed,
    step,
    trace.length,
  ]);

  function openVisualization(
    visualizationId: string,
  ) {
    const test =
      TEST_CATALOG.find(
        (candidate) =>
          candidate.id ===
          visualizationId,
      );

    if (!test) {
      return;
    }

    setFilter(ALL);
    setSelectedId(test.id);
    setStep(0);
    setPlaying(false);

    window.requestAnimationFrame(
      () => {
        document
          .querySelector(
            ".test-lab-layout",
          )
          ?.scrollIntoView({
            behavior: "smooth",
            block: "start",
          });
      },
    );
  }

  function selectTest(
    test: VisualTestCase,
  ) {
    setSelectedId(test.id);
  }

  function reset() {
    setPlaying(false);
    setStep(0);
  }

  const progress =
    trace.length <= 1
      ? 100
      : (step /
          (trace.length - 1)) *
        100;

  const totalTests =
    validation?.total ?? 0;

  const passedTests =
    validation?.passed ?? 0;

  const failedTests =
    validation?.failed ?? 0;

  const passRate =
    totalTests === 0
      ? 0
      : Math.round(
          (passedTests / totalTests) *
            100,
        );

  const validationSuites =
    validation?.suites ?? [];

  const visibleSuites =
    resultSuite === "all"
      ? validationSuites
      : validationSuites.filter(
          (suite) =>
            suite.id === resultSuite,
        );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">
            VALIDATION
          </div>

          <h1>Test Lab</h1>

          <p>
            Explore what ShardRAFT's
            consensus, recovery, routing,
            and end-to-end tests prove.
          </p>
        </div>
      </header>

      <div className="test-summary-grid">
        <div className="panel test-summary-card">
          <span>REAL TESTS</span>
          <strong>
            {validationLoading
              ? "…"
              : reportAvailable
                ? totalTests
                : "—"}
          </strong>
          <small>
            Backend and frontend validation
          </small>
        </div>

        <div className="panel test-summary-card">
          <span>PASS RATE</span>
          <strong>
            {validationLoading
              ? "…"
              : reportAvailable
                ? `${passRate}%`
                : "—"}
          </strong>
          <small>
            {reportAvailable
              ? `${passedTests} passing`
              : "No report generated"}
          </small>
        </div>

        <div className="panel test-summary-card">
          <span>FAILED</span>
          <strong>
            {validationLoading
              ? "…"
              : reportAvailable
                ? failedTests
                : "—"}
          </strong>
          <small>Latest validation run</small>
        </div>

        <div className="panel test-summary-card">
          <span>VALIDATION</span>
          <strong>
            {validationLoading
              ? "…"
              : !reportAvailable
                ? "NOT GENERATED"
                : validation?.status ===
                    "passed"
                  ? "PASS"
                  : validation?.status ===
                      "failed"
                    ? "FAIL"
                    : "—"}
          </strong>
          <small>
            {validation?.git_commit
              ? `${validation.git_branch} · ${validation.git_commit}`
              : "Validation not generated"}
          </small>
        </div>
      </div>

      {!validationLoading &&
        !reportAvailable && (
          <section className="test-report-missing">
            <div className="test-report-missing-icon">
              !
            </div>

            <div>
              <strong>
                {validationRun.state ===
                "running"
                  ? "Validation in progress"
                  : "Validation report not generated"}
              </strong>

              <p>
                {validationRun.state ===
                "running"
                  ? "Running backend and frontend validation. You can continue exploring Test Lab while the checks complete."
                  : "Protocol visualizations are still available, but real validation results have not been generated for this checkout."}
              </p>

              <button
                type="button"
                className="test-run-validation"
                disabled={
                  validationRun.state ===
                  "running"
                }
                onClick={async () => {
                  try {
                    setValidationActionError(
                      "",
                    );

                    const run =
                      await startValidationRun();

                    setValidationRun(
                      run,
                    );
                  } catch (error) {
                    setValidationActionError(
                      error instanceof Error
                        ? error.message
                        : "Unable to start validation.",
                    );
                  }
                }}
              >
                {validationRun.state ===
                "running"
                  ? "Running validation..."
                  : "Run validation"}
              </button>

              <small>
                Runs backend and frontend
                validation in the background.
              </small>

              <code>
                CLI: make validation-standard
              </code>

              {validationActionError && (
                <small className="test-validation-action-error">
                  {validationActionError}
                </small>
              )}

              {reportLoadError && (
                <small>
                  {reportLoadError}
                </small>
              )}
            </div>
          </section>
        )}

      <section className="panel validation-results-panel">
        <div className="panel-heading">
          <div>
            <h2>Validation Results</h2>
            <p>
              Real results from the latest validation run
            </p>
          </div>

          <div
            className={
              validation?.status === "passed"
                ? "validation-status passed"
                : "validation-status failed"
            }
          >
            {validation?.status === "passed"
              ? "ALL SYSTEMS PASS"
              : validationLoading
                ? "LOADING"
                : "VALIDATION REQUIRED"}
          </div>
        </div>

        <div className="validation-suite-tabs">
          <button
            className={resultSuite === "all" ? "active" : ""}
            onClick={() => setResultSuite("all")}
          >
            All
          </button>

          {validationSuites.map((suite) => (
            <button
              key={suite.id}
              className={
                resultSuite === suite.id
                  ? "active"
                  : ""
              }
              onClick={() => setResultSuite(suite.id)}
            >
              {suite.name}
              <span>{suite.passed}</span>
            </button>
          ))}
        </div>

        <div className="validation-suite-grid">
          {visibleSuites.map((suite) => (
            <ValidationSuiteCard
              key={suite.id}
              suite={suite}
            />
          ))}
        </div>
      </section>

      <div className="test-protocol-heading">
        <div>
          <div className="eyebrow">
            PROTOCOL EXPLORER
          </div>
          <h2>Interactive Visualizations</h2>
          <p>
            Replay selected distributed-system
            guarantees step by step.
          </p>
        </div>

        <span>
          {TEST_CATALOG.length} visual scenarios
        </span>
      </div>

      <div className="test-filter-row">
        {[
          ALL,
          "election",
          "replication",
          "safety",
          "recovery",
          "routing",
          "e2e",
        ].map((value) => (
          <button
            key={value}
            className={
              filter === value
                ? "test-filter active"
                : "test-filter"
            }
            onClick={() =>
              setFilter(value as Filter)
            }
          >
            {value === ALL
              ? "All"
              : categoryLabel(
                  value as TestCategory,
                )}
          </button>
        ))}
      </div>

      <div className="test-lab-layout">
        <aside className="panel test-catalog">
          <div className="panel-heading">
            <div>
              <h2>Test Catalog</h2>
              <p>
                Select a scenario to inspect
              </p>
            </div>
          </div>

          <div className="test-catalog-list">
            {tests.map((test) => (
              <button
                key={test.id}
                className={
                  selected.id === test.id
                    ? "test-catalog-item active"
                    : "test-catalog-item"
                }
                onClick={() =>
                  selectTest(test)
                }
              >
                <CheckCircle2
                  size={17}
                  className="test-pass-icon"
                />

                <span>
                  <strong>{test.name}</strong>
                  <small>
                    {categoryLabel(
                      test.category,
                    )}
                  </small>
                </span>

                <ChevronRight size={16} />
              </button>
            ))}
          </div>
        </aside>

        <main className="test-workspace">
          <section className="panel test-detail-panel">
            <div className="test-detail-top">
              <div>
                <div className="test-detail-meta">
                  <span className="test-pass-badge">
                    <CheckCircle2 size={14} />
                    PASS
                  </span>

                  <span>
                    {categoryLabel(
                      selected.category,
                    )}
                  </span>
                </div>

                <h2>{selected.name}</h2>

                <code>{selected.source}</code>
              </div>

              <div className="test-step-counter">
                STEP
                <strong>
                  {step + 1}/{trace.length}
                </strong>
              </div>
            </div>

            <p className="test-purpose">
              {selected.purpose}
            </p>

            <div className="test-invariant">
              <ShieldCheck size={19} />

              <div>
                <span>
                  PROPERTY UNDER TEST
                </span>

                <strong>
                  {selected.invariant}
                </strong>
              </div>
            </div>
          </section>

          <section className="panel test-replay-panel">
            <div className="panel-heading">
              <div>
                <h2>Raft Replay</h2>
                <p>
                  Interactive explanatory trace
                </p>
              </div>

              <span className="test-time">
                +{current.at} ms
              </span>
            </div>

            <div className="test-topology">
              <div className="test-link test-link-left" />
              <div className="test-link test-link-right" />

              <Node
                id={1}
                test={selected}
                current={current}
              />

              <Node
                id={2}
                test={selected}
                current={current}
              />

              <Node
                id={3}
                test={selected}
                current={current}
              />

              {current.from &&
                current.to && (
                  <div className="test-flow-label">
                    Node {current.from}
                    {" → "}
                    Node {current.to}
                  </div>
                )}
            </div>

            <div
              className={`test-current-event ${eventTone(
                current,
              )}`}
            >
              {(() => {
                const Icon =
                  eventIcon(current);

                return <Icon size={20} />;
              })()}

              <div>
                <strong>
                  {current.title}
                </strong>
                <span>
                  {current.detail}
                </span>
              </div>
            </div>

            <div className="test-player">
              <button
                title="Reset"
                onClick={reset}
              >
                <RotateCcw size={17} />
              </button>

              <button
                title="Previous step"
                disabled={step === 0}
                onClick={() => {
                  setPlaying(false);
                  setStep((value) =>
                    Math.max(0, value - 1),
                  );
                }}
              >
                <SkipBack size={17} />
              </button>

              <button
                className="test-play-button"
                title={
                  playing ? "Pause" : "Play"
                }
                onClick={() => {
                  if (
                    step ===
                    trace.length - 1
                  ) {
                    setStep(0);
                  }

                  setPlaying(
                    (value) => !value,
                  );
                }}
              >
                {playing ? (
                  <Pause size={18} />
                ) : (
                  <Play size={18} />
                )}
              </button>

              <button
                title="Next step"
                disabled={
                  step === trace.length - 1
                }
                onClick={() => {
                  setPlaying(false);
                  setStep((value) =>
                    Math.min(
                      trace.length - 1,
                      value + 1,
                    ),
                  );
                }}
              >
                <StepForward size={17} />
              </button>

              <div className="test-progress">
                <div
                  style={{
                    width: `${progress}%`,
                  }}
                />
              </div>

              <button
                className="test-speed"
                onClick={() =>
                  setSpeed((value) =>
                    value === 1
                      ? 2
                      : value === 2
                        ? 0.5
                        : 1,
                  )
                }
              >
                <FastForward size={15} />
                {speed}×
              </button>
            </div>
          </section>

          <section className="panel test-timeline-panel">
            <div className="panel-heading">
              <div>
                <h2>Trace Timeline</h2>
                <p>
                  Select any event to jump
                  directly to it
                </p>
              </div>
            </div>

            <div className="test-timeline">
              {trace.map(
                (event, index) => {
                  const Icon =
                    eventIcon(event);

                  return (
                    <button
                      key={`${event.at}-${event.title}`}
                      className={[
                        "test-timeline-row",
                        index === step
                          ? "active"
                          : "",
                        index < step
                          ? "completed"
                          : "",
                      ].join(" ")}
                      onClick={() => {
                        setPlaying(false);
                        setStep(index);
                      }}
                    >
                      <span className="test-timeline-time">
                        +{event.at}ms
                      </span>

                      <span className="test-timeline-icon">
                        <Icon size={15} />
                      </span>

                      <span className="test-timeline-copy">
                        <strong>
                          {event.title}
                        </strong>
                        <small>
                          {event.detail}
                        </small>
                      </span>
                    </button>
                  );
                },
              )}
            </div>
          </section>
        </main>
      </div>

      <CoverageMatrix
        report={validation}
      />

      <TestExplorer
        suites={validationSuites}
        onVisualize={openVisualization}
      />
    </>
  );
}

function ValidationSuiteCard({
  suite,
}: {
  suite: ValidationSuite;
}) {
  const total =
    suite.passed +
    suite.failed +
    suite.skipped;

  return (
    <article className="validation-suite-card">
      <div className="validation-suite-head">
        <div>
          <span>{suite.kind.toUpperCase()}</span>
          <strong>{suite.name}</strong>
        </div>

        <div
          className={`validation-suite-state ${suite.status}`}
        >
          {suite.status.toUpperCase()}
        </div>
      </div>

      <div className="validation-suite-numbers">
        <div>
          <span>TESTS</span>
          <strong>{total}</strong>
        </div>

        <div>
          <span>PASS</span>
          <strong>{suite.passed}</strong>
        </div>

        <div>
          <span>FAIL</span>
          <strong>{suite.failed}</strong>
        </div>

        <div>
          <span>DURATION</span>
          <strong>
            {(suite.duration_ms / 1000).toFixed(2)}s
          </strong>
        </div>
      </div>

      <div className="validation-test-list">
        {suite.tests
          .slice(0, 8)
          .map((test) => (
            <div
              className="validation-test-row"
              key={`${test.package ?? ""}-${test.name}`}
            >
              <CheckCircle2 size={14} />

              <span title={test.name}>
                {test.name}
              </span>

              <small>
                {test.duration_ms.toFixed(1)}ms
              </small>
            </div>
          ))}

        {suite.tests.length > 8 && (
          <div className="validation-more">
            +{suite.tests.length - 8} more tests
          </div>
        )}
      </div>
    </article>
  );
}

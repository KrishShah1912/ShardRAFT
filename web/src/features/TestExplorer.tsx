import {
  CheckCircle2,
  Clock3,
  ExternalLink,
  Search,
  XCircle,
} from "lucide-react";
import {
  useMemo,
  useState,
} from "react";

import type {
  ValidationSuite,
  ValidationTest,
} from "../validation/types";

import {
  visualizationForTest,
} from "../validation/visualizationMap";

interface Props {
  suites: ValidationSuite[];
  onVisualize: (
    visualizationId: string,
  ) => void;
}

type SortMode =
  | "name"
  | "slowest"
  | "fastest";

interface TestRow {
  suite: ValidationSuite;
  test: ValidationTest;
}

function packageLabel(
  value?: string,
) {
  if (!value) {
    return "frontend";
  }

  const parts = value.split("/");

  return parts[
    parts.length - 1
  ] || value;
}

export function TestExplorer({
  suites,
  onVisualize,
}: Props) {
  const [expanded, setExpanded] =
    useState(false);

  const [query, setQuery] =
    useState("");

  const [suiteFilter, setSuiteFilter] =
    useState("all");

  const [statusFilter, setStatusFilter] =
    useState("all");

  const [sort, setSort] =
    useState<SortMode>("slowest");

  const [selected, setSelected] =
    useState<TestRow | null>(null);

  const allTests = useMemo<TestRow[]>(
    () =>
      suites.flatMap((suite) =>
        suite.tests.map((test) => ({
          suite,
          test,
        })),
      ),
    [suites],
  );

  const visible = useMemo(() => {
    const normalized =
      query.trim().toLowerCase();

    const rows = allTests.filter(
      ({ suite, test }) => {
        if (
          suiteFilter !== "all" &&
          suite.id !== suiteFilter
        ) {
          return false;
        }

        if (
          statusFilter !== "all" &&
          test.status !== statusFilter
        ) {
          return false;
        }

        if (!normalized) {
          return true;
        }

        return [
          test.name,
          test.package ?? "",
          suite.name,
        ].some((value) =>
          value
            .toLowerCase()
            .includes(normalized),
        );
      },
    );

    return [...rows].sort(
      (a, b) => {
        if (sort === "slowest") {
          return (
            b.test.duration_ms -
            a.test.duration_ms
          );
        }

        if (sort === "fastest") {
          return (
            a.test.duration_ms -
            b.test.duration_ms
          );
        }

        return a.test.name.localeCompare(
          b.test.name,
        );
      },
    );
  }, [
    allTests,
    query,
    sort,
    statusFilter,
    suiteFilter,
  ]);

  const visualizable =
    visible.filter(
      ({ test }) =>
        visualizationForTest(
          test.name,
        ) !== undefined,
    ).length;

  return (
    <section className="panel test-explorer">
      <div className="panel-heading test-explorer-heading">
        <div>
          <div className="eyebrow">
            COMPLETE VALIDATION
          </div>

          <h2>All Real Tests</h2>

          <p>
            {allTests.length} tests captured from
            backend, routing and end-to-end validation.
          </p>
        </div>

        <div className="test-explorer-heading-actions">
          <div className="test-explorer-count">
            <strong>{visible.length}</strong>
            <span>
              results · {visualizable} visualizable
            </span>
          </div>

          <button
            className="test-explorer-toggle"
            onClick={() =>
              setExpanded((value) => !value)
            }
          >
            {expanded
              ? "Collapse"
              : "Browse all tests"}
          </button>
        </div>
      </div>

      {expanded && (
        <>
          <div className="test-explorer-toolbar">
        <label className="test-search">
          <Search size={15} />

          <input
            value={query}
            onChange={(event) =>
              setQuery(
                event.target.value,
              )
            }
            placeholder="Search test, package, suite..."
          />
        </label>

        <select
          value={suiteFilter}
          onChange={(event) =>
            setSuiteFilter(
              event.target.value,
            )
          }
        >
          <option value="all">
            All suites
          </option>

          {suites.map((suite) => (
            <option
              value={suite.id}
              key={suite.id}
            >
              {suite.name}
            </option>
          ))}
        </select>

        <select
          value={statusFilter}
          onChange={(event) =>
            setStatusFilter(
              event.target.value,
            )
          }
        >
          <option value="all">
            All status
          </option>
          <option value="passed">
            Passed
          </option>
          <option value="failed">
            Failed
          </option>
          <option value="skipped">
            Skipped
          </option>
        </select>

        <select
          value={sort}
          onChange={(event) =>
            setSort(
              event.target
                .value as SortMode,
            )
          }
        >
          <option value="slowest">
            Slowest first
          </option>
          <option value="fastest">
            Fastest first
          </option>
          <option value="name">
            Name
          </option>
        </select>
      </div>

      <div className="test-explorer-table">
        <div className="test-explorer-header">
          <span>STATUS</span>
          <span>TEST</span>
          <span>SUITE</span>
          <span>PACKAGE</span>
          <span>DURATION</span>
          <span />
        </div>

        {visible
          .slice(0, 40)
          .map((row) => {
            const visualization =
              visualizationForTest(
                row.test.name,
              );

            return (
              <button
                className="test-explorer-row"
                key={`${row.suite.id}-${row.test.package ?? ""}-${row.test.name}`}
                onClick={() =>
                  setSelected(row)
                }
              >
                <span>
                  {row.test.status ===
                  "passed" ? (
                    <CheckCircle2
                      size={15}
                      className="validation-pass"
                    />
                  ) : (
                    <XCircle
                      size={15}
                      className="validation-fail"
                    />
                  )}
                </span>

                <strong>
                  {row.test.name}
                </strong>

                <span>
                  {row.suite.name}
                </span>

                <code>
                  {packageLabel(
                    row.test.package,
                  )}
                </code>

                <span className="test-duration">
                  <Clock3 size={13} />
                  {row.test.duration_ms
                    .toFixed(1)}
                  ms
                </span>

                <span>
                  {visualization && (
                    <span className="visualizable-badge">
                      VISUAL
                    </span>
                  )}
                </span>
              </button>
            );
          })}
      </div>

      {visible.length > 40 && (
        <div className="test-explorer-limit">
          Showing first 40 of{" "}
          {visible.length} results.
          Refine your search to narrow
          the list.
        </div>
      )}

          {selected && (
        <div className="test-inspector">
          <div className="test-inspector-top">
            <div>
              <span>
                {selected.suite.name}
              </span>

              <h3>
                {selected.test.name}
              </h3>
            </div>

            <button
              onClick={() =>
                setSelected(null)
              }
            >
              ×
            </button>
          </div>

          <div className="test-inspector-grid">
            <div>
              <span>STATUS</span>
              <strong>
                {selected.test.status}
              </strong>
            </div>

            <div>
              <span>DURATION</span>
              <strong>
                {selected.test.duration_ms
                  .toFixed(3)}
                ms
              </strong>
            </div>

            <div>
              <span>TYPE</span>
              <strong>
                {selected.suite.kind}
              </strong>
            </div>

            <div>
              <span>PACKAGE</span>
              <strong>
                {packageLabel(
                  selected.test.package,
                )}
              </strong>
            </div>
          </div>

          {selected.test.package && (
            <code className="test-package-path">
              {selected.test.package}
            </code>
          )}

          {visualizationForTest(
            selected.test.name,
          ) && (
            <button
              className="test-visualize-action"
              onClick={() => {
                const id =
                  visualizationForTest(
                    selected.test.name,
                  );

                if (id) {
                  onVisualize(id);
                  setSelected(null);
                }
              }}
            >
              <ExternalLink size={15} />
              Open protocol visualization
            </button>
          )}
        </div>
          )}
        </>
      )}
    </section>
  );
}

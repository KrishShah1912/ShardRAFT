import {
  AlertTriangle,
  Check,
  Circle,
  ShieldCheck,
} from "lucide-react";

import type {
  ValidationReport,
} from "../validation/types";

import {
  buildCoverage,
  type CoverageLevel,
} from "../validation/coverage";

interface Props {
  report: ValidationReport | null;
  onSearchTests?: (
    query: string,
  ) => void;
}

function levelLabel(
  level: CoverageLevel,
) {
  switch (level) {
    case "strong":
      return "STRONG";
    case "covered":
      return "COVERED";
    case "partial":
      return "PARTIAL";
    case "gap":
      return "GAP";
    case "planned":
      return "PLANNED";
  }
}

function CoverageMark({
  value,
}: {
  value: boolean;
}) {
  return value ? (
    <span className="coverage-mark yes">
      <Check size={13} />
    </span>
  ) : (
    <span className="coverage-mark no">
      <Circle size={8} />
    </span>
  );
}

export function CoverageMatrix({
  report,
  onSearchTests,
}: Props) {
  const coverage =
    buildCoverage(report);

  const strong =
    coverage.filter(
      (item) =>
        item.level === "strong",
    ).length;

  const gaps =
    coverage.filter(
      (item) =>
        item.level === "gap" ||
        item.level === "planned",
    ).length;

  return (
    <section className="panel coverage-panel">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">
            SYSTEM GUARANTEES
          </div>

          <h2>Coverage & Invariants</h2>

          <p>
            Test evidence mapped to
            distributed-system capabilities
            and known architectural gaps.
          </p>
        </div>

        <div className="coverage-summary">
          <div>
            <ShieldCheck size={15} />
            <strong>{strong}</strong>
            <span>strong</span>
          </div>

          <div>
            <AlertTriangle size={15} />
            <strong>{gaps}</strong>
            <span>gaps / planned</span>
          </div>
        </div>
      </div>

      <div className="coverage-table">
        <div className="coverage-header">
          <span>CAPABILITY</span>
          <span>TESTS</span>
          <span>UNIT</span>
          <span>INTEGRATION</span>
          <span>E2E</span>
          <span>FAULT</span>
          <span>STATUS</span>
        </div>

        {coverage.map((item) => (
          <button
            className="coverage-row"
            key={item.id}
            disabled={
              item.testCount === 0 ||
              !onSearchTests
            }
            onClick={() =>
              onSearchTests?.(
                item.name,
              )
            }
          >
            <span className="coverage-capability">
              <strong>
                {item.name}
              </strong>

              <small>
                {item.description}
              </small>
            </span>

            <span className="coverage-count">
              {item.testCount || "—"}
            </span>

            <CoverageMark
              value={item.unit}
            />

            <CoverageMark
              value={item.integration}
            />

            <CoverageMark
              value={item.e2e}
            />

            <CoverageMark
              value={item.fault}
            />

            <span
              className={`coverage-level ${item.level}`}
            >
              {levelLabel(
                item.level,
              )}
            </span>
          </button>
        ))}
      </div>

      <div className="coverage-footnote">
        <span>
          Coverage counts are derived
          from the latest validation report.
        </span>

        <span>
          GAP and PLANNED rows represent
          documented architectural
          limitations, not failed tests.
        </span>
      </div>
    </section>
  );
}

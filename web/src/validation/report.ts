import type {
  ValidationReport,
  ValidationSuite,
  ValidationTest,
} from "./types";

export interface ValidationReportLoadResult {
  report: ValidationReport;
  reportAvailable: boolean;
  error?: string;
}

const EMPTY_REPORT: ValidationReport = {
  schema_version: 1,
  generated_at: "",
  git_commit: "",
  git_branch: "",
  duration_ms: 0,
  status: "unknown",
  total: 0,
  passed: 0,
  failed: 0,
  skipped: 0,
  suites: [],
};

function isTest(
  value: unknown,
): value is ValidationTest {
  if (
    typeof value !== "object" ||
    value === null
  ) {
    return false;
  }

  const test =
    value as Partial<ValidationTest>;

  return (
    typeof test.name === "string" &&
    typeof test.status === "string" &&
    typeof test.duration_ms === "number"
  );
}

function isSuite(
  value: unknown,
): value is ValidationSuite {
  if (
    typeof value !== "object" ||
    value === null
  ) {
    return false;
  }

  const suite =
    value as Partial<ValidationSuite>;

  return (
    typeof suite.id === "string" &&
    typeof suite.name === "string" &&
    typeof suite.kind === "string" &&
    typeof suite.status === "string" &&
    typeof suite.duration_ms === "number" &&
    typeof suite.passed === "number" &&
    typeof suite.failed === "number" &&
    typeof suite.skipped === "number" &&
    Array.isArray(suite.tests) &&
    suite.tests.every(isTest)
  );
}

export function isValidationReport(
  value: unknown,
): value is ValidationReport {
  if (
    typeof value !== "object" ||
    value === null
  ) {
    return false;
  }

  const report =
    value as Partial<ValidationReport>;

  return (
    report.schema_version === 1 &&
    typeof report.generated_at === "string" &&
    typeof report.git_commit === "string" &&
    typeof report.git_branch === "string" &&
    typeof report.duration_ms === "number" &&
    typeof report.status === "string" &&
    typeof report.total === "number" &&
    typeof report.passed === "number" &&
    typeof report.failed === "number" &&
    typeof report.skipped === "number" &&
    Array.isArray(report.suites) &&
    report.suites.every(isSuite)
  );
}

export async function loadValidationReport(
  signal?: AbortSignal,
): Promise<ValidationReportLoadResult> {
  try {
    const response = await fetch(
      "/generated/validation-report.json",
      {
        cache: "no-store",
        signal,
      },
    );

    if (!response.ok) {
      return {
        report: EMPTY_REPORT,
        reportAvailable: false,
        error:
          `Validation report unavailable (HTTP ${response.status}).`,
      };
    }

    const value: unknown =
      await response.json();

    if (!isValidationReport(value)) {
      return {
        report: EMPTY_REPORT,
        reportAvailable: false,
        error:
          "Validation report has an invalid format.",
      };
    }

    return {
      report: value,
      reportAvailable: true,
    };
  } catch (error) {
    if (
      error instanceof DOMException &&
      error.name === "AbortError"
    ) {
      return {
        report: EMPTY_REPORT,
        reportAvailable: false,
      };
    }

    return {
      report: EMPTY_REPORT,
      reportAvailable: false,
      error:
        error instanceof Error
          ? error.message
          : "Unable to load validation report.",
    };
  }
}

export function emptyValidationReport() {
  return EMPTY_REPORT;
}

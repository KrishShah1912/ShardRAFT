export type ValidationStatus =
  | "passed"
  | "failed"
  | "skipped"
  | "unknown";

export type ValidationSuiteKind =
  | "go"
  | "frontend"
  | "e2e";

export interface ValidationTest {
  name: string;
  package?: string;
  status: ValidationStatus;
  duration_ms: number;
  output?: string;
}

export interface ValidationSuite {
  id: string;
  name: string;
  kind: ValidationSuiteKind;
  command: string;
  status: ValidationStatus;
  duration_ms: number;
  passed: number;
  failed: number;
  skipped: number;
  tests: ValidationTest[];
}

export interface ValidationReport {
  schema_version: 1;
  generated_at: string;
  git_commit: string;
  git_branch: string;
  duration_ms: number;
  status: ValidationStatus;
  total: number;
  passed: number;
  failed: number;
  skipped: number;
  suites: ValidationSuite[];
}

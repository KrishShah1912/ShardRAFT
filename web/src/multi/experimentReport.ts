export type ExperimentRow = Record<string, string>;

export interface ExperimentDataset {
  source: string;
  columns: string[];
  rows: ExperimentRow[];
  row_count: number;
}

export interface ExperimentReport {
  schema_version: 1;
  generated_at: string;
  mode: "historical";
  description: string;
  datasets: {
    performance: ExperimentDataset;
    faults: ExperimentDataset;
    wal: ExperimentDataset;
  };
  summary: {
    experiments: number;
    requests: number;
    successful: number;
    failed: number;
    unknown: number;
    pending: number;
    linearizable_runs: number;
  };
}

export async function loadExperimentReport(
  signal?: AbortSignal,
): Promise<ExperimentReport> {
  const response = await fetch(
    "/generated/multi-raft-experiments.json",
    {
      cache: "no-store",
      signal,
    },
  );

  if (!response.ok) {
    throw new Error(
      `Experiment report unavailable: HTTP ${response.status}`,
    );
  }

  const report =
    await response.json() as ExperimentReport;

  if (
    report.schema_version !== 1 ||
    report.mode !== "historical" ||
    !report.summary ||
    !report.datasets
  ) {
    throw new Error(
      "Unsupported experiment report format",
    );
  }

  for (const name of [
    "performance",
    "faults",
    "wal",
  ] as const) {
    const dataset = report.datasets[name];

    if (
      !dataset ||
      !Array.isArray(dataset.rows) ||
      !Array.isArray(dataset.columns) ||
      dataset.rows.length === 0 ||
      dataset.rows.length !== dataset.row_count
    ) {
      throw new Error(
        `Invalid experiment dataset: ${name}`,
      );
    }
  }

  return report;
}

export type BenchmarkState =
  | "idle"
  | "running"
  | "passed"
  | "failed";

export interface BenchmarkConfig {
  requests: number;
  concurrency: number;
  read_ratio: number;
  value_size: number;

  mode?: "normal" | "leader-failure";

  failure_delay_ms?: number;
}

export interface BenchmarkLatency {
  average_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  max_ms: number;
}

export interface BenchmarkNode {
  id: number;
  role: number;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
}

export interface BenchmarkResult {
  started_at: string;
  ended_at: string;

  requests: number;
  concurrency: number;
  read_ratio: number;
  value_size: number;

  successful: number;
  failed: number;
  reads: number;
  writes: number;

  duration_ms: number;
  throughput_ops: number;

  overall_latency: BenchmarkLatency;
  read_latency: BenchmarkLatency;
  write_latency: BenchmarkLatency;

  status_codes: Record<string, number>;
  errors: Record<string, number>;

  initial_leader: BenchmarkNode;
  final_leader: BenchmarkNode;
}

export interface BenchmarkExperiment {
  mode: string;

  failed_node_id?: number;

  leader_before?: number;
  leader_after?: number;

  term_before?: number;
  term_after?: number;

  failure_started_at?: string;
  leader_elected_at?: string;
  node_restarted_at?: string;

  failover_ms?: number;

  recovered: boolean;
}

export interface BenchmarkJob {
  id?: string;
  state: BenchmarkState;

  started_at?: string;
  ended_at?: string;

  config?: BenchmarkConfig;

  result?: BenchmarkResult;

  experiment?: BenchmarkExperiment;

  error?: string;
}

export interface BenchmarkHistory {
  runs: BenchmarkJob[];
}

async function readError(
  response: Response,
) {
  const message =
    await response.text();

  return (
    message.trim() ||
    `HTTP ${response.status}`
  );
}

export async function startBenchmark(
  config: BenchmarkConfig,
): Promise<BenchmarkJob> {
  const response = await fetch(
    "/control/api/v1/benchmarks",
    {
      method: "POST",
      headers: {
        "Content-Type":
          "application/json",
      },
      body: JSON.stringify(config),
    },
  );

  if (!response.ok) {
    throw new Error(
      await readError(response),
    );
  }

  return response.json();
}

export async function getCurrentBenchmark():
  Promise<BenchmarkJob> {
  const response = await fetch(
    "/control/api/v1/benchmarks/current",
    {
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error(
      await readError(response),
    );
  }

  return response.json();
}

export async function getBenchmarkHistory():
  Promise<BenchmarkHistory> {
  const response = await fetch(
    "/control/api/v1/benchmarks/history",
    {
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error(
      await readError(response),
    );
  }

  return response.json();
}

import {
  previewKeyRouting,
  type RoutingMap,
} from "./keyRouting";

import type {
  GatewayCommand,
} from "../api/multiGateway";

import type {
  DataShardID,
} from "../api/multiDiagnostics";

export interface DemoWorkloadConfig {
  shardId: DataShardID;
  count: number;
  intervalMs: number;
}

export interface DemoOperation {
  sequence: number;
  shardId: DataShardID;
  slot: number;
  hash: number;
  command: GatewayCommand;
}

export interface DemoWorkload {
  runId: string;
  epoch: number;
  config: DemoWorkloadConfig;
  operations: DemoOperation[];
}

export const DEFAULT_DEMO_CONFIG:
  DemoWorkloadConfig = {
    shardId: 1,
    count: 10,
    intervalMs: 2000,
  };

export const DEMO_OPERATION_COUNTS =
  [5, 10, 15, 20] as const;

export const DEMO_INTERVALS =
  [1000, 2000, 3000, 5000] as const;

const MAX_KEY_ATTEMPTS = 100000;

function validateConfig(
  config: DemoWorkloadConfig,
): void {
  if (![1, 2, 3, 4].includes(config.shardId)) {
    throw new Error("Invalid target shard.");
  }

  if (
    !Number.isInteger(config.count) ||
    config.count < 1 ||
    config.count > 20
  ) {
    throw new Error(
      "Operation count must be between 1 and 20.",
    );
  }

  if (
    !Number.isInteger(config.intervalMs) ||
    config.intervalMs < 1000 ||
    config.intervalMs > 5000
  ) {
    throw new Error(
      "Interval must be between 1000 and 5000 ms.",
    );
  }
}

export function createDemoWorkload(
  config: DemoWorkloadConfig,
  map: RoutingMap,
  runId = crypto.randomUUID(),
): DemoWorkload {
  validateConfig(config);

  if (!/^[a-zA-Z0-9_-]{1,100}$/.test(runId)) {
    throw new Error("Invalid demonstration run ID.");
  }

  if (map.total_slots !== 65536) {
    throw new Error("Unexpected metadata slot count.");
  }

  const targets = map.shards.filter(
    shard => shard.shard_id === config.shardId,
  );

  if (targets.length !== 1) {
    throw new Error(
      "Target shard is missing or ambiguous.",
    );
  }

  const operations: DemoOperation[] = [];

  const clientId = `demo-${runId}`;

  for (
    let candidate = 0;
    candidate < MAX_KEY_ATTEMPTS &&
      operations.length < config.count;
    candidate++
  ) {
    const key =
      `demo-${runId}-s${config.shardId}-${candidate}`;

    const route = previewKeyRouting(key, map);

    if (route.shardId !== config.shardId) {
      continue;
    }

    const sequence = operations.length + 1;

    operations.push({
      sequence,
      shardId: config.shardId,
      slot: route.slot,
      hash: route.hash,
      command: {
        operation: "PUT",
        key,
        value: `demo-value-${runId}-${sequence}`,
        expected: "",
        clientId,
        requestId: `write-${sequence}`,
        readMode: "linearizable",
      },
    });
  }

  if (operations.length !== config.count) {
    throw new Error(
      "Unable to generate enough keys for the target shard.",
    );
  }

  return {
    runId,
    epoch: map.epoch,
    config: { ...config },
    operations,
  };
}

export function validateDemoRoute(
  operation: DemoOperation,
  map: RoutingMap,
): void {
  const route = previewKeyRouting(
    operation.command.key,
    map,
  );

  if (
    route.shardId !== operation.shardId ||
    route.slot !== operation.slot ||
    route.hash !== operation.hash
  ) {
    throw new Error(
      "Metadata routing changed. Stop and prepare a new workload.",
    );
  }
}

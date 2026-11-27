import {
  executeGatewayCommand,
  GatewayRequestError,
} from "../api/multiGateway";

import type {
  GatewayResult,
  GatewayCommand,
} from "../api/multiGateway";

import type {
  GatewayID,
} from "../api/multiRaft";

import type {
  RoutingMap,
} from "./keyRouting";

import {
  validateDemoRoute,
  type DemoOperation,
  type DemoWorkload,
} from "./demoWorkload";

export type DemoOperationStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "unknown"
  | "skipped";

export type DemoRunStatus =
  | "idle"
  | "running"
  | "stopping"
  | "completed"
  | "stopped"
  | "failed";

export interface DemoOperationResult {
  operation: DemoOperation;
  status: DemoOperationStatus;
  startedAt: number | null;
  finishedAt: number | null;
  gatewayId: GatewayID | null;
  durationMs: number | null;
  error: string | null;
  response: GatewayResult | null;
}

export interface DemoExecutionState {
  runId: string;
  status: DemoRunStatus;
  startedAt: number | null;
  finishedAt: number | null;
  operations: DemoOperationResult[];
  error: string | null;
}

export interface DemoExecutionOptions {
  getRoutingMap: () => RoutingMap | null;
  onUpdate: (state: DemoExecutionState) => void;
  preferredGateway?: GatewayID;
  automaticFailover?: boolean;
  execute?: (
    command: GatewayCommand,
    gateway: GatewayID,
    failover: boolean,
  ) => Promise<GatewayResult>;
}

export interface DemoExecutionHandle {
  stop: () => void;
  done: Promise<DemoExecutionState>;
}

export function createDemoExecutionState(
  workload: DemoWorkload,
): DemoExecutionState {
  return {
    runId: workload.runId,
    status: "idle",
    startedAt: null,
    finishedAt: null,
    error: null,
    operations: workload.operations.map(operation => ({
      operation,
      status: "pending",
      startedAt: null,
      finishedAt: null,
      gatewayId: null,
      durationMs: null,
      error: null,
      response: null,
    })),
  };
}

export function summarizeDemoExecution(
  state: DemoExecutionState,
) {
  const count = (status: DemoOperationStatus) =>
    state.operations.filter(
      operation => operation.status === status,
    ).length;

  const successful = count("success");
  const failed = count("failed");
  const unknown = count("unknown");
  const skipped = count("skipped");

  return {
    total: state.operations.length,
    successful,
    failed,
    unknown,
    skipped,
    pending: count("pending"),
    running: count("running"),
    completed: successful + failed + unknown,
    settled: successful + failed + unknown + skipped,
  };
}

export function startDemoExecution(
  workload: DemoWorkload,
  options: DemoExecutionOptions,
): DemoExecutionHandle {
  let state = createDemoExecutionState(workload);

  let stopRequested = false;
  let finished = false;

  let delayTimer:
    ReturnType<typeof setTimeout> | null = null;

  let releaseDelay: (() => void) | null = null;

  const execute =
    options.execute ?? executeGatewayCommand;

  function publish(
    changes: Partial<DemoExecutionState>,
  ) {
    state = {
      ...state,
      ...changes,
    };

    options.onUpdate(state);
  }

  function updateOperation(
    index: number,
    changes: Partial<DemoOperationResult>,
  ) {
    publish({
      operations: state.operations.map(
        (operation, position) =>
          position === index
            ? { ...operation, ...changes }
            : operation,
      ),
    });
  }

  function waitForNextWrite(): Promise<void> {
    if (stopRequested) {
      return Promise.resolve();
    }

    return new Promise(resolve => {
      releaseDelay = () => {
        if (delayTimer !== null) {
          clearTimeout(delayTimer);
        }

        delayTimer = null;
        releaseDelay = null;
        resolve();
      };

      delayTimer = setTimeout(
        () => releaseDelay?.(),
        workload.config.intervalMs,
      );
    });
  }

  function stop() {
    if (finished || stopRequested) return;

    stopRequested = true;

    publish({
      status: "stopping",
    });

    releaseDelay?.();
  }

  async function run(): Promise<DemoExecutionState> {
    publish({
      status: "running",
      startedAt: Date.now(),
    });

    let terminalError: string | null = null;

    try {
      for (
        let index = 0;
        index < workload.operations.length;
        index++
      ) {
        if (stopRequested) break;

        const operation = workload.operations[index];

        const map = options.getRoutingMap();

        if (!map) {
          throw new Error(
            "Metadata map unavailable. Workload stopped.",
          );
        }

        validateDemoRoute(operation, map);

        const startedAt = Date.now();
        const started = performance.now();

        updateOperation(index, {
          status: "running",
          startedAt,
        });

        try {
          const result = await execute(
            operation.command,
            options.preferredGateway ?? 1,
            options.automaticFailover ?? true,
          );

          const body = result.result as {
            applied?: boolean;
          } | null;

          if (body?.applied !== true) {
            throw new GatewayRequestError(
              "PUT application could not be confirmed.",
              "outcome_unknown",
              result.status,
              result.attempts,
            );
          }

          updateOperation(index, {
            status: "success",
            finishedAt: Date.now(),
            gatewayId: result.gatewayId,
            durationMs: result.durationMs,
            response: result,
          });
        } catch (failure) {
          const known =
            failure instanceof GatewayRequestError;

          const unknown =
            !known ||
            failure.code === "outcome_unknown";

          const message =
            failure instanceof Error
              ? failure.message
              : String(failure);

          updateOperation(index, {
            status: unknown ? "unknown" : "failed",
            finishedAt: Date.now(),
            durationMs: performance.now() - started,
            error: message,
          });

          if (unknown) {
            terminalError =
              "Mutation outcome uncertain. " +
              "Remaining writes were not executed.";

            break;
          }
        }

        if (
          !stopRequested &&
          index < workload.operations.length - 1
        ) {
          await waitForNextWrite();
        }
      }
    } catch (failure) {
      terminalError =
        failure instanceof Error
          ? failure.message
          : String(failure);
    } finally {
      finished = true;

      releaseDelay?.();

      publish({
        status: terminalError
          ? "failed"
          : stopRequested
            ? "stopped"
            : "completed",

        finishedAt: Date.now(),

        error: terminalError,

        operations: state.operations.map(operation =>
          operation.status === "pending"
            ? {
                ...operation,
                status: "skipped",
              }
            : operation,
        ),
      });
    }

    return state;
  }

  const done = Promise.resolve().then(run);

  return {
    stop,
    done,
  };
}

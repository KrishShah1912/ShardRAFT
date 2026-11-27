import test from "node:test";
import assert from "node:assert/strict";

import {
  createDemoWorkload,
} from "../src/multi/demoWorkload.ts";

import {
  createDemoExecutionState,
  summarizeDemoExecution,
  startDemoExecution,
} from "../src/multi/demoExecution.ts";

import {
  GatewayRequestError,
} from "../src/api/multiGateway.ts";

const map = {
  epoch: 1,
  total_slots: 65536,
  shards: [1, 2, 3, 4].map(shard_id => ({
    shard_id,
    slot_ranges: [{
      start_slot: (shard_id - 1) * 16384,
      end_slot: shard_id * 16384 - 1,
    }],
    replicas: [
      [1, 2, 3],
      [2, 3, 4],
      [3, 4, 5],
      [1, 4, 5],
    ][shard_id - 1].map(node_id => ({ node_id })),
  })),
};

function workload(count = 3) {
  const result = createDemoWorkload(
    {
      shardId: 2,
      count,
      intervalMs: 1000,
    },
    map,
  );

  // Short test-only interval.
  // Production configuration remains unchanged.
  result.config.intervalMs = 5;

  return result;
}

function success(command, gatewayId = 1) {
  return {
    gatewayId,
    status: 200,
    durationMs: 1,
    attempts: [],
    result: {
      applied: true,
      value: command.value,
      client_id: command.clientId,
      request_id: command.requestId,
    },
  };
}

function deferred() {
  let resolve;

  const promise = new Promise(done => {
    resolve = done;
  });

  return { promise, resolve };
}

function options(overrides = {}) {
  return {
    getRoutingMap: () => map,
    onUpdate: () => {},
    execute: async command => success(command),
    ...overrides,
  };
}

test("Initial execution state contains pending operations", () => {
  const state = createDemoExecutionState(workload());

  assert.equal(state.status, "idle");
  assert.equal(state.startedAt, null);
  assert.equal(state.finishedAt, null);

  const summary = summarizeDemoExecution(state);

  assert.equal(summary.total, 3);
  assert.equal(summary.pending, 3);
  assert.equal(summary.completed, 0);
});

test("All successful operations complete", async () => {
  const handle = startDemoExecution(
    workload(),
    options(),
  );

  const state = await handle.done;
  const summary = summarizeDemoExecution(state);

  assert.equal(state.status, "completed");
  assert.equal(summary.successful, 3);
  assert.equal(summary.failed, 0);
  assert.equal(summary.unknown, 0);
  assert.equal(summary.pending, 0);
  assert.equal(summary.completed, 3);
  assert.equal(summary.settled, 3);
});

test("Operations execute sequentially", async () => {
  let active = 0;
  let peak = 0;

  const seen = [];

  const run = workload(5);

  const handle = startDemoExecution(
    run,
    options({
      execute: async command => {
        active++;
        peak = Math.max(peak, active);

        seen.push(command.requestId);

        await new Promise(resolve =>
          setTimeout(resolve, 2),
        );

        active--;

        return success(command);
      },
    }),
  );

  const state = await handle.done;

  assert.equal(state.status, "completed");
  assert.equal(peak, 1);

  assert.deepEqual(
    seen,
    run.operations.map(op => op.command.requestId),
  );
});

test("Original mutation identities are preserved", async () => {
  const run = workload();

  const seen = [];

  const handle = startDemoExecution(
    run,
    options({
      execute: async command => {
        seen.push({ ...command });
        return success(command);
      },
    }),
  );

  await handle.done;

  assert.deepEqual(
    seen,
    run.operations.map(op => op.command),
  );
});

test("Gateway preference and failover are forwarded", async () => {
  const handle = startDemoExecution(
    workload(1),
    options({
      preferredGateway: 2,
      automaticFailover: false,

      execute: async (command, gateway, failover) => {
        assert.equal(gateway, 2);
        assert.equal(failover, false);

        return success(command, gateway);
      },
    }),
  );

  const state = await handle.done;

  assert.equal(state.status, "completed");
  assert.equal(state.operations[0].gatewayId, 2);
  assert.equal(state.operations[0].status, "success");
});

test("Default gateway and automatic failover are forwarded", async () => {
  const handle = startDemoExecution(
    workload(1),
    options({
      execute: async (command, gateway, failover) => {
        assert.equal(gateway, 1);
        assert.equal(failover, true);

        return success(command);
      },
    }),
  );

  const state = await handle.done;

  assert.equal(state.operations[0].status, "success");
});

test("Known failure is recorded and execution continues", async () => {
  let calls = 0;

  const handle = startDemoExecution(
    workload(),
    options({
      execute: async command => {
        calls++;

        if (calls === 1) {
          throw new GatewayRequestError(
            "Invalid request",
            "invalid_request",
            400,
            [],
          );
        }

        return success(command);
      },
    }),
  );

  const state = await handle.done;
  const summary = summarizeDemoExecution(state);

  assert.equal(calls, 3);
  assert.equal(state.status, "completed");
  assert.equal(summary.failed, 1);
  assert.equal(summary.successful, 2);
  assert.equal(summary.unknown, 0);
});

test("Unknown mutation outcome halts remaining writes", async () => {
  let calls = 0;

  const handle = startDemoExecution(
    workload(),
    options({
      execute: async () => {
        calls++;

        throw new GatewayRequestError(
          "Commit uncertain",
          "outcome_unknown",
          504,
          [],
        );
      },
    }),
  );

  const state = await handle.done;
  const summary = summarizeDemoExecution(state);

  assert.equal(calls, 1);
  assert.equal(state.status, "failed");
  assert.equal(summary.unknown, 1);
  assert.equal(summary.skipped, 2);
  assert.equal(summary.successful, 0);
  assert.ok(state.error);
});

test("Unexpected transport error is treated as unknown", async () => {
  const handle = startDemoExecution(
    workload(),
    options({
      execute: async () => {
        throw new Error("Connection interrupted");
      },
    }),
  );

  const state = await handle.done;

  assert.equal(state.status, "failed");
  assert.equal(state.operations[0].status, "unknown");
  assert.equal(state.operations[1].status, "skipped");
});

test("Unconfirmed PUT application is treated as unknown", async () => {
  const handle = startDemoExecution(
    workload(),
    options({
      execute: async command => ({
        ...success(command),
        result: { applied: false },
      }),
    }),
  );

  const state = await handle.done;

  assert.equal(state.status, "failed");
  assert.equal(state.operations[0].status, "unknown");
  assert.equal(state.operations[1].status, "skipped");
});

test("Missing metadata prevents database execution", async () => {
  let calls = 0;

  const handle = startDemoExecution(
    workload(),
    options({
      getRoutingMap: () => null,

      execute: async command => {
        calls++;
        return success(command);
      },
    }),
  );

  const state = await handle.done;

  assert.equal(calls, 0);
  assert.equal(state.status, "failed");
  assert.equal(
    summarizeDemoExecution(state).skipped,
    3,
  );
});

test("Changed routing prevents subsequent writes", async () => {
  let calls = 0;

  const changed = structuredClone(map);

  changed.epoch = 2;
  changed.shards[1].slot_ranges = [];
  changed.shards[0].slot_ranges.push({
    start_slot: 16384,
    end_slot: 32767,
  });

  const handle = startDemoExecution(
    workload(),
    options({
      getRoutingMap: () =>
        calls === 0 ? map : changed,

      execute: async command => {
        calls++;
        return success(command);
      },
    }),
  );

  const state = await handle.done;

  assert.equal(calls, 1);
  assert.equal(state.status, "failed");
  assert.equal(state.operations[0].status, "success");
  assert.equal(state.operations[1].status, "skipped");
});

test("Stop before execution prevents every write", async () => {
  let calls = 0;

  const handle = startDemoExecution(
    workload(),
    options({
      execute: async command => {
        calls++;
        return success(command);
      },
    }),
  );

  handle.stop();

  const state = await handle.done;

  assert.equal(calls, 0);
  assert.equal(state.status, "stopped");
  assert.equal(
    summarizeDemoExecution(state).skipped,
    3,
  );
});

test("Stop allows an in-flight write to settle", async () => {
  const entered = deferred();
  const release = deferred();

  let calls = 0;

  const handle = startDemoExecution(
    workload(),
    options({
      execute: async command => {
        calls++;

        entered.resolve();

        await release.promise;

        return success(command);
      },
    }),
  );

  await entered.promise;

  handle.stop();
  release.resolve();

  const state = await handle.done;

  assert.equal(calls, 1);
  assert.equal(state.status, "stopped");
  assert.equal(state.operations[0].status, "success");
  assert.equal(state.operations[1].status, "skipped");
  assert.equal(state.operations[2].status, "skipped");
});

test("Stop interrupts the delay between writes", async () => {
  const firstFinished = deferred();

  const run = workload();
  run.config.intervalMs = 5000;

  let calls = 0;

  const handle = startDemoExecution(
    run,
    options({
      onUpdate: state => {
        if (state.operations[0].status === "success") {
          firstFinished.resolve();
        }
      },

      execute: async command => {
        calls++;
        return success(command);
      },
    }),
  );

  await firstFinished.promise;

  handle.stop();

  const state = await handle.done;

  assert.equal(calls, 1);
  assert.equal(state.status, "stopped");
  assert.equal(
    summarizeDemoExecution(state).skipped,
    2,
  );
});

test("Repeated stop calls are safe", async () => {
  const handle = startDemoExecution(
    workload(),
    options(),
  );

  handle.stop();
  handle.stop();
  handle.stop();

  const state = await handle.done;

  assert.equal(state.status, "stopped");

  assert.doesNotThrow(() => handle.stop());
});

test("State updates preserve previous operation snapshots", async () => {
  const snapshots = [];

  const handle = startDemoExecution(
    workload(1),
    options({
      onUpdate: state => snapshots.push(state),
    }),
  );

  const final = await handle.done;

  assert.equal(snapshots[0].status, "running");
  assert.equal(
    snapshots[0].operations[0].status,
    "pending",
  );

  assert.ok(
    snapshots.some(
      state => state.operations[0].status === "running",
    ),
  );

  assert.equal(final.status, "completed");
  assert.equal(final.operations[0].status, "success");
  assert.ok(final.startedAt !== null);
  assert.ok(final.finishedAt >= final.startedAt);
});

test("Final summary accounts for every operation", async () => {
  const handle = startDemoExecution(
    workload(5),
    options(),
  );

  const state = await handle.done;
  const summary = summarizeDemoExecution(state);

  assert.equal(
    summary.successful +
    summary.failed +
    summary.unknown +
    summary.skipped +
    summary.pending +
    summary.running,
    summary.total,
  );

  assert.equal(summary.settled, summary.total);
  assert.equal(summary.pending, 0);
  assert.equal(summary.running, 0);
});

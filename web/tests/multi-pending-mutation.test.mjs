import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const {
  parsePendingMutation,
  loadPendingMutation,
  savePendingMutation,
  clearPendingMutation,
} = require(
  "../.test-build/multi/pendingMutation.js"
);

const command = {
  operation: "CAS",
  key: "demo:retry",
  value: "new-value",
  expected: "old-value",
  clientId: "test-client",
  requestId: "test-request",
  readMode: "linearizable",
};

const entries = new Map();

Object.defineProperty(
  globalThis,
  "sessionStorage",
  {
    configurable: true,
    value: {
      getItem(key) {
        return entries.get(key) ?? null;
      },
      setItem(key, value) {
        entries.set(key, String(value));
      },
      removeItem(key) {
        entries.delete(key);
      },
    },
  },
);

test("missing pending mutation returns null", () => {
  assert.equal(parsePendingMutation(null), null);
});

test("CAS payload survives serialization", () => {
  assert.deepEqual(
    parsePendingMutation(JSON.stringify(command)),
    command,
  );
});

test("PUT and DELETE identities are preserved", () => {
  for (const operation of ["PUT", "DELETE"]) {
    const input = { ...command, operation };

    assert.deepEqual(
      parsePendingMutation(JSON.stringify(input)),
      input,
    );
  }
});

test("GET cannot be stored as a pending mutation", () => {
  assert.throws(() =>
    savePendingMutation({
      ...command,
      operation: "GET",
    }),
  );
});

test("missing request identity is rejected", () => {
  assert.throws(() =>
    parsePendingMutation(
      JSON.stringify({
        ...command,
        requestId: "",
      }),
    ),
  );
});

test("missing client identity is rejected", () => {
  assert.throws(() =>
    parsePendingMutation(
      JSON.stringify({
        ...command,
        clientId: "",
      }),
    ),
  );
});

test("invalid JSON is rejected", () => {
  assert.throws(() =>
    parsePendingMutation("{invalid"),
  );
});

test("stored mutation restores exact original payload", () => {
  entries.clear();

  savePendingMutation(command);

  assert.deepEqual(
    loadPendingMutation(),
    command,
  );
});

test("explicit clearing removes persisted mutation", () => {
  savePendingMutation(command);

  clearPendingMutation();

  assert.equal(loadPendingMutation(), null);
});

test("storage write failure is surfaced", () => {
  const original = sessionStorage.setItem;

  sessionStorage.setItem = () => {
    throw new Error("Storage unavailable");
  };

  try {
    assert.throws(
      () => savePendingMutation(command),
      /Storage unavailable/,
    );
  } finally {
    sessionStorage.setItem = original;
  }
});

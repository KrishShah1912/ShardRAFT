import assert from "node:assert/strict";
import { test, afterEach } from "node:test";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const {
  executeRoutedKV,
  selectEntryNodes,
  KVRoutingError,
} = require("../.test-build/api/routing.js");

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function node(id, reachable = true) {
  return {
    id,
    role: 0,
    term: 5,
    leader_id: 2,
    commit_index: 10,
    applied_index: 10,
    reachable,
  };
}

function cluster() {
  return [node(1), node(2), node(3)];
}

function command(operation = "GET", overrides = {}) {
  return {
    operation,
    key: "demo:language",
    value: "Python",
    expected: "Go",
    clientId: "frontend-test-client",
    requestId: "frontend-test-request",
    ...overrides,
  };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

function unavailable() {
  return jsonResponse(
    {
      error: "leader unavailable",
      code: "leader_unavailable",
    },
    503,
  );
}

function unknownOutcome() {
  return jsonResponse(
    {
      error: "commit not confirmed",
      code: "outcome_unknown",
    },
    504,
  );
}

function mockFetch(...responses) {
  const calls = [];

  globalThis.fetch = async (url, options = {}) => {
    calls.push({
      url: String(url),
      method: options.method,
      body: options.body,
      signal: options.signal,
    });

    const response = responses[calls.length - 1];

    if (response === undefined) {
      throw new Error("Unexpected fetch attempt");
    }

    if (response instanceof Error) {
      throw response;
    }

    if (typeof response === "function") {
      return response(url, options);
    }

    return response;
  };

  return calls;
}

async function expectRoutingError(
  promise,
  code,
  status,
) {
  await assert.rejects(
    promise,
    (error) => {
      assert.ok(error instanceof KVRoutingError);
      assert.equal(error.code, code);
      assert.equal(error.status, status);
      return true;
    },
  );
}

// T1: Reachable nodes receive priority.
test("T1: reachable entry nodes are prioritized", () => {
  const nodes = [
    node(1, false),
    node(2, true),
    node(3, true),
  ];

  assert.deepEqual(
    selectEntryNodes(nodes),
    [2, 3, 1],
  );
});

// T2: Duplicate node IDs are removed.
test("T2: duplicate entry nodes are removed", () => {
  const nodes = [
    node(1, false),
    node(2, true),
    node(2, true),
    node(3, true),
    node(1, true),
  ];

  assert.deepEqual(
    selectEntryNodes(nodes),
    [2, 3, 1],
  );
});

// T3: Successful GET.
test("T3: GET returns the server result", async () => {
  const expected = {
    key: "demo:language",
    value: "Go",
  };

  const calls = mockFetch(
    jsonResponse(expected),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("GET"),
  );

  assert.deepEqual(result.result, expected);
  assert.equal(result.entryNodeId, 1);
  assert.equal(result.attempts, 1);

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
  assert.equal(calls[0].body, undefined);
  assert.equal(
    calls[0].url,
    "/node1/kv/demo%3Alanguage",
  );
});

// T4: Successful PUT.
test("T4: PUT preserves value and identity", async () => {
  const calls = mockFetch(
    jsonResponse({
      applied: true,
      value: "Python",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("PUT"),
  );

  assert.deepEqual(result.result, {
    applied: true,
    value: "Python",
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "PUT");

  assert.deepEqual(
    JSON.parse(calls[0].body),
    {
      value: "Python",
      client_id: "frontend-test-client",
      request_id: "frontend-test-request",
    },
  );
});

// T5: Successful DELETE.
test("T5: DELETE preserves mutation identity", async () => {
  const calls = mockFetch(
    jsonResponse({ applied: true }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("DELETE"),
  );

  assert.deepEqual(
    result.result,
    { applied: true },
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "DELETE");

  assert.deepEqual(
    JSON.parse(calls[0].body),
    {
      client_id: "frontend-test-client",
      request_id: "frontend-test-request",
    },
  );
});

// T6: Successful CAS.
test("T6: CAS uses the correct endpoint and payload", async () => {
  const calls = mockFetch(
    jsonResponse({
      applied: true,
      value: "Python",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("CAS"),
  );

  assert.deepEqual(result.result, {
    applied: true,
    value: "Python",
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");

  assert.equal(
    calls[0].url,
    "/node1/kv/demo%3Alanguage/cas",
  );

  assert.deepEqual(
    JSON.parse(calls[0].body),
    {
      expected: "Go",
      value: "Python",
      client_id: "frontend-test-client",
      request_id: "frontend-test-request",
    },
  );
});

// T7: Entry-node failure.
test("T7: unreachable entry node triggers fallback", async () => {
  const calls = mockFetch(
    new TypeError("Failed to fetch"),
    jsonResponse({
      key: "demo:language",
      value: "Go",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("GET"),
  );

  assert.equal(result.entryNodeId, 2);
  assert.equal(result.attempts, 2);
  assert.equal(calls.length, 2);

  assert.deepEqual(
    calls.map((call) => call.url),
    [
      "/node1/kv/demo%3Alanguage",
      "/node2/kv/demo%3Alanguage",
    ],
  );
});

// T8: Request identity survives retries.
test("T8: mutation payload remains identical across retries", async () => {
  const calls = mockFetch(
    unknownOutcome(),
    unavailable(),
    jsonResponse({
      applied: true,
      value: "Python",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("PUT"),
  );

  assert.equal(result.entryNodeId, 3);
  assert.equal(result.attempts, 3);
  assert.equal(calls.length, 3);

  for (const call of calls) {
    assert.equal(call.method, "PUT");
    assert.equal(call.body, calls[0].body);
  }

  assert.deepEqual(
    JSON.parse(calls[0].body),
    {
      value: "Python",
      client_id: "frontend-test-client",
      request_id: "frontend-test-request",
    },
  );
});

// T9: Conflict is definitive.
test("T9: HTTP 409 stops automatic retries", async () => {
  const calls = mockFetch(
    jsonResponse(
      { error: "request conflict" },
      409,
    ),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("PUT"),
    ),
    "request_failed",
    409,
  );

  assert.equal(calls.length, 1);
});

// T10: Invalid request is definitive.
test("T10: HTTP 400 stops automatic retries", async () => {
  const calls = mockFetch(
    jsonResponse(
      { error: "invalid request" },
      400,
    ),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("PUT"),
    ),
    "request_failed",
    400,
  );

  assert.equal(calls.length, 1);
});

// T11: Missing key is definitive.
test("T11: HTTP 404 stops automatic retries", async () => {
  const calls = mockFetch(
    jsonResponse(
      { error: "key not found" },
      404,
    ),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("GET"),
    ),
    "request_failed",
    404,
  );

  assert.equal(calls.length, 1);
});

// T12: Preserve uncertain mutation outcomes.
test("T12: unknown outcome survives later unavailability", async () => {
  const calls = mockFetch(
    unknownOutcome(),
    unavailable(),
    unavailable(),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("PUT"),
    ),
    "outcome_unknown",
    504,
  );

  assert.equal(calls.length, 3);
});

// T13: Failed CAS is a definitive result.
test("T13: failed CAS does not trigger retries", async () => {
  const calls = mockFetch(
    jsonResponse({ applied: false }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("CAS"),
  );

  assert.deepEqual(
    result.result,
    { applied: false },
  );

  assert.equal(result.attempts, 1);
  assert.equal(calls.length, 1);
});

// T14: Retry exhaustion.
test("T14: maximum retry limit is enforced", async () => {
  const calls = mockFetch(
    unavailable(),
    unavailable(),
    unavailable(),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("GET"),
    ),
    "leader_unavailable",
    503,
  );

  assert.equal(calls.length, 3);
});

// T15: Empty cluster.
test("T15: empty cluster is rejected without network calls", async () => {
  const calls = mockFetch();

  await expectRoutingError(
    executeRoutedKV(
      [],
      command("GET"),
    ),
    "leader_unavailable",
    503,
  );

  assert.equal(calls.length, 0);
});

// T16: Mutation identity validation.
test("T16: missing mutation identity is rejected", async () => {
  const calls = mockFetch();

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("PUT", {
        requestId: "",
      }),
    ),
    "request_failed",
    400,
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("DELETE", {
        clientId: "",
      }),
    ),
    "request_failed",
    400,
  );

  assert.equal(calls.length, 0);
});

// T17: Invalid response.
test("T17: invalid JSON response triggers fallback", async () => {
  const calls = mockFetch(
    new Response("invalid JSON", {
      status: 200,
    }),
    jsonResponse({
      key: "demo:language",
      value: "Go",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("GET"),
  );

  assert.equal(result.entryNodeId, 2);
  assert.equal(result.attempts, 2);
  assert.equal(calls.length, 2);
});

// T18: Actual configured attempt timeout.
test(
  "T18: request timeout aborts fetch and triggers fallback",
  { timeout: 20000 },
  async () => {
    let aborted = false;

    const calls = mockFetch(
      (_url, options) =>
        new Promise((_resolve, reject) => {
          options.signal.addEventListener(
            "abort",
            () => {
              aborted = true;

              reject(
                new DOMException(
                  "Request aborted",
                  "AbortError",
                ),
              );
            },
            { once: true },
          );
        }),
      jsonResponse({
        key: "demo:language",
        value: "Go",
      }),
    );

    const start = Date.now();

    const result = await executeRoutedKV(
      cluster(),
      command("GET"),
    );

    const elapsed = Date.now() - start;

    assert.equal(aborted, true);
    assert.equal(calls.length, 2);
    assert.equal(result.entryNodeId, 2);
    assert.equal(result.attempts, 2);

    assert.ok(
      elapsed >= 11500,
      `Timeout occurred too early: ${elapsed}ms`,
    );

    assert.ok(
      elapsed < 19000,
      `Timeout exceeded tolerance: ${elapsed}ms`,
    );
  },
);

// T19: Unknown outcome followed by conflict.
test("T19: later conflict does not erase earlier uncertainty", async () => {
  const calls = mockFetch(
    unknownOutcome(),
    jsonResponse(
      { error: "request conflict" },
      409,
    ),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("CAS"),
    ),
    "outcome_unknown",
    504,
  );

  assert.equal(calls.length, 2);
});

// T20: Empty key validation.
test("T20: empty key is rejected before sending a request", async () => {
  const calls = mockFetch();

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("GET", {
        key: "   ",
      }),
    ),
    "request_failed",
    400,
  );

  assert.equal(calls.length, 0);
});

// T21: Successful retry after uncertain mutation.
test("T21: successful retry resolves an unknown outcome", async () => {
  const calls = mockFetch(
    new TypeError("Connection interrupted"),
    jsonResponse({
      applied: true,
      value: "Python",
    }),
  );

  const result = await executeRoutedKV(
    cluster(),
    command("CAS"),
  );

  assert.deepEqual(result.result, {
    applied: true,
    value: "Python",
  });

  assert.equal(result.attempts, 2);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].body, calls[1].body);
});

// T22: Invalid mutation response remains uncertain.
test("T22: invalid mutation response preserves uncertainty", async () => {
  const calls = mockFetch(
    new Response("invalid JSON", {
      status: 200,
    }),
    unavailable(),
    unavailable(),
  );

  await expectRoutingError(
    executeRoutedKV(
      cluster(),
      command("DELETE"),
    ),
    "outcome_unknown",
    504,
  );

  assert.equal(calls.length, 3);
});

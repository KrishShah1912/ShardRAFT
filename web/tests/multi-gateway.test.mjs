import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const {
  buildGatewayRequest,
  executeGatewayCommand,
} = require("../.test-build/api/multiGateway.js");

function command(operation = "PUT") {
  return {
    operation,
    key: "demo:user",
    value: "new-value",
    expected: "old-value",
    clientId: "test-client",
    requestId: "test-request",
    readMode: "linearizable",
  };
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

function mutationResult(applied = true) {
  return {
    applied,
    value: "new-value",
    client_id: "test-client",
    request_id: "test-request",
  };
}

test("GET uses linearizable read mode", () => {
  const request = buildGatewayRequest(command("GET"));

  assert.equal(request.options.method, "GET");
  assert.equal(
    request.path,
    "/kv/demo%3Auser?read_mode=linearizable",
  );
});

test("GET supports local reads", () => {
  const request = buildGatewayRequest({
    ...command("GET"),
    readMode: "local",
  });

  assert.ok(request.path.endsWith("read_mode=local"));
});

test("PUT includes value and mutation identity", () => {
  const request = buildGatewayRequest(command());

  assert.deepEqual(JSON.parse(request.options.body), {
    client_id: "test-client",
    request_id: "test-request",
    value: "new-value",
  });
});

test("DELETE excludes value and expected", () => {
  const request = buildGatewayRequest(command("DELETE"));

  assert.deepEqual(JSON.parse(request.options.body), {
    client_id: "test-client",
    request_id: "test-request",
  });
});

test("CAS uses POST and expected value", () => {
  const request = buildGatewayRequest(command("CAS"));

  assert.equal(request.options.method, "POST");
  assert.ok(request.path.endsWith("/cas"));

  assert.equal(
    JSON.parse(request.options.body).expected,
    "old-value",
  );
});

test("empty keys and missing mutation identities are rejected", () => {
  assert.throws(() =>
    buildGatewayRequest({
      ...command(),
      key: "",
    }),
  );

  assert.throws(() =>
    buildGatewayRequest({
      ...command(),
      requestId: "",
    }),
  );
});

test("gateway failover preserves original mutation identity", async t => {
  const requests = [];

  t.mock.method(globalThis, "fetch", async (url, options) => {
    requests.push({
      url,
      body: options.body,
    });

    if (requests.length === 1) {
      throw new Error("Gateway 1 unreachable");
    }

    return json(mutationResult());
  });

  const result = await executeGatewayCommand(command());

  assert.equal(result.gatewayId, 2);
  assert.equal(result.attempts.length, 2);

  assert.ok(requests[0].url.startsWith("/multi/gateway1"));
  assert.ok(requests[1].url.startsWith("/multi/gateway2"));

  assert.equal(requests[0].body, requests[1].body);
});

test("missing GET key is a valid not-found result", async t => {
  t.mock.method(globalThis, "fetch", async () =>
    json({
      error: "key not found",
      code: "key_not_found",
    }, 404),
  );

  const result = await executeGatewayCommand(command("GET"));

  assert.equal(result.result.found, false);
  assert.equal(result.attempts.length, 1);
});

test("CAS mismatch is not a transport failure", async t => {
  t.mock.method(globalThis, "fetch", async () =>
    json(mutationResult(false)),
  );

  const result = await executeGatewayCommand(command("CAS"));

  assert.equal(result.result.applied, false);
});

test("request conflict is not automatically retried", async t => {
  const mock = t.mock.method(globalThis, "fetch", async () =>
    json({
      error: "request identity conflict",
      code: "request_conflict",
    }, 409),
  );

  await assert.rejects(
    executeGatewayCommand(command()),
    error => error.code === "request_conflict",
  );

  assert.equal(mock.mock.callCount(), 1);
});

test("unknown mutation outcome remains unresolved", async t => {
  t.mock.method(globalThis, "fetch", async () =>
    json({
      error: "commit could not be confirmed",
      code: "outcome_unknown",
    }, 504),
  );

  await assert.rejects(
    executeGatewayCommand(command()),
    error =>
      error.code === "outcome_unknown" &&
      error.attempts.length === 2,
  );
});

test("manual gateway selection disables failover", async t => {
  const mock = t.mock.method(globalThis, "fetch", async () => {
    throw new Error("Gateway unavailable");
  });

  await assert.rejects(
    executeGatewayCommand(command(), 2, false),
    error => error.code === "outcome_unknown",
  );

  assert.equal(mock.mock.callCount(), 1);
});

test("malformed mutation success is not trusted", async t => {
  t.mock.method(globalThis, "fetch", async () =>
    json({ applied: true }),
  );

  await assert.rejects(
    executeGatewayCommand(command()),
    error => error.code === "outcome_unknown",
  );
});

test("later rejection does not erase earlier unknown outcome", async t => {
  let calls = 0;

  t.mock.method(globalThis, "fetch", async () => {
    calls++;

    if (calls === 1) {
      throw new Error("Connection lost");
    }

    return json({
      error: "request conflict",
      code: "request_conflict",
    }, 409);
  });

  await assert.rejects(
    executeGatewayCommand(command()),
    error => error.code === "outcome_unknown",
  );
});

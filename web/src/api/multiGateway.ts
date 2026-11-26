import {
  multiGatewayBase,
  type GatewayID,
} from "./multiRaft";

import type { KVOperation } from "./routing";

export type ReadMode = "linearizable" | "local";

export interface GatewayCommand {
  operation: KVOperation;
  key: string;
  value: string;
  expected: string;
  clientId: string;
  requestId: string;
  readMode: ReadMode;
}

export interface GatewayAttempt {
  gatewayId: GatewayID;
  status: number | null;
  durationMs: number;
  outcome: string;
}

export interface GatewayResult {
  gatewayId: GatewayID;
  status: number;
  result: unknown;
  attempts: GatewayAttempt[];
  durationMs: number;
}

export class GatewayRequestError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly status: number | null,
    public readonly attempts: GatewayAttempt[],
  ) {
    super(message);
    this.name = "GatewayRequestError";
  }
}

export function buildGatewayRequest(
  command: GatewayCommand,
): { path: string; options: RequestInit } {
  if (
    !command.key.trim() ||
    command.key === "." ||
    command.key === ".."
  ) {
    throw new GatewayRequestError(
      "A valid key is required.",
      "invalid_request",
      400,
      [],
    );
  }

  const path = `/kv/${encodeURIComponent(command.key)}`;

  if (command.operation === "GET") {
    return {
      path: `${path}?read_mode=${command.readMode}`,
      options: { method: "GET" },
    };
  }

  if (!command.clientId || !command.requestId) {
    throw new GatewayRequestError(
      "Mutation identity is required.",
      "invalid_request",
      400,
      [],
    );
  }

  const body: Record<string, string> = {
    client_id: command.clientId,
    request_id: command.requestId,
  };

  if (
    command.operation === "PUT" ||
    command.operation === "CAS"
  ) {
    body.value = command.value;
  }

  if (command.operation === "CAS") {
    body.expected = command.expected;
  }

  return {
    path: command.operation === "CAS"
      ? `${path}/cas`
      : path,

    options: {
      method: command.operation === "CAS"
        ? "POST"
        : command.operation,

      headers: {
        "Content-Type": "application/json",
      },

      body: JSON.stringify(body),
    },
  };
}

function validSuccess(
  command: GatewayCommand,
  body: Record<string, unknown>,
): boolean {
  if (command.operation === "GET") {
    return (
      body.key === command.key &&
      body.found === true &&
      typeof body.value === "string"
    );
  }

  return (
    typeof body.applied === "boolean" &&
    typeof body.value === "string" &&
    body.client_id === command.clientId &&
    body.request_id === command.requestId
  );
}

export async function executeGatewayCommand(
  command: GatewayCommand,
  preferredGateway: GatewayID = 1,
  automaticFailover = true,
): Promise<GatewayResult> {
  const request = buildGatewayRequest(command);

  const mutation = command.operation !== "GET";

  const gateways: GatewayID[] = automaticFailover
    ? [
        preferredGateway,
        preferredGateway === 1 ? 2 : 1,
      ]
    : [preferredGateway];

  const attempts: GatewayAttempt[] = [];
  const started = performance.now();

  let unknownOutcome = false;
  let lastMessage = "Gateway unavailable.";
  let lastStatus: number | null = null;

  for (const gatewayId of gateways) {
    const attemptStarted = performance.now();

    const controller = new AbortController();

    const timer = setTimeout(
      () => controller.abort(),
      13000,
    );

    let status: number | null = null;

    try {
      const response = await fetch(
        `${multiGatewayBase(gatewayId)}${request.path}`,
        {
          ...request.options,
          cache: "no-store",
          signal: controller.signal,
        },
      );

      status = response.status;
      lastStatus = status;

      const text = await response.text();

      let body: Record<string, unknown> = {};

      try {
        const parsed: unknown = JSON.parse(text);

        if (
          typeof parsed === "object" &&
          parsed !== null &&
          !Array.isArray(parsed)
        ) {
          body = parsed as Record<string, unknown>;
        }
      } catch {
        // Non-JSON responses are handled below.
      }

      const code =
        typeof body.code === "string"
          ? body.code
          : "";

      const missingKey =
        command.operation === "GET" &&
        status === 404 &&
        code === "key_not_found";

      const success =
        response.ok && validSuccess(command, body);

      if (success || missingKey) {
        attempts.push({
          gatewayId,
          status,
          durationMs: performance.now() - attemptStarted,
          outcome: missingKey ? "not_found" : "success",
        });

        return {
          gatewayId,
          status,
          result: missingKey
            ? {
                key: command.key,
                found: false,
                code: "key_not_found",
              }
            : body,
          attempts,
          durationMs: performance.now() - started,
        };
      }

      lastMessage =
        typeof body.error === "string"
          ? body.error
          : response.ok
            ? "Invalid gateway response."
            : `HTTP ${status}`;

      attempts.push({
        gatewayId,
        status,
        durationMs: performance.now() - attemptStarted,
        outcome: code || "invalid_response",
      });

      const retryable =
        response.ok ||
        status >= 500 ||
        status === 408;

      if (!retryable) {
        throw new GatewayRequestError(
          unknownOutcome
            ? "An earlier mutation may have committed."
            : lastMessage,
          unknownOutcome
            ? "outcome_unknown"
            : code || "request_failed",
          unknownOutcome ? 504 : status,
          attempts,
        );
      }

      if (mutation) {
        unknownOutcome = true;
      }
    } catch (error) {
      if (error instanceof GatewayRequestError) {
        throw error;
      }

      if (mutation) {
        unknownOutcome = true;
      }

      lastMessage =
        error instanceof Error
          ? error.message
          : String(error);

      attempts.push({
        gatewayId,
        status,
        durationMs: performance.now() - attemptStarted,
        outcome: "transport_error",
      });
    } finally {
      clearTimeout(timer);
    }
  }

  throw new GatewayRequestError(
    unknownOutcome
      ? "Commit could not be confirmed. Retry using the original request identity."
      : lastMessage,
    unknownOutcome
      ? "outcome_unknown"
      : "leader_unavailable",
    unknownOutcome ? 504 : lastStatus,
    attempts,
  );
}

import { nodeBase } from "./client";
import type { NodeStatus } from "../types";

export type KVOperation = "GET" | "PUT" | "DELETE" | "CAS";

export interface RoutedKVRequest {
  operation: KVOperation;
  key: string;
  value: string;
  expected: string;
  clientId: string;
  requestId: string;
}

export interface RoutedKVResult {
  result: unknown;
  entryNodeId: number;
  attempts: number;
}

export type RoutingErrorCode =
  | "leader_unavailable"
  | "outcome_unknown"
  | "request_failed";

export class KVRoutingError extends Error {
  constructor(
    message: string,
    public readonly code: RoutingErrorCode,
    public readonly status?: number,
  ) {
    super(message);
    this.name = "KVRoutingError";
  }
}

const ATTEMPT_TIMEOUT_MS = 12000;
const TOTAL_TIMEOUT_MS = 30000;
const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 150;

export function selectEntryNodes(
  nodes: NodeStatus[],
): number[] {
  const reachable = nodes
    .filter((node) => node.reachable !== false)
    .map((node) => node.id);

  const remaining = nodes.map((node) => node.id);

  return [...new Set([...reachable, ...remaining])];
}

function buildRequest(
  command: RoutedKVRequest,
): RequestInit {
  const identity = {
    client_id: command.clientId,
    request_id: command.requestId,
  };

  const headers = {
    "Content-Type": "application/json",
  };

  switch (command.operation) {
    case "PUT":
      return {
        method: "PUT",
        headers,
        body: JSON.stringify({
          ...identity,
          value: command.value,
        }),
      };

    case "DELETE":
      return {
        method: "DELETE",
        headers,
        body: JSON.stringify(identity),
      };

    case "CAS":
      return {
        method: "POST",
        headers,
        body: JSON.stringify({
          ...identity,
          expected: command.expected,
          value: command.value,
        }),
      };

    default:
      return { method: "GET" };
  }
}

function parseResponse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

function responseCode(value: unknown): string {
  if (
    typeof value === "object" &&
    value !== null &&
    "code" in value &&
    typeof value.code === "string"
  ) {
    return value.code;
  }

  return "";
}

function responseMessage(
  value: unknown,
  fallback: string,
): string {
  if (
    typeof value === "object" &&
    value !== null &&
    "error" in value &&
    typeof value.error === "string"
  ) {
    return value.error;
  }

  return fallback;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function executeRoutedKV(
  nodes: NodeStatus[],
  command: RoutedKVRequest,
): Promise<RoutedKVResult> {
  const candidates = selectEntryNodes(nodes);
  const mutation = command.operation !== "GET";

  if (!command.key.trim()) {
    throw new KVRoutingError(
      "A key is required.",
      "request_failed",
      400,
    );
  }

  if (
    mutation &&
    (!command.clientId || !command.requestId)
  ) {
    throw new KVRoutingError(
      "Mutation identity is required.",
      "request_failed",
      400,
    );
  }

  if (candidates.length === 0) {
    throw new KVRoutingError(
      "No cluster entry nodes are available.",
      "leader_unavailable",
      503,
    );
  }

  const options = buildRequest(command);

  const suffix =
    `/kv/${encodeURIComponent(command.key)}` +
    (command.operation === "CAS" ? "/cas" : "");

  const deadline = Date.now() + TOTAL_TIMEOUT_MS;

  let unknownOutcome = false;
  let lastMessage = "No leader is currently available.";

  for (
    let attempt = 0;
    attempt < MAX_ATTEMPTS;
    attempt++
  ) {
    const remaining = deadline - Date.now();

    if (remaining <= 0) break;

    const nodeId =
      candidates[attempt % candidates.length];

    const controller = new AbortController();

    const timer = setTimeout(
      () => controller.abort(),
      Math.min(remaining, ATTEMPT_TIMEOUT_MS),
    );

    try {
      const response = await fetch(
        `${nodeBase(nodeId)}${suffix}`,
        {
          ...options,
          signal: controller.signal,
        },
      );

      const text = await response.text();
      const result = parseResponse(text);
      const code = responseCode(result);

      if (response.ok) {
        if (result === undefined) {
          if (mutation) unknownOutcome = true;
          lastMessage = "Invalid server response.";
        } else {
          return {
            result,
            entryNodeId: nodeId,
            attempts: attempt + 1,
          };
        }
      } else {
        lastMessage = responseMessage(
          result,
          text || `HTTP ${response.status}`,
        );

        const knownUnavailable =
          response.status === 503 &&
          (
            code === "not_leader" ||
            code === "leader_unavailable"
          );

        const retryable =
          response.status >= 500 ||
          response.status === 408;

        if (!retryable) {
          if (mutation && unknownOutcome) {
            throw new KVRoutingError(
              "An earlier attempt may have committed. " +
                "The mutation outcome remains unknown.",
              "outcome_unknown",
              504,
            );
          }

          throw new KVRoutingError(
            lastMessage,
            "request_failed",
            response.status,
          );
        }

        if (mutation && !knownUnavailable) {
          unknownOutcome = true;
        }
      }
    } catch (error) {
      if (error instanceof KVRoutingError) {
        throw error;
      }

      if (mutation) unknownOutcome = true;

      lastMessage =
        error instanceof Error
          ? error.message
          : "Entry node request failed.";
    } finally {
      clearTimeout(timer);
    }

    if (attempt + 1 < MAX_ATTEMPTS) {
      const remaining = deadline - Date.now();

      if (remaining <= 0) break;

      await delay(
        Math.min(
          remaining,
          RETRY_DELAY_MS * 2 ** attempt,
        ),
      );
    }
  }

  if (mutation && unknownOutcome) {
    throw new KVRoutingError(
      "Commit could not be confirmed. " +
        "Retry using the original request identity.",
      "outcome_unknown",
      504,
    );
  }

  throw new KVRoutingError(
    `Cluster unavailable: ${lastMessage}`,
    "leader_unavailable",
    503,
  );
}

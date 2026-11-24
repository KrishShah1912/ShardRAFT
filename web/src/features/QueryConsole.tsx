import { useRef, useState } from "react";

import {
  executeRoutedKV,
  KVRoutingError,
  selectEntryNodes,
  type KVOperation,
  type RoutedKVRequest,
} from "../api/routing";

import type { NodeStatus } from "../types";

const OPERATIONS: KVOperation[] = [
  "GET",
  "PUT",
  "DELETE",
  "CAS",
];

interface Props {
  nodes: NodeStatus[];
  onComplete: () => void;
}

export function QueryConsole({
  nodes,
  onComplete,
}: Props) {
  const [operation, setOperation] =
    useState<KVOperation>("GET");

  const [key, setKey] = useState("demo:user");
  const [value, setValue] = useState("");
  const [expected, setExpected] = useState("");
  const [response, setResponse] = useState("");
  const [loading, setLoading] = useState(false);

  const [pendingRequest, setPendingRequest] =
    useState<RoutedKVRequest>();

  const [entryNodeId, setEntryNodeId] =
    useState<number>();

  const [attempts, setAttempts] = useState(0);

  const executing = useRef(false);

  const candidates = selectEntryNodes(nodes);

  const locked =
    loading || pendingRequest !== undefined;

  async function run(command: RoutedKVRequest) {
    if (executing.current) return;

    executing.current = true;

    setLoading(true);
    setEntryNodeId(undefined);
    setAttempts(0);
    setResponse("Routing request to the cluster...");

    try {
      const routed = await executeRoutedKV(
        nodes,
        command,
      );

      setEntryNodeId(routed.entryNodeId);
      setAttempts(routed.attempts);

      setResponse(
        JSON.stringify(routed.result, null, 2),
      );

      setPendingRequest(undefined);

      onComplete();
    } catch (error) {
      if (error instanceof KVRoutingError) {
        const mutation =
          command.operation !== "GET";

        if (
          mutation &&
          (
            error.code === "outcome_unknown" ||
            error.code === "leader_unavailable" ||
            pendingRequest !== undefined
          )
        ) {
          setPendingRequest(command);
        }

        setResponse(
          JSON.stringify(
            {
              error: error.message,
              code: error.code,
              status: error.status,
              ...(mutation
                ? {
                    client_id: command.clientId,
                    request_id: command.requestId,
                  }
                : {}),
            },
            null,
            2,
          ),
        );
      } else {
        if (command.operation !== "GET") {
          setPendingRequest(command);
        }

        setResponse(
          error instanceof Error
            ? error.message
            : "Request failed.",
        );
      }
    } finally {
      executing.current = false;
      setLoading(false);
    }
  }

  async function execute() {
    if (executing.current || pendingRequest) {
      return;
    }

    if (!key.trim()) {
      setResponse("A key is required.");
      return;
    }

    await run({
      operation,
      key,
      value,
      expected,
      clientId: "shardraft-console",
      requestId: crypto.randomUUID(),
    });
  }

  async function retryPending() {
    if (!pendingRequest || executing.current) {
      return;
    }

    await run(pendingRequest);
  }

  function discardPending() {
    if (executing.current) return;

    const confirmed = window.confirm(
      "The previous mutation may already have " +
        "committed. Discarding its request identity " +
        "does not undo it. Continue?",
    );

    if (!confirmed) return;

    setPendingRequest(undefined);

    setResponse(
      "Pending request cleared locally. " +
        "Its server-side outcome may remain unknown.",
    );
  }

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">DATA</div>
          <h1>Query Console</h1>
          <p>
            Execute operations through any reachable
            ShardRAFT node with automatic leader
            routing and bounded retries.
          </p>
        </div>
      </header>

      <div className="console-layout">
        <div className="panel query-panel">
          <div className="form-row">
            <label>Operation</label>

            <div className="operation-tabs">
              {OPERATIONS.map((item) => (
                <button
                  key={item}
                  disabled={locked}
                  className={
                    operation === item
                      ? "selected"
                      : ""
                  }
                  onClick={() =>
                    setOperation(item)
                  }
                >
                  {item}
                </button>
              ))}
            </div>
          </div>

          <div className="form-row">
            <label>Key</label>

            <input
              value={key}
              disabled={locked}
              onChange={(event) =>
                setKey(event.target.value)
              }
              placeholder="user:101"
            />
          </div>

          {operation === "CAS" && (
            <div className="form-row">
              <label>Expected value</label>

              <textarea
                value={expected}
                disabled={locked}
                onChange={(event) =>
                  setExpected(event.target.value)
                }
              />
            </div>
          )}

          {(operation === "PUT" ||
            operation === "CAS") && (
            <div className="form-row">
              <label>Value</label>

              <textarea
                value={value}
                disabled={locked}
                onChange={(event) =>
                  setValue(event.target.value)
                }
                placeholder='{"language":"Go"}'
              />
            </div>
          )}

          <button
            className="primary-button"
            disabled={
              loading ||
              pendingRequest !== undefined
            }
            onClick={execute}
          >
            {loading ? "Executing…" : "Execute"}
          </button>

          {pendingRequest && (
            <div className="form-row">
              <label>Unresolved mutation</label>

              <p>
                Retry the original request without
                generating a new request ID.
              </p>

              <button
                className="primary-button"
                disabled={loading}
                onClick={retryPending}
              >
                {loading
                  ? "Retrying…"
                  : "Retry Same Request"}
              </button>

              <button
                disabled={loading}
                onClick={discardPending}
              >
                Clear Pending Request
              </button>
            </div>
          )}

          <div className="request-target">
            Routing: Automatic
          </div>

          <div className="request-target">
            Entry candidates:{" "}
            {candidates.length > 0
              ? candidates
                  .map((id) => `Node ${id}`)
                  .join(", ")
              : "Discovering cluster"}
          </div>

          {entryNodeId !== undefined && (
            <div className="request-target">
              Last successful entry: Node{" "}
              {entryNodeId}
              {" · "}
              {attempts}{" "}
              {attempts === 1
                ? "attempt"
                : "attempts"}
            </div>
          )}
        </div>

        <div className="panel response-panel">
          <div className="panel-heading">
            <div>
              <h2>Response</h2>
              <p>Server result</p>
            </div>
          </div>

          <pre>
            {response ||
              "Execute an operation to inspect the response."}
          </pre>
        </div>
      </div>
    </>
  );
}

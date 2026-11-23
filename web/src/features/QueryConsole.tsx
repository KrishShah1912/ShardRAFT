import { useState } from "react";
import {
  casKV,
  deleteKV,
  getKV,
  putKV,
} from "../api/client";

interface Props {
  leaderId?: number;
  onComplete: () => void;
}

export function QueryConsole({
  leaderId,
  onComplete,
}: Props) {
  const [operation, setOperation] = useState("GET");
  const [key, setKey] = useState("demo:user");
  const [value, setValue] = useState("");
  const [expected, setExpected] = useState("");
  const [response, setResponse] = useState("");
  const [loading, setLoading] = useState(false);

  async function execute() {
    if (!leaderId) {
      setResponse("No leader is currently available.");
      return;
    }

    setLoading(true);

    try {
      const requestId = crypto.randomUUID();
      let result: unknown;

      switch (operation) {
        case "PUT":
          result = await putKV(
            leaderId,
            key,
            value,
            "shardraft-console",
            requestId,
          );
          break;

        case "DELETE":
          result = await deleteKV(
            leaderId,
            key,
            "shardraft-console",
            requestId,
          );
          break;

        case "CAS":
          result = await casKV(
            leaderId,
            key,
            expected,
            value,
            "shardraft-console",
            requestId,
          );
          break;

        default:
          result = await getKV(leaderId, key);
      }

      setResponse(JSON.stringify(result, null, 2));
      onComplete();
    } catch (error) {
      setResponse(
        error instanceof Error
          ? error.message
          : "Request failed",
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">DATA</div>
          <h1>Query Console</h1>
          <p>
            Execute operations directly against the current
            ShardRAFT leader.
          </p>
        </div>
      </header>

      <div className="console-layout">
        <div className="panel query-panel">
          <div className="form-row">
            <label>Operation</label>
            <div className="operation-tabs">
              {["GET", "PUT", "DELETE", "CAS"].map(
                (item) => (
                  <button
                    key={item}
                    className={
                      operation === item ? "selected" : ""
                    }
                    onClick={() => setOperation(item)}
                  >
                    {item}
                  </button>
                ),
              )}
            </div>
          </div>

          <div className="form-row">
            <label>Key</label>
            <input
              value={key}
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
                onChange={(event) =>
                  setValue(event.target.value)
                }
                placeholder='{"name":"Hetansh"}'
              />
            </div>
          )}

          <button
            className="primary-button"
            disabled={loading}
            onClick={execute}
          >
            {loading ? "Executing…" : "Execute"}
          </button>

          <div className="request-target">
            Target:{" "}
            {leaderId ? `Node ${leaderId} · Leader` : "No leader"}
          </div>
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

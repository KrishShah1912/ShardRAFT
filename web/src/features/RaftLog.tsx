import { Braces } from "lucide-react";
import type { RaftLog } from "../types";

export function RaftLogView({ log }: { log?: RaftLog }) {
  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">CONSENSUS</div>
          <h1>Raft Log</h1>
          <p>
            Inspect replicated entries and their commit/apply state.
          </p>
        </div>
      </header>

      <section className="metrics-grid">
        <Metric label="SNAPSHOT" value={log?.snapshot_index} />
        <Metric label="LAST INDEX" value={log?.last_index} />
        <Metric label="COMMIT" value={log?.commit_index} />
        <Metric label="APPLIED" value={log?.applied_index} />
      </section>

      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Entries</h2>
            <p>Current uncompacted Raft log</p>
          </div>
          <Braces size={17} />
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>INDEX</th>
                <th>TERM</th>
                <th>TYPE</th>
                <th>STATE</th>
                <th>PAYLOAD</th>
              </tr>
            </thead>
            <tbody>
              {[...(log?.entries ?? [])]
                .reverse()
                .map((entry) => {
                  let state = "Pending";

                  if (
                    log &&
                    entry.index <= log.applied_index
                  ) {
                    state = "Applied";
                  } else if (
                    log &&
                    entry.index <= log.commit_index
                  ) {
                    state = "Committed";
                  }

                  return (
                    <tr key={entry.index}>
                      <td>#{entry.index}</td>
                      <td>{entry.term}</td>
                      <td>
                        {entry.type === 1
                          ? "NO-OP"
                          : "NORMAL"}
                      </td>
                      <td>
                        <span className="status-pill">
                          {state}
                        </span>
                      </td>
                      <td>
                        <code className="payload-code">
                          {entry.data
                            ? decodeBase64(entry.data)
                            : "—"}
                        </code>
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function Metric({
  label,
  value,
}: {
  label: string;
  value?: number;
}) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value ?? "—"}</strong>
    </div>
  );
}

function decodeBase64(value: string): string {
  try {
    return atob(value);
  } catch {
    return value;
  }
}

import {
  Database as DatabaseIcon,
  Search,
} from "lucide-react";
import { useMemo, useState } from "react";
import type { KVSnapshot } from "../types";

export function Database({
  snapshot,
}: {
  snapshot?: KVSnapshot;
}) {
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<string>();

  const items = useMemo(
    () =>
      (snapshot?.items ?? []).filter((item) =>
        item.key.toLowerCase().includes(query.toLowerCase()),
      ),
    [snapshot, query],
  );

  const selectedItem = snapshot?.items.find(
    (item) => item.key === selected,
  );

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">DATA</div>
          <h1>Database Explorer</h1>
          <p>
            Browse the leader's currently applied key-value state.
          </p>
        </div>

        <span className="section-chip">
          {snapshot?.count ?? 0} RECORDS
        </span>
      </header>

      <div className="database-layout">
        <section className="panel">
          <div className="database-search">
            <Search size={15} />
            <input
              value={query}
              onChange={(event) =>
                setQuery(event.target.value)
              }
              placeholder="Search keys..."
            />
          </div>

          <div className="database-list">
            {items.map((item) => (
              <button
                className={
                  selected === item.key
                    ? "database-row selected"
                    : "database-row"
                }
                key={item.key}
                onClick={() => setSelected(item.key)}
              >
                <DatabaseIcon size={15} />
                <div>
                  <strong>{item.key}</strong>
                  <span>{item.value}</span>
                </div>
              </button>
            ))}

            {items.length === 0 && (
              <div className="empty-state">
                No matching records.
              </div>
            )}
          </div>
        </section>

        <section className="panel record-inspector">
          <div className="panel-heading">
            <div>
              <h2>
                {selectedItem?.key ?? "Record inspector"}
              </h2>
              <p>
                Applied index {snapshot?.applied_index ?? "—"}
              </p>
            </div>
          </div>

          {selectedItem ? (
            <pre>{prettyValue(selectedItem.value)}</pre>
          ) : (
            <div className="empty-state">
              Select a key to inspect its value.
            </div>
          )}
        </section>
      </div>
    </>
  );
}

function prettyValue(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

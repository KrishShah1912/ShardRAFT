import type {
  ReactNode,
  ComponentType,
} from "react";

export type Tone =
  | "neutral"
  | "primary"
  | "success"
  | "warning"
  | "danger"
  | "replication"
  | "heartbeat";

type MetricIcon = ComponentType<{
  size?: number;
  className?: string;
}>;

export function Metric({
  label,
  value,
  detail,
  tone = "neutral",
  icon: Icon,
}: {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  tone?: Tone;
  icon?: MetricIcon;
}) {
  return (
    <article
      className="sr-metric"
      data-tone={tone}
    >
      <div className="sr-metric-header">
        <span>{label}</span>
        {Icon && <Icon size={15} />}
      </div>

      <strong className="sr-metric-value">
        {value}
      </strong>

      {detail !== undefined && (
        <div className="sr-metric-detail">
          {detail}
        </div>
      )}
    </article>
  );
}

export function StatusBadge({
  children,
  tone = "neutral",
  dot = true,
}: {
  children: ReactNode;
  tone?: Tone;
  dot?: boolean;
}) {
  return (
    <span
      className="sr-status"
      data-tone={tone}
    >
      {dot && (
        <span
          className="sr-status-dot"
          aria-hidden="true"
        />
      )}
      {children}
    </span>
  );
}

export function SectionHeader({
  title,
  description,
  eyebrow,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  eyebrow?: string;
  actions?: ReactNode;
}) {
  return (
    <header className="sr-section-header">
      <div className="sr-section-copy">
        {eyebrow && (
          <span className="sr-eyebrow">
            {eyebrow}
          </span>
        )}

        <h2>{title}</h2>

        {description && <p>{description}</p>}
      </div>

      {actions && (
        <div className="sr-section-actions">
          {actions}
        </div>
      )}
    </header>
  );
}

export function Panel({
  children,
  className = "",
  title,
  description,
  actions,
}: {
  children: ReactNode;
  className?: string;
  title?: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <section
      className={`sr-panel ${className}`.trim()}
    >
      {title !== undefined && (
        <SectionHeader
          title={title}
          description={description}
          actions={actions}
        />
      )}

      {children}
    </section>
  );
}

export type ConnectionState =
  | "connected"
  | "connecting"
  | "disconnected"
  | "paused"
  | "stale";

const CONNECTION_STYLES: Record<
  ConnectionState,
  { label: string; tone: Tone }
> = {
  connected: {
    label: "Connected",
    tone: "success",
  },
  connecting: {
    label: "Connecting",
    tone: "warning",
  },
  disconnected: {
    label: "Disconnected",
    tone: "danger",
  },
  paused: {
    label: "Paused",
    tone: "neutral",
  },
  stale: {
    label: "Stale",
    tone: "warning",
  },
};

export function LiveIndicator({
  state,
  label,
}: {
  state: ConnectionState;
  label?: string;
}) {
  const style = CONNECTION_STYLES[state];

  return (
    <span
      className="sr-live-indicator"
      data-tone={style.tone}
      data-state={state}
      role="status"
    >
      <span
        className="sr-live-dot"
        aria-hidden="true"
      />

      {label ?? style.label}
    </span>
  );
}

export function EmptyState({
  title = "No observations available",
  description,
  action,
}: {
  title?: string;
  description?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="sr-empty-state">
      <strong>{title}</strong>

      {description && <p>{description}</p>}

      {action}
    </div>
  );
}

export interface TableColumn<T> {
  key: string;
  header: ReactNode;
  render: (row: T) => ReactNode;
  align?: "left" | "center" | "right";
  numeric?: boolean;
}

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  emptyMessage = "No records available",
  label,
}: {
  columns: readonly TableColumn<T>[];
  rows: readonly T[];
  rowKey: (row: T) => string | number;
  emptyMessage?: string;
  label: string;
}) {
  return (
    <div className="sr-table-wrap">
      <table
        className="sr-table"
        aria-label={label}
      >
        <thead>
          <tr>
            {columns.map(column => (
              <th
                key={column.key}
                scope="col"
                style={{
                  textAlign: column.align ?? "left",
                }}
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td
                colSpan={Math.max(1, columns.length)}
                className="sr-table-empty"
              >
                {emptyMessage}
              </td>
            </tr>
          ) : (
            rows.map(row => (
              <tr key={rowKey(row)}>
                {columns.map(column => (
                  <td
                    key={column.key}
                    className={
                      column.numeric
                        ? "sr-numeric"
                        : undefined
                    }
                    style={{
                      textAlign:
                        column.align ?? "left",
                    }}
                  >
                    {column.render(row)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export function InspectorPanel({
  title,
  description,
  children,
  onClose,
}: {
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  onClose?: () => void;
}) {
  return (
    <aside className="sr-inspector">
      <SectionHeader
        eyebrow="ENTITY INSPECTOR"
        title={title}
        description={description}
        actions={
          onClose && (
            <button
              type="button"
              className="sr-button"
              onClick={onClose}
              aria-label="Close inspector"
            >
              Close
            </button>
          )
        }
      />

      <div className="sr-inspector-body">
        {children}
      </div>
    </aside>
  );
}

export function ChartContainer({
  title,
  description,
  source,
  children,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  source: "live" | "historical" | "schematic";
  children: ReactNode;
  actions?: ReactNode;
}) {
  const sourceLabels = {
    live: "Live observations",
    historical: "Recorded evidence",
    schematic: "Schematic",
  };

  return (
    <Panel
      className="sr-chart"
      title={title}
      description={description}
      actions={
        <>
          <StatusBadge
            tone="neutral"
            dot={false}
          >
            {sourceLabels[source]}
          </StatusBadge>
          {actions}
        </>
      }
    >
      <div className="sr-chart-body">
        {children}
      </div>
    </Panel>
  );
}

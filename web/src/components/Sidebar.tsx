import {
  Activity,
  Boxes,
  Braces,
  CircleGauge,
  Database,
  FlaskConical,
  Gauge,
  GitBranch,
  HardDrive,
  Network,
  Radio,
  Search,
  ShieldAlert,
  TerminalSquare,
  Waypoints,
  Workflow,
} from "lucide-react";
import type { Page } from "../types";

interface Props {
  page: Page;
  onChange: (page: Page) => void;
  mode?: "legacy" | "multi";
  reachableNodes?: number;
  loading?: boolean;
}

const groups: {
  title?: string;
  items: {
    page: Page;
    label: string;
    icon: typeof Activity;
  }[];
}[] = [
  {
    items: [
      {
        page: "overview",
        label: "Overview",
        icon: CircleGauge,
      },
      {
        page: "architecture",
        label: "Architecture",
        icon: Workflow,
      },
    ],
  },
  {
    title: "CLUSTER",
    items: [
      {
        page: "cluster",
        label: "Cluster",
        icon: Network,
      },
      {
        page: "replication",
        label: "Replication",
        icon: GitBranch,
      },
      {
        page: "raft-log",
        label: "Raft Log",
        icon: Braces,
      },
    ],
  },
  {
    title: "DATA",
    items: [
      {
        page: "database",
        label: "Database",
        icon: Database,
      },
      {
        page: "console",
        label: "Query Console",
        icon: TerminalSquare,
      },
    ],
  },
  {
    title: "INFRASTRUCTURE",
    items: [
      {
        page: "storage",
        label: "Storage",
        icon: HardDrive,
      },
      {
        page: "transport",
        label: "Transport",
        icon: Waypoints,
      },
      {
        page: "shards",
        label: "Shards",
        icon: Boxes,
      },
    ],
  },
  {
    title: "OBSERVABILITY",
    items: [
      {
        page: "metrics",
        label: "Metrics",
        icon: Activity,
      },
      {
        page: "events",
        label: "Events",
        icon: Radio,
      },
    ],
  },
  {
    title: "VALIDATION",
    items: [
      {
        page: "test-lab",
        label: "Test Lab",
        icon: FlaskConical,
      },
    ],
  },
  {
    title: "EXPERIMENTS",
    items: [
      {
        page: "faults",
        label: "Fault Lab",
        icon: ShieldAlert,
      },
      {
        page: "performance",
        label: "Performance Lab",
        icon: Gauge,
      },
    ],
  },
];

export function Sidebar({
  page,
  onChange,
  mode = "legacy",
  reachableNodes = 0,
  loading = false,
}: Props) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">
          <Search size={19} />
        </div>

        <div>
          <strong>ShardRAFT</strong>
          <span>Distributed Database</span>
        </div>
      </div>

      <nav
        aria-label="ShardRAFT navigation"
      >
        {groups.map((group, index) => (
          <div
            className="nav-group"
            key={index}
          >
            {group.title && (
              <div className="nav-title">
                {group.title}
              </div>
            )}

            {group.items.map((item) => {
              const Icon = item.icon;

              return (
                <button
                  key={item.page}
                  className={
                    page === item.page
                      ? "nav-item active"
                      : "nav-item"
                  }
                  onClick={() =>
                    onChange(item.page)
                  }
                >
                  <Icon size={17} />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </div>
        ))}
      </nav>

      <div className="sidebar-footer">
        <div
          className={
            mode === "legacy" ||
            (!loading && reachableNodes === 5)
              ? "status-dot healthy"
              : "status-dot ma-status-warning"
          }
        />

        <div>
          <strong>
            {mode === "multi"
              ? "Multi-Raft Cluster"
              : "Local Cluster"}
          </strong>
          <span>
            {mode === "multi"
              ? loading
                ? "Connecting..."
                : `${reachableNodes}/5 nodes reachable`
              : "3-node development"}
          </span>
        </div>
      </div>
      <a
        className="ma-mode-link"
        href={mode === "multi" ? "/?mode=legacy" : "/"}
        onClick={event => {
          if (
            mode === "multi" &&
            !window.confirm(
              "Switch deployment mode? In-memory query history " +
              "will be lost. Saved unresolved mutations " +
              "remain available in this browser tab.",
            )
          ) {
            event.preventDefault();
          }
        }}
      >
        {mode === "multi"
          ? "Switch to development mode"
          : "Switch to Multi-Raft mode"}
      </a>
    </aside>
  );
}

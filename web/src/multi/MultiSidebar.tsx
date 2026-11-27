import {
  Activity,
  ArrowLeftRight,
  Boxes,
  Braces,
  CircleGauge,
  Database,
  FlaskConical,
  Gauge,
  GitBranch,
  HardDrive,
  Layers,
  Server,
  Network,
  Radio,
  ShieldAlert,
  TerminalSquare,
  Waypoints,
  Workflow,
} from "lucide-react";

import type { LucideIcon } from "lucide-react";
import type { Page } from "../types";
import { StatusBadge } from "../design-system";

import "./multiSidebar.css";

interface Props {
  page: Page;
  onChange: (page: Page) => void;
  reachableNodes: number;
  loading: boolean;
}

interface NavigationItem {
  page: Page;
  label: string;
  icon: LucideIcon;
}

interface NavigationGroup {
  title: string;
  icon: LucideIcon;
  items: NavigationItem[];
}

const GROUPS: NavigationGroup[] = [
  {
    title: "Workspace",
    icon: Layers,
    items: [
      {
        page: "overview",
        label: "Overview",
        icon: CircleGauge,
      },
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
      {
        page: "shards",
        label: "Shards",
        icon: Boxes,
      },
    ],
  },
  {
    title: "Data",
    icon: Database,
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
    title: "Observability",
    icon: Activity,
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
      {
        page: "transport",
        label: "Transport",
        icon: Waypoints,
      },
    ],
  },
  {
    title: "Experiments",
    icon: FlaskConical,
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
      {
        page: "test-lab",
        label: "Test Lab",
        icon: FlaskConical,
      },
    ],
  },
  {
    title: "Infrastructure",
    icon: Server,
    items: [
      {
        page: "storage",
        label: "Storage",
        icon: HardDrive,
      },
      {
        page: "architecture",
        label: "Architecture",
        icon: Workflow,
      },
    ],
  },
];

export function getMultiNavigation(page: Page) {
  for (const group of GROUPS) {
    const item = group.items.find(
      entry => entry.page === page,
    );

    if (item) {
      return {
        category: group.title,
        label: item.label,
        icon: group.icon,
      };
    }
  }

  return {
    category: "Workspace",
    label: page,
    icon: Layers,
  };
}

export function MultiSidebar({
  page,
  onChange,
  reachableNodes,
  loading,
}: Props) {
  const tone = loading
    ? "neutral"
    : reachableNodes === 5
      ? "success"
      : reachableNodes === 0
        ? "danger"
        : "warning";

  return (
    <aside className="sidebar sr-sidebar">
      <header className="sr-sidebar-brand">
        <div
          className="sr-brand-mark"
          aria-hidden="true"
        >
          <Database size={21} />
        </div>

        <div className="sr-brand-copy">
          <strong>ShardRAFT</strong>
          <span>Engineering Control Plane</span>
        </div>
      </header>

      <div className="sr-deployment-label">
        <span>DEPLOYMENT</span>
        <strong>Multi-Raft</strong>
      </div>

      <nav aria-label="ShardRAFT navigation">
        {GROUPS.map(group => (
          <section
            key={group.title}
            className="sr-nav-group"
            aria-label={group.title}
          >
            <h2 className="sr-nav-title">
              {group.title}
            </h2>

            {group.items.map(item => {
              const Icon = item.icon;
              const active = page === item.page;

              return (
                <button
                  type="button"
                  key={item.page}
                  className={
                    active
                      ? "sr-nav-item active"
                      : "sr-nav-item"
                  }
                  aria-current={
                    active ? "page" : undefined
                  }
                  onClick={() => onChange(item.page)}
                >
                  <Icon
                    size={16}
                    aria-hidden="true"
                  />

                  <span>{item.label}</span>
                </button>
              );
            })}
          </section>
        ))}
      </nav>

      <footer className="sr-sidebar-footer">
        <div className="sr-cluster-summary">
          <div className="sr-cluster-heading">
            <strong>Storage cluster</strong>

            <StatusBadge tone={tone}>
              {loading
                ? "Connecting"
                : reachableNodes === 5
                  ? "Reachable"
                  : reachableNodes === 0
                    ? "Unavailable"
                    : "Partial"}
            </StatusBadge>
          </div>

          <div className="sr-cluster-count">
            {loading
              ? "Awaiting node observations"
              : `${reachableNodes} / 5 nodes reachable`}
          </div>

          <div
            className="sr-reachability-track"
            role="progressbar"
            aria-label="Reachable storage nodes"
            aria-valuemin={0}
            aria-valuemax={5}
            aria-valuenow={
              loading ? undefined : reachableNodes
            }
          >
            <div
              data-tone={tone}
              style={{
                width: loading
                  ? "0%"
                  : `${Math.max(
                      0,
                      Math.min(5, reachableNodes),
                    ) * 20}%`,
              }}
            />
          </div>
        </div>

        <a
          className="sr-legacy-link"
          href="/?mode=legacy"
          onClick={event => {
            if (!window.confirm(
              "Switch to the legacy console? " +
              "In-memory query and fault history " +
              "will be lost. Saved unresolved " +
              "mutations remain in this browser tab."
            )) {
              event.preventDefault();
            }
          }}
        >
          <ArrowLeftRight
            size={14}
            aria-hidden="true"
          />

          <span>Legacy console</span>
        </a>

        <div className="sr-sidebar-note">
          Local engineering environment
        </div>
      </footer>
    </aside>
  );
}

import { useState } from "react";

import {
  ChevronRight,
  RefreshCw,
} from "lucide-react";

import {
  StatusBadge,
} from "../design-system";

import type {
  MultiRaftClusterState,
} from "./useMultiRaftCluster";

import type {
  MultiLiveTelemetry,
} from "./useMultiLiveTelemetry";

import type { Page } from "../types";
import { getMultiNavigation } from "./MultiSidebar";

import "./multiWorkspace.css";

interface Props {
  page: Page;
  cluster: MultiRaftClusterState;
  telemetry: MultiLiveTelemetry;
}

export function MultiWorkspaceToolbar({
  page,
  cluster,
  telemetry,
}: Props) {
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState("");

  const navigation = getMultiNavigation(page);
  const CategoryIcon = navigation.icon;

  const connected = telemetry.streams.filter(
    stream => stream.status === "connected",
  ).length;

  const streamTone =
    connected === 5
      ? "success"
      : connected === 0
        ? "neutral"
        : "warning";

  async function refresh() {
    if (refreshing) return;

    setRefreshing(true);
    setRefreshError("");

    try {
      await cluster.refresh();
    } catch (error) {
      setRefreshError(
        error instanceof Error
          ? error.message
          : "Refresh failed",
      );
    } finally {
      setRefreshing(false);
    }
  }

  return (
    <header className="sr-workspace-toolbar">
      <div className="sr-workspace-path">
        <CategoryIcon
          size={15}
          aria-hidden="true"
        />

        <span>{navigation.category}</span>

        <ChevronRight
          size={13}
          aria-hidden="true"
        />

        <strong aria-current="page">{navigation.label}</strong>
      </div>

      <div className="sr-workspace-controls">
        <StatusBadge tone={streamTone}>
          {connected}/5 SSE connected
        </StatusBadge>

        {cluster.errors.length > 0 && (
          <StatusBadge tone="warning">
            {cluster.errors.length} warnings
          </StatusBadge>
        )}

        <span className="sr-observation-time">
          {cluster.loading
            ? "Connecting..."
            : cluster.lastUpdated
              ? `Observed ${new Date(
                  cluster.lastUpdated,
                ).toLocaleTimeString()}`
              : "Awaiting observations"}
        </span>

        <button
          type="button"
          className="sr-button sr-refresh-button"
          disabled={refreshing}
          onClick={() => void refresh()}
        >
          <RefreshCw
            size={13}
            className={
              refreshing ? "sr-refresh-spin" : ""
            }
            aria-hidden="true"
          />

          {refreshing ? "Refreshing" : "Refresh"}
        </button>
      </div>

      {refreshError && (
        <div
          className="sr-refresh-error"
          role="alert"
        >
          {refreshError}
        </div>
      )}
    </header>
  );
}

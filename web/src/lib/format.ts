import type { Role } from "../types";

export function roleName(role: Role): string {
  switch (role) {
    case 2:
      return "Leader";
    case 1:
      return "Candidate";
    default:
      return "Follower";
  }
}

export function formatEventName(type: string): string {
  return type
    .replace(/^raft\./, "")
    .split(".")
    .map(
      (part) =>
        part.charAt(0).toUpperCase() + part.slice(1),
    )
    .join(" ");
}

export function relativeTime(timestamp: string): string {
  const difference =
    Date.now() - new Date(timestamp).getTime();

  if (difference < 1000) {
    return "now";
  }

  if (difference < 60000) {
    return `${Math.floor(difference / 1000)}s`;
  }

  return `${Math.floor(difference / 60000)}m`;
}

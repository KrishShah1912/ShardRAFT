const EXACT_VISUALIZATION_MAP: Record<string, string> = {
  TestThreeNodeElection:
    "three-node-election",

  TestLeaderCommitsAfterMajority:
    "leader-majority-commit",

  TestLaggingFollowerCatchesUp:
    "lagging-follower",

  TestFollowerLearnsCommitThroughHeartbeat:
    "heartbeat-commit",
};

export function visualizationForTest(
  name: string,
): string | undefined {
  const exact =
    EXACT_VISUALIZATION_MAP[name];

  if (exact) {
    return exact;
  }

  const lower =
    name.toLowerCase();

  if (
    lower.includes("leader failover") ||
    lower.includes(
      "continues accepting writes after leader failure",
    )
  ) {
    return "leader-failover-e2e";
  }

  if (
    lower.includes("follower") &&
    lower.includes("routing")
  ) {
    return "request-routing";
  }

  return undefined;
}

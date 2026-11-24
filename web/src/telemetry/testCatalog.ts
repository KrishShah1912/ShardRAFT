export type TestCategory =
  | "election"
  | "replication"
  | "safety"
  | "recovery"
  | "routing"
  | "e2e";

export type TestStatus =
  | "passed"
  | "failed"
  | "not-run";

export type TraceEventKind =
  | "election"
  | "vote"
  | "append"
  | "ack"
  | "commit"
  | "apply"
  | "failure"
  | "recovery"
  | "route"
  | "assertion";

export interface TestTraceEvent {
  at: number;
  kind: TraceEventKind;
  title: string;
  detail: string;
  from?: number;
  to?: number;
  nodeId?: number;
  commitIndex?: number;
}

export interface VisualTestCase {
  id: string;
  name: string;
  category: TestCategory;
  status: TestStatus;
  source: string;
  purpose: string;
  invariant: string;
  trace: TestTraceEvent[];
}

export const TEST_CATALOG: VisualTestCase[] = [
  {
    id: "three-node-election",
    name: "Three Node Election",
    category: "election",
    status: "passed",
    source: "TestThreeNodeElection",
    purpose:
      "Demonstrates leader election across a three-node Raft cluster.",
    invariant:
      "A candidate becomes leader only after receiving a majority of votes.",
    trace: [
      {
        at: 0,
        kind: "election",
        title: "Election timeout",
        detail:
          "Node 1 reaches its election timeout and becomes a candidate.",
        nodeId: 1,
      },
      {
        at: 120,
        kind: "vote",
        title: "Vote request",
        detail:
          "Node 1 requests a vote from Node 2.",
        from: 1,
        to: 2,
      },
      {
        at: 145,
        kind: "vote",
        title: "Vote request",
        detail:
          "Node 1 requests a vote from Node 3.",
        from: 1,
        to: 3,
      },
      {
        at: 210,
        kind: "vote",
        title: "Majority reached",
        detail:
          "Node 1 receives enough votes to form a majority.",
        nodeId: 1,
      },
      {
        at: 240,
        kind: "assertion",
        title: "Leader elected",
        detail:
          "Node 1 transitions to leader for the new term.",
        nodeId: 1,
      },
    ],
  },
  {
    id: "leader-majority-commit",
    name: "Leader Commits After Majority",
    category: "replication",
    status: "passed",
    source: "TestLeaderCommitsAfterMajority",
    purpose:
      "Shows that an entry becomes committed only after majority replication.",
    invariant:
      "The leader must not commit a new entry without replication on a quorum.",
    trace: [
      {
        at: 0,
        kind: "append",
        title: "Command proposed",
        detail:
          "The leader appends a new command at log index 42.",
        nodeId: 1,
        commitIndex: 41,
      },
      {
        at: 90,
        kind: "append",
        title: "AppendEntries",
        detail:
          "The leader sends the new entry to Node 2.",
        from: 1,
        to: 2,
      },
      {
        at: 120,
        kind: "append",
        title: "AppendEntries",
        detail:
          "The leader sends the new entry to Node 3.",
        from: 1,
        to: 3,
      },
      {
        at: 190,
        kind: "ack",
        title: "Replication acknowledged",
        detail:
          "Node 2 acknowledges log index 42.",
        from: 2,
        to: 1,
      },
      {
        at: 230,
        kind: "commit",
        title: "Quorum reached",
        detail:
          "The entry now exists on a majority of replicas.",
        nodeId: 1,
        commitIndex: 42,
      },
      {
        at: 270,
        kind: "apply",
        title: "Entry applied",
        detail:
          "Committed index 42 is applied to the state machine.",
        nodeId: 1,
        commitIndex: 42,
      },
    ],
  },
  {
    id: "lagging-follower",
    name: "Lagging Follower Catches Up",
    category: "recovery",
    status: "passed",
    source: "TestLaggingFollowerCatchesUp",
    purpose:
      "Demonstrates recovery when a follower is missing replicated entries.",
    invariant:
      "A recovering follower converges to the leader's committed log.",
    trace: [
      {
        at: 0,
        kind: "failure",
        title: "Follower behind",
        detail:
          "Node 3 is missing entries that exist on the leader.",
        nodeId: 3,
      },
      {
        at: 130,
        kind: "append",
        title: "Catch-up attempt",
        detail:
          "Leader retries replication using an earlier nextIndex.",
        from: 1,
        to: 3,
      },
      {
        at: 220,
        kind: "ack",
        title: "Missing entries accepted",
        detail:
          "Node 3 accepts the missing log suffix.",
        from: 3,
        to: 1,
      },
      {
        at: 300,
        kind: "recovery",
        title: "Follower synchronized",
        detail:
          "Node 3 reaches the leader's committed position.",
        nodeId: 3,
      },
    ],
  },
  {
    id: "heartbeat-commit",
    name: "Follower Learns Commit Through Heartbeat",
    category: "safety",
    status: "passed",
    source: "TestFollowerLearnsCommitThroughHeartbeat",
    purpose:
      "Shows commit-index propagation even when a heartbeat contains no new entries.",
    invariant:
      "Follower commit indexes advance safely from the leader's committed position.",
    trace: [
      {
        at: 0,
        kind: "commit",
        title: "Leader already committed",
        detail:
          "Leader commit index is ahead of the follower.",
        nodeId: 1,
        commitIndex: 18,
      },
      {
        at: 100,
        kind: "append",
        title: "Heartbeat sent",
        detail:
          "Leader sends an empty AppendEntries heartbeat carrying commit index 18.",
        from: 1,
        to: 2,
      },
      {
        at: 170,
        kind: "commit",
        title: "Follower commit advances",
        detail:
          "Node 2 safely advances its commit index.",
        nodeId: 2,
        commitIndex: 18,
      },
    ],
  },
  {
    id: "request-routing",
    name: "Follower Request Routing",
    category: "routing",
    status: "passed",
    source: "Step 7 integration validation",
    purpose:
      "Visualizes a client request entering through a follower and reaching the leader.",
    invariant:
      "Routing preserves mutation identity while forwarding to the current leader.",
    trace: [
      {
        at: 0,
        kind: "route",
        title: "Client request",
        detail:
          "A mutation enters the cluster through Node 2.",
        to: 2,
      },
      {
        at: 100,
        kind: "route",
        title: "Leader discovery",
        detail:
          "Node 2 discovers Node 1 as the current leader.",
        from: 2,
        to: 1,
      },
      {
        at: 190,
        kind: "append",
        title: "Raft proposal",
        detail:
          "Node 1 proposes the mutation through Raft.",
        nodeId: 1,
      },
      {
        at: 280,
        kind: "commit",
        title: "Mutation committed",
        detail:
          "A majority replicates the mutation.",
        nodeId: 1,
      },
      {
        at: 350,
        kind: "assertion",
        title: "Response returned",
        detail:
          "The original client receives the committed result.",
        nodeId: 2,
      },
    ],
  },
  {
    id: "leader-failover-e2e",
    name: "Leader Failover and Recovery",
    category: "e2e",
    status: "passed",
    source: "Playwright leader-failover.spec.ts",
    purpose:
      "Exercises a real leader failure, replacement, post-failover write, restart, and catch-up.",
    invariant:
      "A majority remains available and committed data survives leader replacement.",
    trace: [
      {
        at: 0,
        kind: "failure",
        title: "Leader stopped",
        detail:
          "The active leader becomes unavailable.",
        nodeId: 1,
      },
      {
        at: 800,
        kind: "election",
        title: "Election begins",
        detail:
          "A surviving follower starts a new election.",
        nodeId: 3,
      },
      {
        at: 1700,
        kind: "vote",
        title: "Majority vote",
        detail:
          "The surviving nodes form a quorum.",
        nodeId: 3,
      },
      {
        at: 3032,
        kind: "assertion",
        title: "New leader elected",
        detail:
          "Node 3 becomes leader after approximately 3.032 seconds.",
        nodeId: 3,
      },
      {
        at: 3400,
        kind: "commit",
        title: "Post-failover write",
        detail:
          "The replacement leader commits another write.",
        nodeId: 3,
      },
      {
        at: 4100,
        kind: "recovery",
        title: "Old leader returns",
        detail:
          "Node 1 restarts and catches up with the current leader.",
        nodeId: 1,
      },
    ],
  },
];

export function categoryLabel(
  category: TestCategory,
) {
  switch (category) {
    case "election":
      return "Elections";
    case "replication":
      return "Replication";
    case "safety":
      return "Safety";
    case "recovery":
      return "Recovery";
    case "routing":
      return "Routing";
    case "e2e":
      return "End-to-End";
  }
}

import type {
  ValidationReport,
  ValidationTest,
} from "./types";

export type CoverageLevel =
  | "strong"
  | "covered"
  | "partial"
  | "gap"
  | "planned";

export interface CoverageCapability {
  id: string;
  name: string;
  description: string;
  level: CoverageLevel;
  matchedTests: ValidationTest[];
  testCount: number;
  unit: boolean;
  integration: boolean;
  e2e: boolean;
  fault: boolean;
}

interface Definition {
  id: string;
  name: string;
  description: string;
  patterns: RegExp[];
  unit?: boolean;
  integration?: boolean;
  e2e?: boolean;
  fault?: boolean;
  limitation?: "gap" | "planned";
}

const DEFINITIONS: Definition[] = [
  {
    id: "leader-election",
    name: "Leader Election",
    description:
      "Leader selection, voting and term transitions.",
    patterns: [
      /election/i,
      /vote/i,
      /candidate/i,
      /leader/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "majority-commit",
    name: "Majority Commit",
    description:
      "Entries commit only after quorum replication.",
    patterns: [
      /commit/i,
      /majority/i,
      /quorum/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "replication",
    name: "Log Replication",
    description:
      "AppendEntries, acknowledgements and follower progress.",
    patterns: [
      /append/i,
      /replicat/i,
      /match.*index/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "catch-up",
    name: "Follower Catch-up",
    description:
      "Lagging replicas converge to the leader log.",
    patterns: [
      /catch.*up/i,
      /lagging/i,
      /conflict/i,
      /next.*index/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "routing",
    name: "Request Routing",
    description:
      "Requests entering followers reach the current leader.",
    patterns: [
      /routing/i,
      /route/i,
      /entry node/i,
      /fallback/i,
      /leader discovery/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "deduplication",
    name: "Request Deduplication",
    description:
      "Mutation identity prevents unsafe duplicate application.",
    patterns: [
      /dedup/i,
      /duplicate/i,
      /identity/i,
      /request.*conflict/i,
    ],
    unit: true,
    integration: true,
    e2e: true,
    fault: true,
  },
  {
    id: "wal",
    name: "WAL Durability",
    description:
      "Persistent log recovery, corruption detection and compaction.",
    patterns: [
      /wal/i,
      /checkpoint/i,
      /segment/i,
      /compaction/i,
    ],
    unit: true,
    integration: true,
    fault: true,
  },
  {
    id: "snapshot",
    name: "Snapshot Recovery",
    description:
      "Local snapshot creation and recovery behavior.",
    patterns: [
      /snapshot/i,
    ],
    unit: true,
    integration: true,
  },
  {
    id: "linearizable-reads",
    name: "Linearizable Reads",
    description:
      "Read results proven against current Raft leadership.",
    patterns: [],
    limitation: "gap",
  },
  {
    id: "multi-shard",
    name: "Multi-Shard Routing",
    description:
      "Routing across independent Raft-backed shards.",
    patterns: [],
    limitation: "planned",
  },
  {
    id: "snapshot-install",
    name: "Network Snapshot Install",
    description:
      "Leader transfers snapshots to remote lagging replicas.",
    patterns: [],
    limitation: "planned",
  },
];

function allTests(
  report: ValidationReport,
) {
  return report.suites.flatMap(
    (suite) => suite.tests,
  );
}

export function buildCoverage(
  report: ValidationReport | null,
): CoverageCapability[] {
  const tests =
    report ? allTests(report) : [];

  return DEFINITIONS.map(
    (definition) => {
      if (definition.limitation) {
        return {
          id: definition.id,
          name: definition.name,
          description:
            definition.description,
          level:
            definition.limitation,
          matchedTests: [],
          testCount: 0,
          unit: false,
          integration: false,
          e2e: false,
          fault: false,
        };
      }

      const matchedTests =
        tests.filter((test) =>
          definition.patterns.some(
            (pattern) =>
              pattern.test(
                `${test.name} ${test.package ?? ""}`,
              ),
          ),
        );

      const testCount =
        matchedTests.length;

      const level: CoverageLevel =
        testCount >= 10
          ? "strong"
          : testCount > 0
            ? "covered"
            : "partial";

      return {
        id: definition.id,
        name: definition.name,
        description:
          definition.description,
        level,
        matchedTests,
        testCount,
        unit:
          Boolean(definition.unit) &&
          testCount > 0,
        integration:
          Boolean(
            definition.integration,
          ) && testCount > 0,
        e2e:
          Boolean(definition.e2e) &&
          testCount > 0,
        fault:
          Boolean(definition.fault) &&
          testCount > 0,
      };
    },
  );
}

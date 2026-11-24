import assert from "node:assert/strict";
import test from "node:test";

const {
  loadValidationReport,
} = await import(
  "../.test-build/validation/report.js"
);

function validReport() {
  return {
    schema_version: 1,
    generated_at:
      "2026-11-24T00:00:00Z",
    git_commit: "abc123",
    git_branch: "test",
    duration_ms: 100,
    status: "passed",
    total: 1,
    passed: 1,
    failed: 0,
    skipped: 0,
    suites: [],
  };
}

test(
  "validation report distinguishes a missing report",
  async () => {
    const originalFetch =
      globalThis.fetch;

    globalThis.fetch =
      async () =>
        new Response(
          "",
          {
            status: 404,
          },
        );

    try {
      const loaded =
        await loadValidationReport();

      assert.equal(
        loaded.reportAvailable,
        false,
      );

      assert.equal(
        loaded.report.total,
        0,
      );

      assert.match(
        loaded.error,
        /404/,
      );
    } finally {
      globalThis.fetch =
        originalFetch;
    }
  },
);

test(
  "validation report accepts valid generated data",
  async () => {
    const originalFetch =
      globalThis.fetch;

    globalThis.fetch =
      async () =>
        Response.json(
          validReport(),
        );

    try {
      const loaded =
        await loadValidationReport();

      assert.equal(
        loaded.reportAvailable,
        true,
      );

      assert.equal(
        loaded.report.total,
        1,
      );

      assert.equal(
        loaded.report.passed,
        1,
      );
    } finally {
      globalThis.fetch =
        originalFetch;
    }
  },
);

test(
  "validation report rejects invalid JSON structure",
  async () => {
    const originalFetch =
      globalThis.fetch;

    globalThis.fetch =
      async () =>
        Response.json({
          hello: "world",
        });

    try {
      const loaded =
        await loadValidationReport();

      assert.equal(
        loaded.reportAvailable,
        false,
      );

      assert.match(
        loaded.error,
        /invalid format/i,
      );
    } finally {
      globalThis.fetch =
        originalFetch;
    }
  },
);

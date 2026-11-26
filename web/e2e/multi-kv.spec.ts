import {
  expect,
  test,
  type Page,
} from "@playwright/test";

import { randomUUID } from "node:crypto";

type Operation = "GET" | "PUT" | "CAS" | "DELETE";

interface Command {
  operation: Operation;
  key: string;
  gateway: 1 | 2;
  value?: string;
  expected?: string;
}

async function openConsole(page: Page) {
  await page.goto("/?mode=multi");

  await page.locator(".sidebar nav")
    .getByText("Query Console", {
      exact: true,
    })
    .click();

  await expect(
    page.getByRole("heading", {
      name: "Query Console",
      exact: true,
    }),
  ).toBeVisible();
}

async function execute(
  page: Page,
  command: Command,
): Promise<Record<string, unknown>> {
  const queryConsole = page.locator(".mq-console");

  const form = queryConsole.locator("form");

  const result = queryConsole.locator(
    ".mq-panel",
  ).filter({
    has: page.getByRole("heading", {
      name: "Execution result",
      exact: true,
    }),
  });

  await form.getByRole("button", {
    name: command.operation,
    exact: true,
  }).click();

  await form.getByLabel(
    "Database key",
  ).fill(command.key);

  await form.getByLabel(
    "Preferred gateway",
  ).selectOption(String(command.gateway));

  await form.getByLabel(
    "Automatically try the other gateway",
  ).uncheck();

  if (command.operation === "GET") {
    await form.getByLabel(
      "Read consistency",
    ).selectOption("linearizable");
  }

  if (
    command.operation === "PUT" ||
    command.operation === "CAS"
  ) {
    await form.getByLabel(
      "New value",
    ).fill(command.value ?? "");
  }

  if (command.operation === "CAS") {
    await form.getByLabel(
      "Expected value",
    ).fill(command.expected ?? "");
  }

  const identity =
    command.operation === "GET"
      ? null
      : await form.locator(
          ".mq-identity code",
        ).allTextContents();

  const method =
    command.operation === "CAS"
      ? "POST"
      : command.operation;

  const path =
    `/multi/gateway${command.gateway}` +
    `/kv/${encodeURIComponent(command.key)}` +
    (command.operation === "CAS" ? "/cas" : "");

  const responsePromise = page.waitForResponse(
    response =>
      new URL(response.url()).pathname === path &&
      response.request().method() === method,
    {
      timeout: 30_000,
    },
  );

  await form.getByRole("button", {
    name: `Execute ${command.operation}`,
    exact: true,
  }).click();

  const response = await responsePromise;

  const raw = await response.json();

  const expectedResult =
    command.operation === "GET" &&
    response.status() === 404
      ? {
          key: command.key,
          found: false,
          code: "key_not_found",
        }
      : raw;

  expect(
    response.ok() ||
    (
      command.operation === "GET" &&
      response.status() === 404 &&
      raw.code === "key_not_found"
    ),
  ).toBeTruthy();

  await expect(
    form.getByRole("button", {
      name: `Execute ${command.operation}`,
      exact: true,
    }),
  ).toBeEnabled({
    timeout: 30_000,
  });

  await expect(
    result.locator(".mq-status"),
  ).toHaveText("SUCCESS");

  await expect(
    result.locator(".mq-result-header code"),
  ).toHaveText(
    `${command.operation} ${command.key}`,
  );

  await expect(
    result.locator(".mq-json"),
  ).toHaveText(
    JSON.stringify(expectedResult, null, 2),
  );

  const stats = result.locator(".mq-stats > div");

  await expect(
    stats.filter({
      hasText: "Responding gateway",
    }).locator("strong"),
  ).toHaveText(
    `Gateway ${command.gateway}`,
  );

  await expect(
    stats.filter({
      hasText: "HTTP status",
    }).locator("strong"),
  ).toHaveText(
    String(response.status()),
  );

  await expect(
    stats.filter({
      hasText: "Attempts",
    }).locator("strong"),
  ).toHaveText("1");

  if (identity) {
    expect(raw.client_id).toBe(identity[0]);
    expect(raw.request_id).toBe(identity[1]);

    const pending = await page.evaluate(
      () => sessionStorage.getItem(
        "shardraft.multi.pending-mutation.v1",
      ),
    );

    expect(pending).toBeNull();
  }

  console.log(
    `${command.operation} via Gateway ` +
    `${command.gateway}: HTTP ${response.status()} ` +
    JSON.stringify(expectedResult),
  );

  return expectedResult;
}

test(
  "live KV lifecycle across both gateways",
  async ({ page, request }) => {
    const errors: string[] = [];

    page.on("pageerror", error => {
      errors.push(error.message);
    });

    const key = `browser-kv-${randomUUID()}`;

    const initial = "initial-value";
    const updated = "updated-value";

    try {
      await openConsole(page);

      await test.step(
        "GET missing key through gateway 1",
        async () => {
          const result = await execute(page, {
            operation: "GET",
            key,
            gateway: 1,
          });

          expect(result.found).toBe(false);
        },
      );

      await test.step(
        "PUT initial value through gateway 1",
        async () => {
          const result = await execute(page, {
            operation: "PUT",
            key,
            gateway: 1,
            value: initial,
          });

          expect(result.applied).toBe(true);
        },
      );

      await test.step(
        "GET initial value through gateway 2",
        async () => {
          const result = await execute(page, {
            operation: "GET",
            key,
            gateway: 2,
          });

          expect(result.found).toBe(true);
          expect(result.value).toBe(initial);
        },
      );

      await test.step(
        "CAS succeeds through gateway 2",
        async () => {
          const result = await execute(page, {
            operation: "CAS",
            key,
            gateway: 2,
            expected: initial,
            value: updated,
          });

          expect(result.applied).toBe(true);
        },
      );

      await test.step(
        "CAS mismatch through gateway 1",
        async () => {
          const result = await execute(page, {
            operation: "CAS",
            key,
            gateway: 1,
            expected: "incorrect-value",
            value: "must-not-be-written",
          });

          expect(result.applied).toBe(false);
        },
      );

      await test.step(
        "GET verifies updated value",
        async () => {
          const result = await execute(page, {
            operation: "GET",
            key,
            gateway: 1,
          });

          expect(result.found).toBe(true);
          expect(result.value).toBe(updated);
        },
      );

      await test.step(
        "DELETE through gateway 2",
        async () => {
          const result = await execute(page, {
            operation: "DELETE",
            key,
            gateway: 2,
          });

          expect(result.applied).toBe(true);
        },
      );

      await test.step(
        "GET confirms deletion",
        async () => {
          const result = await execute(page, {
            operation: "GET",
            key,
            gateway: 1,
          });

          expect(result.found).toBe(false);
        },
      );

      const history = page.locator(
        ".mq-panel",
      ).filter({
        has: page.getByRole("heading", {
          name: "Request history",
          exact: true,
        }),
      });

      await expect(
        history.locator("tbody tr"),
      ).toHaveCount(8);

      await expect(
        page.locator(".mq-slot-active"),
      ).toHaveCount(1);

      expect(errors).toEqual([]);

      console.log(
        "All eight KV operations verified.",
      );

      console.log(
        "Request history and routing preview verified.",
      );

    } finally {
      // Best-effort cleanup of this test's unique key.
      try {
        const cleanup = await request.delete(
          "http://127.0.0.1:18091/kv/" +
          encodeURIComponent(key),
          {
            data: {
              client_id: `cleanup-${randomUUID()}`,
              request_id: randomUUID(),
            },
            timeout: 15_000,
          },
        );

        console.log(
          `Test key cleanup: HTTP ${cleanup.status()}`,
        );
      } catch (error) {
        console.warn(
          "Test key cleanup unavailable:",
          String(error),
        );
      }
    }
  },
);

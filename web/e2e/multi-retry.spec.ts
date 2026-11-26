import {
  expect,
  test,
  type Page,
  type APIRequestContext,
} from "@playwright/test";

import { randomUUID } from "node:crypto";

const STORAGE_KEY =
  "shardraft.multi.pending-mutation.v1";

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

function resultPanel(page: Page) {
  return page.locator(".mq-panel").filter({
    has: page.getByRole("heading", {
      name: "Execution result",
      exact: true,
    }),
  });
}

async function preparePut(
  page: Page,
  key: string,
  value: string,
  failover: boolean,
) {
  const form = page.locator(".mq-console form");

  await form.getByRole("button", {
    name: "PUT",
    exact: true,
  }).click();

  await form.getByLabel("Database key")
    .fill(key);

  await form.getByLabel("New value")
    .fill(value);

  await form.getByLabel("Preferred gateway")
    .selectOption("1");

  await form.getByLabel(
    "Automatically try the other gateway",
  ).setChecked(failover);

  const identities = await form.locator(
    ".mq-identity code",
  ).allTextContents();

  expect(identities).toHaveLength(2);

  return {
    form,
    client_id: identities[0],
    request_id: identities[1],
  };
}

async function readKey(
  request: APIRequestContext,
  key: string,
) {
  const response = await request.get(
    "http://127.0.0.1:18092/kv/" +
    encodeURIComponent(key) +
    "?read_mode=linearizable",
  );

  expect(response.ok()).toBeTruthy();

  return response.json();
}

async function cleanup(
  request: APIRequestContext,
  key: string,
) {
  try {
    const response = await request.delete(
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
      `Cleanup: HTTP ${response.status()}`,
    );
  } catch (error) {
    console.warn(
      "Cleanup unavailable:",
      String(error),
    );
  }
}

test(
  "automatic gateway failover preserves mutation identity",
  async ({ page, request }) => {
    const key = `browser-failover-${randomUUID()}`;

    const value = "gateway-failover-value";

    const attempts: Array<{
      gateway: number;
      body: Record<string, unknown>;
    }> = [];

    try {
      await openConsole(page);

      const command = await preparePut(
        page,
        key,
        value,
        true,
      );

      await page.route(
        `**/multi/gateway1/kv/${key}`,
        async route => {
          attempts.push({
            gateway: 1,
            body: route.request().postDataJSON(),
          });

          // Simulate an unavailable first gateway.
          await route.abort("failed");
        },
      );

      await page.route(
        `**/multi/gateway2/kv/${key}`,
        async route => {
          attempts.push({
            gateway: 2,
            body: route.request().postDataJSON(),
          });

          await route.continue();
        },
      );

      await command.form.getByRole("button", {
        name: "Execute PUT",
        exact: true,
      }).click();

      const result = resultPanel(page);

      await expect(
        result.locator(".mq-status"),
      ).toHaveText("SUCCESS", {
        timeout: 30_000,
      });

      expect(
        attempts.map(attempt => attempt.gateway),
      ).toEqual([1, 2]);

      expect(attempts[0].body)
        .toEqual(attempts[1].body);

      expect(attempts[1].body).toMatchObject({
        client_id: command.client_id,
        request_id: command.request_id,
        value,
      });

      await expect(
        result.locator(".mq-stats > div")
          .filter({
            hasText: "Responding gateway",
          })
          .locator("strong"),
      ).toHaveText("Gateway 2");

      await expect(
        result.locator(".mq-stats > div")
          .filter({
            hasText: "Attempts",
          })
          .locator("strong"),
      ).toHaveText("2");

      expect(await readKey(request, key))
        .toMatchObject({
          found: true,
          value,
        });

      expect(
        await page.evaluate(
          key => sessionStorage.getItem(key),
          STORAGE_KEY,
        ),
      ).toBeNull();

      console.log(
        "PASS: automatic gateway failover",
      );

      console.log(
        "PASS: identical mutation payload and identity",
      );

      console.log(
        "PASS: committed value verified through gateway 2",
      );

    } finally {
      await cleanup(request, key);
    }
  },
);

test(
  "unresolved mutation survives reload and retry is deduplicated",
  async ({ page, request }) => {
    const key = `browser-retry-${randomUUID()}`;

    const originalValue = "original-value";
    const newerValue = "newer-value";

    let originalBody:
      Record<string, unknown> | undefined;

    let originalResponse:
      Record<string, unknown> | undefined;

    try {
      await openConsole(page);

      const command = await preparePut(
        page,
        key,
        originalValue,
        false,
      );

      const pattern =
        `**/multi/gateway1/kv/${key}`;

      await page.route(
        pattern,
        async route => {
          originalBody =
            route.request().postDataJSON();

          // Forward the real mutation to the backend.
          const response = await route.fetch();

          expect(response.ok()).toBeTruthy();

          originalResponse = await response.json();

          expect(originalResponse)
            .toMatchObject({
              applied: true,
              client_id: command.client_id,
              request_id: command.request_id,
            });

          // The backend committed the mutation,
          // but the browser never receives its response.
          await route.abort("failed");
        },
      );

      await command.form.getByRole("button", {
        name: "Execute PUT",
        exact: true,
      }).click();

      await expect.poll(
        () => originalResponse,
        {
          timeout: 30_000,
        },
      ).toBeTruthy();

      await expect(
        resultPanel(page).locator(".mq-status"),
      ).not.toHaveText("SUCCESS");

      const stored = await page.evaluate(
        key => sessionStorage.getItem(key),
        STORAGE_KEY,
      );

      expect(stored).not.toBeNull();

      const pending = JSON.parse(stored!);

      expect(pending).toMatchObject({
        operation: "PUT",
        key,
        value: originalValue,
        clientId: command.client_id,
        requestId: command.request_id,
      });

      expect(await readKey(request, key))
        .toMatchObject({
          found: true,
          value: originalValue,
        });

      console.log(
        "PASS: original mutation committed despite lost response",
      );

      console.log(
        "PASS: original payload persisted in session storage",
      );

      await page.unroute(pattern);

      // Apply a newer mutation using a distinct identity.
      const newer = await request.put(
        "http://127.0.0.1:18092/kv/" +
        encodeURIComponent(key),
        {
          data: {
            value: newerValue,
            client_id: `newer-${randomUUID()}`,
            request_id: randomUUID(),
          },
        },
      );

      expect(newer.ok()).toBeTruthy();

      expect(await newer.json())
        .toMatchObject({
          applied: true,
        });

      await page.reload();

      await page.locator(".sidebar nav")
        .getByText("Query Console", {
          exact: true,
        })
        .click();

      const restored = await page.evaluate(
        key => sessionStorage.getItem(key),
        STORAGE_KEY,
      );

      expect(restored).toBe(stored);

      const form = page.locator(
        ".mq-console form",
      );

      await expect(
        form.getByLabel("Database key"),
      ).toHaveValue(key);

      await expect(
        form.getByLabel("Database key"),
      ).toBeDisabled();

      await expect(
        form.getByLabel("New value"),
      ).toHaveValue(originalValue);

      console.log(
        "PASS: original mutation restored after reload",
      );

      const retryButton = page.locator(
        ".mq-console",
      ).getByRole("button", {
        name: "Retry original request",
        exact: true,
      });

      await expect(retryButton)
        .toBeEnabled({
          timeout: 15_000,
        });

      const responsePromise =
        page.waitForResponse(
          response =>
            new URL(response.url())
              .pathname.endsWith(`/kv/${key}`) &&
            response.request().method() === "PUT",
          {
            timeout: 30_000,
          },
        );

      await retryButton.click();

      const retry = await responsePromise;

      expect(retry.ok()).toBeTruthy();

      expect(retry.request().postDataJSON())
        .toEqual(originalBody);

      expect(await retry.json())
        .toEqual(originalResponse);

      await expect(
        resultPanel(page).locator(".mq-status"),
      ).toHaveText("SUCCESS");

      await expect.poll(
        () => page.evaluate(
          key => sessionStorage.getItem(key),
          STORAGE_KEY,
        ),
      ).toBeNull();

      // A duplicate mutation must not overwrite
      // the value written by the newer request.
      expect(await readKey(request, key))
        .toMatchObject({
          found: true,
          value: newerValue,
        });

      console.log(
        "PASS: retry reused exact original request identity",
      );

      console.log(
        "PASS: duplicate mutation did not overwrite newer value",
      );

      console.log(
        "PASS: pending session storage cleared",
      );

    } finally {
      await cleanup(request, key);
    }
  },
);

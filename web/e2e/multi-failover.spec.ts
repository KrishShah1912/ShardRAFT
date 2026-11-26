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


import { execFileSync } from "node:child_process";

interface GroupStatus {
  shard_id: number;
  role: string;
  term: number;
  commit_index: number;
  applied_index: number;
}

test(
  "live leader failover and replica recovery through Fault Lab",
  async ({ page, request }) => {
    test.setTimeout(180_000);

    const shard = 2;
    const stoppedNode = 2;
    const replicas = [2, 3, 4];

    const errors: string[] = [];

    page.on("pageerror", error => {
      errors.push(error.message);
    });

    async function nodeStatus(node: number) {
      try {
        const response = await request.get(
          `http://127.0.0.1:${18080 + node}/status`,
          { timeout: 3000 },
        );

        if (!response.ok()) return null;

        return await response.json();
      } catch {
        return null;
      }
    }

    async function groupStatus(
      node: number,
    ): Promise<GroupStatus | null> {
      const status = await nodeStatus(node);

      return status?.groups.find(
        (group: GroupStatus) =>
          group.shard_id === shard,
      ) ?? null;
    }

    async function navigate(name: string) {
      await page.locator(".sidebar nav")
        .getByText(name, { exact: true })
        .click();
    }

    async function reachableCount() {
      const statuses = await Promise.all(
        [1, 2, 3, 4, 5].map(nodeStatus),
      );

      return statuses.filter(Boolean).length;
    }

    function faultPanel(heading: string) {
      return page.locator(".mf-page .ma-panel")
        .filter({
          has: page.getByRole("heading", {
            name: heading,
            exact: true,
          }),
        });
    }

    const nodeCard = page.locator(".mf-node")
      .filter({
        has: page.getByRole("heading", {
          name: `Node ${stoppedNode}`,
          exact: true,
        }),
      });

    async function dockerAction(
      action: "stop" | "start",
    ) {
      const button = nodeCard.getByRole("button", {
        name: action === "stop" ? "Stop" : "Start",
        exact: true,
      });

      await expect(button).toBeEnabled({
        timeout: 30_000,
      });

      if (action === "stop") {
        page.once("dialog", dialog => dialog.accept());
      }

      const responsePromise = page.waitForResponse(
        response =>
          new URL(response.url()).pathname ===
          `/multi/control/api/v1/control/nodes/${stoppedNode}/${action}` &&
          response.request().method() === "POST",
        { timeout: 75_000 },
      );

      await button.click();

      const response = await responsePromise;

      expect(response.ok()).toBeTruthy();

      expect(await response.json()).toMatchObject({
        node_id: stoppedNode,
        action,
        success: true,
      });

      const history = faultPanel(
        "Docker action history",
      );

      await expect(
        history.locator("tbody tr").first(),
      ).toContainText("Confirmed");

      await expect(
        history.locator("tbody tr").first(),
      ).toContainText(action);

      console.log(
        `PASS: Docker ${action} confirmed through frontend`,
      );
    }

    // CRC32 IEEE, matching the backend slot calculation.
    function slotFor(key: string) {
      let crc = 0xffffffff;

      for (const byte of Buffer.from(key, "utf8")) {
        crc ^= byte;

        for (let bit = 0; bit < 8; bit++) {
          crc = (crc >>> 1) ^
            ((crc & 1) ? 0xedb88320 : 0);
        }
      }

      return ((crc ^ 0xffffffff) >>> 0) % 65536;
    }

    const prefix = `browser-leader-${randomUUID()}`;

    let key = "";

    for (let i = 0; i < 10000; i++) {
      const candidate = `${prefix}-${i}`;
      const slot = slotFor(candidate);

      if (slot >= 16384 && slot <= 32767) {
        key = candidate;
        break;
      }
    }

    expect(key).not.toBe("");

    console.log(
      `Test key: ${key}, slot=${slotFor(key)}, shard=2`,
    );

    let restoreRequired = false;
    let keyUsed = false;

    try {
      await expect.poll(reachableCount)
        .toBe(5);

      const initialGroups = await Promise.all(
        replicas.map(groupStatus),
      );

      expect(initialGroups.every(Boolean))
        .toBeTruthy();

      expect(
        initialGroups.filter(
          group => group?.role === "leader",
        ),
      ).toHaveLength(1);

      const initial = initialGroups[0];

      expect(initial?.role).toBe("leader");

      const initialTerm = initial!.term;

      const metadata = await Promise.all(
        [1, 2, 3].map(async node => {
          const response = await request.get(
            `http://127.0.0.1:${18080 + node}` +
            "/api/v1/metadata/status",
          );

          expect(response.ok()).toBeTruthy();

          return response.json();
        }),
      );

      const metadataLeaders = metadata.filter(
        status => status.role === "leader",
      );

      expect(metadataLeaders).toHaveLength(1);

      expect(metadataLeaders[0].node_id)
        .not.toBe(stoppedNode);

      console.log(
        `Initial leader: Node 2, term=${initialTerm}`,
      );

      await openConsole(page);

      keyUsed = true;

      const initialWrite = await execute(page, {
        operation: "PUT",
        key,
        gateway: 1,
        value: "before-leader-failure",
      });

      expect(initialWrite.applied).toBe(true);

      await navigate("Fault Lab");

      await expect(
        nodeCard.getByText("S2 · Leader", {
          exact: true,
        }),
      ).toBeVisible();

      const availability = faultPanel(
        "Current shard availability",
      );

      const shardRow = availability
        .locator("tbody tr")
        .filter({
          has: page.getByRole("cell", {
            name: "Shard 2",
            exact: true,
          }),
        });

      await expect(
        shardRow.locator("td").nth(1),
      ).toHaveText("Node 2");

      await expect(
        shardRow.locator("td").nth(2),
      ).toHaveText("3/3");

      restoreRequired = true;

      const failureStarted = Date.now();

      await dockerAction("stop");

      await expect.poll(
        () => nodeStatus(stoppedNode),
        { timeout: 30_000 },
      ).toBeNull();

      await expect.poll(
        reachableCount,
        { timeout: 30_000 },
      ).toBe(4);

      let newLeader = 0;
      let newTerm = 0;

      await expect.poll(
        async () => {
          const groups = await Promise.all(
            [3, 4].map(groupStatus),
          );

          const leaders = groups
            .map((group, index) => ({
              group,
              node: index + 3,
            }))
            .filter(
              item => item.group?.role === "leader",
            );

          if (leaders.length !== 1) return false;

          newLeader = leaders[0].node;
          newTerm = leaders[0].group!.term;

          return newTerm > initialTerm;
        },
        { timeout: 30_000 },
      ).toBe(true);

      console.log(
        `PASS: new leader Node ${newLeader}, ` +
        `term ${initialTerm} -> ${newTerm}`,
      );

      console.log(
        `Observed stop-to-election interval: ` +
        `${Date.now() - failureStarted} ms`,
      );

      await expect(
        shardRow.locator("td").nth(1),
      ).toHaveText(`Node ${newLeader}`, {
        timeout: 30_000,
      });

      await expect(
        shardRow.locator("td").nth(2),
      ).toHaveText("2/3");

      await expect(
        shardRow.locator("td").nth(3),
      ).toHaveText("Yes");

      const timeline = faultPanel(
        "Observed leadership transitions",
      );

      await expect(
        timeline.locator("tbody tr").filter({
          has: page.getByRole("cell", {
            name: "Shard 2",
            exact: true,
          }),
        }).filter({
          has: page.getByRole("cell", {
            name: `Node ${newLeader}`,
            exact: true,
          }),
        }).first(),
      ).toBeVisible();

      console.log(
        "PASS: Fault Lab observed leadership transition",
      );

      await navigate("Query Console");

      const preserved = await execute(page, {
        operation: "GET",
        key,
        gateway: 2,
      });

      expect(preserved).toMatchObject({
        found: true,
        value: "before-leader-failure",
      });

      console.log(
        "PASS: committed value preserved after leader failure",
      );

      const updated = await execute(page, {
        operation: "PUT",
        key,
        gateway: 2,
        value: "after-leader-failure",
      });

      expect(updated.applied).toBe(true);

      const verified = await execute(page, {
        operation: "GET",
        key,
        gateway: 1,
      });

      expect(verified).toMatchObject({
        found: true,
        value: "after-leader-failure",
      });

      console.log(
        "PASS: reads and writes remain available",
      );

      const leaderStatus = await groupStatus(newLeader);

      expect(leaderStatus).not.toBeNull();

      const recoveryTarget =
        leaderStatus!.commit_index;

      await navigate("Fault Lab");

      const recoveryStarted = Date.now();

      await dockerAction("start");

      await expect.poll(
        reachableCount,
        { timeout: 30_000 },
      ).toBe(5);

      await expect.poll(
        async () => {
          const recovered =
            await groupStatus(stoppedNode);

          return Boolean(
            recovered &&
            recovered.commit_index >= recoveryTarget &&
            recovered.applied_index >= recoveryTarget,
          );
        },
        { timeout: 30_000 },
      ).toBe(true);

      const recovered = await groupStatus(stoppedNode);

      console.log(
        `PASS: Node 2 recovered, ` +
        `commit=${recovered!.commit_index}, ` +
        `applied=${recovered!.applied_index}, ` +
        `target=${recoveryTarget}`,
      );

      console.log(
        `Observed recovery interval: ` +
        `${Date.now() - recoveryStarted} ms`,
      );

      await expect(
        shardRow.locator("td").nth(2),
      ).toHaveText("3/3", {
        timeout: 30_000,
      });

      await expect(
        faultPanel("Docker action history")
          .locator("tbody tr"),
      ).toHaveCount(2);

      await navigate("Query Console");

      const finalRead = await execute(page, {
        operation: "GET",
        key,
        gateway: 1,
      });

      expect(finalRead).toMatchObject({
        found: true,
        value: "after-leader-failure",
      });

      const deleted = await execute(page, {
        operation: "DELETE",
        key,
        gateway: 2,
      });

      expect(deleted.applied).toBe(true);

      expect(errors).toEqual([]);

      console.log(
        "PASS: final data correctness verified",
      );

      console.log(
        "PASS: all five storage nodes restored",
      );

    } finally {
      if (restoreRequired) {
        // Restore the node even if a browser assertion fails.
        execFileSync(
          "docker",
          [
            "compose",
            "-f", "../docker-compose.yml",
            "-p", "shardraft",
            "start", "n2",
          ],
          {
            stdio: "inherit",
            timeout: 60000,
          },
        );

        await expect.poll(
          reachableCount,
          { timeout: 30_000 },
        ).toBe(5);
      }

      if (keyUsed) {
        const cleanup = await request.delete(
          "http://127.0.0.1:18091/kv/" +
          encodeURIComponent(key),
          {
            data: {
              client_id: `cleanup-${randomUUID()}`,
              request_id: randomUUID(),
            },
            timeout: 15000,
          },
        );

        console.log(
          `Test key cleanup: HTTP ${cleanup.status()}`,
        );

        expect(cleanup.ok()).toBeTruthy();
      }
    }
  },
);

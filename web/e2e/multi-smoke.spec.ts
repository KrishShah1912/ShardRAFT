import {
  expect,
  test,
} from "@playwright/test";

test(
  "all five storage nodes and metadata are available",
  async ({ request }) => {
    let totalGroups = 0;

    for (const node of [1, 2, 3, 4, 5]) {
      const response = await request.get(
        `http://127.0.0.1:${18080 + node}/status`,
      );

      expect(response.ok()).toBeTruthy();

      const status = await response.json();

      expect(status.id).toBe(node);
      expect(status.mode).toBe("multi-raft");

      totalGroups += status.groups.length;

      console.log(
        `Node ${node}: ${status.groups.length} data groups`,
      );
    }

    expect(totalGroups).toBe(12);

    for (const node of [1, 2, 3]) {
      const response = await request.get(
        `http://127.0.0.1:${18080 + node}` +
        "/api/v1/metadata/shard-map",
      );

      expect(response.ok()).toBeTruthy();

      const result = await response.json();

      expect(result.shard_map.total_slots)
        .toBe(65536);

      expect(result.shard_map.shards)
        .toHaveLength(4);
    }

    const control = await request.get(
      "http://127.0.0.1:18090/health",
    );

    expect(control.ok()).toBeTruthy();

    expect(await control.json()).toMatchObject({
      mode: "docker",
      status: "ok",
    });
  },
);

test(
  "all fifteen dashboard pages render without runtime errors",
  async ({ page }) => {
    const errors: string[] = [];

    page.on("pageerror", error => {
      errors.push(error.message);
    });

    await page.goto("/?mode=multi");

    await expect(
      page.getByText("ShardRAFT").first(),
    ).toBeVisible();

    const navigation = page.locator(
      ".sidebar nav",
    );

    await expect(navigation).toBeVisible();

    const buttons = navigation.locator("button");

    await expect(buttons).toHaveCount(15);

    const labels = await buttons.allTextContents();

    for (let index = 0; index < labels.length; index++) {
      const label = labels[index].trim();

      await buttons.nth(index).click();

      await expect(
        page.locator("main h1:visible").first(),
      ).toBeVisible();

      await expect(
        page.getByText(
          "Multi-Raft dashboard integration pending",
          { exact: false },
        ),
      ).not.toBeVisible();

      const heading = await page
        .locator("main h1:visible")
        .first()
        .innerText();

      console.log(
        `PASS: ${label} -> ${heading}`,
      );

      expect(errors).toEqual([]);
    }

    expect(errors).toEqual([]);
  },
);

test(
  "historical performance and correctness evidence loads",
  async ({ page, request }) => {
    const response = await request.get(
      "/generated/multi-raft-experiments.json",
    );

    expect(response.ok()).toBeTruthy();

    const report = await response.json();

    expect(report.mode).toBe("historical");

    expect(report.summary).toMatchObject({
      experiments: 8,
      requests: 8000,
      successful: 8000,
      failed: 0,
      unknown: 0,
      pending: 0,
      linearizable_runs: 8,
    });

    await page.goto("/?mode=multi");

    await page.locator(".sidebar nav")
      .getByText("Performance Lab", { exact: true })
      .click();

    await expect(
      page.getByText(
        "Historical experiment evidence",
        { exact: true },
      ),
    ).toBeVisible();

    await expect(
      page.getByText(
        "Distributed workload results",
        { exact: true },
      ),
    ).toBeVisible();

    await page
      .getByLabel("Performance metric")
      .selectOption("p99_ms");

    await expect(
      page.getByLabel("Performance metric"),
    ).toHaveValue("p99_ms");

    await page.locator(".sidebar nav")
      .getByText("Test Lab", { exact: true })
      .click();

    await expect(
      page.getByText(
        "Recorded Porcupine verdicts",
        { exact: true },
      ),
    ).toBeVisible();

    await expect(
      page.getByText(
        "LINEARIZABLE",
        { exact: true },
      ),
    ).toHaveCount(8);
  },
);

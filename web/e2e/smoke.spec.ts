import {
  expect,
  test,
} from "@playwright/test";

import {
  findLeader,
  startAllNodes,
} from "./helpers";

test.describe(
  "ShardRAFT smoke test",
  () => {
    test.beforeEach(
      async ({ request }) => {
        await startAllNodes(
          request,
        );
      },
    );

    test(
      "dashboard and Raft cluster are available",
      async ({
        page,
        request,
      }) => {
        await page.goto("/");

        await expect(
          page.getByText(
            "ShardRAFT",
          ).first(),
        ).toBeVisible();

        const leader =
          await findLeader(
            request,
          );

        expect(
          [1, 2, 3],
        ).toContain(
          leader.id,
        );

        expect(
          leader.role,
        ).toBe(2);

        expect(
          leader.leader_id,
        ).toBe(
          leader.id,
        );
      },
    );
  },
);

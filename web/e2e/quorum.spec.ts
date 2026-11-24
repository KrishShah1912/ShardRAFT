import {
  expect,
  test,
} from "@playwright/test";

import {
  findLeader,
  nodeStatus,
  startAllNodes,
  stopNode,
} from "./helpers";

test.describe(
  "ShardRAFT quorum",
  () => {
    test.beforeEach(
      async ({ request }) => {
        await startAllNodes(
          request,
        );
      },
    );

    test.afterEach(
      async ({ request }) => {
        await startAllNodes(
          request,
        );
      },
    );

    test(
      "survives one node failure",
      async ({ request }) => {
        const leader =
          await findLeader(
            request,
          );

        const follower =
          [1, 2, 3].find(
            (id) =>
              id !== leader.id,
          );

        expect(
          follower,
        ).toBeDefined();

        await stopNode(
          request,
          follower!,
        );

        const remainingLeader =
          await findLeader(
            request,
            follower,
          );

        expect(
          remainingLeader.role,
        ).toBe(2);

        let reachable = 0;

        for (
          const id of [1, 2, 3]
        ) {
          if (
            await nodeStatus(
              request,
              id,
            )
          ) {
            reachable++;
          }
        }

        expect(
          reachable,
        ).toBe(2);
      },
    );
  },
);

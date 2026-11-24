import {
  expect,
  test,
} from "@playwright/test";

import {
  findLeader,
  getKV,
  nodeStatus,
  putKV,
  startAllNodes,
  startNode,
  stopNode,
} from "./helpers";

test.describe(
  "ShardRAFT leader failover",
  () => {
    test.beforeEach(
      async ({ request }) => {
        await startAllNodes(request);
      },
    );

    test.afterEach(
      async ({ request }) => {
        await startAllNodes(request);
      },
    );

    test(
      "continues accepting writes after leader failure and recovers the old leader",
      async ({
        page,
        request,
      }) => {
        await page.goto("/");

        await expect(
          page
            .getByText("ShardRAFT")
            .first(),
        ).toBeVisible();

        /*
         * ------------------------------------------------
         * 1. Discover the original leader
         * ------------------------------------------------
         */

        const originalLeader =
          await findLeader(request);

        console.log(
          `Original leader: Node ${originalLeader.id}, term ${originalLeader.term}`,
        );

        const key =
          `e2e:failover:${Date.now()}`;

        const initialValue =
          JSON.stringify({
            phase: "before-failover",
            leader: originalLeader.id,
          });

        /*
         * ------------------------------------------------
         * 2. Write through the current leader
         * ------------------------------------------------
         */

        await putKV(
          request,
          originalLeader.id,
          key,
          initialValue,
          `before-${Date.now()}`,
        );

        /*
         * Verify the leader can read the committed value.
         */

        await expect
          .poll(
            async () =>
              await getKV(
                request,
                originalLeader.id,
                key,
              ),
            {
              timeout: 10_000,
              message:
                "waiting for initial leader read",
            },
          )
          .toBe(initialValue);

        /*
         * ------------------------------------------------
         * 3. Verify Raft replication
         * ------------------------------------------------
         *
         * Followers do not need to expose client reads.
         * Instead verify that all three Raft nodes advance
         * to the leader's committed/applied position.
         */

        const committedLeader =
          await nodeStatus(
            request,
            originalLeader.id,
          );

        expect(
          committedLeader,
        ).toBeDefined();

        const targetCommit =
          committedLeader!.commit_index;

        console.log(
          `Initial write committed at index ${targetCommit}`,
        );

        await expect
          .poll(
            async () => {
              const states =
                await Promise.all(
                  [1, 2, 3].map(
                    (id) =>
                      nodeStatus(
                        request,
                        id,
                      ),
                  ),
                );

              return states.every(
                (state) =>
                  state !== undefined &&
                  state.commit_index >=
                    targetCommit &&
                  state.applied_index >=
                    targetCommit,
              );
            },
            {
              timeout: 15_000,
              intervals: [
                100,
                250,
                500,
              ],
              message:
                "waiting for initial Raft replication",
            },
          )
          .toBe(true);

        console.log(
          "Initial write replicated and applied by all nodes.",
        );

        /*
         * ------------------------------------------------
         * 4. Kill the current leader
         * ------------------------------------------------
         */

        const failureStarted =
          Date.now();

        await stopNode(
          request,
          originalLeader.id,
        );

        console.log(
          `Stopped Node ${originalLeader.id}`,
        );

        /*
         * ------------------------------------------------
         * 5. Wait for a replacement leader
         * ------------------------------------------------
         */

        const newLeader =
          await findLeader(
            request,
            originalLeader.id,
          );

        const failoverMs =
          Date.now() -
          failureStarted;

        console.log(
          `New leader: Node ${newLeader.id}, term ${newLeader.term}`,
        );

        console.log(
          `Observed failover: ${failoverMs} ms`,
        );

        expect(
          newLeader.id,
        ).not.toBe(
          originalLeader.id,
        );

        expect(
          newLeader.term,
        ).toBeGreaterThan(
          originalLeader.term,
        );

        /*
         * ------------------------------------------------
         * 6. Write while the original leader is DOWN
         * ------------------------------------------------
         */

        const failoverValue =
          JSON.stringify({
            phase: "after-failover",
            leader: newLeader.id,
          });

        await putKV(
          request,
          newLeader.id,
          key,
          failoverValue,
          `after-${Date.now()}`,
        );

        /*
         * Verify client-visible state through the new leader.
         */

        await expect
          .poll(
            async () =>
              await getKV(
                request,
                newLeader.id,
                key,
              ),
            {
              timeout: 10_000,
              message:
                "waiting for post-failover leader read",
            },
          )
          .toBe(failoverValue);

        const newLeaderState =
          await nodeStatus(
            request,
            newLeader.id,
          );

        expect(
          newLeaderState,
        ).toBeDefined();

        const failoverCommit =
          newLeaderState!.commit_index;

        console.log(
          `Post-failover write committed at index ${failoverCommit}`,
        );

        /*
         * ------------------------------------------------
         * 7. Verify surviving quorum applied the new write
         * ------------------------------------------------
         */

        const survivingNodes =
          [1, 2, 3].filter(
            (id) =>
              id !==
              originalLeader.id,
          );

        await expect
          .poll(
            async () => {
              const states =
                await Promise.all(
                  survivingNodes.map(
                    (id) =>
                      nodeStatus(
                        request,
                        id,
                      ),
                  ),
                );

              return states.every(
                (state) =>
                  state !== undefined &&
                  state.commit_index >=
                    failoverCommit &&
                  state.applied_index >=
                    failoverCommit,
              );
            },
            {
              timeout: 15_000,
              intervals: [
                100,
                250,
                500,
              ],
              message:
                "waiting for surviving quorum to apply post-failover write",
            },
          )
          .toBe(true);

        console.log(
          "Post-failover write replicated across surviving quorum.",
        );

        /*
         * ------------------------------------------------
         * 8. Restart the old leader
         * ------------------------------------------------
         */

        await startNode(
          request,
          originalLeader.id,
        );

        console.log(
          `Restarted Node ${originalLeader.id}`,
        );

        /*
         * ------------------------------------------------
         * 9. Verify follower catch-up
         * ------------------------------------------------
         */

        await expect
          .poll(
            async () => {
              const recovered =
                await nodeStatus(
                  request,
                  originalLeader.id,
                );

              return (
                recovered !==
                  undefined &&
                recovered.commit_index >=
                  failoverCommit &&
                recovered.applied_index >=
                  failoverCommit
              );
            },
            {
              timeout: 30_000,
              intervals: [
                100,
                250,
                500,
                1000,
              ],
              message:
                "waiting for restarted node to catch up",
            },
          )
          .toBe(true);

        console.log(
          `Node ${originalLeader.id} caught up to commit ${failoverCommit}.`,
        );

        /*
         * ------------------------------------------------
         * 10. Verify cluster convergence
         * ------------------------------------------------
         */

        await expect
          .poll(
            async () => {
              const states =
                await Promise.all(
                  [1, 2, 3].map(
                    (id) =>
                      nodeStatus(
                        request,
                        id,
                      ),
                  ),
                );

              if (
                states.some(
                  (state) =>
                    state ===
                    undefined,
                )
              ) {
                return false;
              }

              const commits =
                states.map(
                  (state) =>
                    state!
                      .commit_index,
                );

              const applied =
                states.map(
                  (state) =>
                    state!
                      .applied_index,
                );

              return (
                commits.every(
                  (index) =>
                    index >=
                    failoverCommit,
                ) &&
                applied.every(
                  (index) =>
                    index >=
                    failoverCommit,
                )
              );
            },
            {
              timeout: 30_000,
              message:
                "waiting for all replicas to converge",
            },
          )
          .toBe(true);

        /*
         * Find the authoritative leader after recovery.
         * The restarted node may or may not become leader.
         */

        const finalLeader =
          await findLeader(request);

        const finalValue =
          await getKV(
            request,
            finalLeader.id,
            key,
          );

        expect(
          finalValue,
        ).toBe(
          failoverValue,
        );

        console.log(
          `Final leader: Node ${finalLeader.id}, term ${finalLeader.term}`,
        );

        console.log(
          "Cluster recovered successfully.",
        );

        /*
         * Browser should remain healthy after the experiment.
         */

        await page.reload();

        await expect(
          page
            .getByText("ShardRAFT")
            .first(),
        ).toBeVisible();
      },
    );
  },
);

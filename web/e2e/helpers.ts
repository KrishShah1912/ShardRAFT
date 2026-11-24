import {
  expect,
  type APIRequestContext,
} from "@playwright/test";

export interface NodeStatus {
  id: number;
  role: number;
  term: number;
  leader_id: number;
  commit_index: number;
  applied_index: number;
}

export interface ControlledNode {
  id: number;
  port: number;
  pid?: number;
  running: boolean;
  reachable: boolean;
}

export interface ClusterControl {
  nodes: ControlledNode[];
}

export const nodeURL = (
  id: number,
) =>
  `http://127.0.0.1:${8080 + id}`;

export const supervisorURL =
  "http://127.0.0.1:8090";

export async function nodeStatus(
  request: APIRequestContext,
  id: number,
): Promise<NodeStatus | undefined> {
  try {
    const response =
      await request.get(
        `${nodeURL(id)}/status`,
        {
          timeout: 1500,
        },
      );

    if (!response.ok()) {
      return undefined;
    }

    return (await response.json()) as NodeStatus;
  } catch {
    return undefined;
  }
}

export async function findLeader(
  request: APIRequestContext,
  excluded?: number,
): Promise<NodeStatus> {
  await expect
    .poll(
      async () => {
        for (
          const id of [1, 2, 3]
        ) {
          if (id === excluded) {
            continue;
          }

          const status =
            await nodeStatus(
              request,
              id,
            );

          if (
            status?.role === 2 &&
            status.leader_id ===
              status.id
          ) {
            return status;
          }
        }

        return undefined;
      },
      {
        timeout: 15_000,
        intervals: [
          100,
          200,
          300,
          500,
        ],
        message:
          "waiting for a Raft leader",
      },
    )
    .not.toBeUndefined();

  for (const id of [1, 2, 3]) {
    if (id === excluded) {
      continue;
    }

    const status =
      await nodeStatus(
        request,
        id,
      );

    if (
      status?.role === 2 &&
      status.leader_id ===
        status.id
    ) {
      return status;
    }
  }

  throw new Error(
    "leader disappeared after detection",
  );
}

export async function stopNode(
  request: APIRequestContext,
  id: number,
) {
  const response =
    await request.post(
      `${supervisorURL}/api/v1/control/nodes/${id}/stop`,
    );

  expect(
    response.ok(),
    await response.text(),
  ).toBeTruthy();

  await expect
    .poll(
      async () =>
        (await nodeStatus(
          request,
          id,
        )) === undefined,
      {
        timeout: 10_000,
        message:
          `waiting for Node ${id} to stop`,
      },
    )
    .toBe(true);
}

export async function startNode(
  request: APIRequestContext,
  id: number,
) {
  const response =
    await request.post(
      `${supervisorURL}/api/v1/control/nodes/${id}/start`,
    );

  expect(
    response.ok(),
    await response.text(),
  ).toBeTruthy();

  await expect
    .poll(
      async () =>
        (await nodeStatus(
          request,
          id,
        )) !== undefined,
      {
        timeout: 10_000,
        message:
          `waiting for Node ${id} to start`,
      },
    )
    .toBe(true);
}

export async function startAllNodes(
  request: APIRequestContext,
) {
  const cluster =
    await request.get(
      `${supervisorURL}/api/v1/control/cluster`,
    );

  expect(
    cluster.ok(),
    "supervisor is not available",
  ).toBeTruthy();

  const state =
    (await cluster.json()) as ClusterControl;

  for (const node of state.nodes) {
    if (
      !node.running ||
      !node.reachable
    ) {
      await startNode(
        request,
        node.id,
      );
    }
  }

  await expect
    .poll(
      async () => {
        let online = 0;

        for (
          const id of [1, 2, 3]
        ) {
          if (
            await nodeStatus(
              request,
              id,
            )
          ) {
            online++;
          }
        }

        return online;
      },
      {
        timeout: 15_000,
        message:
          "waiting for all three nodes",
      },
    )
    .toBe(3);
}

export async function putKV(
  request: APIRequestContext,
  nodeId: number,
  key: string,
  value: string,
  requestId: string,
) {
  const response =
    await request.put(
      `${nodeURL(nodeId)}/kv/${encodeURIComponent(
        key,
      )}`,
      {
        data: {
          value,
          client_id:
            "playwright-e2e",
          request_id: requestId,
        },
      },
    );

  expect(
    response.ok(),
    await response.text(),
  ).toBeTruthy();

  const body =
    (await response.json()) as {
      applied?: boolean;
      value?: string;
    };

  expect(body.applied).toBe(true);

  return body;
}

export async function getKV(
  request: APIRequestContext,
  nodeId: number,
  key: string,
): Promise<string | undefined> {
  try {
    const response =
      await request.get(
        `${nodeURL(nodeId)}/kv/${encodeURIComponent(
          key,
        )}`,
      );

    if (!response.ok()) {
      return undefined;
    }

    const body =
      (await response.json()) as {
        value?: string;
      };

    return body.value;
  } catch {
    return undefined;
  }
}

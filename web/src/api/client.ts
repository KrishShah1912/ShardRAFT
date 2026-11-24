import type { EventHistory, NodeStatus } from "../types";

export const nodeBase = (id: number) => `/node${id}`;

async function request<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const response = await fetch(path, options);

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `HTTP ${response.status}`);
  }

  return response.json() as Promise<T>;
}

export async function getNodeStatus(
  id: number,
): Promise<NodeStatus> {
  return request<NodeStatus>(
    `${nodeBase(id)}/api/v1/node`,
  );
}

export async function getClusterStatuses(): Promise<NodeStatus[]> {
  return Promise.all(
    [1, 2, 3].map(async (id) => {
      try {
        const status = await getNodeStatus(id);
        return {
          ...status,
          reachable: true,
        };
      } catch {
        return {
          id,
          role: 0,
          term: 0,
          leader_id: 0,
          commit_index: 0,
          applied_index: 0,
          latest_event_id: 0,
          reachable: false,
        } satisfies NodeStatus;
      }
    }),
  );
}

export async function getEventHistory(
  nodeId: number,
  after = 0,
): Promise<EventHistory> {
  return request<EventHistory>(
    `${nodeBase(nodeId)}/api/v1/events/history?after=${after}`,
  );
}

export async function getKV(
  nodeId: number,
  key: string,
): Promise<{ key: string; value: string }> {
  return request(
    `${nodeBase(nodeId)}/kv/${encodeURIComponent(key)}`,
  );
}

export async function putKV(
  nodeId: number,
  key: string,
  value: string,
  clientId: string,
  requestId: string,
): Promise<unknown> {
  return request(
    `${nodeBase(nodeId)}/kv/${encodeURIComponent(key)}`,
    {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        value,
        client_id: clientId,
        request_id: requestId,
      }),
    },
  );
}

export async function deleteKV(
  nodeId: number,
  key: string,
  clientId: string,
  requestId: string,
): Promise<unknown> {
  return request(
    `${nodeBase(nodeId)}/kv/${encodeURIComponent(key)}`,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        client_id: clientId,
        request_id: requestId,
      }),
    },
  );
}

export async function casKV(
  nodeId: number,
  key: string,
  expected: string,
  value: string,
  clientId: string,
  requestId: string,
): Promise<unknown> {
  return request(
    `${nodeBase(nodeId)}/kv/${encodeURIComponent(key)}/cas`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        expected,
        value,
        client_id: clientId,
        request_id: requestId,
      }),
    },
  );
}

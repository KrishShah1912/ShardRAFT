export interface ControlledNode {
  id: number;
  port: number;
  pid?: number;
  running: boolean;
  reachable: boolean;
  started_at?: string;
  last_error?: string;
}

export interface ControlledCluster {
  nodes: ControlledNode[];
}

async function request<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const response = await fetch(
    `/control${path}`,
    options,
  );

  if (!response.ok) {
    const message = await response.text();

    throw new Error(
      message || `Control API returned ${response.status}`,
    );
  }

  return response.json() as Promise<T>;
}

export function getControlledCluster() {
  return request<ControlledCluster>(
    "/api/v1/control/cluster",
  );
}

export function startControlledNode(id: number) {
  return request<ControlledNode>(
    `/api/v1/control/nodes/${id}/start`,
    {
      method: "POST",
    },
  );
}

export function stopControlledNode(id: number) {
  return request<ControlledNode>(
    `/api/v1/control/nodes/${id}/stop`,
    {
      method: "POST",
    },
  );
}

export function restartControlledNode(id: number) {
  return request<ControlledNode>(
    `/api/v1/control/nodes/${id}/restart`,
    {
      method: "POST",
    },
  );
}

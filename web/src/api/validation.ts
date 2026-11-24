export type ValidationRunState =
  | "idle"
  | "running"
  | "passed"
  | "failed";

export interface ValidationRun {
  state: ValidationRunState;
  started_at?: string;
  ended_at?: string;
  error?: string;
}

async function readError(
  response: Response,
) {
  const text =
    await response.text();

  return (
    text ||
    `HTTP ${response.status}`
  );
}

export async function getValidationRun():
  Promise<ValidationRun> {
  const response = await fetch(
    "/control/api/v1/validation",
    {
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error(
      await readError(response),
    );
  }

  return response.json();
}

export async function startValidationRun():
  Promise<ValidationRun> {
  const response = await fetch(
    "/control/api/v1/validation",
    {
      method: "POST",
    },
  );

  if (!response.ok) {
    throw new Error(
      await readError(response),
    );
  }

  return response.json();
}

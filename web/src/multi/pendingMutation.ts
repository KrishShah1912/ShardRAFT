import type {
  GatewayCommand,
} from "../api/multiGateway";

const STORAGE_KEY =
  "shardraft.multi.pending-mutation.v1";

export function parsePendingMutation(
  raw: string | null,
): GatewayCommand | null {
  if (raw === null) return null;

  const command = JSON.parse(raw);

  if (
    !command ||
    typeof command !== "object" ||
    !["PUT", "DELETE", "CAS"].includes(
      command.operation,
    ) ||
    typeof command.key !== "string" ||
    !command.key.trim() ||
    typeof command.value !== "string" ||
    typeof command.expected !== "string" ||
    typeof command.clientId !== "string" ||
    !command.clientId.trim() ||
    typeof command.requestId !== "string" ||
    !command.requestId.trim() ||
    !["linearizable", "local"].includes(
      command.readMode,
    )
  ) {
    throw new Error(
      "Invalid stored mutation. " +
      "Inspect session storage before continuing.",
    );
  }

  return command as GatewayCommand;
}

export function loadPendingMutation():
  GatewayCommand | null {
  return parsePendingMutation(
    sessionStorage.getItem(STORAGE_KEY),
  );
}

export function savePendingMutation(
  command: GatewayCommand,
): void {
  if (command.operation === "GET") {
    throw new Error(
      "Read requests must not be persisted as mutations.",
    );
  }

  const raw = JSON.stringify(command);

  parsePendingMutation(raw);

  sessionStorage.setItem(STORAGE_KEY, raw);

  if (sessionStorage.getItem(STORAGE_KEY) !== raw) {
    throw new Error(
      "Unable to verify persisted mutation.",
    );
  }
}

export function clearPendingMutation(): void {
  sessionStorage.removeItem(STORAGE_KEY);
}

import {
  LIVE_EVENT_TYPES,
  parseLiveEvent,
  type LiveEvent,
} from "./liveTelemetry";

export type StreamStatus =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "disconnected";

export type StreamFactory =
  (url: string) => EventSource;

export function multiEventStreamURL(
  nodeId: number,
): string {
  if (
    !Number.isInteger(nodeId) ||
    nodeId < 1 ||
    nodeId > 5
  ) {
    throw new Error("Invalid storage node ID");
  }

  return `/multi/node${nodeId}/api/v1/events?after=0`;
}

export function connectMultiEventStream(
  nodeId: number,
  onEvent: (event: LiveEvent) => void,
  onStatus: (status: StreamStatus) => void,
  createStream: StreamFactory =
    url => new EventSource(url),
): () => void {
  const url = multiEventStreamURL(nodeId);

  let active = true;
  let stream: EventSource | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let delay = 1000;

  function reconnect() {
    if (!active || timer !== null) return;

    stream?.close();
    stream = null;

    onStatus("reconnecting");

    timer = setTimeout(() => {
      timer = null;
      connect();
    }, delay);

    delay = Math.min(delay * 2, 10000);
  }

  function connect() {
    if (!active) return;

    onStatus("connecting");

    let current: EventSource;

    try {
      current = createStream(url);
    } catch {
      reconnect();
      return;
    }

    stream = current;

    current.onopen = () => {
      if (!active || stream !== current) return;

      delay = 1000;
      onStatus("connected");
    };

    current.onerror = () => {
      if (!active || stream !== current) return;

      reconnect();
    };

    function receive(message: Event) {
      if (!active || stream !== current) return;

      try {
        const raw = JSON.parse(
          (message as MessageEvent).data,
        );

        const event = parseLiveEvent(raw, nodeId);

        if (event) onEvent(event);
      } catch {
        // Ignore malformed observability messages.
      }
    }

    for (const name of LIVE_EVENT_TYPES) {
      current.addEventListener(name, receive);
    }

    current.addEventListener("message", receive);
  }

  connect();

  return () => {
    active = false;

    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }

    stream?.close();
    stream = null;

    onStatus("disconnected");
  };
}

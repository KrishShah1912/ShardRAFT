import test from "node:test";
import assert from "node:assert/strict";

import {
  multiEventStreamURL,
  connectMultiEventStream,
} from "../.test-build/multi/liveEventStream.js";

class FakeEventSource {
  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    this.closed = false;
    this.onopen = null;
    this.onerror = null;
  }

  addEventListener(name, listener) {
    this.listeners.set(name, listener);
  }

  emit(name, payload) {
    this.listeners.get(name)?.({
      data: JSON.stringify(payload),
    });
  }

  close() {
    this.closed = true;
  }
}

const event = {
  id: 101,
  timestamp: "2026-11-26T23:37:01Z",
  node_id: 2,
  shard_id: 1,
  category: "raft",
  type: "raft.heartbeat.sent",
  term: 9,
  data: {
    to: 1,
    request_id: 100,
  },
};

function setup() {
  const sources = [];
  const events = [];
  const states = [];

  const close = connectMultiEventStream(
    2,
    value => events.push(value),
    value => states.push(value),
    url => {
      const source = new FakeEventSource(url);
      sources.push(source);
      return source;
    },
  );

  return {
    sources,
    events,
    states,
    close,
  };
}

test("uses correct URLs for all five nodes", () => {
  for (let node = 1; node <= 5; node++) {
    assert.equal(
      multiEventStreamURL(node),
      `/multi/node${node}/api/v1/events?after=0`,
    );
  }
});

test("rejects invalid node IDs", () => {
  for (const node of [0, 6, -1, 1.5, NaN]) {
    assert.throws(() => multiEventStreamURL(node));
  }
});

test("reports connecting and connected states", () => {
  const state = setup();

  try {
    assert.deepEqual(state.states, ["connecting"]);

    state.sources[0].onopen();

    assert.equal(state.states.at(-1), "connected");
  } finally {
    state.close();
  }
});

test("receives valid named heartbeat events", () => {
  const state = setup();

  try {
    state.sources[0].emit(event.type, event);

    assert.deepEqual(state.events, [event]);
  } finally {
    state.close();
  }
});

test("receives default message events", () => {
  const state = setup();

  try {
    state.sources[0].emit("message", event);

    assert.equal(state.events.length, 1);
  } finally {
    state.close();
  }
});

test("rejects events from incorrect nodes", () => {
  const state = setup();

  try {
    state.sources[0].emit(event.type, {
      ...event,
      node_id: 3,
    });

    assert.equal(state.events.length, 0);
  } finally {
    state.close();
  }
});

test("ignores malformed JSON", () => {
  const state = setup();

  try {
    state.sources[0].listeners.get(event.type)({
      data: "{invalid-json",
    });

    assert.equal(state.events.length, 0);
  } finally {
    state.close();
  }
});

test("closes failed stream and schedules reconnection", () => {
  const state = setup();

  try {
    state.sources[0].onerror();

    assert.equal(state.sources[0].closed, true);
    assert.equal(state.states.at(-1), "reconnecting");
  } finally {
    state.close();
  }
});

test("reconnects using a fresh event cursor", async () => {
  const state = setup();

  try {
    state.sources[0].onerror();

    await new Promise(resolve =>
      setTimeout(resolve, 1200),
    );

    assert.equal(state.sources.length, 2);

    assert.equal(
      state.sources[1].url,
      multiEventStreamURL(2),
    );

    state.sources[1].onopen();

    assert.equal(state.states.at(-1), "connected");

    state.sources[0].emit(event.type, event);

    assert.equal(state.events.length, 0);

    state.sources[1].emit(event.type, event);

    assert.equal(state.events.length, 1);
  } finally {
    state.close();
  }
});

test("cleanup closes stream and ignores late events", () => {
  const state = setup();

  state.close();

  assert.equal(state.sources[0].closed, true);
  assert.equal(state.states.at(-1), "disconnected");

  state.sources[0].emit(event.type, event);

  assert.equal(state.events.length, 0);
});

test("cleanup cancels scheduled reconnection", async () => {
  const state = setup();

  state.sources[0].onerror();
  state.close();

  await new Promise(resolve =>
    setTimeout(resolve, 1200),
  );

  assert.equal(state.sources.length, 1);
});

import { test } from "node:test";
import { strict as assert } from "node:assert";
import {
  WorkerDispatcher,
  QueueFullError,
  SupersededTaskError,
  CancelledTaskError,
} from "../dist/src/dispatch.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const tick = () => new Promise(resolve => setImmediate(resolve));

test("dispatch respects concurrency and drains FIFO after a worker slot frees", async () => {
  const gates = [];
  const started = [];
  const dispatcher = new WorkerDispatcher((task) => {
    started.push(task.id);
    const gate = deferred();
    gates.push(gate);
    return gate.promise;
  }, { concurrency: 2, maxQueue: 2 });

  const a = dispatcher.submit({ id: "a", session: "s-a", payload: 1 });
  const b = dispatcher.submit({ id: "b", session: "s-b", payload: 2 });
  const c = dispatcher.submit({ id: "c", session: "s-c", payload: 3 });
  await tick();
  assert.deepEqual(started, ["a", "b"]);
  assert.deepEqual(dispatcher.snapshot(), { active: 2, queued: 1, sessions: 3, closed: false });

  gates[0].resolve("A");
  assert.equal(await a, "A");
  await tick();
  assert.deepEqual(started, ["a", "b", "c"]);
  gates[1].resolve("B");
  gates[2].resolve("C");
  assert.deepEqual(await Promise.all([b, c]), ["B", "C"]);
  assert.equal(dispatcher.snapshot().active, 0);
});

test("a newer session epoch aborts and fences a late worker result", async () => {
  const oldGate = deferred();
  let oldSignal;
  const dispatcher = new WorkerDispatcher((task, context) => {
    if (task.id === "old") {
      oldSignal = context.signal;
      return oldGate.promise;
    }
    return "result:" + task.payload;
  }, { concurrency: 1 });

  const oldTask = dispatcher.submit({ id: "old", session: "chat-1", payload: "stale" });
  await tick();
  const newTask = dispatcher.submit({ id: "new", session: "chat-1", payload: "fresh" });
  await assert.rejects(oldTask, SupersededTaskError);
  assert.equal(oldSignal.aborted, true);
  oldGate.resolve("must-not-commit");
  assert.equal(await newTask, "result:fresh");
  assert.equal(dispatcher.snapshot().active, 0);
});

test("duplicate task IDs are idempotent and do not dispatch twice", async () => {
  let calls = 0;
  const gate = deferred();
  const dispatcher = new WorkerDispatcher(() => {
    calls += 1;
    return gate.promise;
  }, { concurrency: 1 });

  const first = dispatcher.submit({ id: "same-id", session: "s", payload: "x" });
  const duplicate = dispatcher.submit({ id: "same-id", session: "s", payload: "x" });
  assert.equal(first, duplicate);
  await tick();
  assert.equal(calls, 1);
  gate.resolve("one execution");
  assert.equal(await first, "one execution");
});

test("retry policy retries only retryable failures up to maxAttempts", async () => {
  const attempts = [];
  const dispatcher = new WorkerDispatcher((_task, context) => {
    attempts.push(context.attempt);
    if (context.attempt < 3) throw Object.assign(new Error("transient"), { retryable: true });
    return "recovered";
  }, {
    concurrency: 1,
    maxAttempts: 3,
    retryDelayMs: 0,
    shouldRetry: error => error?.retryable === true,
  });

  assert.equal(await dispatcher.submit({ id: "retry", session: "s", payload: null }), "recovered");
  assert.deepEqual(attempts, [1, 2, 3]);
});

test("non-retryable failure is surfaced without replay", async () => {
  let calls = 0;
  const dispatcher = new WorkerDispatcher(() => {
    calls += 1;
    throw new Error("permanent");
  }, {
    concurrency: 1,
    maxAttempts: 4,
    shouldRetry: () => false,
  });

  await assert.rejects(dispatcher.submit({ id: "bad", session: "s", payload: 0 }), /permanent/);
  assert.equal(calls, 1);
});

test("cancelSession aborts running work and rejects its caller", async () => {
  const gate = deferred();
  let signal;
  const dispatcher = new WorkerDispatcher((_task, context) => {
    signal = context.signal;
    return gate.promise;
  }, { concurrency: 1 });

  const pending = dispatcher.submit({ id: "cancel-me", session: "s", payload: 0 });
  await tick();
  assert.equal(dispatcher.cancelSession("s"), true);
  await assert.rejects(pending, CancelledTaskError);
  assert.equal(signal.aborted, true);
  gate.resolve("late");
  await tick();
  assert.equal(dispatcher.snapshot().active, 0);
});

test("bounded waiting queue rejects overload instead of growing without limit", async () => {
  const gate = deferred();
  const dispatcher = new WorkerDispatcher(() => gate.promise, { concurrency: 1, maxQueue: 1 });
  const first = dispatcher.submit({ id: "one", session: "one", payload: 1 });
  const second = dispatcher.submit({ id: "two", session: "two", payload: 2 });
  assert.throws(() => dispatcher.submit({ id: "three", session: "three", payload: 3 }), QueueFullError);
  gate.resolve("done");
  await Promise.all([first, second]);
});

test("sessions do not supersede one another", async () => {
  const gates = new Map();
  const dispatcher = new WorkerDispatcher(task => {
    const gate = deferred();
    gates.set(task.id, gate);
    return gate.promise;
  }, { concurrency: 2 });

  const left = dispatcher.submit({ id: "left", session: "left-session", payload: 1 });
  const right = dispatcher.submit({ id: "right", session: "right-session", payload: 2 });
  await tick();
  gates.get("left").resolve("L");
  gates.get("right").resolve("R");
  assert.deepEqual(await Promise.all([left, right]), ["L", "R"]);
});

test("a delayed retry also obeys queue backpressure", async () => {
  const blockerGate = deferred();
  const queuedGate = deferred();
  let retryAttempts = 0;
  const dispatcher = new WorkerDispatcher(task => {
    if (task.id === "retrying") {
      retryAttempts += 1;
      if (retryAttempts === 1) throw Object.assign(new Error("temporary"), { retryable: true });
      return "unexpected retry";
    }
    if (task.id === "blocker") return blockerGate.promise;
    return queuedGate.promise;
  }, {
    concurrency: 1,
    maxQueue: 1,
    maxAttempts: 2,
    retryDelayMs: 10,
    shouldRetry: error => error?.retryable === true,
  });

  const retrying = dispatcher.submit({ id: "retrying", session: "retry-session", payload: null });
  await tick();
  const blocker = dispatcher.submit({ id: "blocker", session: "blocker-session", payload: null });
  const queued = dispatcher.submit({ id: "queued", session: "queued-session", payload: null });
  await assert.rejects(retrying, QueueFullError);
  assert.equal(retryAttempts, 1);
  blockerGate.resolve("blocker done");
  queuedGate.resolve("queued done");
  assert.deepEqual(await Promise.all([blocker, queued]), ["blocker done", "queued done"]);
});

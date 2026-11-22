/**
 * Bounded, per-session worker dispatch coordinator.
 *
 * This is an in-process scheduling/fencing layer around an injected worker
 * executor. It is not a distributed lock, durable queue, or exactly-once system.
 */
export type DispatchTask<TPayload> = Readonly<{
  id: string;
  session: string;
  payload: TPayload;
}>;

export type WorkerContext = Readonly<{
  attempt: number;
  epoch: bigint;
  signal: AbortSignal;
}>;

export type WorkerExecutor<TPayload, TResult> = (
  task: DispatchTask<TPayload>,
  context: WorkerContext,
) => Promise<TResult> | TResult;

export type DispatchOptions = Readonly<{
  concurrency: number;
  /** Maximum number of waiting tasks, excluding currently running executors. */
  maxQueue?: number;
  /** Number of terminal task IDs retained for duplicate-delivery idempotency. */
  rememberedTasks?: number;
  maxAttempts?: number;
  retryDelayMs?: number;
  shouldRetry?: (error: unknown, attempt: number) => boolean;
}>;

export class DispatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DispatchError";
  }
}

export class QueueFullError extends DispatchError {
  constructor() {
    super("worker dispatch queue is full");
    this.name = "QueueFullError";
  }
}

export class SupersededTaskError extends DispatchError {
  constructor(taskId: string, session: string) {
    super("task " + taskId + " was superseded in session " + session);
    this.name = "SupersededTaskError";
  }
}

export class CancelledTaskError extends DispatchError {
  constructor(taskId: string) {
    super("task " + taskId + " was cancelled");
    this.name = "CancelledTaskError";
  }
}

type State = "queued" | "running" | "waiting-retry" | "completed" | "failed" | "cancelled";
type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
};
type TaskRecord<TPayload, TResult> = {
  task: DispatchTask<TPayload>;
  epoch: bigint;
  controller: AbortController;
  state: State;
  attempt: number;
  settled: boolean;
  deferred: Deferred<TResult>;
  retryTimer: ReturnType<typeof setTimeout> | null;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function terminal(state: State): boolean {
  return state === "completed" || state === "failed" || state === "cancelled";
}

export class WorkerDispatcher<TPayload, TResult> {
  private readonly queue: TaskRecord<TPayload, TResult>[] = [];
  private readonly records = new Map<string, TaskRecord<TPayload, TResult>>();
  private readonly recordOrder: string[] = [];
  private readonly epochs = new Map<string, bigint>();
  private readonly currentBySession = new Map<string, TaskRecord<TPayload, TResult>>();
  private active = 0;
  private closed = false;
  private readonly maxQueue: number;
  private readonly rememberedTasks: number;
  private readonly maxAttempts: number;
  private readonly retryDelayMs: number;
  private readonly shouldRetry: (error: unknown, attempt: number) => boolean;

  constructor(
    private readonly execute: WorkerExecutor<TPayload, TResult>,
    private readonly options: DispatchOptions,
  ) {
    if (!Number.isSafeInteger(options.concurrency) || options.concurrency < 1) {
      throw new RangeError("concurrency must be a positive safe integer");
    }
    this.maxQueue = options.maxQueue ?? 64;
    this.rememberedTasks = options.rememberedTasks ?? 1024;
    this.maxAttempts = options.maxAttempts ?? 1;
    this.retryDelayMs = options.retryDelayMs ?? 0;
    this.shouldRetry = options.shouldRetry ?? (() => false);
    for (const [name, value] of [
      ["maxQueue", this.maxQueue],
      ["rememberedTasks", this.rememberedTasks],
      ["maxAttempts", this.maxAttempts],
      ["retryDelayMs", this.retryDelayMs],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < (name === "retryDelayMs" ? 0 : 1)) {
        throw new RangeError(name + " has an invalid value");
      }
    }
  }

  /**
   * Submitting a newer task for the same session supersedes the previous task.
   * Repeated task IDs return the original promise instead of dispatching twice.
   */
  submit(task: DispatchTask<TPayload>): Promise<TResult> {
    if (this.closed) throw new DispatchError("dispatcher is closed");
    if (!task.id.trim() || !task.session.trim()) {
      throw new TypeError("task id and session must be non-empty");
    }

    const duplicate = this.records.get(task.id);
    if (duplicate) return duplicate.deferred.promise;

    const previous = this.currentBySession.get(task.session);
    const effectiveQueueLength = this.queue.length - (previous?.state === "queued" ? 1 : 0);
    if (this.active >= this.options.concurrency && effectiveQueueLength >= this.maxQueue) {
      throw new QueueFullError();
    }

    if (previous && !terminal(previous.state)) {
      this.supersede(previous);
    }

    const epoch = (this.epochs.get(task.session) ?? 0n) + 1n;
    this.epochs.set(task.session, epoch);
    const record: TaskRecord<TPayload, TResult> = {
      task,
      epoch,
      controller: new AbortController(),
      state: "queued",
      attempt: 0,
      settled: false,
      deferred: deferred<TResult>(),
      retryTimer: null,
    };
    this.records.set(task.id, record);
    this.recordOrder.push(task.id);
    this.currentBySession.set(task.session, record);
    this.queue.push(record);
    this.pruneHistory();
    this.pump();
    return record.deferred.promise;
  }

  /** Cancel the current operation for a session and invalidate its epoch. */
  cancelSession(session: string): boolean {
    const record = this.currentBySession.get(session);
    if (!record || terminal(record.state)) return false;
    this.epochs.set(session, (this.epochs.get(session) ?? record.epoch) + 1n);
    this.currentBySession.delete(session);
    this.cancelRecord(record, new CancelledTaskError(record.task.id));
    return true;
  }

  /** Close admission and abort all current sessions. Running executors must observe AbortSignal. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const [session, record] of this.currentBySession) {
      this.epochs.set(session, (this.epochs.get(session) ?? record.epoch) + 1n);
      this.cancelRecord(record, new CancelledTaskError(record.task.id));
    }
    this.currentBySession.clear();
    this.queue.length = 0;
  }

  snapshot(): Readonly<{ active: number; queued: number; sessions: number; closed: boolean }> {
    return Object.freeze({
      active: this.active,
      queued: this.queue.length,
      sessions: this.currentBySession.size,
      closed: this.closed,
    });
  }

  private isCurrent(record: TaskRecord<TPayload, TResult>): boolean {
    return this.currentBySession.get(record.task.session) === record &&
      this.epochs.get(record.task.session) === record.epoch &&
      !record.controller.signal.aborted;
  }

  private settle(record: TaskRecord<TPayload, TResult>, state: "completed" | "failed" | "cancelled", value: TResult | unknown, reject: boolean): void {
    if (record.settled) return;
    record.settled = true;
    record.state = state;
    if (record.retryTimer !== null) {
      clearTimeout(record.retryTimer);
      record.retryTimer = null;
    }
    if (reject) record.deferred.reject(value);
    else record.deferred.resolve(value as TResult);
    this.pruneHistory();
  }

  private supersede(record: TaskRecord<TPayload, TResult>): void {
    this.cancelRecord(record, new SupersededTaskError(record.task.id, record.task.session));
  }

  private cancelRecord(record: TaskRecord<TPayload, TResult>, reason: Error): void {
    if (terminal(record.state)) return;
    record.controller.abort(reason);
    if (record.retryTimer !== null) {
      clearTimeout(record.retryTimer);
      record.retryTimer = null;
    }
    const index = this.queue.indexOf(record);
    if (index >= 0) this.queue.splice(index, 1);
    this.settle(record, "cancelled", reason, true);
  }

  private pump(): void {
    while (!this.closed && this.active < this.options.concurrency && this.queue.length > 0) {
      const record = this.queue.shift();
      if (!record || record.settled || !this.isCurrent(record)) continue;
      record.state = "running";
      record.attempt += 1;
      this.active += 1;
      void Promise.resolve()
        .then(() => this.execute(record.task, {
          attempt: record.attempt,
          epoch: record.epoch,
          signal: record.controller.signal,
        }))
        .then(
          result => this.onSuccess(record, result),
          error => this.onFailure(record, error),
        );
    }
  }

  private onSuccess(record: TaskRecord<TPayload, TResult>, result: TResult): void {
    this.active -= 1;
    if (!record.settled) {
      if (this.isCurrent(record)) this.settle(record, "completed", result, false);
      else this.settle(record, "cancelled", new SupersededTaskError(record.task.id, record.task.session), true);
    }
    this.pump();
  }

  private onFailure(record: TaskRecord<TPayload, TResult>, error: unknown): void {
    this.active -= 1;
    if (record.settled) {
      this.pump();
      return;
    }
    if (!this.isCurrent(record)) {
      this.settle(record, "cancelled", new SupersededTaskError(record.task.id, record.task.session), true);
    } else if (record.attempt < this.maxAttempts && this.shouldRetry(error, record.attempt)) {
      record.state = "waiting-retry";
      record.retryTimer = setTimeout(() => {
        record.retryTimer = null;
        if (!record.settled && this.isCurrent(record)) {
          record.state = "queued";
          this.queue.push(record);
          this.pump();
        }
      }, this.retryDelayMs);
    } else {
      this.settle(record, "failed", error, true);
    }
    this.pump();
  }

  private pruneHistory(): void {
    while (this.records.size > this.rememberedTasks && this.recordOrder.length > 0) {
      const id = this.recordOrder.shift();
      if (id === undefined) break;
      const record = this.records.get(id);
      if (!record) continue;
      if (terminal(record.state)) this.records.delete(id);
      else this.recordOrder.push(id);
      if (this.recordOrder.every(key => {
        const candidate = this.records.get(key);
        return !candidate || !terminal(candidate.state);
      })) break;
    }
  }
}

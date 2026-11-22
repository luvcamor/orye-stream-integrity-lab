# Orye Stream Integrity Lab

**Adversarial reference implementation for AI streaming transport integrity, worker dispatch, and UI epoch fencing.**

This is an independent public engineering lab associated with the private Orye Windows AI desktop project. It is **not** the full Orye application, a deployed staging service, or a claim of production certification.

## Why it is difficult

Network chunks do not respect UTF-8 code points, CRLF pairs, SSE lines, or event boundaries. Simultaneously, a canceled or superseded AI request may continue yielding network bytes. Correct clients must preserve parser invariants while preventing stale responses from mutating newer UI state. Dispatch adds another race boundary: bounded concurrency, duplicate deliveries, retries, cancellation, and late worker completion must not cross session ownership.

### Components

- `src/sse.ts`: strict, bounded byte-incremental SSE parser. Handles arbitrary fragmentation, CRLF, BOM, comments, IDs, retry fields, UTF-8 validation, and maximum line/event sizes.
- `src/normalizer.ts`: narrow mapping from OpenAI Responses, Anthropic Messages, and OpenAI-compatible Chat Completions to a renderer-neutral text-event vocabulary.
- `src/epoch.ts`: per-session monotonically increasing epoch ownership and terminal-state fencing.
- `src/dispatch.ts`: bounded FIFO worker-dispatch coordinator with per-session supersession, AbortSignal cancellation, task-ID deduplication, retry policy, and queue backpressure.
- `test/sse.test.mjs`: adversarial split-boundary, deterministic fragmentation, malformed input, and late-arrival tests.
- `test/dispatch.test.mjs`: deterministic worker-executor regressions for concurrency limits, FIFO draining, stale completion fencing, duplicate delivery, retry classification, cancellation, queue saturation, and session isolation.

The dispatch coordinator accepts an injected executor. The regression suite uses controlled Promise-based executors to deterministically force race conditions; it does **not** claim to launch a browser Worker or exercise a production Orye runtime.

## Quick start

Requires Node.js >=22.

```bash
npm install
npm test
npm run check
```

## Test scenarios

1. Every possible two-way split of a UTF-8, CRLF and BOM-containing event yields the same result.
2. Byte-at-a-time delivery produces identical events.
3. 150 deterministic xorshift fragmentations match the unfragmented baseline.
4. Invalid UTF-8 and exceeded line/event budgets fail closed.
5. Incomplete final events are discarded on EOF.
6. Old request completion and late chunks cannot overwrite a newer epoch.
7. OpenAI/Anthropic text deltas are normalized; malformed known deltas are rejected.
8. Worker execution never exceeds configured concurrency and queued work drains FIFO.
9. A newer task in the same session aborts and fences the older task's late result.
10. Duplicate task IDs share one promise and do not execute twice while retained in the idempotency window.
11. Retryable errors are retried only under the configured policy and attempt limit.
12. Cancellation, queue saturation, and cross-session isolation are covered explicitly.

## Threat model and non-goals

- Assumes byte order and integrity of the delivered stream. It does not reconstruct missing bytes.
- Epoch tokens fence in-process UI mutation; they are **not distributed fencing tokens**.
- The dispatch queue and task-ID history are in-memory; they do not survive process restart.
- AbortSignal is cooperative. An executor that ignores cancellation can continue occupying a worker slot until it settles.
- The dispatcher does not provide durable delivery, exactly-once side effects, distributed locking, or upstream cancellation guarantees.
- Parser correctness does not imply resumable delivery or exactly-once side effects.
- This is an independent reference implementation; provider API coverage is intentionally partial.
- No private Orye source code or credentials are included.

## Architecture

```text
Untrusted chunk boundaries
          |
          v
Strict bounded SSE decoder --ProtocolError--> fail closed
          |
          v
Provider event normalizer
          |
          v
Per-session epoch arbiter --stale token--> reject
          |
          v
Bounded worker dispatcher --queue full--> backpressure
          |                     |
          |                     +-- retry policy / AbortSignal
          v
Current session result only --stale completion--> discard
          |
          v
UI-visible text / terminal state
```

## License

MIT. See LICENSE.

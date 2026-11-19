# Orye Stream Integrity Lab

**Adversarial reference implementation for AI streaming transport integrity and UI epoch fencing.**

This is an independent public engineering lab associated with the private Orye Windows AI desktop project. It is **not** the full Orye application, a deployed staging service, or a claim of production certification.

## Why it is difficult

Network chunks do not respect UTF-8 code points, CRLF pairs, SSE lines, or event boundaries. Simultaneously, a canceled or superseded AI request may continue yielding network bytes. Correct clients must preserve parser invariants while preventing stale responses from mutating newer UI state.

### Components

- `src/sse.ts`: strict, bounded byte-incremental SSE parser. Handles arbitrary fragmentation, CRLF, BOM, comments, IDs, retry fields, UTF-8 validation, and maximum line/event sizes.
- `src/normalizer.ts`: narrow mapping from OpenAI Responses, Anthropic Messages, and OpenAI-compatible Chat Completions to a renderer-neutral text-event vocabulary.
- `src/epoch.ts`: per-session monotonically increasing epoch ownership and terminal-state fencing.
- `test/sse.test.mjs`: adversarial split-boundary, deterministic fragmentation, malformed input, and late-arrival tests.

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

## Threat model and non-goals

- Assumes byte order and integrity of the delivered stream. It does not reconstruct missing bytes.
- Epoch tokens fence in-process UI mutation; they are **not distributed fencing tokens**.
- Parser correctness does not imply resumable delivery, exactly-once side effects, or upstream cancellation.
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
UI-visible text / terminal state
```

## License

MIT. See LICENSE.

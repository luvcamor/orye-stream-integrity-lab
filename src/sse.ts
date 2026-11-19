/**
 * Strict, bounded, byte-incremental SSE decoder. A transport may split a UTF-8
 * scalar, CRLF pair, SSE field, or frame at any boundary. We decode only complete
 * lines and never interpret surrogate replacement as valid transport input.
 *
 * WHAT THIS DOES NOT GUARANTEE: this is not a resumable delivery protocol and
 * it cannot infer missing bytes or whether the server executed a side effect.
 */
export type SseEvent = Readonly<{
  event: string;
  data: string;
  id: string | null;
  retry: number | null;
}>;

export class ProtocolError extends Error {
  constructor(message: string) { super(message); this.name = 'ProtocolError'; }
}

export type SseLimits = Readonly<{
  maxLineBytes: number;
  maxEventBytes: number;
}>;

export const DEFAULT_LIMITS: SseLimits = Object.freeze({
  maxLineBytes: 64 * 1024,
  maxEventBytes: 1024 * 1024,
});

export class SseDecoder {
  private pending: number[] = [];
  private skipNextLf = false;
  private firstLine = true;
  private data: string[] = [];
  private eventName = '';
  private lastEventId: string | null = null;
  private retry: number | null = null;
  private eventBytes = 0;
  private finished = false;
  private readonly decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

  constructor(private readonly limits: SseLimits = DEFAULT_LIMITS) {
    if (!Number.isSafeInteger(limits.maxLineBytes) || limits.maxLineBytes < 1 ||
        !Number.isSafeInteger(limits.maxEventBytes) || limits.maxEventBytes < 1) {
      throw new RangeError('limits must be positive safe integers');
    }
  }

  push(chunk: Uint8Array): SseEvent[] {
    if (this.finished) throw new ProtocolError('cannot push after finish');
    const out: SseEvent[] = [];
    for (const byte of chunk) {
      if (this.skipNextLf) {
        this.skipNextLf = false;
        if (byte === 10) continue;
      }
      if (byte === 13 || byte === 10) {
        out.push(...this.consumeLine());
        if (byte === 13) this.skipNextLf = true;
      } else {
        this.pending.push(byte);
        if (this.pending.length > this.limits.maxLineBytes) {
          throw new ProtocolError('SSE line exceeds maxLineBytes');
        }
      }
    }
    return out;
  }

  /** EOF processes the final unterminated line but never dispatches an unclosed event. */
  finish(): SseEvent[] {
    if (this.finished) throw new ProtocolError('finish called twice');
    this.finished = true;
    if (!this.pending.length) return [];
    return this.consumeLine();
  }

  private consumeLine(): SseEvent[] {
    let line: string;
    try { line = this.decoder.decode(new Uint8Array(this.pending)); }
    catch { throw new ProtocolError('invalid UTF-8 in SSE line'); }
    this.pending = [];
    if (this.firstLine) {
      this.firstLine = false;
      if (line.startsWith('\uFEFF')) line = line.slice(1);
    }
    if (line.length === 0) {
      const result: SseEvent[] = this.data.length
        ? [{event: this.eventName || 'message', data: this.data.join('\n'), id: this.lastEventId, retry: this.retry}]
        : [];
      this.data = [];
      this.eventName = '';
      this.eventBytes = 0;
      return result;
    }
    if (line.startsWith(':')) return [];
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    switch (field) {
      case 'data':
        this.eventBytes += new TextEncoder().encode(value).length + 1;
        if (this.eventBytes > this.limits.maxEventBytes) {
          throw new ProtocolError('SSE event exceeds maxEventBytes');
        }
        this.data.push(value); break;
      case 'event': this.eventName = value; break;
      case 'id': if (!value.includes('\u0000')) this.lastEventId = value; break;
      case 'retry': if (/^[0-9]+$/.test(value)) {
        const n = Number(value);
        if (Number.isSafeInteger(n)) this.retry = n;
      } break;
      default: break;
    }
    return [];
  }
}

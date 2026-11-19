/**
 * Narrow interoperability layer, deliberately fails closed on malformed known events.
 * OpenAI Responses, Anthropic Messages and OpenAI-compatible chat completion chunks
 * are mapped to a minimal renderer-neutral event vocabulary.
 */
export type AiEvent = Readonly<
  | { kind: 'text.delta'; text: string }
  | { kind: 'completed' }
  | { kind: 'failed'; reason: string }
  | { kind: 'ignored'; type: string }
>;

import type { SseEvent } from './sse.js';
import { ProtocolError } from './sse.js';

function object(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new ProtocolError('expected JSON object');
  }
  return value as Record<string, unknown>;
}
function stringField(o: Record<string, unknown>, key: string): string {
  if (typeof o[key] !== 'string') throw new ProtocolError(`missing string ${key}`);
  return o[key];
}

export function normalize(event: SseEvent): AiEvent[] {
  if (event.data === '[DONE]') return [{kind: 'completed'}];
  let payload: Record<string, unknown>;
  try { payload = object(JSON.parse(event.data)); }
  catch { throw new ProtocolError('invalid JSON event payload'); }
  const type = typeof payload.type === 'string' ? payload.type : event.event;
  if (type === 'response.output_text.delta') {
    return [{kind: 'text.delta', text: stringField(payload, 'delta')}];
  }
  if (type === 'response.completed' || type === 'message_stop') return [{kind: 'completed'}];
  if (type === 'response.failed' || type === 'response.incomplete' || type === 'error') {
    return [{kind: 'failed', reason: JSON.stringify(payload.error ?? payload.response ?? payload)}];
  }
  if (type === 'content_block_delta') {
    const delta = object(payload.delta);
    if (delta.type === 'text_delta') return [{kind: 'text.delta', text: stringField(delta, 'text')}];
    return [{kind: 'ignored', type: String(delta.type ?? 'unknown-delta')}];
  }
  if (Array.isArray(payload.choices)) {
    const events: AiEvent[] = [];
    for (const rawChoice of payload.choices) {
      const choice = object(rawChoice);
      if (choice.delta !== undefined) {
        const delta = object(choice.delta);
        if (typeof delta.content === 'string') events.push({kind: 'text.delta', text: delta.content});
      }
      if (choice.finish_reason !== null && choice.finish_reason !== undefined) {
        events.push({kind: 'completed'});
      }
    }
    return events;
  }
  return [{kind: 'ignored', type}];
}

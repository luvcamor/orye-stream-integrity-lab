import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { SseDecoder, ProtocolError } from '../dist/src/sse.js';
import { EpochArbiter } from '../dist/src/epoch.js';
import { normalize } from '../dist/src/normalizer.js';

const encoder = new TextEncoder();
function parse(chunks) {
  const decoder = new SseDecoder();
  const result = [];
  for (const chunk of chunks) result.push(...decoder.push(chunk));
  result.push(...decoder.finish());
  return result;
}

test('all byte boundaries yield identical events (multi-byte UTF-8 + CRLF + BOM)', () => {
  const bytes = encoder.encode('\uFEFFid: a\r\nevent: message\r\ndata: 你好🙂\r\ndata: line 2\r\n\r\n');
  const baseline = parse([bytes]);
  for (let split = 0; split <= bytes.length; split++) {
    assert.deepEqual(parse([bytes.subarray(0, split), bytes.subarray(split)]), baseline);
  }
  const byteChunks = [...bytes].map((_, i) => bytes.subarray(i, i + 1));
  assert.deepEqual(parse(byteChunks), baseline);
  assert.equal(baseline[0]?.data, '你好🙂\nline 2');
  assert.equal(baseline[0]?.id, 'a');
});

test('a bare CR terminates a line and LF following CR is swallowed', () => {
  const data = encoder.encode('data: one\r\rdata: two\n\n');
  assert.deepEqual(parse([data]).map(x => x.data), ['one', 'two']);
});

test('comments and unknown fields do not dispatch events', () => {
  assert.equal(parse([encoder.encode(': ping\nfoo: bar\n\n')]).length, 0);
});

test('last event id survives dispatch, invalid NUL id is ignored', () => {
  const x = parse([encoder.encode('id: key\ndata: 1\n\nid: bad\u0000id\ndata: 2\n\n')]);
  assert.deepEqual(x.map(e => e.id), ['key', 'key']);
});

test('reject malformed utf8, overlong line, and oversized event', () => {
  assert.throws(() => parse([Uint8Array.of(100, 97, 116, 97, 58, 32, 0xff, 10)]), ProtocolError);
  assert.throws(() => new SseDecoder({maxLineBytes: 4, maxEventBytes: 99}).push(encoder.encode('data: long\n')), ProtocolError);
  assert.throws(() => new SseDecoder({maxLineBytes: 99, maxEventBytes: 2}).push(encoder.encode('data: abc\n')), ProtocolError);
});

test('incomplete final event is not dispatched at EOF', () => {
  assert.deepEqual(parse([encoder.encode('data: incomplete')]), []);
});

test('deterministic xorshift fragmentation differential over 150 seeds', () => {
  const input = encoder.encode('event: message\ndata: αβγ🙂\n\ndata: B\n\nevent: done\ndata: end\n\n');
  const baseline = parse([input]);
  for (let seed = 1; seed <= 150; seed++) {
    let x = seed;
    const chunks = [];
    for (let i = 0; i < input.length;) {
      x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
      const width = 1 + ((x >>> 0) % 13);
      chunks.push(input.subarray(i, i + width)); i += width;
    }
    assert.deepEqual(parse(chunks), baseline, `seed ${seed}`);
  }
});

test('epoch fence prevents stale completion, cancellation and late bytes', () => {
  const arbiter = new EpochArbiter();
  const a = arbiter.begin('session-1');
  assert.equal(arbiter.append(a, 'old'), true);
  const b = arbiter.begin('session-1');
  assert.equal(arbiter.append(a, 'SHOULD-NOT-APPEAR'), false);
  assert.equal(arbiter.complete(a), false);
  assert.equal(arbiter.append(b, 'new'), true);
  assert.equal(arbiter.complete(b), true);
  assert.equal(arbiter.append(b, 'late'), false);
  assert.equal(arbiter.snapshot('session-1').text, 'new');
});

test('normalizes provider text deltas and completion, rejects malformed known delta', () => {
  const event = (data, type = 'message') => ({event: type, data, id: null, retry: null});
  assert.deepEqual(normalize(event('{"type":"response.output_text.delta","delta":"x"}')), [{kind:'text.delta', text:'x'}]);
  assert.deepEqual(normalize(event('{"type":"content_block_delta","delta":{"type":"text_delta","text":"y"}}')), [{kind:'text.delta', text:'y'}]);
  assert.deepEqual(normalize(event('[DONE]')), [{kind:'completed'}]);
  assert.throws(() => normalize(event('{"type":"response.output_text.delta","delta":14}')), ProtocolError);
});

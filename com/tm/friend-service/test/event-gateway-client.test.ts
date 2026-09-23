// Client gọi event-gateway với fetch giả: body, retry khi lỗi mạng / 5xx, không retry khi 4xx.

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventGatewayError, type FriendEventPayload, createEventGatewayClient } from '../src/dao/event-gateway-client.js';

const event: FriendEventPayload = {
  user_id: 1,
  friend_id: 2,
  event_type: 'REQUESTED',
  event_time: '2026-09-23 10:00:00.000',
  event_id: '0228440659025985536',
  source: 'friend-service',
};

const settings = { eventGatewayUrl: 'http://gateway:8080', eventGatewayTimeoutMs: 1000, eventGatewayMaxAttempts: 3 };

/** fetch giả trả lần lượt các phản hồi; phần tử là Error thì throw (lỗi mạng) */
function fakeFetch(responses: (Response | Error)[]) {
  const calls: { url: string; init: RequestInit | undefined }[] = [];
  const fn = (async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), init });
    const next = responses[calls.length - 1];
    if (!next) throw new Error('hết phản hồi giả');
    if (next instanceof Error) throw next;
    return next;
  }) as typeof fetch;
  return { fn, calls };
}

const noSleep = async () => {};

test('POST /v1/friend-events với body {events}', async () => {
  const { fn, calls } = fakeFetch([new Response('{"accepted":1}', { status: 200 })]);
  await createEventGatewayClient(settings, { fetch: fn, sleep: noSleep }).publishFriendEvents([event]);
  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, 'http://gateway:8080/v1/friend-events');
  assert.equal(calls[0]?.init?.method, 'POST');
  assert.deepEqual(JSON.parse(String(calls[0]?.init?.body)), { events: [event] });
});

test('lỗi mạng / 503 → retry, thành công ở lần 3', async () => {
  const { fn, calls } = fakeFetch([
    new Error('ECONNREFUSED'),
    new Response('{"error":"KAFKA_UNAVAILABLE"}', { status: 503 }),
    new Response('{"accepted":1}', { status: 200 }),
  ]);
  await createEventGatewayClient(settings, { fetch: fn, sleep: noSleep }).publishFriendEvents([event]);
  assert.equal(calls.length, 3);
});

test('hết lượt retry → reject', async () => {
  const { fn, calls } = fakeFetch([
    new Response('', { status: 503 }),
    new Response('', { status: 503 }),
    new Response('', { status: 503 }),
  ]);
  await assert.rejects(
    createEventGatewayClient(settings, { fetch: fn, sleep: noSleep }).publishFriendEvents([event]),
    (err) => err instanceof EventGatewayError && err.status === 503,
  );
  assert.equal(calls.length, 3);
});

test('400 (event sai contract) → không retry', async () => {
  const { fn, calls } = fakeFetch([new Response('{"error":"INVALID_EVENT"}', { status: 400 })]);
  await assert.rejects(
    createEventGatewayClient(settings, { fetch: fn, sleep: noSleep }).publishFriendEvents([event]),
    (err) => err instanceof EventGatewayError && err.status === 400,
  );
  assert.equal(calls.length, 1);
});

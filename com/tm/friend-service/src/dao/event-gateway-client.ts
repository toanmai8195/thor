// Gửi event sang event-gateway (Go) qua HTTP; gateway kiểm tra contract rồi gửi Kafka cho StarRocks.

/** 1 event trên topic friend_events (README 2.3) */
export interface FriendEventPayload {
  user_id: number;
  friend_id: number;
  event_type: string;
  event_time: string;
  event_id: string;
  source: string;
}

export interface EventPublisher {
  /** Resolve khi gateway đã ghi xong lên Kafka; reject khi hết lượt retry hoặc gateway từ chối. */
  publishFriendEvents(events: FriendEventPayload[]): Promise<void>;
}

export interface EventGatewaySettings {
  /** vd http://event-gateway:8080 */
  eventGatewayUrl: string;
  eventGatewayTimeoutMs: number;
  /** Số lần gửi tối đa (kể cả lần đầu) khi lỗi mạng / timeout / 5xx */
  eventGatewayMaxAttempts: number;
}

export class EventGatewayError extends Error {
  constructor(
    message: string,
    /** HTTP status nếu gateway có trả lời */
    readonly status?: number,
  ) {
    super(message);
    this.name = 'EventGatewayError';
  }
}

interface ClientOptions {
  fetch?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
}

const BASE_BACKOFF_MS = 200;

export function createEventGatewayClient(
  { eventGatewayUrl, eventGatewayTimeoutMs, eventGatewayMaxAttempts }: EventGatewaySettings,
  { fetch: doFetch = globalThis.fetch, sleep = (ms) => new Promise((r) => setTimeout(r, ms)) }: ClientOptions = {},
): EventPublisher {
  const url = new URL('/v1/friend-events', eventGatewayUrl).toString();

  async function sendOnce(body: string): Promise<void> {
    let res: Response;
    try {
      res = await doFetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body,
        signal: AbortSignal.timeout(eventGatewayTimeoutMs),
      });
    } catch (err) {
      throw new EventGatewayError(`gọi event-gateway lỗi: ${err instanceof Error ? err.message : String(err)}`);
    }
    if (res.ok) return;
    const text = await res.text().catch(() => '');
    throw new EventGatewayError(`event-gateway trả ${res.status}: ${text}`, res.status);
  }

  return {
    async publishFriendEvents(events) {
      const body = JSON.stringify({ events });
      for (let attempt = 1; ; attempt++) {
        try {
          return await sendOnce(body);
        } catch (err) {
          // 4xx = event sai contract, gửi lại cũng vậy → không retry
          const retryable = !(err instanceof EventGatewayError && err.status !== undefined && err.status < 500);
          if (!retryable || attempt >= eventGatewayMaxAttempts) throw err;
          await sleep(BASE_BACKOFF_MS * 2 ** (attempt - 1));
        }
      }
    },
  };
}

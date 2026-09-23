// Gửi event lên Kafka topic nội bộ (friend_service_events); event-gateway đọc, kiểm tra contract
// rồi chuyển tiếp sang friend_events cho StarRocks.

import { Kafka, Partitioners, logLevel } from 'kafkajs';

/** 1 event đúng contract README 2.3 */
export interface FriendEventPayload {
  user_id: number;
  friend_id: number;
  event_type: string;
  event_time: string;
  event_id: string;
  source: string;
}

export interface EventPublisher {
  /** Resolve khi Kafka đã ghi xong (acks=all); reject khi hết lượt retry của kafkajs. */
  publishFriendEvents(events: FriendEventPayload[]): Promise<void>;
}

export interface KafkaSettings {
  kafkaBrokers: string[];
  kafkaClientId: string;
  /** Topic friend-service ghi vào, event-gateway đọc */
  kafkaTopic: string;
}

/** Phần của kafkajs Producer mà publisher dùng (để test bằng producer giả) */
export interface KafkaSender {
  send(record: { topic: string; acks: number; messages: { key: string; value: string }[] }): Promise<unknown>;
}

/** Map event → message Kafka: key = user_id để event của 1 user vào cùng partition, giữ thứ tự. */
export function createEventPublisher(producer: KafkaSender, topic: string): EventPublisher {
  return {
    async publishFriendEvents(events) {
      await producer.send({
        topic,
        acks: -1,
        messages: events.map((e) => ({ key: String(e.user_id), value: JSON.stringify(e) })),
      });
    },
  };
}

/** Producer idempotent, acks=all. Chưa kết nối: gọi `connect()` (kafkaLifecycle) trước khi gửi. */
export function createKafkaProducer({ kafkaBrokers, kafkaClientId }: KafkaSettings) {
  const kafka = new Kafka({ clientId: kafkaClientId, brokers: kafkaBrokers, logLevel: logLevel.WARN });
  return kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    // murmur2, giống Java client: cùng user_id → cùng partition với mọi producer
    createPartitioner: Partitioners.DefaultPartitioner,
  });
}

/** Thành phần cần khởi động / dừng theo thứ tự: kết nối MongoDB, Kafka, HTTP server... */
export interface Lifecycle {
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
}

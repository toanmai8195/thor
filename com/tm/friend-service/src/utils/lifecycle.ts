/** Thành phần cần khởi động / dừng theo thứ tự: kết nối MongoDB, HTTP server... */
export interface Lifecycle {
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
}

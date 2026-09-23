// Lỗi nghiệp vụ / đầu vào: handler chuyển thành HTTP status + mã lỗi.
export class DomainError extends Error {
  constructor(
    readonly httpStatus: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'DomainError';
  }
}

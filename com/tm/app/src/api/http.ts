// Gọi API qua proxy của Vite (/api/phonebook, /api/friend). Lỗi server {error, message} → ApiError.

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

/** Body lớn hơn ngưỡng này thì nén gzip (Content-Encoding: gzip) */
const GZIP_THRESHOLD = 8 * 1024;

async function gzip(text: string): Promise<Uint8Array> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export interface RequestInfo {
  /** Số byte body thực gửi (sau nén) */
  sentBytes: number;
  gzipped: boolean;
}

export async function request<T>(
  method: string,
  url: string,
  body?: unknown,
  info?: RequestInfo,
): Promise<T> {
  const headers: Record<string, string> = {};
  let payload: BodyInit | undefined;
  if (body !== undefined) {
    const text = JSON.stringify(body);
    headers['Content-Type'] = 'application/json';
    if (text.length > GZIP_THRESHOLD) {
      const z = await gzip(text);
      headers['Content-Encoding'] = 'gzip';
      payload = z as BodyInit;
      if (info) Object.assign(info, { sentBytes: z.length, gzipped: true });
    } else {
      payload = text;
      if (info) Object.assign(info, { sentBytes: new TextEncoder().encode(text).length, gzipped: false });
    }
  }
  let res: Response;
  try {
    res = await fetch(url, { method, headers, body: payload });
  } catch (e) {
    throw new ApiError(0, 'NETWORK', `Không gọi được ${url}: ${(e as Error).message}`);
  }
  const text = await res.text();
  let data: unknown = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    // body không phải JSON (vd proxy trả 502 HTML)
  }
  if (!res.ok) {
    const err = (data ?? {}) as { error?: string; message?: string };
    throw new ApiError(res.status, err.error ?? `HTTP_${res.status}`, err.message ?? (text.slice(0, 200) || res.statusText));
  }
  return data as T;
}

export function errorText(e: unknown): string {
  if (e instanceof ApiError) return e.code === 'NETWORK' ? e.message : `${e.status} ${e.code}: ${e.message}`;
  return String((e as Error)?.message ?? e);
}

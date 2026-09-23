// Log JSON 1 dòng
export type LogFields = Record<string, unknown>;

export interface Logger {
  info(fields: LogFields, msg: string): void;
  error(fields: LogFields & { err?: unknown }, msg: string): void;
}

export const log: Logger = {
  info: (fields, msg) => console.log(JSON.stringify({ level: 'info', msg, ...fields })),
  error: ({ err, ...fields }, msg) =>
    console.error(
      JSON.stringify({ level: 'error', msg, ...fields, err: err instanceof Error ? err.stack : String(err) }),
    ),
};

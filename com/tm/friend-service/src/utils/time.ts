// event_time theo format README 2.3, luôn ở UTC: yyyy-MM-dd HH:mm:ss.SSS
export function formatEventTime(date: Date): string {
  return date.toISOString().replace('T', ' ').slice(0, 23);
}

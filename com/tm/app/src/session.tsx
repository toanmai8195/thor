// User + thiết bị đang giả lập. Cấu hình nhỏ nên lưu localStorage (dữ liệu lớn ở IndexedDB).

import { createContext, useContext, useMemo, useState, type ReactNode } from 'react';
import { deviceKeyOf } from './db/db';

export interface Session {
  userId: number;
  deviceId: string;
  deviceKey: string;
  setSession: (userId: number, deviceId: string) => void;
}

const KEY = 'tm-app.session';
const Ctx = createContext<Session | null>(null);

function load(): { userId: number; deviceId: string } {
  try {
    const v = JSON.parse(localStorage.getItem(KEY) ?? '');
    if (Number.isInteger(v.userId) && typeof v.deviceId === 'string') return v;
  } catch {
    // chưa có / hỏng → mặc định
  }
  return { userId: 1001, deviceId: 'web-1' };
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const [s, setS] = useState(load);
  const value = useMemo<Session>(
    () => ({
      ...s,
      deviceKey: deviceKeyOf(s.userId, s.deviceId),
      setSession: (userId, deviceId) => {
        const next = { userId, deviceId };
        localStorage.setItem(KEY, JSON.stringify(next));
        setS(next);
      },
    }),
    [s],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useSession(): Session {
  const s = useContext(Ctx);
  if (!s) throw new Error('useSession ngoài SessionProvider');
  return s;
}

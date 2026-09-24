import { useLiveQuery } from 'dexie-react-hooks';
import { useEffect, useState } from 'react';
import { db } from '../db/db';
import { useSession } from '../session';

const DEVICE_ID = /^[A-Za-z0-9_-]{1,64}$/;

/** Chọn user + thiết bị đang giả lập; gợi ý các thiết bị đã có danh bạ local. */
export function SessionBar() {
  const { userId, deviceId, setSession } = useSession();
  const [u, setU] = useState(String(userId));
  const [d, setD] = useState(deviceId);
  useEffect(() => {
    setU(String(userId));
    setD(deviceId);
  }, [userId, deviceId]);

  const known = useLiveQuery(async () => (await db.contacts.orderBy('deviceKey').uniqueKeys()) as string[], []);
  const uNum = Number(u);
  const valid = Number.isInteger(uNum) && uNum > 0 && DEVICE_ID.test(d);
  const dirty = u !== String(userId) || d !== deviceId;

  return (
    <div className="topbar">
      <span className="label">User</span>
      <input className={`w-sm${Number.isInteger(uNum) && uNum > 0 ? '' : ' bad'}`} value={u} onChange={(e) => setU(e.target.value)} />
      <span className="label">Thiết bị</span>
      <input className={`w-sm${DEVICE_ID.test(d) ? '' : ' bad'}`} value={d} onChange={(e) => setD(e.target.value)} />
      <button className="primary small" disabled={!valid || !dirty} onClick={() => setSession(uNum, d)}>
        Chuyển
      </button>
      {known && known.length > 0 && (
        <select
          value=""
          onChange={(e) => {
            const [uid, dev] = e.target.value.split('#');
            if (uid) setSession(Number(uid), dev);
          }}
        >
          <option value="">Thiết bị đã có danh bạ…</option>
          {known.map((k) => (
            <option key={k} value={k}>
              user {k.split('#')[0]} · {k.split('#')[1]}
            </option>
          ))}
        </select>
      )}
      <span className="spacer" />
      <span className="muted">
        Đang dùng: <b>user {userId}</b> · <b>{deviceId}</b>
      </span>
    </div>
  );
}

import { useCallback, useEffect, useMemo, useState } from 'react';
import { errorText } from '../api/http';
import { phonebookApi, type ListResponse, type ServerContact, type SummaryResponse } from '../api/phonebook';
import { normalizePhone } from '../lib/phone';
import { useSession } from '../session';
import { useLocalDigest } from '../sync/localDigest';

/**
 * Danh bạ user trên server (hợp mọi thiết bị hoặc 1 thiết bị).
 * Cột "Profile / Friend" để dành cho bước enrich sau (map số → user, trạng thái bạn bè).
 */
export function ServerContactsPage() {
  const session = useSession();
  const [userInput, setUserInput] = useState(String(session.userId));
  const [userId, setUserId] = useState(session.userId);
  useEffect(() => {
    setUserId(session.userId);
    setUserInput(String(session.userId));
  }, [session.userId]);

  const [summary, setSummary] = useState<SummaryResponse>();
  const [deviceId, setDeviceId] = useState('');
  const [limit, setLimit] = useState(100);
  const [cursors, setCursors] = useState<(string | null)[]>([null]); // cursor của từng trang đã xem
  const [page, setPage] = useState<ListResponse>();
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [lookup, setLookup] = useState('');
  const [lookupResult, setLookupResult] = useState('');

  // Số có trong danh bạ local của thiết bị đang giả lập (để so)
  const local = useLocalDigest(session.deviceKey);
  const localPhones = useMemo(() => new Set(local?.canon.map((e) => e.p)), [local]);

  const load = useCallback(
    async (cursor: string | null) => {
      setLoading(true);
      setError('');
      try {
        const [s, p] = await Promise.all([
          phonebookApi.summary(userId),
          phonebookApi.list(userId, { limit, cursor, deviceId: deviceId || undefined }),
        ]);
        setSummary(s);
        setPage(p);
      } catch (e) {
        setError(errorText(e));
        setPage(undefined);
      } finally {
        setLoading(false);
      }
    },
    [userId, limit, deviceId],
  );

  useEffect(() => {
    setCursors([null]);
    load(null);
  }, [load]);

  const pageNo = cursors.length;
  const next = () => {
    if (!page?.next_cursor) return;
    setCursors([...cursors, page.next_cursor]);
    load(page.next_cursor);
  };
  const prev = () => {
    if (cursors.length <= 1) return;
    const c = cursors.slice(0, -1);
    setCursors(c);
    load(c[c.length - 1]);
  };

  async function doLookup() {
    setLookupResult('');
    try {
      const c: ServerContact = await phonebookApi.lookup(userId, normalizePhone(lookup));
      setLookupResult(`Có: ${c.phone} — ${c.name || '(không tên)'}`);
    } catch (e) {
      setLookupResult(errorText(e));
    }
  }

  return (
    <>
      <h1>Danh bạ server</h1>
      <p className="sub">
        Đọc từ phonebook-service (HBase) — hợp danh bạ các thiết bị của user; cùng số khác tên thì lấy tên ở thiết bị sync gần nhất.
      </p>

      <div className="card">
        <div className="row">
          <span className="muted">User</span>
          <input className="w-sm" value={userInput} onChange={(e) => setUserInput(e.target.value)} />
          <button className="small" onClick={() => Number(userInput) > 0 && setUserId(Number(userInput))}>
            Xem
          </button>
          <span className="muted">Thiết bị</span>
          <select value={deviceId} onChange={(e) => setDeviceId(e.target.value)}>
            <option value="">Tất cả thiết bị</option>
            {summary?.devices.map((d) => (
              <option key={d.device_id} value={d.device_id}>
                {d.device_id} ({d.contact_cnt})
              </option>
            ))}
          </select>
          <span className="muted">Mỗi trang</span>
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            {[50, 100, 500, 2000].map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
          <button className="small" onClick={() => load(cursors[cursors.length - 1])} disabled={loading}>
            Tải lại
          </button>
        </div>
        {error && <div className="error">{error}</div>}
      </div>

      {summary && (
        <div className="grid cols-4">
          <div className="card stat">
            <div className="k">Contact đã tới DW (user {summary.user_id})</div>
            <div className="v">{summary.contact_cnt}</div>
          </div>
          <div className="card stat">
            <div className="k">Đang chờ gửi bù Kafka</div>
            <div className="v small">{summary.pending ? <span className="chip warn">Có pending</span> : <span className="chip ok">Không</span>}</div>
          </div>
          <div className="card stat" style={{ gridColumn: 'span 2' }}>
            <div className="k">Thiết bị</div>
            <div className="v small">
              {summary.devices.length === 0 && <span className="muted">Chưa có thiết bị nào đồng bộ</span>}
              {summary.devices.map((d) => (
                <div key={d.device_id}>
                  <b>{d.device_id}</b> · {d.contact_cnt} contact · <span className="muted">{new Date(d.synced_at).toLocaleString('vi-VN')}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      <div className="card">
        <div className="row" style={{ marginBottom: 10 }}>
          <input className="w-md mono" placeholder="Tra số: 0366621555" value={lookup} onChange={(e) => setLookup(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && doLookup()} />
          <button className="small" onClick={doLookup} disabled={!lookup.trim()}>
            User có lưu số này?
          </button>
          {lookupResult && <span className="muted">{lookupResult}</span>}
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Số</th>
                <th>Tên</th>
                <th>Ở local (thiết bị đang dùng)</th>
                <th>Profile / Friend</th>
              </tr>
            </thead>
            <tbody>
              {page?.contacts.map((c) => (
                <tr key={c.phone}>
                  <td className="mono">{c.phone}</td>
                  <td>{c.name || <span className="muted">(không tên)</span>}</td>
                  <td>{localPhones.has(c.phone) ? <span className="chip ok">có</span> : <span className="chip">không</span>}</td>
                  <td className="muted">— sắp có</td>
                </tr>
              ))}
              {page && page.contacts.length === 0 && (
                <tr>
                  <td colSpan={4} className="muted">
                    Server chưa có danh bạ của user này — đồng bộ ở màn "Đồng bộ".
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="pager">
          <button className="small" onClick={prev} disabled={loading || pageNo <= 1}>
            ‹ Trước
          </button>
          <span className="muted">
            Trang {pageNo} · tổng {page?.total ?? '…'} contact · thứ tự theo (bucket, phone_enc) như server trả
          </span>
          <button className="small" onClick={next} disabled={loading || !page?.next_cursor}>
            Sau ›
          </button>
        </div>
      </div>
    </>
  );
}

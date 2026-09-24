import { useLiveQuery } from 'dexie-react-hooks';
import { useMemo, useState } from 'react';
import { db, type LocalContact } from '../db/db';
import { fakeContact } from '../lib/fake';
import { isValidMobile, normalizePhone } from '../lib/phone';
import { useSession } from '../session';
import { useLocalDigest } from '../sync/localDigest';

const PAGE = 100;

export function LocalContactsPage() {
  const { deviceKey, userId, deviceId } = useSession();
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(0);
  const [editing, setEditing] = useState<LocalContact | 'new' | null>(null);
  const [genN, setGenN] = useState(100);
  const [confirmClear, setConfirmClear] = useState(false);
  const [busy, setBusy] = useState('');

  const all = useLiveQuery(() => db.contacts.where('[deviceKey+name]').between([deviceKey, ''], [deviceKey, '￿']).toArray(), [deviceKey]);
  const syncState = useLiveQuery(() => db.syncStates.get(deviceKey), [deviceKey]);
  const digest = useLocalDigest(deviceKey);

  const filtered = useMemo(() => {
    if (!all) return [];
    const q = search.trim().toLowerCase();
    if (!q) return all;
    const qDigits = q.replace(/\D/g, '');
    return all.filter(
      (c) => c.name.toLowerCase().includes(q) || (qDigits !== '' && c.phones.some((p) => normalizePhone(p).includes(qDigits))),
    );
  }, [all, search]);
  const pages = Math.max(1, Math.ceil(filtered.length / PAGE));
  const shown = filtered.slice(page * PAGE, (page + 1) * PAGE);

  const synced = digest && syncState?.lastRoot === digest.rootB64;

  async function generate() {
    setBusy(`Đang sinh ${genN} contact…`);
    const now = Date.now();
    await db.contacts.bulkAdd(Array.from({ length: genN }, () => ({ ...fakeContact(), deviceKey, createdAt: now, updatedAt: now })));
    setBusy('');
  }

  async function clearAll() {
    await db.contacts.where('deviceKey').equals(deviceKey).delete();
    setConfirmClear(false);
    setPage(0);
  }

  return (
    <>
      <h1>Danh bạ local</h1>
      <p className="sub">
        Danh bạ trên máy của user {userId}, thiết bị {deviceId} — lưu trong IndexedDB, chưa gửi đi đâu cho tới khi đồng bộ.
      </p>

      <div className="grid cols-4">
        <div className="card stat">
          <div className="k">Contact</div>
          <div className="v">{all?.length ?? '…'}</div>
        </div>
        <div className="card stat">
          <div className="k">Số hợp lệ (sẽ đồng bộ)</div>
          <div className="v">{digest?.canon.length ?? '…'}</div>
        </div>
        <div className="card stat">
          <div className="k">Số không hợp lệ</div>
          <div className="v">{digest?.invalid ?? '…'}</div>
        </div>
        <div className="card stat">
          <div className="k">Trạng thái</div>
          <div className="v small">
            {!digest ? '…' : synced ? <span className="chip ok">Đã đồng bộ</span> : <span className="chip warn">Có thay đổi chưa đồng bộ</span>}
            <div className="muted mono" style={{ marginTop: 6 }}>
              root {digest?.rootB64.slice(0, 16)}…
            </div>
          </div>
        </div>
      </div>

      {editing && <ContactForm contact={editing === 'new' ? undefined : editing} onClose={() => setEditing(null)} />}

      <div className="card">
        <div className="row" style={{ marginBottom: 12 }}>
          <input
            className="w-md"
            placeholder="Tìm theo tên hoặc số…"
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(0);
            }}
          />
          <button className="primary" onClick={() => setEditing('new')}>
            Thêm contact
          </button>
          <span className="spacer" />
          <input className="w-num" type="number" min={1} max={20000} value={genN} onChange={(e) => setGenN(Math.max(1, Math.min(20000, Number(e.target.value) || 1)))} />
          <button onClick={generate} disabled={!!busy}>
            Sinh ngẫu nhiên
          </button>
          {confirmClear ? (
            <>
              <span className="muted">Xoá {all?.length} contact của thiết bị này?</span>
              <button className="danger solid" onClick={clearAll}>
                Xoá
              </button>
              <button onClick={() => setConfirmClear(false)}>Huỷ</button>
            </>
          ) : (
            <button className="danger" onClick={() => setConfirmClear(true)} disabled={!all?.length}>
              Xoá tất cả
            </button>
          )}
        </div>
        {busy && <div className="note">{busy}</div>}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Tên</th>
                <th>Số điện thoại</th>
                <th>Sửa lúc</th>
                <th style={{ width: 120 }} />
              </tr>
            </thead>
            <tbody>
              {shown.map((c) => (
                <tr key={c.id}>
                  <td>{c.name || <span className="muted">(không tên)</span>}</td>
                  <td>
                    {c.phones.map((p, i) => {
                      const n = normalizePhone(p);
                      return (
                        <span key={i} className={`chip mono ${isValidMobile(n) ? '' : 'bad'}`} title={isValidMobile(n) ? n : 'Không phải số di động VN 10 số — không đồng bộ'}>
                          {p}
                        </span>
                      );
                    })}
                  </td>
                  <td className="muted">{new Date(c.updatedAt).toLocaleString('vi-VN')}</td>
                  <td>
                    <button className="link" onClick={() => setEditing(c)}>
                      Sửa
                    </button>
                    <button className="link danger" onClick={() => db.contacts.delete(c.id!)}>
                      Xoá
                    </button>
                  </td>
                </tr>
              ))}
              {all && shown.length === 0 && (
                <tr>
                  <td colSpan={4} className="muted">
                    {all.length === 0 ? 'Chưa có contact. Thêm tay hoặc bấm "Sinh ngẫu nhiên".' : 'Không có contact khớp.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {pages > 1 && (
          <div className="pager">
            <button className="small" disabled={page === 0} onClick={() => setPage(page - 1)}>
              ‹ Trước
            </button>
            <span className="muted">
              Trang {page + 1}/{pages} · {filtered.length} contact
            </span>
            <button className="small" disabled={page >= pages - 1} onClick={() => setPage(page + 1)}>
              Sau ›
            </button>
          </div>
        )}
      </div>
    </>
  );
}

function ContactForm({ contact, onClose }: { contact?: LocalContact; onClose: () => void }) {
  const { deviceKey } = useSession();
  const [name, setName] = useState(contact?.name ?? '');
  const [phones, setPhones] = useState<string[]>(contact?.phones.length ? contact.phones : ['']);

  const cleaned = phones.map((p) => p.trim()).filter(Boolean);
  const canSave = cleaned.length > 0;

  async function save() {
    const now = Date.now();
    if (contact?.id) await db.contacts.update(contact.id, { name: name.trim(), phones: cleaned, updatedAt: now });
    else await db.contacts.add({ deviceKey, name: name.trim(), phones: cleaned, createdAt: now, updatedAt: now });
    onClose();
  }

  return (
    <div className="card">
      <h2>{contact ? 'Sửa contact' : 'Thêm contact'}</h2>
      <div className="form">
        <label>
          Tên
          <input value={name} onChange={(e) => setName(e.target.value)} autoFocus placeholder="vd Mẹ, Anh Tuấn Grab" />
        </label>
        {phones.map((p, i) => {
          const n = normalizePhone(p);
          const ok = p.trim() === '' || isValidMobile(n);
          return (
            <label key={i}>
              Số {phones.length > 1 ? i + 1 : ''}
              <div className="row">
                <input
                  className={`w-md mono${ok ? '' : ' bad'}`}
                  value={p}
                  placeholder="0366 621 555 hoặc +84…"
                  onChange={(e) => setPhones(phones.map((x, j) => (j === i ? e.target.value : x)))}
                />
                {p.trim() !== '' && (
                  <span className={ok ? 'muted mono' : 'chip bad'}>{ok ? `→ ${n}` : 'không phải số di động VN, sẽ không đồng bộ'}</span>
                )}
                {phones.length > 1 && (
                  <button className="link danger" onClick={() => setPhones(phones.filter((_, j) => j !== i))}>
                    Bỏ
                  </button>
                )}
              </div>
            </label>
          );
        })}
        <div className="row">
          <button className="link" onClick={() => setPhones([...phones, ''])}>
            + Thêm số
          </button>
        </div>
        <div className="row">
          <button className="primary" disabled={!canSave} onClick={save}>
            Lưu
          </button>
          <button onClick={onClose}>Huỷ</button>
        </div>
      </div>
    </div>
  );
}

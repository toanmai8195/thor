import { useLiveQuery } from 'dexie-react-hooks';
import { Fragment, useState } from 'react';
import { errorText } from '../api/http';
import { db, type SyncLog, type SyncStep } from '../db/db';
import { BUCKETS } from '../lib/digest';
import { useSession } from '../session';
import { useLocalDigest } from '../sync/localDigest';
import { deleteServerDevice, runSync, type SyncOutcome } from '../sync/syncEngine';

export function SyncPage() {
  const { userId, deviceId, deviceKey } = useSession();
  const digest = useLocalDigest(deviceKey);
  const state = useLiveQuery(() => db.syncStates.get(deviceKey), [deviceKey]);
  const logs = useLiveQuery(() => db.syncLogs.where('deviceKey').equals(deviceKey).reverse().limit(20).toArray(), [deviceKey]);

  const [running, setRunning] = useState(false);
  const [force, setForce] = useState(false);
  const [steps, setSteps] = useState<SyncStep[]>([]);
  const [outcome, setOutcome] = useState<SyncOutcome>();
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [msg, setMsg] = useState('');
  const [openLog, setOpenLog] = useState<number>();

  const synced = digest && state?.lastRoot === digest.rootB64;
  const perBucket = new Array<number>(BUCKETS).fill(0);
  digest?.canon.forEach((e) => perBucket[Number(e.p.slice(-2))]++);
  const changed = new Set(outcome?.changed ?? []);

  async function sync() {
    setRunning(true);
    setSteps([]);
    setOutcome(undefined);
    setMsg('');
    const out = await runSync({ userId, deviceId, forceBuckets: force, onStep: (s) => setSteps((prev) => [...prev, s]) });
    setOutcome(out);
    setRunning(false);
  }

  async function removeDevice() {
    setConfirmDelete(false);
    try {
      const r = await deleteServerDevice(userId, deviceId);
      setMsg(`Đã xoá danh bạ thiết bị ${deviceId} trên server: −${r.deleted} số, user còn ${r.contact_cnt} contact.`);
    } catch (e) {
      setMsg(errorText(e));
    }
  }

  return (
    <>
      <h1>Đồng bộ</h1>
      <p className="sub">
        Tính digest local → <code>check</code> → chỉ upload bucket đổi. Lần login không đổi danh bạ chỉ gửi 1 request (root 32 byte).
      </p>

      <div className="grid cols-4">
        <div className="card stat">
          <div className="k">Số hợp lệ ở local</div>
          <div className="v">{digest?.canon.length ?? '…'}</div>
        </div>
        <div className="card stat">
          <div className="k">Root local</div>
          <div className="v small mono">{digest ? digest.rootB64.slice(0, 22) + '…' : '…'}</div>
          <div className="muted">tính trong {digest ? Math.round(digest.ms) : '…'} ms</div>
        </div>
        <div className="card stat">
          <div className="k">last_root (lần đồng bộ trước)</div>
          <div className="v small mono">{state?.lastRoot ? state.lastRoot.slice(0, 22) + '…' : '—'}</div>
          <div className="muted">{state?.lastSyncAt ? new Date(state.lastSyncAt).toLocaleString('vi-VN') : 'chưa đồng bộ'}</div>
        </div>
        <div className="card stat">
          <div className="k">Trạng thái</div>
          <div className="v small">{!digest ? '…' : synced ? <span className="chip ok">Khớp last_root</span> : <span className="chip warn">Local đã đổi</span>}</div>
        </div>
      </div>

      <div className="card">
        <div className="row">
          <button className="primary" onClick={sync} disabled={running || !digest}>
            {running ? 'Đang đồng bộ…' : 'Đồng bộ'}
          </button>
          <label className="row muted">
            <input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} /> Luôn gửi kèm digest bucket (bỏ qua last_root)
          </label>
          <span className="spacer" />
          {confirmDelete ? (
            <>
              <span className="muted">Xoá danh bạ thiết bị {deviceId} trên server (như logout)?</span>
              <button className="danger solid" onClick={removeDevice}>
                Xoá
              </button>
              <button onClick={() => setConfirmDelete(false)}>Huỷ</button>
            </>
          ) : (
            <button className="danger" onClick={() => setConfirmDelete(true)}>
              Xoá thiết bị trên server
            </button>
          )}
        </div>
        {msg && <div className="note">{msg}</div>}

        {(steps.length > 0 || outcome) && (
          <div style={{ marginTop: 14 }}>
            <StepList steps={steps} />
            {outcome && (
              <div className={outcome.ok ? 'note' : 'error'} style={{ marginTop: 10 }}>
                {outcome.status === 'UNCHANGED' && 'Không đổi — server đã có đúng danh bạ này.'}
                {outcome.status === 'UPLOADED' &&
                  `Đã gửi ${outcome.changed.length} bucket trong ${outcome.requests} request: +${outcome.added} số, −${outcome.deleted} số (đổi tên không tính).`}
                {outcome.status === 'ERROR' && 'Đồng bộ lỗi — xem bước cuối.'}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="grid cols-2">
        <div className="card">
          <h2>100 bucket (2 số cuối)</h2>
          <p className="muted" style={{ marginTop: 0 }}>
            Số trong ô = số contact của bucket. Ô vàng = bucket vừa upload ở lần đồng bộ này.
          </p>
          <div className="buckets">
            {perBucket.map((n, k) => (
              <div key={k} className={`${n ? 'has' : ''} ${changed.has(k) ? 'changed' : ''}`} title={`bucket ${String(k).padStart(2, '0')}: ${n} contact`}>
                {String(k).padStart(2, '0')}
                <br />
                {n}
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <h2>Lịch sử đồng bộ</h2>
          <table>
            <thead>
              <tr>
                <th>Lúc</th>
                <th>Kết quả</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {logs?.map((l: SyncLog) => (
                <Fragment key={l.id}>
                  <tr>
                    <td className="muted">{new Date(l.startedAt).toLocaleString('vi-VN')}</td>
                    <td>
                      <span className={`chip ${l.ok ? 'ok' : 'bad'}`}>{l.ok ? 'OK' : 'Lỗi'}</span> {l.summary}
                    </td>
                    <td>
                      <button className="link" onClick={() => setOpenLog(openLog === l.id ? undefined : l.id)}>
                        {openLog === l.id ? 'Ẩn' : 'Chi tiết'}
                      </button>
                    </td>
                  </tr>
                  {openLog === l.id && (
                    <tr>
                      <td colSpan={3}>
                        <StepList steps={l.steps} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
              {logs?.length === 0 && (
                <tr>
                  <td colSpan={3} className="muted">
                    Chưa đồng bộ lần nào.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function StepList({ steps }: { steps: SyncStep[] }) {
  return (
    <ul className="steps">
      {steps.map((s, i) => (
        <li key={i} className={s.ok ? '' : 'fail'}>
          <span className="dot" />
          <b>{s.title}</b>
          <span>{s.detail}</span>
          <span className="ms">{s.ms} ms</span>
        </li>
      ))}
    </ul>
  );
}

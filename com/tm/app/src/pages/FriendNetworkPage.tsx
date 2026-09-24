import { useLiveQuery } from 'dexie-react-hooks';
import { useCallback, useEffect, useState } from 'react';
import { ACTION_LABEL, allowedActions, friendApi, type Action, type FriendSummary, type ListResult, type Status } from '../api/friend';
import { errorText } from '../api/http';
import { db } from '../db/db';

const STATUS_LABEL: Record<Status, string> = {
  REQUESTED: 'Đã gửi lời mời',
  REVIEWED: 'Chờ duyệt',
  FRIEND: 'Bạn bè',
  CANCEL: 'Đã huỷ lời mời',
  UNFRIEND: 'Đã huỷ kết bạn',
  BLOCKING: 'Đang block',
  BLOCKED: 'Bị block',
};
const STATUS_CHIP: Record<Status, string> = {
  REQUESTED: 'info',
  REVIEWED: 'warn',
  FRIEND: 'ok',
  CANCEL: '',
  UNFRIEND: '',
  BLOCKING: 'bad',
  BLOCKED: 'bad',
};
const DEFAULT_USERS = [1001, 1002, 1003];

type Rel = Record<string, Status | undefined>; // "a>b" → trạng thái a → b
const key = (a: number, b: number) => `${a}>${b}`;

function StatusChip({ s }: { s?: Status }) {
  if (!s) return <span className="muted">—</span>;
  return <span className={`chip ${STATUS_CHIP[s]}`}>{STATUS_LABEL[s]}</span>;
}

/** Quản lý quan hệ bạn bè giữa nhiều user qua friend-service. */
export function FriendNetworkPage() {
  const users = useLiveQuery(() => db.friendUsers.orderBy('userId').toArray(), []);
  const ids = users?.map((u) => u.userId) ?? [];
  const idsKey = ids.join(',');

  const [rel, setRel] = useState<Rel>({});
  const [summaries, setSummaries] = useState<Record<number, FriendSummary>>({});
  const [selected, setSelected] = useState<[number, number]>();
  const [mutual, setMutual] = useState<number[]>();
  const [detailUser, setDetailUser] = useState<number>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState('');
  const [addInput, setAddInput] = useState('');

  // Lần đầu: 3 user mẫu
  useEffect(() => {
    if (users && users.length === 0) db.friendUsers.bulkPut(DEFAULT_USERS.map((userId) => ({ userId, label: '' })));
  }, [users]);

  const refresh = useCallback(async () => {
    const list = idsKey ? idsKey.split(',').map(Number) : [];
    if (list.length === 0) return;
    setError('');
    try {
      const pairs: [number, number][] = [];
      for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) pairs.push([list[i], list[j]]);
      const [rels, sums] = await Promise.all([
        Promise.all(pairs.map(([a, b]) => friendApi.relationship(a, b))),
        Promise.all(list.map((u) => friendApi.summary(u))),
      ]);
      const r: Rel = {};
      rels.forEach((x) => {
        r[key(x.user_id, x.friend_id)] = x.outgoing?.status;
        r[key(x.friend_id, x.user_id)] = x.incoming?.status;
      });
      setRel(r);
      setSummaries(Object.fromEntries(sums.map((s) => [s.user_id, s])));
    } catch (e) {
      setError(errorText(e));
    }
  }, [idsKey]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    setMutual(undefined);
    if (!selected) return;
    friendApi
      .mutualFriends(selected[0], selected[1])
      .then((r) => setMutual(r.items))
      .catch(() => setMutual(undefined));
  }, [selected, rel]);

  async function act(action: Action, actor: number, target: number) {
    setError('');
    try {
      await friendApi.act(action, actor, target);
      await refresh();
    } catch (e) {
      setError(errorText(e));
    }
  }

  /** Mọi cặp chưa là bạn (và không block) → A gửi lời mời, B chấp nhận. */
  async function befriendAll() {
    setError('');
    const list = ids;
    let done = 0;
    try {
      for (let i = 0; i < list.length; i++) {
        for (let j = i + 1; j < list.length; j++) {
          const [a, b] = [list[i], list[j]];
          const ab = rel[key(a, b)];
          setBusy(`Kết bạn ${a} ↔ ${b}…`);
          if (ab === 'FRIEND' || ab === 'BLOCKING' || ab === 'BLOCKED') continue;
          if (ab === 'REVIEWED') await friendApi.act('accept', a, b); // b đã mời a
          else {
            if (ab !== 'REQUESTED') await friendApi.act('request', a, b);
            await friendApi.act('accept', b, a);
          }
          done++;
        }
      }
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(done ? `Đã kết bạn ${done} cặp` : '');
      await refresh();
    }
  }

  async function addUsers() {
    const nums = addInput
      .split(/[\s,]+/)
      .map(Number)
      .filter((n) => Number.isInteger(n) && n > 0);
    if (nums.length) await db.friendUsers.bulkPut(nums.map((userId) => ({ userId, label: '' })));
    setAddInput('');
  }

  const [sa, sb] = selected ?? [];
  const selStatus = selected ? rel[key(sa!, sb!)] : undefined;

  return (
    <>
      <h1>Friend network</h1>
      <p className="sub">Quan hệ giữa các user qua friend-service. Ô hàng A, cột B = trạng thái A → B; bấm ô để thao tác thay A.</p>

      <div className="card">
        <div className="row">
          <span className="muted">User:</span>
          {users?.map((u) => (
            <span key={u.userId} className="chip">
              {u.userId}{' '}
              <button className="link small danger" title="Bỏ khỏi bảng" onClick={() => db.friendUsers.delete(u.userId)}>
                ×
              </button>
            </span>
          ))}
          <input className="w-sm" placeholder="thêm: 1004, 1005" value={addInput} onChange={(e) => setAddInput(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && addUsers()} />
          <button className="small" onClick={addUsers} disabled={!addInput.trim()}>
            Thêm
          </button>
          <span className="spacer" />
          <button className="small" onClick={refresh}>
            Tải lại
          </button>
          <button className="primary small" onClick={befriendAll} disabled={ids.length < 2 || !!busy.endsWith('…')}>
            Kết bạn tất cả
          </button>
        </div>
        {busy && <div className="note">{busy}</div>}
        {error && <div className="error">{error}</div>}
      </div>

      <div className="card">
        <h2>Ma trận quan hệ</h2>
        <div className="table-wrap">
          <table className="matrix">
            <thead>
              <tr>
                <th>A \ B</th>
                {ids.map((b) => (
                  <th key={b}>{b}</th>
                ))}
                <th>Bạn / chờ / block</th>
              </tr>
            </thead>
            <tbody>
              {ids.map((a) => (
                <tr key={a}>
                  <th>
                    <button className="link" onClick={() => setDetailUser(detailUser === a ? undefined : a)}>
                      {a}
                    </button>
                  </th>
                  {ids.map((b) =>
                    a === b ? (
                      <td key={b} className="self" />
                    ) : (
                      <td key={b} className={`cell ${sa === a && sb === b ? 'selected' : ''}`} onClick={() => setSelected([a, b])}>
                        <StatusChip s={rel[key(a, b)]} />
                      </td>
                    ),
                  )}
                  <td className="muted">
                    {summaries[a]
                      ? `${summaries[a].friend_cnt} / ${summaries[a].pending_sent_cnt}↑ ${summaries[a].pending_received_cnt}↓ / ${summaries[a].blocking_cnt}`
                      : '…'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid cols-2">
        <div className="card">
          <h2>Cặp đang chọn</h2>
          {!selected && <p className="muted">Bấm 1 ô trong ma trận.</p>}
          {selected && (
            <>
              <p>
                <b>{sa}</b> → <b>{sb}</b>: <StatusChip s={selStatus} />
                <br />
                <b>{sb}</b> → <b>{sa}</b>: <StatusChip s={rel[key(sb!, sa!)]} />
              </p>
              <div className="row">
                <span className="muted">{sa} có thể:</span>
                {allowedActions(selStatus).map((a) => (
                  <button key={a} className={a === 'block' || a === 'unfriend' ? 'small danger' : 'small primary'} onClick={() => act(a, sa!, sb!)}>
                    {ACTION_LABEL[a]}
                  </button>
                ))}
                {allowedActions(selStatus).length === 0 && <span className="muted">không có hành động (đang block, chưa hỗ trợ unblock)</span>}
                <button className="small" onClick={() => setSelected([sb!, sa!])}>
                  Đổi chiều
                </button>
              </div>
              <p className="muted" style={{ marginBottom: 0 }}>
                Bạn chung: {mutual === undefined ? '…' : mutual.length ? mutual.join(', ') : 'không có'}
              </p>
            </>
          )}
        </div>
        <UserDetail userId={detailUser} version={rel} />
      </div>
    </>
  );
}

/** Danh sách bạn / lời mời / block của 1 user (bấm vào tên hàng trong ma trận). */
function UserDetail({ userId, version }: { userId?: number; version: unknown }) {
  const [data, setData] = useState<{ friends: ListResult; requests: ListResult; blocks: ListResult }>();
  const [error, setError] = useState('');
  useEffect(() => {
    setData(undefined);
    if (!userId) return;
    Promise.all([friendApi.friends(userId), friendApi.requests(userId), friendApi.blocks(userId)])
      .then(([friends, requests, blocks]) => setData({ friends, requests, blocks }))
      .catch((e) => setError(errorText(e)));
  }, [userId, version]);

  if (!userId)
    return (
      <div className="card">
        <h2>Chi tiết user</h2>
        <p className="muted">Bấm vào số user ở cột đầu ma trận — xem cả quan hệ với user ngoài bảng.</p>
      </div>
    );
  const section = (title: string, r?: ListResult) => (
    <div style={{ marginBottom: 10 }}>
      <b>{title}</b> <span className="muted">({r?.items.length ?? '…'})</span>
      <div>
        {r?.items.map((i) => (
          <span key={i.friend_id} className={`chip ${STATUS_CHIP[i.status]}`} title={`${STATUS_LABEL[i.status]} từ ${i.since}`}>
            {i.friend_id}
          </span>
        ))}
      </div>
    </div>
  );
  return (
    <div className="card">
      <h2>Chi tiết user {userId}</h2>
      {error && <div className="error">{error}</div>}
      {section('Bạn bè', data?.friends)}
      {section('Lời mời (gửi / nhận)', data?.requests)}
      {section('Block (đang block / bị block)', data?.blocks)}
    </div>
  );
}

// Nghiệp vụ Friend Network: kiểm tra luật, ghi 2 chiều trong 1 transaction, commit xong gửi 2 event lên Kafka (event-gateway đọc); và các truy vấn.
// Không biết gì về HTTP: nhận id đã parse, trả object thuần.

import type { FriendshipDao, FriendshipWrite } from '../dao/friendship-dao.js';
import type { DbSession, TransactionRunner } from '../dao/mongo.js';
import type { EventPublisher, FriendEventPayload } from '../dao/event-publisher.js';
import { DomainError } from '../utils/errors.js';
import type { IdGenerator } from '../utils/ids.js';
import type { Logger } from '../utils/logger.js';
import { formatEventTime } from '../utils/time.js';
import { type Action, Status, plan } from './friendship-rules.js';

const MAX_DUP_KEY_RETRIES = 3;

const REQUEST_TYPES = {
  sent: [Status.REQUESTED],
  received: [Status.REVIEWED],
  all: [Status.REQUESTED, Status.REVIEWED],
} as const satisfies Record<string, readonly Status[]>;

const BLOCK_TYPES = {
  blocking: [Status.BLOCKING],
  blocked: [Status.BLOCKED],
  all: [Status.BLOCKING, Status.BLOCKED],
} as const satisfies Record<string, readonly Status[]>;

// ---------------------------------------------------------------------------
// Kiểu dữ liệu trả ra ngoài
// ---------------------------------------------------------------------------

/** Event gửi lên Kafka friend_service_events → event-gateway → friend_events (README 2.3) */
export interface FriendEvent extends FriendEventPayload {
  event_type: Status;
}

export interface ActionResult {
  user_id: number;
  friend_id: number;
  /** Trạng thái mới của user_id → friend_id */
  status: Status;
  event_time: string;
}

export interface Paging {
  limit: number;
  after?: number;
}

export interface ListResult {
  items: { friend_id: number; status: Status; since: string }[];
  /** friend_id cuối trang, truyền vào `after` để lấy trang sau; null = hết */
  next_after: number | null;
}

export interface DirectionView {
  status: Status;
  since: string;
  event_id: string;
}

export interface Relationship {
  user_id: number;
  friend_id: number;
  outgoing: DirectionView | null;
  incoming: DirectionView | null;
}

export interface Summary {
  user_id: number;
  friend_cnt: number;
  pending_sent_cnt: number;
  pending_received_cnt: number;
  blocking_cnt: number;
  blocked_cnt: number;
  last_activity: string | null;
}

/** Kết quả của 1 transaction đã commit */
interface Committed {
  result: ActionResult;
  events: FriendEvent[];
}

// ---------------------------------------------------------------------------

export interface FriendControllerDeps {
  runInTransaction: TransactionRunner;
  friendshipDao: Pick<FriendshipDao, 'findPair' | 'upsertMany' | 'listByStatus' | 'countByStatus' | 'commonFriendIds'>;
  eventPublisher: EventPublisher;
  /** Sinh event_id */
  nextId: IdGenerator;
  /** Field `source` của event */
  source: string;
  log: Logger;
  now?: () => number;
}

function statusesOf<T extends Record<string, readonly Status[]>>(types: T, type: string, name: string): readonly Status[] {
  if (!Object.hasOwn(types, type)) {
    throw new DomainError(400, 'INVALID_QUERY', `${name} phải là một trong: ${Object.keys(types).join(', ')}`);
  }
  return types[type as keyof T] as readonly Status[];
}

function assertDifferent(userId: number, otherId: number): void {
  if (userId === otherId) {
    throw new DomainError(400, 'SAME_USER', 'user_id và friend_id phải khác nhau');
  }
}

// Dao lưu status dạng string; chỉ Controller ghi vào nên luôn là Status hợp lệ
const asStatus = (s: string) => s as Status;

function isDuplicateKey(err: unknown): boolean {
  return typeof err === 'object' && err !== null && 'code' in err && err.code === 11000;
}

export class FriendController {
  private readonly runInTransaction: TransactionRunner;
  private readonly friendshipDao: FriendControllerDeps['friendshipDao'];
  private readonly eventPublisher: EventPublisher;
  private readonly log: Logger;
  private readonly nextId: IdGenerator;
  private readonly source: string;
  private readonly now: () => number;

  constructor({
    runInTransaction,
    friendshipDao,
    eventPublisher,
    nextId,
    source,
    log,
    now = () => Date.now(),
  }: FriendControllerDeps) {
    this.runInTransaction = runInTransaction;
    this.friendshipDao = friendshipDao;
    this.eventPublisher = eventPublisher;
    this.log = log;
    this.nextId = nextId;
    this.source = source;
    this.now = now;
  }

  // ---------------------------------------------------------------------------
  // Hành động
  // ---------------------------------------------------------------------------

  /**
   * Thực hiện 1 hành động của actor với target: ghi 2 chiều trong 1 transaction,
   * commit thành công thì gửi 2 event lên Kafka.
   */
  async apply(action: Action, actorId: number, targetId: number): Promise<ActionResult> {
    assertDifferent(actorId, targetId);
    const { result, events } = await this.commit(action, actorId, targetId);
    await this.publish(events);
    return result;
  }

  private async commit(action: Action, actorId: number, targetId: number): Promise<Committed> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.runInTransaction((session) => this.applyInTx(session, action, actorId, targetId));
      } catch (err) {
        // 2 request đồng thời cùng upsert 1 cặp mới → 1 bên dính duplicate key, thử lại là thấy trạng thái mới
        if (isDuplicateKey(err) && attempt < MAX_DUP_KEY_RETRIES) continue;
        throw err;
      }
    }
  }

  /**
   * Gửi event sau khi DB đã commit. Gửi lỗi (sau khi kafkajs đã retry) thì chỉ log kèm event,
   * không báo lỗi cho client vì hành động đã thành công; event đó không tới được DW.
   */
  private async publish(events: FriendEvent[]): Promise<void> {
    try {
      await this.eventPublisher.publishFriendEvents(events);
    } catch (err) {
      this.log.error({ err, events }, 'event publish failed after db commit');
    }
  }

  private async applyInTx(session: DbSession, action: Action, actorId: number, targetId: number): Promise<Committed> {
    const { fwd, rev } = await this.friendshipDao.findPair(actorId, targetId, { session });
    const next = plan(action, fwd ? asStatus(fwd.status) : null);

    // event_time phải tăng dần theo từng cặp, kể cả khi đồng hồ giữa các instance lệch nhau:
    // nếu không, merge_condition phía DWD sẽ bỏ qua event mới.
    const eventTime = new Date(
      Math.max(
        this.now(),
        (fwd?.last_event_time.getTime() ?? 0) + 1,
        (rev?.last_event_time.getTime() ?? 0) + 1,
      ),
    );
    const rows: (FriendshipWrite & { status: Status })[] = [
      { user_id: actorId, friend_id: targetId, status: next.fwd, event_id: this.nextId() },
      { user_id: targetId, friend_id: actorId, status: next.rev, event_id: this.nextId() },
    ];

    await this.friendshipDao.upsertMany(rows, eventTime, { session });

    const eventTimeStr = formatEventTime(eventTime);
    return {
      result: { user_id: actorId, friend_id: targetId, status: next.fwd, event_time: eventTimeStr },
      events: rows.map((r) => ({
        user_id: r.user_id,
        friend_id: r.friend_id,
        event_type: r.status,
        event_time: eventTimeStr,
        event_id: r.event_id,
        source: this.source,
      })),
    };
  }

  // ---------------------------------------------------------------------------
  // Truy vấn (luôn là trạng thái mới nhất)
  // ---------------------------------------------------------------------------

  async listFriends(userId: number, paging: Paging): Promise<ListResult> {
    return this.listByStatus(userId, [Status.FRIEND], paging);
  }

  /** type: sent | received | all */
  async listRequests(userId: number, type: string, paging: Paging): Promise<ListResult> {
    return this.listByStatus(userId, statusesOf(REQUEST_TYPES, type, 'type'), paging);
  }

  /** type: blocking | blocked | all */
  async listBlocks(userId: number, type: string, paging: Paging): Promise<ListResult> {
    return this.listByStatus(userId, statusesOf(BLOCK_TYPES, type, 'type'), paging);
  }

  private async listByStatus(userId: number, statuses: readonly Status[], { limit, after }: Paging): Promise<ListResult> {
    const docs = await this.friendshipDao.listByStatus(userId, statuses, { limit, after });
    const last = docs.at(-1);
    return {
      items: docs.map((d) => ({
        friend_id: d.friend_id,
        status: asStatus(d.status),
        since: formatEventTime(d.last_event_time),
      })),
      next_after: docs.length === limit && last ? last.friend_id : null,
    };
  }

  /** Quan hệ 2 chiều giữa 2 user. */
  async relationship(userId: number, otherId: number): Promise<Relationship> {
    const { fwd, rev } = await this.friendshipDao.findPair(userId, otherId);
    const view = (d: typeof fwd): DirectionView | null =>
      d ? { status: asStatus(d.status), since: formatEventTime(d.last_event_time), event_id: d.last_event_id } : null;
    return { user_id: userId, friend_id: otherId, outgoing: view(fwd), incoming: view(rev) };
  }

  /** Số lượng theo từng status (giống dws_friend_summary nhưng không trễ). */
  async summary(userId: number): Promise<Summary> {
    const rows = await this.friendshipDao.countByStatus(userId);
    const count = (s: Status) => rows.find((r) => r._id === s)?.n ?? 0;
    const last = rows.reduce<Date | null>((m, r) => (m && m > r.last ? m : r.last), null);
    return {
      user_id: userId,
      friend_cnt: count(Status.FRIEND),
      pending_sent_cnt: count(Status.REQUESTED),
      pending_received_cnt: count(Status.REVIEWED),
      blocking_cnt: count(Status.BLOCKING),
      blocked_cnt: count(Status.BLOCKED),
      last_activity: last ? formatEventTime(last) : null,
    };
  }

  /** Bạn chung của 2 user. */
  async mutualFriends(userId: number, otherId: number, { limit }: Pick<Paging, 'limit'>): Promise<{ items: number[] }> {
    assertDifferent(userId, otherId);
    return { items: await this.friendshipDao.commonFriendIds(userId, otherId, Status.FRIEND, limit) };
  }
}

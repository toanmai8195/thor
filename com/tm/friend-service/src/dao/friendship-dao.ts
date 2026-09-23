// Collection `friendships`: 1 document / 1 cặp có hướng (tương đương dwd_friend_status).

import type { ClientSession, Collection, Db } from 'mongodb';

export interface FriendshipDoc {
  user_id: number;
  friend_id: number;
  /** 1 trong 7 status (README 2.1); luật nằm ở Controller */
  status: string;
  last_event_time: Date;
  last_event_id: string;
  created_at: Date;
  updated_at: Date;
}

/** 1 chiều cần ghi, cùng event_time với chiều còn lại */
export interface FriendshipWrite {
  user_id: number;
  friend_id: number;
  status: string;
  event_id: string;
}

export type FriendshipListItem = Pick<FriendshipDoc, 'friend_id' | 'status' | 'last_event_time'>;

export interface StatusCount {
  _id: string;
  n: number;
  last: Date;
}

interface SessionOption {
  session?: ClientSession;
}

export class FriendshipDao {
  private readonly col: Collection<FriendshipDoc>;

  constructor(db: Db) {
    this.col = db.collection<FriendshipDoc>('friendships');
  }

  async ensureIndexes(): Promise<void> {
    await this.col.createIndexes([
      { key: { user_id: 1, friend_id: 1 }, name: 'uq_pair', unique: true },
      { key: { user_id: 1, status: 1, friend_id: 1 }, name: 'ix_user_status_friend' },
    ]);
  }

  /** Đọc cả 2 chiều của cặp: fwd = userId→otherId, rev = otherId→userId. */
  async findPair(
    userId: number,
    otherId: number,
    { session }: SessionOption = {},
  ): Promise<{ fwd: FriendshipDoc | null; rev: FriendshipDoc | null }> {
    const docs = await this.col
      .find(
        {
          $or: [
            { user_id: userId, friend_id: otherId },
            { user_id: otherId, friend_id: userId },
          ],
        },
        { session },
      )
      .toArray();
    return {
      fwd: docs.find((d) => d.user_id === userId) ?? null,
      rev: docs.find((d) => d.user_id === otherId) ?? null,
    };
  }

  /** Upsert nhiều chiều với cùng event_time. */
  async upsertMany(rows: FriendshipWrite[], eventTime: Date, { session }: SessionOption = {}): Promise<void> {
    const now = new Date();
    await this.col.bulkWrite(
      rows.map((r) => ({
        updateOne: {
          filter: { user_id: r.user_id, friend_id: r.friend_id },
          update: {
            $set: {
              status: r.status,
              last_event_time: eventTime,
              last_event_id: r.event_id,
              updated_at: now,
            },
            $setOnInsert: { created_at: now },
          },
          upsert: true,
        },
      })),
      { session, ordered: true },
    );
  }

  /** Danh sách theo status, sắp theo friend_id, phân trang bằng `after`. */
  async listByStatus(
    userId: number,
    statuses: readonly string[],
    { limit, after }: { limit: number; after?: number },
  ): Promise<FriendshipListItem[]> {
    return this.col
      .find<FriendshipListItem>(
        {
          user_id: userId,
          status: { $in: [...statuses] },
          ...(after === undefined ? {} : { friend_id: { $gt: after } }),
        },
        { projection: { _id: 0, friend_id: 1, status: 1, last_event_time: 1 } },
      )
      .sort({ friend_id: 1 })
      .limit(limit)
      .toArray();
  }

  /** Số cặp theo từng status. */
  async countByStatus(userId: number): Promise<StatusCount[]> {
    return this.col
      .aggregate<StatusCount>([
        { $match: { user_id: userId } },
        { $group: { _id: '$status', n: { $sum: 1 }, last: { $max: '$last_event_time' } } },
      ])
      .toArray();
  }

  /** friend_id có `status` với cả 2 user. */
  async commonFriendIds(userId: number, otherId: number, status: string, limit: number): Promise<number[]> {
    const rows = await this.col
      .aggregate<{ _id: number }>([
        { $match: { user_id: { $in: [userId, otherId] }, status } },
        { $group: { _id: '$friend_id', n: { $sum: 1 } } },
        { $match: { n: 2 } },
        { $sort: { _id: 1 } },
        { $limit: limit },
      ])
      .toArray();
    return rows.map((r) => r._id);
  }
}

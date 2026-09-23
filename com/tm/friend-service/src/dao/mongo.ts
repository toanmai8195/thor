import type { ClientSession, MongoClient, TransactionOptions } from 'mongodb';

/** Session của transaction; các layer trên chỉ truyền lại cho Dao, không dùng trực tiếp. */
export type DbSession = ClientSession;

/** Chạy `fn(session)` trong 1 transaction; fn có thể chạy lại khi driver retry write conflict. */
export type TransactionRunner = <T>(fn: (session: DbSession) => Promise<T>) => Promise<T>;

const TXN_OPTIONS: TransactionOptions = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
};

export function createTransactionRunner(client: MongoClient): TransactionRunner {
  return async (fn) => {
    const session = client.startSession();
    try {
      return await session.withTransaction(() => fn(session), TXN_OPTIONS);
    } finally {
      await session.endSession();
    }
  };
}

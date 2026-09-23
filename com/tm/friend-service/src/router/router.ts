// Router: map method + path → handler. Không có logic.

import express, { type RequestHandler, type Router } from 'express';
import type { FriendHandler } from '../handler/friend-handler.js';

export interface RouterHandlers {
  friend: FriendHandler;
  health: RequestHandler;
}

export function createRouter({ friend, health }: RouterHandlers): Router {
  const root = express.Router();
  root.get('/healthz', health);

  const v1 = express.Router();

  // Hành động (mỗi hành động sinh 2 event đối xứng)
  v1.post('/users/:userId/requests/:targetId', friend.sendRequest); //          gửi lời mời
  v1.delete('/users/:userId/requests/:targetId', friend.cancelRequest); //      huỷ lời mời đã gửi
  v1.post('/users/:userId/requests/:targetId/accept', friend.acceptRequest); // chấp nhận lời mời target gửi
  v1.post('/users/:userId/requests/:targetId/reject', friend.rejectRequest); // từ chối lời mời target gửi
  v1.delete('/users/:userId/friends/:targetId', friend.unfriend); //            huỷ kết bạn
  v1.post('/users/:userId/blocks/:targetId', friend.block); //                  block

  // Truy vấn
  v1.get('/users/:userId/friends', friend.listFriends);
  v1.get('/users/:userId/requests', friend.listRequests); //   ?type=sent|received|all
  v1.get('/users/:userId/blocks', friend.listBlocks); //       ?type=blocking|blocked|all
  v1.get('/users/:userId/summary', friend.summary);
  v1.get('/users/:userId/relationships/:targetId', friend.relationship);
  v1.get('/users/:userId/mutual-friends/:targetId', friend.mutualFriends);

  root.use('/v1', v1);
  return root;
}

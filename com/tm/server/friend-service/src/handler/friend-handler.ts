// Handler: đọc request → gọi FriendController → ghi response. Không chứa luật nghiệp vụ.

import type { Request, RequestHandler } from 'express';
import type { FriendController } from '../controller/friend-controller.js';
import { Action } from '../controller/friendship-rules.js';
import { parsePaging, parseQueryString, parseUserId } from './params.js';

type UserParams = { userId: string };
type PairParams = { userId: string; targetId: string };

const userIdOf = (req: Request<UserParams>) => parseUserId(req.params.userId, 'userId');
const pairOf = (req: Request<PairParams>) => ({
  userId: parseUserId(req.params.userId, 'userId'),
  targetId: parseUserId(req.params.targetId, 'targetId'),
});

export interface FriendHandler {
  // Hành động
  sendRequest: RequestHandler<PairParams>;
  cancelRequest: RequestHandler<PairParams>;
  acceptRequest: RequestHandler<PairParams>;
  rejectRequest: RequestHandler<PairParams>;
  unfriend: RequestHandler<PairParams>;
  block: RequestHandler<PairParams>;
  // Truy vấn
  listFriends: RequestHandler<UserParams>;
  listRequests: RequestHandler<UserParams>;
  listBlocks: RequestHandler<UserParams>;
  summary: RequestHandler<UserParams>;
  relationship: RequestHandler<PairParams>;
  mutualFriends: RequestHandler<PairParams>;
}

export function createFriendHandler(
  controller: Pick<
    FriendController,
    'apply' | 'listFriends' | 'listRequests' | 'listBlocks' | 'summary' | 'relationship' | 'mutualFriends'
  >,
): FriendHandler {
  const act =
    (action: Action, status = 200): RequestHandler<PairParams> =>
    async (req, res) => {
      const { userId, targetId } = pairOf(req);
      res.status(status).json(await controller.apply(action, userId, targetId));
    };

  return {
    sendRequest: act(Action.REQUEST, 201),
    cancelRequest: act(Action.CANCEL),
    acceptRequest: act(Action.ACCEPT),
    rejectRequest: act(Action.REJECT),
    unfriend: act(Action.UNFRIEND),
    block: act(Action.BLOCK, 201),

    async listFriends(req, res) {
      res.json(await controller.listFriends(userIdOf(req), parsePaging(req.query)));
    },
    async listRequests(req, res) {
      const type = parseQueryString(req.query.type, 'type', 'all');
      res.json(await controller.listRequests(userIdOf(req), type, parsePaging(req.query)));
    },
    async listBlocks(req, res) {
      const type = parseQueryString(req.query.type, 'type', 'all');
      res.json(await controller.listBlocks(userIdOf(req), type, parsePaging(req.query)));
    },
    async summary(req, res) {
      res.json(await controller.summary(userIdOf(req)));
    },
    async relationship(req, res) {
      const { userId, targetId } = pairOf(req);
      res.json(await controller.relationship(userId, targetId));
    },
    async mutualFriends(req, res) {
      const { userId, targetId } = pairOf(req);
      res.json(await controller.mutualFriends(userId, targetId, { limit: parsePaging(req.query).limit }));
    },
  };
}

// Package sim giữ trạng thái quan hệ của tập user giả lập và chọn hành động hợp lệ tiếp theo,
// theo đúng luật của friend-service (README 10.5), để phần lớn request thành công.
package sim

import (
	"math/rand"
	"sync"
)

// Action: hành động gọi vào friend-service
type Action string

const (
	ActRequest  Action = "request"  // Actor mời Target
	ActCancel   Action = "cancel"   // Actor huỷ lời mời đã gửi Target
	ActAccept   Action = "accept"   // Actor chấp nhận lời mời Target gửi
	ActReject   Action = "reject"   // Actor từ chối lời mời Target gửi
	ActUnfriend Action = "unfriend" // Actor huỷ kết bạn với Target
	ActBlock    Action = "block"    // Actor block Target
)

// Step: 1 lần gọi API
type Step struct {
	Action Action
	Actor  int64
	Target int64
}

// Weight: tỉ lệ chọn mỗi hành động
type Weight struct {
	Action Action
	Weight int
}

// DefaultWeights: phần lớn là mời + chấp nhận để mạng bạn bè lớn dần
var DefaultWeights = []Weight{
	{ActRequest, 40},
	{ActAccept, 25},
	{ActReject, 5},
	{ActCancel, 5},
	{ActUnfriend, 15},
	{ActBlock, 10},
}

type pair struct{ a, b int64 }

// unordered: khoá không hướng cho 1 cặp
func unordered(a, b int64) pair {
	if a > b {
		a, b = b, a
	}
	return pair{a, b}
}

// State: trạng thái các cặp mà simulator biết. An toàn khi gọi từ nhiều goroutine.
type State struct {
	mu       sync.Mutex
	rnd      *rand.Rand
	start    int64
	n        int64
	weights  []Weight
	total    int
	pending  map[pair]struct{} // a đã mời b, đang chờ
	friends  map[pair]struct{} // khoá không hướng
	blocked  map[pair]struct{} // a block b
	inflight map[pair]struct{} // khoá không hướng: cặp đang có request chưa xong
}

func NewState(userIDStart int64, users int, seed int64, weights []Weight) *State {
	total := 0
	for _, w := range weights {
		total += w.Weight
	}
	return &State{
		rnd:      rand.New(rand.NewSource(seed)),
		start:    userIDStart,
		n:        int64(users),
		weights:  weights,
		total:    total,
		pending:  map[pair]struct{}{},
		friends:  map[pair]struct{}{},
		blocked:  map[pair]struct{}{},
		inflight: map[pair]struct{}{},
	}
}

// Next chọn hành động hợp lệ tiếp theo và đánh dấu cặp đang xử lý.
// Hành động được chọn không có cặp phù hợp thì chuyển sang gửi lời mời.
// Trả false nếu không tìm được cặp nào (tập user quá nhỏ / đều đang xử lý).
func (s *State) Next() (Step, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	step, ok := s.pick(s.chooseAction())
	if !ok {
		step, ok = s.pick(ActRequest)
	}
	if ok {
		s.inflight[unordered(step.Actor, step.Target)] = struct{}{}
	}
	return step, ok
}

func (s *State) chooseAction() Action {
	r := s.rnd.Intn(s.total)
	for _, w := range s.weights {
		if r < w.Weight {
			return w.Action
		}
		r -= w.Weight
	}
	return ActRequest
}

func (s *State) pick(a Action) (Step, bool) {
	switch a {
	case ActRequest:
		return s.randomPair(ActRequest, func(x, y int64) bool { return s.noRelation(x, y) })
	case ActBlock:
		return s.randomPair(ActBlock, func(x, y int64) bool { return !s.isBlocked(x, y) })
	case ActCancel:
		if p, ok := s.anyFree(s.pending); ok {
			return Step{ActCancel, p.a, p.b}, true // người mời huỷ
		}
	case ActAccept, ActReject:
		if p, ok := s.anyFree(s.pending); ok {
			return Step{a, p.b, p.a}, true // người được mời trả lời
		}
	case ActUnfriend:
		if p, ok := s.anyFree(s.friends); ok {
			if s.rnd.Intn(2) == 0 {
				p.a, p.b = p.b, p.a
			}
			return Step{ActUnfriend, p.a, p.b}, true
		}
	}
	return Step{}, false
}

// randomPair thử ngẫu nhiên vài cặp user khác nhau thoả điều kiện
func (s *State) randomPair(a Action, ok func(x, y int64) bool) (Step, bool) {
	for i := 0; i < 50; i++ {
		x := s.start + s.rnd.Int63n(s.n)
		y := s.start + s.rnd.Int63n(s.n)
		if x == y {
			continue
		}
		if _, busy := s.inflight[unordered(x, y)]; busy {
			continue
		}
		if ok(x, y) {
			return Step{a, x, y}, true
		}
	}
	return Step{}, false
}

// anyFree lấy 1 cặp bất kỳ (thứ tự duyệt map của Go là ngẫu nhiên) không đang xử lý
func (s *State) anyFree(m map[pair]struct{}) (pair, bool) {
	for p := range m {
		if _, busy := s.inflight[unordered(p.a, p.b)]; !busy {
			return p, true
		}
	}
	return pair{}, false
}

func (s *State) isBlocked(x, y int64) bool {
	_, a := s.blocked[pair{x, y}]
	_, b := s.blocked[pair{y, x}]
	return a || b
}

func (s *State) noRelation(x, y int64) bool {
	if s.isBlocked(x, y) {
		return false
	}
	if _, ok := s.friends[unordered(x, y)]; ok {
		return false
	}
	_, p1 := s.pending[pair{x, y}]
	_, p2 := s.pending[pair{y, x}]
	return !p1 && !p2
}

// Done: request của step đã xong. succeeded = friend-service trả 2xx → cập nhật trạng thái.
func (s *State) Done(step Step, succeeded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, unordered(step.Actor, step.Target))
	if !succeeded {
		return
	}
	x, y := step.Actor, step.Target
	switch step.Action {
	case ActRequest:
		s.pending[pair{x, y}] = struct{}{}
	case ActCancel:
		delete(s.pending, pair{x, y})
	case ActAccept:
		delete(s.pending, pair{y, x})
		s.friends[unordered(x, y)] = struct{}{}
	case ActReject:
		delete(s.pending, pair{y, x})
	case ActUnfriend:
		delete(s.friends, unordered(x, y))
	case ActBlock:
		s.clear(x, y)
		s.blocked[pair{x, y}] = struct{}{}
	}
}

// Resync đặt lại trạng thái cặp theo friend-service (dùng khi request bị 409:
// trạng thái local lệch, vd có client khác thao tác trên cùng cặp).
// outgoing = status của x→y, "" nếu chưa có quan hệ.
func (s *State) Resync(x, y int64, outgoing string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clear(x, y)
	switch outgoing {
	case "REQUESTED":
		s.pending[pair{x, y}] = struct{}{}
	case "REVIEWED":
		s.pending[pair{y, x}] = struct{}{}
	case "FRIEND":
		s.friends[unordered(x, y)] = struct{}{}
	case "BLOCKING":
		s.blocked[pair{x, y}] = struct{}{}
	case "BLOCKED":
		s.blocked[pair{y, x}] = struct{}{}
	}
}

func (s *State) clear(x, y int64) {
	delete(s.pending, pair{x, y})
	delete(s.pending, pair{y, x})
	delete(s.friends, unordered(x, y))
	delete(s.blocked, pair{x, y})
	delete(s.blocked, pair{y, x})
}

// Counts: số cặp theo loại (cho /stats)
type Counts struct {
	Pending  int `json:"pending"`
	Friends  int `json:"friends"`
	Blocked  int `json:"blocked"`
	InFlight int `json:"in_flight"`
}

func (s *State) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Counts{len(s.pending), len(s.friends), len(s.blocked), len(s.inflight)}
}

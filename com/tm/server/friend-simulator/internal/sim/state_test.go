package sim

import "testing"

func TestNext_FirstStepIsRequest(t *testing.T) {
	s := NewState(100, 10, 1, DefaultWeights)
	step, ok := s.Next()
	if !ok || step.Action != ActRequest || step.Actor == step.Target {
		t.Fatalf("step=%+v ok=%v", step, ok)
	}
	if step.Actor < 100 || step.Actor >= 110 || step.Target < 100 || step.Target >= 110 {
		t.Fatalf("user ngoài tập: %+v", step)
	}
}

func TestLifecycle_RequestAcceptUnfriend(t *testing.T) {
	s := NewState(1, 2, 1, []Weight{{ActRequest, 1}})
	req, _ := s.Next()
	s.Done(req, true)
	if c := s.Counts(); c.Pending != 1 || c.InFlight != 0 {
		t.Fatalf("sau request: %+v", c)
	}

	s.weights, s.total = []Weight{{ActAccept, 1}}, 1
	acc, ok := s.Next()
	if !ok || acc.Action != ActAccept || acc.Actor != req.Target || acc.Target != req.Actor {
		t.Fatalf("accept phải do người được mời thực hiện: req=%+v acc=%+v", req, acc)
	}
	s.Done(acc, true)
	if c := s.Counts(); c.Pending != 0 || c.Friends != 1 {
		t.Fatalf("sau accept: %+v", c)
	}

	s.weights = []Weight{{ActUnfriend, 1}}
	unf, ok := s.Next()
	if !ok || unf.Action != ActUnfriend {
		t.Fatalf("unfriend=%+v", unf)
	}
	s.Done(unf, true)
	if c := s.Counts(); c.Friends != 0 {
		t.Fatalf("sau unfriend: %+v", c)
	}
}

func TestNoCandidate_FallsBackToRequest(t *testing.T) {
	s := NewState(1, 10, 1, []Weight{{ActAccept, 1}})
	step, ok := s.Next()
	if !ok || step.Action != ActRequest {
		t.Fatalf("chưa có lời mời nào thì phải gửi lời mời: %+v", step)
	}
}

func TestInFlightPairNotReused(t *testing.T) {
	// 2 user → chỉ 1 cặp; đang xử lý thì không chọn được nữa
	s := NewState(1, 2, 1, []Weight{{ActRequest, 1}})
	if _, ok := s.Next(); !ok {
		t.Fatal("lần 1 phải chọn được")
	}
	if _, ok := s.Next(); ok {
		t.Fatal("cặp đang xử lý không được chọn lại")
	}
}

func TestFailedStepDoesNotChangeState(t *testing.T) {
	s := NewState(1, 2, 1, []Weight{{ActRequest, 1}})
	step, _ := s.Next()
	s.Done(step, false)
	if c := s.Counts(); c.Pending != 0 || c.InFlight != 0 {
		t.Fatalf("%+v", c)
	}
}

func TestBlock_ClearsOtherRelations(t *testing.T) {
	s := NewState(1, 2, 1, nil)
	s.friends[unordered(1, 2)] = struct{}{}
	s.Done(Step{ActBlock, 1, 2}, true)
	if c := s.Counts(); c.Friends != 0 || c.Blocked != 1 {
		t.Fatalf("%+v", c)
	}
	if s.noRelation(1, 2) {
		t.Fatal("cặp đã block không được mời nữa")
	}
}

func TestResync(t *testing.T) {
	s := NewState(1, 2, 1, nil)
	s.pending[pair{1, 2}] = struct{}{}
	s.Resync(1, 2, "BLOCKED") // thực tế 2 đã block 1
	if _, ok := s.blocked[pair{2, 1}]; !ok || len(s.pending) != 0 {
		t.Fatalf("pending=%v blocked=%v", s.pending, s.blocked)
	}
	s.Resync(1, 2, "")
	if c := s.Counts(); c.Blocked != 0 {
		t.Fatalf("%+v", c)
	}
}

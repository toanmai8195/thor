package sim

import (
	"sync"
	"time"
)

// Outcome: kết quả 1 request
type Outcome string

const (
	OutcomeOK       Outcome = "ok"       // 2xx
	OutcomeConflict Outcome = "conflict" // 409 / 403: trạng thái local lệch, đã resync
	OutcomeError    Outcome = "error"    // lỗi mạng hoặc status khác
	OutcomeSkipped  Outcome = "skipped"  // bỏ lượt: hết slot song song hoặc không chọn được cặp
)

// Stats: đếm request theo hành động và kết quả
type Stats struct {
	mu        sync.Mutex
	started   time.Time
	counts    map[Action]map[Outcome]int
	skipped   int
	latencyNs int64
	done      int
}

func NewStats() *Stats {
	return &Stats{started: time.Now(), counts: map[Action]map[Outcome]int{}}
}

func (s *Stats) Record(a Action, o Outcome, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts[a] == nil {
		s.counts[a] = map[Outcome]int{}
	}
	s.counts[a][o]++
	s.latencyNs += latency.Nanoseconds()
	s.done++
}

func (s *Stats) Skip() {
	s.mu.Lock()
	s.skipped++
	s.mu.Unlock()
}

// Snapshot: số liệu tích luỹ từ lúc start
type Snapshot struct {
	UptimeSec    float64                    `json:"uptime_sec"`
	Requests     int                        `json:"requests"`
	ActualRPS    float64                    `json:"actual_rps"`
	AvgLatencyMs float64                    `json:"avg_latency_ms"`
	Skipped      int                        `json:"skipped"`
	ByAction     map[Action]map[Outcome]int `json:"by_action"`
	ByOutcome    map[Outcome]int            `json:"by_outcome"`
	Pairs        Counts                     `json:"pairs"`
}

func (s *Stats) Snapshot(pairs Counts) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	up := time.Since(s.started).Seconds()
	snap := Snapshot{
		UptimeSec: up,
		Requests:  s.done,
		Skipped:   s.skipped,
		ByAction:  map[Action]map[Outcome]int{},
		ByOutcome: map[Outcome]int{},
		Pairs:     pairs,
	}
	if up > 0 {
		snap.ActualRPS = float64(s.done) / up
	}
	if s.done > 0 {
		snap.AvgLatencyMs = float64(s.latencyNs) / float64(s.done) / 1e6
	}
	for a, m := range s.counts {
		snap.ByAction[a] = map[Outcome]int{}
		for o, n := range m {
			snap.ByAction[a][o] = n
			snap.ByOutcome[o] += n
		}
	}
	return snap
}

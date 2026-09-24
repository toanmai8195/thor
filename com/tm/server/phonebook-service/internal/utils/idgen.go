package utils

import (
	"fmt"
	"sync"
	"time"
)

var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// IDGenerator sinh sync_id kiểu snowflake: tăng dần theo thời gian, 19 chữ số khi in.
//
//	41 bit ms từ 2020-01-01 | 10 bit worker | 12 bit sequence
type IDGenerator struct {
	mu     sync.Mutex
	worker int64
	lastMs int64
	seq    int64
	now    func() time.Time
}

func NewIDGenerator(worker int64) *IDGenerator {
	return &IDGenerator{worker: worker & 0x3ff, now: time.Now}
}

// Next trả id mới, luôn lớn hơn id trước đó của cùng Generator.
func (g *IDGenerator) Next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := g.now().Sub(epoch).Milliseconds()
	if ms < g.lastMs {
		ms = g.lastMs // đồng hồ lùi: dùng tiếp mốc cũ
	}
	if ms == g.lastMs {
		g.seq = (g.seq + 1) & 0xfff
		if g.seq == 0 {
			ms++ // hết 4096 id trong 1 ms: mượn ms kế tiếp
		}
	} else {
		g.seq = 0
	}
	g.lastMs = ms
	return uint64(ms<<22 | g.worker<<12 | g.seq)
}

// FormatID: id → chuỗi 19 chữ số (pad 0), so sánh chuỗi đúng thứ tự thời gian.
func FormatID(id uint64) string {
	return fmt.Sprintf("%019d", id)
}

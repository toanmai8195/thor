package dao

import "testing"

func TestBatches(t *testing.T) {
	for _, c := range []struct{ n, batches, last int }{{0, 0, 0}, {1, 1, 1}, {1000, 1, 1000}, {1001, 2, 1}, {20000, 20, 1000}} {
		b := Batches(make([]Event, c.n))
		if len(b) != c.batches {
			t.Errorf("%d event: %d lô, muốn %d", c.n, len(b), c.batches)
			continue
		}
		if c.batches > 0 && len(b[len(b)-1]) != c.last {
			t.Errorf("%d event: lô cuối %d, muốn %d", c.n, len(b[len(b)-1]), c.last)
		}
	}
}

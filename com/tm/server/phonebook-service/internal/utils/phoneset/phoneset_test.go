package phoneset

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, set := range [][]uint64{
		nil,
		{0},
		{7},
		{1000000100, 1000000350, 1000001000},
		{1, 127, 128, 16383, 16384, 1 << 40, 1<<63 + 5},
	} {
		b, err := Encode(set)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, set) && !(len(got) == 0 && len(set) == 0) {
			t.Errorf("Decode(Encode(%v)) = %v", set, got)
		}
	}
}

func TestEncodeBytes(t *testing.T) {
	// delta [300, 1]: 300 = 0xAC 0x02, 1 = 0x01
	b, _ := Encode([]uint64{300, 301})
	if want := []byte{0xAC, 0x02, 0x01}; !bytes.Equal(b, want) {
		t.Errorf("Encode = % x, muốn % x", b, want)
	}
}

func TestEncodeRejectsUnsorted(t *testing.T) {
	for _, set := range [][]uint64{{2, 1}, {5, 5}} {
		if _, err := Encode(set); err == nil {
			t.Errorf("Encode(%v) phải lỗi", set)
		}
	}
}

func TestDecodeCorrupt(t *testing.T) {
	for _, b := range [][]byte{
		{0x80},       // varint bị cắt
		{0x05, 0x00}, // delta 0 → trùng phần tử
	} {
		if _, err := Decode(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Decode(% x) err = %v, muốn ErrCorrupt", b, err)
		}
	}
}

func TestNormalize(t *testing.T) {
	got := Normalize([]uint64{5, 1, 3, 1, 5})
	if want := []uint64{1, 3, 5}; !slices.Equal(got, want) {
		t.Errorf("Normalize = %v, muốn %v", got, want)
	}
}

func TestDiff(t *testing.T) {
	cases := []struct {
		prev, next, added, deleted []uint64
	}{
		{nil, nil, nil, nil},
		{nil, []uint64{1, 2}, []uint64{1, 2}, nil},
		{[]uint64{1, 2}, nil, nil, []uint64{1, 2}},
		{[]uint64{1, 3, 5, 7}, []uint64{1, 4, 5, 8, 9}, []uint64{4, 8, 9}, []uint64{3, 7}},
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3}, nil, nil},
	}
	for _, c := range cases {
		added, deleted := Diff(c.prev, c.next)
		if !slices.Equal(added, c.added) || !slices.Equal(deleted, c.deleted) {
			t.Errorf("Diff(%v, %v) = +%v -%v, muốn +%v -%v", c.prev, c.next, added, deleted, c.added, c.deleted)
		}
	}
}

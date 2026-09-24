// Package phoneset lưu 1 tập số (vd phone_enc của 1 danh bạ) thành []byte bằng
// delta + varint, và diff 2 tập (PHONEBOOK.md mục 2.3, 2.4).
//
// Tập đã sort tăng dần, lưu khoảng cách giữa 2 số liên tiếp dưới dạng varint
// (7 bit / byte, bit cao = còn byte tiếp). Số đầu tiên lưu nguyên giá trị.
//
//	[1000000100, 1000000350, 1000001000] → delta [1000000100, 250, 650] → 5 + 2 + 2 byte
package phoneset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

var ErrCorrupt = errors.New("phoneset: dữ liệu hỏng")

// Normalize sort tăng dần và bỏ phần tử trùng (sửa trực tiếp slice đầu vào).
func Normalize(nums []uint64) []uint64 {
	slices.Sort(nums)
	return slices.Compact(nums)
}

// Encode: tập tăng ngặt → []byte. Chưa sort / có trùng → lỗi (gọi Normalize trước).
func Encode(sorted []uint64) ([]byte, error) {
	b := make([]byte, 0, len(sorted)*4)
	var prev uint64
	for i, v := range sorted {
		if i > 0 && v <= prev {
			return nil, fmt.Errorf("phoneset: phần tử %d (%d) không lớn hơn phần tử trước (%d)", i, v, prev)
		}
		b = binary.AppendUvarint(b, v-prev)
		prev = v
	}
	return b, nil
}

// Decode: []byte → tập tăng ngặt.
func Decode(b []byte) ([]uint64, error) {
	out := make([]uint64, 0, len(b)/3)
	var prev uint64
	for len(b) > 0 {
		d, n := binary.Uvarint(b)
		if n <= 0 || (len(out) > 0 && d == 0) {
			return nil, ErrCorrupt
		}
		prev += d
		out = append(out, prev)
		b = b[n:]
	}
	return out, nil
}

// Diff 2 tập đã sort: added = có trong next mà không có trong prev, deleted = ngược lại.
// Merge 1 lượt, O(len(prev) + len(next)).
func Diff(prev, next []uint64) (added, deleted []uint64) {
	i, j := 0, 0
	for i < len(prev) && j < len(next) {
		switch {
		case prev[i] == next[j]:
			i++
			j++
		case prev[i] < next[j]:
			deleted = append(deleted, prev[i])
			i++
		default:
			added = append(added, next[j])
			j++
		}
	}
	deleted = append(deleted, prev[i:]...)
	added = append(added, next[j:]...)
	return added, deleted
}

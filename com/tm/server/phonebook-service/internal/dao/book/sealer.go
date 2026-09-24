package book

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// Sealer: AES-256-GCM có version khoá.
//
//	blob = [1 byte version][12 byte nonce ngẫu nhiên][bản mã + 16 byte tag]
//
// Ghi luôn dùng version hiện tại; đọc chọn khoá theo byte đầu → đổi khoá dần được.
type Sealer struct {
	current byte
	aeads   map[byte]cipher.AEAD
}

var ErrKeyVersion = errors.New("book: không có khoá cho version này")

// NewSealer: keys[version] = khoá 32 byte; current là version dùng khi ghi.
func NewSealer(current byte, keys map[byte][]byte) (*Sealer, error) {
	s := &Sealer{current: current, aeads: make(map[byte]cipher.AEAD, len(keys))}
	for v, k := range keys {
		if len(k) != 32 {
			return nil, fmt.Errorf("khoá version %d phải 32 byte", v)
		}
		block, err := aes.NewCipher(k)
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		s.aeads[v] = aead
	}
	if _, ok := s.aeads[current]; !ok {
		return nil, fmt.Errorf("thiếu khoá cho version hiện tại %d", current)
	}
	return s, nil
}

func (s *Sealer) Seal(plain, aad []byte) ([]byte, error) {
	aead := s.aeads[s.current]
	out := make([]byte, 1+aead.NonceSize(), 1+aead.NonceSize()+len(plain)+aead.Overhead())
	out[0] = s.current
	if _, err := rand.Read(out[1:]); err != nil {
		return nil, err
	}
	return aead.Seal(out, out[1:], plain, aad), nil
}

// Open báo lỗi khi sai khoá, dữ liệu bị sửa, hoặc aad khác lúc Seal.
func (s *Sealer) Open(blob, aad []byte) ([]byte, error) {
	if len(blob) == 0 {
		return nil, fmt.Errorf("%w: blob rỗng", ErrCorrupt)
	}
	aead, ok := s.aeads[blob[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrKeyVersion, blob[0])
	}
	n := aead.NonceSize()
	if len(blob) < 1+n+aead.Overhead() {
		return nil, fmt.Errorf("%w: blob quá ngắn", ErrCorrupt)
	}
	return aead.Open(nil, blob[1:1+n], blob[1+n:], aad)
}

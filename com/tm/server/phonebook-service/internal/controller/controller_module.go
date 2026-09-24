package controller

import (
	"log/slog"
	"time"

	"go.uber.org/fx"

	"thor/server/phonebook-service/internal/dao"
	"thor/server/phonebook-service/internal/dao/book"
	"thor/server/phonebook-service/internal/utils"
	"thor/server/phonebook-service/internal/utils/phonecodec"
)

// Settings: phần config layer controller cần.
type Settings struct {
	// Khoá FF1 cho số điện thoại (32 byte), version 1
	PhoneKey []byte
	// Khoá AES-256-GCM cho tên contact và digest bucket (32 byte), version 1
	DataKey []byte
	// Khoá HMAC cho root digest lưu ở HBase
	DigestKey        []byte
	LeaseTTL         time.Duration
	MaxContacts      int
	MaxBatchContacts int
}

// Module đăng ký: *phonecodec.Codec (FF1), *book.Codec (layout + AES-GCM), *PhonebookController.
var Module = fx.Module("controller",
	fx.Provide(
		func(s Settings) (*phonecodec.Codec, error) { return phonecodec.New(1, s.PhoneKey) },
		func(s Settings) (*book.Codec, error) {
			sealer, err := book.NewSealer(1, map[byte][]byte{1: s.DataKey})
			if err != nil {
				return nil, err
			}
			return book.NewCodec(sealer)
		},
		func(d dao.PhonebookDao, pub dao.EventPublisher, phones *phonecodec.Codec, bc *book.Codec,
			ids *utils.IDGenerator, s Settings, log *slog.Logger) *PhonebookController {
			return NewPhonebookController(d, pub, phones, bc, s.DigestKey, ids, Options{
				LeaseTTL:         s.LeaseTTL,
				MaxContacts:      s.MaxContacts,
				MaxBatchContacts: s.MaxBatchContacts,
			}, log)
		},
	),
)

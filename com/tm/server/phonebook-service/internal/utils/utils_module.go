package utils

import (
	"log/slog"
	"os"

	"go.uber.org/fx"
)

// Settings: phần config layer utils cần.
type Settings struct {
	// 0–1023, mỗi instance 1 giá trị khác nhau để sync_id không trùng
	WorkerID int64
}

// Module đăng ký: *slog.Logger, *IDGenerator.
var Module = fx.Module("utils",
	fx.Provide(
		NewLogger,
		func(s Settings) *IDGenerator { return NewIDGenerator(s.WorkerID) },
	),
)

// NewLogger: log JSON ra stdout.
func NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

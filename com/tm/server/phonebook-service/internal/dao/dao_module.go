package dao

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.uber.org/fx"
)

// Settings: phần config layer dao cần.
type Settings struct {
	HBaseZK    string
	HBaseTable string
	// > 0: tự tạo bảng với số region này khi start (chỉ local / test)
	HBaseCreateRegions int
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaClientID      string
}

// HealthPinger: kiểm tra kết nối HBase cho /healthz.
type HealthPinger func(ctx context.Context) error

// Module đăng ký PhonebookDao (HBase), EventPublisher (Kafka), HealthPinger.
// Lifecycle: start = (tạo bảng) → kết nối Kafka; stop = ngắt Kafka → đóng HBase.
var Module = fx.Module("dao",
	fx.Provide(
		newHBaseDao,
		func(h *HBasePhonebookDao) PhonebookDao { return h },
		func(h *HBasePhonebookDao) HealthPinger {
			return func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				return h.Ping(ctx)
			}
		},
		newKafkaPublisher,
		func(k *KafkaEventPublisher) EventPublisher { return k },
	),
)

func newHBaseDao(lc fx.Lifecycle, s Settings, log *slog.Logger) *HBasePhonebookDao {
	h := NewHBasePhonebookDao(s.HBaseZK, s.HBaseTable)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if s.HBaseCreateRegions <= 0 {
				return nil
			}
			if err := CreateTable(ctx, s.HBaseZK, s.HBaseTable, s.HBaseCreateRegions); err != nil {
				return fmt.Errorf("tạo bảng %s: %w", s.HBaseTable, err)
			}
			log.Info("hbase table ready", "table", s.HBaseTable, "regions", s.HBaseCreateRegions)
			return nil
		},
		OnStop: func(context.Context) error {
			h.Close()
			return nil
		},
	})
	return h
}

func newKafkaPublisher(lc fx.Lifecycle, s Settings, log *slog.Logger) *KafkaEventPublisher {
	k := NewKafkaEventPublisher(s.KafkaBrokers, s.KafkaClientID, s.KafkaTopic, log)
	lc.Append(fx.Hook{OnStart: k.Start, OnStop: k.Stop})
	return k
}

package main

import (
	"log/slog"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"thor/server/phonebook-service/internal/configs"
	"thor/server/phonebook-service/internal/controller"
	"thor/server/phonebook-service/internal/dao"
	"thor/server/phonebook-service/internal/handler"
	"thor/server/phonebook-service/internal/router"
	"thor/server/phonebook-service/internal/utils"
)

// startTimeout: đủ cho Kafka / HBase vừa khởi động (dao thử lại kết nối trong thời gian này)
const startTimeout = 90 * time.Second

// Options: toàn bộ đồ thị DI. Tách khỏi main để test kiểm tra đồ thị (app_test.go) mà không kết nối thật.
func Options(cfg configs.Config) fx.Option {
	return fx.Options(
		// Config chỉ MainServer đọc; mỗi layer nhận Settings của riêng nó
		fx.Supply(
			utils.Settings{WorkerID: cfg.WorkerID},
			dao.Settings{
				HBaseZK:            cfg.HBaseZK,
				HBaseTable:         cfg.HBaseTable,
				HBaseCreateRegions: cfg.HBaseCreateRegions,
				KafkaBrokers:       cfg.KafkaBrokers,
				KafkaTopic:         cfg.KafkaTopic,
				KafkaClientID:      cfg.KafkaClientID,
			},
			controller.Settings{
				PhoneKey:         cfg.PhoneKey,
				DataKey:          cfg.DataKey,
				DigestKey:        cfg.DigestKey,
				LeaseTTL:         cfg.LeaseTTL,
				MaxContacts:      cfg.MaxContacts,
				MaxBatchContacts: cfg.MaxBatchContacts,
			},
			httpSettings{Port: cfg.Port},
		),
		utils.Module,
		dao.Module,
		controller.Module,
		handler.Module,
		router.Module,
		// handler không phụ thuộc dao: MainServer nối health check của dao vào handler
		fx.Provide(func(p dao.HealthPinger) handler.Pinger { return handler.Pinger(p) }),
		fx.Provide(newHTTPServer),
		fx.Invoke(func(*httpServer) {}),
		fx.StartTimeout(startTimeout),
		// Sự kiện DI của fx (provided / invoking / hook...) ở mức debug; lỗi vẫn ở mức error
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log}
			l.UseLogLevel(slog.LevelDebug)
			l.UseErrorLevel(slog.LevelError)
			return l
		}),
	)
}

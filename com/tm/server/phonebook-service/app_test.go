package main

import (
	"testing"
	"time"

	"go.uber.org/fx"

	"thor/server/phonebook-service/internal/configs"
)

// Đồ thị DI resolve được mọi thành phần mà không kết nối HBase / Kafka thật:
// thiếu / sai kiểu dependency là test fail (giống test/container.test.ts của friend-service).
func TestDependencyGraph(t *testing.T) {
	key := make([]byte, 32)
	cfg := configs.Config{
		Port: 0, HBaseZK: "localhost:2181", HBaseTable: "phonebook",
		KafkaBrokers: []string{"localhost:29092"}, KafkaTopic: "t", KafkaClientID: "c",
		PhoneKey: key, DataKey: key, DigestKey: key,
		LeaseTTL: time.Minute, MaxContacts: 20000, MaxBatchContacts: 5000,
	}
	if err := fx.ValidateApp(Options(cfg)); err != nil {
		t.Fatal(err)
	}
}

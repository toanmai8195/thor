// Package configs đọc cấu hình phonebook-service từ biến môi trường (PHONEBOOK.md mục 2).
// Chỉ MainServer đọc; mỗi layer nhận phần Settings của riêng nó.
package configs

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Cổng HTTP
	Port int
	// ZooKeeper quorum của HBase, vd "zk1:2181,zk2:2181"
	HBaseZK string
	// Tên bảng HBase (production tạo sẵn, xem PHONEBOOK.md mục 2.3)
	HBaseTable string
	// Local / test: tự tạo bảng nếu chưa có, với số region này (0 = không tạo)
	HBaseCreateRegions int
	// Danh sách broker Kafka
	KafkaBrokers []string
	// Topic ghi event thay đổi danh bạ (event-gateway đọc)
	KafkaTopic    string
	KafkaClientID string
	// 0–1023, mỗi instance 1 giá trị khác nhau để sync_id không trùng
	WorkerID int64
	// Khoá FF1 cho số điện thoại (32 byte), version 1 (chữ số đầu của phone_enc)
	PhoneKey []byte
	// Khoá AES-256-GCM cho tên contact và digest bucket (32 byte), version 1
	DataKey []byte
	// Khoá HMAC cho root digest lưu ở HBase (≥ 32 byte)
	DigestKey []byte
	// Thời gian giữ khoá theo user trong lúc sync
	LeaseTTL time.Duration
	// Số contact tối đa của 1 thiết bị
	MaxContacts int
	// Số contact tối đa trong 1 request upload
	MaxBatchContacts int
}

func Load() (Config, error) {
	cfg := Config{
		Port:               intEnv("PORT", 3100),
		HBaseZK:            strEnv("HBASE_ZK", "localhost:2181"),
		HBaseTable:         strEnv("HBASE_TABLE", "phonebook"),
		HBaseCreateRegions: intEnv("HBASE_CREATE_TABLE_REGIONS", 0),
		KafkaBrokers:       splitEnv("KAFKA_BROKERS", "localhost:29092"),
		KafkaTopic:         strEnv("KAFKA_TOPIC", "phonebook_service_events"),
		KafkaClientID:      strEnv("KAFKA_CLIENT_ID", "phonebook-service"),
		WorkerID:           int64(intEnv("WORKER_ID", 0)),
		LeaseTTL:           time.Duration(intEnv("LEASE_TTL_SECONDS", 60)) * time.Second,
		MaxContacts:        intEnv("MAX_CONTACTS", 20000),
		MaxBatchContacts:   intEnv("MAX_BATCH_CONTACTS", 5000),
	}
	var err error
	if cfg.PhoneKey, err = keyEnv("PHONE_KEY_V1"); err != nil {
		return cfg, err
	}
	if cfg.DataKey, err = keyEnv("DATA_KEY_V1"); err != nil {
		return cfg, err
	}
	if cfg.DigestKey, err = keyEnv("DIGEST_KEY"); err != nil {
		return cfg, err
	}
	if cfg.WorkerID < 0 || cfg.WorkerID > 1023 {
		return cfg, fmt.Errorf("WORKER_ID phải trong 0..1023: %d", cfg.WorkerID)
	}
	return cfg, nil
}

// keyEnv đọc khoá hex 32 byte (64 ký tự) — bắt buộc, không có giá trị mặc định.
func keyEnv(name string) ([]byte, error) {
	v := os.Getenv(name)
	if v == "" {
		return nil, fmt.Errorf("thiếu biến %s (hex 32 byte)", name)
	}
	k, err := hex.DecodeString(v)
	if err != nil || len(k) != 32 {
		return nil, fmt.Errorf("%s phải là hex 32 byte (64 ký tự)", name)
	}
	return k, nil
}

func strEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return def
}

func splitEnv(key, def string) []string {
	parts := strings.Split(strEnv(key, def), ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

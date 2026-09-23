// Package config đọc cấu hình event-gateway từ biến môi trường.
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Cổng HTTP nhận event
	Port int
	// Danh sách broker Kafka
	KafkaBrokers []string
	// Topic nhận friend event (StarRocks Routine Load đọc topic này)
	FriendTopic string
	// client.id gửi lên Kafka
	KafkaClientID string
}

func Load() Config {
	return Config{
		Port:          intEnv("PORT", 8080),
		KafkaBrokers:  splitEnv("KAFKA_BROKERS", "localhost:29092"),
		FriendTopic:   strEnv("KAFKA_FRIEND_TOPIC", "friend_events"),
		KafkaClientID: strEnv("KAFKA_CLIENT_ID", "event-gateway"),
	}
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

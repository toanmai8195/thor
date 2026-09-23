// Package config đọc cấu hình event-gateway từ biến môi trường.
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Cổng HTTP cho health check
	Port int
	// Danh sách broker Kafka
	KafkaBrokers []string
	// Consumer group đọc InputTopic
	GroupID string
	// Topic friend-service gửi event vào
	InputTopic string
	// Topic đã kiểm tra contract; StarRocks Routine Load đọc topic này
	FriendTopic string
	// Topic chứa event sai contract (dead letter)
	DLQTopic string
	// client.id gửi lên Kafka
	KafkaClientID string
}

func Load() Config {
	return Config{
		Port:          intEnv("PORT", 8080),
		KafkaBrokers:  splitEnv("KAFKA_BROKERS", "localhost:29092"),
		GroupID:       strEnv("KAFKA_GROUP_ID", "event-gateway"),
		InputTopic:    strEnv("KAFKA_INPUT_TOPIC", "friend_service_events"),
		FriendTopic:   strEnv("KAFKA_FRIEND_TOPIC", "friend_events"),
		DLQTopic:      strEnv("KAFKA_DLQ_TOPIC", "friend_events_dlq"),
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

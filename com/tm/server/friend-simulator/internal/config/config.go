// Package config đọc cấu hình friend-simulator từ biến môi trường.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Cổng HTTP cho /healthz và /stats
	Port int
	// Base URL của friend-service
	FriendServiceURL string
	// Số request mỗi giây gửi tới friend-service
	RPS float64
	// Số request chạy song song tối đa
	Concurrency int
	// Tập user giả lập: [UserIDStart, UserIDStart + Users)
	UserIDStart int64
	Users       int
	// Seed cho random; 0 = theo thời gian
	Seed int64
	// Thời gian chạy; 0 = chạy tới khi bị dừng
	Duration time.Duration
	// Chu kỳ in thống kê
	StatsInterval time.Duration
}

func Load() Config {
	return Config{
		Port:             intEnv("PORT", 8090),
		FriendServiceURL: strEnv("FRIEND_SERVICE_URL", "http://localhost:3000"),
		RPS:              floatEnv("RPS", 10),
		Concurrency:      intEnv("CONCURRENCY", 8),
		UserIDStart:      int64(intEnv("USER_ID_START", 100000)),
		Users:            intEnv("USERS", 1000),
		Seed:             int64(intEnv("SEED", 0)),
		Duration:         durationEnv("DURATION", 0),
		StatsInterval:    durationEnv("STATS_INTERVAL", 10*time.Second),
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

func floatEnv(key string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return f
	}
	return def
}

func durationEnv(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}

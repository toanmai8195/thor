module thor/server

go 1.22.0

// =============================================================================
// DEPENDENCIES FOR TRACKING SERVICES
// =============================================================================
// Để install: go mod tidy
// Để update: go get -u ./...
// =============================================================================

require (
	// Kafka client library - IBM Sarama
	// - High-performance Kafka client
	// - Producer và Consumer APIs
	// - Full Kafka protocol support
	github.com/IBM/sarama v1.43.0

	// FF1 format-preserving encryption (NIST SP 800-38G)
	// - Mã hoá số điện thoại tất định, giải mã được (phonecodec)
	github.com/capitalone/fpe v1.2.1

	// UUID generation
	// - UUID v4 cho event_id, session_id
	// - Cryptographically secure
	github.com/google/uuid v1.6.0

	// zstd nén tên contact trước khi mã hoá (phonebook-service); sarama cũng dùng
	github.com/klauspost/compress v1.17.7

	// HBase client (phonebook-service). Pin commit cuối còn hỗ trợ Go 1.22;
	// bản mới hơn cần Go 1.23+ (Bazel đang dùng go_sdk 1.22)
	github.com/tsuna/gohbase v0.0.0-20250203233440-58ca36d3c163

	// DI + lifecycle cho phonebook-service (cùng version với momo)
	go.uber.org/fx v1.22.2

	// Unicode NFC cho tên contact khi tính digest danh bạ (phonedigest)
	golang.org/x/text v0.14.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.6.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-zookeeper/zk v1.0.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/prometheus/client_golang v1.19.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.54.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	go.opentelemetry.io/otel v1.27.0 // indirect
	go.opentelemetry.io/otel/metric v1.27.0 // indirect
	go.opentelemetry.io/otel/trace v1.27.0 // indirect
	go.uber.org/dig v1.18.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	modernc.org/b/v2 v2.1.0 // indirect
)

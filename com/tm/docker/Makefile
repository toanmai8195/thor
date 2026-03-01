# =============================================================================
# MAKEFILE - DROPSHIPPING TRACKING PLATFORM
# =============================================================================
# File này cung cấp các commands thuận tiện để quản lý hệ thống.
#
# USAGE:
#   make help         # Xem tất cả commands
#   make up           # Start all services
#   make down         # Stop all services
#   make logs         # View logs
#   make producer     # Run event producer
#   make ingestor     # Run tracking ingestor
# =============================================================================

.PHONY: help up down restart logs ps clean \
        producer ingestor dbt \
        kafka-topics kafka-ui clickhouse-cli starrocks-cli \
        test build

# Default target
.DEFAULT_GOAL := help

# =============================================================================
# HELP
# =============================================================================
help: ## Hiển thị help
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║        DROPSHIPPING TRACKING PLATFORM - COMMANDS                 ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "INFRASTRUCTURE:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E 'up|down|restart|logs|ps|clean' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "SERVICES:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E 'producer|ingestor|dbt' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "TOOLS:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E 'kafka|clickhouse|starrocks|superset' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "BUILD:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E 'build|test' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""

# =============================================================================
# INFRASTRUCTURE COMMANDS
# =============================================================================

up: ## Start tất cả services (Docker Compose)
	@echo "Starting all services..."
	docker-compose up -d
	@echo ""
	@echo "Services starting up. Wait 2-3 minutes for all to be ready."
	@echo "Check status: make ps"

down: ## Stop tất cả services
	@echo "Stopping all services..."
	docker-compose down

restart: ## Restart tất cả services
	@echo "Restarting all services..."
	docker-compose restart

logs: ## Xem logs của tất cả services
	docker-compose logs -f --tail=100

logs-kafka: ## Xem logs của Kafka
	docker-compose logs -f kafka

logs-clickhouse: ## Xem logs của ClickHouse
	docker-compose logs -f clickhouse

logs-airflow: ## Xem logs của Airflow
	docker-compose logs -f airflow-webserver airflow-scheduler

ps: ## Xem status của các services
	docker-compose ps

clean: ## Xóa tất cả containers và volumes (DANGEROUS!)
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Are you sure? [y/N] " confirm && [ $$confirm = y ] || exit 1
	docker-compose down -v
	@echo "Cleaned up."

# =============================================================================
# SERVICE COMMANDS
# =============================================================================

producer: ## Run Event Producer (generate test events)
	@echo "Building event producer..."
	cd .. && bazel build //com/tm/src/services/event-producer:event-producer
	@echo ""
	@echo "Running event producer (10K events/sec for 60 seconds)..."
	cd .. && bazel run //com/tm/src/services/event-producer:event-producer -- \
		--kafka=localhost:9092 \
		--topic=tracking-events \
		--rate=10000 \
		--duration=60s

producer-burst: ## Run Event Producer ở burst mode (max throughput)
	cd .. && bazel run //com/tm/src/services/event-producer:event-producer -- \
		--kafka=localhost:9092 \
		--mode=burst \
		--duration=30s

ingestor: ## Run Tracking Ingestor (consume from Kafka, insert to ClickHouse)
	@echo "Building tracking ingestor..."
	cd .. && bazel build //com/tm/src/services/tracking-ingestor:tracking-ingestor
	@echo ""
	@echo "Running tracking ingestor..."
	cd .. && bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor -- \
		--kafka=localhost:9092 \
		--clickhouse-host=localhost \
		--batch-size=10000

dbt: ## Run dbt models
	@echo "Running dbt..."
	cd ../src/services/analytics-aggregator/dbt && \
		dbt deps && \
		dbt run --profiles-dir . --project-dir .

dbt-test: ## Run dbt tests
	cd ../src/services/analytics-aggregator/dbt && \
		dbt test --profiles-dir . --project-dir .

dbt-docs: ## Generate dbt documentation
	cd ../src/services/analytics-aggregator/dbt && \
		dbt docs generate --profiles-dir . --project-dir . && \
		dbt docs serve --profiles-dir . --project-dir .

# =============================================================================
# TOOL COMMANDS
# =============================================================================

kafka-topics: ## List Kafka topics
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--list

kafka-create-topic: ## Create tracking-events topic
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create \
		--topic tracking-events \
		--partitions 6 \
		--replication-factor 1

kafka-describe: ## Describe tracking-events topic
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--describe \
		--topic tracking-events

kafka-consume: ## Consume messages từ tracking-events (for debugging)
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic tracking-events \
		--from-beginning \
		--max-messages 5

kafka-ui: ## Open Kafka UI in browser
	@echo "Opening Kafka UI at http://localhost:8080"
	@open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080 2>/dev/null || echo "Open http://localhost:8080 in browser"

clickhouse-cli: ## Open ClickHouse CLI
	docker exec -it clickhouse clickhouse-client

clickhouse-query: ## Run a query on ClickHouse
	@read -p "Enter query: " query && \
	curl "http://localhost:8123/?query=$${query}"

clickhouse-count: ## Count events in ClickHouse
	@echo "Event count in ClickHouse:"
	curl "http://localhost:8123/?query=SELECT%20count()%20FROM%20tracking.events"

clickhouse-recent: ## Show recent events
	curl "http://localhost:8123/?query=SELECT%20*%20FROM%20tracking.events%20LIMIT%205%20FORMAT%20Pretty"

starrocks-cli: ## Open StarRocks CLI (MySQL)
	@echo "Connecting to StarRocks..."
	mysql -h 127.0.0.1 -P 9030 -u root

starrocks-init: ## Initialize StarRocks schema
	mysql -h 127.0.0.1 -P 9030 -u root < starrocks/init.sql

superset-open: ## Open Superset in browser
	@echo "Opening Superset at http://localhost:8088"
	@echo "Default credentials: admin / admin123"
	@open http://localhost:8088 2>/dev/null || xdg-open http://localhost:8088 2>/dev/null || echo "Open http://localhost:8088 in browser"

airflow-open: ## Open Airflow in browser
	@echo "Opening Airflow at http://localhost:8081"
	@echo "Default credentials: admin / admin123"
	@open http://localhost:8081 2>/dev/null || xdg-open http://localhost:8081 2>/dev/null || echo "Open http://localhost:8081 in browser"

# =============================================================================
# BUILD COMMANDS
# =============================================================================

build: ## Build tất cả Go services với Bazel
	@echo "Building all services..."
	cd .. && bazel build //com/tm/src/services/...
	@echo "Build complete!"

test: ## Run tất cả tests
	cd .. && bazel test //...

gazelle: ## Update BUILD files với Gazelle
	cd .. && bazel run //:gazelle

deps-update: ## Update Go dependencies
	cd .. && go mod tidy && bazel run //:gazelle-update-repos && bazel run //:gazelle

# =============================================================================
# DEMO COMMANDS
# =============================================================================

demo: up demo-wait demo-run ## Run full demo (start infra, generate data, view in Superset)

demo-wait: ## Wait for services to be ready
	@echo "Waiting for services to start..."
	@sleep 30
	@echo "Checking Kafka..."
	@until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null; do sleep 5; done
	@echo "Checking ClickHouse..."
	@until curl -s http://localhost:8123/ping 2>/dev/null; do sleep 5; done
	@echo "All services ready!"

demo-run: ## Run demo data generation
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║                    STARTING DEMO                                 ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "1. Starting ingestor in background..."
	cd .. && bazel run //com/tm/src/services/tracking-ingestor:tracking-ingestor -- \
		--kafka=localhost:9092 \
		--clickhouse-host=localhost &
	@sleep 5
	@echo ""
	@echo "2. Generating 1 million events..."
	cd .. && bazel run //com/tm/src/services/event-producer:event-producer -- \
		--kafka=localhost:9092 \
		--total=1000000 \
		--rate=50000
	@echo ""
	@echo "3. Check data in ClickHouse:"
	@make clickhouse-count
	@echo ""
	@echo "Demo complete! Open Superset at http://localhost:8088"

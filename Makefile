# Makefile for RustRabbit - A high-performance RabbitMQ library for Rust

.PHONY: help build test test-unit test-integration test-all clean lint fmt check docs examples docker-up docker-down setup

# Default target
help: ## Show this help message
	@echo "RustRabbit - Development Commands"
	@echo "================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Build
build: ## Build the project
	cargo build

build-release: ## Build the project in release mode
	cargo build --release

# Testing
test: test-unit ## Run unit tests only
	
test-unit: ## Run unit tests
	cargo test --lib

test-integration: ## Run integration tests with RabbitMQ
	./scripts/run-integration-tests.sh

test-integration-keep: ## Run integration tests and keep RabbitMQ running
	./scripts/run-integration-tests.sh --keep-running

test-all: test-unit test-integration ## Run all tests (unit + integration)

# Code quality
check: ## Run cargo check
	cargo check
	cargo check --examples
	cargo check --tests

lint: ## Run clippy lints
	cargo clippy -- -D warnings
	cargo clippy --examples -- -D warnings
	cargo clippy --tests -- -D warnings

fmt: ## Format code with rustfmt
	cargo fmt

fmt-check: ## Check if code is formatted
	cargo fmt -- --check

# Documentation
docs: ## Build documentation
	cargo doc --no-deps --open

docs-private: ## Build documentation including private items
	cargo doc --no-deps --document-private-items --open

# Examples
examples: ## Run all examples
	@echo "Running Publisher Example..."
	cargo run --example publisher_example
	@echo "Running Consumer Example..."
	cargo run --example consumer_example
	@echo "Running Retry Example..."
	cargo run --example retry_example
	@echo "Running Health Monitoring Example..."
	cargo run --example health_monitoring_example
	@echo "Running Builder Pattern Example..."
	cargo run --example builder_pattern_example

example-publisher: ## Run publisher example
	cargo run --example publisher_example

example-consumer: ## Run consumer example
	cargo run --example consumer_example

example-retry: ## Run retry example
	cargo run --example retry_example

example-health: ## Run health monitoring example
	cargo run --example health_monitoring_example

example-builder: ## Run builder pattern example
	cargo run --example builder_pattern_example

# Docker & RabbitMQ
docker-up: ## Start RabbitMQ containers for testing
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for RabbitMQ to be ready..."
	@timeout 60 bash -c 'until docker-compose -f docker-compose.test.yml exec -T rabbitmq rabbitmq-diagnostics ping >/dev/null 2>&1; do sleep 2; done'
	@echo "RabbitMQ is ready!"
	@echo "Management UI: http://localhost:15672 (admin/password)"
	@echo "AMQP Port: localhost:5672"

docker-down: ## Stop RabbitMQ containers
	docker-compose -f docker-compose.test.yml down

docker-logs: ## Show RabbitMQ container logs
	docker-compose -f docker-compose.test.yml logs -f

docker-clean: ## Clean up Docker containers and volumes
	docker-compose -f docker-compose.test.yml down -v
	docker system prune -f

# Setup and installation
setup: ## Setup development environment
	@echo "Setting up RustRabbit development environment..."
	rustup component add clippy rustfmt
	cargo install cargo-watch
	chmod +x scripts/*.sh
	@echo "Development environment setup complete!"

install-deps: ## Install additional development dependencies
	@echo "Installing development dependencies..."
	@if command -v docker >/dev/null 2>&1; then \
		echo "✓ Docker is installed"; \
	else \
		echo "✗ Docker is not installed. Please install Docker first."; \
		exit 1; \
	fi
	@if command -v docker-compose >/dev/null 2>&1; then \
		echo "✓ Docker Compose is installed"; \
	else \
		echo "✗ Docker Compose is not installed. Please install Docker Compose first."; \
		exit 1; \
	fi

# Development workflow
dev: ## Start development workflow with file watching
	cargo watch -x check -x test

dev-integration: docker-up ## Start integration testing workflow
	cargo watch -x "test --test integration_example -- --test-threads=1"

# Performance and benchmarks
bench: ## Run benchmarks
	cargo bench

bench-integration: ## Run integration performance tests
	cargo test --test integration_example test_performance_benchmark -- --nocapture

# Cleaning
clean: ## Clean build artifacts
	cargo clean

clean-all: clean docker-clean ## Clean everything including Docker resources

# Release preparation
pre-release: fmt lint test-all docs ## Run all checks before release
	@echo "Pre-release checks completed successfully!"

version-patch: ## Bump patch version
	cargo set-version --bump patch

version-minor: ## Bump minor version
	cargo set-version --bump minor

version-major: ## Bump major version
	cargo set-version --bump major

# CI/CD simulation
ci: ## Simulate CI pipeline locally
	@echo "Running CI pipeline simulation..."
	make fmt-check
	make lint
	make check
	make test-unit
	make test-integration
	make docs
	@echo "CI pipeline simulation completed successfully!"

# Utility targets
rabbitmq-status: ## Check RabbitMQ status
	@if docker-compose -f docker-compose.test.yml ps | grep -q "Up"; then \
		echo "RabbitMQ containers are running:"; \
		docker-compose -f docker-compose.test.yml ps; \
		echo "Management UI: http://localhost:15672 (admin/password)"; \
	else \
		echo "RabbitMQ containers are not running. Use 'make docker-up' to start them."; \
	fi

stats: ## Show project statistics
	@echo "RustRabbit Project Statistics"
	@echo "============================="
	@echo "Lines of code:"
	@find src -name "*.rs" -exec wc -l {} + | tail -1
	@echo "Test files:"
	@find tests -name "*.rs" | wc -l
	@echo "Example files:"
	@find examples -name "*.rs" | wc -l
	@echo "Dependencies:"
	@cargo tree --depth 1 | grep -v "└─" | grep -v "├─" | wc -l

# Quick development commands
quick-test: ## Quick test (unit tests only)
	cargo test --lib --quiet

quick-check: ## Quick check (no tests)
	cargo check --quiet

rebuild: clean build ## Clean and rebuild

# Default target when no arguments provided
.DEFAULT_GOAL := help
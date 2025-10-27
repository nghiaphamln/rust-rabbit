# RustRabbit

[![Rust](https://github.com/nghiaphamln/rust-rabbit/workflows/CI/badge.svg)](https://github.com/nghiaphamln/rust-rabbit/actions)
[![Crates.io](https://img.shields.io/crates/v/rust-rabbit.svg)](https://crates.io/crates/rust-rabbit)
[![Documentation](https://docs.rs/rust-rabbit/badge.svg)](https://docs.rs/rust-rabbit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, production-ready RabbitMQ client library for Rust with advanced observability, resilience, and performance features. Inspired by MassTransit for .NET, RustRabbit provides enterprise-grade messaging capabilities.

## ✨ Features

### 🚀 **Performance & Scalability**
- **Message Batching**: High-throughput batch publishing for optimal performance
- **Connection Pooling**: Automatic connection management with health monitoring
- **Async/Await**: Full tokio async runtime integration

### 🔍 **Observability & Monitoring**
- **Prometheus Metrics**: Comprehensive metrics for throughput, latency, and errors
- **Health Checks**: Real-time connection health monitoring with detailed status
- **Structured Logging**: Integrated tracing with correlation IDs

### 🛡️ **Resilience & Reliability**
- **Circuit Breaker**: Automatic failure detection and recovery for connections
- **Advanced Retry**: Built-in exponential backoff with jitter
- **Graceful Shutdown**: Multi-phase shutdown with signal handling

### ⚙️ **Developer Experience**
- **Builder Pattern**: Fluent, type-safe configuration API
- **Type Safety**: Strongly typed message handling with serde integration
- **Comprehensive Testing**: Extensive test coverage with integration tests

## 🚀 Quick Start

Add RustRabbit to your `Cargo.toml`:

```toml
[dependencies]
rust-rabbit = "0.2.0"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### Basic Usage

```rust
use rust_rabbit::{RustRabbit, RabbitConfig};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderMessage {
    order_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .virtual_host("my-app")
        .retry(|retry| retry.max_retries(5).aggressive())
        .pool(|pool| pool.high_throughput())
        .build();
    
    // Create RustRabbit instance
    let rabbit = RustRabbit::new(config).await?;
    
    // Publish message
    let publisher = rabbit.publisher();
    let order = OrderMessage {
        order_id: "ORD-12345".to_string(),
        amount: 99.99,
    };
    
    publisher.publish_to_queue("orders", &order, None).await?;
    
    Ok(())
}
```

## 🔍 Advanced Features

### Prometheus Metrics

Enable comprehensive metrics collection:

```rust
use rust_rabbit::{RustRabbit, RabbitConfig, metrics::RustRabbitMetrics};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable metrics
    let metrics = RustRabbitMetrics::new()?;
    let config = RabbitConfig::default();
    
    // Create RustRabbit with metrics
    let rabbit = RustRabbit::with_metrics(config, metrics.clone()).await?;
    
    // Expose metrics via HTTP endpoint (using warp)
    // See examples/metrics_example.rs for complete implementation
    
    Ok(())
}
```

### Message Batching

For high-throughput scenarios:

```rust
use rust_rabbit::{RustRabbit, batching::BatchConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rabbit = RustRabbit::new(RabbitConfig::default()).await?;
    
    // Create batcher with custom config
    let batch_config = BatchConfig::builder()
        .max_batch_size(100)
        .flush_interval(std::time::Duration::from_millis(500))
        .build();
    
    let batcher = rabbit.create_batcher(batch_config).await?;
    
    // Batch publish messages
    for i in 0..1000 {
        let message = OrderMessage {
            order_id: format!("ORD-{}", i),
            amount: 100.0 + i as f64,
        };
        batcher.queue_message("orders", &message).await?;
    }
    
    // Flush remaining messages
    batcher.flush().await?;
    
    Ok(())
}
```

### Graceful Shutdown

Handle shutdown signals properly:

```rust
use rust_rabbit::{RustRabbit, shutdown::{ShutdownConfig, setup_signal_handling}};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rabbit = RustRabbit::new(RabbitConfig::default()).await?;
    
    // Enable graceful shutdown
    let shutdown_config = ShutdownConfig::builder()
        .pending_timeout(std::time::Duration::from_secs(30))
        .phase_delay(std::time::Duration::from_secs(1))
        .build();
    
    let shutdown_manager = rabbit.enable_shutdown_handling(shutdown_config);
    
    // Setup signal handling
    setup_signal_handling(shutdown_manager.clone()).await?;
    
    // Your application logic here
    // ...
    
    Ok(())
}
```

### Circuit Breaker

Automatic connection resilience:

```rust
use rust_rabbit::{RustRabbit, circuit_breaker::CircuitBreakerConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 5,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 3,
            half_open_max_requests: 5,
        })
        .build();
    
    let rabbit = RustRabbit::new(config).await?;
    // Circuit breaker automatically handles connection failures
    
    Ok(())
}
```

## 📊 Performance Benchmarks

RustRabbit v0.2.0 Performance Results:

| Metric | Value |
|--------|--------|
| **Throughput** | 50,000+ msgs/sec |
| **Latency (P99)** | < 10ms |
| **Memory Usage** | < 50MB baseline |
| **Connection Pool** | 10-100 connections |

*Benchmarks run on: Intel i7-10700K, 32GB RAM, RabbitMQ 3.12*

## 🏗️ Architecture

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Application       │    │   RustRabbit        │    │   RabbitMQ          │
│                     │    │                     │    │                     │
│  ┌─────────────┐    │    │  ┌─────────────┐    │    │  ┌─────────────┐    │
│  │ Publishers  │◄───┼────┼──► Message       │    │    │  │ Exchanges   │    │
│  └─────────────┘    │    │  │ Batching     │◄───┼────┼──► & Queues    │    │
│                     │    │  └─────────────┘    │    │  └─────────────┘    │
│  ┌─────────────┐    │    │                     │    │                     │
│  │ Consumers   │◄───┼────┼──► Circuit        │    │    │  ┌─────────────┐    │
│  └─────────────┘    │    │  │ Breaker       │◄───┼────┼──► Health       │    │
│                     │    │  └─────────────┘    │    │  │ Monitoring    │    │
│  ┌─────────────┐    │    │                     │    │  └─────────────┘    │
│  │ Metrics     │◄───┼────┼──► Prometheus      │    │    │                     │
│  │ Dashboard   │    │    │  │ Metrics        │    │    │                     │
│  └─────────────┘    │    │  └─────────────┘    │    │                     │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .build();
        
    let rabbit = RustRabbit::new(config).await?;
    
    // Use builder for consumer options
    let consumer_options = ConsumerOptions::builder("orders")
        .consumer_tag("order-processor")
        .concurrency(5)
        .auto_declare_queue()
        .reliable()
        .build();
    
    let consumer = rabbit.consumer(consumer_options).await?;
    let handler = Arc::new(OrderHandler);
    
## 🛠️ Configuration

RustRabbit offers extensive configuration options:

```rust
use rust_rabbit::{RabbitConfig, PoolConfig, RetryConfig, HealthCheckConfig};
use std::time::Duration;

let config = RabbitConfig::builder()
    .connection_string("amqp://user:pass@localhost:5672")
    .virtual_host("/production")
    .pool(|pool| pool
        .max_connections(20)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_mins(10))
    )
    .retry(|retry| retry
        .max_retries(5)
        .initial_delay(Duration::from_millis(100))
        .max_delay(Duration::from_secs(30))
        .exponential_backoff()
        .jitter(0.1)
    )
    .health_check(|health| health
        .interval(Duration::from_secs(30))
        .timeout(Duration::from_secs(5))
        .failure_threshold(3)
    )
    .build();
```

## 📈 Metrics & Monitoring

### Available Metrics

RustRabbit exposes comprehensive Prometheus metrics:

**Message Metrics:**
- `rustrabbit_messages_published_total` - Total messages published
- `rustrabbit_messages_consumed_total` - Total messages consumed  
- `rustrabbit_messages_failed_total` - Total failed messages
- `rustrabbit_message_processing_duration_seconds` - Message processing latency

**Connection Metrics:**
- `rustrabbit_connections_total` - Total connections in pool
- `rustrabbit_connections_healthy` - Healthy connections
- `rustrabbit_connection_failures_total` - Connection failures

**Queue Metrics:**
- `rustrabbit_queue_depth` - Messages waiting in queue
- `rustrabbit_consumer_count` - Active consumers per queue

### Grafana Dashboard

Import our pre-built Grafana dashboard for instant visibility:

```bash
# Download dashboard JSON
curl -o rustrabbit-dashboard.json https://raw.githubusercontent.com/nghiaphamln/rust-rabbit/main/grafana/dashboard.json

# Import into Grafana via UI or API
```

## 🚀 Production Deployment

### Docker Compose Example

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.12-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: secret
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin

volumes:
  rabbitmq_data:
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-rustrabbit-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-rustrabbit-app
  template:
    metadata:
      labels:
        app: my-rustrabbit-app
    spec:
      containers:
      - name: app
        image: my-rustrabbit-app:latest
        ports:
        - containerPort: 8080
        env:
        - name: RABBITMQ_URL
          value: "amqp://rabbitmq:5672"
        - name: RUST_LOG
          value: "info"
```

## 🔧 Development

### Prerequisites

- Rust 1.70+ 
- RabbitMQ 3.9+
- Docker (for integration tests)

### Building

```bash
git clone https://github.com/nghiaphamln/rust-rabbit.git
cd rust-rabbit
cargo build
```

### Testing

```bash
# Unit tests
cargo test --lib

# Integration tests (requires RabbitMQ)
docker-compose up -d rabbitmq
cargo test --tests

# Run with coverage
cargo tarpaulin --out Html --output-dir target/coverage
```

### Examples

Run the examples to see RustRabbit in action:

```bash
# Basic publisher/consumer
cargo run --example consumer_example
cargo run --example publisher_example

# Advanced features
cargo run --example metrics_example
cargo run --example health_monitoring_example
cargo run --example builder_pattern_example
```

## 🗺️ Roadmap

### Phase 1 (v0.2.0) ✅ **COMPLETED**
- [x] Prometheus metrics integration
- [x] Circuit breaker pattern
- [x] Message batching
- [x] Graceful shutdown handling

### Phase 2 (v0.3.0) - Advanced Messaging Patterns
- [ ] Request-Response pattern with correlation IDs
- [ ] Saga pattern for distributed transactions  
- [ ] Event sourcing support
- [ ] Message deduplication
- [ ] Priority queues

### Phase 3 (v0.4.0) - Enterprise Features
- [ ] Multi-broker support with failover
- [ ] Message encryption at rest
- [ ] Schema registry integration
- [ ] Advanced routing patterns
- [ ] Performance optimization

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Areas for Contribution

- 🐛 Bug fixes and improvements
- 📚 Documentation enhancements
- ✨ New features from roadmap
- 🧪 Additional test coverage
- 📊 Performance optimizations

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [MassTransit](https://masstransit-project.com/) for .NET
- Built on [lapin](https://github.com/amqp-rs/lapin) for RabbitMQ protocol
- Metrics powered by [Prometheus](https://prometheus.io/)

## 🆘 Support

- 📖 [Documentation](https://docs.rs/rust-rabbit)
- 💬 [GitHub Discussions](https://github.com/nghiaphamln/rust-rabbit/discussions)
- 🐛 [Issue Tracker](https://github.com/nghiaphamln/rust-rabbit/issues)
- 📧 Email: nghiaphamln3@gmail.com

---

<div align="center">

**⭐ Star us on GitHub if RustRabbit helps your project! ⭐**

[GitHub](https://github.com/nghiaphamln/rust-rabbit) • [Crates.io](https://crates.io/crates/rust-rabbit) • [Docs.rs](https://docs.rs/rust-rabbit)

</div>

### Health Monitoring

Monitor your RabbitMQ connections with built-in health checking:

```rust
use rust_rabbit::{
    health::{HealthCheckConfigExt, HealthChecker},
    config::HealthCheckConfig,
};

// Configure health checking
let mut config = RabbitConfig::default();
config.health_check = HealthCheckConfig::aggressive();

let rabbit = RustRabbit::new(config).await?;
let health_checker = rabbit.health_checker();

// Start monitoring
health_checker.start_monitoring().await?;

// Check health status
let is_healthy = health_checker.is_healthy().await;
let summary = health_checker.get_health_summary().await;

// Wait for healthy connection
health_checker.wait_for_healthy(Some(Duration::from_secs(30))).await?;
```

### Connection Pooling

RustRabbit automatically manages connection pools with builder pattern:

```rust
use rust_rabbit::RabbitConfig;

let config = RabbitConfig::builder()
    .connection_string("amqp://localhost:5672")
    .retry(|retry| {
        retry
            .max_retries(3)
            .initial_delay(Duration::from_millis(1000))
            .max_delay(Duration::from_secs(60))
            .backoff_multiplier(2.0)
            .jitter(0.1)
    })
    .pool(|pool| {
        pool
            .max_connections(20)
            .min_connections(2)
            .idle_timeout(Duration::from_secs(300))
    })
    .build();
```

### Message Options

Customize message publishing with builder pattern:

```rust
use rust_rabbit::PublishOptions;

let options = PublishOptions::builder()
    .durable()
    .message_id("MSG-12345")
    .correlation_id("CORR-67890")
    .ttl(Duration::from_secs(300))
    .priority(5)
    .header_string("source", "order-service")
    .header_int("version", 1)
    .auto_declare_exchange()
    .development()
    .build();

publisher.publish_to_exchange(
    "my-exchange",
    "routing.key",
    &message,
    Some(options)
).await?;
```

## Configuration with Builder Pattern

### Environment-Specific Configurations

```rust
// Development configuration
let dev_config = RabbitConfig::builder()
    .connection_string("amqp://localhost:5672")
    .retry(|retry| retry.conservative())
    .health(|health| health.infrequent())
    .pool(|pool| pool.single_connection())
    .build();

// Production configuration
let prod_config = RabbitConfig::builder()
    .connection_string("amqp://prod-server:5672")
    .connection_timeout(Duration::from_secs(30))
    .retry(|retry| retry.aggressive())
    .health(|health| health.frequent())
    .pool(|pool| pool.high_throughput())
    .build();
```

### Consumer Configuration

```rust
// High throughput consumer
let high_throughput_options = ConsumerOptions::builder("orders")
    .consumer_tag("bulk-processor")
    .high_throughput()
    .auto_declare_queue()
    .dead_letter_exchange("failed-orders")
    .build();

// Reliable consumer
let reliable_options = ConsumerOptions::builder("critical-orders")
    .consumer_tag("critical-processor")
    .reliable()
    .manual_ack()
    .prefetch_count(1)
    .build();
```

## Configuration

### RabbitConfig

The main configuration struct with builder pattern:

```rust
use rust_rabbit::RabbitConfig;

let config = RabbitConfig::builder()
    .connection_string("amqp://user:pass@localhost:5672")
    .virtual_host("my-vhost")
    .connection_timeout(Duration::from_secs(30))
    .heartbeat(Duration::from_secs(60))
    .retry(|retry| {
        retry
            .max_retries(3)
            .initial_delay(Duration::from_millis(1000))
            .max_delay(Duration::from_secs(60))
            .backoff_multiplier(2.0)
            .jitter(0.1)
    })
    .health(|health| {
        health
            .check_interval(Duration::from_secs(30))
            .check_timeout(Duration::from_secs(5))
            .enabled()
    })
    .pool(|pool| {
        pool
            .max_connections(10)
            .min_connections(1)
            .idle_timeout(Duration::from_secs(300))
    })
    .build();
```

### Consumer Options

Configure consumer behavior with builder:

```rust
use rust_rabbit::ConsumerOptions;

let consumer_options = ConsumerOptions::builder("my-queue")
    .consumer_tag("my-consumer")
    .concurrency(10)
    .prefetch_count(20)
    .auto_declare_queue()
    .dead_letter_exchange("failed-messages")
    .manual_ack()
    .build();
```

## Error Handling

RustRabbit provides comprehensive error handling:

```rust
use rust_rabbit::{RabbitError, Result};

match publisher.publish_to_queue("orders", &message, None).await {
    Ok(_) => println!("Message published successfully"),
    Err(RabbitError::Connection(e)) => eprintln!("Connection error: {}", e),
    Err(RabbitError::Serialization(e)) => eprintln!("Serialization error: {}", e),
    Err(RabbitError::RetryExhausted(msg)) => eprintln!("Retry exhausted: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Testing

RustRabbit includes comprehensive test suites to ensure reliability and performance.

### Unit Tests

Run unit tests with:

```bash
cargo test --lib
# or
make test-unit
```

### Integration Tests with Real RabbitMQ

RustRabbit supports integration testing with real RabbitMQ instances using Docker:

```bash
# Start RabbitMQ and run integration tests
make test-integration

# Or manually:
docker-compose -f docker-compose.test.yml up -d
cargo test --test integration_example -- --test-threads=1
docker-compose -f docker-compose.test.yml down
```

### Quick Development Setup

```bash
# Setup development environment
make setup

# Start RabbitMQ for development
make docker-up

# Run tests with file watching
make dev

# Run integration tests with file watching
make dev-integration
```

### Test Coverage

The integration tests cover:
- ✅ End-to-end message publishing and consumption
- ✅ Retry mechanisms with delayed message exchange
- ✅ Health monitoring and connection management
- ✅ Performance benchmarking
- ✅ Error handling and recovery scenarios
- ✅ Builder pattern configuration
- ✅ Concurrent message processing

See [INTEGRATION_TESTING.md](INTEGRATION_TESTING.md) for detailed testing documentation.

## Requirements

- **RabbitMQ**: Version 3.8 or higher
- **Rust**: Version 1.70 or higher
- **Optional**: RabbitMQ delayed message exchange plugin for advanced retry features

### Installing RabbitMQ Delayed Message Exchange Plugin

For advanced retry functionality, install the delayed message exchange plugin:

```bash
# Download and enable the plugin
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

## Examples

Check out the `examples/` directory for more comprehensive examples:

- `builder_pattern_example.rs` - Comprehensive builder pattern usage
- `publisher_example.rs` - Various publishing patterns
- `consumer_example.rs` - Consumer with retry and error handling
- `retry_example.rs` - Advanced retry mechanisms
- `health_monitoring_example.rs` - Health monitoring and connection management

Run examples with:

```bash
cargo run --example builder_pattern_example
cargo run --example publisher_example
cargo run --example consumer_example
cargo run --example retry_example
cargo run --example health_monitoring_example
```

## Performance

RustRabbit is designed for high performance:

- **Connection Pooling**: Efficient connection reuse
- **Async/Await**: Non-blocking I/O operations
- **Concurrent Processing**: Configurable message processing concurrency
- **Memory Efficient**: Minimal allocations and zero-copy where possible

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [MassTransit](https://masstransit-project.com/) for .NET
- Built on top of the excellent [lapin](https://github.com/CleverCloud/lapin) RabbitMQ client
- Thanks to the Rust async ecosystem (tokio, futures, etc.)
# RustRabbit

A high-performance, feature-rich RabbitMQ client library for Rust, inspired by MassTransit for .NET. RustRabbit provides a comprehensive messaging solution with publisher/consumer patterns, advanced retry mechanisms, health monitoring, and extensive configuration options.

## Features

- **🚀 High Performance**: Async/await support with connection pooling
- **🔄 Advanced Retry Mechanism**: Built-in support for RabbitMQ delayed message exchange plugin
- **💪 Robust Error Handling**: Comprehensive error types and recovery strategies
- **🏥 Health Monitoring**: Real-time connection health checks and monitoring
- **⚙️ Flexible Configuration**: Extensive configuration options for all components
- **🔗 Connection Management**: Automatic connection pooling with failover support
- **📊 Message Patterns**: Support for various messaging patterns (publish/subscribe, request/response, etc.)
- **🎯 Type Safety**: Strongly typed message handling with serde integration

## Quick Start

Add RustRabbit to your `Cargo.toml`:

```toml
[dependencies]
rust-rabbit = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### Basic Publisher Example

```rust
use rust_rabbit::{RustRabbit, RabbitConfig, PublishOptions};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderMessage {
    order_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = RabbitConfig::default();
    
    // Create RustRabbit instance
    let rabbit = RustRabbit::new(config).await?;
    let publisher = rabbit.publisher();
    
    // Create and publish message
    let order = OrderMessage {
        order_id: "ORD-12345".to_string(),
        amount: 99.99,
    };
    
    publisher.publish_to_queue("orders", &order, None).await?;
    
    Ok(())
}
```

### Basic Consumer Example

```rust
use rust_rabbit::{
    RustRabbit, RabbitConfig, ConsumerOptions, 
    MessageHandler, MessageContext, MessageResult
};
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
struct OrderMessage {
    order_id: String,
    amount: f64,
}

struct OrderHandler;

#[async_trait]
impl MessageHandler<OrderMessage> for OrderHandler {
    async fn handle(&self, message: OrderMessage, _context: MessageContext) -> MessageResult {
        println!("Processing order: {:?}", message);
        // Process the message here
        MessageResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RabbitConfig::default();
    let rabbit = RustRabbit::new(config).await?;
    
    let consumer_options = ConsumerOptions {
        queue_name: "orders".to_string(),
        concurrency: 5,
        auto_declare_queue: true,
        ..Default::default()
    };
    
    let consumer = rabbit.consumer(consumer_options).await?;
    let handler = Arc::new(OrderHandler);
    
    consumer.consume::<OrderMessage, OrderHandler>(handler).await?;
    
    Ok(())
}
```

## Advanced Features

### Retry Mechanism with Delayed Message Exchange

RustRabbit supports advanced retry mechanisms using the RabbitMQ delayed message exchange plugin:

```rust
use rust_rabbit::{
    retry::{RetryPolicy, DelayedMessageExchange},
    connection::ConnectionManager,
};
use std::time::Duration;

// Configure retry policy
let retry_policy = RetryPolicy {
    max_retries: 5,
    initial_delay: Duration::from_millis(1000),
    max_delay: Duration::from_secs(60),
    backoff_multiplier: 2.0,
    jitter: 0.1,
    dead_letter_exchange: Some("failed-messages".to_string()),
    ..Default::default()
};

// Setup delayed message exchange
let connection_manager = ConnectionManager::new(config).await?;
let delayed_exchange = DelayedMessageExchange::new(
    connection_manager,
    "retry-exchange".to_string(),
    retry_policy,
);

// Setup infrastructure
delayed_exchange.setup().await?;
delayed_exchange.setup_retry_queues("my-queue").await?;

// Publish with retry
delayed_exchange.publish_with_retry(
    "my-queue",
    &message,
    retry_count,
    Some(original_headers),
).await?;
```

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

RustRabbit automatically manages connection pools:

```rust
use rust_rabbit::config::{RabbitConfig, PoolConfig, RetryConfig};

let config = RabbitConfig {
    connection_string: "amqp://localhost:5672".to_string(),
    pool_config: PoolConfig {
        max_connections: 20,
        min_connections: 2,
        idle_timeout: Duration::from_secs(300),
    },
    retry_config: RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_secs(60),
        backoff_multiplier: 2.0,
        jitter: 0.1,
    },
    ..Default::default()
};
```

### Message Options

Customize message publishing with various options:

```rust
use rust_rabbit::{PublishOptions, publisher::ExchangeDeclareOptions};
use std::collections::HashMap;

let mut options = PublishOptions {
    persistent: true,
    message_id: Some("MSG-12345".to_string()),
    correlation_id: Some("CORR-67890".to_string()),
    ttl: Some(Duration::from_secs(300)),
    priority: Some(5),
    auto_declare_exchange: true,
    exchange_options: ExchangeDeclareOptions {
        durable: true,
        exchange_type: lapin::ExchangeKind::Topic,
        ..Default::default()
    },
    ..Default::default()
};

// Add custom headers
options.headers.insert(
    "custom-header".to_string(),
    lapin::types::AMQPValue::LongString("custom-value".into())
);

publisher.publish_to_exchange(
    "my-exchange",
    "routing.key",
    &message,
    Some(options)
).await?;
```

## Configuration

### RabbitConfig

The main configuration struct for RustRabbit:

```rust
use rust_rabbit::config::{RabbitConfig, RetryConfig, HealthCheckConfig, PoolConfig};

let config = RabbitConfig {
    connection_string: "amqp://user:pass@localhost:5672".to_string(),
    virtual_host: Some("/my-vhost".to_string()),
    connection_timeout: Some(Duration::from_secs(30)),
    heartbeat: Some(Duration::from_secs(60)),
    retry_config: RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_secs(60),
        backoff_multiplier: 2.0,
        jitter: 0.1,
    },
    health_check: HealthCheckConfig {
        check_interval: Duration::from_secs(30),
        check_timeout: Duration::from_secs(5),
        enabled: true,
    },
    pool_config: PoolConfig {
        max_connections: 10,
        min_connections: 1,
        idle_timeout: Duration::from_secs(300),
    },
};
```

### Consumer Options

Configure consumer behavior:

```rust
use rust_rabbit::{ConsumerOptions, retry::RetryPolicy};

let consumer_options = ConsumerOptions {
    queue_name: "my-queue".to_string(),
    consumer_tag: Some("my-consumer".to_string()),
    concurrency: 10,
    prefetch_count: Some(20),
    auto_declare_queue: true,
    retry_policy: Some(RetryPolicy::default()),
    dead_letter_exchange: Some("failed-messages".to_string()),
    auto_ack: false,
    exclusive: false,
    ..Default::default()
};
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

- `publisher_example.rs` - Various publishing patterns
- `consumer_example.rs` - Consumer with retry and error handling
- `retry_example.rs` - Advanced retry mechanisms
- `health_monitoring_example.rs` - Health monitoring and connection management

Run examples with:

```bash
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
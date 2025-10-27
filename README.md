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
use rust_rabbit::{RustRabbit, RabbitConfig};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderMessage {
    order_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration using builder pattern
    let config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .virtual_host("my-app")
        .retry(|retry| retry.max_retries(5).aggressive())
        .pool(|pool| pool.high_throughput())
        .build();
    
    // Create RustRabbit instance
    let rabbit = RustRabbit::new(config).await?;
    let publisher = rabbit.publisher();
    
    // Create and publish message
    let order = OrderMessage {
        order_id: "ORD-12345".to_string(),
        amount: 99.99,
    };
    
    // Use builder for publish options
    let options = rust_rabbit::PublishOptions::builder()
        .durable()
        .auto_declare_queue()
        .header_string("source", "order-service")
        .build();
    
    publisher.publish_to_queue("orders", &order, Some(options)).await?;
    
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
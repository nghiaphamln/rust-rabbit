# rust-rabbit

[![Rust](https://github.com/nghiaphamln/rust-rabbit/workflows/CI/badge.svg)](https://github.com/nghiaphamln/rust-rabbit/actions)
[![Crates.io](https://img.shields.io/crates/v/rust-rabbit.svg)](https://crates.io/crates/rust-rabbit)
[![Documentation](https://docs.rs/rust-rabbit/badge.svg)](https://docs.rs/rust-rabbit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple and reliable RabbitMQ client library for Rust. Easy to use with minimal configuration and flexible retry mechanisms.

## Key Features

- Simple API with just Publisher and Consumer
- Flexible retry mechanisms: exponential, linear, or custom delays
- Automatic queue and exchange declaration
- Built-in reliability with intelligent error handling
- MassTransit integration for C# interoperability
- Production-ready with persistent messages and proper ACK handling

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rust-rabbit = "1.2"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### Basic Publisher Example

```rust
use rust_rabbit::{Connection, Publisher};
use serde::Serialize;

#[derive(Serialize)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    let publisher = Publisher::new(connection);
    
    let order = Order { id: 123, amount: 99.99 };
    
    // Publish to exchange
    publisher.publish_to_exchange("orders", "new.order", &order, None).await?;
    
    // Publish to queue
    publisher.publish_to_queue("order_queue", &order, None).await?;
    
    Ok(())
}
```

### Basic Consumer Example

```rust
use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    
    let consumer = Consumer::builder(connection, "order_queue")
        .with_retry(RetryConfig::exponential_default())
        .bind_to_exchange("orders", "new.order")
        .with_prefetch(5)
        .build();
    
    consumer.consume(|msg: Order| async move {
        println!("Processing order {}: ${}", msg.id, msg.amount);
        
        if msg.amount > 1000.0 {
            return Err("Amount too high".into());
        }
        
        Ok(())
    }).await?;
    
    Ok(())
}
```

## Documentation

### User Guides

Comprehensive guides for common use cases and patterns:

| Guide | Description |
|-------|-------------|
| [Retry Configuration Guide](docs/retry-guide.md) | Learn about retry mechanisms, delay strategies, and DLQ configuration |
| [Queues and Exchanges Guide](docs/queues-exchanges.md) | Understanding queue binding, exchange types, and routing patterns |
| [Error Handling Guide](docs/error-handling.md) | Error types, classification, and recovery strategies |
| [Best Practices Guide](docs/best-practices.md) | Production patterns, performance optimization, and operational tips |

### API Reference

Full API documentation is available at [docs.rs/rust-rabbit](https://docs.rs/rust-rabbit).

### Examples

See the [examples/](examples/) directory for complete working examples:

- [basic_publisher.rs](examples/basic_publisher.rs) - Simple message publishing
- [basic_consumer.rs](examples/basic_consumer.rs) - Simple message consumption
- [retry_examples.rs](examples/retry_examples.rs) - Different retry configurations
- [delayed_exchange_example.rs](examples/delayed_exchange_example.rs) - Using delayed exchange plugin
- [dlq_ttl_example.rs](examples/dlq_ttl_example.rs) - Auto-cleanup DLQ with TTL
- [masstransit_option_example.rs](examples/masstransit_option_example.rs) - MassTransit integration
- [production_setup.rs](examples/production_setup.rs) - Production-ready configuration

## Core Concepts

### Retry Mechanisms

rust-rabbit provides flexible retry mechanisms for handling message processing failures:

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

// Exponential backoff: 1s, 2s, 4s, 8s, 16s
let exponential = RetryConfig::exponential_default();

// Custom exponential with base and max delay
let custom_exp = RetryConfig::exponential(
    5, 
    Duration::from_secs(2), 
    Duration::from_secs(60)
);

// Linear retry: same delay for each attempt
let linear = RetryConfig::linear(3, Duration::from_secs(10));

// Custom delays for each retry
let custom = RetryConfig::custom(vec![
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
]);

// No retries
let no_retry = RetryConfig::no_retry();
```

See the [Retry Configuration Guide](docs/retry-guide.md) for detailed information.

### Delay Strategies

Two strategies for implementing message delays:

**TTL Strategy (Default)**
- Uses RabbitMQ's TTL feature
- No plugin required
- Works out-of-the-box

**DelayedExchange Strategy**
- Uses rabbitmq_delayed_message_exchange plugin
- More precise timing
- Better for high-volume scenarios
- Requires plugin installation

```rust
use rust_rabbit::{RetryConfig, DelayStrategy};

// TTL strategy (default)
let config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::TTL);

// Delayed exchange strategy (requires plugin)
let config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);
```

See the [Retry Configuration Guide](docs/retry-guide.md) for setup instructions.

### Dead Letter Queue

Failed messages that exceed max retries are automatically sent to a Dead Letter Queue. You can configure automatic cleanup:

```rust
let retry_config = RetryConfig::exponential_default()
    .with_dlq_ttl(Duration::from_secs(86400)); // Auto-cleanup after 1 day

let consumer = Consumer::builder(connection, "orders")
    .with_retry(retry_config)
    .build();
```

### MassTransit Integration

rust-rabbit seamlessly integrates with C# services using MassTransit.

**Publishing to MassTransit services:**

```rust
use rust_rabbit::PublishOptions;

publisher.publish_to_exchange(
    "order-exchange",
    "order.created",
    &order,
    Some(PublishOptions::new().with_masstransit("Contracts:OrderCreated"))
).await?;
```

**Consuming MassTransit messages:**

Messages published by MassTransit are automatically detected and unwrapped. Your handler receives just the payload:

```rust
consumer.consume(|msg: OrderMessage| async move {
    println!("Order ID: {}", msg.order_id);
    Ok(())
}).await?;
```

**Access envelope metadata:**

Use `consume_envelopes()` to access correlation IDs, timestamps, and other metadata:

```rust
use rust_rabbit::MessageEnvelope;

consumer.consume_envelopes(|envelope: MessageEnvelope<OrderMessage>| async move {
    println!("Correlation ID: {:?}", envelope.metadata.correlation_id);
    println!("Timestamp: {:?}", envelope.metadata.timestamp);
    
    let order = envelope.payload;
    process_order(&order).await?;
    Ok(())
}).await?;
```

### Publisher Options

```rust
use rust_rabbit::PublishOptions;

let options = PublishOptions::new()
    .mandatory()
    .priority(5);

publisher.publish_to_queue("orders", &message, Some(options)).await?;
```

### Consumer Configuration

```rust
let consumer = Consumer::builder(connection, "order_queue")
    .with_retry(RetryConfig::exponential_default())
    .bind_to_exchange("order_exchange", "new.order")
    .with_prefetch(10)
    .build();
```

## Requirements

- Rust 1.70 or higher
- RabbitMQ 3.8 or higher
- Tokio async runtime

## Testing

Run the tests:

```bash
cargo test
```

For integration tests with real RabbitMQ:

```bash
# Start RabbitMQ with Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Run tests
cargo test
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome. Please read our contributing guide and submit pull requests.

## Support

- Issues: [GitHub Issues](https://github.com/nghiaphamln/rust-rabbit/issues)
- Documentation: [docs.rs](https://docs.rs/rust-rabbit)

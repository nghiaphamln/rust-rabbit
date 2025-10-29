# rust-rabbit 🐰

[![Rust](https://github.com/nghiaphamln/rust-rabbit/workflows/CI/badge.svg)](https://github.com/nghiaphamln/rust-rabbit/actions)
[![Crates.io](https://img.shields.io/crates/v/rust-rabbit.svg)](https://crates.io/crates/rust-rabbit)
[![Documentation](https://docs.rs/rust-rabbit/badge.svg)](https://docs.rs/rust-rabbit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A **simple, reliable** RabbitMQ client library for Rust. Easy to use with minimal configuration and flexible retry mechanisms.

## 🚀 **Key Features**

- **Simple API**: Just `Publisher` and `Consumer` with essential methods
- **Flexible Retry**: Exponential, linear, or custom retry mechanisms  
- **Auto-Setup**: Automatic queue/exchange declaration and binding
- **Built-in Reliability**: Default ACK behavior with intelligent error handling
- **Zero Complexity**: No enterprise patterns, no metrics - just core messaging

## 📦 **Quick Start**

Add to your `Cargo.toml`:

```toml
[dependencies]
rust-rabbit = "1.0"
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

## 🎯 **Basic Usage**

### Publisher - Send Messages

```rust
use rust_rabbit::{Connection, Publisher, PublishOptions};
use serde::Serialize;

#[derive(Serialize)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to RabbitMQ
    let connection = Connection::new("amqp://localhost:5672").await?;
    let publisher = Publisher::new(connection);
    
    let order = Order { id: 123, amount: 99.99 };
    
    // Publish to exchange (with routing)
    publisher.publish_to_exchange("orders", "new.order", &order, None).await?;
    
    // Publish directly to queue (simple)
    publisher.publish_to_queue("order_queue", &order, None).await?;
    
    Ok(())
}
```

### Consumer - Receive Messages with Retry

```rust
use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    
    let consumer = Consumer::builder(connection, "order_queue")
        .with_retry(RetryConfig::exponential_default()) // 1s->2s->4s->8s->16s
        .bind_to_exchange("orders", "order.*")
        .concurrency(5)
        .build();
    
    consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
        println!("Processing order {}: ${}", msg.data.id, msg.data.amount);
        
        // Your business logic here
        if msg.data.amount > 1000.0 {
            return Err("Amount too high".into()); // Will retry
        }
        
        Ok(()) // ACK message
    }).await?;
    
    Ok(())
}
```

## 🔄 **Retry Configurations**

### Built-in Retry Patterns

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

// Exponential: 1s → 2s → 4s → 8s → 16s (5 retries)
let exponential = RetryConfig::exponential_default();

// Custom exponential: 2s → 4s → 8s → 16s → 32s (max 60s)
let custom_exp = RetryConfig::exponential(5, Duration::from_secs(2), Duration::from_secs(60));

// Linear: 10s → 10s → 10s (3 retries)  
let linear = RetryConfig::linear(3, Duration::from_secs(10));

// Custom delays: 1s → 5s → 30s
let custom = RetryConfig::custom(vec![
    Duration::from_secs(1),
    Duration::from_secs(5), 
    Duration::from_secs(30),
]);

// No retries - fail immediately
let no_retry = RetryConfig::no_retry();
```

### How Retry Works

```rust
// Failed messages are automatically:
// 1. Sent to retry queue with delay (e.g., orders.retry.1)
// 2. After delay, returned to original queue for retry
// 3. If max retries exceeded, sent to dead letter queue (e.g., orders.dlq)
```

### Delay Strategies (TTL vs DelayedExchange)

rust-rabbit supports two strategies for implementing message delays:

#### 1. **TTL Strategy (Default)** - No Plugin Required

Uses RabbitMQ's TTL feature with temporary retry queues. Simple but less precise.

```rust
use rust_rabbit::{RetryConfig, DelayStrategy, Consumer, Connection};
use std::time::Duration;

let retry_config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::TTL); // Default, no plugin needed

// Messages failed are sent to: orders.retry.1 (with TTL set)
// After TTL expires, automatically routed back to: orders
```

**Pros:**
- No external plugin required
- Works out-of-the-box with standard RabbitMQ
- Simple setup

**Cons:**
- Less precise timing (TTL granularity depends on RabbitMQ configuration)
- Creates many temporary queues (one per retry attempt)
- Requires cleanup of old retry queues

#### 2. **DelayedExchange Strategy** - Better Precision

Uses the `rabbitmq_delayed_message_exchange` plugin for server-side delay management. More reliable and cleaner architecture.

```rust
use rust_rabbit::{RetryConfig, DelayStrategy, Consumer, Connection};
use std::time::Duration;

let retry_config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);

// Messages are published to: orders.delay (delay exchange)
// With x-delay header set to desired delay time
// Exchange automatically routes back to: orders after delay
```

**Setup Requirements:**

1. Install the plugin on RabbitMQ:
   ```bash
   # Download from: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
   # Place .ez file in RabbitMQ plugins directory
   
   # Enable plugin
   rabbitmq-plugins enable rabbitmq_delayed_message_exchange
   
   # Restart RabbitMQ
   sudo systemctl restart rabbitmq-server
   ```

2. Use in code:
   ```rust
   let retry_config = RetryConfig::linear(3, Duration::from_secs(5))
       .with_delay_strategy(DelayStrategy::DelayedExchange);
   
   let consumer = Consumer::builder(connection, "orders")
       .with_retry(retry_config)
       .build();
   ```

**Pros:**
- More precise timing (microsecond-level)
- Cleaner architecture (single delay exchange)
- Better for high-volume scenarios
- Lower memory footprint on RabbitMQ
- Built-in reliability

**Cons:**
- Requires external plugin installation
- Plugin adds complexity to RabbitMQ setup
- Small performance overhead for delay management

**Flow Diagram:**

```
DelayedExchange Strategy Flow:
┌──────────┐
│  Queue   │  (e.g., "orders")
└────┬─────┘
     │
     ├─> Handler fails ──┐
     │                   │
     │              Publish to
     │              Delay Exchange
     │                   │
     └─────────── ┌──────▼──────┐
                  │ orders.delay │ (x-delay: 5000ms)
                  └──────┬───────┘
                         │
                    After delay
                         │
                  ┌──────▼──────┐
                  │  Queue      │
                  │  (orders)   │
                  └─────────────┘
```

**Example:**

See [delayed_exchange_example.rs](examples/delayed_exchange_example.rs) for a complete working example.

## ⚙️ **Advanced Features**

### MessageEnvelope System

For advanced retry tracking and error handling, use the MessageEnvelope system:

```rust
use rust_rabbit::{Connection, Publisher, Consumer, MessageEnvelope, RetryConfig};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    
    // Publisher with envelope
    let publisher = Publisher::new(connection.clone());
    let order = Order { id: 123, amount: 99.99 };
    let envelope = MessageEnvelope::new(order, "order_queue")
        .with_max_retries(3);
    
    publisher.publish_envelope_to_queue("order_queue", &envelope, None).await?;
    
    // Consumer with envelope processing
    let consumer = Consumer::builder(connection, "order_queue")
        .with_retry(RetryConfig::exponential_default())
        .build();
    
    consumer.consume_envelopes(|envelope: MessageEnvelope<Order>| async move {
        println!("Processing order {} (attempt {})", 
                 envelope.payload.id, 
                 envelope.metadata.retry_attempt + 1);
        
        // Access retry metadata
        if !envelope.is_first_attempt() {
            println!("This is a retry. Last error: {:?}", envelope.last_error());
        }
        
        Ok(())
    }).await?;
    
    Ok(())
}
```

### Connection Options

```rust
use rust_rabbit::Connection;

// Simple connection
let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

// Different connection URLs
let local = Connection::new("amqp://localhost:5672").await?;
let remote = Connection::new("amqp://user:pass@remote-host:5672/vhost").await?;
let secure = Connection::new("amqps://user:pass@secure-host:5671").await?;
```

### Publisher Options

```rust
use rust_rabbit::PublishOptions;

let options = PublishOptions::new()
    .mandatory()                         // Make delivery mandatory
    .priority(5);                        // Message priority (0-255)

publisher.publish_to_queue("orders", &message, Some(options)).await?;
```

### Consumer Options

```rust
let consumer = Consumer::builder(connection, "order_queue")
    .with_retry(RetryConfig::exponential_default())
    .bind_to_exchange("order_exchange", "new.order")  // Exchange binding with routing key
    .concurrency(10)                     // Process 10 messages in parallel
    .build();
```

## 📚 **Documentation**

For detailed guides and advanced topics:

- **[Retry Configuration Guide](docs/retry-guide.md)** - Detailed retry patterns and configuration
- **[Exchange & Queue Management](docs/queues-exchanges.md)** - Queue binding, exchange types, and best practices  
- **[Error Handling](docs/error-handling.md)** - Error types and handling strategies
- **[Best Practices](docs/best-practices.md)** - Production tips and patterns

## 🛠️ **Examples**

See the [`examples/`](examples/) directory for complete working examples:

- **[Basic Publisher](examples/basic_publisher.rs)** - Simple message publishing
- **[Basic Consumer](examples/basic_consumer.rs)** - Simple message consumption  
- **[Retry Examples](examples/retry_examples.rs)** - Different retry configurations
- **[Delayed Exchange Example](examples/delayed_exchange_example.rs)** - Using rabbitmq_delayed_message_exchange plugin
- **[Production Setup](examples/production_setup.rs)** - Production-ready configuration

## 🧪 **Testing**

Run the tests:

```bash
cargo test
```

For integration tests with real RabbitMQ:

```bash
# Start RabbitMQ with Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Run integration tests
cargo test --test integration
```

## 🔧 **Requirements**

- **Rust**: 1.70+
- **RabbitMQ**: 3.8+
- **Tokio**: Async runtime

## 🚦 **Migration from v0.x**

If you're upgrading from the complex v0.x version:

```rust
// OLD (v0.x) - Complex
let consumer = Consumer::new(connection_manager, ConsumerOptions {
    auto_ack: false,
    retry_policy: Some(RetryPolicy::fast()),
    prefetch_count: Some(10),
    // ... many more options
}).await?;

// NEW (v1.0) - Simple  
let consumer = Consumer::builder(connection, "queue")
    .with_retry(RetryConfig::exponential_default())
    .concurrency(10)
    .build();
```

**Major Changes:**
- ✅ Simplified API with just `Publisher` and `Consumer`
- ✅ Removed enterprise patterns (saga, event sourcing, request-response)
- ✅ Removed metrics and health monitoring 
- ✅ Unified retry system with flexible mechanisms
- ✅ Auto-declare queues and exchanges by default

## 🎯 **Design Philosophy**

rust-rabbit v1.0 follows these principles:

1. **Simplicity First**: Only essential features, no bloat
2. **Reliability Built-in**: Automatic retry and error handling  
3. **Easy Configuration**: Sensible defaults, minimal setup
4. **Production Ready**: Persistent messages, proper ACK handling
5. **Developer Friendly**: Clear errors, good documentation

## 🗺️ **Roadmap**

See [ROADMAP.md](ROADMAP.md) for planned features:

- Connection pooling and load balancing
- Monitoring and metrics integration
- Advanced retry patterns and policies
- Performance optimizations
- Additional messaging patterns

## 📄 **License**

MIT License - see [LICENSE](LICENSE) for details.

## 🤝 **Contributing**

Contributions welcome! Please read our [contributing guide](CONTRIBUTING.md) and submit pull requests.

## 💬 **Support**

- **Issues**: [GitHub Issues](https://github.com/nghiaphamln/rust-rabbit/issues)
- **Discussions**: [GitHub Discussions](https://github.com/nghiaphamln/rust-rabbit/discussions)
- **Documentation**: [docs.rs](https://docs.rs/rust-rabbit)

---

Made with ❤️ by the rust-rabbit team
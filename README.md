# rust-rabbit

A small RabbitMQ client library for Rust with a narrow API:

- `Connection` for broker access
- `Publisher` for queue or exchange publishing
- `Consumer` for queue consumption
- `RetryConfig` for retry and DLQ behavior
- `MessageEnvelope` for payloads that need retry metadata

## Install

```toml
[dependencies]
rust-rabbit = "1.2"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Quick Start

```rust,no_run
use rust_rabbit::{Connection, Consumer, Publisher, RetryConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    let publisher = Publisher::new(connection.clone());

    publisher
        .publish_to_queue("orders", &Order { id: 1, amount: 10.0 }, None)
        .await?;

    let consumer = Consumer::builder(connection, "orders")
        .with_retry(RetryConfig::exponential_default())
        .with_prefetch(5)
        .build();

    consumer
        .consume(|order: Order| async move {
            println!("processing order {}", order.id);
            Ok(())
        })
        .await?;

    Ok(())
}
```

## Core Behavior

- Publishing to a queue declares the queue if needed.
- Publishing to an exchange declares the exchange as durable topic.
- Consumers declare their queue and optional exchange binding on startup.
- Handler errors trigger retry when `RetryConfig` is set; exhausted messages go to DLQ.
- `consume()` works with raw payloads and detects MassTransit envelopes automatically.
- `consume_envelopes()` works with `MessageEnvelope<T>`.
- `manual_ack()` is currently not supported at runtime because handlers do not receive an ack handle.

## Retry

```rust
use rust_rabbit::{DelayStrategy, RetryConfig};
use std::time::Duration;

let exponential = RetryConfig::exponential_default();
let linear = RetryConfig::linear(3, Duration::from_secs(5));
let custom = RetryConfig::custom(vec![Duration::from_secs(1), Duration::from_secs(10)]);
let delayed = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);
```

See [docs/retry-guide.md](docs/retry-guide.md) for retry choices and [docs/queues-exchanges.md](docs/queues-exchanges.md) for routing patterns.

## Examples

- `examples/basic_publisher.rs`
- `examples/basic_consumer.rs`
- `examples/retry_examples.rs`
- `examples/envelope_example.rs`
- `examples/masstransit_option_example.rs`
- `examples/delayed_exchange_example.rs`
- `examples/production_setup.rs`

## Documentation

- [Retry Guide](docs/retry-guide.md)
- [Queues and Exchanges](docs/queues-exchanges.md)
- [Error Handling](docs/error-handling.md)
- [Best Practices](docs/best-practices.md)

API docs: <https://docs.rs/rust-rabbit>

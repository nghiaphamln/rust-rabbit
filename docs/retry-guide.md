# Retry Guide

Use retry only for failures that may succeed later. `rust-rabbit` supports four retry setups through `RetryConfig`.

## Choose a Retry Pattern

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

let default_exp = RetryConfig::exponential_default();
let exp = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(30));
let linear = RetryConfig::linear(3, Duration::from_secs(10));
let custom = RetryConfig::custom(vec![Duration::from_secs(1), Duration::from_secs(5)]);
let none = RetryConfig::no_retry();
```

- `exponential_default()` fits most transient network or dependency failures.
- `linear()` fits fixed retry windows.
- `custom()` fits broker or business schedules you already know.
- `no_retry()` sends failures directly to DLQ.

## Choose a Delay Strategy

```rust
use rust_rabbit::{DelayStrategy, RetryConfig};

let ttl = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::TTL);

let delayed_exchange = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);
```

- `TTL` is the default. It works on standard RabbitMQ and creates retry queues like `orders.retry.1`.
- `DelayedExchange` requires the `rabbitmq_delayed_message_exchange` plugin and uses one delay exchange like `orders.delay`.

Use `DelayedExchange` only when the plugin is already part of your broker standard.

## DLQ

Messages that exceed retries go to `{queue}.dlq` unless you override names.

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

let config = RetryConfig::exponential_default()
    .with_dead_letter("orders.dlx".to_string(), "orders.dead".to_string())
    .with_dlq_ttl(Duration::from_secs(86_400));
```

## Consumer Example

```rust,no_run
let consumer = Consumer::builder(connection, "orders")
    .with_retry(RetryConfig::exponential_default())
    .with_prefetch(10)
    .build();
```

## Practical Rules

- Retry transient failures, not validation failures.
- Keep delays short at first and cap them.
- Prefer envelope consumers when you need retry history in the payload.
- Verify broker plugin availability before switching to `DelayedExchange`.

# Retry Configuration Guide

This guide covers the retry system in rust-rabbit, including retry mechanisms, delay strategies, and Dead Letter Queue configuration.

## Table of Contents

- [Overview](#overview)
- [Retry Mechanisms](#retry-mechanisms)
  - [Exponential Backoff](#exponential-backoff)
  - [Linear Retry](#linear-retry)
  - [Custom Delays](#custom-delays)
  - [No Retry](#no-retry)
- [Delay Strategies](#delay-strategies)
  - [TTL Strategy](#ttl-strategy)
  - [DelayedExchange Strategy](#delayedexchange-strategy)
- [Dead Letter Queue](#dead-letter-queue)
- [Configuration Examples](#configuration-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

rust-rabbit provides an automatic retry system for failed message processing. When a message handler returns an error, the message is retried with configurable delays before being sent to a Dead Letter Queue.

The retry flow:

1. Message processing fails
2. Message is sent to retry queue with delay
3. After delay, message returns to original queue
4. If max retries exceeded, message goes to DLQ

## Retry Mechanisms

### Exponential Backoff

Exponential retry doubles the delay with each attempt. Ideal for transient errors that may resolve over time.

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

// Default: 1s, 2s, 4s, 8s, 16s (5 retries)
let config = RetryConfig::exponential_default();

// Custom with base delay and max cap
let config = RetryConfig::exponential(
    10,                                // max retries
    Duration::from_millis(500),       // base delay
    Duration::from_secs(30)           // max delay
);
// Delays: 500ms, 1s, 2s, 4s, 8s, 16s, 30s, 30s, 30s, 30s
```

Use cases:
- Network timeouts
- Database connection errors
- Rate limiting errors
- Transient API failures

### Linear Retry

Linear retry uses the same delay for each attempt. Good for consistent retry intervals.

```rust
// 3 retries with 10 second intervals
let config = RetryConfig::linear(3, Duration::from_secs(10));
// Delays: 10s, 10s, 10s
```

Use cases:
- External API calls with known retry windows
- Rate-limited services with fixed windows
- Scheduled processing tasks

### Custom Delays

Define exact delays for each retry attempt.

```rust
let config = RetryConfig::custom(vec![
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
    Duration::from_secs(60),
]);
// Delays: 1s, 5s, 30s, 60s (4 retries total)
```

Use cases:
- Complex business logic requiring specific timing
- Gradual backoff patterns
- Integration with external system schedules

### No Retry

Disable retries completely. Messages go directly to DLQ on failure.

```rust
let config = RetryConfig::no_retry();
```

Use cases:
- Idempotent operations that should not retry
- Testing and development
- Operations requiring immediate failure notification

## Delay Strategies

### TTL Strategy

Uses RabbitMQ's built-in TTL feature. This is the default strategy.

```rust
use rust_rabbit::{RetryConfig, DelayStrategy};

let config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::TTL);
```

How it works:

1. Failed message sent to retry queue with TTL set
2. Message expires after TTL
3. Dead letter routing returns it to original queue

Pros:
- No plugin required
- Works with standard RabbitMQ
- Simple setup

Cons:
- Less precise timing
- Creates multiple retry queues
- Higher memory usage

Queue naming:
- Retry queues: `{queue_name}.retry.{attempt}`
- Example: `orders.retry.1`, `orders.retry.2`

### DelayedExchange Strategy

Uses the `rabbitmq_delayed_message_exchange` plugin for precise delays.

```rust
use rust_rabbit::{RetryConfig, DelayStrategy};

let config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);
```

Installation required:

```bash
# For RabbitMQ 3.8+
# Download plugin from:
# https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases

# Or install via package manager
# Ubuntu/Debian:
sudo apt-get install rabbitmq-delayed-message-exchange

# Or manually download and copy to plugins directory:
wget https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.13.0/rabbitmq_delayed_message_exchange-3.13.0.ez
sudo cp rabbitmq_delayed_message_exchange-3.13.0.ez /usr/lib/rabbitmq/plugins/

# Enable plugin
rabbitmq-plugins enable rabbitmq_delayed_message_exchange

# Restart RabbitMQ
sudo systemctl restart rabbitmq-server

# Verify plugin is enabled
rabbitmq-plugins list | grep delayed
# Should show: [E*] rabbitmq_delayed_message_exchange
```

Docker setup:

```dockerfile
FROM rabbitmq:3.13-management

# Install delayed message exchange plugin
RUN rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

Or use docker-compose:

```yaml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_PLUGINS: rabbitmq_delayed_message_exchange
```

How it works:

1. Failed message published to delay exchange with x-delay header
2. Exchange holds message for specified delay
3. Message automatically routed back to original queue

Pros:
- Precise timing (microsecond level)
- Single delay exchange per queue
- Lower memory footprint
- Better for high-volume scenarios

Cons:
- Requires plugin installation
- Additional RabbitMQ configuration
- Small performance overhead

Queue naming:
- Delay exchange: `{queue_name}.delay`
- Example: `orders.delay`

Important: Application will fail if plugin is not installed when using this strategy. Always verify plugin is enabled before deployment.

## Dead Letter Queue

Messages that exceed max retries are sent to a Dead Letter Queue (DLQ) for manual inspection or cleanup.

### Basic DLQ

By default, DLQ has no TTL and messages remain until manually removed.

```rust
let config = RetryConfig::exponential_default();

let consumer = Consumer::builder(connection, "orders")
    .with_retry(config)
    .build();
```

Queue naming:
- DLQ: `{queue_name}.dlq`
- Example: `orders.dlq`

### DLQ with Auto-Cleanup

Configure TTL for automatic message removal from DLQ.

```rust
use std::time::Duration;

let config = RetryConfig::exponential_default()
    .with_dlq_ttl(Duration::from_secs(86400)); // 1 day

let consumer = Consumer::builder(connection, "orders")
    .with_retry(config)
    .build();
```

Common TTL values:

```rust
// 1 hour (for testing or fast cleanup)
.with_dlq_ttl(Duration::from_secs(3600))

// 1 day (default for most applications)
.with_dlq_ttl(Duration::from_secs(86400))

// 1 week (for audit trail or analysis)
.with_dlq_ttl(Duration::from_secs(604800))
```

Monitoring DLQ:

Access RabbitMQ Management UI at http://localhost:15672:

1. Navigate to Queues tab
2. Find `{queue_name}.dlq`
3. View message count and TTL configuration
4. Inspect failed messages
5. Manually requeue or delete if needed

## Configuration Examples

### Production Configuration

Exponential retry with delayed exchange and DLQ cleanup:

```rust
use rust_rabbit::{Connection, Consumer, RetryConfig, DelayStrategy};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = Connection::new("amqp://localhost:5672").await?;
    
    let retry_config = RetryConfig::exponential(
        5,                              // max 5 retries
        Duration::from_secs(1),        // start with 1s
        Duration::from_secs(60)        // cap at 60s
    )
    .with_delay_strategy(DelayStrategy::DelayedExchange)
    .with_dlq_ttl(Duration::from_secs(86400)); // 1 day retention
    
    let consumer = Consumer::builder(connection, "orders")
        .with_retry(retry_config)
        .with_prefetch(10)
        .bind_to_exchange("order_events", "order.*")
        .build();
    
    consumer.consume(|msg: Order| async move {
        process_order(msg).await
    }).await?;
    
    Ok(())
}
```

### Development Configuration

Simple TTL strategy with fast retries:

```rust
let retry_config = RetryConfig::linear(
    3,                              // only 3 retries
    Duration::from_secs(5)         // 5 second delay
)
.with_delay_strategy(DelayStrategy::TTL)
.with_dlq_ttl(Duration::from_secs(3600)); // 1 hour cleanup

let consumer = Consumer::builder(connection, "test_queue")
    .with_retry(retry_config)
    .build();
```

### High-Volume Configuration

Custom delays with delayed exchange for performance:

```rust
let retry_config = RetryConfig::custom(vec![
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
    Duration::from_secs(10),
    Duration::from_secs(30),
])
.with_delay_strategy(DelayStrategy::DelayedExchange)
.with_dlq_ttl(Duration::from_secs(604800)); // 1 week retention

let consumer = Consumer::builder(connection, "high_volume_queue")
    .with_retry(retry_config)
    .with_prefetch(100)
    .build();
```

## Best Practices

1. Start Conservative: Begin with exponential default and adjust based on monitoring
2. Match Business Needs: Choose retry timing based on downstream system SLAs
3. Monitor DLQ: Set up alerts for DLQ message count
4. Use Delayed Exchange in Production: Better performance and reliability
5. Set DLQ TTL: Prevent unbounded DLQ growth
6. Test Retry Logic: Verify retry behavior in staging environment
7. Consider Idempotency: Ensure message handlers can safely retry
8. Log Retry Attempts: Add logging to track retry patterns

## Troubleshooting

### Messages not retrying

- Verify retry config is applied to consumer
- Check RabbitMQ logs for errors
- Ensure queues are properly declared

### Delayed exchange errors

- Confirm plugin is installed: `rabbitmq-plugins list`
- Check plugin is enabled: Look for `[E*]` marker
- Verify RabbitMQ version compatibility (3.8+)
- Check RabbitMQ logs: `sudo journalctl -u rabbitmq-server -f`
- Test plugin manually via management UI

If you see "NOT_FOUND - no exchange" error:
- Plugin is not enabled
- RabbitMQ needs restart after enabling plugin
- Check plugin version matches RabbitMQ version

### DLQ filling up

- Check message processing logic
- Review error patterns in logs
- Adjust retry configuration
- Verify DLQ TTL is set

### Timing imprecision

- Use DelayedExchange strategy for precision
- Check RabbitMQ server load
- Review TTL queue configuration

## See Also

- [Error Handling Guide](error-handling.md) - Error classification and handling strategies
- [Best Practices Guide](best-practices.md) - Production deployment patterns
- [Queues and Exchanges Guide](queues-exchanges.md) - Queue and exchange configuration

## Quick Reference

### Retry Configuration Quick Guide

```rust
// Exponential backoff (recommended for production)
RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange)
    .with_dlq_ttl(Duration::from_secs(86400))

// Linear retry (for predictable timing)
RetryConfig::linear(3, Duration::from_secs(10))

// Custom delays (for complex scenarios)
RetryConfig::custom(vec![
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
])

// No retry (fail immediately to DLQ)
RetryConfig::no_retry()
```

### DelayedExchange Plugin Installation

**Ubuntu/Debian:**
```bash
sudo apt-get install rabbitmq-delayed-message-exchange
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
sudo systemctl restart rabbitmq-server
```

**Docker:**
```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    environment:
      RABBITMQ_PLUGINS: rabbitmq_delayed_message_exchange
```

**Verify installation:**
```bash
rabbitmq-plugins list | grep delayed
# Output: [E*] rabbitmq_delayed_message_exchange
```

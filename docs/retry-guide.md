# Retry Configuration Guide

This guide covers the flexible retry system in rust-rabbit, including different retry mechanisms and configuration options.

## Overview

rust-rabbit provides a simple but powerful retry system that automatically handles failed message processing. When a message handler returns an error, the message can be retried with configurable delays before being sent to a dead letter queue.

## Retry Mechanisms

### 1. Exponential Backoff

Exponential retry doubles the delay with each attempt, ideal for transient errors that may resolve over time.

```rust
use rust_rabbit::RetryConfig;
use std::time::Duration;

// Default exponential: 1s → 2s → 4s → 8s → 16s (5 retries)
let config = RetryConfig::exponential_default();

// Custom exponential with base delay and max cap
let config = RetryConfig::exponential(
    10,                                    // max retries
    Duration::from_millis(500),           // base delay: 500ms
    Duration::from_secs(30)               // max delay: 30s
);

// Delays will be: 500ms → 1s → 2s → 4s → 8s → 16s → 30s → 30s → 30s → 30s
```

**When to use:**
- Network timeouts
- Database connection errors  
- Rate limiting errors
- Any transient failures

### 2. Linear Retry

Linear retry uses the same delay for each attempt, good for consistent retry intervals.

```rust
// 3 retries with 10 second intervals
let config = RetryConfig::linear(3, Duration::from_secs(10));

// Delays will be: 10s → 10s → 10s
```

**When to use:**
- External API calls with known retry windows
- When you want predictable retry timing
- Testing and development

### 3. Custom Delays

Define exact delays for each retry attempt.

```rust
let config = RetryConfig::custom(vec![
    Duration::from_secs(1),       // First retry after 1s
    Duration::from_secs(5),       // Second retry after 5s  
    Duration::from_secs(30),      // Third retry after 30s
    Duration::from_minutes(5),    // Fourth retry after 5 minutes
]);
```

**When to use:**
- Complex business requirements
- Integration with external systems with specific retry windows
- Fine-tuned performance optimization

### 4. No Retry

Disable retries entirely - failed messages go directly to dead letter queue.

```rust
let config = RetryConfig::no_retry();
```

**When to use:**
- Messages that must not be reprocessed
- Testing error handling
- Poison message detection

## Configuration Options

### Dead Letter Handling

```rust
let config = RetryConfig::exponential_default()
    .with_dead_letter("custom.dlx".to_string(), "custom.dlq".to_string());

// Default names (if not specified):
// DLX: {queue_name}.dlx  
// DLQ: {queue_name}.dlq
```

### Consumer Integration

```rust
use rust_rabbit::{Consumer, RetryConfig};

let consumer = Consumer::builder(connection, "order_queue")
    .with_retry(RetryConfig::exponential_default())
    .build();

consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    // Process order
    match process_order(msg.data).await {
        Ok(_) => Ok(()),      // Message will be ACKed
        Err(e) => Err(e),     // Message will be retried according to config
    }
}).await?;
```

## How Retry Works Internally

1. **Message Processing**: Handler returns `Err(_)`
2. **Retry Check**: Check if more retries are available
3. **Delay Queue**: Message sent to temporary queue with TTL
4. **Return to Original**: After TTL expires, message returns to original queue
5. **Retry Attempt**: Message reprocessed with incremented retry count
6. **Dead Letter**: If max retries exceeded, message sent to DLQ

```
Original Queue → Handler Error → Retry Queue (with TTL) → Original Queue → ... → DLQ
```

## Queue and Exchange Structure

For a queue named `orders` with retry enabled:

```
orders                    # Main processing queue
orders.retry.1           # First retry (temporary, auto-expires)
orders.retry.2           # Second retry (temporary, auto-expires)  
orders.retry.3           # Third retry (temporary, auto-expires)
orders.dlx               # Dead letter exchange
orders.dlq               # Dead letter queue (permanent failures)
```

## Best Practices

### 1. Choose Appropriate Mechanisms

```rust
// For network/API errors - exponential backoff
let network_retry = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(60));

// For business logic errors - linear or custom
let business_retry = RetryConfig::linear(3, Duration::from_secs(30));

// For critical messages - no retry, manual investigation
let critical_retry = RetryConfig::no_retry();
```

### 2. Set Reasonable Limits

```rust
// Good: Reasonable retry counts and delays
let good_config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_minutes(5));

// Bad: Too many retries, too long delays
let bad_config = RetryConfig::exponential(50, Duration::from_secs(1), Duration::from_hours(1));
```

### 3. Monitor Dead Letter Queues

```rust
// Set up monitoring for DLQs
let config = RetryConfig::exponential_default()
    .with_dead_letter("monitoring.dlx".to_string(), "failed_orders.dlq".to_string());
```

### 4. Error Classification

```rust
consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    match process_order(order).await {
        Ok(_) => Ok(()),
        Err(e) => {
            match e.kind() {
                ErrorKind::Transient => Err(e),     // Retry
                ErrorKind::Permanent => {
                    log::error!("Permanent error: {}", e);
                    Ok(()) // Don't retry, but ACK to avoid infinite loop
                }
            }
        }
    }
}).await?;
```

## Examples

### E-commerce Order Processing

```rust
use rust_rabbit::{Consumer, RetryConfig};
use std::time::Duration;

let retry_config = RetryConfig::exponential(
    5,                                    // 5 retry attempts
    Duration::from_secs(2),              // Start with 2 seconds
    Duration::from_minutes(10)           // Cap at 10 minutes
);

let consumer = Consumer::builder(connection, "order_processing")
    .with_retry(retry_config)
    .concurrency(10)
    .build();

consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    // Try to process the order
    match charge_payment(&msg.data).await {
        Ok(_) => {
            fulfill_order(&msg.data).await?;
            Ok(()) // Success - ACK
        }
        Err(PaymentError::TemporaryFailure(_)) => {
            Err("Payment temporarily unavailable".into()) // Retry
        }
        Err(PaymentError::InvalidCard(_)) => {
            log::warn!("Invalid card for order {}", msg.data.id);
            Ok(()) // Don't retry invalid cards
        }
    }
}).await?;
```

### API Integration with Rate Limiting

```rust
let api_retry = RetryConfig::custom(vec![
    Duration::from_secs(1),      // Quick first retry
    Duration::from_secs(5),      // Medium wait  
    Duration::from_secs(30),     // Longer wait for rate limits
    Duration::from_minutes(5),   // Long wait before giving up
]);

let consumer = Consumer::builder(connection, "api_calls")
    .with_retry(api_retry)
    .build();

consumer.consume(|msg: rust_rabbit::Message<ApiRequest>| async move {
    match external_api_call(&msg.data).await {
        Ok(response) => {
            store_response(response).await?;
            Ok(())
        }
        Err(ApiError::RateLimit) => Err("Rate limited".into()), // Retry
        Err(ApiError::BadRequest) => Ok(()), // Don't retry bad requests
        Err(ApiError::ServerError) => Err("Server error".into()), // Retry
    }
}).await?;
```

## Troubleshooting

### Common Issues

1. **Messages stuck in retry loops**
   - Check error classification logic
   - Ensure permanent errors don't trigger retries
   - Monitor DLQ for patterns

2. **Retry delays too short/long**
   - Adjust base delay and max delay
   - Consider exponential vs linear based on error type
   - Test with realistic error scenarios

3. **DLQ filling up**
   - Investigate root cause of failures
   - Review retry configuration
   - Implement DLQ monitoring and alerting

### Debugging

```rust
// Add logging to understand retry behavior
consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    log::info!("Processing order {} (attempt {})", msg.data.id, attempt);
    
    match process_order(order).await {
        Ok(_) => {
            log::info!("Order {} processed successfully", msg.data.id);
            Ok(())
        }
        Err(e) => {
            log::warn!("Order {} failed: {} (will retry)", msg.data.id, e);
            Err(e)
        }
    }
}).await?;
```

## Performance Considerations

1. **Retry queues are temporary** - They auto-delete after message TTL
2. **Each retry creates a new queue** - Monitor queue count in high-volume systems
3. **DLQ messages persist** - Implement cleanup policies for old DLQ messages
4. **Retry delays affect throughput** - Balance reliability vs speed

For more information, see:
- [Error Handling Guide](error-handling.md)
- [Best Practices](best-practices.md)
- [Examples](../examples/retry_examples.rs)

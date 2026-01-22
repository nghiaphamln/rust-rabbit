# Error Handling Guide

This guide covers error handling patterns and strategies in rust-rabbit, including error classification, recovery strategies, and best practices.

## Table of Contents

- [Overview](#overview)
- [Error Types](#error-types)
- [Error Classification](#error-classification)
- [Handling Strategies](#handling-strategies)
- [Best Practices](#best-practices)
- [Common Patterns](#common-patterns)

## Overview

rust-rabbit provides comprehensive error handling through the `RustRabbitError` type and built-in retry mechanisms. Understanding how to properly handle different error types is crucial for building reliable messaging applications.

## Error Types

### Core Error Categories

```rust
use rust_rabbit::{RustRabbitError, Result};

// Connection-related errors
RustRabbitError::Connection(String)

// Protocol errors from RabbitMQ
RustRabbitError::Protocol(lapin::Error)

// Message serialization errors
RustRabbitError::Serialization(String)

// Configuration errors
RustRabbitError::Configuration(String)

// Consumer processing errors
RustRabbitError::Consumer(String)

// Publisher errors
RustRabbitError::Publisher(String)

// Retry system errors
RustRabbitError::Retry(String)

// IO errors
RustRabbitError::Io(std::io::Error)
```

### Examples

```rust
use rust_rabbit::RustRabbitError;

// Connection error
let conn_error = RustRabbitError::Connection(
    "Failed to connect to amqp://localhost:5672".to_string()
);

// Serialization error
let ser_error = RustRabbitError::Serialization(
    "Failed to deserialize Order: missing field 'id'".to_string()
);

// Consumer error
let consumer_error = RustRabbitError::Consumer(
    "Message processing failed: database timeout".to_string()
);
```

## Error Classification

### Retryable vs Non-Retryable

rust-rabbit classifies errors automatically:

```rust
use rust_rabbit::RustRabbitError;

fn handle_error(error: &RustRabbitError) {
    if error.is_retryable() {
        println!("This error might resolve itself, will retry");
    } else {
        println!("Permanent error, will not retry");
    }
}
```

Retryable errors:
- Connection errors
- Protocol errors (IO and AMQP)
- Consumer errors
- Publisher errors
- IO errors

Non-retryable errors:
- Serialization errors
- Configuration errors
- Retry system errors

### Connection Errors

Check if error is connection-related:

```rust
if error.is_connection_error() {
    println!("Connection issue detected");
    // Maybe try reconnecting
}
```

## Handling Strategies

### Basic Error Handling

Simple error handling in message consumer:

```rust
use rust_rabbit::{Connection, Consumer};

consumer.consume(|msg: Order| async move {
    // Return Err to trigger retry
    if msg.amount <= 0.0 {
        return Err("Invalid amount".into());
    }
    
    // Return Ok to ACK message
    Ok(())
}).await?;
```

### Detailed Error Context

Provide context with errors:

```rust
consumer.consume(|msg: Order| async move {
    match process_order(&msg).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let error_msg = format!(
                "Failed to process order {}: {}",
                msg.id,
                e
            );
            Err(error_msg.into())
        }
    }
}).await?;
```

### Error Recovery

Attempt recovery before failing:

```rust
use tokio::time::{sleep, Duration};

consumer.consume(|msg: Order| async move {
    let mut attempts = 0;
    
    loop {
        match save_to_database(&msg).await {
            Ok(_) => return Ok(()),
            Err(e) if attempts < 3 => {
                attempts += 1;
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => {
                return Err(format!("Database save failed after {} attempts: {}", attempts, e).into());
            }
        }
    }
}).await?;
```

### Selective Retry

Decide whether to retry based on error type:

```rust
consumer.consume(|msg: Order| async move {
    match validate_and_process(&msg).await {
        Ok(_) => Ok(()),
        Err(e) if e.is_validation_error() => {
            // Don't retry validation errors - send to DLQ immediately
            log::error!("Validation failed: {}", e);
            Err(format!("Validation error (no retry): {}", e).into())
        }
        Err(e) => {
            // Retry other errors
            log::warn!("Processing error (will retry): {}", e);
            Err(e.to_string().into())
        }
    }
}).await?;
```

## Best Practices

### 1. Provide Context

Always include relevant context in error messages:

```rust
// Good
Err(format!(
    "Failed to process order {} for customer {}: {}",
    order.id, customer.id, error
).into())

// Avoid
Err("Processing failed".into())
```

### 2. Log Appropriately

Use appropriate log levels:

```rust
consumer.consume(|msg: Order| async move {
    match process_order(&msg).await {
        Ok(_) => {
            log::info!("Successfully processed order {}", msg.id);
            Ok(())
        }
        Err(e) if e.is_retryable() => {
            log::warn!("Retryable error for order {}: {}", msg.id, e);
            Err(e.to_string().into())
        }
        Err(e) => {
            log::error!("Fatal error for order {}: {}", msg.id, e);
            Err(e.to_string().into())
        }
    }
}).await?;
```

### 3. Monitor DLQ

Set up monitoring for dead letter queue:

```rust
// In your monitoring/alerting system
// Alert if orders.dlq message count > 0
// Track DLQ growth rate
// Review DLQ messages regularly
```

### 4. Handle Panics

Catch panics to prevent consumer crash:

```rust
use std::panic::AssertUnwindSafe;
use futures::FutureExt;

consumer.consume(|msg: Order| async move {
    let result = AssertUnwindSafe(process_order(&msg))
        .catch_unwind()
        .await;
    
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string().into()),
        Err(_) => {
            log::error!("Panic while processing order {}", msg.id);
            Err("Processing panicked".into())
        }
    }
}).await?;
```

### 5. Implement Timeouts

Add timeouts to prevent hanging:

```rust
use tokio::time::{timeout, Duration};

consumer.consume(|msg: Order| async move {
    match timeout(Duration::from_secs(30), process_order(&msg)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("Processing failed: {}", e).into()),
        Err(_) => Err("Processing timeout after 30s".into()),
    }
}).await?;
```

## Common Patterns

### Pattern 1: Graceful Degradation

Continue with reduced functionality on non-critical errors:

```rust
consumer.consume(|msg: Order| async move {
    // Critical operation
    save_order(&msg).await?;
    
    // Non-critical operation
    if let Err(e) = send_notification(&msg).await {
        log::warn!("Failed to send notification: {}", e);
        // Don't fail the whole operation
    }
    
    Ok(())
}).await?;
```

### Pattern 2: Circuit Breaker

Prevent cascading failures:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

struct CircuitBreaker {
    failures: Arc<RwLock<u32>>,
    threshold: u32,
}

impl CircuitBreaker {
    async fn call<F, T>(&self, f: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<T, Box<dyn std::error::Error>>,
    {
        let failures = *self.failures.read().await;
        
        if failures >= self.threshold {
            return Err("Circuit breaker open".into());
        }
        
        match f() {
            Ok(result) => {
                *self.failures.write().await = 0;
                Ok(result)
            }
            Err(e) => {
                *self.failures.write().await += 1;
                Err(e)
            }
        }
    }
}
```

### Pattern 3: Idempotency Check

Prevent duplicate processing:

```rust
consumer.consume(|msg: Order| async move {
    // Check if already processed
    if is_already_processed(&msg.id).await? {
        log::info!("Order {} already processed, skipping", msg.id);
        return Ok(());
    }
    
    // Mark as processing
    mark_as_processing(&msg.id).await?;
    
    // Process order
    match process_order(&msg).await {
        Ok(_) => {
            mark_as_completed(&msg.id).await?;
            Ok(())
        }
        Err(e) => {
            mark_as_failed(&msg.id, &e).await?;
            Err(e.into())
        }
    }
}).await?;
```

### Pattern 4: Structured Error Types

Define application-specific errors:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
enum OrderError {
    #[error("Invalid order: {0}")]
    Validation(String),
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    
    #[error("External API error: {0}")]
    ExternalApi(String),
}

consumer.consume(|msg: Order| async move {
    match process_order(&msg).await {
        Ok(_) => Ok(()),
        Err(OrderError::Validation(e)) => {
            // Don't retry validation errors
            log::error!("Validation error: {}", e);
            Ok(()) // ACK to prevent retry
        }
        Err(e) => {
            // Retry other errors
            Err(e.to_string().into())
        }
    }
}).await?;
```

## Troubleshooting

### Messages going to DLQ immediately

- Check error handling logic
- Verify retry configuration is applied
- Review error types being returned
- Check if errors are being caught and handled

### Infinite retry loops

- Ensure proper error classification
- Check retry limits are set
- Verify DLQ is configured
- Review error messages in logs

### Silent failures

- Add comprehensive logging
- Monitor consumer activity
- Check error handling doesn't swallow errors
- Verify ACK/NACK behavior

### Memory leaks from error handling

- Don't accumulate errors in memory
- Use bounded collections for error tracking
- Implement proper cleanup
- Monitor memory usage

## See Also

- [Retry Configuration Guide](retry-guide.md) - Configure retry behavior
- [Best Practices Guide](best-practices.md) - Production deployment patterns
- [Queues and Exchanges Guide](queues-exchanges.md) - Queue configuration

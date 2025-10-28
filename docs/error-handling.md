# Error Handling Guide

This guide covers error handling patterns and strategies in rust-rabbit, including error classification, recovery strategies, and best practices.

## Overview

rust-rabbit provides comprehensive error handling through the `RustRabbitError` type and built-in retry mechanisms. Understanding how to properly handle different error types is crucial for building reliable messaging applications.

## Error Types

### Core Error Categories

```rust
use rust_rabbit::{RustRabbitError, Result};

// Connection-related errors
let conn_error = RustRabbitError::Connection("Failed to connect".to_string());

// Protocol errors from RabbitMQ
let protocol_error = RustRabbitError::Protocol(lapin_error);

// Message serialization errors
let ser_error = RustRabbitError::Serialization("Invalid JSON".to_string());

// Configuration errors
let config_error = RustRabbitError::Configuration("Invalid URL".to_string());

// Consumer processing errors
let consumer_error = RustRabbitError::Consumer("Processing failed".to_string());

// Publisher errors
let publisher_error = RustRabbitError::Publisher("Send failed".to_string());

// Retry system errors
let retry_error = RustRabbitError::Retry("Retry failed".to_string());

// IO errors
let io_error = RustRabbitError::Io(std::io::Error::new(std::io::ErrorKind::Other, "IO failed"));
```

### Error Classification

```rust
// Check if an error should trigger a retry
if error.is_retryable() {
    // This error might resolve itself, safe to retry
    println!("Retryable error: {}", error);
} else {
    // Permanent error, don't retry
    println!("Permanent error: {}", error);
}

// Check if it's a connection issue
if error.is_connection_error() {
    // Connection problem, might need reconnection
    println!("Connection issue: {}", error);
}

// Get user-friendly message (no technical details)
let user_msg = error.user_message();
println!("User message: {}", user_msg);
```

## Error Handling Patterns

### 1. Consumer Error Handling

```rust
use rust_rabbit::{Consumer, RetryConfig};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
    amount: f64,
}

// Basic error handling with retry
let consumer = Consumer::builder(connection, "orders")
    .with_retry(RetryConfig::exponential_default())
    .build();

consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    match process_order(msg.data).await {
        Ok(_) => Ok(()),           // Success - message ACKed
        Err(e) => Err(e),         // Error - message retried according to config
    }
}).await?;
```

### 2. Selective Error Handling

```rust
#[derive(Debug)]
enum ProcessingError {
    Transient(String),      // Should retry
    Permanent(String),      // Should not retry  
    Critical(String),       // Needs immediate attention
}

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProcessingError::Transient(msg) => write!(f, "Transient: {}", msg),
            ProcessingError::Permanent(msg) => write!(f, "Permanent: {}", msg),
            ProcessingError::Critical(msg) => write!(f, "Critical: {}", msg),
        }
    }
}

impl std::error::Error for ProcessingError {}

consumer.consume(|msg: rust_rabbit::Message<Order>| async move {
    match process_order(msg.data).await {
        Ok(_) => Ok(()),
        Err(ProcessingError::Transient(msg)) => {
            log::warn!("Transient error, will retry: {}", msg);
            Err(msg.into())  // Trigger retry
        }
        Err(ProcessingError::Permanent(msg)) => {
            log::error!("Permanent error, not retrying: {}", msg);
            Ok(())  // ACK to avoid infinite loop
        }
        Err(ProcessingError::Critical(msg)) => {
            log::error!("Critical error: {}", msg);
            send_alert(&msg).await;
            Ok(())  // ACK but alert
        }
    }
}).await?;
```

### 3. Publisher Error Handling

```rust
use rust_rabbit::{Publisher, PublishOptions, RustRabbitError};

async fn safe_publish(
    publisher: &Publisher,
    queue: &str,
    message: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut retries = 3;
    
    loop {
        match publisher.publish_to_queue(queue, message, None).await {
            Ok(_) => return Ok(()),
            Err(RustRabbitError::Connection(_)) if retries > 0 => {
                log::warn!("Connection error, retrying... ({} attempts left)", retries);
                retries -= 1;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            Err(RustRabbitError::Serialization(e)) => {
                log::error!("Serialization error (permanent): {}", e);
                return Err(e.into());
            }
            Err(e) => {
                log::error!("Publisher error: {}", e);
                return Err(e.into());
            }
        }
    }
}
```

## Connection Error Recovery

### Automatic Reconnection

```rust
use rust_rabbit::{Connection, ConnectionBuilder};

// Connection with automatic reconnection on channel creation
let connection = ConnectionBuilder::new("amqp://localhost:5672")
    .connection_timeout(30)
    .heartbeat(60)
    .connect()
    .await?;

// Channels automatically reconnect when needed
let publisher = Publisher::new(connection.clone());

// This will reconnect if connection is lost
match publisher.publish_to_queue("queue", &message, None).await {
    Ok(_) => println!("Published successfully"),
    Err(e) => {
        log::error!("Failed to publish after reconnection attempts: {}", e);
        // Handle permanent failure
    }
}
```

### Manual Connection Monitoring

```rust
use tokio::time::{interval, Duration};

async fn monitor_connection(connection: Arc<Connection>) {
    let mut interval = interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        
        if !connection.is_connected().await {
            log::warn!("Connection lost, attempting reconnection...");
            
            match connection.reconnect().await {
                Ok(_) => log::info!("Reconnected successfully"),
                Err(e) => log::error!("Reconnection failed: {}", e),
            }
        }
    }
}

// Start monitoring in background
let monitor_handle = tokio::spawn(monitor_connection(connection.clone()));
```

## Retry Strategies

### Built-in Retry with Error Classification

```rust
use rust_rabbit::RetryConfig;

consumer.consume(|msg: rust_rabbit::Message<Message>| async move {
    match process_message(message).await {
        Ok(_) => Ok(()),
        Err(e) => {
            // Classify error to determine if retry is appropriate
            match e.kind() {
                ErrorKind::NetworkTimeout => {
                    log::info!("Network timeout, will retry");
                    Err(e.into())  // Retry with exponential backoff
                }
                ErrorKind::RateLimited => {
                    log::info!("Rate limited, will retry");
                    Err(e.into())  // Retry with delays
                }
                ErrorKind::InvalidData => {
                    log::error!("Invalid data, not retrying: {}", e);
                    Ok(())  // Don't retry bad data
                }
                ErrorKind::InternalError => {
                    log::error!("Internal error: {}", e);
                    Err(e.into())  // Retry internal errors
                }
            }
        }
    }
}).await?;
```

### Custom Retry Logic

```rust
use std::time::Duration;

// Custom retry for specific error types
let custom_retry = RetryConfig::custom(vec![
    Duration::from_secs(1),     // Quick first retry
    Duration::from_secs(5),     // Medium wait
    Duration::from_secs(30),    // Longer wait
    Duration::from_minutes(5),  // Long wait before giving up
]);

let consumer = Consumer::builder(connection, "api_calls")
    .with_retry(custom_retry)
    .build()
    .await?;

consumer.consume(|msg: rust_rabbit::Message<ApiRequest>| async move {
    match external_api_call(&request).await {
        Ok(response) => {
            store_response(response).await?;
            Ok(())
        }
        Err(ApiError::RateLimit) => {
            log::info!("Rate limited, backing off");
            Err("Rate limit exceeded".into())  // Will retry with custom delays
        }
        Err(ApiError::ServerError) => {
            log::warn!("Server error, will retry");
            Err("Server error".into())  // Will retry
        }
        Err(ApiError::BadRequest) => {
            log::error!("Bad request, not retrying");
            Ok(())  // Don't retry client errors
        }
    }
}).await?;
```

## Dead Letter Queue Handling

### Monitoring Failed Messages

```rust
// Consumer for processing dead letter queue
let dlq_consumer = Consumer::builder(connection, "orders.dlq")
    .manual_declare()  // DLQ created by retry system
    .build()
    .await?;

dlq_consumer.consume(|msg: rust_rabbit::Message<FailedOrder>| async move {
    log::error!("Processing failed order from DLQ: {:?}", failed_order);
    
    // Extract failure information from headers
    let headers = failed_order.delivery.properties.headers();
    if let Some(headers) = headers {
        if let Some(reason) = headers.get("x-failure-reason") {
            log::error!("Failure reason: {:?}", reason);
        }
        if let Some(failed_at) = headers.get("x-failed-at") {
            log::error!("Failed at: {:?}", failed_at);
        }
    }
    
    // Send to monitoring system
    send_dlq_alert(&failed_order).await?;
    
    // Optionally requeue to original queue after manual investigation
    // requeue_message(&failed_order).await?;
    
    Ok(())
}).await?;
```

### DLQ Recovery Strategies

```rust
// Requeue messages from DLQ after fixing issues
async fn requeue_from_dlq(
    connection: Arc<Connection>,
    dlq_name: &str,
    target_queue: &str,
) -> Result<u32, RustRabbitError> {
    let dlq_consumer = Consumer::builder(connection.clone(), dlq_name)
        .manual_declare()
        .build()
        .await?;
    
    let publisher = Publisher::new(connection);
    let mut requeued_count = 0;
    
    dlq_consumer.consume(|message: serde_json::Value| async move {
        // Remove retry headers and requeue
        let options = PublishOptions::new().persistent(true);
        
        match publisher.publish_to_queue(target_queue, &message, Some(options)).await {
            Ok(_) => {
                requeued_count += 1;
                log::info!("Requeued message {} to {}", requeued_count, target_queue);
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to requeue message: {}", e);
                Err(e.into())
            }
        }
    }).await?;
    
    Ok(requeued_count)
}
```

## Error Logging and Monitoring

### Structured Logging

```rust
use tracing::{error, warn, info, instrument};

#[instrument(skip(order))]
async fn process_order(order: Order) -> Result<(), ProcessingError> {
    info!(order_id = msg.data.id, "Processing order");
    
    match validate_order(&msg.data).await {
        Ok(_) => info!(order_id = msg.data.id, "Order validated"),
        Err(e) => {
            error!(
                order_id = msg.data.id,
                error = %e,
                "Order validation failed"
            );
            return Err(ProcessingError::Permanent(e.to_string()));
        }
    }
    
    match charge_payment(&msg.data).await {
        Ok(_) => info!(order_id = msg.data.id, "Payment charged"),
        Err(PaymentError::NetworkError(e)) => {
            warn!(
                order_id = msg.data.id,
                error = %e,
                "Payment network error, will retry"
            );
            return Err(ProcessingError::Transient(e.to_string()));
        }
        Err(PaymentError::InsufficientFunds) => {
            error!(order_id = msg.data.id, "Insufficient funds");
            return Err(ProcessingError::Permanent("Insufficient funds".to_string()));
        }
    }
    
    Ok(())
}
```

### Error Metrics

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct ErrorMetrics {
    connection_errors: Arc<AtomicU64>,
    serialization_errors: Arc<AtomicU64>,
    processing_errors: Arc<AtomicU64>,
    retry_count: Arc<AtomicU64>,
}

impl ErrorMetrics {
    fn new() -> Self {
        Self {
            connection_errors: Arc::new(AtomicU64::new(0)),
            serialization_errors: Arc::new(AtomicU64::new(0)),
            processing_errors: Arc::new(AtomicU64::new(0)),
            retry_count: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn record_error(&self, error: &RustRabbitError) {
        match error {
            RustRabbitError::Connection(_) => {
                self.connection_errors.fetch_add(1, Ordering::Relaxed);
            }
            RustRabbitError::Serialization(_) => {
                self.serialization_errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.processing_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    
    fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }
}

// Use in consumer
let metrics = ErrorMetrics::new();
let metrics_clone = metrics.clone();

consumer.consume(move |message: Message| {
    let metrics = metrics_clone.clone();
    async move {
        match process_message(message).await {
            Ok(_) => Ok(()),
            Err(e) => {
                metrics.record_error(&e);
                if e.is_retryable() {
                    metrics.record_retry();
                }
                Err(e)
            }
        }
    }
}).await?;
```

## Best Practices

### 1. Error Classification

```rust
// Always classify errors appropriately
match result {
    Err(DatabaseError::ConnectionLost) => Err("Transient DB error".into()), // Retry
    Err(DatabaseError::ConstraintViolation) => Ok(()), // Don't retry, ACK
    Err(ValidationError::InvalidFormat) => Ok(()), // Don't retry bad data
    Err(ExternalApiError::RateLimit) => Err("Rate limited".into()), // Retry
    Ok(value) => Ok(()),
}
```

### 2. Timeout Handling

```rust
use tokio::time::{timeout, Duration};

async fn process_with_timeout(message: Message) -> Result<(), Box<dyn std::error::Error>> {
    match timeout(Duration::from_secs(30), process_message(message)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),  // Processing error
        Err(_) => Err("Processing timeout".into()),  // Timeout error
    }
}
```

### 3. Circuit Breaker Pattern

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct CircuitBreaker {
    failure_count: Arc<RwLock<u32>>,
    last_failure: Arc<RwLock<Option<Instant>>>,
    threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, timeout: Duration) -> Self {
        Self {
            failure_count: Arc::new(RwLock::new(0)),
            last_failure: Arc::new(RwLock::new(None)),
            threshold,
            timeout,
        }
    }
    
    async fn is_open(&self) -> bool {
        let count = *self.failure_count.read().await;
        let last = *self.last_failure.read().await;
        
        count >= self.threshold && 
        last.map(|t| t.elapsed() < self.timeout).unwrap_or(false)
    }
    
    async fn record_success(&self) {
        *self.failure_count.write().await = 0;
        *self.last_failure.write().await = None;
    }
    
    async fn record_failure(&self) {
        *self.failure_count.write().await += 1;
        *self.last_failure.write().await = Some(Instant::now());
    }
}

// Use in consumer
let circuit_breaker = CircuitBreaker::new(5, Duration::from_secs(60));

consumer.consume(move |message: Message| {
    let cb = circuit_breaker.clone();
    async move {
        if cb.is_open().await {
            log::warn!("Circuit breaker is open, skipping message");
            return Ok(()); // ACK but don't process
        }
        
        match process_message(message).await {
            Ok(_) => {
                cb.record_success().await;
                Ok(())
            }
            Err(e) => {
                cb.record_failure().await;
                Err(e)
            }
        }
    }
}).await?;
```

## Troubleshooting Common Issues

### 1. Messages Not Being Retried

```rust
// Check error return - must return Err() to trigger retry
consumer.consume(|msg: rust_rabbit::Message<Message>| async move {
    match process(message).await {
        Ok(_) => Ok(()),        // ✅ ACK message
        Err(e) => Err(e.into()) // ✅ Trigger retry
        // DON'T: Ok(())         // ❌ Would ACK on error
    }
}).await?;
```

### 2. Infinite Retry Loops

```rust
// Classify errors to avoid infinite loops
match error {
    ValidationError(_) => Ok(()), // ✅ Don't retry bad data
    NetworkError(_) => Err(error.into()), // ✅ Retry network issues
}
```

### 3. Memory Leaks from Error Objects

```rust
// Use appropriate error types, avoid holding large objects
#[derive(Debug)]
enum ProcessingError {
    Network(String),     // ✅ Just error message
    Validation(String),  // ✅ Lightweight
    // DON'T: Database(Database), // ❌ Would hold database connection
}
```

For more information, see:
- [Retry Configuration Guide](retry-guide.md)
- [Queue Management](queues-exchanges.md)
- [Best Practices](best-practices.md)

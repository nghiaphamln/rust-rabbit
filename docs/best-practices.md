# Best Practices Guide

This guide covers production best practices for using rust-rabbit effectively, including performance optimization, reliability patterns, and operational considerations.

## Table of Contents

- [Architecture Patterns](#architecture-patterns)
- [Performance Optimization](#performance-optimization)
- [Reliability Patterns](#reliability-patterns)
- [Security](#security)
- [Monitoring and Observability](#monitoring-and-observability)
- [Deployment](#deployment)
- [Testing](#testing)

## Architecture Patterns

### Shared Connection

Reuse connections across application:

```rust
use rust_rabbit::{Connection, Publisher};
use std::sync::Arc;

#[derive(Clone)]
struct MessageBus {
    connection: Arc<Connection>,
    publisher: Publisher,
}

impl MessageBus {
    async fn new(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = Connection::new(url).await?;
        let publisher = Publisher::new(connection.clone());
        
        Ok(Self {
            connection,
            publisher,
        })
    }
    
    async fn publish_event<T>(&self, event_type: &str, event: &T) 
        -> Result<(), Box<dyn std::error::Error>>
    where
        T: serde::Serialize,
    {
        self.publisher.publish_to_exchange(
            "domain.events",
            event_type,
            event,
            None
        ).await?;
        Ok(())
    }
}
```

### Service-to-Service Communication

Structure for microservice messaging:

```rust
// Event publishing
async fn publish_order_created(bus: &MessageBus, order: &Order) -> Result<()> {
    bus.publish_event("order.created", order).await
}

// Command sending
async fn send_command<T>(bus: &MessageBus, service: &str, command: &T) -> Result<()>
where
    T: serde::Serialize,
{
    let queue = format!("{}.commands", service);
    bus.publisher.publish_to_queue(&queue, command, None).await
}

// Event consumption
async fn start_consumer(bus: &MessageBus) -> Result<()> {
    let consumer = Consumer::builder(bus.connection.clone(), "order_service")
        .bind_to_exchange("domain.events", "order.#")
        .with_retry(RetryConfig::exponential_default())
        .with_prefetch(10)
        .build();
    
    consumer.consume(|msg: OrderEvent| async move {
        handle_order_event(msg).await
    }).await
}
```

## Performance Optimization

### Prefetch Configuration

Balance throughput and fairness:

```rust
// Low throughput, fair distribution
let consumer = Consumer::builder(connection, "orders")
    .with_prefetch(1)
    .build();

// Medium throughput, balanced
let consumer = Consumer::builder(connection, "orders")
    .with_prefetch(10)
    .build();

// High throughput, may cause uneven distribution
let consumer = Consumer::builder(connection, "orders")
    .with_prefetch(100)
    .build();
```

Guidelines:
- Fast processing: Higher prefetch (50-100)
- Slow processing: Lower prefetch (1-10)
- CPU-bound: Match CPU core count
- IO-bound: Higher prefetch (20-50)

### Connection Pooling

For high-volume publishing:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

struct PublisherPool {
    publishers: Arc<RwLock<Vec<Publisher>>>,
    current: Arc<RwLock<usize>>,
}

impl PublisherPool {
    async fn new(connection: Connection, size: usize) -> Self {
        let publishers = (0..size)
            .map(|_| Publisher::new(connection.clone()))
            .collect();
        
        Self {
            publishers: Arc::new(RwLock::new(publishers)),
            current: Arc::new(RwLock::new(0)),
        }
    }
    
    async fn publish<T>(&self, queue: &str, message: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        let publishers = self.publishers.read().await;
        let mut current = self.current.write().await;
        
        let publisher = &publishers[*current];
        *current = (*current + 1) % publishers.len();
        
        publisher.publish_to_queue(queue, message, None).await
    }
}
```

### Batch Processing

Process messages in batches:

```rust
use tokio::sync::mpsc;

async fn batch_processor(connection: Connection) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(1000);
    
    // Consumer sends to channel
    let consumer = Consumer::builder(connection, "orders")
        .with_prefetch(50)
        .build();
    
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        consumer.consume(move |msg: Order| {
            let tx = tx_clone.clone();
            async move {
                tx.send(msg).await.ok();
                Ok(())
            }
        }).await
    });
    
    // Batch processor
    let mut batch = Vec::with_capacity(100);
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                batch.push(msg);
                if batch.len() >= 100 {
                    process_batch(&batch).await?;
                    batch.clear();
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if !batch.is_empty() {
                    process_batch(&batch).await?;
                    batch.clear();
                }
            }
        }
    }
}
```

## Reliability Patterns

### Graceful Shutdown

Handle shutdown signals properly:

```rust
use tokio::signal;

async fn run_with_shutdown(connection: Connection) -> Result<()> {
    let consumer = Consumer::builder(connection, "orders")
        .with_retry(RetryConfig::exponential_default())
        .build();
    
    tokio::select! {
        result = consumer.consume(|msg: Order| async move {
            process_order(msg).await
        }) => {
            result?;
        }
        _ = signal::ctrl_c() => {
            log::info!("Shutdown signal received");
        }
    }
    
    log::info!("Shutting down gracefully");
    Ok(())
}
```

### Health Checks

Implement health check endpoint:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

struct HealthCheck {
    last_message_time: Arc<RwLock<std::time::Instant>>,
}

impl HealthCheck {
    async fn update(&self) {
        *self.last_message_time.write().await = std::time::Instant::now();
    }
    
    async fn is_healthy(&self, threshold: Duration) -> bool {
        let last = *self.last_message_time.read().await;
        last.elapsed() < threshold
    }
}

// In consumer
consumer.consume(|msg: Order| async move {
    health_check.update().await;
    process_order(msg).await
}).await?;
```

### Idempotency

Ensure operations can be retried safely:

```rust
use std::collections::HashSet;
use tokio::sync::RwLock;

struct IdempotencyTracker {
    processed: Arc<RwLock<HashSet<String>>>,
}

impl IdempotencyTracker {
    async fn is_processed(&self, id: &str) -> bool {
        self.processed.read().await.contains(id)
    }
    
    async fn mark_processed(&self, id: String) {
        self.processed.write().await.insert(id);
    }
}

// In consumer
consumer.consume(|msg: Order| async move {
    if tracker.is_processed(&msg.id).await {
        return Ok(()); // Skip already processed
    }
    
    process_order(&msg).await?;
    tracker.mark_processed(msg.id.clone()).await;
    
    Ok(())
}).await?;
```

## Security

### Connection Security

Use secure connections in production:

```rust
// Use TLS
let connection = Connection::new("amqps://user:pass@host:5671").await?;

// Load credentials from environment
let user = std::env::var("RABBITMQ_USER")?;
let pass = std::env::var("RABBITMQ_PASS")?;
let host = std::env::var("RABBITMQ_HOST")?;
let url = format!("amqps://{}:{}@{}:5671", user, pass, host);
let connection = Connection::new(&url).await?;
```

### Message Validation

Validate messages before processing:

```rust
use validator::Validate;

#[derive(Deserialize, Validate)]
struct Order {
    #[validate(range(min = 1))]
    id: u32,
    
    #[validate(range(min = 0.01))]
    amount: f64,
}

consumer.consume(|msg: Order| async move {
    // Validate message
    msg.validate()
        .map_err(|e| format!("Validation failed: {}", e))?;
    
    process_order(msg).await
}).await?;
```

## Monitoring and Observability

### Structured Logging

Use structured logging for better insights:

```rust
use tracing::{info, warn, error, instrument};

#[instrument(skip(msg))]
async fn process_order(msg: Order) -> Result<()> {
    info!(
        order_id = msg.id,
        amount = msg.amount,
        "Processing order"
    );
    
    match save_order(&msg).await {
        Ok(_) => {
            info!(order_id = msg.id, "Order saved successfully");
            Ok(())
        }
        Err(e) => {
            error!(
                order_id = msg.id,
                error = %e,
                "Failed to save order"
            );
            Err(e)
        }
    }
}
```

### Metrics Collection

Track key metrics:

```rust
struct Metrics {
    messages_processed: Arc<AtomicU64>,
    messages_failed: Arc<AtomicU64>,
    processing_time: Arc<RwLock<Vec<Duration>>>,
}

impl Metrics {
    async fn record_success(&self, duration: Duration) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.processing_time.write().await.push(duration);
    }
    
    async fn record_failure(&self) {
        self.messages_failed.fetch_add(1, Ordering::Relaxed);
    }
    
    async fn average_processing_time(&self) -> Duration {
        let times = self.processing_time.read().await;
        let sum: Duration = times.iter().sum();
        sum / times.len() as u32
    }
}
```

## Deployment

### Configuration Management

Externalize configuration:

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    rabbitmq_url: String,
    queue_name: String,
    prefetch_count: u16,
    retry_max_attempts: u32,
    retry_base_delay_ms: u64,
}

async fn load_config() -> Result<Config> {
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "config.toml".to_string());
    
    let content = tokio::fs::read_to_string(config_path).await?;
    let config: Config = toml::from_str(&content)?;
    
    Ok(config)
}
```

### Environment-Specific Settings

```rust
// config.production.toml
rabbitmq_url = "amqps://prod-host:5671"
prefetch_count = 100
retry_max_attempts = 5

// config.development.toml
rabbitmq_url = "amqp://localhost:5672"
prefetch_count = 1
retry_max_attempts = 3
```

## Testing

### Unit Testing

Test message handlers:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_order_processing() {
        let order = Order {
            id: 123,
            amount: 99.99,
        };
        
        let result = process_order(order).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_invalid_order() {
        let order = Order {
            id: 0,
            amount: -10.0,
        };
        
        let result = process_order(order).await;
        assert!(result.is_err());
    }
}
```

### Integration Testing

Test with real RabbitMQ:

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_end_to_end() {
        // Setup
        let connection = Connection::new("amqp://localhost:5672").await.unwrap();
        let publisher = Publisher::new(connection.clone());
        
        // Publish
        let order = Order { id: 123, amount: 99.99 };
        publisher.publish_to_queue("test_queue", &order, None).await.unwrap();
        
        // Consume
        let consumer = Consumer::builder(connection, "test_queue")
            .build();
        
        let received = Arc::new(RwLock::new(None));
        let received_clone = received.clone();
        
        tokio::spawn(async move {
            consumer.consume(move |msg: Order| {
                let received = received_clone.clone();
                async move {
                    *received.write().await = Some(msg);
                    Ok(())
                }
            }).await
        });
        
        // Wait and verify
        tokio::time::sleep(Duration::from_secs(1)).await;
        let msg = received.read().await;
        assert!(msg.is_some());
        assert_eq!(msg.as_ref().unwrap().id, 123);
    }
}
```

## Summary

Key takeaways:

1. Reuse connections across application
2. Configure appropriate prefetch based on workload
3. Implement graceful shutdown
4. Use structured logging and metrics
5. Validate all input messages
6. Ensure operations are idempotent
7. Externalize configuration
8. Test with real RabbitMQ
9. Monitor DLQ and retry metrics
10. Use TLS in production

## See Also

- [Retry Configuration Guide](retry-guide.md) - Configure retry behavior
- [Error Handling Guide](error-handling.md) - Error handling strategies
- [Queues and Exchanges Guide](queues-exchanges.md) - Queue and exchange configuration

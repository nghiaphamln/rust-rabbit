# Best Practices Guide

This guide covers production best practices for using rust-rabbit effectively, including performance optimization, reliability patterns, and operational considerations.

## Architecture Patterns

### 1. Microservice Communication

```rust
use rust_rabbit::{Connection, Publisher, Consumer, RetryConfig};
use std::sync::Arc;

// Shared connection across services
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
    
    // Service-to-service events
    async fn publish_event<T>(&self, event_type: &str, event: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: serde::Serialize,
    {
        let exchange = "domain.events";
        self.publisher.publish_to_exchange(exchange, event_type, event, None).await?;
        Ok(())
    }
    
    // Direct service commands
    async fn send_command<T>(&self, service: &str, command: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: serde::Serialize,
    {
        let queue = format!("{}.commands", service);
        self.publisher.publish_to_queue(&queue, command, None).await?;
        Ok(())
    }
}

// Usage in order service
async fn order_service_example() -> Result<(), Box<dyn std::error::Error>> {
    let bus = MessageBus::new("amqp://localhost:5672").await?;
    
    // Listen for commands
    let consumer = Consumer::builder(bus.connection.clone(), "order.commands")
        .with_retry(RetryConfig::exponential_default())
        .concurrency(10)
        .build();
    
    consumer.consume(|msg: rust_rabbit::Message<OrderCommand>| async move {
        match msg.data {
            OrderCommand::Create(order) => {
                let result = create_order(order).await?;
                
                // Publish event for other services
                bus.publish_event("order.created", &result).await?;
                Ok(())
            }
            OrderCommand::Cancel(order_id) => {
                cancel_order(order_id).await?;
                bus.publish_event("order.cancelled", &msg.data_id).await?;
                Ok(())
            }
        }
    }).await?;
    
    Ok(())
}
```

### 2. Event-Driven Architecture

```rust
// Domain events structure
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum DomainEvent {
    UserRegistered { user_id: String, email: String, timestamp: u64 },
    OrderPlaced { order_id: String, user_id: String, amount: f64 },
    PaymentProcessed { payment_id: String, order_id: String, status: String },
}

// Event publisher
struct EventPublisher {
    publisher: Publisher,
}

impl EventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), Box<dyn std::error::Error>> {
        let event_type = match event {
            DomainEvent::UserRegistered { .. } => "user.registered",
            DomainEvent::OrderPlaced { .. } => "order.placed", 
            DomainEvent::PaymentProcessed { .. } => "payment.processed",
        };
        
        let options = PublishOptions::new()
            .persistent(true)
            .header("event_type", event_type)
            .header("timestamp", &chrono::Utc::now().timestamp().to_string());
        
        self.publisher.publish_to_exchange("domain.events", event_type, event, Some(options)).await?;
        Ok(())
    }
}

// Event handlers
async fn setup_event_handlers(connection: Arc<Connection>) -> Result<(), Box<dyn std::error::Error>> {
    // Email service handles user events
    let email_consumer = Consumer::builder(connection.clone(), "email.service")
        .bind_to_exchange("domain.events")
        .routing_key("user.*")
        .with_retry(RetryConfig::linear(3, Duration::from_secs(30)))
        .build();
    
    tokio::spawn(async move {
        email_consumer.consume(|msg: rust_rabbit::Message<DomainEvent>| async move {
            match msg.data {
                DomainEvent::UserRegistered { email, .. } => {
                    send_welcome_email(&email).await?;
                    Ok(())
                }
                _ => Ok(()), // Ignore other events
            }
        }).await
    });
    
    // Analytics service handles all events
    let analytics_consumer = Consumer::builder(connection.clone(), "analytics.service")
        .bind_to_exchange("domain.events")
        .routing_key("#") // All events
        .with_retry(RetryConfig::exponential_default())
        .concurrency(20)
        .build();
    
    tokio::spawn(async move {
        analytics_consumer.consume(|msg: rust_rabbit::Message<DomainEvent>| async move {
            store_event_for_analytics(&msg.data).await?;
            Ok(())
        }).await
    });
    
    Ok(())
}
```

## Performance Optimization

### 1. Connection Management

```rust
use std::sync::Arc;
use tokio::sync::OnceCell;

// Singleton connection pattern
static CONNECTION: OnceCell<Arc<Connection>> = OnceCell::const_new();

async fn get_connection() -> &'static Arc<Connection> {
    CONNECTION.get_or_init(|| async {
        Arc::new(
            Connection::new("amqp://localhost:5672")
                .await
                .expect("Failed to connect to RabbitMQ")
        )
    }).await
}

// Use across application
async fn publish_message<T: serde::Serialize>(message: &T) -> Result<(), Box<dyn std::error::Error>> {
    let connection = get_connection().await;
    let publisher = Publisher::new(connection.clone());
    publisher.publish_to_queue("default", message, None).await?;
    Ok(())
}
```

### 2. Batch Processing

```rust
use tokio::time::{interval, Duration};
use std::collections::VecDeque;
use tokio::sync::Mutex;

struct BatchProcessor<T> {
    buffer: Arc<Mutex<VecDeque<T>>>,
    batch_size: usize,
    flush_interval: Duration,
    publisher: Publisher,
}

impl<T> BatchProcessor<T> 
where 
    T: serde::Serialize + Send + 'static,
{
    fn new(publisher: Publisher, batch_size: usize, flush_interval: Duration) -> Self {
        let processor = Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            batch_size,
            flush_interval,
            publisher,
        };
        
        // Start flush timer
        let buffer_clone = processor.buffer.clone();
        let publisher_clone = processor.publisher.clone();
        let batch_size = processor.batch_size;
        
        tokio::spawn(async move {
            let mut interval = interval(flush_interval);
            loop {
                interval.tick().await;
                Self::flush_buffer(&buffer_clone, &publisher_clone, batch_size).await;
            }
        });
        
        processor
    }
    
    async fn add(&self, item: T) {
        let mut buffer = self.buffer.lock().await;
        buffer.push_back(item);
        
        if buffer.len() >= self.batch_size {
            drop(buffer); // Release lock
            Self::flush_buffer(&self.buffer, &self.publisher, self.batch_size).await;
        }
    }
    
    async fn flush_buffer(
        buffer: &Arc<Mutex<VecDeque<T>>>,
        publisher: &Publisher,
        batch_size: usize,
    ) {
        let mut buffer = buffer.lock().await;
        if buffer.is_empty() {
            return;
        }
        
        let batch: Vec<T> = buffer.drain(..std::cmp::min(batch_size, buffer.len())).collect();
        drop(buffer); // Release lock early
        
        if let Err(e) = publisher.publish_to_queue("batch_queue", &batch, None).await {
            log::error!("Failed to publish batch: {}", e);
            // Could implement retry logic here
        }
    }
}

// Usage
let batch_processor = BatchProcessor::new(publisher, 100, Duration::from_secs(5));

// Add items (they'll be batched automatically)
for item in items {
    batch_processor.add(item).await;
}
```

### 3. High-Throughput Consumer

```rust
use tokio::sync::Semaphore;
use std::sync::Arc;

struct HighThroughputConsumer {
    consumer: Consumer,
    semaphore: Arc<Semaphore>,
}

impl HighThroughputConsumer {
    async fn new(connection: Arc<Connection>, queue: &str, concurrency: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let consumer = Consumer::builder(connection, queue)
            .concurrency(concurrency)
            .with_retry(RetryConfig::exponential(3, Duration::from_millis(100), Duration::from_secs(10)))
            .build()
            .await?;
        
        Ok(Self {
            consumer,
            semaphore: Arc::new(Semaphore::new(concurrency * 2)), // Allow some queuing
        })
    }
    
    async fn start<T, F, Fut>(&self, handler: F) -> Result<(), Box<dyn std::error::Error>>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let semaphore = self.semaphore.clone();
        
        self.consumer.consume(move |message: T| {
            let handler = handler.clone();
            let semaphore = semaphore.clone();
            
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                handler(message).await
            }
        }).await?;
        
        Ok(())
    }
}

// Usage
let high_throughput_consumer = HighThroughputConsumer::new(connection, "high_volume_queue", 50).await?;

high_throughput_consumer.start(|message: HighVolumeMessage| async move {
    process_high_volume_message(message).await
}).await?;
```

## Reliability Patterns

### 1. Idempotent Message Processing

```rust
use std::collections::HashSet;
use tokio::sync::RwLock;

// Simple in-memory deduplication (use Redis in production)
struct MessageDeduplicator {
    processed_ids: Arc<RwLock<HashSet<String>>>,
}

impl MessageDeduplicator {
    fn new() -> Self {
        Self {
            processed_ids: Arc::new(RwLock::new(HashSet::new())),
        }
    }
    
    async fn is_duplicate(&self, message_id: &str) -> bool {
        let processed = self.processed_ids.read().await;
        processed.contains(message_id)
    }
    
    async fn mark_processed(&self, message_id: String) {
        let mut processed = self.processed_ids.write().await;
        processed.insert(message_id);
        
        // Cleanup old entries periodically (simplified)
        if processed.len() > 10000 {
            processed.clear(); // In production, use LRU or TTL
        }
    }
}

// Idempotent consumer
let deduplicator = MessageDeduplicator::new();

consumer.consume(move |message: OrderMessage| {
    let deduplicator = deduplicator.clone();
    async move {
        let message_id = message.id.clone();
        
        if deduplicator.is_duplicate(&message_id).await {
            log::info!("Duplicate message {}, skipping", message_id);
            return Ok(()); // ACK duplicate
        }
        
        match process_order(message).await {
            Ok(_) => {
                deduplicator.mark_processed(message_id).await;
                Ok(())
            }
            Err(e) => Err(e), // Will retry, don't mark as processed
        }
    }
}).await?;
```

### 2. Graceful Shutdown

```rust
use tokio::signal;
use std::sync::atomic::{AtomicBool, Ordering};

struct GracefulService {
    connection: Arc<Connection>,
    running: Arc<AtomicBool>,
}

impl GracefulService {
    async fn new(connection_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = Connection::new(connection_url).await?;
        
        Ok(Self {
            connection,
            running: Arc::new(AtomicBool::new(true)),
        })
    }
    
    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let running = self.running.clone();
        
        // Spawn shutdown handler
        tokio::spawn(async move {
            signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
            log::info!("Shutdown signal received, stopping gracefully...");
            running.store(false, Ordering::SeqCst);
        });
        
        // Start consumers with shutdown check
        let consumer = Consumer::builder(self.connection.clone(), "orders")
            .with_retry(RetryConfig::exponential_default())
            .build()
            .await?;
        
        let running_clone = self.running.clone();
        
        tokio::select! {
            result = consumer.consume(|msg: rust_rabbit::Message<OrderMessage>| async move {
                // Check shutdown flag before processing
                if !running_clone.load(Ordering::SeqCst) {
                    log::info!("Shutdown in progress, skipping message processing");
                    return Ok(());
                }
                
                process_order(message).await
            }) => {
                result?;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if !self.running.load(Ordering::SeqCst) {
                    log::info!("Service stopped gracefully");
                    return Ok(());
                }
            }
        }
        
        Ok(())
    }
    
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for in-flight messages to complete
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // Close connection
        self.connection.close().await?;
        
        log::info!("Service shutdown complete");
        Ok(())
    }
}
```

### 3. Health Checks

```rust
use serde::Serialize;

#[derive(Serialize)]
struct HealthStatus {
    status: String,
    connection: String,
    last_message: Option<u64>,
    error_rate: f64,
}

struct HealthMonitor {
    connection: Arc<Connection>,
    last_message_time: Arc<RwLock<Option<std::time::Instant>>>,
    error_count: Arc<AtomicU64>,
    success_count: Arc<AtomicU64>,
}

impl HealthMonitor {
    fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            last_message_time: Arc::new(RwLock::new(None)),
            error_count: Arc::new(AtomicU64::new(0)),
            success_count: Arc::new(AtomicU64::new(0)),
        }
    }
    
    async fn check_health(&self) -> HealthStatus {
        let connection_status = if self.connection.is_connected().await {
            "connected".to_string()
        } else {
            "disconnected".to_string()
        };
        
        let last_message = self.last_message_time.read().await
            .map(|t| t.elapsed().as_secs());
        
        let errors = self.error_count.load(Ordering::SeqCst);
        let successes = self.success_count.load(Ordering::SeqCst);
        let total = errors + successes;
        let error_rate = if total > 0 { errors as f64 / total as f64 } else { 0.0 };
        
        let status = if connection_status == "connected" && error_rate < 0.1 {
            "healthy".to_string()
        } else {
            "unhealthy".to_string()
        };
        
        HealthStatus {
            status,
            connection: connection_status,
            last_message,
            error_rate,
        }
    }
    
    fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::SeqCst);
        let mut last_message = self.last_message_time.blocking_write();
        *last_message = Some(std::time::Instant::now());
    }
    
    fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::SeqCst);
    }
}

// Use in consumer
let health_monitor = HealthMonitor::new(connection.clone());
let monitor_clone = health_monitor.clone();

consumer.consume(move |message: Message| {
    let monitor = monitor_clone.clone();
    async move {
        match process_message(message).await {
            Ok(_) => {
                monitor.record_success();
                Ok(())
            }
            Err(e) => {
                monitor.record_error();
                Err(e)
            }
        }
    }
}).await?;

// Health check endpoint (for HTTP health checks)
async fn health_endpoint(monitor: Arc<HealthMonitor>) -> impl warp::Reply {
    let health = monitor.check_health().await;
    warp::reply::json(&health)
}
```

## Security Best Practices

### 1. Connection Security

```rust
// Use TLS for production connections
let connection = Connection::new("amqps://user:pass@localhost:5671").await?;

// Or with custom TLS configuration
let config = ConnectionConfig::new("amqps://localhost:5671")
    .connection_timeout(30)
    .heartbeat(60);

let connection = Connection::with_config(config).await?;
```

### 2. Message Encryption

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct EncryptedMessage {
    encrypted_data: String,
    nonce: String,
}

struct MessageEncryption {
    key: [u8; 32], // In production, use proper key management
}

impl MessageEncryption {
    fn encrypt<T: Serialize>(&self, message: &T) -> Result<EncryptedMessage, Box<dyn std::error::Error>> {
        let json = serde_json::to_string(message)?;
        // Implement actual encryption here (e.g., using ring or age crates)
        Ok(EncryptedMessage {
            encrypted_data: base64::encode(&json), // Placeholder
            nonce: "nonce".to_string(), // Placeholder
        })
    }
    
    fn decrypt<T: serde::de::DeserializeOwned>(&self, message: &EncryptedMessage) -> Result<T, Box<dyn std::error::Error>> {
        // Implement actual decryption here
        let json = base64::decode(&message.encrypted_data)?; // Placeholder
        let message: T = serde_json::from_slice(&json)?;
        Ok(message)
    }
}

// Usage
let encryption = MessageEncryption { key: [0u8; 32] }; // Use proper key

// Publisher
let sensitive_data = SensitiveData { ssn: "123-45-6789".to_string() };
let encrypted = encryption.encrypt(&sensitive_data)?;
publisher.publish_to_queue("secure_queue", &encrypted, None).await?;

// Consumer
consumer.consume(move |encrypted_msg: EncryptedMessage| async move {
    let decrypted: SensitiveData = encryption.decrypt(&encrypted_msg)?;
    process_sensitive_data(decrypted).await
}).await?;
```

## Testing Strategies

### 1. Unit Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;
    
    #[tokio::test]
    async fn test_message_processing() {
        let test_message = OrderMessage {
            id: "test-123".to_string(),
            amount: 99.99,
        };
        
        let result = process_order(test_message).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_error_handling() {
        let invalid_message = OrderMessage {
            id: "invalid".to_string(),
            amount: -1.0, // Invalid amount
        };
        
        let result = process_order(invalid_message).await;
        assert!(result.is_err());
    }
}
```

### 2. Integration Testing

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use testcontainers::*;
    
    async fn setup_test_rabbitmq() -> (Container<RabbitMq>, String) {
        let docker = clients::Cli::default();
        let rabbitmq_container = docker.run(images::rabbitmq::RabbitMq::default());
        let connection_url = format!("amqp://localhost:{}", rabbitmq_container.get_host_port(5672));
        (rabbitmq_container, connection_url)
    }
    
    #[tokio::test]
    async fn test_end_to_end_message_flow() {
        let (_container, url) = setup_test_rabbitmq().await;
        let connection = Connection::new(&url).await.unwrap();
        let publisher = Publisher::new(connection.clone());
        
        // Send test message
        let test_message = TestMessage { id: 1, content: "test".to_string() };
        publisher.publish_to_queue("test_queue", &test_message, None).await.unwrap();
        
        // Verify message received
        let consumer = Consumer::builder(connection, "test_queue")
            .build()
            .await
            .unwrap();
        
        let mut received = false;
        let timeout = tokio::time::timeout(Duration::from_secs(5), async {
            consumer.consume(|msg: rust_rabbit::Message<TestMessage>| async move {
                assert_eq!(msg.id, 1);
                assert_eq!(msg.content, "test");
                received = true;
                Ok(())
            }).await
        });
        
        timeout.await.unwrap().unwrap();
        assert!(received);
    }
}
```

## Monitoring and Observability

### 1. Metrics Collection

```rust
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct MessageMetrics {
    messages_published: Arc<AtomicU64>,
    messages_consumed: Arc<AtomicU64>,
    processing_time_total: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
}

impl MessageMetrics {
    fn new() -> Self {
        Self {
            messages_published: Arc::new(AtomicU64::new(0)),
            messages_consumed: Arc::new(AtomicU64::new(0)),
            processing_time_total: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn record_publish(&self) {
        self.messages_published.fetch_add(1, Ordering::SeqCst);
    }
    
    fn record_consume(&self, processing_time: Duration) {
        self.messages_consumed.fetch_add(1, Ordering::SeqCst);
        self.processing_time_total.fetch_add(processing_time.as_millis() as u64, Ordering::SeqCst);
    }
    
    fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::SeqCst);
    }
    
    fn get_stats(&self) -> (u64, u64, f64, u64) {
        let published = self.messages_published.load(Ordering::SeqCst);
        let consumed = self.messages_consumed.load(Ordering::SeqCst);
        let total_time = self.processing_time_total.load(Ordering::SeqCst);
        let errors = self.errors_total.load(Ordering::SeqCst);
        
        let avg_processing_time = if consumed > 0 {
            total_time as f64 / consumed as f64
        } else {
            0.0
        };
        
        (published, consumed, avg_processing_time, errors)
    }
}
```

### 2. Structured Logging

```rust
use tracing::{info, warn, error, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Initialize structured logging
fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[instrument(skip(message))]
async fn process_order_with_logging(message: OrderMessage) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    
    info!(
        order_id = %message.id,
        amount = %message.amount,
        "Processing order"
    );
    
    match validate_order(&message).await {
        Ok(_) => info!(order_id = %message.id, "Order validated"),
        Err(e) => {
            error!(
                order_id = %message.id,
                error = %e,
                "Order validation failed"
            );
            return Err(e);
        }
    }
    
    let processing_time = start.elapsed();
    info!(
        order_id = %message.id,
        processing_time_ms = processing_time.as_millis(),
        "Order processed successfully"
    );
    
    Ok(())
}
```

For more information, see:
- [Retry Configuration Guide](retry-guide.md)
- [Error Handling](error-handling.md)
- [Queue Management](queues-exchanges.md)

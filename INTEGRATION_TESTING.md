# Integration Tests với RabbitMQ

Thư viện RustRabbit có thể viết integration test với RabbitMQ thật một cách rất hiệu quả. Dưới đây là hướng dẫn chi tiết:

## 🐳 Setup với Docker

### 1. Docker Compose cho Development

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:3.12-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
      RABBITMQ_DEFAULT_VHOST: test
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  rabbitmq-delayed:
    image: rabbitmq:3.12-management
    ports:
      - "5673:5672"
      - "15673:15672"
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
    volumes:
      - ./plugins:/opt/rabbitmq/plugins
    command: >
      bash -c "
        rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange &&
        rabbitmq-server
      "
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
```

### 2. RabbitMQ Configuration Files

```ini
# rabbitmq.conf
loopback_users.guest = false
listeners.tcp.default = 5672
management.tcp.port = 15672
management.tcp.ip = 0.0.0.0
```

```
# enabled_plugins
[rabbitmq_management,rabbitmq_delayed_message_exchange].
```

## 🧪 Integration Test Structure

### 1. Test Utilities

```rust
// tests/common/mod.rs
use rust_rabbit::{RustRabbit, RabbitConfig};
use std::time::Duration;
use tokio::time::sleep;
use testcontainers::*;

pub struct TestEnvironment {
    pub rabbit: RustRabbit,
    pub config: RabbitConfig,
}

impl TestEnvironment {
    pub async fn new() -> anyhow::Result<Self> {
        // Wait for RabbitMQ to be ready
        wait_for_rabbitmq().await?;
        
        let config = RabbitConfig::builder()
            .connection_string("amqp://admin:password@localhost:5672/test")
            .retry(|retry| retry.max_retries(3).aggressive())
            .health(|health| health.frequent())
            .pool(|pool| pool.single_connection())
            .build();
            
        let rabbit = RustRabbit::new(config.clone()).await?;
        
        Ok(Self { rabbit, config })
    }
}

async fn wait_for_rabbitmq() -> anyhow::Result<()> {
    let max_attempts = 30;
    let mut attempts = 0;
    
    while attempts < max_attempts {
        match RustRabbit::new(
            RabbitConfig::builder()
                .connection_string("amqp://admin:password@localhost:5672/test")
                .build()
        ).await {
            Ok(_) => return Ok(()),
            Err(_) => {
                attempts += 1;
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    
    Err(anyhow::anyhow!("RabbitMQ not available after {} attempts", max_attempts))
}

pub async fn cleanup_queue(rabbit: &RustRabbit, queue_name: &str) -> anyhow::Result<()> {
    // Implementation để cleanup queue sau mỗi test
    Ok(())
}

pub fn generate_unique_queue_name(prefix: &str) -> String {
    format!("{}_{}", prefix, uuid::Uuid::new_v4())
}
```

### 2. Basic Integration Tests

```rust
// tests/integration_basic.rs
mod common;

use common::*;
use rust_rabbit::{PublishOptions, ConsumerOptions, MessageHandler, MessageResult, MessageContext};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::time::{timeout, Duration};
use async_trait::async_trait;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestMessage {
    id: String,
    content: String,
    number: i32,
}

struct TestMessageHandler {
    received_count: Arc<AtomicUsize>,
    received_messages: Arc<tokio::sync::Mutex<Vec<TestMessage>>>,
}

#[async_trait]
impl MessageHandler<TestMessage> for TestMessageHandler {
    async fn handle(&self, message: TestMessage, _context: MessageContext) -> MessageResult {
        self.received_count.fetch_add(1, Ordering::SeqCst);
        
        let mut received = self.received_messages.lock().await;
        received.push(message);
        
        MessageResult::Ack
    }
}

#[tokio::test]
async fn test_basic_publish_consume() -> anyhow::Result<()> {
    let env = TestEnvironment::new().await?;
    let queue_name = generate_unique_queue_name("test_basic");
    
    // Setup publisher
    let publisher = env.rabbit.publisher();
    
    // Setup consumer
    let handler = Arc::new(TestMessageHandler {
        received_count: Arc::new(AtomicUsize::new(0)),
        received_messages: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    });
    
    let consumer_options = ConsumerOptions::builder(&queue_name)
        .auto_declare_queue()
        .development()
        .build();
        
    let consumer = env.rabbit.consumer(consumer_options).await?;
    
    // Start consuming in background
    let handler_clone = handler.clone();
    let consume_task = tokio::spawn(async move {
        consumer.consume::<TestMessage, TestMessageHandler>(handler_clone).await
    });
    
    // Give consumer time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Publish test messages
    let test_messages = vec![
        TestMessage {
            id: "1".to_string(),
            content: "Hello World".to_string(),
            number: 42,
        },
        TestMessage {
            id: "2".to_string(),
            content: "Test Message".to_string(),
            number: 100,
        },
    ];
    
    let publish_options = PublishOptions::builder()
        .auto_declare_queue()
        .development()
        .build();
    
    for message in &test_messages {
        publisher.publish_to_queue(&queue_name, message, Some(publish_options.clone())).await?;
    }
    
    // Wait for messages to be processed
    let timeout_duration = Duration::from_secs(5);
    timeout(timeout_duration, async {
        loop {
            if handler.received_count.load(Ordering::SeqCst) >= test_messages.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await?;
    
    // Verify results
    assert_eq!(handler.received_count.load(Ordering::SeqCst), test_messages.len());
    
    let received_messages = handler.received_messages.lock().await;
    assert_eq!(received_messages.len(), test_messages.len());
    
    // Verify message content
    for expected in &test_messages {
        assert!(received_messages.iter().any(|msg| msg == expected));
    }
    
    // Cleanup
    consume_task.abort();
    cleanup_queue(&env.rabbit, &queue_name).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_publish_to_exchange() -> anyhow::Result<()> {
    let env = TestEnvironment::new().await?;
    let exchange_name = "test_exchange";
    let queue_name = generate_unique_queue_name("test_exchange_queue");
    
    let publisher = env.rabbit.publisher();
    
    let test_message = TestMessage {
        id: "exchange_test".to_string(),
        content: "Exchange Message".to_string(),
        number: 200,
    };
    
    let publish_options = PublishOptions::builder()
        .auto_declare_exchange()
        .development()
        .build();
    
    publisher.publish_to_exchange(
        exchange_name,
        &queue_name,
        &test_message,
        Some(publish_options)
    ).await?;
    
    Ok(())
}
```

### 3. Retry Mechanism Tests

```rust
// tests/integration_retry.rs
mod common;

use common::*;
use rust_rabbit::{
    retry::{RetryPolicy, DelayedMessageExchange},
    connection::ConnectionManager,
    ConsumerOptions, MessageHandler, MessageResult, MessageContext
};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::time::Duration;
use async_trait::async_trait;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RetryTestMessage {
    id: String,
    should_fail: bool,
    fail_count: usize,
}

struct RetryTestHandler {
    attempt_count: Arc<AtomicUsize>,
    success_count: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageHandler<RetryTestMessage> for RetryTestHandler {
    async fn handle(&self, message: RetryTestMessage, context: MessageContext) -> MessageResult {
        self.attempt_count.fetch_add(1, Ordering::SeqCst);
        
        if message.should_fail && context.retry_count < message.fail_count {
            return MessageResult::Retry;
        }
        
        self.success_count.fetch_add(1, Ordering::SeqCst);
        MessageResult::Ack
    }
}

#[tokio::test]
async fn test_delayed_message_exchange() -> anyhow::Result<()> {
    let env = TestEnvironment::new().await?;
    
    let retry_policy = RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(5),
        backoff_multiplier: 2.0,
        jitter: 0.0,
        ..Default::default()
    };
    
    let connection_manager = ConnectionManager::new(env.config.clone()).await?;
    let delayed_exchange = DelayedMessageExchange::new(
        connection_manager,
        "test_retry_exchange".to_string(),
        retry_policy.clone(),
    );
    
    // Setup infrastructure
    delayed_exchange.setup().await?;
    delayed_exchange.setup_retry_queues("test_retry_queue").await?;
    
    // Test message that should be retried
    let test_message = RetryTestMessage {
        id: "retry_test_1".to_string(),
        should_fail: true,
        fail_count: 2, // Fail first 2 attempts, succeed on 3rd
    };
    
    // Publish with retry
    for attempt in 0..3 {
        delayed_exchange.publish_with_retry(
            "test_retry_queue",
            &test_message,
            attempt,
            None,
        ).await?;
        
        // Small delay between retries
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    Ok(())
}

#[tokio::test]
async fn test_retry_policy_calculation() -> anyhow::Result<()> {
    let retry_policy = RetryPolicy {
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 2.0,
        jitter: 0.0,
        ..Default::default()
    };
    
    // Test delay calculation
    assert_eq!(retry_policy.calculate_delay(0), Duration::from_millis(1000));
    assert_eq!(retry_policy.calculate_delay(1), Duration::from_millis(2000));
    assert_eq!(retry_policy.calculate_delay(2), Duration::from_millis(4000));
    
    // Test max delay cap
    let large_delay = retry_policy.calculate_delay(10);
    assert_eq!(large_delay, Duration::from_secs(30));
    
    Ok(())
}
```

### 4. Health Monitoring Tests

```rust
// tests/integration_health.rs
mod common;

use common::*;
use rust_rabbit::health::{HealthCheckConfigExt, ConnectionStatus};
use rust_rabbit::config::HealthCheckConfig;
use tokio::time::Duration;

#[tokio::test]
async fn test_health_monitoring() -> anyhow::Result<()> {
    let mut config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .health(|health| health.frequent().enabled())
        .build();
    
    let rabbit = RustRabbit::new(config).await?;
    let health_checker = rabbit.health_checker();
    
    // Start monitoring
    health_checker.start_monitoring().await?;
    
    // Check initial health
    let initial_health = health_checker.check_health().await?;
    assert_eq!(initial_health.status, ConnectionStatus::Healthy);
    
    // Wait for background monitoring to run
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // Check if monitoring is working
    assert!(health_checker.is_healthy().await);
    assert!(health_checker.is_operational().await);
    
    // Get health summary
    let summary = health_checker.get_health_summary().await;
    assert!(summary.monitoring_enabled);
    assert!(summary.healthy_connections > 0);
    
    // Stop monitoring
    health_checker.stop_monitoring().await;
    
    Ok(())
}

#[tokio::test]
async fn test_wait_for_healthy() -> anyhow::Result<()> {
    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .build();
        
    let rabbit = RustRabbit::new(config).await?;
    let health_checker = rabbit.health_checker();
    
    // Should complete quickly since connection is healthy
    health_checker.wait_for_healthy(Some(Duration::from_secs(5))).await?;
    
    Ok(())
}
```

### 5. Performance Tests

```rust
// tests/integration_performance.rs
mod common;

use common::*;
use rust_rabbit::{PublishOptions, ConsumerOptions, MessageHandler, MessageResult, MessageContext};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::time::{Duration, Instant};
use async_trait::async_trait;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PerformanceTestMessage {
    id: usize,
    payload: String,
    timestamp: u64,
}

struct PerformanceTestHandler {
    processed_count: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageHandler<PerformanceTestMessage> for PerformanceTestHandler {
    async fn handle(&self, _message: PerformanceTestMessage, _context: MessageContext) -> MessageResult {
        self.processed_count.fetch_add(1, Ordering::SeqCst);
        MessageResult::Ack
    }
}

#[tokio::test]
async fn test_high_throughput_publishing() -> anyhow::Result<()> {
    let env = TestEnvironment::new().await?;
    let queue_name = generate_unique_queue_name("perf_test");
    
    let publisher = env.rabbit.publisher();
    let message_count = 1000;
    let payload = "x".repeat(1024); // 1KB payload
    
    let publish_options = PublishOptions::builder()
        .auto_declare_queue()
        .production()
        .build();
    
    let start = Instant::now();
    
    // Publish messages concurrently
    let mut tasks = Vec::new();
    for i in 0..message_count {
        let publisher = publisher.clone();
        let queue_name = queue_name.clone();
        let message = PerformanceTestMessage {
            id: i,
            payload: payload.clone(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };
        let options = publish_options.clone();
        
        tasks.push(tokio::spawn(async move {
            publisher.publish_to_queue(&queue_name, &message, Some(options)).await
        }));
    }
    
    // Wait for all publishes to complete
    for task in tasks {
        task.await??;
    }
    
    let duration = start.elapsed();
    let throughput = message_count as f64 / duration.as_secs_f64();
    
    println!("Published {} messages in {:?}", message_count, duration);
    println!("Throughput: {:.2} messages/second", throughput);
    
    // Verify reasonable performance (adjust threshold as needed)
    assert!(throughput > 100.0, "Throughput too low: {:.2} msg/s", throughput);
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_consumption() -> anyhow::Result<()> {
    let env = TestEnvironment::new().await?;
    let queue_name = generate_unique_queue_name("concurrent_test");
    
    let handler = Arc::new(PerformanceTestHandler {
        processed_count: Arc::new(AtomicUsize::new(0)),
    });
    
    let consumer_options = ConsumerOptions::builder(&queue_name)
        .auto_declare_queue()
        .high_throughput() // 20 concurrent workers
        .build();
    
    let consumer = env.rabbit.consumer(consumer_options).await?;
    let handler_clone = handler.clone();
    
    // Start consumer
    let consume_task = tokio::spawn(async move {
        consumer.consume::<PerformanceTestMessage, PerformanceTestHandler>(handler_clone).await
    });
    
    // Give consumer time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Publish test messages
    let publisher = env.rabbit.publisher();
    let message_count = 100;
    
    let publish_options = PublishOptions::builder()
        .development()
        .build();
    
    for i in 0..message_count {
        let message = PerformanceTestMessage {
            id: i,
            payload: format!("Message {}", i),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };
        
        publisher.publish_to_queue(&queue_name, &message, Some(publish_options.clone())).await?;
    }
    
    // Wait for processing
    let timeout_duration = Duration::from_secs(10);
    tokio::time::timeout(timeout_duration, async {
        loop {
            if handler.processed_count.load(Ordering::SeqCst) >= message_count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await?;
    
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), message_count);
    
    consume_task.abort();
    
    Ok(())
}
```

## 🚀 Chạy Integration Tests

### 1. Setup Environment

```bash
# Start RabbitMQ với Docker Compose
docker-compose -f docker-compose.test.yml up -d

# Chờ RabbitMQ sẵn sàng
./scripts/wait-for-rabbitmq.sh

# Chạy integration tests
cargo test --test integration_* -- --test-threads=1
```

### 2. Continuous Integration

```yaml
# .github/workflows/integration-tests.yml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    
    services:
      rabbitmq:
        image: rabbitmq:3.12-management
        ports:
          - 5672:5672
          - 15672:15672
        env:
          RABBITMQ_DEFAULT_USER: admin
          RABBITMQ_DEFAULT_PASS: password
        options: >-
          --health-cmd "rabbitmq-diagnostics ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
          
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        
    - name: Wait for RabbitMQ
      run: |
        timeout 60 bash -c 'until rabbitmq-diagnostics ping -q; do sleep 1; done'
        
    - name: Run integration tests
      run: |
        cargo test --test integration_* -- --test-threads=1
```

## ✅ Kết Luận

**Có thể viết integration test với RabbitMQ thật một cách rất hiệu quả:**

1. **Docker Setup**: Dễ dàng setup RabbitMQ với Docker Compose
2. **Test Isolation**: Mỗi test sử dụng queue riêng biệt
3. **Real Scenarios**: Test với RabbitMQ thật cho độ tin cậy cao
4. **Performance Testing**: Benchmark thông lượng và latency
5. **CI/CD Integration**: Chạy tự động trong GitHub Actions

**Lợi ích của Integration Testing:**
- Phát hiện vấn đề với RabbitMQ server thật
- Test retry mechanism với delayed message exchange
- Verify connection pooling và health monitoring
- Performance benchmarking
- End-to-end workflow validation

Thư viện RustRabbit đã có đầy đủ foundation để viết integration test hiệu quả! 🎉
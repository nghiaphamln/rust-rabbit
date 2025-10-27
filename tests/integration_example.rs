// tests/integration_example.rs
// Example integration test demonstrating real RabbitMQ usage

use async_trait::async_trait;
use rust_rabbit::consumer::{MessageContext, MessageResult};
use rust_rabbit::{
    connection::ConnectionManager,
    retry::{DelayedMessageExchange, RetryPolicy},
    ConsumerOptions, MessageHandler, PublishOptions, RabbitConfig, RustRabbit,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::{sleep, timeout, Duration};
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<String>,
    priority: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ProcessingResult {
    order_id: String,
    status: String,
    processed_at: String,
}

struct OrderProcessor {
    processed_orders: Arc<tokio::sync::Mutex<Vec<OrderMessage>>>,
    processing_count: Arc<AtomicUsize>,
    should_fail: Arc<AtomicUsize>, // For testing retry scenarios
}

#[async_trait]
impl MessageHandler<OrderMessage> for OrderProcessor {
    async fn handle(&self, message: OrderMessage, context: MessageContext) -> MessageResult {
        self.processing_count.fetch_add(1, Ordering::SeqCst);

        info!(
            "Processing order: {} (retry count: {})",
            message.order_id, context.retry_count
        );

        // Simulate processing failure for testing retry
        if self.should_fail.load(Ordering::SeqCst) > 0 && context.retry_count < 2 {
            self.should_fail.fetch_sub(1, Ordering::SeqCst);
            warn!(
                "Simulating processing failure for order: {}",
                message.order_id
            );
            return MessageResult::Retry;
        }

        // Simulate processing time
        sleep(Duration::from_millis(10)).await;

        // Store processed order
        let mut processed = self.processed_orders.lock().await;
        processed.push(message.clone());

        info!("Successfully processed order: {}", message.order_id);
        MessageResult::Ack
    }
}

async fn wait_for_rabbitmq() -> anyhow::Result<()> {
    let max_attempts = 30;
    let mut attempts = 0;

    while attempts < max_attempts {
        match RustRabbit::new(
            RabbitConfig::builder()
                .connection_string("amqp://admin:password@localhost:5672/test")
                .build(),
        )
        .await
        {
            Ok(rabbit) => {
                rabbit.close().await?;
                return Ok(());
            }
            Err(_) => {
                attempts += 1;
                if attempts % 5 == 0 {
                    println!(
                        "Waiting for RabbitMQ... attempt {}/{}",
                        attempts, max_attempts
                    );
                }
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "RabbitMQ not available after {} attempts",
        max_attempts
    ))
}

fn generate_test_orders(count: usize) -> Vec<OrderMessage> {
    (0..count)
        .map(|i| OrderMessage {
            order_id: format!("ORD-{:04}", i + 1),
            customer_id: format!("CUST-{}", (i % 10) + 1),
            amount: 10.0 + (i as f64 * 1.5),
            items: vec![format!("Product-{}", i + 1), format!("Accessory-{}", i + 1)],
            priority: ((i % 3) + 1) as u8,
        })
        .collect()
}

#[tokio::test]
async fn test_end_to_end_order_processing() -> anyhow::Result<()> {
    // Initialize tracing for better debugging
    tracing_subscriber::fmt::init();

    // Wait for RabbitMQ to be ready
    wait_for_rabbitmq().await?;

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .retry(|retry| retry.max_retries(3).aggressive())
        .health(|health| health.frequent())
        .pool(|pool| pool.single_connection())
        .build();

    let rabbit = RustRabbit::new(config).await?;
    let queue_name = format!("orders_{}", uuid::Uuid::new_v4());

    // Setup order processor
    let processor = Arc::new(OrderProcessor {
        processed_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        processing_count: Arc::new(AtomicUsize::new(0)),
        should_fail: Arc::new(AtomicUsize::new(0)), // No failures initially
    });

    // Configure consumer with high reliability
    let consumer_options = ConsumerOptions::builder(&queue_name)
        .consumer_tag("order-processor-test")
        .concurrency(3)
        .prefetch_count(5)
        .auto_declare_queue()
        .reliable()
        .build();

    let consumer = rabbit.consumer(consumer_options).await?;

    // Start consuming in background
    let processor_clone = processor.clone();
    let consume_task = tokio::spawn(async move {
        if let Err(e) = consumer
            .consume::<OrderMessage, OrderProcessor>(processor_clone)
            .await
        {
            eprintln!("Consumer error: {}", e);
        }
    });

    // Give consumer time to start
    sleep(Duration::from_millis(200)).await;

    // Generate and publish test orders
    let test_orders = generate_test_orders(10);
    let publisher = rabbit.publisher();

    let publish_options = PublishOptions::builder()
        .durable()
        .auto_declare_queue()
        .header_string("source", "integration-test")
        .header_int("version", 1)
        .build();

    info!("Publishing {} test orders...", test_orders.len());

    for order in &test_orders {
        publisher
            .publish_to_queue(&queue_name, order, Some(publish_options.clone()))
            .await?;
        info!("Published order: {}", order.order_id);
    }

    // Wait for all orders to be processed
    let timeout_duration = Duration::from_secs(30);
    timeout(timeout_duration, async {
        loop {
            let processed = processor.processed_orders.lock().await;
            if processed.len() >= test_orders.len() {
                break;
            }
            drop(processed);
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Timeout waiting for order processing");

    // Verify results
    let processed_orders = processor.processed_orders.lock().await;
    let processing_count = processor.processing_count.load(Ordering::SeqCst);

    assert_eq!(
        processed_orders.len(),
        test_orders.len(),
        "Not all orders were processed"
    );
    assert_eq!(
        processing_count,
        test_orders.len(),
        "Processing count mismatch"
    );

    // Verify all orders were processed correctly
    for expected_order in &test_orders {
        assert!(
            processed_orders.iter().any(|order| order == expected_order),
            "Order {} was not processed correctly",
            expected_order.order_id
        );
    }

    info!(
        "✅ All {} orders processed successfully!",
        test_orders.len()
    );

    // Cleanup
    consume_task.abort();
    rabbit.close().await?;

    Ok(())
}

#[tokio::test]
#[ignore] // Requires rabbitmq-delayed-message-exchange plugin
async fn test_retry_mechanism_with_delayed_exchange() -> anyhow::Result<()> {
    wait_for_rabbitmq().await?;

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .build();

    let connection_manager = ConnectionManager::new(config).await?;

    // Setup retry policy
    let retry_policy = RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(5),
        backoff_multiplier: 2.0,
        jitter: 0.1,
        dead_letter_exchange: Some("failed_orders".to_string()),
        ..Default::default()
    };

    let delayed_exchange = DelayedMessageExchange::new(
        connection_manager,
        "test_retry_exchange".to_string(),
        retry_policy.clone(),
    );

    // Setup infrastructure
    delayed_exchange.setup().await?;

    let queue_name = format!("retry_test_{}", uuid::Uuid::new_v4());
    delayed_exchange.setup_retry_queues(&queue_name).await?;

    // Test retry delay calculation
    let delay1 = retry_policy.calculate_delay(0);
    let delay2 = retry_policy.calculate_delay(1);
    let delay3 = retry_policy.calculate_delay(2);

    info!("Retry delays: {:?}, {:?}, {:?}", delay1, delay2, delay3);

    // Verify exponential backoff
    assert!(delay2 > delay1);
    assert!(delay3 > delay2);

    // Test publishing with retry
    let test_order = OrderMessage {
        order_id: "RETRY-001".to_string(),
        customer_id: "CUST-RETRY".to_string(),
        amount: 99.99,
        items: vec!["Retry Test Item".to_string()],
        priority: 1,
    };

    for attempt in 0..retry_policy.max_retries {
        info!(
            "Publishing retry attempt {} for order: {}",
            attempt + 1,
            test_order.order_id
        );

        delayed_exchange
            .publish_with_retry(&queue_name, &test_order, attempt, None)
            .await?;

        sleep(Duration::from_millis(50)).await;
    }

    info!("✅ Retry mechanism test completed successfully!");

    Ok(())
}

#[tokio::test]
async fn test_health_monitoring_integration() -> anyhow::Result<()> {
    wait_for_rabbitmq().await?;

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .health(|health| {
            health
                .check_interval(Duration::from_millis(500))
                .check_timeout(Duration::from_secs(2))
                .enabled()
        })
        .build();

    let rabbit = RustRabbit::new(config).await?;
    let health_checker = rabbit.health_checker();

    // Perform initial health check
    let health_result = health_checker.check_health().await?;
    info!("Initial health check: {:?}", health_result.status);
    info!("Response time: {:?}", health_result.response_time);
    info!("Connection stats: {:?}", health_result.connection_stats);

    assert!(health_result.status.is_healthy());
    assert!(health_result.response_time < Duration::from_secs(1));

    // Start background monitoring
    health_checker.start_monitoring().await?;

    // Wait for monitoring to run a few cycles
    sleep(Duration::from_secs(2)).await;

    // Check health status
    assert!(health_checker.is_healthy().await);
    assert!(health_checker.is_operational().await);

    // Test wait for healthy (should complete immediately)
    let wait_start = std::time::Instant::now();
    health_checker
        .wait_for_healthy(Some(Duration::from_secs(5)))
        .await?;
    let wait_duration = wait_start.elapsed();

    assert!(
        wait_duration < Duration::from_secs(1),
        "Wait for healthy took too long: {:?}",
        wait_duration
    );

    // Get health summary
    let summary = health_checker.get_health_summary().await;
    info!("Health summary: {:?}", summary);

    assert!(summary.monitoring_enabled);
    assert!(summary.healthy_connections > 0);
    assert_eq!(summary.unhealthy_connections, 0);

    // Stop monitoring
    health_checker.stop_monitoring().await;

    rabbit.close().await?;

    info!("✅ Health monitoring test completed successfully!");

    Ok(())
}

#[tokio::test]
async fn test_performance_benchmark() -> anyhow::Result<()> {
    wait_for_rabbitmq().await?;

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .pool(|pool| pool.high_throughput())
        .build();

    let rabbit = RustRabbit::new(config).await?;
    let queue_name = format!("perf_test_{}", uuid::Uuid::new_v4());

    let message_count = 100;
    let payload_size = 1024; // 1KB messages

    // Generate test data
    let test_orders: Vec<OrderMessage> = (0..message_count)
        .map(|i| OrderMessage {
            order_id: format!("PERF-{:05}", i),
            customer_id: format!("CUST-{}", i % 50),
            amount: 10.0 + (i as f64),
            items: vec!["x".repeat(payload_size / 4)], // Roughly 1KB when serialized
            priority: (i % 3) as u8 + 1,
        })
        .collect();

    let publisher = rabbit.publisher();
    let publish_options = PublishOptions::builder()
        .auto_declare_queue()
        .production()
        .build();

    // Benchmark publishing
    let publish_start = std::time::Instant::now();

    for order in &test_orders {
        publisher
            .publish_to_queue(&queue_name, order, Some(publish_options.clone()))
            .await?;
    }

    let publish_duration = publish_start.elapsed();
    let publish_throughput = message_count as f64 / publish_duration.as_secs_f64();

    info!("📊 Performance Results:");
    info!("  Messages: {}", message_count);
    info!("  Payload size: {} bytes", payload_size);
    info!("  Publish time: {:?}", publish_duration);
    info!("  Publish throughput: {:.2} msg/s", publish_throughput);

    // Basic performance assertions (adjust thresholds for Docker environment)
    assert!(
        publish_throughput > 10.0,
        "Publish throughput too low: {:.2} msg/s",
        publish_throughput
    );
    assert!(
        publish_duration < Duration::from_secs(30),
        "Publishing took too long: {:?}",
        publish_duration
    );

    rabbit.close().await?;

    info!("✅ Performance benchmark completed successfully!");

    Ok(())
}

// Helper function to run a complete integration test scenario
#[tokio::test]
async fn test_complete_workflow_simulation() -> anyhow::Result<()> {
    wait_for_rabbitmq().await?;

    info!("🚀 Starting complete workflow simulation...");

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .retry(|retry| retry.max_retries(2).conservative())
        .health(|health| health.frequent())
        .pool(|pool| pool.high_throughput())
        .build();

    let rabbit = RustRabbit::new(config).await?;

    // Simulate multiple queues for different processing stages
    let order_queue = format!("orders_{}", uuid::Uuid::new_v4());
    let _processing_queue = format!("processing_{}", uuid::Uuid::new_v4());
    let notification_queue = format!("notifications_{}", uuid::Uuid::new_v4());

    // Create test orders
    let orders = generate_test_orders(5);
    let publisher = rabbit.publisher();

    // Stage 1: Publish orders
    info!("📝 Stage 1: Publishing orders...");
    let publish_options = PublishOptions::builder()
        .auto_declare_queue()
        .header_string("stage", "order-intake")
        .header_string("source", "workflow-test")
        .build();

    for order in &orders {
        publisher
            .publish_to_queue(&order_queue, order, Some(publish_options.clone()))
            .await?;
    }

    // Stage 2: Simulate order processing workflow
    info!("⚙️ Stage 2: Processing orders...");

    let processing_processor = Arc::new(OrderProcessor {
        processed_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        processing_count: Arc::new(AtomicUsize::new(0)),
        should_fail: Arc::new(AtomicUsize::new(1)), // Simulate one failure for retry testing
    });

    let consumer_options = ConsumerOptions::builder(&order_queue)
        .consumer_tag("workflow-processor")
        .concurrency(2)
        .auto_declare_queue()
        .reliable()
        .build();

    let consumer = rabbit.consumer(consumer_options).await?;
    let processor_clone = processing_processor.clone();

    // Start processing
    let process_task = tokio::spawn(async move {
        consumer
            .consume::<OrderMessage, OrderProcessor>(processor_clone)
            .await
    });

    // Wait for processing
    timeout(Duration::from_secs(15), async {
        loop {
            let processed = processing_processor.processed_orders.lock().await;
            if processed.len() >= orders.len() {
                break;
            }
            drop(processed);
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Processing timeout");

    // Stage 3: Verify results and publish notifications
    info!("📧 Stage 3: Sending notifications...");

    let processed_orders = processing_processor.processed_orders.lock().await;
    assert_eq!(processed_orders.len(), orders.len());

    // Send completion notifications
    let notification_options = PublishOptions::builder()
        .auto_declare_queue()
        .header_string("type", "order-completion")
        .build();

    for order in processed_orders.iter() {
        let notification = ProcessingResult {
            order_id: order.order_id.clone(),
            status: "completed".to_string(),
            processed_at: chrono::Utc::now().to_rfc3339(),
        };

        publisher
            .publish_to_queue(
                &notification_queue,
                &notification,
                Some(notification_options.clone()),
            )
            .await?;
    }

    // Health check
    info!("🏥 Stage 4: Health verification...");
    let health_checker = rabbit.health_checker();
    let health_result = health_checker.check_health().await?;

    assert!(health_result.status.is_healthy());
    info!("Health status: {:?}", health_result.status);

    // Cleanup
    process_task.abort();
    rabbit.close().await?;

    info!("✅ Complete workflow simulation finished successfully!");
    info!("   - Processed {} orders", orders.len());
    info!("   - Sent {} notifications", processed_orders.len());
    info!("   - System health: {:?}", health_result.status);

    Ok(())
}

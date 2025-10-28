use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    retry::{DelayedMessageExchange, RetryPolicy},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time;
use tracing::{info, warn, Level};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage {
    id: String,
    content: String,
    timestamp: chrono::DateTime<chrono::Utc>,
}

struct TestHandler;

#[async_trait::async_trait]
impl MessageHandler<TestMessage> for TestHandler {
    async fn handle(&self, message: TestMessage, _context: MessageContext) -> MessageResult {
        info!("Processing message: {:?}", message);

        // Simulate random failure for testing retry
        if fastrand::f32() < 0.7 {
            warn!("Simulated processing failure for message: {}", message.id);
            return MessageResult::Retry;
        }

        info!("Successfully processed message: {}", message.id);
        MessageResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/")
        .build();

    let connection_manager = ConnectionManager::new(config).await?;

    // Setup retry policy
    let retry_policy = RetryPolicy::builder()
        .max_retries(3)
        .initial_delay(Duration::from_millis(500))
        .max_delay(Duration::from_secs(10))
        .backoff_multiplier(2.0)
        .jitter(0.1)
        .dead_letter_exchange("test_dlx".to_string())
        .dead_letter_queue("test_dlq".to_string())
        .build();

    // Create delayed exchange for testing
    let delayed_exchange = DelayedMessageExchange::new(
        connection_manager.clone(),
        "test_queue.retry".to_string(),
        retry_policy.clone(),
    );

    // Setup the delayed exchange infrastructure
    delayed_exchange.setup().await?;

    // This is the key fix - setup queue binding for retry mechanism
    delayed_exchange.setup_queue_retry("test_queue").await?;

    info!("✅ Delay exchange setup completed with proper queue binding");

    // Setup consumer with retry policy
    let consumer_options = ConsumerOptions::builder("test_queue")
        .auto_declare_queue()
        .prefetch_count(10)
        .retry_policy(retry_policy)
        .build();

    let consumer = Consumer::new(connection_manager.clone(), consumer_options).await?;
    let handler = std::sync::Arc::new(TestHandler);

    info!("Starting consumer...");

    // Start consumer in background
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = consumer.consume(handler).await {
            warn!("Consumer error: {}", e);
        }
    });

    // Give consumer time to start
    time::sleep(Duration::from_millis(1000)).await;

    // Test publishing messages that will trigger retries
    for i in 1..=5 {
        let test_message = TestMessage {
            id: Uuid::new_v4().to_string(),
            content: format!("Test message {}", i),
            timestamp: chrono::Utc::now(),
        };

        // Simulate failed processing that triggers retry
        delayed_exchange
            .publish_with_retry("test_queue", &test_message, 0, None)
            .await?;

        info!("Published message {} for retry testing", i);
        time::sleep(Duration::from_millis(100)).await;
    }

    info!("All messages published. Monitoring for retry behavior...");

    // Monitor for a while to see retry behavior
    time::sleep(Duration::from_secs(30)).await;

    info!("Test completed!");
    consumer_handle.abort();

    Ok(())
}

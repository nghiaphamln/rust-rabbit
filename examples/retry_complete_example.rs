use rust_rabbit::{
    config::RustRabbitConfig,
    connection::Connection,
    consumer::Consumer,
    publisher::Publisher,
    retry::RetryConfig,
    message::MessageEnvelope,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, error};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TestMessage {
    pub content: String,
    pub attempt_count: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::init();

    let config = RustRabbitConfig::default();
    let connection = Arc::new(Connection::new(config).await?);

    // Publisher setup
    let publisher = Publisher::new(connection.clone());

    // Consumer setup with retry configuration
    let retry_config = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(1000),
        backoff_factor: 2.0,
        max_delay: Duration::from_secs(30),
    };

    let consumer = Consumer::new(
        connection.clone(),
        "test_retry_queue".to_string(),
        Some(retry_config),
    );

    // Test message
    let test_message = TestMessage {
        content: "This message will intentionally fail for testing retry".to_string(),
        attempt_count: 0,
    };

    // Publish test message
    info!("Publishing test message...");
    publisher.publish("test_retry_queue", &test_message).await?;

    // Consume with retry logic - simulating failures
    info!("Starting consumer with retry logic...");
    consumer
        .consume_envelopes(|envelope: MessageEnvelope<TestMessage>| async move {
            info!(
                "Processing message: {} (retry attempt: {})",
                envelope.data.content, envelope.metadata.retry_attempt
            );

            // Simulate failure for first 2 attempts
            if envelope.metadata.retry_attempt < 2 {
                error!("Simulated failure - retry attempt {}", envelope.metadata.retry_attempt);
                return Err("Simulated failure".into());
            }

            // Success on 3rd attempt
            info!("Message processed successfully on attempt {}", envelope.metadata.retry_attempt);
            Ok(())
        })
        .await?;

    Ok(())
}
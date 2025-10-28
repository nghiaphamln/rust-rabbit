use async_trait::async_trait;
use rust_rabbit::consumer::MessageContext;
use rust_rabbit::{
    BaseConsumer, ConsumerOptions, ProcessingError, RabbitConfig, RetryPolicy, RustRabbit,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Debug)]
struct SimpleMessage {
    id: String,
    content: String,
}

/// Simple handler demonstrating BaseConsumer usage
struct SimpleHandler;

#[async_trait]
impl BaseConsumer<SimpleMessage> for SimpleHandler {
    async fn handle(
        &self,
        message: SimpleMessage,
        context: MessageContext,
    ) -> Result<(), ProcessingError> {
        info!("📨 Received message: {}", message.content);

        // Simulate processing
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Simple error demonstration
        if message.id.contains("retry") {
            if context.retry_count < 2 {
                warn!("⚠️ Simulating retryable error for message: {}", message.id);
                return Err(ProcessingError::retryable("Temporary processing error"));
            } else {
                info!("✅ Message processed after retries: {}", message.id);
            }
        } else if message.id.contains("fail") {
            warn!("❌ Permanent failure for message: {}", message.id);
            return Err(ProcessingError::non_retryable("Permanent processing error"));
        }

        info!("✅ Successfully processed message: {}", message.id);
        Ok(()) // Automatic ACK
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = RabbitConfig::default();
    let rabbit = RustRabbit::new(config).await?;

    // Simple consumer setup with retry
    let options = ConsumerOptions::builder("simple_queue")
        .auto_declare_queue()
        .auto_declare_exchange()
        .retry_policy(RetryPolicy::fast())
        .manual_ack() // Required for retry
        .build();

    let consumer = rabbit.consumer(options).await?;
    let handler = Arc::new(SimpleHandler);

    info!("🚀 Starting simple BaseConsumer example...");

    // Use the new BaseConsumer method
    consumer
        .consume_with_base_consumer::<SimpleMessage, SimpleHandler>(handler)
        .await?;

    Ok(())
}

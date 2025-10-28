use async_trait::async_trait;
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    error::Result,
    retry::RetryPolicy,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct SimpleMessage {
    id: String,
    content: String,
}

struct SimpleHandler;

#[async_trait]
impl MessageHandler<SimpleMessage> for SimpleHandler {
    async fn handle(&self, message: SimpleMessage, _context: MessageContext) -> MessageResult {
        info!("Received message: {} - {}", message.id, message.content);

        // Simulate some processing
        if message.id == "fail" {
            warn!("Message failed, will retry");
            MessageResult::Retry
        } else {
            info!("Message processed successfully");
            MessageResult::Ack
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting simple auto-declare consumer example...");

    // Create configuration
    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // Configure simple retry policy
    let retry_policy = RetryPolicy::default();

    // Create consumer with auto-declare features
    let options = ConsumerOptions::builder("my.queue")
        .auto_declare_queue() // Tự động tạo queue
        .auto_declare_exchange() // Tự động tạo exchange (tên = queue name) và bind
        .retry_policy(retry_policy) // Tự động setup retry với delayed exchange
        .development() // Preset cho development
        .build();

    let consumer = Consumer::new(connection_manager, options).await?;
    let handler = Arc::new(SimpleHandler);

    info!(
        "Consumer setup complete. Queue, exchange, and retry infrastructure created automatically!"
    );
    info!("Exchange name: 'my.queue' (same as queue name)");
    info!("Routing key: 'my.queue' (same as queue name)");
    info!("Retry exchange: 'my.queue.retry' (delayed message exchange)");
    info!("Dead letter exchange: 'dead-letter'");

    // Start consuming
    consumer.consume::<SimpleMessage, _>(handler).await?;

    Ok(())
}

/*
 * Khi chạy example này:
 *
 * 1. Consumer sẽ tự động tạo:
 *    - Queue: "my.queue"
 *    - Exchange: "my.queue" (direct exchange)
 *    - Binding: queue "my.queue" -> exchange "my.queue" với routing key "my.queue"
 *    - Delayed exchange: "my.queue.retry" (x-delayed-message type)
 *    - Dead letter exchange: "dead-letter"
 *    - Dead letter queue: "dead-letter-queue"
 *
 * 2. Khi message fail và retry:
 *    - Message sẽ được publish vào "my.queue.retry" exchange
 *    - Với delay header để retry sau một khoảng thời gian
 *    - Delay tăng dần theo exponential backoff (1s, 2s, 4s, ...)
 *    - Sau max retries, message sẽ đi vào dead letter exchange
 *
 * 3. Để test, có thể publish message:
 *    publisher.publish_to_exchange("my.queue", "my.queue", &message, None).await?;
 */

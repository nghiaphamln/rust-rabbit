use async_trait::async_trait;
use rust_rabbit::consumer::{MessageContext, MessageResult};
use rust_rabbit::{retry::RetryPolicy, ConsumerOptions, MessageHandler, RabbitConfig, RustRabbit};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<String>,
}

// Custom message handler
struct OrderHandler;

#[async_trait]
impl MessageHandler<OrderMessage> for OrderHandler {
    async fn handle(&self, message: OrderMessage, context: MessageContext) -> MessageResult {
        info!("Processing order: {:?}", message);
        info!(
            "Message context: queue={}, retry_count={}",
            context.routing_key, context.retry_count
        );

        // Simulate processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate occasional failures for retry demonstration
        if message.order_id.ends_with("999") && context.retry_count < 2 {
            error!(
                "Simulated processing failure for order: {}",
                message.order_id
            );
            return MessageResult::Retry;
        }

        // Simulate permanent failures
        if message.order_id.ends_with("666") {
            error!("Permanent failure for order: {}", message.order_id);
            return MessageResult::Reject;
        }

        info!("Successfully processed order: {}", message.order_id);
        MessageResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create configuration
    let config = RabbitConfig {
        connection_string: "amqp://localhost:5672".to_string(),
        virtual_host: Some("/".to_string()),
        ..Default::default()
    };

    // Create RustRabbit instance
    let rabbit = RustRabbit::new(config).await?;

    // Configure consumer options with retry policy
    let retry_policy = RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 2.0,
        jitter: 0.1,
        ..Default::default()
    };

    let consumer_options = ConsumerOptions {
        queue_name: "orders".to_string(),
        consumer_tag: Some("order-processor".to_string()),
        concurrency: 5, // Process up to 5 messages concurrently
        prefetch_count: Some(10),
        auto_declare_queue: true,
        retry_policy: Some(retry_policy),
        dead_letter_exchange: Some("order-failed".to_string()),
        auto_ack: false, // Manual acknowledgment for reliability
        ..Default::default()
    };

    // Create consumer
    let consumer = rabbit.consumer(consumer_options).await?;

    // Create message handler
    let handler = Arc::new(OrderHandler);

    info!("Starting consumer for orders queue...");

    // Start consuming (this will run indefinitely)
    // In a real application, you might want to handle shutdown signals
    tokio::select! {
        result = consumer.consume::<OrderMessage, OrderHandler>(handler) => {
            if let Err(e) = result {
                error!("Consumer error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    // Close connections
    rabbit.close().await?;

    Ok(())
}

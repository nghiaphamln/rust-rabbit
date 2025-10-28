use async_trait::async_trait;
use lapin::ExchangeKind;
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    error::Result,
    publisher::CustomExchangeDeclareOptions,
    retry::RetryPolicy,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    status: String,
}

struct OrderHandler;

#[async_trait]
impl MessageHandler<OrderMessage> for OrderHandler {
    async fn handle(&self, message: OrderMessage, context: MessageContext) -> MessageResult {
        info!(
            "Processing order {} for customer {} (attempt: {})",
            message.order_id,
            message.customer_id,
            context.retry_count + 1
        );

        // Simulate processing
        sleep(Duration::from_millis(100)).await;

        // Simulate different outcomes based on order_id
        match message.order_id.as_str() {
            id if id.starts_with("success_") => {
                info!("Successfully processed order: {}", message.order_id);
                MessageResult::Ack
            }
            id if id.starts_with("retry_") => {
                warn!("Order {} needs retry", message.order_id);
                if context.retry_count < 2 {
                    MessageResult::Retry
                } else {
                    error!("Order {} failed after max retries", message.order_id);
                    MessageResult::Reject
                }
            }
            id if id.starts_with("fail_") => {
                error!("Order {} failed permanently", message.order_id);
                MessageResult::Reject
            }
            _ => {
                info!("Order {} processed normally", message.order_id);
                MessageResult::Ack
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting enhanced consumer example with auto exchange and retry...");

    // Create configuration
    let config = RabbitConfig::builder()
        .connection_string("amqp://guest:guest@localhost:5672/")
        .connection_timeout(Duration::from_secs(30))
        .heartbeat(Duration::from_secs(60))
        .build();

    // Create connection manager
    let connection_manager = ConnectionManager::new(config).await?;

    // Configure retry policy
    let retry_policy = RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 2.0,
        jitter: 0.1,
        retry_queue_pattern: "{queue_name}.retry.{attempt}".to_string(),
        dead_letter_exchange: Some("orders.dlx".to_string()),
        dead_letter_queue: Some("orders.dlq".to_string()),
    };

    // Configure exchange options for direct exchange
    let exchange_options = CustomExchangeDeclareOptions {
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        exchange_type: ExchangeKind::Direct,
        original_type: ExchangeKind::Direct,
        arguments: Default::default(),
    };

    // Example 1: Consumer with auto exchange declaration
    info!("Setting up consumer with auto exchange declaration...");

    let consumer_options = ConsumerOptions::builder("orders.processing")
        .auto_declare_queue() // Auto declare queue
        .auto_declare_exchange() // Auto declare exchange and bind to queue
        .exchange_name("orders.exchange") // Custom exchange name (optional)
        .exchange_options(exchange_options)
        .routing_key("order.process") // Custom routing key (optional)
        .retry_policy(retry_policy.clone())
        .concurrency(5)
        .prefetch_count(10)
        .build();

    let consumer = Consumer::new(connection_manager.clone(), consumer_options).await?;
    let handler = Arc::new(OrderHandler);

    // Start consuming in a background task
    let consumer_handle = {
        let consumer = consumer;
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<OrderMessage, _>(handler).await {
                error!("Consumer error: {}", e);
            }
        })
    };

    // Example 2: Consumer with default exchange (uses queue name as exchange name)
    info!("Setting up consumer with default exchange settings...");

    let simple_options = ConsumerOptions::builder("simple.orders")
        .auto_declare_queue()
        .auto_declare_exchange() // Will use "simple.orders" as exchange name
        .retry_policy(retry_policy)
        .development() // Use development preset (includes auto_declare_exchange)
        .build();

    let simple_consumer = Consumer::new(connection_manager.clone(), simple_options).await?;
    let simple_handler = Arc::new(OrderHandler);

    let simple_consumer_handle = {
        let consumer = simple_consumer;
        let handler = simple_handler;
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<OrderMessage, _>(handler).await {
                error!("Simple consumer error: {}", e);
            }
        })
    };

    // Example 3: Send test messages to demonstrate retry functionality
    info!("Sending test messages...");

    use rust_rabbit::publisher::{PublishOptions, Publisher};

    let publisher = Publisher::new(connection_manager);

    let test_messages = vec![
        OrderMessage {
            order_id: "success_001".to_string(),
            customer_id: "customer_1".to_string(),
            amount: 100.0,
            status: "pending".to_string(),
        },
        OrderMessage {
            order_id: "retry_002".to_string(),
            customer_id: "customer_2".to_string(),
            amount: 200.0,
            status: "pending".to_string(),
        },
        OrderMessage {
            order_id: "fail_003".to_string(),
            customer_id: "customer_3".to_string(),
            amount: 300.0,
            status: "pending".to_string(),
        },
    ];

    // Publish messages to the exchanges
    for message in test_messages {
        // Publish to the custom exchange
        publisher
            .publish_to_exchange(
                "orders.exchange",
                "order.process",
                &message,
                Some(PublishOptions::builder().auto_declare_exchange().build()),
            )
            .await?;

        // Also publish to the simple exchange
        publisher
            .publish_to_exchange(
                "simple.orders",
                "simple.orders", // routing key same as queue name
                &message,
                Some(PublishOptions::builder().auto_declare_exchange().build()),
            )
            .await?;

        sleep(Duration::from_secs(1)).await;
    }

    info!("Test messages sent. Observing message processing...");

    // Let consumers run for a while to demonstrate retry behavior
    sleep(Duration::from_secs(30)).await;

    info!("Shutting down consumers...");

    // Cancel the consumer tasks
    consumer_handle.abort();
    simple_consumer_handle.abort();

    // Wait a bit for graceful shutdown
    sleep(Duration::from_secs(2)).await;

    info!("Enhanced consumer example completed!");
    Ok(())
}

use async_trait::async_trait;
use rust_rabbit::consumer::{MessageContext, MessageResult};
use rust_rabbit::{ConsumerOptions, MessageHandler, PublishOptions, RabbitConfig, RustRabbit};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<String>,
}

struct OrderHandler;

#[async_trait]
impl MessageHandler<OrderMessage> for OrderHandler {
    async fn handle(&self, message: OrderMessage, _context: MessageContext) -> MessageResult {
        info!("Processing order: {:?}", message);
        MessageResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Example 1: Basic configuration using builder pattern
    let _config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .virtual_host("my-vhost")
        .connection_timeout(Duration::from_secs(10))
        .build();

    info!("Example 1: Basic config created");

    // Example 2: Advanced configuration with nested builders
    let advanced_config = RabbitConfig::builder()
        .connection_string("amqp://user:pass@localhost:5672")
        .retry(|retry| {
            retry
                .max_retries(5)
                .initial_delay(Duration::from_millis(500))
                .aggressive()
        })
        .health(|health| {
            health
                .check_interval(Duration::from_secs(15))
                .frequent()
                .enabled()
        })
        .pool(|pool| {
            pool.max_connections(20)
                .min_connections(3)
                .high_throughput()
        })
        .build();

    info!("Example 2: Advanced config created");

    // Example 3: Environment-specific configurations
    let _dev_config = RabbitConfig::builder()
        .connection_string("amqp://localhost:5672")
        .retry(|retry| retry.conservative())
        .health(|health| health.infrequent())
        .pool(|pool| pool.single_connection())
        .build();

    let _prod_config = RabbitConfig::builder()
        .connection_string("amqp://prod-server:5672")
        .connection_timeout(Duration::from_secs(30))
        .retry(|retry| retry.aggressive())
        .health(|health| health.frequent())
        .pool(|pool| pool.high_throughput())
        .build();

    info!("Example 3: Environment configs created");

    // Create RustRabbit instance with builder config
    let rabbit = RustRabbit::new(advanced_config).await?;
    let publisher = rabbit.publisher();

    // Example 4: Publisher options with builder
    let basic_publish_options = PublishOptions::builder()
        .persistent(true)
        .random_message_id()
        .development()
        .build();

    let advanced_publish_options = PublishOptions::builder()
        .durable()
        .ttl(Duration::from_secs(300))
        .priority(5)
        .header_string("source", "order-service")
        .header_int("version", 1)
        .correlation_id("order-correlation-123")
        .auto_declare_queue()
        .build();

    let _rpc_publish_options = PublishOptions::builder()
        .request_response("reply-queue", "correlation-456")
        .ttl(Duration::from_secs(30))
        .production()
        .build();

    info!("Example 4: Publish options created");

    // Example 5: Consumer options with builder
    let basic_consumer_options = ConsumerOptions::builder("orders")
        .consumer_tag("order-processor")
        .concurrency(5)
        .development()
        .build();

    let _high_throughput_consumer_options = ConsumerOptions::builder("high-throughput.queue")
        .consumer_tag("bulk-processor")
        .high_throughput()
        .auto_declare_queue()
        .dead_letter_exchange("failed-orders")
        .build();

    let _reliable_consumer_options = ConsumerOptions::builder("critical.queue")
        .consumer_tag("critical-processor")
        .reliable()
        .manual_ack()
        .prefetch_count(1)
        .build();

    info!("Example 5: Consumer options created");

    // Create sample message
    let order = OrderMessage {
        order_id: "ORD-12345".to_string(),
        customer_id: "CUST-67890".to_string(),
        amount: 99.99,
        items: vec!["Item 1".to_string(), "Item 2".to_string()],
    };

    // Publish with different options
    publisher
        .publish_to_queue("orders", &order, Some(basic_publish_options))
        .await?;

    publisher
        .publish_to_exchange(
            "order-exchange",
            "new-order",
            &order,
            Some(advanced_publish_options),
        )
        .await?;

    info!("Messages published successfully");

    // Create consumer with builder options
    let _consumer = rabbit.consumer(basic_consumer_options).await?;
    let _handler = Arc::new(OrderHandler);

    info!("Starting consumer with builder configuration...");

    // In a real application, you would run this
    // consumer.consume::<OrderMessage, OrderHandler>(handler).await?;

    // Close connections
    rabbit.close().await?;

    info!("Builder pattern examples completed successfully!");

    Ok(())
}

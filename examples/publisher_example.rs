use rust_rabbit::{
    RustRabbit, RabbitConfig, PublishOptions, 
    CustomQueueDeclareOptions,
};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<String>,
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
    let publisher = rabbit.publisher();

    // Create sample message
    let order = OrderMessage {
        order_id: "ORD-12345".to_string(),
        customer_id: "CUST-67890".to_string(),
        amount: 99.99,
        items: vec!["Item 1".to_string(), "Item 2".to_string()],
    };

    // Example 1: Simple queue publish with auto-declaration
    let mut options = PublishOptions::default();
    options.auto_declare_queue = true;
    options.queue_options = CustomQueueDeclareOptions {
        durable: true,
        ..Default::default()
    };

    publisher
        .publish_to_queue("orders", &order, Some(options))
        .await?;

    info!("Published order to queue: orders");

    // Example 2: Publish to exchange
    publisher
        .publish_to_exchange("order-exchange", "new-order", &order, None)
        .await?;

    info!("Published order to exchange: order-exchange");

    // Example 3: Publish with TTL
    publisher
        .publish_with_ttl(
            "order-exchange",
            "priority-order",
            &order,
            Duration::from_secs(300), // 5 minutes TTL
            None,
        )
        .await?;

    info!("Published order with TTL to exchange: order-exchange");

    // Example 4: Delayed message (requires rabbitmq-delayed-message-exchange plugin)
    publisher
        .publish_delayed(
            "delayed-exchange",
            "delayed-order",
            &order,
            Duration::from_secs(60), // 1 minute delay
            None,
        )
        .await?;

    info!("Published delayed order to exchange: delayed-exchange");

    // Close connections
    rabbit.close().await?;

    Ok(())
}
use async_trait::async_trait;
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    error::Result,
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
    operation: String,
}

struct OrderProcessor;

#[async_trait]
impl MessageHandler<OrderMessage> for OrderProcessor {
    async fn handle(&self, message: OrderMessage, context: MessageContext) -> MessageResult {
        info!(
            "🛒 Processing order {} for customer {} (${}, attempt: {})",
            message.order_id,
            message.customer_id,
            message.amount,
            context.retry_count + 1
        );

        // Simulate processing time
        sleep(Duration::from_millis(200)).await;

        match message.operation.as_str() {
            "process_immediately" => {
                info!("✅ Order {} processed successfully", message.order_id);
                MessageResult::Ack
            }
            "retry_few_times" => {
                if context.retry_count < 2 {
                    warn!(
                        "⚠️ Order {} failed, will retry with minutes delay",
                        message.order_id
                    );
                    MessageResult::Retry
                } else {
                    info!(
                        "✅ Order {} succeeded after {} retries",
                        message.order_id, context.retry_count
                    );
                    MessageResult::Ack
                }
            }
            "fail_permanently" => {
                error!(
                    "❌ Order {} failed permanently - invalid data",
                    message.order_id
                );
                MessageResult::Reject // Will go to DLX after retries
            }
            "retry_until_exhausted" => {
                warn!(
                    "⚠️ Order {} keeps failing, will exhaust all retries",
                    message.order_id
                );
                MessageResult::Retry // Will exhaust 5 retries then go to DLX
            }
            _ => {
                info!("✅ Order {} processed normally", message.order_id);
                MessageResult::Ack
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Minutes Retry Preset Demo");
    info!("Pattern: 1min → 2min → 4min → 8min → 16min (max 5 retries)");

    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // 🎯 Using the new minutes_retry() preset - ONE LINE SETUP!
    info!("📝 Setting up consumer with minutes_retry() preset...");

    let options = ConsumerOptions::builder("orders.processing")
        .minutes_retry() // 🔥 This single method sets up everything!
        .build();

    info!("✅ Configuration applied:");
    info!("   - Queue: orders.processing (auto-declared)");
    info!("   - Exchange: orders.processing (auto-declared & bound)");
    info!("   - Retry Exchange: orders.processing.retry (delayed message)");
    info!("   - Dead Letter Exchange: orders.processing.dlx");
    info!("   - Dead Letter Queue: orders.processing.dlq");
    info!("   - Retry Pattern: 1min → 2min → 4min → 8min → 16min");
    info!("   - Max Retries: 5");
    info!("   - Concurrency: 1 (reliable processing)");
    info!("   - Manual ACK: enabled");

    let consumer = Consumer::new(connection_manager.clone(), options).await?;
    let handler = Arc::new(OrderProcessor);

    // Start consumer in background
    let consumer_handle = {
        let consumer = consumer;
        let handler = handler;
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<OrderMessage, _>(handler).await {
                error!("Consumer error: {}", e);
            }
        })
    };

    // Wait a bit for consumer to start
    sleep(Duration::from_secs(2)).await;

    // Publish test messages
    info!("📨 Publishing test messages...");

    use rust_rabbit::publisher::{PublishOptions, Publisher};
    let publisher = Publisher::new(connection_manager);

    let test_orders = vec![
        OrderMessage {
            order_id: "ORD001".to_string(),
            customer_id: "CUST001".to_string(),
            amount: 99.99,
            operation: "process_immediately".to_string(),
        },
        OrderMessage {
            order_id: "ORD002".to_string(),
            customer_id: "CUST002".to_string(),
            amount: 199.99,
            operation: "retry_few_times".to_string(),
        },
        OrderMessage {
            order_id: "ORD003".to_string(),
            customer_id: "CUST003".to_string(),
            amount: 299.99,
            operation: "fail_permanently".to_string(),
        },
        OrderMessage {
            order_id: "ORD004".to_string(),
            customer_id: "CUST004".to_string(),
            amount: 399.99,
            operation: "retry_until_exhausted".to_string(),
        },
    ];

    for order in test_orders {
        publisher
            .publish_to_exchange(
                "orders.processing",
                "orders.processing",
                &order,
                Some(PublishOptions::builder().auto_declare_exchange().build()),
            )
            .await?;

        info!("📤 Published order: {}", order.order_id);
        sleep(Duration::from_millis(500)).await;
    }

    info!("⏳ Demo running... Observe the retry patterns:");
    info!("   📊 Expected behavior:");
    info!("   - ORD001: ✅ Immediate success");
    info!("   - ORD002: ⚠️ Fail → 1min delay → Fail → 2min delay → ✅ Success");
    info!("   - ORD003: ❌ Fail → 1min → 2min → 4min → 8min → 16min → 🚫 DLX");
    info!("   - ORD004: ⚠️ Fail → 1min → 2min → 4min → 8min → 16min → 🚫 DLX");

    // Let demo run for a reasonable time (in real scenario, retries would take much longer)
    sleep(Duration::from_secs(30)).await;

    info!("🛑 Stopping demo...");
    consumer_handle.abort();

    sleep(Duration::from_secs(1)).await;
    info!("✅ Minutes Retry Preset Demo completed!");
    info!("");
    info!("🎯 Key Benefits of minutes_retry() preset:");
    info!("   ✅ One-line setup for complex retry infrastructure");
    info!("   ✅ Automatically handles queue, exchange, and DLX setup");
    info!("   ✅ Optimized settings for reliable processing");
    info!("   ✅ Perfect for business-critical operations");
    info!("   ✅ Built-in dead letter handling");
    info!("");
    info!("💡 Usage in your code:");
    info!("   let options = ConsumerOptions::builder(\"your.queue\")");
    info!("       .minutes_retry()  // <- This is all you need!");
    info!("       .build();");

    Ok(())
}

/*
🔥 BEFORE (Complex Setup):

let retry_policy = RetryPolicy::builder()
    .max_retries(5)
    .initial_delay(Duration::from_secs(60))
    .max_delay(Duration::from_secs(2000))
    .backoff_multiplier(2.0)
    .jitter(0.1)
    .dead_letter_exchange("orders.processing.dlx")
    .dead_letter_queue("orders.processing.dlq")
    .build();

let options = ConsumerOptions::builder("orders.processing")
    .auto_declare_queue()
    .auto_declare_exchange()
    .retry_policy(retry_policy)
    .concurrency(1)
    .prefetch_count(1)
    .manual_ack()
    .build();

🎯 AFTER (Simple Setup):

let options = ConsumerOptions::builder("orders.processing")
    .minutes_retry()  // <- Everything configured!
    .build();

Both setups create identical infrastructure, but the preset is much simpler!
*/

use async_trait::async_trait;
use rust_rabbit::consumer::MessageContext;
use rust_rabbit::{
    BaseConsumer, ConsumerOptions, ProcessingError, RabbitConfig, RetryPolicy, RustRabbit,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::{error, info, warn};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderMessage {
    order_id: String,
    customer_id: String,
    amount: f64,
    items: Vec<String>,
}

/// Smart Order Handler using BaseConsumer trait
/// This demonstrates the new simplified error handling approach
struct SmartOrderHandler;

#[async_trait]
impl BaseConsumer<OrderMessage> for SmartOrderHandler {
    async fn handle(
        &self,
        message: OrderMessage,
        context: MessageContext,
    ) -> Result<(), ProcessingError> {
        info!("Processing order: {:?}", message);
        info!(
            "Message context: queue={}, retry_count={}",
            context.routing_key, context.retry_count
        );

        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Business logic with smart error handling
        match self.process_order(&message, &context).await {
            Ok(()) => {
                info!("✅ Order {} processed successfully", message.order_id);
                Ok(()) // Message will be automatically ACK'd
            }
            Err(err) => Err(err), // Let the framework handle retry/DLQ logic
        }
    }
}

impl SmartOrderHandler {
    async fn process_order(
        &self,
        message: &OrderMessage,
        context: &MessageContext,
    ) -> Result<(), ProcessingError> {
        // Simulate different types of errors

        // 1. Temporary network error - should retry
        if message.order_id.ends_with("temp-fail") && context.retry_count < 2 {
            return Err(ProcessingError::retryable(
                "Temporary network error - will retry automatically",
            ));
        }

        // 2. Rate limit error - retry with custom longer delay
        if message.order_id.ends_with("rate-limit") && context.retry_count < 3 {
            return Err(ProcessingError::retryable_with_delay(
                "Rate limited - retrying with longer delay",
                Duration::from_secs(30), // Custom longer delay
            ));
        }

        // 3. Invalid payment method - permanent error, send to DLQ for manual review
        if message.order_id.ends_with("invalid-payment") {
            return Err(ProcessingError::non_retryable(
                "Invalid payment method - requires manual review",
            ));
        }

        // 4. Spam order - permanent error, discard (don't send to DLQ)
        if message.order_id.ends_with("spam") {
            return Err(ProcessingError::discard(
                "Detected spam order - discarding without DLQ",
            ));
        }

        // 5. Business validation errors
        if message.amount <= 0.0 {
            return Err(ProcessingError::non_retryable(
                "Invalid order amount - must be positive",
            ));
        }

        if message.items.is_empty() {
            return Err(ProcessingError::non_retryable(
                "Empty order - no items to process",
            ));
        }

        // Simulate occasional transient database errors
        if message.customer_id.ends_with("db-error") && context.retry_count == 0 {
            return Err(ProcessingError::retryable(
                "Database temporarily unavailable",
            ));
        }

        // Success case - simulate actual order processing
        info!("💳 Processing payment for ${}", message.amount);
        info!("📦 Preparing {} items for shipment", message.items.len());
        info!("👤 Updating customer {} order history", message.customer_id);

        Ok(())
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

    // Configure consumer with smart retry policy
    let consumer_options = ConsumerOptions::builder("smart_orders")
        .auto_declare_queue()
        .auto_declare_exchange()
        .retry_policy(RetryPolicy::fast_for_queue("smart_orders"))
        .concurrency(3) // Process up to 3 messages concurrently
        .prefetch_count(5)
        .manual_ack() // Required for retry support
        .build();

    // Create consumer
    let consumer = rabbit.consumer(consumer_options).await?;

    // Create the smart message handler
    let handler = Arc::new(SmartOrderHandler);

    info!("🚀 Starting smart order consumer...");
    info!("📋 Features enabled:");
    info!("   ✅ Automatic ACK on success");
    info!("   ♻️  Smart retry with delay exchange");
    info!("   💀 Dead letter queue for permanent failures");
    info!("   🗑️  Message discarding for spam");
    info!("   ⏱️  Custom retry delays");

    // Start consuming with BaseConsumer trait
    // This method handles all the retry logic automatically
    tokio::select! {
        result = consumer.consume_with_base_consumer::<OrderMessage, SmartOrderHandler>(handler) => {
            if let Err(e) = result {
                error!("Consumer error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping consumer...");
        }
    }

    info!("✋ Consumer stopped");
    Ok(())
}

// Additional example showing how to create different types of test messages
#[allow(dead_code)]
async fn create_test_messages() -> Vec<OrderMessage> {
    vec![
        // Success case
        OrderMessage {
            order_id: "order-001".to_string(),
            customer_id: "customer-123".to_string(),
            amount: 99.99,
            items: vec!["laptop".to_string(), "mouse".to_string()],
        },
        // Temporary failure - will retry
        OrderMessage {
            order_id: "order-002-temp-fail".to_string(),
            customer_id: "customer-456".to_string(),
            amount: 49.99,
            items: vec!["book".to_string()],
        },
        // Rate limit - will retry with longer delay
        OrderMessage {
            order_id: "order-003-rate-limit".to_string(),
            customer_id: "customer-789".to_string(),
            amount: 199.99,
            items: vec!["phone".to_string()],
        },
        // Invalid payment - goes to DLQ
        OrderMessage {
            order_id: "order-004-invalid-payment".to_string(),
            customer_id: "customer-101".to_string(),
            amount: 299.99,
            items: vec!["tablet".to_string()],
        },
        // Spam - discarded
        OrderMessage {
            order_id: "order-005-spam".to_string(),
            customer_id: "spammer".to_string(),
            amount: 0.01,
            items: vec!["fake-item".to_string()],
        },
        // Database error - will retry once
        OrderMessage {
            order_id: "order-006".to_string(),
            customer_id: "customer-db-error".to_string(),
            amount: 79.99,
            items: vec!["headphones".to_string()],
        },
    ]
}

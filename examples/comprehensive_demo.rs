use async_trait::async_trait;
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    error::Result,
    publisher::{PublishOptions, Publisher},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    product_name: String,
    quantity: i32,
    price: f64,
    event_type: String, // "created", "retry_payment", "fail_inventory", "success"
}

/// Comprehensive message handler that demonstrates all retry scenarios
struct OrderProcessor;

#[async_trait]
impl MessageHandler<OrderEvent> for OrderProcessor {
    async fn handle(&self, message: OrderEvent, context: MessageContext) -> MessageResult {
        info!(
            "🛒 Processing order {} for customer {} (attempt: {}/5)",
            message.order_id,
            message.customer_id,
            context.retry_count + 1
        );

        // Simulate processing time
        sleep(Duration::from_millis(100)).await;

        match message.event_type.as_str() {
            "created" => {
                info!("✅ Order {} created successfully", message.order_id);
                MessageResult::Ack
            }
            "retry_payment" => {
                if context.retry_count < 2 {
                    warn!(
                        "💳 Payment failed for order {}, will retry in {} minutes",
                        message.order_id,
                        (context.retry_count + 1) * 2 // Exponential: 1min, 2min, 4min...
                    );
                    MessageResult::Retry
                } else {
                    info!(
                        "✅ Payment succeeded for order {} after retries",
                        message.order_id
                    );
                    MessageResult::Ack
                }
            }
            "fail_inventory" => {
                if context.retry_count < 4 {
                    warn!(
                        "📦 Inventory check failed for order {}, retry attempt {}/5",
                        message.order_id,
                        context.retry_count + 1
                    );
                    MessageResult::Retry
                } else {
                    error!(
                        "❌ Inventory permanently unavailable for order {} - sending to dead letter",
                        message.order_id
                    );
                    MessageResult::Reject
                }
            }
            "success" => {
                info!("🎉 Order {} completed successfully!", message.order_id);
                MessageResult::Ack
            }
            "permanent_failure" => {
                error!(
                    "💥 Order {} has invalid data - permanent failure",
                    message.order_id
                );
                MessageResult::Reject
            }
            _ => {
                info!("📋 Processing standard order {}", message.order_id);
                MessageResult::Ack
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("🚀 RustRabbit Comprehensive Example - All Features Demo");
    info!("This example demonstrates:");
    info!("  ✅ One-line setup with .minutes_retry()");
    info!("  ✅ Auto-declaration of infrastructure");
    info!("  ✅ Intelligent retry patterns (1min → 2min → 4min → 8min → 16min)");
    info!("  ✅ Dead letter handling");
    info!("  ✅ Multiple retry scenarios");
    info!("  ✅ Production-ready configuration");
    println!();

    // Setup connection
    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // 🔥 ONE LINE SETUP - This is the magic!
    info!("📝 Setting up consumer with .minutes_retry() preset...");
    let options = ConsumerOptions::builder("comprehensive.orders")
        .minutes_retry() // ← This single method configures everything!
        .build();

    info!("✅ Configuration automatically applied:");
    info!("   📋 Queue: comprehensive.orders (auto-declared)");
    info!("   🔄 Exchange: comprehensive.orders (auto-declared & bound)");
    info!("   ⏱️  Retry Pattern: 1min → 2min → 4min → 8min → 16min");
    info!("   💀 Dead Letter Exchange: comprehensive.orders.dlx");
    info!("   📪 Dead Letter Queue: comprehensive.orders.dlq");
    info!("   ⚙️  Settings: manual_ack=true, prefetch=1, concurrency=1");
    println!();

    // Create consumer
    let _consumer = Consumer::new(connection_manager.clone(), options).await?;
    let _handler = Arc::new(OrderProcessor);

    info!("🏭 Consumer created and ready to process messages");
    println!();

    // Publish test messages to demonstrate different scenarios
    info!("📨 Publishing test messages to demonstrate retry patterns...");
    let publisher = Publisher::new(connection_manager);

    let test_orders = vec![
        OrderEvent {
            order_id: "ORD-001".to_string(),
            customer_id: "CUST-123".to_string(),
            product_name: "Laptop Pro".to_string(),
            quantity: 1,
            price: 1299.99,
            event_type: "created".to_string(),
        },
        OrderEvent {
            order_id: "ORD-002".to_string(),
            customer_id: "CUST-456".to_string(),
            product_name: "Wireless Mouse".to_string(),
            quantity: 2,
            price: 49.99,
            event_type: "retry_payment".to_string(),
        },
        OrderEvent {
            order_id: "ORD-003".to_string(),
            customer_id: "CUST-789".to_string(),
            product_name: "Monitor 4K".to_string(),
            quantity: 1,
            price: 599.99,
            event_type: "fail_inventory".to_string(),
        },
        OrderEvent {
            order_id: "ORD-004".to_string(),
            customer_id: "CUST-101".to_string(),
            product_name: "Keyboard Mechanical".to_string(),
            quantity: 1,
            price: 149.99,
            event_type: "success".to_string(),
        },
        OrderEvent {
            order_id: "ORD-005".to_string(),
            customer_id: "INVALID".to_string(),
            product_name: "".to_string(),
            quantity: -1,
            price: -100.0,
            event_type: "permanent_failure".to_string(),
        },
    ];

    for order in test_orders {
        publisher
            .publish_to_exchange(
                "comprehensive.orders",
                "comprehensive.orders",
                &order,
                Some(PublishOptions::builder().auto_declare_exchange().build()),
            )
            .await?;

        info!(
            "📤 Published order: {} ({})",
            order.order_id, order.event_type
        );
        sleep(Duration::from_millis(500)).await;
    }

    println!();
    info!("🎯 Messages published! Here's what will happen:");
    info!("");
    info!("📋 Expected Processing Flow:");
    info!("  1. ORD-001 (created): ✅ Processed immediately");
    info!("  2. ORD-002 (retry_payment):");
    info!("     - ❌ Initial failure");
    info!("     - ⏱️  Retry after 1 minute");
    info!("     - ❌ Second failure");
    info!("     - ⏱️  Retry after 2 minutes");
    info!("     - ✅ Success on 3rd attempt");
    info!("  3. ORD-003 (fail_inventory):");
    info!("     - ❌ Initial failure");
    info!("     - ⏱️  Retry after 1 minute");
    info!("     - ❌ 1st retry failure");
    info!("     - ⏱️  Retry after 2 minutes");
    info!("     - ❌ 2nd retry failure");
    info!("     - ⏱️  Retry after 4 minutes");
    info!("     - ❌ 3rd retry failure");
    info!("     - ⏱️  Retry after 8 minutes");
    info!("     - ❌ 4th retry failure");
    info!("     - ⏱️  Retry after 16 minutes");
    info!("     - ❌ 5th retry failure");
    info!("     - 🚫 Send to dead letter exchange");
    info!("  4. ORD-004 (success): ✅ Processed immediately");
    info!("  5. ORD-005 (permanent_failure): ❌ Rejected → dead letter");
    println!();

    info!("⏰ In a real application, you would start the consumer:");
    info!("   consumer.consume(handler).await?;");
    println!();

    info!("🔍 To monitor the processing:");
    info!("   - Check RabbitMQ Management UI: http://localhost:15672");
    info!("   - Watch the queues: comprehensive.orders, comprehensive.orders.dlq");
    info!("   - Monitor delayed exchanges for retry messages");
    println!();

    info!("🎉 Demo complete! Key benefits of .minutes_retry():");
    info!("   ✅ Zero configuration - just one method call");
    info!("   ✅ Production-ready settings automatically applied");
    info!("   ✅ Intelligent retry timing for business-critical operations");
    info!("   ✅ Automatic dead letter handling");
    info!("   ✅ Type-safe message processing");
    info!("   ✅ Comprehensive error handling");
    println!();

    info!("🚀 This same infrastructure would require 15+ lines of manual configuration");
    info!("   without the .minutes_retry() preset - RustRabbit makes it effortless!");

    Ok(())
}

/*
Expected Output:

🚀 RustRabbit Comprehensive Example - All Features Demo
This example demonstrates:
  ✅ One-line setup with .minutes_retry()
  ✅ Auto-declaration of infrastructure
  ✅ Intelligent retry patterns (1min → 2min → 4min → 8min → 16min)
  ✅ Dead letter handling
  ✅ Multiple retry scenarios
  ✅ Production-ready configuration

📝 Setting up consumer with .minutes_retry() preset...
✅ Configuration automatically applied:
   📋 Queue: comprehensive.orders (auto-declared)
   🔄 Exchange: comprehensive.orders (auto-declared & bound)
   ⏱️  Retry Pattern: 1min → 2min → 4min → 8min → 16min
   💀 Dead Letter Exchange: comprehensive.orders.dlx
   📪 Dead Letter Queue: comprehensive.orders.dlq
   ⚙️  Settings: manual_ack=true, prefetch=1, concurrency=1

🏭 Consumer created and ready to process messages

📨 Publishing test messages to demonstrate retry patterns...
📤 Published order: ORD-001 (created)
📤 Published order: ORD-002 (retry_payment)
📤 Published order: ORD-003 (fail_inventory)
📤 Published order: ORD-004 (success)
📤 Published order: ORD-005 (permanent_failure)

🎯 Messages published! Here's what will happen:

📋 Expected Processing Flow:
  1. ORD-001 (created): ✅ Processed immediately
  2. ORD-002 (retry_payment):
     - ❌ Initial failure
     - ⏱️  Retry after 1 minute
     - ❌ Second failure
     - ⏱️  Retry after 2 minutes
     - ✅ Success on 3rd attempt
  3. ORD-003 (fail_inventory):
     - ❌ Initial failure → retry after 1 minute
     - ❌ 1st retry failure → retry after 2 minutes
     - ❌ 2nd retry failure → retry after 4 minutes
     - ❌ 3rd retry failure → retry after 8 minutes
     - ❌ 4th retry failure → retry after 16 minutes
     - ❌ 5th retry failure → send to dead letter
  4. ORD-004 (success): ✅ Processed immediately
  5. ORD-005 (permanent_failure): ❌ Rejected → dead letter

🎉 Demo complete! Key benefits of .minutes_retry():
   ✅ Zero configuration - just one method call
   ✅ Production-ready settings automatically applied
   ✅ Intelligent retry timing for business-critical operations
   ✅ Automatic dead letter handling
   ✅ Type-safe message processing
   ✅ Comprehensive error handling

🚀 This same infrastructure would require 15+ lines of manual configuration
   without the .minutes_retry() preset - RustRabbit makes it effortless!

Key Features Demonstrated:
1. 🔥 One-line setup: .minutes_retry() configures everything
2. 🏗️ Auto-infrastructure: Queue, exchange, bindings created automatically
3. ⏱️ Smart retry: Exponential backoff (1→2→4→8→16 minutes)
4. 💀 Dead letter: Automatic failure handling
5. 🎯 Type safety: Strongly typed message handling
6. 📊 Observability: Comprehensive logging and tracing
7. 🛡️ Reliability: Manual ACK, optimal prefetch settings
8. 🚀 Performance: Production-ready defaults
*/

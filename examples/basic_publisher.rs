//! Basic Publisher Example (Simplified)
//!
//! This example demonstrates core Publisher functionality:
//! - Direct queue publishing
//! - Exchange publishing with routing
//! - PublishOptions usage
//! - Different message types

use rust_rabbit::{Connection, PublishOptions, Publisher};
use serde::Serialize;
use tracing::info;

#[derive(Serialize)]
struct Order {
    id: u32,
    customer_id: u32,
    amount: f64,
    status: String,
}

#[derive(Serialize)]
struct Notification {
    recipient: String,
    subject: String,
    priority: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rust_rabbit::init_tracing();
    info!("Starting basic publisher examples");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    let publisher = Publisher::new(connection);

    // Example 1: Simple queue publishing
    info!("Publishing to queue...");

    let order = Order {
        id: 1001,
        customer_id: 123,
        amount: 299.99,
        status: "pending".to_string(),
    };

    publisher
        .publish_to_queue("order_queue", &order, None)
        .await?;
    info!("Order {} published to queue", order.id);

    // Example 2: Publishing with options
    let priority_order = Order {
        id: 1002,
        customer_id: 456,
        amount: 999.99,
        status: "urgent".to_string(),
    };

    let options = PublishOptions::new().priority(9).with_expiration("300000"); // 5 minutes TTL

    publisher
        .publish_to_queue("order_queue", &priority_order, Some(options))
        .await?;
    info!("Priority order {} published", priority_order.id);

    // Example 3: Exchange publishing with routing
    info!("Publishing to exchange...");

    let notification = Notification {
        recipient: "customer@example.com".to_string(),
        subject: "Order Confirmation".to_string(),
        priority: 5,
    };

    publisher
        .publish_to_exchange("notifications", "order.confirmation", &notification, None)
        .await?;
    info!("Notification published to exchange");

    // Example 4: High priority notification
    let urgent_notification = Notification {
        recipient: "admin@example.com".to_string(),
        subject: "High Value Order Alert".to_string(),
        priority: 9,
    };

    let urgent_options = PublishOptions::new().priority(9);

    publisher
        .publish_to_exchange(
            "notifications",
            "order.alert",
            &urgent_notification,
            Some(urgent_options),
        )
        .await?;
    info!("Urgent notification published");

    // Example 5: Batch publishing
    info!("Publishing batch messages...");

    for i in 1..=5 {
        let batch_order = Order {
            id: 2000 + i,
            customer_id: 200 + i,
            amount: (i as f64) * 50.0,
            status: if i % 2 == 0 { "priority" } else { "normal" }.to_string(),
        };

        let batch_options = if i % 2 == 0 {
            Some(PublishOptions::new().priority(7))
        } else {
            None
        };

        publisher
            .publish_to_queue("batch_orders", &batch_order, batch_options)
            .await?;
    }
    info!("Batch of 5 orders published");

    // Example 6: Different routing patterns
    let routing_examples = vec![
        ("order.created", "Order created notification"),
        ("order.updated", "Order updated notification"),
        ("order.completed", "Order completed notification"),
        ("user.registered", "User registration notification"),
    ];

    for (routing_key, description) in routing_examples {
        let routing_notification = Notification {
            recipient: "system@example.com".to_string(),
            subject: description.to_string(),
            priority: 3,
        };

        publisher
            .publish_to_exchange("events", routing_key, &routing_notification, None)
            .await?;

        info!("Published: {}", description);
    }

    info!("All publishing examples completed successfully!");
    Ok(())
}

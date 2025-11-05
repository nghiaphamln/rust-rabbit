//! Example: Using MassTransit option in existing publish functions
//!
//! This example shows how to use the MassTransit conversion option
//! with the existing publish functions instead of separate MassTransit methods.

use rust_rabbit::{Connection, MassTransitOptions, PublishOptions, Publisher};
use serde::Serialize;

#[derive(Serialize)]
struct OrderCreated {
    order_id: u32,
    customer_id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to RabbitMQ
    let connection = Connection::new("amqp://localhost:5672").await?;
    let publisher = Publisher::new(connection);

    let order = OrderCreated {
        order_id: 12345,
        customer_id: 67890,
        amount: 99.99,
    };

    // Example 1: Simple MassTransit conversion with message type
    // Message type in format "Namespace:TypeName" (will be converted to URN automatically)
    println!("Example 1: Publishing with MassTransit option (simple)");
    publisher
        .publish_to_exchange(
            "order-exchange",
            "order.created",
            &order,
            Some(
                PublishOptions::new().with_masstransit("Contracts:OrderCreated"), // Simple format
            ),
        )
        .await?;

    // Example 2: MassTransit with URN format message type
    println!("Example 2: Publishing with MassTransit option (URN format)");
    publisher
        .publish_to_queue(
            "order-queue",
            &order,
            Some(
                PublishOptions::new().with_masstransit("urn:message:Contracts:OrderCreated"), // URN format
            ),
        )
        .await?;

    // Example 3: MassTransit with full options (correlation ID, custom addresses)
    println!("Example 3: Publishing with full MassTransit options");
    let mt_options = MassTransitOptions::new("Contracts:OrderCreated")
        .with_correlation_id("correlation-12345")
        .with_source_address("rabbitmq://myhost/order-exchange")
        .with_destination_address("rabbitmq://myhost/order.created");

    publisher
        .publish_to_exchange(
            "order-exchange",
            "order.created",
            &order,
            Some(PublishOptions::new().with_masstransit_options(mt_options)),
        )
        .await?;

    // Example 4: Regular publish without MassTransit (default behavior)
    println!("Example 4: Publishing without MassTransit (regular format)");
    publisher
        .publish_to_exchange(
            "order-exchange",
            "order.created",
            &order,
            None, // No MassTransit conversion
        )
        .await?;

    // Example 5: Combining MassTransit with other options
    println!("Example 5: MassTransit with expiration and priority");
    publisher
        .publish_to_exchange(
            "order-exchange",
            "order.created",
            &order,
            Some(
                PublishOptions::new()
                    .with_masstransit("Contracts:OrderCreated")
                    .with_expiration("60000") // 60 seconds
                    .with_priority(5),
            ),
        )
        .await?;

    println!("All messages published successfully!");

    Ok(())
}

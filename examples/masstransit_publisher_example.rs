//! Example: Publishing messages to MassTransit-compatible services
//!
//! This example demonstrates how to publish messages that will be accepted
//! by MassTransit services. MassTransit has strict validation and will reject
//! or send messages to skip queue if the format is incorrect.

use rust_rabbit::{Connection, MassTransitEnvelope, Publisher};
use serde::Serialize;

#[derive(Serialize)]
struct OrderCreated {
    order_id: u32,
    customer_id: u32,
    amount: f64,
    timestamp: String,
}

#[derive(Serialize)]
struct PaymentProcessed {
    payment_id: u32,
    order_id: u32,
    amount: f64,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to RabbitMQ
    let connection = Connection::new("amqp://localhost:5672").await?;
    let publisher = Publisher::new(connection);

    // Example 1: Publish to exchange with message type
    // This is the simplest way - message type is required for MassTransit routing
    println!("Example 1: Publishing OrderCreated to exchange");
    let order = OrderCreated {
        order_id: 12345,
        customer_id: 67890,
        amount: 99.99,
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    publisher
        .publish_masstransit_to_exchange(
            "order-exchange",
            "order.created", // routing key
            &order,
            "Contracts:OrderCreated", // Message type - matches C# contract namespace
            None,
        )
        .await?;

    // Example 2: Publish to queue directly
    println!("Example 2: Publishing PaymentProcessed to queue");
    let payment = PaymentProcessed {
        payment_id: 11111,
        order_id: 12345,
        amount: 99.99,
        status: "Completed".to_string(),
    };

    publisher
        .publish_masstransit_to_queue(
            "payment-queue",
            &payment,
            "Contracts:PaymentProcessed", // Message type
            None,
        )
        .await?;

    // Example 3: Create custom MassTransit envelope with correlation ID
    println!("Example 3: Publishing with custom envelope and correlation ID");
    let order2 = OrderCreated {
        order_id: 54321,
        customer_id: 98765,
        amount: 199.99,
        timestamp: "2024-01-01T13:00:00Z".to_string(),
    };

    let envelope = MassTransitEnvelope::with_message_type(&order2, "Contracts:OrderCreated")?
        .with_correlation_id("correlation-12345")
        .with_source_address("rabbitmq://localhost/order-exchange")
        .with_destination_address("rabbitmq://localhost/order.created")
        .with_header("X-Custom-Header", serde_json::json!("custom-value"));

    publisher
        .publish_masstransit_envelope_to_exchange(
            "order-exchange",
            "order.created",
            &envelope,
            None,
        )
        .await?;

    // Example 4: Using URN format for message type (alternative format)
    println!("Example 4: Publishing with URN message type format");
    let order3 = OrderCreated {
        order_id: 99999,
        customer_id: 88888,
        amount: 299.99,
        timestamp: "2024-01-01T14:00:00Z".to_string(),
    };

    // MassTransit also accepts URN format: "urn:message:Namespace:MessageType"
    publisher
        .publish_masstransit_to_exchange(
            "order-exchange",
            "order.created",
            &order3,
            "urn:message:Contracts:OrderCreated", // URN format
            None,
        )
        .await?;

    println!("All messages published successfully!");

    Ok(())
}

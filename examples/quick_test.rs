use rust_rabbit::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage {
    id: u32,
    content: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create connection
    let connection = Arc::new(Connection::new("amqp://guest:guest@localhost:5672").await?);

    // Create publisher
    let publisher = Publisher::new(Arc::clone(&connection));

    // Create some test messages
    let messages = vec![
        TestMessage {
            id: 1,
            content: "Hello World!".to_string(),
        },
        TestMessage {
            id: 2,
            content: "Rust Rabbit is simple!".to_string(),
        },
        TestMessage {
            id: 3,
            content: "Easy messaging!".to_string(),
        },
    ];

    // Publish messages to queue
    println!("Publishing messages...");
    for msg in &messages {
        publisher.publish_to_queue("test.simple", msg, None).await?;
        println!("Published message: {:?}", msg);
    }

    // Create consumer
    let consumer = Consumer::builder(Arc::clone(&connection), "test.simple").build();

    // Start consuming (will run forever)
    println!("Starting consumer...");
    consumer
        .consume(|message: TestMessage| async move {
            println!("Received message: {:?}", message);

            // Simulate some processing
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            Ok(())
        })
        .await?;

    Ok(())
}

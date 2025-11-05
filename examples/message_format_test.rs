//! Test message format
//!
//! This example shows the new message format when publishing and consuming

use rust_rabbit::{Connection, Consumer, Publisher, RetryConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TestMessage {
    id: u32,
    content: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    println!("🧪 Testing message format...");

    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Test Publisher
    let publisher = Publisher::new(connection.clone());
    let test_msg = TestMessage {
        id: 123,
        content: "Hello World!".to_string(),
    };

    println!("📤 Publishing message: {:?}", test_msg);
    publisher
        .publish_to_queue("test_format_queue", &test_msg, None)
        .await?;
    println!("✅ Message published successfully");

    // Test Consumer (run briefly then exit)
    let consumer = Consumer::builder(connection, "test_format_queue")
        .with_retry(RetryConfig::no_retry())
        .build();

    println!("📥 Starting consumer...");

    // Process messages
    consumer
        .consume(|msg: TestMessage| async move {
            println!("🎯 Received message:");
            println!("  - Data: {:?}", msg);
            println!("✅ Message format test successful!");

            // For demo purposes, exit after first message
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            std::process::exit(0);
        })
        .await?;

    Ok(())
}

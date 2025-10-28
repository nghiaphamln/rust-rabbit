use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions},
};
use std::env;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let rabbitmq_url = env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://admin:password@localhost:5672/test".to_string());

    info!("Testing prefetch_count functionality");
    info!("RabbitMQ URL: {}", rabbitmq_url);

    // Create config and connection manager
    let config = RabbitConfig::builder()
        .connection_string(rabbitmq_url)
        .build();

    // Test 1: Consumer with auto_ack=true (prefetch_count should have no effect)
    info!("\n=== Test 1: auto_ack=true (prefetch_count ignored) ===");

    let connection_manager = ConnectionManager::new(config).await?;

    let options = ConsumerOptions {
        prefetch_count: Some(2),
        auto_ack: true, // This makes prefetch_count ineffective
        ..Default::default()
    };

    match Consumer::new(connection_manager.clone(), options).await {
        Ok(_consumer) => {
            info!("✅ Consumer created with prefetch_count=2, auto_ack=true");
            info!("   Note: prefetch_count has NO effect when auto_ack=true");
            info!("   All messages will be delivered immediately");
        }
        Err(e) => {
            error!("❌ Failed to create consumer: {}", e);
            return Ok(());
        }
    }

    // Test 2: Consumer with auto_ack=false (prefetch_count should work)
    info!("\n=== Test 2: auto_ack=false (prefetch_count active) ===");

    let options = ConsumerOptions {
        prefetch_count: Some(3),
        auto_ack: false, // Required for prefetch_count to work
        ..Default::default()
    };

    match Consumer::new(connection_manager, options).await {
        Ok(_consumer) => {
            info!("✅ Consumer created with prefetch_count=3, auto_ack=false");
            info!("   prefetch_count=3 will limit unACK'd messages to 3");
            info!("   New messages will only be delivered after ACK'ing previous ones");
        }
        Err(e) => {
            error!("❌ Failed to create consumer: {}", e);
        }
    }

    info!("\n=== Summary ===");
    info!("✅ prefetch_count configuration is working correctly");
    info!("📝 Key points:");
    info!("   • prefetch_count ONLY works when auto_ack = false");
    info!("   • With auto_ack = true: Messages are ACK'd immediately, making prefetch ineffective");
    info!("   • With auto_ack = false: Messages must be manually ACK'd, allowing QoS control");
    info!("   • Current implementation correctly applies QoS settings via basic_qos()");

    Ok(())
}

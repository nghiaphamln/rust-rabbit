use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions},
    retry::RetryPolicy,
};
use std::env;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let rabbitmq_url = env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://admin:password@localhost:5672/test".to_string());

    info!("🚀 Creating Fast Consumer with Retry Configuration");

    // Step 1: Create config and connection
    let config = RabbitConfig::builder()
        .connection_string(rabbitmq_url)
        .build();

    let connection_manager = ConnectionManager::new(config).await?;

    // Step 2: Quick retry policies (chọn 1 trong các cách sau)

    info!("\n=== Cách 1: Sử dụng preset có sẵn ===");

    // Cách 1a: Fast preset - Retry nhanh cho transient errors
    let fast_retry = RetryPolicy::fast();
    info!("✅ Fast retry policy:");
    info!("   • Max retries: 5");
    info!("   • Initial delay: 200ms");
    info!("   • Max delay: 10s");
    info!("   • Backoff: 1.5x");
    info!("   • DLX: fast.dlx, DLQ: fast.dlq");

    // Cách 1b: Fast retry cho queue cụ thể
    let queue_specific_retry = RetryPolicy::fast_for_queue("my_queue");
    info!("✅ Queue-specific fast retry:");
    info!("   • DLX: my_queue.dlx, DLQ: my_queue.dlq");

    info!("\n=== Cách 2: Sử dụng builder pattern ===");

    // Cách 2: Custom với builder (nhanh và linh hoạt)
    let custom_retry = RetryPolicy::builder()
        .fast_preset() // Áp dụng fast preset
        .max_retries(3) // Override max retries
        .build();

    info!("✅ Custom fast retry with builder:");
    info!("   • Uses fast preset + custom max_retries=3");

    // Cách 3: Custom hoàn toàn
    let ultra_fast_retry = RetryPolicy::builder()
        .max_retries(3)
        .initial_delay(std::time::Duration::from_millis(100)) // 100ms
        .max_delay(std::time::Duration::from_secs(5)) // 5s max
        .backoff_multiplier(2.0) // 2x backoff
        .jitter(0.1) // 10% jitter
        .dead_letter_exchange("urgent.dlx")
        .dead_letter_queue("urgent.dlq")
        .build();

    info!("✅ Ultra-fast custom retry:");
    info!("   • 3 retries, 100ms→200ms→400ms→800ms (capped at 5s)");

    // Step 3: Create consumer với retry policy
    info!("\n=== Tạo Consumer với Retry ===");

    let options = ConsumerOptions {
        auto_ack: false,                // Cần thiết cho retry
        prefetch_count: Some(10),       // Xử lý 10 messages đồng thời
        retry_policy: Some(fast_retry), // Chọn retry policy
        ..Default::default()
    };

    match Consumer::new(connection_manager, options).await {
        Ok(consumer) => {
            info!("✅ Consumer created successfully with retry!");

            // Step 4: Start consuming (example logic)
            info!("\n=== Example Usage ===");
            print_usage_example();
        }
        Err(e) => {
            error!("❌ Failed to create consumer: {}", e);
        }
    }

    Ok(())
}

fn print_usage_example() {
    info!("📝 Cách sử dụng consumer với retry:");
    println!(
        r#"
consumer.consume("my_queue", |delivery| async move {{
    // Xử lý message
    match process_message(&delivery.data).await {{
        Ok(_) => {{
            // Thành công -> ACK
            delivery.ack(Default::default()).await?;
            Ok(())
        }}
        Err(e) if is_retryable_error(&e) => {{
            // Lỗi có thể retry -> NACK (sẽ tự động retry)
            warn!("Retryable error: {{}}, will retry", e);
            delivery.nack(Default::default()).await?;
            Ok(())
        }}
        Err(e) => {{
            // Lỗi không thể retry -> Reject (đưa vào DLQ)
            error!("Non-retryable error: {{}}, sending to DLQ", e);
            delivery.reject(Default::default()).await?;
            Ok(())
        }}
    }}
}}).await?;
"#
    );

    info!("\n🎯 Quick Setup Patterns:");

    info!("🔥 SIÊU NHANH - Copy/paste này:");
    println!(
        r#"
// Fast consumer với retry trong 5 dòng:
let config = RabbitConfig::builder().connection_string("amqp://...").build();
let connection = ConnectionManager::new(config).await?;
let options = ConsumerOptions {{
    auto_ack: false,
    retry_policy: Some(RetryPolicy::fast()),
    ..Default::default()
}};
let consumer = Consumer::new(connection, options).await?;
"#
    );

    info!("⚡ Hoặc custom nhanh:");
    println!(
        r#"
let retry = RetryPolicy::builder()
    .fast_preset()
    .max_retries(3)
    .build();
"#
    );
}

// Helper functions cho ví dụ
async fn process_message(_data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Simulate processing
    Ok(())
}

fn is_retryable_error(_e: &Box<dyn std::error::Error>) -> bool {
    // Determine if error should be retried
    true
}

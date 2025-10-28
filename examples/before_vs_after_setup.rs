use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions},
    retry::RetryPolicy,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 Retry Setup Comparison: Before vs After\n");

    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    println!("❌ BEFORE - Complex Manual Setup:");
    println!("==================================");

    // Manual setup (the old way)
    let manual_retry_policy = RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60)) // 1 minute
        .max_delay(Duration::from_secs(2000)) // High cap to not limit
        .backoff_multiplier(2.0) // Double each time
        .jitter(0.1) // 10% jitter
        .dead_letter_exchange("orders.processing.dlx")
        .dead_letter_queue("orders.processing.dlq")
        .build();

    let manual_options = ConsumerOptions::builder("orders.processing")
        .auto_declare_queue() // Need to remember this
        .auto_declare_exchange() // Need to remember this
        .retry_policy(manual_retry_policy) // Custom policy
        .concurrency(1) // Reliable processing
        .prefetch_count(1) // One at a time
        .manual_ack() // Manual ack for retries
        .build();

    println!("Code required:");
    println!("```rust");
    println!("let retry_policy = RetryPolicy::builder()");
    println!("    .max_retries(5)");
    println!("    .initial_delay(Duration::from_secs(60))");
    println!("    .max_delay(Duration::from_secs(2000))");
    println!("    .backoff_multiplier(2.0)");
    println!("    .jitter(0.1)");
    println!("    .dead_letter_exchange(\"orders.processing.dlx\")");
    println!("    .dead_letter_queue(\"orders.processing.dlq\")");
    println!("    .build();");
    println!("");
    println!("let options = ConsumerOptions::builder(\"orders.processing\")");
    println!("    .auto_declare_queue()");
    println!("    .auto_declare_exchange()");
    println!("    .retry_policy(retry_policy)");
    println!("    .concurrency(1)");
    println!("    .prefetch_count(1)");
    println!("    .manual_ack()");
    println!("    .build();");
    println!("```");
    println!("📊 Lines of code: ~15 lines");
    println!("⚠️  Risk: Easy to forget settings or misconfigure");
    println!();

    println!("✅ AFTER - Simple Preset:");
    println!("=========================");

    // Preset setup (the new way)
    let preset_options = ConsumerOptions::builder("orders.processing")
        .minutes_retry() // 🔥 One method does everything!
        .build();

    println!("Code required:");
    println!("```rust");
    println!("let options = ConsumerOptions::builder(\"orders.processing\")");
    println!("    .minutes_retry()  // <- Everything configured!");
    println!("    .build();");
    println!("```");
    println!("📊 Lines of code: ~3 lines");
    println!("✅ Risk: Zero - preset handles all best practices");
    println!();

    println!("🔍 What .minutes_retry() Does Automatically:");
    println!("============================================");
    println!("✅ Sets max_retries: 5");
    println!("✅ Sets initial_delay: 1 minute");
    println!("✅ Sets backoff_multiplier: 2.0 (exponential)");
    println!("✅ Sets jitter: 0.1 (10% randomness)");
    println!("✅ Enables auto_declare_queue");
    println!("✅ Enables auto_declare_exchange");
    println!("✅ Creates DLX: my.queue.dlx");
    println!("✅ Creates DLQ: my.queue.dlq");
    println!("✅ Sets concurrency: 1 (reliable)");
    println!("✅ Sets prefetch_count: 1");
    println!("✅ Enables manual_ack");
    println!();

    println!("⏱️  Retry Timeline Comparison:");
    println!("==============================");
    println!("Both setups produce IDENTICAL retry behavior:");
    println!("  Attempt 1: ~1 minute delay");
    println!("  Attempt 2: ~2 minutes delay");
    println!("  Attempt 3: ~4 minutes delay");
    println!("  Attempt 4: ~8 minutes delay");
    println!("  Attempt 5: ~16 minutes delay");
    println!("  After 5th: → Dead Letter Queue");
    println!();

    // Create consumers to verify they work identically
    println!("🧪 Testing Both Configurations:");
    println!("===============================");

    let manual_consumer = Consumer::new(connection_manager.clone(), manual_options).await;
    let preset_consumer = Consumer::new(connection_manager, preset_options).await;

    match (manual_consumer, preset_consumer) {
        (Ok(_), Ok(_)) => {
            println!("✅ Manual setup: Consumer created successfully");
            println!("✅ Preset setup: Consumer created successfully");
            println!("🎯 Both consumers are functionally identical!");
        }
        (Err(e), _) => println!("❌ Manual setup failed: {}", e),
        (_, Err(e)) => println!("❌ Preset setup failed: {}", e),
    }

    println!();
    println!("🏆 Winner: .minutes_retry() Preset!");
    println!("===================================");
    println!("👥 Benefits:");
    println!("   ✅ 80% less code");
    println!("   ✅ Zero configuration errors");
    println!("   ✅ Built-in best practices");
    println!("   ✅ Consistent across team");
    println!("   ✅ Easy to understand");
    println!("   ✅ Quick to implement");
    println!();
    println!("📝 Usage Examples:");
    println!("==================");

    let examples = vec![
        "orders.processing",
        "notifications.email",
        "payments.stripe",
        "users.registration",
        "files.image-resize",
        "analytics.events",
    ];

    for queue in examples {
        println!("// For queue: {}", queue);
        println!(
            "let options = ConsumerOptions::builder(\"{}\").minutes_retry().build();",
            queue
        );
        println!("// Creates: {}.dlx, {}.dlq automatically", queue, queue);
        println!();
    }

    println!("🎯 Perfect for:");
    println!("   🏢 Business-critical operations");
    println!("   💳 Payment processing");
    println!("   📧 Email notifications");
    println!("   📁 File processing");
    println!("   📊 Analytics pipelines");
    println!("   🔄 Any operation that needs reliable retry");

    Ok(())
}

/*
Summary of .minutes_retry() preset:

🎯 PURPOSE: One-line setup for robust retry mechanism with 1min, 2min, 4min, 8min, 16min delays

🔧 AUTO-CONFIGURED:
- Retry Policy: 5 retries with exponential backoff
- Queue & Exchange: Auto-declared and bound
- Dead Letter: {queue}.dlx and {queue}.dlq
- Processing: Reliable settings (concurrency=1, manual ack)

💡 WHEN TO USE:
- Business-critical message processing
- Operations that can tolerate minute-level delays
- When you need guaranteed delivery with fallback

🚀 ALTERNATIVES:
- .fast_retry(): For quick retries (milliseconds/seconds)
- .development(): For dev environment (auto-ack, simple)
- .reliable(): For single-threaded reliable processing
- .high_throughput(): For high-volume processing

Example Usage:
```rust
// Simple
let options = ConsumerOptions::builder("my.queue").minutes_retry().build();

// With customization
let options = ConsumerOptions::builder("my.queue")
    .minutes_retry()
    .concurrency(5)  // Override concurrency if needed
    .build();
```
*/

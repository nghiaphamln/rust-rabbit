use anyhow::Result;
use rust_rabbit::patterns::deduplication::*;
use rust_rabbit::patterns::priority::*;
use rust_rabbit::patterns::request_response::*;
use std::sync::Arc;
use std::time::Duration;

/// Example demonstrating all Phase 2 advanced messaging patterns
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 RustRabbit Phase 2 - Advanced Messaging Patterns Demo");
    println!("========================================================");

    // 1. Request-Response Pattern
    println!("\n📡 1. Request-Response Pattern Demo");
    demo_request_response().await?;

    // 2. Priority Queues
    println!("\n🎯 2. Priority Queues Demo");
    demo_priority_queues().await?;

    // 3. Message Deduplication
    println!("\n🔍 3. Message Deduplication Demo");
    demo_deduplication().await?;

    println!("\n✅ All Phase 2 demos completed successfully!");
    Ok(())
}

async fn demo_request_response() -> Result<()> {
    println!("   Setting up Request-Response client...");

    let _client = RequestResponseClient::new(Duration::from_secs(30));

    // Simulate sending a request
    println!("   📤 Sending request message...");
        let _request_payload = b"{'action': 'get_user', 'user_id': 123}";

    // In a real scenario, this would integrate with RabbitMQ
    println!("   ⏳ Would send request to queue and wait for response...");
    println!("   ✅ Request-Response pattern ready for integration");

    Ok(())
}

async fn demo_priority_queues() -> Result<()> {
    println!("   Creating priority queue...");

    let config = PriorityQueueConfig::default();
    let queue = Arc::new(PriorityQueue::new(config));

    // Add messages with different priorities
    println!("   📥 Adding messages with different priorities...");

    queue.enqueue(PriorityMessage::new(
        b"Normal priority task".to_vec(),
        Priority::Normal,
    ))?;

    queue.enqueue(PriorityMessage::new(
        b"Critical system alert!".to_vec(),
        Priority::Critical,
    ))?;

    queue.enqueue(PriorityMessage::new(
        b"Low priority cleanup".to_vec(),
        Priority::Low,
    ))?;

    queue.enqueue(PriorityMessage::new(
        b"High priority order".to_vec(),
        Priority::High,
    ))?;

    println!("   📊 Queue size: {}", queue.size());

    // Process messages in priority order
    println!("   🔄 Processing messages in priority order:");
    while let Some(message) = queue.dequeue() {
        println!(
            "      {:?}: {}",
            message.priority,
            String::from_utf8_lossy(&message.payload)
        );
    }

    println!("   ✅ Priority queue demo completed");
    Ok(())
}

async fn demo_deduplication() -> Result<()> {
    println!("   Setting up message deduplication...");

    let config = DeduplicationConfig::default();
    let manager = DeduplicationManager::new(config);

    // Create a message
    let message = DeduplicatedMessage::new(b"Important business event".to_vec())
        .with_custom_key("order_123".to_string());

    println!("   📨 Processing message first time...");
    match manager.check_duplicate(&message)? {
        DeduplicationResult::Unique => {
            println!("   ✅ Message is unique, processing...");
        }
        DeduplicationResult::Duplicate(info) => {
            println!("   ⚠️  Duplicate detected: {:?}", info);
        }
    }

    println!("   📨 Processing same message again...");
    match manager.check_duplicate(&message)? {
        DeduplicationResult::Unique => {
            println!("   ✅ Message is unique, processing...");
        }
        DeduplicationResult::Duplicate(info) => {
            println!("   ⚠️  Duplicate detected! Count: {}", info.duplicate_count);
        }
    }

    // Show cache stats
    let stats = manager.cache_stats();
    println!(
        "   📊 Cache stats: {} entries, {:.1}% hit rate",
        stats.total_entries, stats.cache_hit_rate
    );

    println!("   ✅ Deduplication demo completed");
    Ok(())
}

// Demonstrate different priority strategies
async fn demo_priority_strategies() -> Result<()> {
    println!("\n🎯 Priority Strategy Examples:");

    // Show priority values
    println!("   Priority Levels:");
    println!(
        "   - Critical: {} (emergencies, alerts)",
        Priority::Critical.value()
    );
    println!("   - High: {} (important orders)", Priority::High.value());
    println!(
        "   - Normal: {} (regular processing)",
        Priority::Normal.value()
    );
    println!("   - Low: {} (cleanup, maintenance)", Priority::Low.value());

    // Example use cases
    println!("\n   📋 Example Use Cases:");
    println!("   🚨 Critical: System alerts, security breaches");
    println!("   ⚡ High: Customer orders, payment processing");
    println!("   📝 Normal: Regular notifications, updates");
    println!("   🧹 Low: Log cleanup, batch reports");

    Ok(())
}

// Demonstrate deduplication strategies
async fn demo_dedup_strategies() -> Result<()> {
    println!("\n🔍 Deduplication Strategy Examples:");

    let payload = b"sample message".to_vec();
    let message = DeduplicatedMessage::new(payload).with_custom_key("business_key_123".to_string());

    println!("   Strategy comparisons for same message:");
    println!(
        "   - Message ID: {}",
        message.get_dedup_key(&DeduplicationStrategy::MessageId)
    );
    println!(
        "   - Content Hash: {}",
        message.get_dedup_key(&DeduplicationStrategy::ContentHash)
    );
    println!(
        "   - Custom Key: {}",
        message.get_dedup_key(&DeduplicationStrategy::CustomKey("test".to_string()))
    );

    Ok(())
}

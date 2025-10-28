use async_trait::async_trait;
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions, MessageContext, MessageHandler, MessageResult},
    error::Result,
    retry::RetryPolicy,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct HeavyTaskMessage {
    task_id: String,
    operation: String,
}

struct HeavyTaskHandler;

#[async_trait]
impl MessageHandler<HeavyTaskMessage> for HeavyTaskHandler {
    async fn handle(&self, message: HeavyTaskMessage, context: MessageContext) -> MessageResult {
        info!(
            "🔄 Processing heavy task {} (operation: {}, attempt: {})",
            message.task_id,
            message.operation,
            context.retry_count + 1
        );

        // Simulate heavy processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        match message.operation.as_str() {
            "succeed_eventually" => {
                if context.retry_count < 2 {
                    warn!(
                        "❌ Heavy task {} failed, will retry with minutes delay",
                        message.task_id
                    );
                    MessageResult::Retry
                } else {
                    info!(
                        "✅ Heavy task {} succeeded after {} attempts",
                        message.task_id,
                        context.retry_count + 1
                    );
                    MessageResult::Ack
                }
            }
            "always_fail" => {
                warn!(
                    "❌ Heavy task {} failed (will retry with exponential minutes delay)",
                    message.task_id
                );
                MessageResult::Retry
            }
            "succeed_immediately" => {
                info!("✅ Heavy task {} succeeded immediately", message.task_id);
                MessageResult::Ack
            }
            _ => MessageResult::Ack,
        }
    }
}

fn demonstrate_delay_calculations() {
    info!("📊 Demonstrating delay calculations for minutes exponential pattern:");

    let policy = RetryPolicy::minutes_exponential();

    info!("Policy configuration:");
    info!("  - max_retries: {}", policy.max_retries);
    info!("  - initial_delay: {:?}", policy.initial_delay);
    info!("  - max_delay: {:?}", policy.max_delay);
    info!("  - backoff_multiplier: {}", policy.backoff_multiplier);
    info!("  - jitter: {}", policy.jitter);

    info!("Delay pattern:");
    for attempt in 0..policy.max_retries {
        let delay = policy.calculate_delay(attempt);
        let minutes = delay.as_secs() / 60;
        let seconds = delay.as_secs() % 60;
        info!(
            "  Attempt {}: {}m{}s ({:?})",
            attempt + 1,
            minutes,
            seconds,
            delay
        );
    }
}

fn demonstrate_custom_configurations() {
    info!("🔧 Custom configurations for different minute patterns:");

    // Exact pattern: 1, 2, 4, 8, 16 minutes (no jitter, high max_delay)
    let exact_pattern = RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60))
        .max_delay(Duration::from_secs(2000)) // Higher than 16min to avoid capping
        .backoff_multiplier(2.0)
        .jitter(0.0) // No jitter for exact timing
        .dead_letter_exchange("exact.dlx")
        .build();

    info!("Exact Pattern (no jitter, no cap):");
    for attempt in 0..exact_pattern.max_retries {
        let delay = exact_pattern.calculate_delay(attempt);
        let minutes = delay.as_secs() / 60;
        info!("  Attempt {}: {}m", attempt + 1, minutes);
    }

    // Conservative pattern: 1, 2, 4, 8 minutes (capped at 10 minutes)
    let conservative_pattern = RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60))
        .max_delay(Duration::from_secs(600)) // 10 minutes cap
        .backoff_multiplier(2.0)
        .jitter(0.1)
        .dead_letter_exchange("conservative.dlx")
        .build();

    info!("Conservative Pattern (10min cap, with jitter):");
    for attempt in 0..conservative_pattern.max_retries {
        let delay = conservative_pattern.calculate_delay(attempt);
        let minutes = delay.as_secs() / 60;
        let seconds = delay.as_secs() % 60;
        info!(
            "  Attempt {}: ~{}m{}s (with jitter)",
            attempt + 1,
            minutes,
            seconds
        );
    }

    // Custom pattern: 2, 6, 18, 54 minutes (3x multiplier)
    let triple_pattern = RetryPolicy::builder()
        .max_retries(4)
        .initial_delay(Duration::from_secs(120)) // 2 minutes
        .max_delay(Duration::from_secs(4000)) // High cap
        .backoff_multiplier(3.0) // Triple each time
        .jitter(0.05)
        .dead_letter_exchange("triple.dlx")
        .build();

    info!("Triple Pattern (3x multiplier):");
    for attempt in 0..triple_pattern.max_retries {
        let delay = triple_pattern.calculate_delay(attempt);
        let minutes = delay.as_secs() / 60;
        info!("  Attempt {}: {}m", attempt + 1, minutes);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("⏱️  Minutes Exponential Retry Demo");
    info!("Pattern: 1min → 2min → 4min → 8min → 16min");

    // Demonstrate delay calculations
    demonstrate_delay_calculations();
    println!();

    // Show custom configurations
    demonstrate_custom_configurations();
    println!();

    // Setup actual consumer
    info!("🚀 Setting up consumer with minutes exponential retry policy...");

    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // Use the new preset method
    let retry_policy = RetryPolicy::minutes_exponential();

    let options = ConsumerOptions::builder("heavy.tasks")
        .auto_declare_queue()
        .auto_declare_exchange()
        .retry_policy(retry_policy)
        .prefetch_count(1) // Process one at a time for heavy tasks
        .build();

    let _consumer = Consumer::new(connection_manager.clone(), options).await?;
    let _handler = Arc::new(HeavyTaskHandler);

    info!("✅ Consumer setup complete!");
    info!("Ready to process heavy tasks with minutes exponential retry:");
    info!("  - 1st retry: 1 minute delay");
    info!("  - 2nd retry: 2 minutes delay");
    info!("  - 3rd retry: 4 minutes delay");
    info!("  - 4th retry: 8 minutes delay");
    info!("  - 5th retry: 16 minutes delay");
    info!("  - After 5th retry: Send to dead letter");

    // In a real application, you would start the consumer:
    // consumer.consume::<HeavyTaskMessage, _>(handler).await?;

    // Demo publish some test messages
    info!("📨 Publishing test messages...");

    use rust_rabbit::publisher::{PublishOptions, Publisher};
    let publisher = Publisher::new(connection_manager);

    let test_messages = vec![
        HeavyTaskMessage {
            task_id: "task_001".to_string(),
            operation: "succeed_immediately".to_string(),
        },
        HeavyTaskMessage {
            task_id: "task_002".to_string(),
            operation: "succeed_eventually".to_string(),
        },
        HeavyTaskMessage {
            task_id: "task_003".to_string(),
            operation: "always_fail".to_string(),
        },
    ];

    for message in test_messages {
        publisher
            .publish_to_exchange(
                "heavy.tasks",
                "heavy.tasks",
                &message,
                Some(PublishOptions::builder().auto_declare_exchange().build()),
            )
            .await?;

        info!("Published task: {}", message.task_id);
    }

    info!("🎯 Demo complete! In production:");
    info!("  1. Start the consumer to process messages");
    info!("  2. Failed messages will retry with exponential minute delays");
    info!("  3. Monitor the dead letter queue for permanently failed messages");

    Ok(())
}

/*
Expected behavior:

1. task_001 (succeed_immediately): ✅ Processed immediately, no retries

2. task_002 (succeed_eventually):
   - ❌ Initial failure
   - ⏱️  Retry after 1 minute
   - ❌ Second failure
   - ⏱️  Retry after 2 minutes
   - ✅ Success on 3rd attempt

3. task_003 (always_fail):
   - ❌ Initial failure
   - ⏱️  Retry after 1 minute
   - ❌ 1st retry failure
   - ⏱️  Retry after 2 minutes
   - ❌ 2nd retry failure
   - ⏱️  Retry after 4 minutes
   - ❌ 3rd retry failure
   - ⏱️  Retry after 8 minutes
   - ❌ 4th retry failure
   - ⏱️  Retry after 16 minutes
   - ❌ 5th retry failure
   - 🚫 Send to dead letter exchange
*/

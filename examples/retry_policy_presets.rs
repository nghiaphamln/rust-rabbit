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
struct TaskMessage {
    task_id: String,
    task_type: String,
}

struct TaskHandler;

#[async_trait]
impl MessageHandler<TaskMessage> for TaskHandler {
    async fn handle(&self, message: TaskMessage, context: MessageContext) -> MessageResult {
        info!(
            "Processing task {} (type: {}, attempt: {})",
            message.task_id,
            message.task_type,
            context.retry_count + 1
        );

        match message.task_type.as_str() {
            "success" => MessageResult::Ack,
            "retry" => {
                if context.retry_count < 2 {
                    warn!("Task {} failed, will retry", message.task_id);
                    MessageResult::Retry
                } else {
                    info!("Task {} succeeded after retries", message.task_id);
                    MessageResult::Ack
                }
            }
            _ => MessageResult::Ack,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Retry Policy Presets Demo");

    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // Demo 1: Using preset methods
    info!("📝 Demo 1: Using RetryPolicy preset methods");

    let fast_policy = RetryPolicy::fast();
    info!("Fast Policy: {:?}", fast_policy);

    let slow_policy = RetryPolicy::slow();
    info!("Slow Policy: {:?}", slow_policy);

    let aggressive_policy = RetryPolicy::aggressive();
    info!("Aggressive Policy: {:?}", aggressive_policy);

    let conservative_policy = RetryPolicy::conservative();
    info!("Conservative Policy: {:?}", conservative_policy);

    let linear_policy = RetryPolicy::linear(Duration::from_secs(2), 5);
    info!("Linear Policy: {:?}", linear_policy);

    let no_retry_policy = RetryPolicy::no_retry();
    info!("No Retry Policy: {:?}", no_retry_policy);

    // Demo 2: Using builder pattern
    info!("📝 Demo 2: Using RetryPolicy builder pattern");

    let custom_policy = RetryPolicy::builder()
        .max_retries(7)
        .initial_delay(Duration::from_millis(500))
        .max_delay(Duration::from_secs(45))
        .backoff_multiplier(1.8)
        .jitter(0.15)
        .dead_letter_exchange("custom.dlx")
        .dead_letter_queue("custom.dlq")
        .build();
    info!("Custom Policy: {:?}", custom_policy);

    let fast_preset_policy = RetryPolicy::builder()
        .fast_preset()
        .dead_letter_exchange("my.fast.dlx")
        .build();
    info!("Fast Preset Policy: {:?}", fast_preset_policy);

    let slow_preset_policy = RetryPolicy::builder()
        .slow_preset()
        .max_retries(5) // Override preset
        .build();
    info!("Slow Preset Policy (modified): {:?}", slow_preset_policy);

    let linear_preset_policy = RetryPolicy::builder()
        .linear_preset(Duration::from_secs(3))
        .max_retries(6)
        .build();
    info!("Linear Preset Policy: {:?}", linear_preset_policy);

    let no_dlx_policy = RetryPolicy::builder()
        .max_retries(3)
        .initial_delay(Duration::from_secs(1))
        .no_dead_letter() // No dead letter exchange
        .build();
    info!("No DLX Policy: {:?}", no_dlx_policy);

    // Demo 3: Calculate delays for different policies
    info!("📝 Demo 3: Delay calculations for different policies");

    let policies = vec![
        ("Default", RetryPolicy::default()),
        ("Fast", RetryPolicy::fast()),
        ("Slow", RetryPolicy::slow()),
        ("Aggressive", RetryPolicy::aggressive()),
        ("Linear", RetryPolicy::linear(Duration::from_secs(2), 5)),
    ];

    for (name, policy) in policies {
        info!("=== {} Policy Delays ===", name);
        for attempt in 0..policy.max_retries.min(5) {
            let delay = policy.calculate_delay(attempt);
            info!("  Attempt {}: {:?}", attempt + 1, delay);
        }
    }

    // Demo 4: Setup consumer with custom retry policy
    info!("📝 Demo 4: Setting up consumer with custom retry policy");

    let consumer_policy = RetryPolicy::builder()
        .fast_preset()
        .max_retries(4)
        .dead_letter_exchange("demo.dlx")
        .dead_letter_queue("demo.dlq")
        .build();

    let options = ConsumerOptions::builder("demo.tasks")
        .auto_declare_queue()
        .auto_declare_exchange()
        .retry_policy(consumer_policy)
        .development()
        .build();

    let _consumer = Consumer::new(connection_manager, options).await?;
    let _handler = Arc::new(TaskHandler);

    info!("✅ Consumer setup complete with custom retry policy!");
    info!("Consumer is ready to process messages with retries.");

    // Note: In a real application, you would call:
    // consumer.consume::<TaskMessage, _>(handler).await?;

    Ok(())
}

/*
Expected output shows:

1. Different preset policies with their configurations
2. Custom builder configurations
3. Delay calculations showing exponential vs linear patterns
4. Working consumer setup

Key takeaways:
- RetryPolicy::fast() - Quick retries for transient failures
- RetryPolicy::slow() - Slower retries for resource-intensive operations
- RetryPolicy::aggressive() - Many attempts with exponential backoff
- RetryPolicy::conservative() - Few attempts with large delays
- RetryPolicy::linear() - Fixed delay intervals
- RetryPolicy::builder() - Full customization with presets
*/

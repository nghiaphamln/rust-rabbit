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
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
struct ProcessingMessage {
    id: String,
    message_type: String,
    content: String,
    priority: u8,
}

struct RetryDemoHandler {
    handler_name: String,
}

impl RetryDemoHandler {
    fn new(name: &str) -> Self {
        Self {
            handler_name: name.to_string(),
        }
    }
}

#[async_trait]
impl MessageHandler<ProcessingMessage> for RetryDemoHandler {
    async fn handle(&self, message: ProcessingMessage, context: MessageContext) -> MessageResult {
        info!(
            "[{}] Processing message {} (type: {}, attempt: {})",
            self.handler_name,
            message.id,
            message.message_type,
            context.retry_count + 1
        );

        // Simulate processing time
        sleep(Duration::from_millis(100)).await;

        // Simulate different scenarios based on message type
        match message.message_type.as_str() {
            "success" => {
                info!(
                    "[{}] ✅ Message {} processed successfully",
                    self.handler_name, message.id
                );
                MessageResult::Ack
            }
            "retry_once" => {
                if context.retry_count == 0 {
                    warn!(
                        "[{}] ⚠️ Message {} failed, will retry",
                        self.handler_name, message.id
                    );
                    MessageResult::Retry
                } else {
                    info!(
                        "[{}] ✅ Message {} succeeded on retry",
                        self.handler_name, message.id
                    );
                    MessageResult::Ack
                }
            }
            "retry_multiple" => {
                if context.retry_count < 3 {
                    warn!(
                        "[{}] ⚠️ Message {} failed (attempt {}), will retry",
                        self.handler_name,
                        message.id,
                        context.retry_count + 1
                    );
                    MessageResult::Retry
                } else {
                    info!(
                        "[{}] ✅ Message {} finally succeeded",
                        self.handler_name, message.id
                    );
                    MessageResult::Ack
                }
            }
            "permanent_fail" => {
                error!(
                    "[{}] ❌ Message {} permanently failed",
                    self.handler_name, message.id
                );
                MessageResult::Reject
            }
            "max_retries" => {
                warn!(
                    "[{}] ⚠️ Message {} will exhaust retries",
                    self.handler_name, message.id
                );
                MessageResult::Retry
            }
            _ => {
                info!(
                    "[{}] ✅ Message {} processed normally",
                    self.handler_name, message.id
                );
                MessageResult::Ack
            }
        }
    }
}

fn create_fast_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 5,
        initial_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(5),
        backoff_multiplier: 1.5,
        jitter: 0.05,
        dead_letter_exchange: Some("fast.dlx".to_string()),
        dead_letter_queue: Some("fast.dlq".to_string()),
        ..Default::default()
    }
}

fn create_slow_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 3,
        initial_delay: Duration::from_secs(2),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 3.0,
        jitter: 0.2,
        dead_letter_exchange: Some("slow.dlx".to_string()),
        dead_letter_queue: Some("slow.dlq".to_string()),
        ..Default::default()
    }
}

fn create_aggressive_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 8,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(60),
        backoff_multiplier: 2.0,
        jitter: 0.1,
        dead_letter_exchange: Some("aggressive.dlx".to_string()),
        dead_letter_queue: Some("aggressive.dlq".to_string()),
        ..Default::default()
    }
}

fn create_linear_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 4,
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(1), // Same as initial = linear
        backoff_multiplier: 1.0,           // No exponential growth
        jitter: 0.0,                       // No jitter
        dead_letter_exchange: Some("linear.dlx".to_string()),
        dead_letter_queue: Some("linear.dlq".to_string()),
        ..Default::default()
    }
}

async fn setup_consumer_with_retry_policy(
    connection_manager: ConnectionManager,
    queue_name: &str,
    retry_policy: RetryPolicy,
    handler_name: &str,
) -> Result<Consumer> {
    info!("Setting up consumer: {} with retry policy", handler_name);

    // Log retry policy details
    info!(
        "Retry Policy [{}]: max_retries={}, initial_delay={:?}, max_delay={:?}, multiplier={}, jitter={}",
        handler_name,
        retry_policy.max_retries,
        retry_policy.initial_delay,
        retry_policy.max_delay,
        retry_policy.backoff_multiplier,
        retry_policy.jitter
    );

    let options = ConsumerOptions::builder(queue_name)
        .auto_declare_queue()
        .auto_declare_exchange()
        .retry_policy(retry_policy)
        .prefetch_count(5)
        .build();

    Consumer::new(connection_manager, options).await
}

async fn publish_test_messages(connection_manager: ConnectionManager) -> Result<()> {
    use rust_rabbit::publisher::{PublishOptions, Publisher};

    let publisher = Publisher::new(connection_manager);

    let test_messages = vec![
        ProcessingMessage {
            id: "msg_001".to_string(),
            message_type: "success".to_string(),
            content: "This should succeed immediately".to_string(),
            priority: 1,
        },
        ProcessingMessage {
            id: "msg_002".to_string(),
            message_type: "retry_once".to_string(),
            content: "This should fail once then succeed".to_string(),
            priority: 2,
        },
        ProcessingMessage {
            id: "msg_003".to_string(),
            message_type: "retry_multiple".to_string(),
            content: "This should fail multiple times then succeed".to_string(),
            priority: 3,
        },
        ProcessingMessage {
            id: "msg_004".to_string(),
            message_type: "permanent_fail".to_string(),
            content: "This should fail permanently".to_string(),
            priority: 1,
        },
        ProcessingMessage {
            id: "msg_005".to_string(),
            message_type: "max_retries".to_string(),
            content: "This should exhaust all retries".to_string(),
            priority: 2,
        },
    ];

    let queues = [
        "fast.retry.demo",
        "slow.retry.demo",
        "aggressive.retry.demo",
        "linear.retry.demo",
    ];

    for queue in &queues {
        info!("Publishing test messages to queue: {}", queue);

        for message in &test_messages {
            publisher
                .publish_to_exchange(
                    queue,
                    queue,
                    message,
                    Some(PublishOptions::builder().auto_declare_exchange().build()),
                )
                .await?;

            sleep(Duration::from_millis(100)).await;
        }
    }

    info!("All test messages published!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Starting Retry Policy Demo...");

    // Create connection
    let config = RabbitConfig::default();
    let connection_manager = ConnectionManager::new(config).await?;

    // Create consumers with different retry policies
    info!("📝 Setting up consumers with different retry policies...");

    // 1. Fast Retry Consumer
    let fast_consumer = setup_consumer_with_retry_policy(
        connection_manager.clone(),
        "fast.retry.demo",
        create_fast_retry_policy(),
        "FastRetry",
    )
    .await?;

    // 2. Slow Retry Consumer
    let slow_consumer = setup_consumer_with_retry_policy(
        connection_manager.clone(),
        "slow.retry.demo",
        create_slow_retry_policy(),
        "SlowRetry",
    )
    .await?;

    // 3. Aggressive Retry Consumer
    let aggressive_consumer = setup_consumer_with_retry_policy(
        connection_manager.clone(),
        "aggressive.retry.demo",
        create_aggressive_retry_policy(),
        "AggressiveRetry",
    )
    .await?;

    // 4. Linear Retry Consumer
    let linear_consumer = setup_consumer_with_retry_policy(
        connection_manager.clone(),
        "linear.retry.demo",
        create_linear_retry_policy(),
        "LinearRetry",
    )
    .await?;

    // Start consumers
    info!("▶️  Starting consumers...");

    let fast_handle = {
        let consumer = fast_consumer;
        let handler = Arc::new(RetryDemoHandler::new("FastRetry"));
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<ProcessingMessage, _>(handler).await {
                error!("Fast consumer error: {}", e);
            }
        })
    };

    let slow_handle = {
        let consumer = slow_consumer;
        let handler = Arc::new(RetryDemoHandler::new("SlowRetry"));
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<ProcessingMessage, _>(handler).await {
                error!("Slow consumer error: {}", e);
            }
        })
    };

    let aggressive_handle = {
        let consumer = aggressive_consumer;
        let handler = Arc::new(RetryDemoHandler::new("AggressiveRetry"));
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<ProcessingMessage, _>(handler).await {
                error!("Aggressive consumer error: {}", e);
            }
        })
    };

    let linear_handle = {
        let consumer = linear_consumer;
        let handler = Arc::new(RetryDemoHandler::new("LinearRetry"));
        tokio::spawn(async move {
            if let Err(e) = consumer.consume::<ProcessingMessage, _>(handler).await {
                error!("Linear consumer error: {}", e);
            }
        })
    };

    // Wait a bit for consumers to start
    sleep(Duration::from_secs(2)).await;

    // Publish test messages
    info!("📨 Publishing test messages...");
    publish_test_messages(connection_manager).await?;

    // Let the demo run for a while to observe retry behavior
    info!("⏳ Demo running... Observe the retry patterns:");
    info!("   - FastRetry: Quick retries with 1.5x backoff");
    info!("   - SlowRetry: Slower retries with 3x backoff");
    info!("   - AggressiveRetry: Many retries with 2x backoff");
    info!("   - LinearRetry: Fixed 1s intervals");
    info!("   - Watch for DLX messages after max retries");

    sleep(Duration::from_secs(120)).await; // 2 minutes demo

    // Cleanup
    info!("🛑 Stopping demo...");
    fast_handle.abort();
    slow_handle.abort();
    aggressive_handle.abort();
    linear_handle.abort();

    sleep(Duration::from_secs(1)).await;
    info!("✅ Retry Policy Demo completed!");

    Ok(())
}

/*
Expected behavior when running this demo:

1. FastRetry (1.5x multiplier, 200ms initial):
   - retry_once: 200ms delay
   - retry_multiple: 200ms -> 300ms -> 450ms delays
   - max_retries: 200ms -> 300ms -> 450ms -> 675ms -> 1012ms -> DLX

2. SlowRetry (3x multiplier, 2s initial):
   - retry_once: 2s delay
   - retry_multiple: 2s -> 6s -> 18s delays
   - max_retries: 2s -> 6s -> 18s -> DLX

3. AggressiveRetry (2x multiplier, 100ms initial):
   - retry_once: 100ms delay
   - retry_multiple: 100ms -> 200ms -> 400ms delays
   - max_retries: 100ms -> 200ms -> 400ms -> 800ms -> 1.6s -> 3.2s -> 6.4s -> 12.8s -> DLX

4. LinearRetry (1x multiplier, 1s fixed):
   - retry_once: 1s delay
   - retry_multiple: 1s -> 1s -> 1s delays
   - max_retries: 1s -> 1s -> 1s -> 1s -> DLX
*/

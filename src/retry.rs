use crate::{
    connection::ConnectionManager,
    error::{RabbitError, Result},
};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, ExchangeKind,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Retry policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Initial delay between retries
    pub initial_delay: Duration,

    /// Maximum delay between retries
    pub max_delay: Duration,

    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,

    /// Jitter factor (0.0 to 1.0) to add randomness to delays
    pub jitter: f64,

    /// Retry queue naming pattern
    pub retry_queue_pattern: String,

    /// Dead letter exchange for failed messages
    pub dead_letter_exchange: Option<String>,

    /// Dead letter queue for failed messages
    pub dead_letter_queue: Option<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter: 0.1,
            retry_queue_pattern: "{queue_name}.retry.{attempt}".to_string(),
            dead_letter_exchange: Some("dead-letter".to_string()),
            dead_letter_queue: Some("dead-letter-queue".to_string()),
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for a specific retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay = Duration::from_millis(
            (self.initial_delay.as_millis() as f64 * self.backoff_multiplier.powi(attempt as i32))
                as u64,
        );

        let delay = if base_delay > self.max_delay {
            self.max_delay
        } else {
            base_delay
        };

        // Add jitter to prevent thundering herd
        if self.jitter > 0.0 {
            let jitter_amount = (delay.as_millis() as f64 * self.jitter) as u64;
            let jitter = fastrand::u64(0..=jitter_amount);
            Duration::from_millis(delay.as_millis() as u64 + jitter)
        } else {
            delay
        }
    }

    /// Generate retry queue name for a specific attempt
    pub fn get_retry_queue_name(&self, original_queue: &str, attempt: u32) -> String {
        self.retry_queue_pattern
            .replace("{queue_name}", original_queue)
            .replace("{attempt}", &attempt.to_string())
    }
}

/// Delayed Message Exchange handler for implementing retry mechanism
pub struct DelayedMessageExchange {
    connection_manager: ConnectionManager,
    exchange_name: String,
    retry_policy: RetryPolicy,
}

impl DelayedMessageExchange {
    /// Create a new DelayedMessageExchange
    pub fn new(
        connection_manager: ConnectionManager,
        exchange_name: String,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            connection_manager,
            exchange_name,
            retry_policy,
        }
    }

    /// Setup the delayed message exchange and retry infrastructure
    pub async fn setup(&self) -> Result<()> {
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Declare the delayed message exchange
        self.declare_delayed_exchange(&channel).await?;

        // Setup dead letter exchange and queue if configured
        if let Some(ref dle) = self.retry_policy.dead_letter_exchange {
            self.setup_dead_letter_infrastructure(&channel, dle).await?;
        }

        info!(
            "Delayed message exchange setup completed: {}",
            self.exchange_name
        );
        Ok(())
    }

    /// Declare the delayed message exchange
    async fn declare_delayed_exchange(&self, channel: &Channel) -> Result<()> {
        // Arguments for delayed message exchange
        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-delayed-type".into(),
            lapin::types::AMQPValue::LongString("direct".into()),
        );

        let options = ExchangeDeclareOptions {
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
        };

        channel
            .exchange_declare(
                &self.exchange_name,
                ExchangeKind::Custom("x-delayed-message".to_string()),
                options,
                arguments,
            )
            .await?;

        debug!("Declared delayed message exchange: {}", self.exchange_name);
        Ok(())
    }

    /// Setup dead letter exchange and queue
    async fn setup_dead_letter_infrastructure(
        &self,
        channel: &Channel,
        dle_name: &str,
    ) -> Result<()> {
        // Declare dead letter exchange
        let dle_options = ExchangeDeclareOptions {
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
        };

        channel
            .exchange_declare(
                dle_name,
                ExchangeKind::Direct,
                dle_options,
                FieldTable::default(),
            )
            .await?;

        // Declare dead letter queue if configured
        if let Some(ref dlq_name) = self.retry_policy.dead_letter_queue {
            let dlq_options = QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
            };

            channel
                .queue_declare(dlq_name, dlq_options, FieldTable::default())
                .await?;

            // Bind dead letter queue to dead letter exchange
            channel
                .queue_bind(
                    dlq_name,
                    dle_name,
                    "dead-letter",
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;

            debug!("Setup dead letter queue: {}", dlq_name);
        }

        debug!("Setup dead letter exchange: {}", dle_name);
        Ok(())
    }

    /// Publish a message with retry mechanism
    pub async fn publish_with_retry<T>(
        &self,
        original_queue: &str,
        message: &T,
        retry_count: u32,
        original_headers: Option<FieldTable>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        if retry_count >= self.retry_policy.max_retries {
            // Send to dead letter exchange
            if let Some(ref dle) = self.retry_policy.dead_letter_exchange {
                return self
                    .send_to_dead_letter(message, dle, original_headers)
                    .await;
            } else {
                return Err(RabbitError::RetryExhausted(format!(
                    "Max retries ({}) exceeded for queue: {}",
                    self.retry_policy.max_retries, original_queue
                )));
            }
        }

        let delay = self.retry_policy.calculate_delay(retry_count);
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Serialize message
        let payload = serde_json::to_vec(message).map_err(RabbitError::Serialization)?;

        // Build properties with delay header
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2); // Persistent

        // Add delay header for delayed message exchange
        let mut headers = original_headers.unwrap_or_default();
        headers.insert(
            "x-delay".into(),
            lapin::types::AMQPValue::LongLongInt(delay.as_millis() as i64),
        );
        headers.insert(
            "x-retry-count".into(),
            lapin::types::AMQPValue::LongInt(retry_count as i32),
        );
        headers.insert(
            "x-original-queue".into(),
            lapin::types::AMQPValue::LongString(original_queue.into()),
        );

        properties = properties.with_headers(headers);

        // Publish to delayed exchange with original queue as routing key
        channel
            .basic_publish(
                &self.exchange_name,
                original_queue, // Use original queue name as routing key
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?;

        info!(
            "Published retry message for queue: {} (attempt: {}, delay: {:?})",
            original_queue,
            retry_count + 1,
            delay
        );

        Ok(())
    }

    /// Send message to dead letter exchange
    async fn send_to_dead_letter<T>(
        &self,
        message: &T,
        dead_letter_exchange: &str,
        original_headers: Option<FieldTable>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Serialize message
        let payload = serde_json::to_vec(message).map_err(RabbitError::Serialization)?;

        // Build properties
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2); // Persistent

        // Add failure headers
        let mut headers = original_headers.unwrap_or_default();
        headers.insert(
            "x-death-reason".into(),
            lapin::types::AMQPValue::LongString("max-retries-exceeded".into()),
        );
        headers.insert(
            "x-death-timestamp".into(),
            lapin::types::AMQPValue::LongLongInt(chrono::Utc::now().timestamp()),
        );

        properties = properties.with_headers(headers);

        // Publish to dead letter exchange
        channel
            .basic_publish(
                dead_letter_exchange,
                "dead-letter", // Fixed routing key for dead letters
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?;

        warn!(
            "Message sent to dead letter exchange: {}",
            dead_letter_exchange
        );
        Ok(())
    }

    /// Setup retry queues for a specific original queue
    pub async fn setup_retry_queues(&self, original_queue: &str) -> Result<()> {
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Setup retry queues for each retry attempt
        for attempt in 1..=self.retry_policy.max_retries {
            let retry_queue_name = self
                .retry_policy
                .get_retry_queue_name(original_queue, attempt);

            // Create retry queue arguments
            let mut arguments = FieldTable::default();

            // Set message TTL based on retry delay
            let delay = self.retry_policy.calculate_delay(attempt - 1);
            arguments.insert(
                "x-message-ttl".into(),
                lapin::types::AMQPValue::LongLongInt(delay.as_millis() as i64),
            );

            // Set dead letter exchange to route back to original queue or next retry
            if attempt < self.retry_policy.max_retries {
                arguments.insert(
                    "x-dead-letter-exchange".into(),
                    lapin::types::AMQPValue::LongString("".into()), // Default exchange
                );
                arguments.insert(
                    "x-dead-letter-routing-key".into(),
                    lapin::types::AMQPValue::LongString(original_queue.into()),
                );
            } else {
                // Last retry attempt - send to dead letter exchange
                if let Some(ref dle) = self.retry_policy.dead_letter_exchange {
                    arguments.insert(
                        "x-dead-letter-exchange".into(),
                        lapin::types::AMQPValue::LongString(dle.clone().into()),
                    );
                    arguments.insert(
                        "x-dead-letter-routing-key".into(),
                        lapin::types::AMQPValue::LongString("dead-letter".into()),
                    );
                }
            }

            // Declare retry queue
            let queue_options = QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
            };

            channel
                .queue_declare(&retry_queue_name, queue_options, arguments)
                .await?;

            debug!(
                "Setup retry queue: {} for attempt: {}",
                retry_queue_name, attempt
            );
        }

        info!("Retry queues setup completed for: {}", original_queue);
        Ok(())
    }
}

/// Retry message wrapper for serialization
#[derive(Debug, Serialize, Deserialize)]
pub struct RetryMessage<T> {
    pub original_message: T,
    pub retry_count: u32,
    pub original_queue: String,
    pub original_headers: Option<serde_json::Value>,
    pub retry_timestamp: chrono::DateTime<chrono::Utc>,
}

impl<T> RetryMessage<T> {
    pub fn new(
        original_message: T,
        retry_count: u32,
        original_queue: String,
        original_headers: Option<serde_json::Value>,
    ) -> Self {
        Self {
            original_message,
            retry_count,
            original_queue,
            original_headers,
            retry_timestamp: chrono::Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_delay, Duration::from_millis(1000));
        assert_eq!(policy.max_delay, Duration::from_secs(60));
        assert_eq!(policy.backoff_multiplier, 2.0);
        assert_eq!(policy.jitter, 0.1);
    }

    #[test]
    fn test_retry_policy_calculate_delay() {
        let policy = RetryPolicy {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: 0.0, // No jitter for predictable tests
            ..Default::default()
        };

        let delay1 = policy.calculate_delay(0);
        assert_eq!(delay1, Duration::from_millis(1000));

        let delay2 = policy.calculate_delay(1);
        assert_eq!(delay2, Duration::from_millis(2000));

        let delay3 = policy.calculate_delay(2);
        assert_eq!(delay3, Duration::from_millis(4000));

        // Test max delay cap
        let delay_large = policy.calculate_delay(10);
        assert_eq!(delay_large, Duration::from_secs(30));
    }

    #[test]
    fn test_retry_queue_name_generation() {
        let policy = RetryPolicy::default();
        let queue_name = policy.get_retry_queue_name("orders", 1);
        assert_eq!(queue_name, "orders.retry.1");

        let queue_name = policy.get_retry_queue_name("user-events", 3);
        assert_eq!(queue_name, "user-events.retry.3");
    }

    #[test]
    fn test_retry_message_creation() {
        let original_message = "test message";
        let retry_msg = RetryMessage::new(original_message, 2, "test-queue".to_string(), None);

        assert_eq!(retry_msg.original_message, "test message");
        assert_eq!(retry_msg.retry_count, 2);
        assert_eq!(retry_msg.original_queue, "test-queue");
        assert!(retry_msg.original_headers.is_none());
    }
}

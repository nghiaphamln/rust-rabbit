mod envelope;
mod headers;
mod raw;
mod topology;

use crate::{connection::Connection, error::RustRabbitError, retry::RetryConfig};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
    Channel, Consumer as LapinConsumer,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

/// Consumer configuration builder
pub struct ConsumerBuilder {
    connection: Arc<Connection>,
    queue_name: String,
    exchange_name: Option<String>,
    routing_key: Option<String>,
    retry_config: Option<RetryConfig>,
    prefetch_count: Option<u16>,
    auto_ack: bool,
}

impl ConsumerBuilder {
    pub fn new(connection: Arc<Connection>, queue_name: impl Into<String>) -> Self {
        Self {
            connection,
            queue_name: queue_name.into(),
            exchange_name: None,
            routing_key: None,
            retry_config: None,
            prefetch_count: Some(10),
            auto_ack: true,
        }
    }

    /// Bind to an exchange with routing key
    pub fn bind_to_exchange(
        mut self,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        self.exchange_name = Some(exchange.into());
        self.routing_key = Some(routing_key.into());
        self
    }

    /// Set routing key (for use with bind_to_exchange)
    pub fn routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }

    /// Configure retry behavior
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
        self
    }

    /// Set TTL for dead letter queue (auto-cleanup failed messages)
    /// This is a convenience method that modifies the retry_config if it exists
    ///
    /// # Example
    /// ```rust,no_run
    /// # use rust_rabbit::{Connection, Consumer, RetryConfig};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let connection = Connection::new("amqp://localhost").await?;
    /// let consumer = Consumer::builder(connection, "orders")
    ///     .with_retry(RetryConfig::exponential_default())
    ///     .with_dlq_ttl(Duration::from_secs(86400))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_dlq_ttl(mut self, ttl: Duration) -> Self {
        if let Some(retry_config) = self.retry_config.as_mut() {
            retry_config.dlq_ttl = Some(ttl);
        }
        self
    }

    /// Set prefetch count
    pub fn with_prefetch(mut self, count: u16) -> Self {
        self.prefetch_count = Some(count);
        self
    }

    /// Disable auto-acknowledge (manual ack required)
    pub fn manual_ack(mut self) -> Self {
        self.auto_ack = false;
        self
    }

    /// Build the consumer
    pub fn build(self) -> Consumer {
        Consumer {
            connection: self.connection,
            queue_name: self.queue_name,
            exchange_name: self.exchange_name,
            routing_key: self.routing_key,
            retry_config: self.retry_config,
            prefetch_count: self.prefetch_count.unwrap_or(10),
            auto_ack: self.auto_ack,
        }
    }
}

/// Simplified Consumer for message consumption
pub struct Consumer {
    connection: Arc<Connection>,
    queue_name: String,
    exchange_name: Option<String>,
    routing_key: Option<String>,
    retry_config: Option<RetryConfig>,
    prefetch_count: u16,
    auto_ack: bool,
}

impl Consumer {
    pub(crate) fn ensure_supported_ack_mode(&self) -> Result<(), RustRabbitError> {
        if self.auto_ack {
            Ok(())
        } else {
            Err(RustRabbitError::Consumer(
                "manual_ack() is not supported by rust-rabbit's current public API because handlers do not receive an ack handle".to_string(),
            ))
        }
    }

    pub(crate) fn runtime_clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            queue_name: self.queue_name.clone(),
            exchange_name: self.exchange_name.clone(),
            routing_key: self.routing_key.clone(),
            retry_config: self.retry_config.clone(),
            prefetch_count: self.prefetch_count,
            auto_ack: self.auto_ack,
        }
    }

    /// Create a new consumer builder
    pub fn builder(connection: Arc<Connection>, queue_name: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::new(connection, queue_name)
    }

    pub(crate) async fn start_consumer_channel(
        &self,
        consumer_tag: &str,
    ) -> Result<(Channel, LapinConsumer), RustRabbitError> {
        let channel = self.connection.create_channel().await?;
        channel
            .basic_qos(
                self.prefetch_count,
                lapin::options::BasicQosOptions::default(),
            )
            .await?;
        self.setup_infrastructure(&channel).await?;

        let consumer = channel
            .basic_consume(
                self.queue_name.clone().into(),
                consumer_tag.into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok((channel, consumer))
    }

    pub(crate) async fn ack_delivery(&self, channel: &Channel, delivery_tag: u64, context: &str) {
        if let Err(e) = channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await
        {
            error!("Failed to ack message after {}: {}", context, e);
        }
    }

    pub(crate) async fn nack_delivery(&self, channel: &Channel, delivery_tag: u64, context: &str) {
        if let Err(e) = channel
            .basic_nack(
                delivery_tag,
                lapin::options::BasicNackOptions {
                    multiple: false,
                    requeue: false,
                },
            )
            .await
        {
            error!("Failed to nack message after {}: {}", context, e);
        }
    }
}

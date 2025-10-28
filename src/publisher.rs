use crate::{connection::Connection, error::RustRabbitError};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, ExchangeKind,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::debug;

/// Publish options builder
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    pub mandatory: bool,
    pub immediate: bool,
    pub expiration: Option<String>,
    pub priority: Option<u8>,
}

impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mandatory(mut self) -> Self {
        self.mandatory = true;
        self
    }

    pub fn with_expiration(mut self, expiration: impl Into<String>) -> Self {
        self.expiration = Some(expiration.into());
        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }
}

/// Simplified Publisher for message publishing
pub struct Publisher {
    connection: Arc<Connection>,
}

impl Publisher {
    /// Create a new publisher
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    /// Publish message to an exchange
    pub async fn publish_to_exchange<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        let channel = self.connection.create_channel().await?;

        // Declare exchange (simplified - always topic for flexibility)
        channel
            .exchange_declare(
                exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        self.publish_message(&channel, exchange, routing_key, message, options)
            .await
    }

    /// Publish message directly to a queue
    pub async fn publish_to_queue<T>(
        &self,
        queue: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        let channel = self.connection.create_channel().await?;

        // Declare queue
        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        // Publish to default exchange with queue name as routing key
        self.publish_message(&channel, "", queue, message, options)
            .await
    }

    /// Internal method to publish message
    async fn publish_message<T>(
        &self,
        channel: &Channel,
        exchange: &str,
        routing_key: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        // Serialize message
        let payload = serde_json::to_vec(message)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        // Build properties
        let options = options.unwrap_or_default();
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2); // Persistent

        if let Some(expiration) = options.expiration {
            properties = properties.with_expiration(expiration.into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

        // Publish message
        let confirm = channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions {
                    mandatory: options.mandatory,
                    immediate: options.immediate,
                },
                &payload,
                properties,
            )
            .await?;

        // Wait for confirmation (simplified)
        confirm.await?;

        debug!(
            "Published message to exchange '{}' with routing key '{}'",
            exchange, routing_key
        );

        Ok(())
    }
}

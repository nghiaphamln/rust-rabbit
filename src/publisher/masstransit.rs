use super::{MassTransitEnvelope, PublishOptions, Publisher};
use crate::error::RustRabbitError;
use lapin::{
    options::{ExchangeDeclareOptions, QueueDeclareOptions},
    types::FieldTable,
    ExchangeKind,
};
use serde::Serialize;
use tracing::debug;

impl Publisher {
    /// Publish a message to MassTransit-compatible exchange
    pub async fn publish_masstransit_to_exchange<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &T,
        message_type: &str,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        let channel = self.connection.create_channel().await?;
        channel
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let host = self.connection_host();
        let envelope = MassTransitEnvelope::with_message_type(message, message_type)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
            .with_source_address(format!("rabbitmq://{}/{}", host, exchange))
            .with_destination_address(format!("rabbitmq://{}/{}", host, routing_key));

        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        let options = options.unwrap_or_default();
        self.publish_serialized(&channel, exchange, routing_key, &payload, &options)
            .await?;

        debug!(
            "Published MassTransit message to exchange '{}' with routing key '{}' (type: {})",
            exchange, routing_key, message_type
        );

        Ok(())
    }

    /// Publish a message to MassTransit-compatible queue
    pub async fn publish_masstransit_to_queue<T>(
        &self,
        queue: &str,
        message: &T,
        message_type: &str,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        let channel = self.connection.create_channel().await?;
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let host = self.connection_host();
        let envelope = MassTransitEnvelope::with_message_type(message, message_type)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
            .with_source_address(format!("rabbitmq://{}/{}", host, queue))
            .with_destination_address(format!("rabbitmq://{}/{}", host, queue));

        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        let options = options.unwrap_or_default();
        self.publish_serialized(&channel, "", queue, &payload, &options)
            .await?;

        debug!(
            "Published MassTransit message to queue '{}' (type: {})",
            queue, message_type
        );

        Ok(())
    }

    /// Publish a MassTransit envelope (already created) to an exchange
    pub async fn publish_masstransit_envelope_to_exchange(
        &self,
        exchange: &str,
        routing_key: &str,
        envelope: &MassTransitEnvelope,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError> {
        let channel = self.connection.create_channel().await?;
        channel
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let payload = serde_json::to_vec(envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        let options = options.unwrap_or_default();
        self.publish_serialized(&channel, exchange, routing_key, &payload, &options)
            .await?;

        debug!(
            "Published MassTransit envelope to exchange '{}' with routing key '{}'",
            exchange, routing_key
        );

        Ok(())
    }

    /// Publish a MassTransit envelope (already created) to a queue
    pub async fn publish_masstransit_envelope_to_queue(
        &self,
        queue: &str,
        envelope: &MassTransitEnvelope,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError> {
        let channel = self.connection.create_channel().await?;
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let payload = serde_json::to_vec(envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        let options = options.unwrap_or_default();
        self.publish_serialized(&channel, "", queue, &payload, &options)
            .await?;

        debug!("Published MassTransit envelope to queue '{}'", queue);

        Ok(())
    }
}

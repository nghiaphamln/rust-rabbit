use super::{MassTransitEnvelope, PublishOptions, Publisher};
use crate::error::RustRabbitError;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, ExchangeKind,
};
use serde::Serialize;
use tracing::debug;
use url::Url;

impl Publisher {
    pub(super) fn connection_host(&self) -> String {
        self.connection
            .url()
            .parse::<Url>()
            .ok()
            .and_then(|url| url.host_str().map(|h| h.to_string()))
            .unwrap_or_else(|| "localhost".to_string())
    }

    fn build_properties(
        &self,
        options: &PublishOptions,
        include_retry_header: bool,
    ) -> BasicProperties {
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2);

        let mut headers = FieldTable::default();
        if include_retry_header {
            headers.insert("x-retry-attempt".into(), AMQPValue::LongLongInt(0));
        }
        properties = properties.with_headers(headers);

        if let Some(expiration) = &options.expiration {
            properties = properties.with_expiration(expiration.clone().into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

        properties
    }

    pub(super) async fn publish_serialized(
        &self,
        channel: &Channel,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        options: &PublishOptions,
    ) -> Result<(), RustRabbitError> {
        let confirm = channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: options.mandatory,
                    immediate: options.immediate,
                },
                payload,
                self.build_properties(options, true),
            )
            .await?;

        confirm.await?;
        Ok(())
    }

    fn serialize_message<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &T,
        options: &PublishOptions,
    ) -> Result<Vec<u8>, RustRabbitError>
    where
        T: Serialize,
    {
        if let Some(mt_options) = &options.masstransit {
            let mut envelope =
                MassTransitEnvelope::with_message_type(message, &mt_options.message_type)
                    .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

            if let Some(corr_id) = &mt_options.correlation_id {
                envelope = envelope.with_correlation_id(corr_id.clone());
            }

            let host = self.connection_host();
            let source = mt_options
                .source_address
                .clone()
                .unwrap_or_else(|| format!("rabbitmq://{}/{}", host, exchange));
            let dest = mt_options
                .destination_address
                .clone()
                .unwrap_or_else(|| format!("rabbitmq://{}/{}", host, routing_key));

            serde_json::to_vec(
                &envelope
                    .with_source_address(source)
                    .with_destination_address(dest),
            )
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))
        } else {
            serde_json::to_vec(message).map_err(|e| RustRabbitError::Serialization(e.to_string()))
        }
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

        self.publish_message(&channel, "", queue, message, options)
            .await
    }

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
        let options = options.unwrap_or_default();
        let payload = self.serialize_message(exchange, routing_key, message, &options)?;
        self.publish_serialized(channel, exchange, routing_key, &payload, &options)
            .await?;

        if options.masstransit.is_some() {
            debug!(
                "Published MassTransit message to exchange '{}' with routing key '{}'",
                exchange, routing_key
            );
        } else {
            debug!(
                "Published message to exchange '{}' with routing key '{}'",
                exchange, routing_key
            );
        }

        Ok(())
    }
}

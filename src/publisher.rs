use crate::{
    connection::Connection,
    error::RustRabbitError,
    message::{MassTransitEnvelope, MessageEnvelope},
};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, ExchangeKind,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::debug;
use url::Url;

/// Publish options builder
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    pub mandatory: bool,
    pub immediate: bool,
    pub expiration: Option<String>,
    pub priority: Option<u8>,
    /// Enable MassTransit format conversion
    pub masstransit: Option<MassTransitOptions>,
}

/// MassTransit-specific options for message publishing
#[derive(Debug, Clone)]
pub struct MassTransitOptions {
    /// Message type in URN format: "urn:message:Namespace:TypeName"
    /// or simple format: "Namespace:TypeName" (will be converted to URN)
    pub message_type: String,
    /// Optional correlation ID
    pub correlation_id: Option<String>,
    /// Optional source address (defaults to exchange/queue if not provided)
    pub source_address: Option<String>,
    /// Optional destination address (defaults to routing_key/queue if not provided)
    pub destination_address: Option<String>,
}

impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mandatory(mut self) -> Self {
        self.mandatory = true;
        self
    }

    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
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

    /// Enable MassTransit format conversion
    /// Message type can be in format "Namespace:TypeName" or "urn:message:Namespace:TypeName"
    pub fn with_masstransit(mut self, message_type: impl Into<String>) -> Self {
        self.masstransit = Some(MassTransitOptions {
            message_type: message_type.into(),
            correlation_id: None,
            source_address: None,
            destination_address: None,
        });
        self
    }

    /// Enable MassTransit format with full options
    pub fn with_masstransit_options(mut self, options: MassTransitOptions) -> Self {
        self.masstransit = Some(options);
        self
    }
}

impl MassTransitOptions {
    /// Create new MassTransit options with message type
    pub fn new(message_type: impl Into<String>) -> Self {
        Self {
            message_type: message_type.into(),
            correlation_id: None,
            source_address: None,
            destination_address: None,
        }
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Set source address
    pub fn with_source_address(mut self, source_address: impl Into<String>) -> Self {
        self.source_address = Some(source_address.into());
        self
    }

    /// Set destination address
    pub fn with_destination_address(mut self, destination_address: impl Into<String>) -> Self {
        self.destination_address = Some(destination_address.into());
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
    /// Publishes raw payload with headers (retry_attempt and correlation_id in headers)
    /// If MassTransit options are provided, wraps message in MassTransit envelope format
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

        // Check if MassTransit conversion is requested
        let payload = if let Some(mt_options) = &options.masstransit {
            // Create MassTransit envelope
            let mut envelope =
                MassTransitEnvelope::with_message_type(message, &mt_options.message_type)
                    .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

            // Set correlation ID if provided
            if let Some(corr_id) = &mt_options.correlation_id {
                envelope = envelope.with_correlation_id(corr_id.clone());
            }

            // Extract host from connection URL for MassTransit addresses
            let host = self
                .connection
                .url()
                .parse::<Url>()
                .ok()
                .and_then(|url| url.host_str().map(|h| h.to_string()))
                .unwrap_or_else(|| "localhost".to_string());

            // Set source address (default to exchange if not provided)
            let source = mt_options
                .source_address
                .clone()
                .unwrap_or_else(|| format!("rabbitmq://{}/{}", host, exchange));
            envelope = envelope.with_source_address(source);

            // Set destination address (default to routing key if not provided)
            let dest = mt_options
                .destination_address
                .clone()
                .unwrap_or_else(|| format!("rabbitmq://{}/{}", host, routing_key));
            envelope = envelope.with_destination_address(dest);

            // Serialize MassTransit envelope
            serde_json::to_vec(&envelope)
                .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
        } else {
            // Serialize raw payload (no wrapper)
            serde_json::to_vec(message)
                .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
        };

        // Build properties with headers
        // Create headers with retry_attempt = 0 (first attempt)
        let mut headers = FieldTable::default();
        headers.insert("x-retry-attempt".into(), AMQPValue::LongLongInt(0));

        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2) // Persistent
            .with_headers(headers);

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

    /// Publish a message envelope to an exchange (includes retry metadata)
    pub async fn publish_envelope_to_exchange<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        envelope: &MessageEnvelope<T>,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        self.publish_to_exchange(exchange, routing_key, envelope, options)
            .await
    }

    /// Publish a message envelope directly to a queue (includes retry metadata)
    pub async fn publish_envelope_to_queue<T>(
        &self,
        queue: &str,
        envelope: &MessageEnvelope<T>,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize,
    {
        self.publish_to_queue(queue, envelope, options).await
    }

    /// Create a message envelope with source tracking and publish to exchange
    pub async fn publish_with_envelope<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &T,
        source_queue: &str,
        max_retries: u32,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize + Clone,
    {
        let envelope = MessageEnvelope::with_source(
            payload.clone(),
            source_queue,
            Some(exchange),
            Some(routing_key),
            Some("rust-rabbit-publisher"), // Publisher identifier
        )
        .with_max_retries(max_retries);

        self.publish_envelope_to_exchange(exchange, routing_key, &envelope, options)
            .await
    }

    /// Create a message envelope and publish directly to queue
    pub async fn publish_with_envelope_to_queue<T>(
        &self,
        queue: &str,
        payload: &T,
        max_retries: u32,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError>
    where
        T: Serialize + Clone,
    {
        let envelope = MessageEnvelope::new(payload.clone(), queue).with_max_retries(max_retries);

        self.publish_envelope_to_queue(queue, &envelope, options)
            .await
    }

    /// Publish a message to MassTransit-compatible exchange
    /// This ensures the message format matches MassTransit's expectations
    ///
    /// # Arguments
    /// * `exchange` - Exchange name (MassTransit typically uses exchange names)
    /// * `routing_key` - Routing key (often the message type name)
    /// * `message` - The message payload to publish
    /// * `message_type` - Message type name (e.g., "YourNamespace:YourMessageType") - required for MassTransit routing
    /// * `options` - Optional publish options
    ///
    /// # Example
    /// ```rust,no_run
    /// use rust_rabbit::{Connection, Publisher};
    ///
    /// #[derive(serde::Serialize)]
    /// struct OrderCreated {
    ///     order_id: u32,
    ///     amount: f64,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let connection = Connection::new("amqp://localhost:5672").await?;
    ///     let publisher = Publisher::new(connection);
    ///     
    ///     let order = OrderCreated { order_id: 123, amount: 99.99 };
    ///     publisher.publish_masstransit_to_exchange(
    ///         "order-exchange",
    ///         "order.created",
    ///         &order,
    ///         "Contracts:OrderCreated", // Message type for MassTransit
    ///         None
    ///     ).await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
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

        // Declare exchange (MassTransit typically uses topic exchanges)
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

        // Extract host from connection URL for MassTransit addresses
        let host = self
            .connection
            .url()
            .parse::<Url>()
            .ok()
            .and_then(|url| url.host_str().map(|h| h.to_string()))
            .unwrap_or_else(|| "localhost".to_string());

        // Create MassTransit envelope with message type
        let envelope = MassTransitEnvelope::with_message_type(message, message_type)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
            .with_source_address(format!("rabbitmq://{}/{}", host, exchange))
            .with_destination_address(format!("rabbitmq://{}/{}", host, routing_key));

        // Serialize envelope
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        // Build properties
        let options = options.unwrap_or_default();
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2) // Persistent
            .with_headers(FieldTable::default());

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

        confirm.await?;

        debug!(
            "Published MassTransit message to exchange '{}' with routing key '{}' (type: {})",
            exchange, routing_key, message_type
        );

        Ok(())
    }

    /// Publish a message to MassTransit-compatible queue
    /// This ensures the message format matches MassTransit's expectations
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `message` - The message payload to publish
    /// * `message_type` - Message type name (e.g., "YourNamespace:YourMessageType") - required for MassTransit routing
    /// * `options` - Optional publish options
    ///
    /// # Example
    /// ```rust,no_run
    /// use rust_rabbit::{Connection, Publisher};
    ///
    /// #[derive(serde::Serialize)]
    /// struct OrderCreated {
    ///     order_id: u32,
    ///     amount: f64,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let connection = Connection::new("amqp://localhost:5672").await?;
    ///     let publisher = Publisher::new(connection);
    ///     
    ///     let order = OrderCreated { order_id: 123, amount: 99.99 };
    ///     publisher.publish_masstransit_to_queue(
    ///         "order-queue",
    ///         &order,
    ///         "Contracts:OrderCreated", // Message type for MassTransit
    ///         None
    ///     ).await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
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

        // Extract host from connection URL for MassTransit addresses
        let host = self
            .connection
            .url()
            .parse::<Url>()
            .ok()
            .and_then(|url| url.host_str().map(|h| h.to_string()))
            .unwrap_or_else(|| "localhost".to_string());

        // Create MassTransit envelope with message type
        let envelope = MassTransitEnvelope::with_message_type(message, message_type)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?
            .with_source_address(format!("rabbitmq://{}/{}", host, queue))
            .with_destination_address(format!("rabbitmq://{}/{}", host, queue));

        // Serialize envelope
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        // Build properties
        let options = options.unwrap_or_default();
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2) // Persistent
            .with_headers(FieldTable::default());

        if let Some(expiration) = options.expiration {
            properties = properties.with_expiration(expiration.into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

        // Publish to default exchange with queue name as routing key
        let confirm = channel
            .basic_publish(
                "", // Default exchange
                queue,
                BasicPublishOptions {
                    mandatory: options.mandatory,
                    immediate: options.immediate,
                },
                &payload,
                properties,
            )
            .await?;

        confirm.await?;

        debug!(
            "Published MassTransit message to queue '{}' (type: {})",
            queue, message_type
        );

        Ok(())
    }

    /// Publish a MassTransit envelope (already created) to an exchange
    /// Useful when you need full control over the envelope structure
    pub async fn publish_masstransit_envelope_to_exchange(
        &self,
        exchange: &str,
        routing_key: &str,
        envelope: &MassTransitEnvelope,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError> {
        let channel = self.connection.create_channel().await?;

        // Declare exchange
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

        // Serialize envelope
        let payload = serde_json::to_vec(envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        // Build properties
        let options = options.unwrap_or_default();
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_headers(FieldTable::default());

        if let Some(expiration) = options.expiration {
            properties = properties.with_expiration(expiration.into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

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

        confirm.await?;

        debug!(
            "Published MassTransit envelope to exchange '{}' with routing key '{}'",
            exchange, routing_key
        );

        Ok(())
    }

    /// Publish a MassTransit envelope (already created) to a queue
    /// Useful when you need full control over the envelope structure
    pub async fn publish_masstransit_envelope_to_queue(
        &self,
        queue: &str,
        envelope: &MassTransitEnvelope,
        options: Option<PublishOptions>,
    ) -> Result<(), RustRabbitError> {
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

        // Serialize envelope
        let payload = serde_json::to_vec(envelope)
            .map_err(|e| RustRabbitError::Serialization(e.to_string()))?;

        // Build properties
        let options = options.unwrap_or_default();
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_headers(FieldTable::default());

        if let Some(expiration) = options.expiration {
            properties = properties.with_expiration(expiration.into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

        let confirm = channel
            .basic_publish(
                "",
                queue,
                BasicPublishOptions {
                    mandatory: options.mandatory,
                    immediate: options.immediate,
                },
                &payload,
                properties,
            )
            .await?;

        confirm.await?;

        debug!("Published MassTransit envelope to queue '{}'", queue);

        Ok(())
    }
}

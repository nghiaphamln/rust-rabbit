mod core;
mod masstransit;

use crate::{
    connection::Connection,
    message::{MassTransitEnvelope, MessageEnvelope},
};
use std::sync::Arc;

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

    /// Publish a message envelope to an exchange (includes retry metadata)
    pub async fn publish_envelope_to_exchange<T>(
        &self,
        exchange: &str,
        routing_key: &str,
        envelope: &MessageEnvelope<T>,
        options: Option<PublishOptions>,
    ) -> Result<(), crate::error::RustRabbitError>
    where
        T: serde::Serialize,
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
    ) -> Result<(), crate::error::RustRabbitError>
    where
        T: serde::Serialize,
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
    ) -> Result<(), crate::error::RustRabbitError>
    where
        T: serde::Serialize + Clone,
    {
        let envelope = MessageEnvelope::with_source(
            payload.clone(),
            source_queue,
            Some(exchange),
            Some(routing_key),
            Some("rust-rabbit-publisher"),
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
    ) -> Result<(), crate::error::RustRabbitError>
    where
        T: serde::Serialize + Clone,
    {
        let envelope = MessageEnvelope::new(payload.clone(), queue).with_max_retries(max_retries);
        self.publish_envelope_to_queue(queue, &envelope, options)
            .await
    }
}

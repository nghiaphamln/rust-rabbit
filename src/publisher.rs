use crate::{
    connection::ConnectionManager,
    error::{RabbitError, Result},
};
use lapin::{
    options::{
        BasicPublishOptions, ExchangeDeclareOptions as LapinExchangeDeclareOptions,
        QueueDeclareOptions as LapinQueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, ExchangeKind,
};
use serde::Serialize;
use std::{collections::HashMap, time::Duration};
use tracing::debug;
use uuid::Uuid;

/// Publisher for sending messages to RabbitMQ
#[derive(Debug, Clone)]
pub struct Publisher {
    connection_manager: ConnectionManager,
}

impl Publisher {
    /// Create a new publisher
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self { connection_manager }
    }

    /// Publish a message to a queue
    pub async fn publish_to_queue<T>(
        &self,
        queue_name: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let channel = self.get_channel().await?;

        // Declare queue if auto_declare is enabled
        let options = options.unwrap_or_default();
        if options.auto_declare_queue {
            self.declare_queue(&channel, queue_name, &options.queue_options)
                .await?;
        }

        let payload = self.serialize_message(message)?;
        let properties = self.build_basic_properties(&options)?;

        channel
            .basic_publish(
                "", // default exchange
                queue_name,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?;

        debug!("Published message to queue: {}", queue_name);
        Ok(())
    }

    /// Publish a message to an exchange
    pub async fn publish_to_exchange<T>(
        &self,
        exchange_name: &str,
        routing_key: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let channel = self.get_channel().await?;

        // Declare exchange if auto_declare is enabled
        let options = options.unwrap_or_default();
        if options.auto_declare_exchange {
            self.declare_exchange(&channel, exchange_name, &options.exchange_options)
                .await?;
        }

        let payload = self.serialize_message(message)?;
        let properties = self.build_basic_properties(&options)?;

        channel
            .basic_publish(
                exchange_name,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?;

        debug!(
            "Published message to exchange: {} with routing key: {}",
            exchange_name, routing_key
        );
        Ok(())
    }

    /// Publish a delayed message using the delayed message exchange plugin
    pub async fn publish_delayed<T>(
        &self,
        exchange_name: &str,
        routing_key: &str,
        message: &T,
        delay: Duration,
        options: Option<PublishOptions>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let channel = self.get_channel().await?;

        let options = options.unwrap_or_default();

        // Ensure the exchange is declared as delayed type
        if options.auto_declare_exchange {
            let mut exchange_opts = options.exchange_options.clone();
            exchange_opts.exchange_type = ExchangeKind::Custom("x-delayed-message".to_string());

            // We'll handle arguments in the declare_exchange method
            self.declare_exchange(&channel, exchange_name, &exchange_opts)
                .await?;
        }

        let payload = self.serialize_message(message)?;
        let mut properties = self.build_basic_properties(&options)?;

        // Add delay header
        let mut headers = properties.headers().clone().unwrap_or_default();
        headers.insert(
            "x-delay".into(),
            lapin::types::AMQPValue::LongLongInt(delay.as_millis() as i64),
        );
        properties = properties.with_headers(headers);

        channel
            .basic_publish(
                exchange_name,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?;

        debug!(
            "Published delayed message to exchange: {} with delay: {:?}",
            exchange_name, delay
        );
        Ok(())
    }

    /// Publish a message with TTL (Time To Live)
    pub async fn publish_with_ttl<T>(
        &self,
        exchange_name: &str,
        routing_key: &str,
        message: &T,
        ttl: Duration,
        options: Option<PublishOptions>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let mut options = options.unwrap_or_default();
        options.ttl = Some(ttl);

        self.publish_to_exchange(exchange_name, routing_key, message, Some(options))
            .await
    }

    /// Get a channel from the connection manager
    async fn get_channel(&self) -> Result<Channel> {
        let connection = self.connection_manager.get_connection().await?;
        connection.create_channel().await
    }

    /// Serialize message to bytes
    fn serialize_message<T>(&self, message: &T) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        serde_json::to_vec(message).map_err(RabbitError::Serialization)
    }

    /// Build BasicProperties from PublishOptions
    fn build_basic_properties(&self, options: &PublishOptions) -> Result<BasicProperties> {
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(if options.persistent { 2 } else { 1 });

        if let Some(message_id) = &options.message_id {
            properties = properties.with_message_id(message_id.clone().into());
        }

        if let Some(correlation_id) = &options.correlation_id {
            properties = properties.with_correlation_id(correlation_id.clone().into());
        }

        if let Some(reply_to) = &options.reply_to {
            properties = properties.with_reply_to(reply_to.clone().into());
        }

        if let Some(ttl) = options.ttl {
            properties = properties.with_expiration(ttl.as_millis().to_string().into());
        }

        if let Some(priority) = options.priority {
            properties = properties.with_priority(priority);
        }

        if !options.headers.is_empty() {
            let mut field_table = FieldTable::default();
            for (key, value) in &options.headers {
                field_table.insert(key.clone().into(), value.clone());
            }
            properties = properties.with_headers(field_table);
        }

        properties = properties.with_timestamp(chrono::Utc::now().timestamp() as u64);

        Ok(properties)
    }

    /// Declare a queue
    async fn declare_queue(
        &self,
        channel: &Channel,
        queue_name: &str,
        options: &CustomQueueDeclareOptions,
    ) -> Result<()> {
        let queue_options = LapinQueueDeclareOptions {
            passive: options.passive,
            durable: options.durable,
            exclusive: options.exclusive,
            auto_delete: options.auto_delete,
            nowait: false,
        };

        channel
            .queue_declare(queue_name, queue_options, options.arguments.clone())
            .await?;

        debug!("Declared queue: {}", queue_name);
        Ok(())
    }

    /// Declare an exchange
    async fn declare_exchange(
        &self,
        channel: &Channel,
        exchange_name: &str,
        options: &CustomExchangeDeclareOptions,
    ) -> Result<()> {
        let exchange_kind = match &options.exchange_type {
            ExchangeKind::Custom(custom_type) => {
                if custom_type == "x-delayed-message" {
                    // For delayed message exchange, we need special handling
                    let mut arguments = options.arguments.clone();
                    let original_type_str = match &options.original_type {
                        ExchangeKind::Direct => "direct",
                        ExchangeKind::Fanout => "fanout",
                        ExchangeKind::Topic => "topic",
                        ExchangeKind::Headers => "headers",
                        ExchangeKind::Custom(custom) => custom,
                    };
                    arguments.insert(
                        "x-delayed-type".into(),
                        lapin::types::AMQPValue::LongString(original_type_str.into()),
                    );

                    let exchange_options = LapinExchangeDeclareOptions {
                        passive: options.passive,
                        durable: options.durable,
                        auto_delete: options.auto_delete,
                        internal: options.internal,
                        nowait: false,
                    };

                    channel
                        .exchange_declare(
                            exchange_name,
                            ExchangeKind::Custom("x-delayed-message".to_string()),
                            exchange_options,
                            arguments,
                        )
                        .await?;

                    debug!("Declared delayed message exchange: {}", exchange_name);
                    return Ok(());
                } else {
                    lapin::ExchangeKind::Custom(custom_type.clone())
                }
            }
            other => other.clone(),
        };

        let exchange_options = LapinExchangeDeclareOptions {
            passive: options.passive,
            durable: options.durable,
            auto_delete: options.auto_delete,
            internal: options.internal,
            nowait: false,
        };

        channel
            .exchange_declare(
                exchange_name,
                exchange_kind,
                exchange_options,
                options.arguments.clone(),
            )
            .await?;

        debug!(
            "Declared exchange: {} of type: {:?}",
            exchange_name, options.exchange_type
        );
        Ok(())
    }
}

/// Options for publishing messages
#[derive(Debug, Clone)]
pub struct PublishOptions {
    /// Whether the message should be persistent
    pub persistent: bool,

    /// Message ID
    pub message_id: Option<String>,

    /// Correlation ID for request-response patterns
    pub correlation_id: Option<String>,

    /// Reply-to queue for RPC patterns
    pub reply_to: Option<String>,

    /// Message Time To Live
    pub ttl: Option<Duration>,

    /// Message priority (0-255)
    pub priority: Option<u8>,

    /// Custom headers
    pub headers: HashMap<String, lapin::types::AMQPValue>,

    /// Auto-declare queue before publishing
    pub auto_declare_queue: bool,

    /// Auto-declare exchange before publishing
    pub auto_declare_exchange: bool,

    /// Queue declaration options
    pub queue_options: CustomQueueDeclareOptions,

    /// Exchange declaration options
    pub exchange_options: CustomExchangeDeclareOptions,
}

impl PublishOptions {
    /// Create a new publish options builder
    pub fn builder() -> PublishOptionsBuilder {
        PublishOptionsBuilder::new()
    }
}

/// Builder for PublishOptions
#[derive(Debug, Clone)]
pub struct PublishOptionsBuilder {
    persistent: bool,
    message_id: Option<String>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    ttl: Option<Duration>,
    priority: Option<u8>,
    headers: HashMap<String, lapin::types::AMQPValue>,
    auto_declare_queue: bool,
    auto_declare_exchange: bool,
    queue_options: CustomQueueDeclareOptions,
    exchange_options: CustomExchangeDeclareOptions,
}

impl PublishOptionsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            persistent: true,
            message_id: Some(Uuid::new_v4().to_string()),
            correlation_id: None,
            reply_to: None,
            ttl: None,
            priority: None,
            headers: HashMap::new(),
            auto_declare_queue: false,
            auto_declare_exchange: false,
            queue_options: CustomQueueDeclareOptions::default(),
            exchange_options: CustomExchangeDeclareOptions::default(),
        }
    }

    /// Set message persistence
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Make message persistent
    pub fn durable(mut self) -> Self {
        self.persistent = true;
        self
    }

    /// Make message non-persistent
    pub fn transient(mut self) -> Self {
        self.persistent = false;
        self
    }

    /// Set message ID
    pub fn message_id<S: Into<String>>(mut self, id: S) -> Self {
        self.message_id = Some(id.into());
        self
    }

    /// Generate random message ID
    pub fn random_message_id(mut self) -> Self {
        self.message_id = Some(Uuid::new_v4().to_string());
        self
    }

    /// Clear message ID
    pub fn no_message_id(mut self) -> Self {
        self.message_id = None;
        self
    }

    /// Set correlation ID
    pub fn correlation_id<S: Into<String>>(mut self, id: S) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Set reply-to queue
    pub fn reply_to<S: Into<String>>(mut self, queue: S) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Set message TTL
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set message priority
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Add a custom header
    pub fn header<S: Into<String>>(mut self, key: S, value: lapin::types::AMQPValue) -> Self {
        self.headers.insert(key.into(), value);
        self
    }

    /// Add a string header
    pub fn header_string<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(
            key.into(),
            lapin::types::AMQPValue::LongString(value.into().into()),
        );
        self
    }

    /// Add an integer header
    pub fn header_int<K: Into<String>>(mut self, key: K, value: i64) -> Self {
        self.headers
            .insert(key.into(), lapin::types::AMQPValue::LongLongInt(value));
        self
    }

    /// Enable auto-declare queue
    pub fn auto_declare_queue(mut self) -> Self {
        self.auto_declare_queue = true;
        self
    }

    /// Enable auto-declare exchange
    pub fn auto_declare_exchange(mut self) -> Self {
        self.auto_declare_exchange = true;
        self
    }

    /// Set queue options
    pub fn queue_options(mut self, options: CustomQueueDeclareOptions) -> Self {
        self.queue_options = options;
        self
    }

    /// Set exchange options
    pub fn exchange_options(mut self, options: CustomExchangeDeclareOptions) -> Self {
        self.exchange_options = options;
        self
    }

    /// Configure for request-response pattern
    pub fn request_response<S: Into<String>>(mut self, reply_to: S, correlation_id: S) -> Self {
        self.reply_to = Some(reply_to.into());
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Configure for development (auto-declare everything)
    pub fn development(mut self) -> Self {
        self.auto_declare_queue = true;
        self.auto_declare_exchange = true;
        self
    }

    /// Configure for production (no auto-declare)
    pub fn production(mut self) -> Self {
        self.auto_declare_queue = false;
        self.auto_declare_exchange = false;
        self.persistent = true;
        self
    }

    /// Build the final options
    pub fn build(self) -> PublishOptions {
        PublishOptions {
            persistent: self.persistent,
            message_id: self.message_id,
            correlation_id: self.correlation_id,
            reply_to: self.reply_to,
            ttl: self.ttl,
            priority: self.priority,
            headers: self.headers,
            auto_declare_queue: self.auto_declare_queue,
            auto_declare_exchange: self.auto_declare_exchange,
            queue_options: self.queue_options,
            exchange_options: self.exchange_options,
        }
    }
}

impl Default for PublishOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            persistent: true,
            message_id: Some(Uuid::new_v4().to_string()),
            correlation_id: None,
            reply_to: None,
            ttl: None,
            priority: None,
            headers: HashMap::new(),
            auto_declare_queue: false,
            auto_declare_exchange: false,
            queue_options: CustomQueueDeclareOptions::default(),
            exchange_options: CustomExchangeDeclareOptions::default(),
        }
    }
}

/// Custom Queue declaration options (wrapper around lapin's options)
#[derive(Debug, Clone)]
pub struct CustomQueueDeclareOptions {
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: FieldTable,
}

impl Default for CustomQueueDeclareOptions {
    fn default() -> Self {
        Self {
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::default(),
        }
    }
}

/// Custom Exchange declaration options (wrapper around lapin's options)
#[derive(Debug, Clone)]
pub struct CustomExchangeDeclareOptions {
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub exchange_type: ExchangeKind,
    pub original_type: ExchangeKind, // Used for delayed message exchanges
    pub arguments: FieldTable,
}

impl Default for CustomExchangeDeclareOptions {
    fn default() -> Self {
        Self {
            passive: false,
            durable: true,
            auto_delete: false,
            internal: false,
            exchange_type: ExchangeKind::Direct,
            original_type: ExchangeKind::Direct,
            arguments: FieldTable::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_options_default() {
        let options = PublishOptions::default();
        assert!(options.persistent);
        assert!(options.message_id.is_some());
        assert!(options.correlation_id.is_none());
        assert!(options.reply_to.is_none());
        assert!(options.ttl.is_none());
        assert!(options.priority.is_none());
        assert!(options.headers.is_empty());
        assert!(!options.auto_declare_queue);
        assert!(!options.auto_declare_exchange);
    }

    #[test]
    fn test_queue_declare_options_default() {
        let options = CustomQueueDeclareOptions::default();
        assert!(!options.passive);
        assert!(options.durable);
        assert!(!options.exclusive);
        assert!(!options.auto_delete);
    }

    #[test]
    fn test_exchange_declare_options_default() {
        let options = CustomExchangeDeclareOptions::default();
        assert!(!options.passive);
        assert!(options.durable);
        assert!(!options.auto_delete);
        assert!(!options.internal);
        assert!(matches!(options.exchange_type, ExchangeKind::Direct));
        assert!(matches!(options.original_type, ExchangeKind::Direct));
    }
}

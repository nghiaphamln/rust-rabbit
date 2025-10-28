use crate::{
    connection::ConnectionManager,
    error::{ProcessingError, RabbitError, Result},
    metrics::RustRabbitMetrics,
    publisher::{CustomExchangeDeclareOptions, CustomQueueDeclareOptions, Publisher},
    retry::{DelayedMessageExchange, RetryPolicy},
};
use async_trait::async_trait;
use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions as LapinQueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, ExchangeKind,
};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

/// Base consumer trait for processing messages with smart retry handling
///
/// This trait provides a simplified interface where:
/// - Messages are automatically ACK'd after successful processing
/// - Retryable errors automatically publish to delay exchange
/// - Non-retryable errors send to DLQ or discard based on configuration
#[async_trait]
pub trait BaseConsumer<T>: Send + Sync + 'static
where
    T: DeserializeOwned + Send + Sync,
{
    /// Process a message and return the result
    ///
    /// # Returns
    /// - `Ok(())` - Message processed successfully, will be ACK'd automatically
    /// - `Err(ProcessingError::Retryable { .. })` - Will retry with delay exchange
    /// - `Err(ProcessingError::NonRetryable { .. })` - Will reject/send to DLQ
    async fn handle(
        &self,
        message: T,
        context: MessageContext,
    ) -> std::result::Result<(), ProcessingError>;
}

/// Message handler trait for processing consumed messages (legacy, prefer BaseConsumer)
#[async_trait]
pub trait MessageHandler<T>: Send + Sync + 'static
where
    T: DeserializeOwned + Send + Sync,
{
    /// Handle a received message
    async fn handle(&self, message: T, context: MessageContext) -> MessageResult;
}

/// Context information for a received message
#[derive(Debug, Clone)]
pub struct MessageContext {
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub headers: FieldTable,
    pub timestamp: Option<u64>,
    pub retry_count: u32,
}

/// Result of message processing
#[derive(Debug)]
pub enum MessageResult {
    /// Message processed successfully
    Ack,
    /// Message processing failed, should be retried
    Retry,
    /// Message processing failed permanently, should be rejected
    Reject,
    /// Message processing failed, should be requeued
    Requeue,
}

/// Consumer options
#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    /// Queue name to consume from
    pub queue_name: String,

    /// Consumer tag (optional)
    pub consumer_tag: Option<String>,

    /// Number of concurrent message processors
    pub concurrency: usize,

    /// Prefetch count (QoS)
    pub prefetch_count: Option<u16>,

    /// Auto-declare queue before consuming
    pub auto_declare_queue: bool,

    /// Queue declaration options
    pub queue_options: CustomQueueDeclareOptions,

    /// Auto-declare exchange and bind to queue
    pub auto_declare_exchange: bool,

    /// Exchange name (if not provided, uses queue_name as exchange name)
    pub exchange_name: Option<String>,

    /// Exchange declaration options
    pub exchange_options: CustomExchangeDeclareOptions,

    /// Routing key for binding queue to exchange (default: queue_name)
    pub routing_key: Option<String>,

    /// Retry policy for failed messages
    pub retry_policy: Option<RetryPolicy>,

    /// Dead letter exchange for failed messages
    pub dead_letter_exchange: Option<String>,

    /// Auto-ack messages (not recommended for production)
    pub auto_ack: bool,

    /// Consumer exclusive mode
    pub exclusive: bool,

    /// Consumer arguments
    pub arguments: FieldTable,
}

impl ConsumerOptions {
    /// Create a new consumer options builder
    pub fn builder<S: Into<String>>(queue_name: S) -> ConsumerOptionsBuilder {
        ConsumerOptionsBuilder::new(queue_name.into())
    }
}

/// Builder for ConsumerOptions
#[derive(Debug, Clone)]
pub struct ConsumerOptionsBuilder {
    queue_name: String,
    consumer_tag: Option<String>,
    concurrency: usize,
    prefetch_count: Option<u16>,
    auto_declare_queue: bool,
    queue_options: CustomQueueDeclareOptions,
    auto_declare_exchange: bool,
    exchange_name: Option<String>,
    exchange_options: CustomExchangeDeclareOptions,
    routing_key: Option<String>,
    retry_policy: Option<RetryPolicy>,
    dead_letter_exchange: Option<String>,
    auto_ack: bool,
    exclusive: bool,
    arguments: FieldTable,
}

impl ConsumerOptionsBuilder {
    /// Create a new builder with default values
    pub fn new(queue_name: String) -> Self {
        Self {
            queue_name,
            consumer_tag: None,
            concurrency: 1,
            prefetch_count: Some(10),
            auto_declare_queue: false,
            queue_options: CustomQueueDeclareOptions::default(),
            auto_declare_exchange: false,
            exchange_name: None,
            exchange_options: CustomExchangeDeclareOptions::default(),
            routing_key: None,
            retry_policy: None,
            dead_letter_exchange: None,
            auto_ack: false,
            exclusive: false,
            arguments: FieldTable::default(),
        }
    }

    /// Set consumer tag
    pub fn consumer_tag<S: Into<String>>(mut self, tag: S) -> Self {
        self.consumer_tag = Some(tag.into());
        self
    }

    /// Set concurrency level
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set prefetch count
    pub fn prefetch_count(mut self, count: u16) -> Self {
        self.prefetch_count = Some(count);
        self
    }

    /// Disable prefetch limit
    pub fn no_prefetch_limit(mut self) -> Self {
        self.prefetch_count = None;
        self
    }

    /// Enable auto-declare queue
    pub fn auto_declare_queue(mut self) -> Self {
        self.auto_declare_queue = true;
        self
    }

    /// Enable auto-declare exchange and bind to queue
    pub fn auto_declare_exchange(mut self) -> Self {
        self.auto_declare_exchange = true;
        self
    }

    /// Set exchange name (if not set, uses queue_name)
    pub fn exchange_name<S: Into<String>>(mut self, name: S) -> Self {
        self.exchange_name = Some(name.into());
        self
    }

    /// Set exchange options
    pub fn exchange_options(mut self, options: CustomExchangeDeclareOptions) -> Self {
        self.exchange_options = options;
        self
    }

    /// Set routing key for binding (default: queue_name)
    pub fn routing_key<S: Into<String>>(mut self, key: S) -> Self {
        self.routing_key = Some(key.into());
        self
    }

    /// Set queue options
    pub fn queue_options(mut self, options: CustomQueueDeclareOptions) -> Self {
        self.queue_options = options;
        self
    }

    /// Set retry policy
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Set dead letter exchange
    pub fn dead_letter_exchange<S: Into<String>>(mut self, exchange: S) -> Self {
        self.dead_letter_exchange = Some(exchange.into());
        self
    }

    /// Enable auto-ack (not recommended for production)
    pub fn auto_ack(mut self) -> Self {
        self.auto_ack = true;
        self
    }

    /// Enable manual ack (recommended for production)
    pub fn manual_ack(mut self) -> Self {
        self.auto_ack = false;
        self
    }

    /// Enable exclusive mode
    pub fn exclusive(mut self) -> Self {
        self.exclusive = true;
        self
    }

    /// Configure for high throughput
    pub fn high_throughput(mut self) -> Self {
        self.concurrency = 20;
        self.prefetch_count = Some(50);
        self.auto_ack = false;
        self
    }

    /// Configure for reliability (lower throughput but safer)
    pub fn reliable(mut self) -> Self {
        self.concurrency = 1;
        self.prefetch_count = Some(1);
        self.auto_ack = false;
        self
    }

    /// Configure for development (simpler settings)
    pub fn development(mut self) -> Self {
        self.concurrency = 1;
        self.prefetch_count = Some(1);
        self.auto_ack = true;
        self.auto_declare_queue = true;
        self.auto_declare_exchange = true; // Auto-declare exchange in development
        self
    }

    /// Configure for minutes exponential retry (1min, 2min, 4min, 8min, 16min - max 5 retries)
    /// This preset automatically sets up:
    /// - Auto declare queue and exchange
    /// - Retry policy with minutes exponential backoff
    /// - Dead letter exchange/queue based on queue name
    /// - Reliable processing settings
    pub fn minutes_retry(mut self) -> Self {
        let queue_name = self.queue_name.clone();

        self.auto_declare_queue = true;
        self.auto_declare_exchange = true;
        self.retry_policy = Some(RetryPolicy::minutes_exponential_for_queue(&queue_name));
        self.concurrency = 1; // Process one at a time for reliable retries
        self.prefetch_count = Some(1); // One message at a time
        self.auto_ack = false; // Manual ack for retry support
        self
    }

    /// Build the final configuration
    pub fn build(self) -> ConsumerOptions {
        ConsumerOptions {
            queue_name: self.queue_name,
            consumer_tag: self.consumer_tag,
            concurrency: self.concurrency,
            prefetch_count: self.prefetch_count,
            auto_declare_queue: self.auto_declare_queue,
            queue_options: self.queue_options,
            auto_declare_exchange: self.auto_declare_exchange,
            exchange_name: self.exchange_name,
            exchange_options: self.exchange_options,
            routing_key: self.routing_key,
            retry_policy: self.retry_policy,
            dead_letter_exchange: self.dead_letter_exchange,
            auto_ack: self.auto_ack,
            exclusive: self.exclusive,
            arguments: self.arguments,
        }
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            queue_name: String::new(),
            consumer_tag: None,
            concurrency: 1,
            prefetch_count: Some(10),
            auto_declare_queue: false,
            queue_options: CustomQueueDeclareOptions::default(),
            auto_declare_exchange: false,
            exchange_name: None,
            exchange_options: CustomExchangeDeclareOptions::default(),
            routing_key: None,
            retry_policy: None,
            dead_letter_exchange: None,
            auto_ack: false,
            exclusive: false,
            arguments: FieldTable::default(),
        }
    }
}

/// Consumer for receiving messages from RabbitMQ
pub struct Consumer {
    #[allow(dead_code)] // Will be used for connection health monitoring
    connection_manager: ConnectionManager,
    options: ConsumerOptions,
    channel: Channel,
    semaphore: Arc<Semaphore>,
    metrics: Option<RustRabbitMetrics>,
    publisher: Publisher,
}

impl Consumer {
    /// Create a new consumer
    pub async fn new(
        connection_manager: ConnectionManager,
        options: ConsumerOptions,
    ) -> Result<Self> {
        let connection = connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Set QoS if prefetch_count is specified
        if let Some(prefetch_count) = options.prefetch_count {
            debug!("Setting prefetch_count: {}", prefetch_count);
            channel
                .basic_qos(
                    prefetch_count,
                    lapin::options::BasicQosOptions { global: false },
                )
                .await
                .map_err(|e| {
                    error!("Failed to set QoS prefetch_count={}: {}", prefetch_count, e);
                    RabbitError::Connection(e)
                })?;
            debug!("Successfully set prefetch_count: {}", prefetch_count);
        }

        // Declare queue if auto_declare is enabled
        if options.auto_declare_queue {
            Self::declare_queue_and_exchange(&channel, &options).await?;
        }

        let semaphore = Arc::new(Semaphore::new(options.concurrency));

        // Setup delayed exchange infrastructure if retry policy is configured
        if options.retry_policy.is_some() {
            Self::setup_retry_infrastructure(&connection_manager, &options).await?;
        }

        let publisher = Publisher::new(connection_manager.clone());

        Ok(Self {
            connection_manager,
            options,
            channel,
            semaphore,
            metrics: None,
            publisher,
        })
    }

    /// Set metrics for this consumer
    pub fn set_metrics(&mut self, metrics: RustRabbitMetrics) {
        self.metrics = Some(metrics);
    }

    /// Consume messages using BaseConsumer trait with automatic retry handling
    pub async fn consume_with_base_consumer<T, H>(&self, handler: Arc<H>) -> Result<()>
    where
        T: DeserializeOwned + Send + Sync + 'static,
        H: BaseConsumer<T>,
    {
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;
        let publisher = Publisher::new(self.connection_manager.clone());

        // Set up QoS if prefetch count is specified
        if let Some(prefetch_count) = self.options.prefetch_count {
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await?;
        }

        let semaphore = Arc::new(Semaphore::new(self.options.concurrency));

        // Consume messages
        let mut consumer = channel
            .basic_consume(
                &self.options.queue_name,
                self.options.consumer_tag.as_deref().unwrap_or(""),
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: self.options.auto_ack,
                    exclusive: self.options.exclusive,
                    nowait: false,
                },
                self.options.arguments.clone(),
            )
            .await?;

        info!(
            "Started consuming from queue: {} with BaseConsumer",
            self.options.queue_name
        );

        // Process messages
        while let Some(delivery) = consumer.next().await {
            let delivery = delivery?;
            let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                RabbitError::Generic(anyhow::anyhow!("Semaphore acquire error: {}", e))
            })?;

            let handler_clone = handler.clone();
            let retry_policy = self.options.retry_policy.clone();
            let dead_letter_exchange = self.options.dead_letter_exchange.clone();
            let channel_clone = channel.clone();
            let publisher_clone = publisher.clone();
            let exchange_name = self
                .options
                .exchange_name
                .clone()
                .unwrap_or_else(|| self.options.queue_name.clone());

            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = Self::process_message_with_base_consumer(
                    delivery,
                    handler_clone,
                    retry_policy,
                    dead_letter_exchange,
                    channel_clone,
                    publisher_clone,
                    exchange_name,
                )
                .await
                {
                    error!("Error processing message with BaseConsumer: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Process a single message using BaseConsumer
    async fn process_message_with_base_consumer<T, H>(
        delivery: Delivery,
        handler: Arc<H>,
        retry_policy: Option<RetryPolicy>,
        dead_letter_exchange: Option<String>,
        channel: Channel,
        publisher: Publisher,
        exchange_name: String,
    ) -> Result<()>
    where
        T: DeserializeOwned + Send + Sync,
        H: BaseConsumer<T>,
    {
        let context = Self::build_message_context(&delivery);

        // Deserialize message
        let message: T = match serde_json::from_slice(&delivery.data) {
            Ok(msg) => msg,
            Err(e) => {
                error!("Failed to deserialize message: {}", e);
                Self::reject_message(&delivery, &channel, false).await?;
                return Ok(());
            }
        };

        // Handle message with BaseConsumer
        match handler.handle(message, context.clone()).await {
            Ok(()) => {
                // Success - automatically ACK the message
                Self::ack_message(&delivery, &channel).await?;
                debug!(
                    "Message processed successfully and ACK'd: {}",
                    delivery.delivery_tag
                );
            }
            Err(ProcessingError::Retryable {
                message: error_msg,
                custom_delay,
            }) => {
                // Retryable error - send to retry/delay exchange
                if let Some(ref policy) = retry_policy {
                    info!("Retryable error occurred: {}. Scheduling retry.", error_msg);

                    // Use custom delay if specified, otherwise calculate from policy
                    let delay = custom_delay
                        .unwrap_or_else(|| policy.calculate_delay(context.retry_count + 1));

                    Self::handle_retry_with_delay(
                        &delivery,
                        &channel,
                        &context,
                        policy,
                        &publisher,
                        &exchange_name,
                        delay,
                    )
                    .await?;
                } else {
                    warn!(
                        "Retryable error but no retry policy configured. Rejecting message: {}",
                        error_msg
                    );
                    Self::reject_message(&delivery, &channel, false).await?;
                }
            }
            Err(ProcessingError::NonRetryable {
                message: error_msg,
                send_to_dlq,
            }) => {
                // Non-retryable error
                error!("Non-retryable error occurred: {}", error_msg);

                if send_to_dlq {
                    if let Some(ref dle) = dead_letter_exchange {
                        Self::send_to_dead_letter(&delivery, dle, &context, &publisher).await?;
                    } else {
                        warn!("Error should go to DLQ but no dead letter exchange configured. Rejecting message.");
                        Self::reject_message(&delivery, &channel, false).await?;
                    }
                } else {
                    // Discard the message (reject without DLQ)
                    info!(
                        "Discarding message due to non-retryable error: {}",
                        error_msg
                    );
                    Self::reject_message(&delivery, &channel, false).await?;
                }
            }
        }

        Ok(())
    }

    /// Handle retry with custom delay
    async fn handle_retry_with_delay(
        delivery: &Delivery,
        channel: &Channel,
        context: &MessageContext,
        retry_policy: &RetryPolicy,
        publisher: &Publisher,
        exchange_name: &str,
        delay: std::time::Duration,
    ) -> Result<()> {
        let max_retries = retry_policy.max_retries;
        let current_retry = context.retry_count;

        if current_retry >= max_retries {
            warn!(
                "Max retries ({}) exceeded for message, sending to dead letter",
                max_retries
            );

            // Send to dead letter exchange if configured
            if let Some(dlx) = &retry_policy.dead_letter_exchange {
                Self::send_to_dead_letter(delivery, dlx, context, publisher).await?;
            } else {
                Self::reject_message(delivery, channel, false).await?;
            }
            return Ok(());
        }

        // Create delayed exchange name
        let delayed_exchange_name = format!("{}.retry", exchange_name);

        // Prepare message for retry with updated headers
        let mut headers = delivery.properties.headers().clone().unwrap_or_default();
        headers.insert(
            "x-retry-count".into(),
            lapin::types::AMQPValue::LongInt((current_retry + 1) as i32),
        );
        headers.insert(
            "x-original-exchange".into(),
            lapin::types::AMQPValue::LongString(exchange_name.into()),
        );
        headers.insert(
            "x-original-routing-key".into(),
            lapin::types::AMQPValue::LongString(delivery.routing_key.to_string().into()),
        );

        // Set delay
        headers.insert(
            "x-delay".into(),
            lapin::types::AMQPValue::LongInt(delay.as_millis() as i32),
        );

        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_headers(headers);

        // Publish to delay exchange
        let connection = publisher.get_connection().await?;
        let retry_channel = connection.create_channel().await?;

        retry_channel
            .basic_publish(
                &delayed_exchange_name,
                delivery.routing_key.as_str(), // Use original routing key as string
                BasicPublishOptions::default(),
                &delivery.data,
                properties,
            )
            .await?;

        // ACK the original message since we've scheduled retry
        Self::ack_message(delivery, channel).await?;

        info!(
            "Message scheduled for retry #{} with delay {:?}ms",
            current_retry + 1,
            delay.as_millis()
        );

        Ok(())
    }

    /// Start consuming messages with the given handler (legacy MessageHandler trait)
    pub async fn consume<T, H>(&self, handler: Arc<H>) -> Result<()>
    where
        T: DeserializeOwned + Send + Sync + 'static,
        H: MessageHandler<T>,
    {
        let consumer_tag = self
            .options
            .consumer_tag
            .clone()
            .unwrap_or_else(|| format!("rust-rabbit-{}", uuid::Uuid::new_v4()));

        let consume_options = BasicConsumeOptions {
            no_local: false,
            no_ack: self.options.auto_ack,
            exclusive: self.options.exclusive,
            nowait: false,
        };

        let mut consumer = self
            .channel
            .basic_consume(
                &self.options.queue_name,
                &consumer_tag,
                consume_options,
                self.options.arguments.clone(),
            )
            .await?;

        info!(
            "Started consuming from queue: {} with tag: {}",
            self.options.queue_name, consumer_tag
        );

        while let Some(delivery) = consumer.next().await {
            let delivery = delivery?;
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| RabbitError::Generic(e.into()))?;

            let handler = handler.clone();
            let retry_policy = self.options.retry_policy.clone();
            let dead_letter_exchange = self.options.dead_letter_exchange.clone();
            let channel = self.channel.clone();
            let publisher = self.publisher.clone();
            let exchange_name = self
                .options
                .exchange_name
                .clone()
                .unwrap_or_else(|| self.options.queue_name.clone());

            // Process message in a separate task
            tokio::spawn(async move {
                let _permit = permit; // Hold the permit for the duration of processing

                if let Err(e) = Self::process_message::<T, H>(
                    delivery,
                    handler,
                    retry_policy,
                    dead_letter_exchange,
                    channel,
                    publisher,
                    exchange_name,
                )
                .await
                {
                    error!("Error processing message: {}", e);
                }
            });
        }

        warn!(
            "Consumer stream ended for queue: {}",
            self.options.queue_name
        );
        Ok(())
    }

    /// Process a single message
    async fn process_message<T, H>(
        delivery: Delivery,
        handler: Arc<H>,
        retry_policy: Option<RetryPolicy>,
        dead_letter_exchange: Option<String>,
        channel: Channel,
        publisher: Publisher,
        exchange_name: String,
    ) -> Result<()>
    where
        T: DeserializeOwned + Send + Sync,
        H: MessageHandler<T>,
    {
        let context = Self::build_message_context(&delivery);

        // Deserialize message
        let message: T = match serde_json::from_slice(&delivery.data) {
            Ok(msg) => msg,
            Err(e) => {
                error!("Failed to deserialize message: {}", e);
                Self::reject_message(&delivery, &channel, false).await?;
                return Ok(());
            }
        };

        // Handle message
        let result = handler.handle(message, context.clone()).await;

        match result {
            MessageResult::Ack => {
                Self::ack_message(&delivery, &channel).await?;
                debug!("Message acknowledged: {}", delivery.delivery_tag);
            }
            MessageResult::Retry => {
                if let Some(ref policy) = retry_policy {
                    Self::handle_retry(
                        &delivery,
                        &channel,
                        &context,
                        policy,
                        &publisher,
                        &exchange_name,
                    )
                    .await?;
                } else {
                    Self::reject_message(&delivery, &channel, true).await?;
                }
            }
            MessageResult::Reject => {
                if let Some(ref dle) = dead_letter_exchange {
                    Self::send_to_dead_letter(&delivery, dle, &context, &publisher).await?;
                } else {
                    Self::reject_message(&delivery, &channel, false).await?;
                }
            }
            MessageResult::Requeue => {
                Self::reject_message(&delivery, &channel, true).await?;
            }
        }

        Ok(())
    }

    /// Build message context from delivery
    fn build_message_context(delivery: &Delivery) -> MessageContext {
        let properties = &delivery.properties;

        MessageContext {
            message_id: properties.message_id().as_ref().map(|s| s.to_string()),
            correlation_id: properties.correlation_id().as_ref().map(|s| s.to_string()),
            reply_to: properties.reply_to().as_ref().map(|s| s.to_string()),
            delivery_tag: delivery.delivery_tag,
            redelivered: delivery.redelivered,
            exchange: delivery.exchange.to_string(),
            routing_key: delivery.routing_key.to_string(),
            headers: properties.headers().clone().unwrap_or_default(),
            timestamp: *properties.timestamp(),
            retry_count: Self::get_retry_count_from_headers(
                properties
                    .headers()
                    .as_ref()
                    .unwrap_or(&FieldTable::default()),
            ),
        }
    }

    /// Get retry count from message headers
    fn get_retry_count_from_headers(headers: &FieldTable) -> u32 {
        headers
            .inner()
            .get("x-retry-count")
            .and_then(|v| match v {
                lapin::types::AMQPValue::LongInt(count) => Some(*count as u32),
                lapin::types::AMQPValue::LongLongInt(count) => Some(*count as u32),
                _ => None,
            })
            .unwrap_or(0)
    }

    /// Acknowledge a message
    async fn ack_message(delivery: &Delivery, channel: &Channel) -> Result<()> {
        channel
            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await?;
        Ok(())
    }

    /// Reject a message
    async fn reject_message(delivery: &Delivery, channel: &Channel, requeue: bool) -> Result<()> {
        channel
            .basic_nack(
                delivery.delivery_tag,
                BasicNackOptions {
                    multiple: false,
                    requeue,
                },
            )
            .await?;
        Ok(())
    }

    /// Handle retry logic
    async fn handle_retry(
        delivery: &Delivery,
        channel: &Channel,
        context: &MessageContext,
        retry_policy: &RetryPolicy,
        publisher: &Publisher,
        exchange_name: &str,
    ) -> Result<()> {
        if context.retry_count >= retry_policy.max_retries {
            warn!(
                "Max retries exceeded for message: {}",
                delivery.delivery_tag
            );

            // Send to dead letter exchange if configured
            if let Some(ref dle) = retry_policy.dead_letter_exchange {
                Self::send_to_dead_letter(delivery, dle, context, publisher).await?;
            } else {
                Self::reject_message(delivery, channel, false).await?;
            }
            return Ok(());
        }

        // Calculate delay for next retry
        let delay = retry_policy.calculate_delay(context.retry_count);
        let delayed_exchange_name = format!("{}.retry", exchange_name);

        // Create retry message with updated headers
        let mut headers = delivery.properties.headers().clone().unwrap_or_default();
        headers.insert(
            "x-retry-count".into(),
            lapin::types::AMQPValue::LongInt((context.retry_count + 1) as i32),
        );
        headers.insert(
            "x-original-queue".into(),
            lapin::types::AMQPValue::LongString(context.routing_key.clone().into()),
        );

        // Build properties with delay header for delayed message exchange
        let mut properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_headers(headers);

        // Add delay header for delayed message exchange
        let mut delay_headers = properties.headers().clone().unwrap_or_default();
        delay_headers.insert(
            "x-delay".into(),
            lapin::types::AMQPValue::LongLongInt(delay.as_millis() as i64),
        );
        properties = properties.with_headers(delay_headers);

        // Publish to delayed exchange using channel
        channel
            .basic_publish(
                &delayed_exchange_name,
                &context.routing_key,
                BasicPublishOptions::default(),
                &delivery.data,
                properties,
            )
            .await?;

        info!(
            "Retrying message after {:?} (attempt {})",
            delay,
            context.retry_count + 1
        );

        // Acknowledge the original message since we've republished it
        Self::ack_message(delivery, channel).await?;

        Ok(())
    }

    /// Send message to dead letter exchange
    async fn send_to_dead_letter(
        delivery: &Delivery,
        dead_letter_exchange: &str,
        _context: &MessageContext,
        publisher: &Publisher,
    ) -> Result<()> {
        // Create dead letter message with additional headers
        let mut headers = delivery.properties.headers().clone().unwrap_or_default();
        headers.insert(
            "x-death-reason".into(),
            lapin::types::AMQPValue::LongString("max-retries-exceeded".into()),
        );
        headers.insert(
            "x-death-time".into(),
            lapin::types::AMQPValue::LongLongInt(chrono::Utc::now().timestamp_millis()),
        );

        // Build properties for dead letter message
        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_headers(headers);

        // Get connection and publish to dead letter exchange
        let connection = publisher.get_connection().await?;
        let dlx_channel = connection.create_channel().await?;

        dlx_channel
            .basic_publish(
                dead_letter_exchange,
                "dead-letter", // routing key for dead letter
                BasicPublishOptions::default(),
                &delivery.data,
                properties,
            )
            .await?;

        warn!(
            "Sent message to dead letter exchange: {}",
            dead_letter_exchange
        );

        Ok(())
    }

    /// Stop consuming (close the consumer)
    pub async fn stop(&self) -> Result<()> {
        // The consumer will stop when the channel is closed
        // or when the stream ends
        info!("Stopping consumer for queue: {}", self.options.queue_name);
        Ok(())
    }

    /// Declare queue and optionally exchange with binding
    async fn declare_queue_and_exchange(
        channel: &Channel,
        options: &ConsumerOptions,
    ) -> Result<()> {
        // First declare the queue
        let queue_options = LapinQueueDeclareOptions {
            passive: options.queue_options.passive,
            durable: options.queue_options.durable,
            exclusive: options.queue_options.exclusive,
            auto_delete: options.queue_options.auto_delete,
            nowait: false,
        };

        channel
            .queue_declare(
                &options.queue_name,
                queue_options,
                options.queue_options.arguments.clone(),
            )
            .await?;

        debug!("Declared queue: {}", options.queue_name);

        // Declare exchange and bind if auto_declare_exchange is enabled
        if options.auto_declare_exchange {
            let exchange_name = options
                .exchange_name
                .as_ref()
                .unwrap_or(&options.queue_name);

            // Declare exchange
            let exchange_options = ExchangeDeclareOptions {
                passive: options.exchange_options.passive,
                durable: options.exchange_options.durable,
                auto_delete: options.exchange_options.auto_delete,
                internal: options.exchange_options.internal,
                nowait: false,
            };

            // Handle delayed message exchange if needed
            let mut arguments = options.exchange_options.arguments.clone();
            if matches!(options.exchange_options.exchange_type, ExchangeKind::Custom(ref kind) if kind == "x-delayed-message")
            {
                arguments.insert(
                    "x-delayed-type".into(),
                    lapin::types::AMQPValue::LongString(
                        match options.exchange_options.original_type {
                            ExchangeKind::Direct => "direct".into(),
                            ExchangeKind::Fanout => "fanout".into(),
                            ExchangeKind::Topic => "topic".into(),
                            ExchangeKind::Headers => "headers".into(),
                            ExchangeKind::Custom(ref s) => s.clone().into(),
                        },
                    ),
                );
            }

            channel
                .exchange_declare(
                    exchange_name,
                    options.exchange_options.exchange_type.clone(),
                    exchange_options,
                    arguments,
                )
                .await?;

            debug!("Declared exchange: {}", exchange_name);

            // Bind queue to exchange
            let routing_key = options.routing_key.as_ref().unwrap_or(&options.queue_name);

            channel
                .queue_bind(
                    &options.queue_name,
                    exchange_name,
                    routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;

            debug!(
                "Bound queue '{}' to exchange '{}' with routing key '{}'",
                options.queue_name, exchange_name, routing_key
            );
        }

        Ok(())
    }

    /// Setup retry infrastructure (delayed exchange) if retry policy is configured
    async fn setup_retry_infrastructure(
        connection_manager: &ConnectionManager,
        options: &ConsumerOptions,
    ) -> Result<()> {
        if let Some(ref retry_policy) = options.retry_policy {
            // Create delayed exchange name
            let delayed_exchange_name = format!(
                "{}.retry",
                options
                    .exchange_name
                    .as_ref()
                    .unwrap_or(&options.queue_name)
            );

            // Create DelayedMessageExchange instance and setup infrastructure
            let delayed_exchange = DelayedMessageExchange::new(
                connection_manager.clone(),
                delayed_exchange_name.clone(),
                retry_policy.clone(),
            );

            // Setup the delayed exchange and dead letter infrastructure
            delayed_exchange.setup().await?;

            // Setup queue binding for retry mechanism
            delayed_exchange
                .setup_queue_retry(&options.queue_name)
                .await?;

            debug!(
                "Setup retry infrastructure for queue: {} with delayed exchange: {}",
                options.queue_name, delayed_exchange_name
            );
        }

        Ok(())
    }
}

// Example message handler implementation
pub struct SimpleMessageHandler<F, T>
where
    F: Fn(T, MessageContext) -> MessageResult + Send + Sync,
    T: DeserializeOwned + Send + Sync,
{
    handler_fn: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<F, T> SimpleMessageHandler<F, T>
where
    F: Fn(T, MessageContext) -> MessageResult + Send + Sync + 'static,
    T: DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(handler_fn: F) -> Self {
        Self {
            handler_fn,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, T> MessageHandler<T> for SimpleMessageHandler<F, T>
where
    F: Fn(T, MessageContext) -> MessageResult + Send + Sync + 'static,
    T: DeserializeOwned + Send + Sync + 'static,
{
    async fn handle(&self, message: T, context: MessageContext) -> MessageResult {
        (self.handler_fn)(message, context)
    }
}

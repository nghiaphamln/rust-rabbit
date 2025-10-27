use crate::{
    connection::ConnectionManager,
    error::{RabbitError, Result},
    publisher::CustomQueueDeclareOptions,
    retry::RetryPolicy,
};
use async_trait::async_trait;
use futures::StreamExt;
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions,
        QueueDeclareOptions as LapinQueueDeclareOptions,
    },
    types::FieldTable,
    Channel,
};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

/// Message handler trait for processing consumed messages
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
            channel
                .basic_qos(prefetch_count, lapin::options::BasicQosOptions::default())
                .await?;
        }

        // Declare queue if auto_declare is enabled
        if options.auto_declare_queue {
            Self::declare_queue(&channel, &options).await?;
        }

        let semaphore = Arc::new(Semaphore::new(options.concurrency));

        Ok(Self {
            connection_manager,
            options,
            channel,
            semaphore,
        })
    }

    /// Start consuming messages with the given handler
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

            // Process message in a separate task
            tokio::spawn(async move {
                let _permit = permit; // Hold the permit for the duration of processing

                if let Err(e) = Self::process_message::<T, H>(
                    delivery,
                    handler,
                    retry_policy,
                    dead_letter_exchange,
                    channel,
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
                    Self::handle_retry(&delivery, &channel, &context, policy).await?;
                } else {
                    Self::reject_message(&delivery, &channel, true).await?;
                }
            }
            MessageResult::Reject => {
                if let Some(ref dle) = dead_letter_exchange {
                    Self::send_to_dead_letter(&delivery, &channel, dle, &context).await?;
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
    ) -> Result<()> {
        if context.retry_count >= retry_policy.max_retries {
            warn!(
                "Max retries exceeded for message: {}",
                delivery.delivery_tag
            );
            Self::reject_message(delivery, channel, false).await?;
            return Ok(());
        }

        // Calculate delay for next retry
        let delay = retry_policy.calculate_delay(context.retry_count);

        // For now, just requeue the message
        // In a production implementation, you would use the delayed message exchange
        // or implement a retry queue pattern
        info!(
            "Retrying message after {:?} (attempt {})",
            delay,
            context.retry_count + 1
        );
        Self::reject_message(delivery, channel, true).await?;

        Ok(())
    }

    /// Send message to dead letter exchange
    async fn send_to_dead_letter(
        delivery: &Delivery,
        channel: &Channel,
        dead_letter_exchange: &str,
        _context: &MessageContext,
    ) -> Result<()> {
        // In a real implementation, you would republish the message to the DLE
        // For now, just reject without requeue
        warn!(
            "Sending message to dead letter exchange: {}",
            dead_letter_exchange
        );
        Self::reject_message(delivery, channel, false).await?;
        Ok(())
    }

    /// Declare queue with options
    async fn declare_queue(channel: &Channel, options: &ConsumerOptions) -> Result<()> {
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
        Ok(())
    }

    /// Stop consuming (close the consumer)
    pub async fn stop(&self) -> Result<()> {
        // The consumer will stop when the channel is closed
        // or when the stream ends
        info!("Stopping consumer for queue: {}", self.options.queue_name);
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

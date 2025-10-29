use crate::{
    connection::Connection, 
    error::RustRabbitError, 
    message::{ErrorType, MessageEnvelope, WireMessage},
    retry::RetryConfig
};
use futures_lite::stream::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::{FieldTable, AMQPValue},
    BasicProperties, Channel,
};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

/// Message wrapper with retry tracking
#[derive(Debug)]
pub struct Message<T>
where
    T: Clone,
{
    pub data: T,
    pub retry_attempt: u32,
    tag: u64,
    channel: Arc<Channel>,
}

impl<T> Clone for Message<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            retry_attempt: self.retry_attempt,
            tag: self.tag,
            channel: Arc::clone(&self.channel),
        }
    }
}

impl<T> Message<T>
where
    T: Clone,
{
    /// Acknowledge the message
    pub async fn ack(&self) -> Result<(), RustRabbitError> {
        self.channel
            .basic_ack(self.tag, BasicAckOptions::default())
            .await
            .map_err(RustRabbitError::from)
    }

    /// Reject and requeue the message
    pub async fn nack(&self, requeue: bool) -> Result<(), RustRabbitError> {
        self.channel
            .basic_nack(
                self.tag,
                lapin::options::BasicNackOptions {
                    multiple: false,
                    requeue,
                },
            )
            .await
            .map_err(RustRabbitError::from)
    }
}

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

    /// Set concurrency level (same as prefetch count)
    pub fn concurrency(mut self, count: u16) -> Self {
        self.prefetch_count = Some(count);
        self
    }

    /// Configure retry behavior
    pub fn with_retry(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
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
    /// Create a new consumer builder
    pub fn builder(connection: Arc<Connection>, queue_name: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::new(connection, queue_name)
    }

    /// Create retry queue with TTL
    async fn create_retry_queue(
        &self,
        channel: &Channel,
        retry_attempt: u32,
        delay: std::time::Duration,
    ) -> Result<String, RustRabbitError> {
        let retry_queue_name = format!("{}.retry.{}", self.queue_name, retry_attempt);
        let delay_ms = delay.as_millis() as i64;
        
        // Create retry queue with TTL that routes back to original queue
        let mut args = FieldTable::default();
        args.insert("x-message-ttl".into(), AMQPValue::LongLongInt(delay_ms));
        args.insert("x-dead-letter-exchange".into(), AMQPValue::LongString("".into())); // Default exchange
        args.insert("x-dead-letter-routing-key".into(), AMQPValue::LongString(self.queue_name.clone().into()));
        
        channel
            .queue_declare(
                &retry_queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await?;
            
        debug!("Created retry queue: {} with TTL: {}ms", retry_queue_name, delay_ms);
        Ok(retry_queue_name)
    }

    /// Create DLQ (Dead Letter Queue)
    async fn create_dlq(&self, channel: &Channel) -> Result<String, RustRabbitError> {
        let dlq_name = format!("{}.dlq", self.queue_name);
        
        channel
            .queue_declare(
                &dlq_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
            
        debug!("Created DLQ: {}", dlq_name);
        Ok(dlq_name)
    }

    /// Send message to retry queue with delay
    async fn send_to_retry_queue(
        &self,
        channel: &Channel,
        message_data: &[u8],
        retry_attempt: u32,
        delay: std::time::Duration,
    ) -> Result<(), RustRabbitError> {
        let retry_queue_name = self.create_retry_queue(channel, retry_attempt, delay).await?;
        
        // Publish to retry queue
        channel
            .basic_publish(
                "", // Default exchange
                &retry_queue_name,
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // Persistent
            )
            .await?
            .await?;
            
        debug!("Sent message to retry queue: {}", retry_queue_name);
        Ok(())
    }

    /// Send message to DLQ
    async fn send_to_dlq_simple(
        &self,
        channel: &Channel,
        message_data: &[u8],
    ) -> Result<(), RustRabbitError> {
        let dlq_name = self.create_dlq(channel).await?;
        
        // Publish to DLQ
        channel
            .basic_publish(
                "", // Default exchange
                &dlq_name,
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // Persistent
            )
            .await?
            .await?;
            
        debug!("Sent message to DLQ: {}", dlq_name);
        Ok(())
    }

    /// Start consuming messages
    pub async fn consume<T, H, Fut>(&self, handler: H) -> Result<(), RustRabbitError>
    where
        T: DeserializeOwned + Send + Clone + Sync + 'static + serde::Serialize,
        H: Fn(Message<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let channel = self.connection.create_channel().await?;

        // Set prefetch count
        channel
            .basic_qos(
                self.prefetch_count,
                lapin::options::BasicQosOptions::default(),
            )
            .await?;

        // Setup infrastructure (queues, exchanges)
        self.setup_infrastructure(&channel).await?;

        // Start consuming
        let mut consumer = channel
            .basic_consume(
                &self.queue_name,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let semaphore = Arc::new(Semaphore::new(self.prefetch_count as usize));

        debug!("Started consuming from queue: {}", self.queue_name);

        // Process messages
        while let Some(delivery_result) = consumer.next().await {
            let delivery = delivery_result?;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let handler_clone = handler.clone();
            let auto_ack = self.auto_ack;
            let channel_clone = Arc::new(channel.clone());
            let retry_config = self.retry_config.clone();
            let consumer_self = Consumer {
                connection: self.connection.clone(),
                queue_name: self.queue_name.clone(),
                exchange_name: self.exchange_name.clone(),
                routing_key: self.routing_key.clone(),
                retry_config: self.retry_config.clone(),
                prefetch_count: self.prefetch_count,
                auto_ack: self.auto_ack,
            };

            tokio::spawn(async move {
                let _permit = permit;

                // Deserialize as WireMessage format
                match serde_json::from_slice::<crate::message::WireMessage<T>>(&delivery.data) {
                    Ok(wire_msg) => {
                        let message = Message {
                            data: wire_msg.data,
                            retry_attempt: wire_msg.retry_attempt,
                            tag: delivery.delivery_tag,
                            channel: channel_clone.clone(),
                        };

                        // Process message
                        match handler_clone(message.clone()).await {
                            Ok(()) => {
                                if auto_ack {
                                    if let Err(e) = message.ack().await {
                                        error!("Failed to ack message: {}", e);
                                    }
                                }
                                debug!("Message processed successfully");
                            }
                            Err(e) => {
                                error!("Handler error: {}", e);
                                if auto_ack {
                                    // Check if retry is configured
                                    if let Some(retry_cfg) = &retry_config {
                                        if message.retry_attempt < retry_cfg.max_retries {
                                            // Calculate delay for next retry
                                            if let Some(delay) = retry_cfg.calculate_delay(message.retry_attempt) {
                                                warn!(
                                                    "Scheduling retry {} with delay {:?} for message", 
                                                    message.retry_attempt + 1, 
                                                    delay
                                                );
                                                
                                                // Update retry attempt in wire message
                                                let wire_msg = WireMessage {
                                                    data: message.data.clone(),
                                                    retry_attempt: message.retry_attempt + 1,
                                                };
                                                
                                                let retry_payload = match serde_json::to_vec(&wire_msg) {
                                                    Ok(payload) => payload,
                                                    Err(e) => {
                                                        error!("Failed to serialize retry message: {}", e);
                                                        if let Err(e) = message.nack(false).await {
                                                            error!("Failed to nack message: {}", e);
                                                        }
                                                        return;
                                                    }
                                                };
                                                
                                                // Send to retry queue with delay
                                                if let Err(e) = consumer_self.send_to_retry_queue(
                                                    &channel_clone,
                                                    &retry_payload,
                                                    message.retry_attempt + 1,
                                                    delay,
                                                ).await {
                                                    error!("Failed to send to retry queue: {}", e);
                                                    if let Err(e) = message.nack(false).await {
                                                        error!("Failed to nack message: {}", e);
                                                    }
                                                    return;
                                                }
                                                
                                                // ACK original message (it's now in retry queue)
                                                if let Err(e) = message.ack().await {
                                                    error!("Failed to ack message after retry: {}", e);
                                                }
                                            } else {
                                                // No more retries, send to DLQ
                                                warn!("Retry exhausted, sending to DLQ");
                                                if let Err(e) = consumer_self.send_to_dlq_simple(&channel_clone, &delivery.data).await {
                                                    error!("Failed to send to DLQ: {}", e);
                                                }
                                                if let Err(e) = message.ack().await {
                                                    error!("Failed to ack message after DLQ: {}", e);
                                                }
                                            }
                                        } else {
                                            // Retry exhausted, send to DLQ
                                            warn!("Max retries reached, sending to DLQ");
                                            if let Err(e) = consumer_self.send_to_dlq_simple(&channel_clone, &delivery.data).await {
                                                error!("Failed to send to DLQ: {}", e);
                                            }
                                            if let Err(e) = message.ack().await {
                                                error!("Failed to ack message after DLQ: {}", e);
                                            }
                                        }
                                    } else {
                                        // No retry config, just nack
                                        if let Err(e) = message.nack(false).await {
                                            error!("Failed to nack message: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize message: {}", e);
                        if auto_ack {
                            // Reject malformed messages
                            if let Err(e) = channel_clone
                                .basic_nack(
                                    delivery.delivery_tag,
                                    lapin::options::BasicNackOptions {
                                        multiple: false,
                                        requeue: false,
                                    },
                                )
                                .await
                            {
                                error!("Failed to nack malformed message: {}", e);
                            }
                        }
                    }
                }
            });
        }

        Ok(())
    }



    /// Setup queue and exchange infrastructure
    async fn setup_infrastructure(&self, channel: &Channel) -> Result<(), RustRabbitError> {
        // Declare queue
        channel
            .queue_declare(
                &self.queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        // Bind to exchange if specified
        if let (Some(exchange), Some(routing_key)) = (&self.exchange_name, &self.routing_key) {
            channel
                .queue_bind(
                    &self.queue_name,
                    exchange,
                    routing_key,
                    lapin::options::QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
        }

        Ok(())
    }

    /// Start consuming message envelopes with full retry support
    pub async fn consume_envelopes<T, H, Fut>(&self, handler: H) -> Result<(), RustRabbitError>
    where
        T: DeserializeOwned + Send + Clone + Sync + 'static + serde::Serialize,
        H: Fn(MessageEnvelope<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let channel = self.connection.create_channel().await?;
        let retry_config = self.retry_config.clone();

        // Set prefetch count
        channel
            .basic_qos(
                self.prefetch_count,
                lapin::options::BasicQosOptions::default(),
            )
            .await?;

        // Setup queue and exchange
        self.setup_infrastructure(&channel).await?;

        // Create consumer
        let mut consumer = channel
            .basic_consume(
                &self.queue_name,
                "rust-rabbit-envelope-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let semaphore = Arc::new(Semaphore::new(self.prefetch_count as usize));

        debug!("Started consuming envelopes from queue: {}", self.queue_name);

        // Process message envelopes with retry support
        while let Some(delivery_result) = consumer.next().await {
            let delivery = delivery_result?;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let handler_clone = handler.clone();
            let auto_ack = self.auto_ack;
            let channel_clone = Arc::new(channel.clone());
            let retry_config_clone = retry_config.clone();
            let queue_name = self.queue_name.clone();
            let connection = self.connection.clone();

            tokio::spawn(async move {
                let _permit = permit;

                // Try to deserialize as MessageEnvelope
                match serde_json::from_slice::<MessageEnvelope<T>>(&delivery.data) {
                    Ok(mut envelope) => {
                        debug!(
                            "Processing envelope {} (attempt {}/{})",
                            envelope.metadata.message_id,
                            envelope.metadata.retry_attempt + 1,
                            envelope.metadata.max_retries + 1
                        );

                        // Process message
                        match handler_clone(envelope.clone()).await {
                            Ok(()) => {
                                if auto_ack {
                                    if let Err(e) = channel_clone
                                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                        .await
                                    {
                                        error!("Failed to ack message: {}", e);
                                    }
                                }
                                debug!("Envelope {} processed successfully", envelope.metadata.message_id);
                            }
                            Err(e) => {
                                error!("Handler error for envelope {}: {}", envelope.metadata.message_id, e);
                                
                                // Determine error type (simplified classification)
                                let error_type = classify_error(e.as_ref());
                                
                                // Add error to envelope
                                envelope = envelope.with_error(
                                    &e.to_string(),
                                    error_type,
                                    Some(&format!("Queue: {}", queue_name))
                                );

                                if auto_ack {
                                    // Check if we should retry
                                    if let Some(retry_cfg) = &retry_config_clone {
                                        if !envelope.is_retry_exhausted() {
                                            // Calculate delay and schedule retry
                                            if let Some(delay) = retry_cfg.calculate_delay(envelope.metadata.retry_attempt) {
                                                warn!(
                                                    "Scheduling retry {} for envelope {} with delay {:?}",
                                                    envelope.metadata.retry_attempt + 1,
                                                    envelope.metadata.message_id,
                                                    delay
                                                );
                                                
                                                // Increment retry attempt in envelope
                                                envelope.metadata.retry_attempt += 1;
                                                
                                                // Serialize updated envelope
                                                match serde_json::to_vec(&envelope) {
                                                    Ok(retry_payload) => {
                                                        // Create consumer instance for access to methods
                                                        let consumer_self = Consumer {
                                                            connection: connection.clone(),
                                                            queue_name: queue_name.clone(),
                                                            exchange_name: None,
                                                            routing_key: None,
                                                            retry_config: retry_config_clone.clone(),
                                                            prefetch_count: 10,
                                                            auto_ack: true,
                                                        };
                                                        
                                                        // Send to retry queue with delay
                                                        if let Err(e) = consumer_self.send_to_retry_queue(
                                                            &channel_clone,
                                                            &retry_payload,
                                                            envelope.metadata.retry_attempt,
                                                            delay,
                                                        ).await {
                                                            error!("Failed to send envelope to retry queue: {}", e);
                                                            // Fallback to simple nack
                                                            if let Err(e) = channel_clone
                                                                .basic_nack(
                                                                    delivery.delivery_tag,
                                                                    lapin::options::BasicNackOptions {
                                                                        multiple: false,
                                                                        requeue: false,
                                                                    },
                                                                )
                                                                .await
                                                            {
                                                                error!("Failed to nack message: {}", e);
                                                            }
                                                            return;
                                                        }
                                                        
                                                        // ACK original message (it's now in retry queue)
                                                        if let Err(e) = channel_clone
                                                            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                                            .await
                                                        {
                                                            error!("Failed to ack message after retry: {}", e);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to serialize envelope for retry: {}", e);
                                                        // Fallback to simple nack
                                                        if let Err(e) = channel_clone
                                                            .basic_nack(
                                                                delivery.delivery_tag,
                                                                lapin::options::BasicNackOptions {
                                                                    multiple: false,
                                                                    requeue: false,
                                                                },
                                                            )
                                                            .await
                                                        {
                                                            error!("Failed to nack message: {}", e);
                                                        }
                                                    }
                                                }
                                            } else {
                                                // No more retries, send to DLQ
                                                Self::send_to_dlq(&envelope, retry_cfg, &connection, &queue_name).await;
                                                
                                                // ACK original message
                                                if let Err(e) = channel_clone
                                                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                                    .await
                                                {
                                                    error!("Failed to ack message after DLQ: {}", e);
                                                }
                                            }
                                        } else {
                                            // Retry exhausted, send to DLQ
                                            warn!("Retry exhausted for envelope {}", envelope.metadata.message_id);
                                            Self::send_to_dlq(&envelope, retry_cfg, &connection, &queue_name).await;
                                            
                                            // ACK original message
                                            if let Err(e) = channel_clone
                                                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                                .await
                                            {
                                                error!("Failed to ack message after DLQ: {}", e);
                                            }
                                        }
                                    } else {
                                        // No retry config, just nack
                                        if let Err(e) = channel_clone
                                            .basic_nack(
                                                delivery.delivery_tag,
                                                lapin::options::BasicNackOptions {
                                                    multiple: false,
                                                    requeue: false,
                                                },
                                            )
                                            .await
                                        {
                                            error!("Failed to nack message: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize message envelope: {}", e);
                        if auto_ack {
                            // Reject malformed messages
                            if let Err(e) = channel_clone
                                .basic_nack(
                                    delivery.delivery_tag,
                                    lapin::options::BasicNackOptions {
                                        multiple: false,
                                        requeue: false,
                                    },
                                )
                                .await
                            {
                                error!("Failed to nack malformed envelope: {}", e);
                            }
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Send failed message to Dead Letter Queue
    async fn send_to_dlq<T>(
        envelope: &MessageEnvelope<T>, 
        retry_config: &RetryConfig,
        connection: &Arc<Connection>,
        queue_name: &str,
    ) where
        T: serde::Serialize,
    {
        match connection.create_channel().await {
            Ok(dlq_channel) => {
                let dlq_name = retry_config.get_dead_letter_queue(queue_name);
                
                // Declare DLQ
                if let Err(e) = dlq_channel
                    .queue_declare(
                        &dlq_name,
                        QueueDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                {
                    error!("Failed to declare DLQ {}: {}", dlq_name, e);
                    return;
                }

                // Publish to DLQ with failure summary
                let failure_summary = envelope.get_failure_summary();
                let dlq_payload = serde_json::json!({
                    "envelope": envelope,
                    "failure_summary": failure_summary,
                    "sent_to_dlq_at": chrono::Utc::now(),
                });

                if let Ok(payload_bytes) = serde_json::to_vec(&dlq_payload) {
                    if let Err(e) = dlq_channel
                        .basic_publish(
                            "",
                            &dlq_name,
                            lapin::options::BasicPublishOptions::default(),
                            &payload_bytes,
                            lapin::BasicProperties::default(),
                        )
                        .await
                    {
                        error!("Failed to publish to DLQ {}: {}", dlq_name, e);
                    } else {
                        warn!("Sent envelope {} to DLQ: {}", envelope.metadata.message_id, failure_summary);
                    }
                }
            }
            Err(e) => {
                error!("Failed to create DLQ channel: {}", e);
            }
        }
    }
}

/// Classify error type based on error message (simplified heuristics)
fn classify_error(error: &(dyn std::error::Error + Send + Sync)) -> ErrorType {
    let error_msg = error.to_string().to_lowercase();
    
    if error_msg.contains("timeout") 
        || error_msg.contains("connection") 
        || error_msg.contains("network") 
        || error_msg.contains("temporary") {
        ErrorType::Transient
    } else if error_msg.contains("rate limit") 
        || error_msg.contains("quota") 
        || error_msg.contains("resource") {
        ErrorType::Resource
    } else if error_msg.contains("validation") 
        || error_msg.contains("authentication") 
        || error_msg.contains("authorization") 
        || error_msg.contains("invalid") 
        || error_msg.contains("bad request") {
        ErrorType::Permanent
    } else {
        ErrorType::Unknown
    }
}

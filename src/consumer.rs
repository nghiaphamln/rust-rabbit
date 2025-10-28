use crate::{connection::Connection, error::RustRabbitError, retry::RetryConfig};
use futures_lite::stream::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Channel,
};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error};

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
    #[allow(dead_code)]
    retry_config: Option<RetryConfig>,
    prefetch_count: u16,
    auto_ack: bool,
}

impl Consumer {
    /// Create a new consumer builder
    pub fn builder(connection: Arc<Connection>, queue_name: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::new(connection, queue_name)
    }

    /// Start consuming messages
    pub async fn consume<T, H, Fut>(&self, handler: H) -> Result<(), RustRabbitError>
    where
        T: DeserializeOwned + Send + Clone + Sync + 'static,
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

        // Setup queue and exchange
        self.setup_infrastructure(&channel).await?;

        // Create consumer
        let mut consumer = channel
            .basic_consume(
                &self.queue_name,
                "rust-rabbit-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let semaphore = Arc::new(Semaphore::new(self.prefetch_count as usize));

        debug!("Started consuming from queue: {}", self.queue_name);

        // Process messages (simplified - no retry for now)
        while let Some(delivery_result) = consumer.next().await {
            let delivery = delivery_result?;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let handler_clone = handler.clone();
            let auto_ack = self.auto_ack;
            let channel_clone = Arc::new(channel.clone());

            tokio::spawn(async move {
                let _permit = permit;

                // Deserialize message
                match serde_json::from_slice::<T>(&delivery.data) {
                    Ok(data) => {
                        let message = Message {
                            data,
                            retry_attempt: 0, // Simplified for now
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
                                    // Simple reject without retry for now
                                    if let Err(e) = message.nack(false).await {
                                        error!("Failed to nack message: {}", e);
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
}

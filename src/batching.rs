//! Message batching implementation for high-throughput scenarios
//!
//! This module provides efficient message batching to improve performance by:
//! - Grouping multiple messages into single publish operations
//! - Reducing RabbitMQ connection overhead
//! - Configurable batch sizes and timeouts
//! - Memory-efficient batch accumulation

use crate::{
    error::{RabbitError, Result},
    metrics::{MetricsTimer, RustRabbitMetrics},
    publisher::{PublishOptions, Publisher},
};
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{interval, Instant as TokioInstant};
use tracing::{debug, error, info};

/// Configuration for message batching
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of messages per batch
    pub max_batch_size: usize,
    /// Maximum time to wait before sending a partial batch
    pub max_batch_timeout: Duration,
    /// Buffer size for the internal message queue
    pub buffer_size: usize,
    /// Whether to flush immediately when buffer is full
    pub flush_on_full: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_batch_timeout: Duration::from_millis(100),
            buffer_size: 1000,
            flush_on_full: true,
        }
    }
}

/// A single message in a batch
#[derive(Debug)]
struct BatchMessage {
    queue_name: String,
    payload: Vec<u8>,
    options: Option<PublishOptions>,
    timestamp: Instant,
}

/// Message batcher for high-throughput publishing
#[derive(Debug)]
pub struct MessageBatcher {
    config: BatchConfig,
    #[allow(dead_code)]
    publisher: Publisher,
    sender: mpsc::Sender<BatchMessage>,
    metrics: Option<RustRabbitMetrics>,
}

impl MessageBatcher {
    /// Create a new message batcher
    pub async fn new(publisher: Publisher, config: BatchConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.buffer_size);

        let batcher = Self {
            config: config.clone(),
            publisher: publisher.clone(),
            sender,
            metrics: None,
        };

        // Start the batch processing task
        let batch_processor = BatchProcessor::new(publisher, receiver, config, None);

        tokio::spawn(async move {
            if let Err(e) = batch_processor.run().await {
                error!("Batch processor error: {}", e);
            }
        });

        Ok(batcher)
    }

    /// Create a new message batcher with metrics
    pub async fn with_metrics(
        publisher: Publisher,
        config: BatchConfig,
        metrics: RustRabbitMetrics,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.buffer_size);

        let batcher = Self {
            config: config.clone(),
            publisher: publisher.clone(),
            sender,
            metrics: Some(metrics.clone()),
        };

        // Start the batch processing task
        let batch_processor = BatchProcessor::new(publisher, receiver, config, Some(metrics));

        tokio::spawn(async move {
            if let Err(e) = batch_processor.run().await {
                error!("Batch processor error: {}", e);
            }
        });

        Ok(batcher)
    }

    /// Add a message to the batch queue
    pub async fn queue_message<T>(
        &self,
        queue_name: &str,
        message: &T,
        options: Option<PublishOptions>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let payload = serde_json::to_vec(message)
            .map_err(|e| RabbitError::SerializationError(e.to_string()))?;

        let batch_message = BatchMessage {
            queue_name: queue_name.to_string(),
            payload,
            options,
            timestamp: Instant::now(),
        };

        self.sender
            .send(batch_message)
            .await
            .map_err(|_| RabbitError::ChannelError("Batch queue is closed".to_string()))?;

        // Record queued message metric
        if let Some(metrics) = &self.metrics {
            metrics.record_message_published(queue_name, "", "batch_queued");
        }

        Ok(())
    }

    /// Get current queue length (approximate)
    pub fn queue_len(&self) -> usize {
        // Note: This is approximate since we can't get exact count from mpsc
        self.config
            .buffer_size
            .saturating_sub(self.sender.capacity())
    }

    /// Check if the batch queue is nearly full
    pub fn is_nearly_full(&self) -> bool {
        let remaining_capacity = self.sender.capacity();
        let usage_percentage =
            (self.config.buffer_size - remaining_capacity) * 100 / self.config.buffer_size;
        usage_percentage > 80
    }
}

/// Internal batch processor
struct BatchProcessor {
    publisher: Publisher,
    receiver: mpsc::Receiver<BatchMessage>,
    config: BatchConfig,
    metrics: Option<RustRabbitMetrics>,
    current_batch: Vec<BatchMessage>,
    last_flush: TokioInstant,
}

impl BatchProcessor {
    fn new(
        publisher: Publisher,
        receiver: mpsc::Receiver<BatchMessage>,
        config: BatchConfig,
        metrics: Option<RustRabbitMetrics>,
    ) -> Self {
        Self {
            publisher,
            receiver,
            config: config.clone(),
            metrics,
            current_batch: Vec::with_capacity(config.max_batch_size),
            last_flush: TokioInstant::now(),
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut flush_interval = interval(self.config.max_batch_timeout);

        info!("Batch processor started with config: {:?}", self.config);

        loop {
            tokio::select! {
                // Receive new messages
                message = self.receiver.recv() => {
                    match message {
                        Some(msg) => {
                            self.add_to_batch(msg).await?;
                        }
                        None => {
                            // Channel closed, flush remaining messages and exit
                            info!("Batch processor channel closed, flushing remaining messages");
                            self.flush_batch().await?;
                            break;
                        }
                    }
                }

                // Periodic flush check
                _ = flush_interval.tick() => {
                    if self.should_flush() {
                        self.flush_batch().await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn add_to_batch(&mut self, message: BatchMessage) -> Result<()> {
        self.current_batch.push(message);

        // Check if we should flush
        if self.should_flush() {
            self.flush_batch().await?;
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        if self.current_batch.is_empty() {
            return false;
        }

        // Flush if batch is full
        if self.current_batch.len() >= self.config.max_batch_size {
            return true;
        }

        // Flush if timeout exceeded
        let oldest_message_time = self
            .current_batch
            .first()
            .map(|msg| msg.timestamp)
            .unwrap_or_else(Instant::now);

        let elapsed = oldest_message_time.elapsed();
        elapsed >= self.config.max_batch_timeout
    }

    async fn flush_batch(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        let batch_size = self.current_batch.len();
        let timer = MetricsTimer::new();

        debug!("Flushing batch of {} messages", batch_size);

        // Group messages by queue for efficient publishing
        let mut queue_batches: std::collections::HashMap<String, Vec<&BatchMessage>> =
            std::collections::HashMap::new();

        for message in &self.current_batch {
            queue_batches
                .entry(message.queue_name.clone())
                .or_default()
                .push(message);
        }

        // Publish each queue's batch
        let mut total_published = 0;
        let mut total_errors = 0;

        for (queue_name, messages) in &queue_batches {
            match self.publish_queue_batch(queue_name, messages.clone()).await {
                Ok(count) => total_published += count,
                Err(e) => {
                    error!("Failed to publish batch for queue {}: {}", queue_name, e);
                    total_errors += messages.len();
                }
            }
        }

        // Record metrics
        if let Some(metrics) = &self.metrics {
            let duration = timer.elapsed();

            // Record batch statistics
            for (queue_name, messages) in &queue_batches {
                for _ in messages {
                    metrics.record_message_published(queue_name, "", "batch_sent");
                }
            }

            // Record timing
            metrics.record_publish_duration("", "batch", duration);
        }

        info!(
            "Batch flush completed: {} published, {} errors, took {:?}",
            total_published,
            total_errors,
            timer.elapsed()
        );

        // Clear the batch
        self.current_batch.clear();
        self.last_flush = TokioInstant::now();

        Ok(())
    }

    async fn publish_queue_batch(
        &self,
        queue_name: &str,
        messages: Vec<&BatchMessage>,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        // For now, publish messages individually within the batch
        // Future optimization: implement true bulk publishing
        let mut published_count = 0;

        for message in messages {
            // Deserialize the message back to publish it
            // This is a simplified approach - in a real implementation,
            // we might want to optimize this further
            let payload_str = String::from_utf8(message.payload.clone())
                .map_err(|e| RabbitError::SerializationError(e.to_string()))?;

            let json_value: serde_json::Value = serde_json::from_str(&payload_str)
                .map_err(|e| RabbitError::SerializationError(e.to_string()))?;

            match self
                .publisher
                .publish_to_queue(queue_name, &json_value, message.options.clone())
                .await
            {
                Ok(_) => published_count += 1,
                Err(e) => {
                    error!("Failed to publish message in batch: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(published_count)
    }
}

/// Builder for BatchConfig
#[derive(Debug)]
pub struct BatchConfigBuilder {
    config: BatchConfig,
}

impl BatchConfigBuilder {
    /// Create a new BatchConfigBuilder
    pub fn new() -> Self {
        Self {
            config: BatchConfig::default(),
        }
    }

    /// Set the maximum batch size
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.config.max_batch_size = size;
        self
    }

    /// Set the maximum batch timeout
    pub fn max_batch_timeout(mut self, timeout: Duration) -> Self {
        self.config.max_batch_timeout = timeout;
        self
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set whether to flush when buffer is full
    pub fn flush_on_full(mut self, flush: bool) -> Self {
        self.config.flush_on_full = flush;
        self
    }

    /// Build the BatchConfig
    pub fn build(self) -> BatchConfig {
        self.config
    }
}

impl Default for BatchConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_builder() {
        let config = BatchConfigBuilder::new()
            .max_batch_size(50)
            .max_batch_timeout(Duration::from_millis(200))
            .buffer_size(500)
            .flush_on_full(false)
            .build();

        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.max_batch_timeout, Duration::from_millis(200));
        assert_eq!(config.buffer_size, 500);
        assert!(!config.flush_on_full);
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();

        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_batch_timeout, Duration::from_millis(100));
        assert_eq!(config.buffer_size, 1000);
        assert!(config.flush_on_full);
    }
}

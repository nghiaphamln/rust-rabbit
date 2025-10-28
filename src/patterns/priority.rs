use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::RustRabbitError;

/// Message priority levels
#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Priority {
    /// Lowest priority - processed last
    Low = 1,
    /// Normal priority - default
    #[default]
    Normal = 5,
    /// High priority - processed before normal
    High = 8,
    /// Critical priority - processed first
    Critical = 10,
}

impl Priority {
    pub fn value(&self) -> u8 {
        *self as u8
    }

    pub fn from_value(value: u8) -> Self {
        match value {
            0 => Priority::Low,
            1..=2 => Priority::Low,
            3..=6 => Priority::Normal,
            7..=9 => Priority::High,
            10.. => Priority::Critical,
        }
    }
}

/// Priority message with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityMessage {
    pub message_id: String,
    pub priority: Priority,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub expiry: Option<DateTime<Utc>>,
    pub retry_count: u32,
    pub max_retries: u32,
}

impl PriorityMessage {
    pub fn new(payload: Vec<u8>, priority: Priority) -> Self {
        Self {
            message_id: Uuid::new_v4().to_string(),
            priority,
            payload,
            headers: HashMap::new(),
            timestamp: Utc::now(),
            expiry: None,
            retry_count: 0,
            max_retries: 3,
        }
    }

    pub fn with_expiry(mut self, expiry: DateTime<Utc>) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            Utc::now() > expiry
        } else {
            false
        }
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

/// Priority message wrapper for heap ordering
#[derive(Debug, Clone)]
struct PriorityMessageWrapper {
    message: PriorityMessage,
    enqueue_time: Instant,
}

impl PartialEq for PriorityMessageWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.message.priority == other.message.priority && self.enqueue_time == other.enqueue_time
    }
}

impl Eq for PriorityMessageWrapper {}

impl PartialOrd for PriorityMessageWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityMessageWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then FIFO for same priority
        match self.message.priority.cmp(&other.message.priority) {
            Ordering::Equal => other.enqueue_time.cmp(&self.enqueue_time),
            other => other,
        }
    }
}

/// Priority queue configuration
#[derive(Debug, Clone)]
pub struct PriorityQueueConfig {
    pub max_queue_size: usize,
    pub dead_letter_enabled: bool,
    pub dead_letter_threshold: u32,
    pub cleanup_interval: Duration,
    pub metrics_enabled: bool,
}

impl Default for PriorityQueueConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10_000,
            dead_letter_enabled: true,
            dead_letter_threshold: 3,
            cleanup_interval: Duration::from_secs(60),
            metrics_enabled: true,
        }
    }
}

/// Priority queue statistics
#[derive(Debug, Clone)]
pub struct PriorityQueueStats {
    pub total_messages: usize,
    pub messages_by_priority: HashMap<Priority, usize>,
    pub dead_letter_count: usize,
    pub expired_count: usize,
    pub average_wait_time: Duration,
    pub throughput_per_second: f64,
}

/// Priority queue implementation
#[derive(Debug)]
pub struct PriorityQueue {
    config: PriorityQueueConfig,
    queue: Arc<Mutex<BinaryHeap<PriorityMessageWrapper>>>,
    dead_letter_queue: Arc<Mutex<VecDeque<PriorityMessage>>>,
    stats: Arc<Mutex<PriorityQueueStats>>,
    notify: Arc<Notify>,
}

impl PriorityQueue {
    pub fn new(config: PriorityQueueConfig) -> Self {
        let queue = Self {
            config: config.clone(),
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
            dead_letter_queue: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(PriorityQueueStats {
                total_messages: 0,
                messages_by_priority: HashMap::new(),
                dead_letter_count: 0,
                expired_count: 0,
                average_wait_time: Duration::ZERO,
                throughput_per_second: 0.0,
            })),
            notify: Arc::new(Notify::new()),
        };

        // Start cleanup task
        if config.cleanup_interval > Duration::ZERO {
            queue.start_cleanup_task();
        }

        queue
    }

    /// Enqueue a message with priority
    pub fn enqueue(&self, message: PriorityMessage) -> Result<()> {
        let priority = message.priority;

        debug!(
            message_id = %message.message_id,
            priority = ?priority,
            "Enqueuing priority message"
        );

        {
            let mut queue = self.queue.lock().unwrap();

            // Check queue size limit
            if queue.len() >= self.config.max_queue_size {
                warn!(
                    queue_size = queue.len(),
                    max_size = self.config.max_queue_size,
                    "Priority queue is full"
                );
                return Err(RustRabbitError::QueueFull.into());
            }

            let wrapper = PriorityMessageWrapper {
                message,
                enqueue_time: Instant::now(),
            };

            queue.push(wrapper);
        }

        // Update statistics
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_messages += 1;
            *stats.messages_by_priority.entry(priority).or_insert(0) += 1;
        }

        // Notify waiting consumers
        self.notify.notify_one();

        Ok(())
    }

    /// Dequeue the highest priority message
    pub fn dequeue(&self) -> Option<PriorityMessage> {
        let mut queue = self.queue.lock().unwrap();

        while let Some(wrapper) = queue.pop() {
            let message = wrapper.message;

            // Check if message has expired
            if message.is_expired() {
                warn!(
                    message_id = %message.message_id,
                    "Message expired, moving to dead letter queue"
                );

                self.move_to_dead_letter(message);
                continue;
            }

            debug!(
                message_id = %message.message_id,
                priority = ?message.priority,
                wait_time_ms = wrapper.enqueue_time.elapsed().as_millis(),
                "Dequeued priority message"
            );

            // Update statistics
            {
                let mut stats = self.stats.lock().unwrap();
                stats.total_messages = stats.total_messages.saturating_sub(1);
                if let Some(count) = stats.messages_by_priority.get_mut(&message.priority) {
                    *count = count.saturating_sub(1);
                }
            }

            return Some(message);
        }

        None
    }

    /// Dequeue with async waiting
    pub async fn dequeue_async(&self) -> Option<PriorityMessage> {
        loop {
            if let Some(message) = self.dequeue() {
                return Some(message);
            }

            // Wait for notification that new message was enqueued
            self.notify.notified().await;
        }
    }

    /// Dequeue with timeout
    pub async fn dequeue_timeout(&self, timeout: Duration) -> Option<PriorityMessage> {
        tokio::select! {
            message = self.dequeue_async() => message,
            _ = tokio::time::sleep(timeout) => None,
        }
    }

    /// Peek at the highest priority message without removing it
    pub fn peek(&self) -> Option<PriorityMessage> {
        let queue = self.queue.lock().unwrap();
        queue.peek().map(|wrapper| wrapper.message.clone())
    }

    /// Get current queue size
    pub fn size(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }

    /// Get queue statistics
    pub fn stats(&self) -> PriorityQueueStats {
        self.stats.lock().unwrap().clone()
    }

    /// Requeue a failed message (with retry logic)
    pub fn requeue(&self, mut message: PriorityMessage) -> Result<()> {
        if message.can_retry() {
            message.increment_retry();

            info!(
                message_id = %message.message_id,
                retry_count = message.retry_count,
                max_retries = message.max_retries,
                "Requeuing message for retry"
            );

            self.enqueue(message)
        } else {
            warn!(
                message_id = %message.message_id,
                retry_count = message.retry_count,
                "Message exceeded max retries, moving to dead letter queue"
            );

            self.move_to_dead_letter(message);
            Ok(())
        }
    }

    /// Move message to dead letter queue
    fn move_to_dead_letter(&self, message: PriorityMessage) {
        if self.config.dead_letter_enabled {
            let mut dead_letter = self.dead_letter_queue.lock().unwrap();
            dead_letter.push_back(message);

            // Update statistics
            let mut stats = self.stats.lock().unwrap();
            stats.dead_letter_count += 1;
        }
    }

    /// Get dead letter queue contents
    pub fn dead_letter_messages(&self) -> Vec<PriorityMessage> {
        self.dead_letter_queue
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Clear dead letter queue
    pub fn clear_dead_letter(&self) -> usize {
        let mut dead_letter = self.dead_letter_queue.lock().unwrap();
        let count = dead_letter.len();
        dead_letter.clear();

        // Update statistics
        {
            let mut stats = self.stats.lock().unwrap();
            stats.dead_letter_count = 0;
        }

        count
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let queue = self.queue.clone();
        let dead_letter = self.dead_letter_queue.clone();
        let stats = self.stats.clone();
        let cleanup_interval = self.config.cleanup_interval;
        let dead_letter_enabled = self.config.dead_letter_enabled;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                let mut expired_count = 0;

                // Clean expired messages from main queue
                {
                    let mut queue_guard = queue.lock().unwrap();
                    let mut temp_queue = BinaryHeap::new();

                    while let Some(wrapper) = queue_guard.pop() {
                        if wrapper.message.is_expired() {
                            expired_count += 1;

                            if dead_letter_enabled {
                                let mut dead_letter_guard = dead_letter.lock().unwrap();
                                dead_letter_guard.push_back(wrapper.message);
                            }
                        } else {
                            temp_queue.push(wrapper);
                        }
                    }

                    *queue_guard = temp_queue;
                }

                // Update statistics
                if expired_count > 0 {
                    let mut stats_guard = stats.lock().unwrap();
                    stats_guard.expired_count += expired_count;
                    stats_guard.total_messages =
                        stats_guard.total_messages.saturating_sub(expired_count);

                    debug!(
                        expired_count = expired_count,
                        "Cleanup task removed expired messages"
                    );
                }
            }
        });
    }
}

/// Priority-based message router
#[derive(Debug)]
pub struct PriorityRouter {
    queues: HashMap<String, Arc<PriorityQueue>>,
    default_queue: String,
}

impl PriorityRouter {
    pub fn new(default_queue: String) -> Self {
        Self {
            queues: HashMap::new(),
            default_queue,
        }
    }

    /// Add a priority queue for a specific topic/route
    pub fn add_queue(&mut self, name: String, queue: Arc<PriorityQueue>) {
        self.queues.insert(name, queue);
    }

    /// Route message to appropriate priority queue
    pub fn route_message(
        &self,
        queue_name: Option<String>,
        message: PriorityMessage,
    ) -> Result<()> {
        let queue_name = queue_name.unwrap_or_else(|| self.default_queue.clone());

        if let Some(queue) = self.queues.get(&queue_name) {
            queue.enqueue(message)
        } else {
            error!(queue_name = %queue_name, "Priority queue not found");
            Err(RustRabbitError::QueueNotFound(queue_name).into())
        }
    }

    /// Get message from highest priority across all queues
    pub async fn dequeue_any(&self) -> Option<(String, PriorityMessage)> {
        // This is a simple round-robin approach
        // In production, you might want a more sophisticated algorithm
        for (queue_name, queue) in &self.queues {
            if let Some(message) = queue.dequeue() {
                return Some((queue_name.clone(), message));
            }
        }
        None
    }

    /// Get queue by name
    pub fn get_queue(&self, name: &str) -> Option<Arc<PriorityQueue>> {
        self.queues.get(name).cloned()
    }

    /// Get all queue names
    pub fn queue_names(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }

    /// Get aggregate statistics across all queues
    pub fn aggregate_stats(&self) -> HashMap<String, PriorityQueueStats> {
        self.queues
            .iter()
            .map(|(name, queue)| (name.clone(), queue.stats()))
            .collect()
    }
}

/// Priority-aware consumer
#[derive(Debug)]
pub struct PriorityConsumer {
    queue: Arc<PriorityQueue>,
    batch_size: usize,
    processing_timeout: Duration,
}

impl PriorityConsumer {
    pub fn new(queue: Arc<PriorityQueue>) -> Self {
        Self {
            queue,
            batch_size: 1,
            processing_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.processing_timeout = timeout;
        self
    }

    /// Consume messages in priority order
    pub async fn consume_batch(&self) -> Vec<PriorityMessage> {
        let mut batch = Vec::new();

        for _ in 0..self.batch_size {
            if let Some(message) = self.queue.dequeue_timeout(Duration::from_millis(100)).await {
                batch.push(message);
            } else {
                break; // No more messages available
            }
        }

        debug!(batch_size = batch.len(), "Consumed priority message batch");
        batch
    }

    /// Consume single message with timeout
    pub async fn consume_one(&self) -> Option<PriorityMessage> {
        self.queue.dequeue_timeout(self.processing_timeout).await
    }

    /// Start consuming messages with a handler
    pub async fn start_consuming<F, Fut>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(PriorityMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        info!("Starting priority consumer");

        loop {
            if let Some(message) = self.queue.dequeue_async().await {
                let message_id = message.message_id.clone();

                debug!(
                    message_id = %message_id,
                    priority = ?message.priority,
                    "Processing priority message"
                );

                match handler(message.clone()).await {
                    Ok(()) => {
                        debug!(message_id = %message_id, "Message processed successfully");
                    }
                    Err(err) => {
                        error!(
                            message_id = %message_id,
                            error = %err,
                            "Message processing failed"
                        );

                        // Requeue for retry
                        if let Err(requeue_err) = self.queue.requeue(message) {
                            error!(
                                message_id = %message_id,
                                error = %requeue_err,
                                "Failed to requeue message"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn test_priority_from_value() {
        assert_eq!(Priority::from_value(1), Priority::Low);
        assert_eq!(Priority::from_value(5), Priority::Normal);
        assert_eq!(Priority::from_value(8), Priority::High);
        assert_eq!(Priority::from_value(10), Priority::Critical);
    }

    #[tokio::test]
    async fn test_priority_queue_ordering() {
        let config = PriorityQueueConfig::default();
        let queue = PriorityQueue::new(config);

        // Enqueue messages in random order
        queue
            .enqueue(PriorityMessage::new(b"low".to_vec(), Priority::Low))
            .unwrap();
        queue
            .enqueue(PriorityMessage::new(
                b"critical".to_vec(),
                Priority::Critical,
            ))
            .unwrap();
        queue
            .enqueue(PriorityMessage::new(b"normal".to_vec(), Priority::Normal))
            .unwrap();
        queue
            .enqueue(PriorityMessage::new(b"high".to_vec(), Priority::High))
            .unwrap();

        // Should dequeue in priority order
        let msg1 = queue.dequeue().unwrap();
        assert_eq!(msg1.priority, Priority::Critical);

        let msg2 = queue.dequeue().unwrap();
        assert_eq!(msg2.priority, Priority::High);

        let msg3 = queue.dequeue().unwrap();
        assert_eq!(msg3.priority, Priority::Normal);

        let msg4 = queue.dequeue().unwrap();
        assert_eq!(msg4.priority, Priority::Low);
    }

    #[tokio::test]
    async fn test_fifo_within_same_priority() {
        let config = PriorityQueueConfig::default();
        let queue = PriorityQueue::new(config);

        // Enqueue multiple messages with same priority
        queue
            .enqueue(PriorityMessage::new(b"first".to_vec(), Priority::Normal))
            .unwrap();
        sleep(Duration::from_millis(1)).await; // Ensure different timestamps
        queue
            .enqueue(PriorityMessage::new(b"second".to_vec(), Priority::Normal))
            .unwrap();
        sleep(Duration::from_millis(1)).await;
        queue
            .enqueue(PriorityMessage::new(b"third".to_vec(), Priority::Normal))
            .unwrap();

        // Should dequeue in FIFO order for same priority
        let msg1 = queue.dequeue().unwrap();
        assert_eq!(msg1.payload, b"first");

        let msg2 = queue.dequeue().unwrap();
        assert_eq!(msg2.payload, b"second");

        let msg3 = queue.dequeue().unwrap();
        assert_eq!(msg3.payload, b"third");
    }

    #[tokio::test]
    async fn test_message_expiry() {
        let config = PriorityQueueConfig::default();
        let queue = PriorityQueue::new(config);

        let expired_message = PriorityMessage::new(b"expired".to_vec(), Priority::Normal)
            .with_expiry(Utc::now() - chrono::Duration::seconds(1));

        queue.enqueue(expired_message).unwrap();

        // Should not return expired message
        let result = queue.dequeue();
        assert!(result.is_none());

        // Should have moved to dead letter queue
        let dead_letters = queue.dead_letter_messages();
        assert_eq!(dead_letters.len(), 1);
    }

    #[tokio::test]
    async fn test_retry_logic() {
        let config = PriorityQueueConfig::default();
        let queue = PriorityQueue::new(config);

        let message = PriorityMessage::new(b"retry".to_vec(), Priority::Normal).with_max_retries(2);

        // First requeue should succeed
        queue.requeue(message.clone()).unwrap();
        assert_eq!(queue.size(), 1);

        let mut requeued = queue.dequeue().unwrap();
        assert_eq!(requeued.retry_count, 1);

        // Second requeue should succeed
        queue.requeue(requeued.clone()).unwrap();
        assert_eq!(queue.size(), 1);

        requeued = queue.dequeue().unwrap();
        assert_eq!(requeued.retry_count, 2);

        // Third requeue should move to dead letter (exceeded max retries)
        queue.requeue(requeued).unwrap();
        assert_eq!(queue.size(), 0);

        let dead_letters = queue.dead_letter_messages();
        assert_eq!(dead_letters.len(), 1);
    }

    #[tokio::test]
    async fn test_priority_router() {
        let mut router = PriorityRouter::new("default".to_string());

        let config = PriorityQueueConfig::default();
        let queue1 = Arc::new(PriorityQueue::new(config.clone()));
        let queue2 = Arc::new(PriorityQueue::new(config));

        router.add_queue("queue1".to_string(), queue1.clone());
        router.add_queue("queue2".to_string(), queue2.clone());

        let message1 = PriorityMessage::new(b"msg1".to_vec(), Priority::High);
        let message2 = PriorityMessage::new(b"msg2".to_vec(), Priority::Normal);

        // Route messages to different queues
        router
            .route_message(Some("queue1".to_string()), message1)
            .unwrap();
        router
            .route_message(Some("queue2".to_string()), message2)
            .unwrap();

        // Verify messages are in correct queues
        assert_eq!(queue1.size(), 1);
        assert_eq!(queue2.size(), 1);

        let msg_from_q1 = queue1.dequeue().unwrap();
        assert_eq!(msg_from_q1.payload, b"msg1");

        let msg_from_q2 = queue2.dequeue().unwrap();
        assert_eq!(msg_from_q2.payload, b"msg2");
    }

    #[tokio::test]
    async fn test_priority_consumer() {
        let config = PriorityQueueConfig::default();
        let queue = Arc::new(PriorityQueue::new(config));
        let consumer = PriorityConsumer::new(queue.clone()).with_batch_size(2);

        // Add some messages
        queue
            .enqueue(PriorityMessage::new(b"msg1".to_vec(), Priority::High))
            .unwrap();
        queue
            .enqueue(PriorityMessage::new(b"msg2".to_vec(), Priority::Normal))
            .unwrap();

        // Consume batch
        let batch = consumer.consume_batch().await;
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].priority, Priority::High); // Higher priority first
        assert_eq!(batch[1].priority, Priority::Normal);
    }

    #[tokio::test]
    async fn test_queue_full_behavior() {
        let config = PriorityQueueConfig {
            max_queue_size: 2,
            ..Default::default()
        };
        let queue = PriorityQueue::new(config);

        // Fill queue to capacity
        queue
            .enqueue(PriorityMessage::new(b"msg1".to_vec(), Priority::Normal))
            .unwrap();
        queue
            .enqueue(PriorityMessage::new(b"msg2".to_vec(), Priority::Normal))
            .unwrap();

        // Third message should fail
        let result = queue.enqueue(PriorityMessage::new(b"msg3".to_vec(), Priority::Normal));
        assert!(result.is_err());
    }
}

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use uuid::Uuid;

/// Unique message identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(String);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Content-based hash for duplicate detection
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(u64);

impl ContentHash {
    pub fn from_content(content: &[u8]) -> Self {
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Self(hasher.finish())
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Deduplication strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeduplicationStrategy {
    /// Use message ID for deduplication
    MessageId,
    /// Use content hash for deduplication
    ContentHash,
    /// Use both message ID and content hash
    IdAndContent,
    /// Use custom key provided in message
    CustomKey(String),
}

/// Message with deduplication metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduplicatedMessage {
    pub message_id: MessageId,
    pub content_hash: ContentHash,
    pub custom_key: Option<String>,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub ttl: Option<Duration>,
}

impl DeduplicatedMessage {
    pub fn new(payload: Vec<u8>) -> Self {
        let content_hash = ContentHash::from_content(&payload);
        Self {
            message_id: MessageId::new(),
            content_hash,
            custom_key: None,
            payload,
            headers: HashMap::new(),
            timestamp: Utc::now(),
            ttl: None,
        }
    }

    pub fn with_message_id(mut self, message_id: MessageId) -> Self {
        self.message_id = message_id;
        self
    }

    pub fn with_custom_key(mut self, key: String) -> Self {
        self.custom_key = Some(key);
        self
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Get deduplication key based on strategy
    pub fn get_dedup_key(&self, strategy: &DeduplicationStrategy) -> String {
        match strategy {
            DeduplicationStrategy::MessageId => self.message_id.as_str().to_string(),
            DeduplicationStrategy::ContentHash => self.content_hash.value().to_string(),
            DeduplicationStrategy::IdAndContent => {
                format!("{}:{}", self.message_id.as_str(), self.content_hash.value())
            }
            DeduplicationStrategy::CustomKey(_key) => self
                .custom_key
                .as_ref()
                .unwrap_or(&self.message_id.0)
                .clone(),
        }
    }

    /// Check if message has expired based on TTL
    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let elapsed = Utc::now().signed_duration_since(self.timestamp);
            elapsed.to_std().unwrap_or(Duration::ZERO) > ttl
        } else {
            false
        }
    }
}

/// Deduplication result
#[derive(Debug, Clone, PartialEq)]
pub enum DeduplicationResult {
    /// Message is unique, should be processed
    Unique,
    /// Message is a duplicate, should be ignored
    Duplicate(DuplicateInfo),
}

/// Information about detected duplicate
#[derive(Debug, Clone, PartialEq)]
pub struct DuplicateInfo {
    pub original_message_id: MessageId,
    pub original_timestamp: DateTime<Utc>,
    pub duplicate_count: u32,
}

/// Deduplication record stored in cache
#[derive(Debug, Clone)]
struct DeduplicationRecord {
    message_id: MessageId,
    timestamp: DateTime<Utc>,
    access_count: u32,
    last_accessed: Instant,
}

impl DeduplicationRecord {
    fn new(message_id: MessageId) -> Self {
        Self {
            message_id,
            timestamp: Utc::now(),
            access_count: 1,
            last_accessed: Instant::now(),
        }
    }

    fn increment_access(&mut self) {
        self.access_count += 1;
        self.last_accessed = Instant::now();
    }
}

/// Configuration for deduplication manager
#[derive(Debug, Clone)]
pub struct DeduplicationConfig {
    pub strategy: DeduplicationStrategy,
    pub default_ttl: Duration,
    pub cache_size_limit: usize,
    pub cleanup_interval: Duration,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            strategy: DeduplicationStrategy::MessageId,
            default_ttl: Duration::from_secs(24 * 60 * 60),
            cache_size_limit: 100_000,
            cleanup_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Message deduplication manager
#[derive(Debug)]
pub struct DeduplicationManager {
    config: DeduplicationConfig,
    dedup_cache: Arc<Mutex<HashMap<String, DeduplicationRecord>>>,
}

impl DeduplicationManager {
    pub fn new(config: DeduplicationConfig) -> Self {
        let manager = Self {
            config,
            dedup_cache: Arc::new(Mutex::new(HashMap::new())),
        };

        // Start cleanup task
        manager.start_cleanup_task();
        manager
    }

    /// Check if message is duplicate
    pub fn check_duplicate(&self, message: &DeduplicatedMessage) -> Result<DeduplicationResult> {
        let dedup_key = message.get_dedup_key(&self.config.strategy);

        debug!(
            message_id = %message.message_id,
            dedup_key = %dedup_key,
            "Checking for duplicate message"
        );

        let mut cache = self.dedup_cache.lock().unwrap();

        if let Some(record) = cache.get_mut(&dedup_key) {
            // Found duplicate
            record.increment_access();

            warn!(
                message_id = %message.message_id,
                original_message_id = %record.message_id,
                duplicate_count = record.access_count,
                "Duplicate message detected"
            );

            Ok(DeduplicationResult::Duplicate(DuplicateInfo {
                original_message_id: record.message_id.clone(),
                original_timestamp: record.timestamp,
                duplicate_count: record.access_count,
            }))
        } else {
            // New unique message
            let record = DeduplicationRecord::new(message.message_id.clone());
            cache.insert(dedup_key.clone(), record);

            debug!(
                message_id = %message.message_id,
                dedup_key = %dedup_key,
                "Message is unique"
            );

            Ok(DeduplicationResult::Unique)
        }
    }

    /// Manually mark a message as processed (for external dedup stores)
    pub fn mark_processed(&self, message: &DeduplicatedMessage) -> Result<()> {
        let dedup_key = message.get_dedup_key(&self.config.strategy);
        let mut cache = self.dedup_cache.lock().unwrap();

        cache
            .entry(dedup_key)
            .or_insert_with(|| DeduplicationRecord::new(message.message_id.clone()));

        Ok(())
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> DeduplicationStats {
        let cache = self.dedup_cache.lock().unwrap();

        let total_entries = cache.len();
        let total_access_count: u32 = cache.values().map(|record| record.access_count).sum();

        DeduplicationStats {
            total_entries,
            total_access_count,
            cache_hit_rate: if total_access_count > 0 {
                ((total_access_count - total_entries as u32) as f64 / total_access_count as f64)
                    * 100.0
            } else {
                0.0
            },
        }
    }

    /// Clear expired entries from cache
    pub fn cleanup_expired(&self) -> usize {
        let mut cache = self.dedup_cache.lock().unwrap();
        let mut expired_keys = Vec::new();
        let now = Instant::now();

        for (key, record) in cache.iter() {
            // Remove entries older than TTL
            let age = now.duration_since(record.last_accessed);
            if age > self.config.default_ttl {
                expired_keys.push(key.clone());
            }
        }

        let expired_count = expired_keys.len();
        for key in expired_keys {
            cache.remove(&key);
        }

        if expired_count > 0 {
            debug!(
                expired_count = expired_count,
                "Cleaned up expired deduplication entries"
            );
        }

        expired_count
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let cache = self.dedup_cache.clone();
        let cleanup_interval = self.config.cleanup_interval;
        let default_ttl = self.config.default_ttl;
        let cache_size_limit = self.config.cache_size_limit;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                // Cleanup expired entries
                let mut expired_keys = Vec::new();
                let now = Instant::now();

                {
                    let cache = cache.lock().unwrap();
                    for (key, record) in cache.iter() {
                        let age = now.duration_since(record.last_accessed);
                        if age > default_ttl {
                            expired_keys.push(key.clone());
                        }
                    }
                }

                if !expired_keys.is_empty() {
                    let mut cache = cache.lock().unwrap();
                    for key in &expired_keys {
                        cache.remove(key);
                    }

                    debug!(
                        expired_count = expired_keys.len(),
                        "Background cleanup removed expired entries"
                    );
                }

                // Enforce cache size limit (LRU eviction)
                {
                    let mut cache = cache.lock().unwrap();
                    if cache.len() > cache_size_limit {
                        let mut entries: Vec<_> = cache
                            .iter()
                            .map(|(k, v)| (k.clone(), v.last_accessed))
                            .collect();

                        entries.sort_by(|a, b| a.1.cmp(&b.1));

                        let remove_count = cache.len() - cache_size_limit;
                        for (key, _) in entries.into_iter().take(remove_count) {
                            cache.remove(&key);
                        }

                        debug!(
                            removed_count = remove_count,
                            "Background cleanup removed LRU entries to enforce size limit"
                        );
                    }
                }
            }
        });
    }
}

/// Deduplication statistics
#[derive(Debug, Clone)]
pub struct DeduplicationStats {
    pub total_entries: usize,
    pub total_access_count: u32,
    pub cache_hit_rate: f64,
}

/// Trait for custom deduplication stores
#[async_trait::async_trait]
pub trait DeduplicationStore {
    async fn is_duplicate(&self, key: &str) -> Result<bool>;
    async fn mark_processed(&self, key: &str, message_id: &MessageId) -> Result<()>;
    async fn cleanup_expired(&self) -> Result<usize>;
}

/// Redis-based deduplication store (placeholder implementation)
#[derive(Debug)]
pub struct RedisDeduplicationStore {
    // This would contain Redis connection details
    _connection_string: String,
}

impl RedisDeduplicationStore {
    pub fn new(connection_string: String) -> Self {
        Self {
            _connection_string: connection_string,
        }
    }
}

#[async_trait::async_trait]
impl DeduplicationStore for RedisDeduplicationStore {
    async fn is_duplicate(&self, _key: &str) -> Result<bool> {
        // TODO: Implement Redis SETNX operation
        // This would use Redis SET with NX option to atomically check and set
        Ok(false)
    }

    async fn mark_processed(&self, _key: &str, _message_id: &MessageId) -> Result<()> {
        // TODO: Implement Redis SET operation with TTL
        Ok(())
    }

    async fn cleanup_expired(&self) -> Result<usize> {
        // Redis handles TTL automatically, so this might be a no-op
        // or could scan for keys with specific patterns
        Ok(0)
    }
}

/// Distributed deduplication manager using external store
pub struct DistributedDeduplicationManager {
    config: DeduplicationConfig,
    store: Arc<dyn DeduplicationStore + Send + Sync>,
    local_cache: Arc<Mutex<HashMap<String, DeduplicationRecord>>>,
}

impl DistributedDeduplicationManager {
    pub fn new(
        config: DeduplicationConfig,
        store: Arc<dyn DeduplicationStore + Send + Sync>,
    ) -> Self {
        Self {
            config,
            store,
            local_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn check_duplicate(
        &self,
        message: &DeduplicatedMessage,
    ) -> Result<DeduplicationResult> {
        let dedup_key = message.get_dedup_key(&self.config.strategy);

        // Check local cache first
        {
            let mut cache = self.local_cache.lock().unwrap();
            if let Some(record) = cache.get_mut(&dedup_key) {
                record.increment_access();
                return Ok(DeduplicationResult::Duplicate(DuplicateInfo {
                    original_message_id: record.message_id.clone(),
                    original_timestamp: record.timestamp,
                    duplicate_count: record.access_count,
                }));
            }
        }

        // Check distributed store
        if self.store.is_duplicate(&dedup_key).await? {
            // Add to local cache for faster future lookups
            {
                let mut cache = self.local_cache.lock().unwrap();
                let record = DeduplicationRecord::new(message.message_id.clone());
                cache.insert(dedup_key, record);
            }

            Ok(DeduplicationResult::Duplicate(DuplicateInfo {
                original_message_id: message.message_id.clone(), // We don't have the original ID from store
                original_timestamp: Utc::now(), // We don't have the original timestamp from store
                duplicate_count: 1,
            }))
        } else {
            // Mark as processed in both local cache and distributed store
            self.store
                .mark_processed(&dedup_key, &message.message_id)
                .await?;

            {
                let mut cache = self.local_cache.lock().unwrap();
                let record = DeduplicationRecord::new(message.message_id.clone());
                cache.insert(dedup_key, record);
            }

            Ok(DeduplicationResult::Unique)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_id_generation() {
        let id1 = MessageId::new();
        let id2 = MessageId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_content_hash() {
        let content1 = b"hello world";
        let content2 = b"hello world";
        let content3 = b"different content";

        let hash1 = ContentHash::from_content(content1);
        let hash2 = ContentHash::from_content(content2);
        let hash3 = ContentHash::from_content(content3);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_deduplication_keys() {
        let payload = b"test message".to_vec();
        let message = DeduplicatedMessage::new(payload).with_custom_key("custom_123".to_string());

        let key1 = message.get_dedup_key(&DeduplicationStrategy::MessageId);
        let key2 = message.get_dedup_key(&DeduplicationStrategy::ContentHash);
        let key3 = message.get_dedup_key(&DeduplicationStrategy::IdAndContent);
        let key4 = message.get_dedup_key(&DeduplicationStrategy::CustomKey("test".to_string()));

        assert_eq!(key1, message.message_id.as_str());
        assert_eq!(key2, message.content_hash.value().to_string());
        assert!(key3.contains(&message.message_id.as_str()));
        assert!(key3.contains(&message.content_hash.value().to_string()));
        assert_eq!(key4, "custom_123");
    }

    #[tokio::test]
    async fn test_deduplication_manager() {
        let config = DeduplicationConfig::default();
        let manager = DeduplicationManager::new(config);

        let payload = b"test message".to_vec();
        let message = DeduplicatedMessage::new(payload);

        // First check should be unique
        let result1 = manager.check_duplicate(&message).unwrap();
        assert_eq!(result1, DeduplicationResult::Unique);

        // Second check should be duplicate
        let result2 = manager.check_duplicate(&message).unwrap();
        assert!(matches!(result2, DeduplicationResult::Duplicate(_)));

        if let DeduplicationResult::Duplicate(info) = result2 {
            assert_eq!(info.original_message_id, message.message_id);
            assert_eq!(info.duplicate_count, 2);
        }
    }

    #[tokio::test]
    async fn test_different_strategies() {
        let config_id = DeduplicationConfig {
            strategy: DeduplicationStrategy::MessageId,
            ..Default::default()
        };
        let manager_id = DeduplicationManager::new(config_id);

        let config_content = DeduplicationConfig {
            strategy: DeduplicationStrategy::ContentHash,
            ..Default::default()
        };
        let manager_content = DeduplicationManager::new(config_content);

        // Same content, different message IDs
        let payload = b"same content".to_vec();
        let message1 = DeduplicatedMessage::new(payload.clone());
        let message2 = DeduplicatedMessage::new(payload).with_message_id(MessageId::new());

        // For message ID strategy - should be unique (different IDs)
        let result1_id = manager_id.check_duplicate(&message1).unwrap();
        let result2_id = manager_id.check_duplicate(&message2).unwrap();
        assert_eq!(result1_id, DeduplicationResult::Unique);
        assert_eq!(result2_id, DeduplicationResult::Unique);

        // For content hash strategy - should be duplicate (same content)
        let result1_content = manager_content.check_duplicate(&message1).unwrap();
        let result2_content = manager_content.check_duplicate(&message2).unwrap();
        assert_eq!(result1_content, DeduplicationResult::Unique);
        assert!(matches!(result2_content, DeduplicationResult::Duplicate(_)));
    }

    #[test]
    fn test_message_expiry() {
        let mut message =
            DeduplicatedMessage::new(b"test".to_vec()).with_ttl(Duration::from_millis(1));

        assert!(!message.is_expired());

        // Manually set timestamp to past
        message.timestamp = Utc::now() - chrono::Duration::seconds(1);
        assert!(message.is_expired());
    }

    #[tokio::test]
    async fn test_cache_cleanup() {
        let config = DeduplicationConfig {
            default_ttl: Duration::from_millis(100),
            ..Default::default()
        };
        let manager = DeduplicationManager::new(config);

        let payload = b"test message".to_vec();
        let message = DeduplicatedMessage::new(payload);

        // Add message to cache
        manager.check_duplicate(&message).unwrap();

        // Initially should have 1 entry
        let stats = manager.cache_stats();
        assert_eq!(stats.total_entries, 1);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Cleanup expired entries
        let cleaned = manager.cleanup_expired();
        assert_eq!(cleaned, 1);

        // Should have 0 entries now
        let stats = manager.cache_stats();
        assert_eq!(stats.total_entries, 0);
    }
}

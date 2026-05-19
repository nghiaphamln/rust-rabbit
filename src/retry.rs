//! Simplified and flexible retry system for rust-rabbit
//!
//! This module provides a simple but powerful retry mechanism with support for:
//! - Configurable max retry count
//! - Multiple retry mechanisms: exponential (1s->2s->4s->8s), linear, custom delays
//! - Delay-based message retry using RabbitMQ delayed exchanges
//! - Two delay strategies: TTL-based (original) or RabbitMQ delayed exchange plugin

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Strategy for delaying retry messages
///
/// # TTL (Time-To-Live) Strategy
/// Uses RabbitMQ's TTL feature with temporary retry queues.
/// Messages are published to temporary queues that expire and route to the original queue.
/// **Pros**: No plugin required, simple setup
/// **Cons**: Less precise timing, requires queue cleanup
///
/// # DelayedExchange Strategy
/// Uses the RabbitMQ delayed message exchange plugin (requires x-delayed-message plugin).
/// Messages are published to a delay exchange with x-delay header.
/// The exchange automatically routes messages after the delay period.
/// **Pros**: More precise, cleaner architecture, single exchange
/// **Cons**: Requires RabbitMQ plugin installation
///
/// # Example
/// ```rust
/// use rust_rabbit::{DelayStrategy, RetryConfig};
/// use std::time::Duration;
///
/// // Using TTL strategy (default, no plugin required)
/// let config = RetryConfig::exponential_default()
///     .with_delay_strategy(DelayStrategy::TTL);
///
/// // Using RabbitMQ delayed exchange plugin (requires plugin)
/// let config = RetryConfig::linear(3, Duration::from_secs(5))
///     .with_delay_strategy(DelayStrategy::DelayedExchange);
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum DelayStrategy {
    /// TTL-based delays using temporary queues (no plugin required)
    #[default]
    TTL,
    /// RabbitMQ delayed message exchange plugin (requires x-delayed-message plugin)
    ///
    /// **IMPORTANT**: Using this strategy without the `rabbitmq_delayed_message_exchange` plugin
    /// will cause your application to crash with "NOT_FOUND - operation not permitted on this exchange" error.
    ///
    /// Before deploying code with `DelayedExchange`, ensure:
    /// 1. Plugin is installed: <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases>
    /// 2. Plugin is enabled: `rabbitmq-plugins enable rabbitmq_delayed_message_exchange`
    /// 3. RabbitMQ is restarted after plugin installation
    DelayedExchange,
}

/// Retry mechanism configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryMechanism {
    /// Exponential backoff: base_delay * 2^attempt, capped at max_delay
    /// Example: 1s -> 2s -> 4s -> 8s -> 16s (capped at max_delay)
    Exponential {
        base_delay: Duration,
        max_delay: Duration,
    },

    /// Linear delay: same delay for each retry attempt
    /// Example: 5s -> 5s -> 5s -> 5s
    Linear { delay: Duration },

    /// Custom delays: specify exact delay for each retry attempt
    /// Example: [1s, 5s, 30s] for 3 retries with increasing delays
    Custom { delays: Vec<Duration> },
}

/// Simple retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 means no retries)
    pub max_retries: u32,

    /// Retry mechanism to use
    pub mechanism: RetryMechanism,

    /// Strategy for delaying retry messages (TTL or DelayedExchange)
    #[serde(default)]
    pub delay_strategy: DelayStrategy,

    /// Dead letter exchange for messages that exceed max retries
    /// If None, uses default: "{queue_name}.dlx"
    pub dead_letter_exchange: Option<String>,

    /// Dead letter queue for permanently failed messages
    /// If None, uses default: "{queue_name}.dlq"
    pub dead_letter_queue: Option<String>,

    /// TTL for dead letter queue (auto-cleanup failed messages after this duration)
    /// If set, messages in DLQ will be automatically removed after TTL expires
    /// Example: Duration::from_secs(86400) = 1 day
    pub dlq_ttl: Option<Duration>,
}

impl RetryConfig {
    /// Create exponential retry: 1s -> 2s -> 4s -> 8s -> 16s (max 5 retries)
    pub fn exponential_default() -> Self {
        Self {
            max_retries: 5,
            mechanism: RetryMechanism::Exponential {
                base_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(60),
            },
            delay_strategy: DelayStrategy::default(),
            dead_letter_exchange: None,
            dead_letter_queue: None,
            dlq_ttl: None,
        }
    }

    /// Create exponential retry with custom parameters
    pub fn exponential(max_retries: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries,
            mechanism: RetryMechanism::Exponential {
                base_delay,
                max_delay,
            },
            delay_strategy: DelayStrategy::default(),
            dead_letter_exchange: None,
            dead_letter_queue: None,
            dlq_ttl: None,
        }
    }

    /// Create linear retry: same delay for each attempt
    pub fn linear(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            mechanism: RetryMechanism::Linear { delay },
            delay_strategy: DelayStrategy::default(),
            dead_letter_exchange: None,
            dead_letter_queue: None,
            dlq_ttl: None,
        }
    }

    /// Create custom retry with specific delays for each attempt
    pub fn custom(delays: Vec<Duration>) -> Self {
        let max_retries = delays.len() as u32;
        Self {
            max_retries,
            mechanism: RetryMechanism::Custom { delays },
            delay_strategy: DelayStrategy::default(),
            dead_letter_exchange: None,
            dead_letter_queue: None,
            dlq_ttl: None,
        }
    }

    /// No retries - fail immediately
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            mechanism: RetryMechanism::Linear {
                delay: Duration::from_secs(0),
            },
            delay_strategy: DelayStrategy::default(),
            dead_letter_exchange: None,
            dead_letter_queue: None,
            dlq_ttl: None,
        }
    }

    /// Set custom dead letter exchange and queue names
    pub fn with_dead_letter(mut self, exchange: String, queue: String) -> Self {
        self.dead_letter_exchange = Some(exchange);
        self.dead_letter_queue = Some(queue);
        self
    }

    /// Set delay strategy to use (TTL or DelayedExchange)
    pub fn with_delay_strategy(mut self, strategy: DelayStrategy) -> Self {
        self.delay_strategy = strategy;
        self
    }

    /// Set TTL for dead letter queue (auto-cleanup failed messages)
    /// Messages in DLQ will be automatically removed after this duration
    ///
    /// # Example
    /// ```rust
    /// # use rust_rabbit::RetryConfig;
    /// # use std::time::Duration;
    /// let config = RetryConfig::exponential_default()
    ///     .with_dlq_ttl(Duration::from_secs(86400));  // 1 day
    /// ```
    pub fn with_dlq_ttl(mut self, ttl: Duration) -> Self {
        self.dlq_ttl = Some(ttl);
        self
    }

    /// Calculate delay for a specific retry attempt (0-indexed)
    pub fn calculate_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None; // No more retries
        }

        let delay = match &self.mechanism {
            RetryMechanism::Exponential {
                base_delay,
                max_delay,
            } => {
                let exponential_delay =
                    Duration::from_millis(base_delay.as_millis() as u64 * 2_u64.pow(attempt));
                std::cmp::min(exponential_delay, *max_delay)
            }
            RetryMechanism::Linear { delay } => *delay,
            RetryMechanism::Custom { delays } => {
                if (attempt as usize) < delays.len() {
                    delays[attempt as usize]
                } else {
                    return None; // No more custom delays
                }
            }
        };

        Some(delay)
    }

    /// Get dead letter exchange name (with fallback to default)
    pub fn get_dead_letter_exchange(&self, queue_name: &str) -> String {
        self.dead_letter_exchange
            .clone()
            .unwrap_or_else(|| format!("{}.dlx", queue_name))
    }

    /// Get dead letter queue name (with fallback to default)
    pub fn get_dead_letter_queue(&self, queue_name: &str) -> String {
        self.dead_letter_queue
            .clone()
            .unwrap_or_else(|| format!("{}.dlq", queue_name))
    }

    /// Generate retry queue name for delayed messages
    pub fn get_retry_queue_name(&self, queue_name: &str, attempt: u32) -> String {
        format!("{}.retry.{}", queue_name, attempt + 1)
    }

    /// Get delay exchange name (for delayed exchange strategy)
    pub fn get_delay_exchange(&self, queue_name: &str) -> String {
        format!("{}.delay", queue_name)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::exponential_default()
    }
}

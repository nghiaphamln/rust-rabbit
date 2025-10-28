//! Simplified and flexible retry system for rust-rabbit
//! 
//! This module provides a simple but powerful retry mechanism with support for:
//! - Configurable max retry count
//! - Multiple retry mechanisms: exponential (1s->2s->4s->8s), linear, custom delays
//! - Delay-based message retry using RabbitMQ delayed exchanges

use serde::{Deserialize, Serialize};
use std::time::Duration;

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
    Linear {
        delay: Duration,
    },
    
    /// Custom delays: specify exact delay for each retry attempt
    /// Example: [1s, 5s, 30s] for 3 retries with increasing delays
    Custom {
        delays: Vec<Duration>,
    },
}

/// Simple retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 means no retries)
    pub max_retries: u32,
    
    /// Retry mechanism to use
    pub mechanism: RetryMechanism,
    
    /// Dead letter exchange for messages that exceed max retries
    /// If None, uses default: "{queue_name}.dlx"
    pub dead_letter_exchange: Option<String>,
    
    /// Dead letter queue for permanently failed messages
    /// If None, uses default: "{queue_name}.dlq"
    pub dead_letter_queue: Option<String>,
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
            dead_letter_exchange: None,
            dead_letter_queue: None,
        }
    }
    
    /// Create exponential retry with custom parameters
    pub fn exponential(max_retries: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries,
            mechanism: RetryMechanism::Exponential { base_delay, max_delay },
            dead_letter_exchange: None,
            dead_letter_queue: None,
        }
    }
    
    /// Create linear retry: same delay for each attempt
    pub fn linear(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            mechanism: RetryMechanism::Linear { delay },
            dead_letter_exchange: None,
            dead_letter_queue: None,
        }
    }
    
    /// Create custom retry with specific delays for each attempt
    pub fn custom(delays: Vec<Duration>) -> Self {
        let max_retries = delays.len() as u32;
        Self {
            max_retries,
            mechanism: RetryMechanism::Custom { delays },
            dead_letter_exchange: None,
            dead_letter_queue: None,
        }
    }
    
    /// No retries - fail immediately
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            mechanism: RetryMechanism::Linear {
                delay: Duration::from_secs(0),
            },
            dead_letter_exchange: None,
            dead_letter_queue: None,
        }
    }
    
    /// Set custom dead letter exchange and queue names
    pub fn with_dead_letter(mut self, exchange: String, queue: String) -> Self {
        self.dead_letter_exchange = Some(exchange);
        self.dead_letter_queue = Some(queue);
        self
    }
    
    /// Calculate delay for a specific retry attempt (0-indexed)
    pub fn calculate_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None; // No more retries
        }
        
        let delay = match &self.mechanism {
            RetryMechanism::Exponential { base_delay, max_delay } => {
                let exponential_delay = Duration::from_millis(
                    base_delay.as_millis() as u64 * 2_u64.pow(attempt)
                );
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
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::exponential_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_retry() {
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(30));
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));  // 1s
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2)));  // 2s
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4)));  // 4s
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(8)));  // 8s
        assert_eq!(config.calculate_delay(4), Some(Duration::from_secs(16))); // 16s
        assert_eq!(config.calculate_delay(5), None); // No more retries
    }
    
    #[test]
    fn test_exponential_retry_with_cap() {
        let config = RetryConfig::exponential(10, Duration::from_secs(1), Duration::from_secs(5));
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1))); // 1s
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2))); // 2s
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4))); // 4s
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(5))); // 5s (capped)
        assert_eq!(config.calculate_delay(4), Some(Duration::from_secs(5))); // 5s (capped)
    }
    
    #[test]
    fn test_linear_retry() {
        let config = RetryConfig::linear(3, Duration::from_secs(5));
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(3), None); // No more retries
    }
    
    #[test]
    fn test_custom_retry() {
        let delays = vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
        ];
        let config = RetryConfig::custom(delays);
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(30)));
        assert_eq!(config.calculate_delay(3), None); // No more retries
    }
    
    #[test]
    fn test_no_retry() {
        let config = RetryConfig::no_retry();
        
        assert_eq!(config.calculate_delay(0), None); // No retries at all
    }
    
    #[test]
    fn test_dead_letter_names() {
        let config = RetryConfig::exponential_default();
        
        assert_eq!(config.get_dead_letter_exchange("orders"), "orders.dlx");
        assert_eq!(config.get_dead_letter_queue("orders"), "orders.dlq");
        
        let config_custom = config.with_dead_letter("custom.dlx".to_string(), "custom.dlq".to_string());
        assert_eq!(config_custom.get_dead_letter_exchange("orders"), "custom.dlx");
        assert_eq!(config_custom.get_dead_letter_queue("orders"), "custom.dlq");
    }
    
    #[test]
    fn test_retry_queue_names() {
        let config = RetryConfig::exponential_default();
        
        assert_eq!(config.get_retry_queue_name("orders", 0), "orders.retry.1");
        assert_eq!(config.get_retry_queue_name("orders", 1), "orders.retry.2");
        assert_eq!(config.get_retry_queue_name("orders", 4), "orders.retry.5");
    }
}
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration for RabbitMQ connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitConfig {
    /// RabbitMQ connection string (e.g., "amqp://localhost:5672")
    pub connection_string: String,
    
    /// Virtual host (default: "/")
    pub virtual_host: Option<String>,
    
    /// Connection timeout
    pub connection_timeout: Option<Duration>,
    
    /// Heartbeat interval
    pub heartbeat: Option<Duration>,
    
    /// Retry configuration for connections
    pub retry_config: RetryConfig,
    
    /// Health check configuration
    pub health_check: HealthCheckConfig,
    
    /// Connection pool configuration
    pub pool_config: PoolConfig,
}

impl Default for RabbitConfig {
    fn default() -> Self {
        Self {
            connection_string: "amqp://localhost:5672".to_string(),
            virtual_host: Some("/".to_string()),
            connection_timeout: Some(Duration::from_secs(30)),
            heartbeat: Some(Duration::from_secs(60)),
            retry_config: RetryConfig::default(),
            health_check: HealthCheckConfig::default(),
            pool_config: PoolConfig::default(),
        }
    }
}

/// Retry configuration for various operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    
    /// Initial delay between retries
    pub initial_delay: Duration,
    
    /// Maximum delay between retries
    pub max_delay: Duration,
    
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    
    /// Jitter factor (0.0 to 1.0) to add randomness to delays
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter: 0.1,
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Interval between health checks
    pub check_interval: Duration,
    
    /// Timeout for each health check
    pub check_timeout: Duration,
    
    /// Enable health monitoring
    pub enabled: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            enabled: true,
        }
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    
    /// Connection idle timeout
    pub idle_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            idle_timeout: Duration::from_secs(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_rabbit_config_default() {
        let config = RabbitConfig::default();
        assert_eq!(config.connection_string, "amqp://localhost:5672");
        assert_eq!(config.virtual_host, Some("/".to_string()));
        assert_eq!(config.connection_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.heartbeat, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(1000));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.jitter, 0.1);
    }

    #[test]
    fn test_health_check_config_presets() {
        use crate::health::HealthCheckConfigExt;
        
        let conservative = HealthCheckConfig::conservative();
        assert_eq!(conservative.check_interval, Duration::from_secs(60));
        assert_eq!(conservative.check_timeout, Duration::from_secs(10));
        assert!(conservative.enabled);

        let aggressive = HealthCheckConfig::aggressive();
        assert_eq!(aggressive.check_interval, Duration::from_secs(10));
        assert_eq!(aggressive.check_timeout, Duration::from_secs(3));
        assert!(aggressive.enabled);

        let minimal = HealthCheckConfig::minimal();
        assert_eq!(minimal.check_interval, Duration::from_secs(300));
        assert_eq!(minimal.check_timeout, Duration::from_secs(15));
        assert!(minimal.enabled);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
    }
}
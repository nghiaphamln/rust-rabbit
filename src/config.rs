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

impl RabbitConfig {
    /// Create a new configuration builder
    pub fn builder() -> RabbitConfigBuilder {
        RabbitConfigBuilder::new()
    }
}

/// Builder for RabbitConfig
#[derive(Debug, Clone)]
pub struct RabbitConfigBuilder {
    connection_string: String,
    virtual_host: Option<String>,
    connection_timeout: Option<Duration>,
    heartbeat: Option<Duration>,
    retry_config: RetryConfig,
    health_check: HealthCheckConfig,
    pool_config: PoolConfig,
}

impl RabbitConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
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

    /// Set the connection string
    pub fn connection_string<S: Into<String>>(mut self, connection_string: S) -> Self {
        self.connection_string = connection_string.into();
        self
    }

    /// Set the virtual host
    pub fn virtual_host<S: Into<String>>(mut self, virtual_host: S) -> Self {
        self.virtual_host = Some(virtual_host.into());
        self
    }

    /// Clear the virtual host (use default)
    pub fn no_virtual_host(mut self) -> Self {
        self.virtual_host = None;
        self
    }

    /// Set the connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Disable connection timeout
    pub fn no_connection_timeout(mut self) -> Self {
        self.connection_timeout = None;
        self
    }

    /// Set the heartbeat interval
    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// Disable heartbeat
    pub fn no_heartbeat(mut self) -> Self {
        self.heartbeat = None;
        self
    }

    /// Set retry configuration
    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Configure retry settings with a builder
    pub fn retry<F>(mut self, f: F) -> Self 
    where
        F: FnOnce(RetryConfigBuilder) -> RetryConfigBuilder,
    {
        self.retry_config = f(RetryConfigBuilder::new()).build();
        self
    }

    /// Set health check configuration
    pub fn health_check(mut self, health_check: HealthCheckConfig) -> Self {
        self.health_check = health_check;
        self
    }

    /// Configure health check settings with a builder
    pub fn health<F>(mut self, f: F) -> Self 
    where
        F: FnOnce(HealthCheckConfigBuilder) -> HealthCheckConfigBuilder,
    {
        self.health_check = f(HealthCheckConfigBuilder::new()).build();
        self
    }

    /// Set pool configuration
    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    /// Configure pool settings with a builder
    pub fn pool<F>(mut self, f: F) -> Self 
    where
        F: FnOnce(PoolConfigBuilder) -> PoolConfigBuilder,
    {
        self.pool_config = f(PoolConfigBuilder::new()).build();
        self
    }

    /// Build the final configuration
    pub fn build(self) -> RabbitConfig {
        RabbitConfig {
            connection_string: self.connection_string,
            virtual_host: self.virtual_host,
            connection_timeout: self.connection_timeout,
            heartbeat: self.heartbeat,
            retry_config: self.retry_config,
            health_check: self.health_check,
            pool_config: self.pool_config,
        }
    }
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

impl RetryConfig {
    /// Create a new retry configuration builder
    pub fn builder() -> RetryConfigBuilder {
        RetryConfigBuilder::new()
    }
}

/// Builder for RetryConfig
#[derive(Debug, Clone)]
pub struct RetryConfigBuilder {
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: f64,
    jitter: f64,
}

impl RetryConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            jitter: 0.1,
        }
    }

    /// Set maximum number of retry attempts
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set initial delay between retries
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set maximum delay between retries
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set backoff multiplier for exponential backoff
    pub fn backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Set jitter factor (0.0 to 1.0)
    pub fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter.clamp(0.0, 1.0);
        self
    }

    /// Disable jitter
    pub fn no_jitter(mut self) -> Self {
        self.jitter = 0.0;
        self
    }

    /// Configure for aggressive retries (more attempts, shorter delays)
    pub fn aggressive(mut self) -> Self {
        self.max_retries = 5;
        self.initial_delay = Duration::from_millis(500);
        self.max_delay = Duration::from_secs(30);
        self.backoff_multiplier = 1.5;
        self.jitter = 0.05;
        self
    }

    /// Configure for conservative retries (fewer attempts, longer delays)
    pub fn conservative(mut self) -> Self {
        self.max_retries = 2;
        self.initial_delay = Duration::from_secs(2);
        self.max_delay = Duration::from_secs(120);
        self.backoff_multiplier = 3.0;
        self.jitter = 0.2;
        self
    }

    /// Build the final configuration
    pub fn build(self) -> RetryConfig {
        RetryConfig {
            max_retries: self.max_retries,
            initial_delay: self.initial_delay,
            max_delay: self.max_delay,
            backoff_multiplier: self.backoff_multiplier,
            jitter: self.jitter,
        }
    }
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

impl HealthCheckConfig {
    /// Create a new health check configuration builder
    pub fn builder() -> HealthCheckConfigBuilder {
        HealthCheckConfigBuilder::new()
    }
}

/// Builder for HealthCheckConfig
#[derive(Debug, Clone)]
pub struct HealthCheckConfigBuilder {
    check_interval: Duration,
    check_timeout: Duration,
    enabled: bool,
}

impl HealthCheckConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            enabled: true,
        }
    }

    /// Set health check interval
    pub fn check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Set health check timeout
    pub fn check_timeout(mut self, timeout: Duration) -> Self {
        self.check_timeout = timeout;
        self
    }

    /// Enable health monitoring
    pub fn enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Disable health monitoring
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Configure for frequent health checks
    pub fn frequent(mut self) -> Self {
        self.check_interval = Duration::from_secs(10);
        self.check_timeout = Duration::from_secs(3);
        self.enabled = true;
        self
    }

    /// Configure for infrequent health checks
    pub fn infrequent(mut self) -> Self {
        self.check_interval = Duration::from_secs(120);
        self.check_timeout = Duration::from_secs(10);
        self.enabled = true;
        self
    }

    /// Build the final configuration
    pub fn build(self) -> HealthCheckConfig {
        HealthCheckConfig {
            check_interval: self.check_interval,
            check_timeout: self.check_timeout,
            enabled: self.enabled,
        }
    }
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

impl PoolConfig {
    /// Create a new pool configuration builder
    pub fn builder() -> PoolConfigBuilder {
        PoolConfigBuilder::new()
    }
}

/// Builder for PoolConfig
#[derive(Debug, Clone)]
pub struct PoolConfigBuilder {
    max_connections: usize,
    min_connections: usize,
    idle_timeout: Duration,
}

impl PoolConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            idle_timeout: Duration::from_secs(300),
        }
    }

    /// Set maximum number of connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set minimum number of connections
    pub fn min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set connection idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Configure for high throughput (more connections)
    pub fn high_throughput(mut self) -> Self {
        self.max_connections = 50;
        self.min_connections = 5;
        self.idle_timeout = Duration::from_secs(180);
        self
    }

    /// Configure for low resource usage (fewer connections)
    pub fn low_resource(mut self) -> Self {
        self.max_connections = 3;
        self.min_connections = 1;
        self.idle_timeout = Duration::from_secs(600);
        self
    }

    /// Configure for single connection mode
    pub fn single_connection(mut self) -> Self {
        self.max_connections = 1;
        self.min_connections = 1;
        self.idle_timeout = Duration::from_secs(3600);
        self
    }

    /// Build the final configuration
    pub fn build(self) -> PoolConfig {
        PoolConfig {
            max_connections: self.max_connections,
            min_connections: self.min_connections.min(self.max_connections),
            idle_timeout: self.idle_timeout,
        }
    }
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
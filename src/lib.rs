pub mod connection;
pub mod publisher;
pub mod consumer;
pub mod config;
pub mod error;
pub mod retry;
pub mod health;

pub use connection::{Connection, ConnectionManager};
pub use publisher::{Publisher, PublishOptions, PublishOptionsBuilder, CustomQueueDeclareOptions, CustomExchangeDeclareOptions};
pub use consumer::{Consumer, ConsumerOptions, ConsumerOptionsBuilder, MessageHandler};
pub use config::{
    RabbitConfig, RabbitConfigBuilder,
    RetryConfig, RetryConfigBuilder,
    HealthCheckConfig, HealthCheckConfigBuilder,
    PoolConfig, PoolConfigBuilder,
};
pub use error::{RabbitError, Result};
pub use retry::{RetryPolicy, DelayedMessageExchange};
pub use health::{HealthChecker, ConnectionStatus};

/// Main facade for the rust-rabbit library
pub struct RustRabbit {
    connection_manager: ConnectionManager,
}

impl RustRabbit {
    /// Create a new RustRabbit instance with the given configuration
    pub async fn new(config: RabbitConfig) -> Result<Self> {
        let connection_manager = ConnectionManager::new(config).await?;
        Ok(Self { connection_manager })
    }

    /// Get a publisher instance
    pub fn publisher(&self) -> Publisher {
        Publisher::new(self.connection_manager.clone())
    }

    /// Create a consumer with the given options
    pub async fn consumer(&self, options: ConsumerOptions) -> Result<Consumer> {
        Consumer::new(self.connection_manager.clone(), options).await
    }

    /// Get the health checker
    pub fn health_checker(&self) -> HealthChecker {
        HealthChecker::new(self.connection_manager.clone())
    }

    /// Close all connections
    pub async fn close(&self) -> Result<()> {
        self.connection_manager.close().await
    }
}
pub mod config;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod health;
pub mod publisher;
pub mod retry;

pub use config::{
    HealthCheckConfig, HealthCheckConfigBuilder, PoolConfig, PoolConfigBuilder, RabbitConfig,
    RabbitConfigBuilder, RetryConfig, RetryConfigBuilder,
};
pub use connection::{Connection, ConnectionManager};
pub use consumer::{Consumer, ConsumerOptions, ConsumerOptionsBuilder, MessageHandler};
pub use error::{RabbitError, Result};
pub use health::{ConnectionStatus, HealthChecker};
pub use publisher::{
    CustomExchangeDeclareOptions, CustomQueueDeclareOptions, PublishOptions, PublishOptionsBuilder,
    Publisher,
};
pub use retry::{DelayedMessageExchange, RetryPolicy};

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

//! # RustRabbit
//!
//! A high-performance, production-ready RabbitMQ client library for Rust with advanced 
//! observability, resilience, and performance features.
//!
//! ## Features
//!
//! - **🚀 Performance**: Message batching and connection pooling for high throughput
//! - **🔍 Observability**: Comprehensive Prometheus metrics and health monitoring  
//! - **🛡️ Resilience**: Circuit breaker pattern and graceful shutdown handling
//! - **⚙️ Developer Experience**: Builder pattern APIs and type-safe message handling
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rust_rabbit::{RustRabbit, RabbitConfig};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct OrderMessage {
//!     order_id: String,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let config = RabbitConfig::builder()
//!         .connection_string("amqp://localhost:5672")
//!         .build();
//!     
//!     // Create RustRabbit instance
//!     let rabbit = RustRabbit::new(config).await?;
//!     
//!     // Publish message
//!     let publisher = rabbit.publisher();
//!     let order = OrderMessage {
//!         order_id: "ORD-12345".to_string(),
//!         amount: 99.99,
//!     };
//!     
//!     publisher.publish_to_queue("orders", &order, None).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Advanced Features
//!
//! ### Prometheus Metrics
//!
//! Enable comprehensive metrics collection:
//!
//! ```rust,no_run
//! use rust_rabbit::{RustRabbit, RabbitConfig, metrics::RustRabbitMetrics};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let metrics = RustRabbitMetrics::new()?;
//!     let config = RabbitConfig::default();
//!     let rabbit = RustRabbit::with_metrics(config, metrics).await?;
//!     Ok(())
//! }
//! ```
//!
//! ### Message Batching
//!
//! For high-throughput scenarios:
//!
//! ```rust,no_run
//! use rust_rabbit::{RustRabbit, batching::BatchConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let rabbit = RustRabbit::new(RabbitConfig::default()).await?;
//!     
//!     let batch_config = BatchConfig::builder()
//!         .max_batch_size(100)
//!         .flush_interval(Duration::from_millis(500))
//!         .build();
//!     
//!     let batcher = rabbit.create_batcher(batch_config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ### Graceful Shutdown
//!
//! Handle shutdown signals properly:
//!
//! ```rust,no_run
//! use rust_rabbit::{RustRabbit, shutdown::ShutdownConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut rabbit = RustRabbit::new(RabbitConfig::default()).await?;
//!     
//!     let shutdown_config = ShutdownConfig::builder()
//!         .pending_timeout(Duration::from_secs(30))
//!         .build();
//!     
//!     let _shutdown_manager = rabbit.enable_shutdown_handling(shutdown_config);
//!     Ok(())
//! }
//! ```

pub mod batching;
pub mod circuit_breaker;
pub mod config;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod health;
pub mod metrics;
pub mod publisher;
pub mod retry;
pub mod shutdown;

pub use batching::{BatchConfig, BatchConfigBuilder, MessageBatcher};
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats, CircuitState,
};
pub use config::{
    HealthCheckConfig, HealthCheckConfigBuilder, PoolConfig, PoolConfigBuilder, RabbitConfig,
    RabbitConfigBuilder, RetryConfig, RetryConfigBuilder,
};
pub use connection::{Connection, ConnectionManager, ConnectionStats};
pub use consumer::{Consumer, ConsumerOptions, ConsumerOptionsBuilder, MessageHandler};
pub use error::{RabbitError, Result};
pub use health::{ConnectionStatus, HealthChecker};
pub use metrics::{MetricsTimer, RustRabbitMetrics};
pub use publisher::{
    CustomExchangeDeclareOptions, CustomQueueDeclareOptions, PublishOptions, PublishOptionsBuilder,
    Publisher,
};
pub use retry::{DelayedMessageExchange, RetryPolicy};
pub use shutdown::{
    setup_signal_handling, ShutdownConfig, ShutdownHandler, ShutdownManager, ShutdownSignal,
};

/// Main facade for the rust-rabbit library
pub struct RustRabbit {
    connection_manager: ConnectionManager,
    metrics: Option<RustRabbitMetrics>,
    shutdown_manager: Option<std::sync::Arc<ShutdownManager>>,
}

impl RustRabbit {
    /// Create a new RustRabbit instance with the given configuration
    pub async fn new(config: RabbitConfig) -> Result<Self> {
        let connection_manager = ConnectionManager::new(config).await?;
        Ok(Self {
            connection_manager,
            metrics: None,
            shutdown_manager: None,
        })
    }

    /// Create a new RustRabbit instance with metrics enabled
    pub async fn with_metrics(config: RabbitConfig, metrics: RustRabbitMetrics) -> Result<Self> {
        let connection_manager = ConnectionManager::new(config).await?;
        Ok(Self {
            connection_manager,
            metrics: Some(metrics),
            shutdown_manager: None,
        })
    }

    /// Get a publisher instance
    pub fn publisher(&self) -> Publisher {
        let mut publisher = Publisher::new(self.connection_manager.clone());
        if let Some(metrics) = &self.metrics {
            publisher.set_metrics(metrics.clone());
        }
        publisher
    }

    /// Create a consumer with the given options
    pub async fn consumer(&self, options: ConsumerOptions) -> Result<Consumer> {
        let mut consumer = Consumer::new(self.connection_manager.clone(), options).await?;
        if let Some(metrics) = &self.metrics {
            consumer.set_metrics(metrics.clone());
        }
        Ok(consumer)
    }

    /// Get the health checker
    pub fn health_checker(&self) -> HealthChecker {
        let mut health_checker = HealthChecker::new(self.connection_manager.clone());
        if let Some(metrics) = &self.metrics {
            health_checker.set_metrics(metrics.clone());
        }
        health_checker
    }

    /// Get the metrics instance if enabled
    pub fn metrics(&self) -> Option<&RustRabbitMetrics> {
        self.metrics.as_ref()
    }

    /// Create a message batcher for high-throughput publishing
    pub async fn create_batcher(&self, config: BatchConfig) -> Result<MessageBatcher> {
        let publisher = self.publisher();

        if let Some(metrics) = &self.metrics {
            MessageBatcher::with_metrics(publisher, config, metrics.clone()).await
        } else {
            MessageBatcher::new(publisher, config).await
        }
    }

    /// Enable graceful shutdown handling
    pub fn enable_shutdown_handling(
        &mut self,
        config: ShutdownConfig,
    ) -> std::sync::Arc<ShutdownManager> {
        let shutdown_manager = std::sync::Arc::new(ShutdownManager::new(config));
        self.shutdown_manager = Some(shutdown_manager.clone());
        shutdown_manager
    }

    /// Get the shutdown manager if enabled
    pub fn shutdown_manager(&self) -> Option<std::sync::Arc<ShutdownManager>> {
        self.shutdown_manager.clone()
    }

    /// Close all connections with optional graceful shutdown
    pub async fn close(&self) -> Result<()> {
        if let Some(shutdown_manager) = &self.shutdown_manager {
            shutdown_manager.shutdown(ShutdownSignal::Graceful).await?;
        }
        self.connection_manager.close().await
    }
}

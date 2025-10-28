//! # RustRabbit 🐰
//!
//! A **high-performance, production-ready** RabbitMQ client library for Rust with **zero-configuration**
//! simplicity and enterprise-grade features. Built for reliability, observability, and developer happiness.
//!
//! ## Features
//!
//! - **🚀 Smart Automation**: One-line setup with `RetryPolicy::fast()` configures everything
//! - **🔄 Advanced Retry System**: Multiple presets, exponential backoff, dead letter integration  
//! - **🏗️ Enterprise Patterns**: Request-Response, Saga, Event Sourcing, Priority Queues
//! - **🔍 Production Observability**: Prometheus metrics, health monitoring, circuit breaker
//! - **🛡️ Reliability**: Connection pooling, graceful shutdown, error recovery
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rust_rabbit::{
//!     config::RabbitConfig,
//!     connection::ConnectionManager,
//!     consumer::{Consumer, ConsumerOptions},
//!     retry::RetryPolicy,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Connection
//!     let config = RabbitConfig::builder()
//!         .connection_string("amqp://user:pass@localhost:5672/vhost")
//!         .build();
//!     let connection = ConnectionManager::new(config).await?;
//!
//!     // 2. Consumer with retry (1 line!)
//!     let options = ConsumerOptions {
//!         auto_ack: false,
//!         retry_policy: Some(RetryPolicy::fast()),
//!         ..Default::default()
//!     };
//!
//!     // 3. Create consumer (ready to use)
//!     let _consumer = Consumer::new(connection, options).await?;
//!     
//!     // Consumer is ready! See examples/ for usage patterns
//!     Ok(())
//! }
//! ```
//!
//! **What `RetryPolicy::fast()` creates automatically:**
//! - ✅ **5 retries**: 200ms → 300ms → 450ms → 675ms → 1s (capped at 10s)
//! - ✅ **Dead Letter Queue**: Automatic DLX/DLQ setup for failed messages
//! - ✅ **Backoff + Jitter**: Intelligent delay with randomization
//! - ✅ **Production Ready**: Optimal settings for most use cases
//!
//! ## Retry Patterns
//!
//! ```rust,no_run
//! use rust_rabbit::retry::RetryPolicy;
//! use std::time::Duration;
//!
//! // Quick presets for common scenarios
//! let fast = RetryPolicy::fast();               // 5 retries, 200ms→10s, 1.5x backoff
//! let slow = RetryPolicy::slow();               // 3 retries, 1s→1min, 2.0x backoff
//! let linear = RetryPolicy::linear(Duration::from_millis(500), 3); // Fixed 500ms intervals
//!
//! // Custom with builder
//! let custom = RetryPolicy::builder()
//!     .max_retries(5)
//!     .initial_delay(Duration::from_millis(100))
//!     .backoff_multiplier(2.0)
//!     .jitter(0.1)
//!     .dead_letter_exchange("my.dlx")
//!     .build();
//! ```
//!
//! ## Advanced Patterns
//!
//! ### Request-Response (RPC)
//!
//! ```rust,no_run
//! use rust_rabbit::patterns::request_response::*;
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Simple example - actual usage requires proper message types
//!     let client = RequestResponseClient::new(Duration::from_secs(30));
//!     
//!     // In real usage, you would send actual request messages
//!     // let response = client.send_request("queue", request_data, None).await?;
//!     Ok(())
//! }
//! ```
//!
//! ### Event Sourcing (CQRS)
//!
//! ```rust,no_run
//! use rust_rabbit::patterns::event_sourcing::*;
//! use std::sync::Arc;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let event_store = Arc::new(InMemoryEventStore::new());
//!     
//!     // Example - actual usage requires implementing AggregateRoot trait
//!     // let repository = EventSourcingRepository::<MyAggregate>::new(event_store);
//!     Ok(())
//! }
//! ```
//!
//! ## Production Features
//!
//! ### Health Monitoring
//!
//! ```rust,no_run
//! use rust_rabbit::{health::HealthChecker, connection::ConnectionManager, config::RabbitConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RabbitConfig::builder()
//!         .connection_string("amqp://localhost:5672")
//!         .build();
//!     let connection_manager = ConnectionManager::new(config).await?;
//!     let health_checker = HealthChecker::new(connection_manager.clone());
//!     
//!     match health_checker.check_health().await {
//!         Ok(status) => println!("Connection healthy: {:?}", status),
//!         Err(e) => println!("Connection issues: {}", e),
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ### Prometheus Metrics
//!
//! ```rust,no_run
//! use rust_rabbit::metrics::RustRabbitMetrics;
//!
//! let metrics = RustRabbitMetrics::new();
//! // Metrics automatically collected:
//! // - rust_rabbit_messages_published_total
//! // - rust_rabbit_messages_consumed_total  
//! // - rust_rabbit_message_processing_duration_seconds
//! // - rust_rabbit_connection_health
//! ```

pub mod batching;
pub mod circuit_breaker;
pub mod config;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod health;
pub mod metrics;
pub mod patterns; // Phase 2: Advanced messaging patterns
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
pub use error::{RabbitError, Result, RustRabbitError};
pub use health::{ConnectionStatus, HealthChecker};
pub use metrics::{MetricsTimer, RustRabbitMetrics};
pub use patterns::{
    // Message deduplication
    deduplication::{
        ContentHash, DeduplicatedMessage, DeduplicationConfig, DeduplicationManager,
        DeduplicationResult, DeduplicationStrategy, DuplicateInfo, MessageId,
    },
    // Event sourcing
    event_sourcing::{
        AggregateId, AggregateRoot, AggregateSnapshot, DomainEvent, EventReplayService,
        EventSequence, EventSourcingRepository, EventStore, InMemoryEventStore,
    },
    // Priority queues
    priority::{
        Priority, PriorityConsumer, PriorityMessage, PriorityQueue, PriorityQueueConfig,
        PriorityRouter,
    },
    // Request-Response pattern
    request_response::{
        CorrelationId, RequestHandler, RequestMessage, RequestResponseClient,
        RequestResponseServer, ResponseMessage,
    },
    // Saga pattern
    saga::{
        SagaAction, SagaCoordinator, SagaId, SagaInstance, SagaStatus, SagaStep, SagaStepExecutor,
        StepResult, StepStatus,
    },
};
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

//! # rust-rabbit 🐰
//!
//! A **simple, reliable** RabbitMQ client library for Rust. 
//! Focus on core functionality with minimal configuration.
//!
//! ## Features
//!
//! - **🚀 Simple API**: Just Publisher and Consumer with essential methods
//! - **🔄 Flexible Retry**: Exponential, linear, or custom retry mechanisms  
//! - **🛠️ Auto-Setup**: Automatic queue/exchange declaration and binding
//! - **⚡ Built-in Reliability**: Default ACK behavior with error handling
//!
//! ## Quick Start
//!
//! ### Publisher
//!
//! ```rust,no_run
//! use rust_rabbit::{Connection, Publisher, PublishOptions};
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct Order {
//!     id: u32,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection = Connection::new("amqp://localhost:5672").await?;
//!     let publisher = Publisher::new(connection);
//!     
//!     let order = Order { id: 123, amount: 99.99 };
//!     
//!     // Publish to exchange
//!     publisher.publish_to_exchange("orders", "new.order", &order, None).await?;
//!     
//!     // Publish directly to queue
//!     publisher.publish_to_queue("order_queue", &order, None).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Consumer with Retry
//!
//! ```rust,no_run
//! use rust_rabbit::{Connection, Consumer, RetryConfig};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u32,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection = Connection::new("amqp://localhost:5672").await?;
//!     
//!     let consumer = Consumer::builder(connection, "order_queue")
//!         .retry(RetryConfig::exponential_default()) // 1s->2s->4s->8s->16s
//!         .bind_to_exchange("orders")
//!         .concurrency(5)
//!         .build()
//!         .await?;
//!     
//!     consumer.consume(|order: Order| async move {
//!         println!("Processing order {}: ${}", order.id, order.amount);
//!         // Your business logic here
//!         Ok(()) // ACK message
//!     }).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Retry Configurations
//!
//! ```rust,no_run
//! use rust_rabbit::RetryConfig;
//! use std::time::Duration;
//!
//! // Exponential: 1s -> 2s -> 4s -> 8s -> 16s (5 retries)
//! let exponential = RetryConfig::exponential_default();
//!
//! // Custom exponential: 2s -> 4s -> 8s -> 16s -> 32s (with cap at 60s)
//! let custom_exp = RetryConfig::exponential(5, Duration::from_secs(2), Duration::from_secs(60));
//!
//! // Linear: 10s -> 10s -> 10s (3 retries)  
//! let linear = RetryConfig::linear(3, Duration::from_secs(10));
//!
//! // Custom delays: 1s -> 5s -> 30s
//! let custom = RetryConfig::custom(vec![
//!     Duration::from_secs(1),
//!     Duration::from_secs(5), 
//!     Duration::from_secs(30),
//! ]);
//!
//! // No retries
//! let no_retry = RetryConfig::no_retry();
//! ```

// Re-export main types for easy access
pub use connection::{Connection, ConnectionBuilder, ConnectionConfig};
pub use consumer::{Consumer, ConsumerBuilder, Message};
pub use error::{Result, RustRabbitError};
pub use publisher::{PublishOptions, Publisher};
pub use retry::{RetryConfig, RetryMechanism};

// Internal modules
mod connection;
mod consumer;
mod error;
mod publisher;
mod retry;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        Connection, Consumer, ConsumerBuilder, Message, PublishOptions, Publisher, Result,
        RetryConfig, RetryMechanism, RustRabbitError,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[tokio::test]
    async fn test_api_compilation() {
        // This test ensures the API compiles correctly
        // Real integration tests would require a RabbitMQ instance

        let _connection_result = Connection::new("amqp://localhost:5672").await;
        
        // Test retry configurations
        let _exponential = RetryConfig::exponential_default();
        let _linear = RetryConfig::linear(3, Duration::from_secs(5));
        let _custom = RetryConfig::custom(vec![Duration::from_secs(1), Duration::from_secs(5)]);
        let _no_retry = RetryConfig::no_retry();

        // Test publish options
        let _options = PublishOptions::new()
            .persistent(true)
            .priority(5)
            .ttl(Duration::from_secs(60))
            .header("source", "test");
    }

    #[test]
    fn test_retry_config_calculations() {
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(30));

        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4)));
        assert_eq!(config.calculate_delay(5), None); // Max retries exceeded
    }

    #[test]
    fn test_error_handling() {
        let error = RustRabbitError::Connection("test error".to_string());
        assert!(error.is_retryable());
        assert!(error.is_connection_error());
        assert!(!error.user_message().is_empty());
    }
}
//! # rust-rabbit
//!
//! A **simple, reliable** RabbitMQ client library for Rust.
//! Focus on core functionality with minimal configuration.
//!
//! ## Features
//!
//! - **Simple API**: Just Publisher and Consumer with essential methods
//! - **Flexible Retry**: Exponential, linear, or custom retry mechanisms  
//! - **Auto-Setup**: Automatic queue/exchange declaration and binding
//! - **Built-in Reliability**: Default ACK behavior with error handling
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
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
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
//!         .with_retry(RetryConfig::exponential_default()) // 1s->2s->4s->8s->16s
//!         .bind_to_exchange("orders", "order.*")
//!         .with_prefetch(5)
//!         .build();
//!     
//!     consumer.consume(|msg: rust_rabbit::MessageEnvelope<Order>| async move {
//!         println!("Processing order {}: ${}", msg.payload.id, msg.payload.amount);
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
//!
//! ## MessageEnvelope System
//!
//! For advanced retry tracking and error handling, use the MessageEnvelope system:
//!
//! ```rust,no_run
//! use rust_rabbit::{Connection, Publisher, Consumer, MessageEnvelope, RetryConfig};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct Order {
//!     id: u32,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection = Connection::new("amqp://localhost:5672").await?;
//!     
//!     // Publisher with envelope
//!     let publisher = Publisher::new(connection.clone());
//!     let order = Order { id: 123, amount: 99.99 };
//!     let envelope = MessageEnvelope::new(order, "order_queue")
//!         .with_max_retries(3);
//!     
//!     publisher.publish_envelope_to_queue("order_queue", &envelope, None).await?;
//!     
//!     // Consumer with envelope processing
//!     let consumer = Consumer::builder(connection, "order_queue")
//!         .with_retry(RetryConfig::exponential_default())
//!         .build();
//!     
//!     consumer.consume_envelopes(|envelope: MessageEnvelope<Order>| async move {
//!         println!("Processing order {} (attempt {})",
//!                  envelope.payload.id,
//!                  envelope.metadata.retry_attempt + 1);
//!         
//!         // Access retry metadata
//!         if !envelope.is_first_attempt() {
//!             println!("This is a retry. Last error: {:?}", envelope.last_error());
//!         }
//!         
//!         // Your business logic here
//!         Ok(())
//!     }).await?;
//!     
//!     Ok(())
//! }
//! ```

// Re-export main types for easy access
pub use connection::Connection;
pub use consumer::{Consumer, ConsumerBuilder};
pub use error::{Result, RustRabbitError};
pub use message::{
    ErrorRecord, ErrorType, MassTransitEnvelope, MessageEnvelope, MessageMetadata, MessageSource,
    WireMessage,
};
pub use publisher::{MassTransitOptions, PublishOptions, Publisher};
pub use retry::{DelayStrategy, RetryConfig, RetryMechanism};

// Internal modules
mod connection;
mod consumer;
mod error;
mod message;
mod publisher;
mod retry;

/// Initialize tracing with recommended defaults for rust-rabbit.
///
/// This sets up tracing with the following filters:
/// - `info` level for general application logs
/// - `warn` level for lapin (RabbitMQ client) to suppress spurious ERROR logs from io_loop
///
/// You can override the filter using the `RUST_LOG` environment variable.
///
/// # Example
///
/// ```rust,no_run
/// use rust_rabbit::init_tracing;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Initialize tracing with recommended settings
///     init_tracing();
///     
///     // Your application code
///     Ok(())
/// }
/// ```
///
/// # Custom Configuration
///
/// To use custom log levels, set the `RUST_LOG` environment variable:
///
/// ```bash
/// RUST_LOG=debug,lapin=warn cargo run
/// ```
#[cfg(feature = "tracing")]
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,lapin=warn"));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        Connection, Consumer, ConsumerBuilder, DelayStrategy, ErrorRecord, ErrorType,
        MassTransitEnvelope, MassTransitOptions, MessageEnvelope, MessageMetadata, MessageSource,
        PublishOptions, Publisher, Result, RetryConfig, RetryMechanism, RustRabbitError,
        WireMessage,
    };

    #[cfg(feature = "tracing")]
    pub use crate::init_tracing;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[allow(dead_code)]
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
    }

    #[test]
    fn test_basic_api_exists() {
        // Test that our main types exist and can be referenced
        use crate::prelude::*;

        // This is a compile-time test - if it compiles, our API is accessible
        let _: Option<Connection> = None;
        let _: Option<Publisher> = None;
        let _: Option<Consumer> = None;
        let _: Option<RetryConfig> = None;

        // Test that we can create basic configs
        let _retry = RetryConfig::exponential_default();
        let _options = PublishOptions::new();
    }

    #[test]
    fn test_retry_config_calculations() {
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(30));

        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4)));
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(8)));
        assert_eq!(config.calculate_delay(4), Some(Duration::from_secs(16)));
        // Attempt 5 exceeds max_retries (5), so should return None
        assert_eq!(config.calculate_delay(5), None);
    }

    #[test]
    fn test_retry_config_linear() {
        let config = RetryConfig::linear(3, Duration::from_secs(5));

        assert_eq!(config.max_retries, 3);
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(3), None); // Exceeds max_retries
    }

    #[test]
    fn test_retry_config_custom() {
        let delays = vec![
            Duration::from_secs(1),
            Duration::from_secs(3),
            Duration::from_secs(7),
        ];
        let config = RetryConfig::custom(delays.clone());

        assert_eq!(config.max_retries, 3);
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(3)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(7)));
        assert_eq!(config.calculate_delay(3), None); // Exceeds max_retries
    }

    #[test]
    fn test_publish_options() {
        let options = PublishOptions::new()
            .mandatory()
            .with_expiration("60000")
            .with_priority(5);

        assert!(options.mandatory);
        assert_eq!(options.expiration, Some("60000".to_string()));
        assert_eq!(options.priority, Some(5));
    }
}

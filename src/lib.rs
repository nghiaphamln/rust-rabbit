//! # rust-rabbit
//!
//! A small RabbitMQ client library with a narrow API.
//!
//! ## Main Types
//!
//! - `Connection`
//! - `Publisher`
//! - `Consumer`
//! - `RetryConfig`
//! - `MessageEnvelope`
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rust_rabbit::{Connection, Consumer, Publisher, RetryConfig};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Order {
//!     id: u32,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection = Connection::new("amqp://localhost:5672").await?;
//!     let publisher = Publisher::new(connection.clone());
//!
//!     publisher
//!         .publish_to_queue("orders", &Order { id: 1, amount: 10.0 }, None)
//!         .await?;
//!
//!     let consumer = Consumer::builder(connection, "orders")
//!         .with_retry(RetryConfig::exponential_default())
//!         .with_prefetch(5)
//!         .build();
//!
//!     consumer
//!         .consume(|order: Order| async move {
//!             println!("processing order {}", order.id);
//!             Ok(())
//!         })
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Retry
//!
//! ```rust
//! use rust_rabbit::{DelayStrategy, RetryConfig};
//! use std::time::Duration;
//!
//! let exponential = RetryConfig::exponential_default();
//! let linear = RetryConfig::linear(3, Duration::from_secs(5));
//! let custom = RetryConfig::custom(vec![Duration::from_secs(1), Duration::from_secs(10)]);
//! let delayed = RetryConfig::exponential_default()
//!     .with_delay_strategy(DelayStrategy::DelayedExchange);
//! ```
//!
//! ## Notes
//!
//! - `publish_to_queue()` declares a durable queue if needed.
//! - `publish_to_exchange()` declares a durable topic exchange.
//! - `consume()` accepts raw payloads and can detect MassTransit envelopes.
//! - `consume_envelopes()` works with `MessageEnvelope<T>`.
//! - `manual_ack()` is currently rejected at runtime because handlers do not receive an ack handle.

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
mod tests;

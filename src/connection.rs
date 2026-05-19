//! Simplified Connection Management for rust-rabbit
//!
//! Basic RabbitMQ connection handling without complex pooling or health monitoring.
//! Just simple, reliable connection management.

use crate::error::RustRabbitError;
use lapin::{Channel, Connection as LapinConnection, ConnectionProperties};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};
use url::Url;

/// Simple connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Connection URL (e.g., "amqp://user:pass@localhost:5672/vhost")
    pub url: String,

    /// Connection timeout in seconds
    pub connection_timeout: u64,

    /// Heartbeat interval in seconds (0 to disable)
    pub heartbeat: u64,
}

impl ConnectionConfig {
    /// Create a new connection config with URL
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            connection_timeout: 30,
            heartbeat: 60,
        }
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout_secs: u64) -> Self {
        self.connection_timeout = timeout_secs;
        self
    }

    /// Set heartbeat interval (0 to disable)
    pub fn heartbeat(mut self, heartbeat_secs: u64) -> Self {
        self.heartbeat = heartbeat_secs;
        self
    }
}

/// Simple RabbitMQ connection wrapper
#[derive(Debug)]
pub struct Connection {
    inner: Arc<RwLock<Option<LapinConnection>>>,
    reconnect_lock: Arc<Mutex<()>>,
    config: ConnectionConfig,
}

impl Connection {
    /// Create a new connection
    ///
    /// # Arguments
    /// * `url` - RabbitMQ connection URL (e.g., "amqp://localhost:5672")
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_rabbit::Connection;
    ///
    /// let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(url: &str) -> Result<Arc<Self>, RustRabbitError> {
        let config = ConnectionConfig::new(url);
        Self::with_config(config).await
    }

    /// Create a new connection with custom configuration
    pub async fn with_config(config: ConnectionConfig) -> Result<Arc<Self>, RustRabbitError> {
        let connection = Self {
            inner: Arc::new(RwLock::new(None)),
            reconnect_lock: Arc::new(Mutex::new(())),
            config,
        };

        let arc_connection = Arc::new(connection);
        arc_connection.connect().await?;

        Ok(arc_connection)
    }

    /// Get connection URL for debugging
    pub fn url(&self) -> &str {
        &self.config.url
    }

    /// Check if connection is active
    pub async fn is_connected(&self) -> bool {
        let conn_guard = self.inner.read().await;
        if let Some(ref conn) = *conn_guard {
            conn.status().connected()
        } else {
            false
        }
    }

    /// Create a new channel
    ///
    /// Automatically reconnects if the connection is lost.
    pub async fn create_channel(&self) -> Result<Channel, RustRabbitError> {
        self.ensure_connected().await?;

        let conn_guard = self.inner.read().await;
        if let Some(ref conn) = *conn_guard {
            let channel = conn.create_channel().await?;
            debug!("Created new channel");
            Ok(channel)
        } else {
            Err(RustRabbitError::Connection(
                "No active connection".to_string(),
            ))
        }
    }

    /// Manually reconnect
    pub async fn reconnect(&self) -> Result<(), RustRabbitError> {
        let _guard = self.reconnect_lock.lock().await;
        info!("Reconnecting to RabbitMQ...");
        self.connect().await
    }

    /// Close the connection
    pub async fn close(&self) -> Result<(), RustRabbitError> {
        let mut conn_guard = self.inner.write().await;
        if let Some(conn) = conn_guard.take() {
            conn.close(200, "Normal shutdown".into()).await?;
            info!("Connection closed");
        }
        Ok(())
    }

    /// Internal connection establishment
    async fn connect(&self) -> Result<(), RustRabbitError> {
        // Validate URL
        let _parsed_url = Url::parse(&self.config.url)
            .map_err(|e| RustRabbitError::Configuration(format!("Invalid URL: {}", e)))?;

        // Create connection properties (heartbeat removed - not available in this lapin version)
        let properties = ConnectionProperties::default();

        // Establish connection
        debug!("Connecting to RabbitMQ at {}", self.config.url);

        let connection = tokio::time::timeout(
            Duration::from_secs(self.config.connection_timeout),
            LapinConnection::connect(&self.config.url, properties),
        )
        .await
        .map_err(|_| {
            RustRabbitError::Connection(format!(
                "Connection attempt timed out after {} seconds",
                self.config.connection_timeout
            ))
        })??;

        info!("Successfully connected to RabbitMQ");

        // Store connection
        let mut conn_guard = self.inner.write().await;
        *conn_guard = Some(connection);

        Ok(())
    }

    async fn ensure_connected(&self) -> Result<(), RustRabbitError> {
        if self.is_connected().await {
            return Ok(());
        }

        let _guard = self.reconnect_lock.lock().await;
        if self.is_connected().await {
            return Ok(());
        }

        warn!("Connection lost, attempting to reconnect...");
        self.connect().await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Connection will be automatically closed when dropped
        debug!("Connection dropped");
    }
}

impl Connection {
    #[cfg(test)]
    pub(crate) fn disconnected_for_tests(url: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(RwLock::new(None)),
            reconnect_lock: Arc::new(Mutex::new(())),
            config: ConnectionConfig::new(url),
        })
    }
}

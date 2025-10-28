//! Simplified Connection Management for rust-rabbit
//! 
//! Basic RabbitMQ connection handling without complex pooling or health monitoring.
//! Just simple, reliable connection management.

use crate::error::RustRabbitError;
use lapin::{Channel, Connection as LapinConnection, ConnectionProperties};
use std::sync::Arc;
use tokio::sync::RwLock;
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
        // Check if we need to reconnect
        if !self.is_connected().await {
            warn!("Connection lost, attempting to reconnect...");
            self.reconnect().await?;
        }
        
        let conn_guard = self.inner.read().await;
        if let Some(ref conn) = *conn_guard {
            let channel = conn.create_channel().await?;
            debug!("Created new channel");
            Ok(channel)
        } else {
            Err(RustRabbitError::Connection("No active connection".to_string()))
        }
    }
    
    /// Manually reconnect
    pub async fn reconnect(&self) -> Result<(), RustRabbitError> {
        info!("Reconnecting to RabbitMQ...");
        self.connect().await
    }
    
    /// Close the connection
    pub async fn close(&self) -> Result<(), RustRabbitError> {
        let mut conn_guard = self.inner.write().await;
        if let Some(conn) = conn_guard.take() {
            conn.close(200, "Normal shutdown").await?;
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
        
        let connection = LapinConnection::connect(&self.config.url, properties).await?;
        
        info!("Successfully connected to RabbitMQ");
        
        // Store connection
        let mut conn_guard = self.inner.write().await;
        *conn_guard = Some(connection);
        
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Connection will be automatically closed when dropped
        debug!("Connection dropped");
    }
}

/// Connection builder for fluent configuration
pub struct ConnectionBuilder {
    config: ConnectionConfig,
}

impl ConnectionBuilder {
    /// Create a new connection builder
    pub fn new(url: &str) -> Self {
        Self {
            config: ConnectionConfig::new(url),
        }
    }
    
    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout_secs: u64) -> Self {
        self.config = self.config.connection_timeout(timeout_secs);
        self
    }
    
    /// Set heartbeat interval
    pub fn heartbeat(mut self, heartbeat_secs: u64) -> Self {
        self.config = self.config.heartbeat(heartbeat_secs);
        self
    }
    
    /// Build and connect
    pub async fn connect(self) -> Result<Arc<Connection>, RustRabbitError> {
        Connection::with_config(self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_connection_config() {
        let config = ConnectionConfig::new("amqp://localhost:5672")
            .connection_timeout(60)
            .heartbeat(30);
        
        assert_eq!(config.url, "amqp://localhost:5672");
        assert_eq!(config.connection_timeout, 60);
        assert_eq!(config.heartbeat, 30);
    }
    
    #[test]
    fn test_connection_builder() {
        let builder = ConnectionBuilder::new("amqp://localhost:5672")
            .connection_timeout(45)
            .heartbeat(20);
        
        assert_eq!(builder.config.url, "amqp://localhost:5672");
        assert_eq!(builder.config.connection_timeout, 45);
        assert_eq!(builder.config.heartbeat, 20);
    }
    
    #[test]
    fn test_invalid_url() {
        let result = std::panic::catch_unwind(|| {
            let _config = ConnectionConfig::new("invalid-url");
        });
        
        assert!(result.is_ok()); // Config creation should not panic, validation happens on connect
    }
}
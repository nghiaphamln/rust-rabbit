use crate::{
    config::RabbitConfig,
    error::{RabbitError, Result},
};
use lapin::{Channel, Connection as LapinConnection, ConnectionProperties};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

/// Connection wrapper with metadata
#[derive(Debug)]
pub struct Connection {
    inner: LapinConnection,
    created_at: Instant,
    last_used: Arc<RwLock<Instant>>,
}

impl Connection {
    pub fn new(connection: LapinConnection) -> Self {
        let now = Instant::now();
        Self {
            inner: connection,
            created_at: now,
            last_used: Arc::new(RwLock::new(now)),
        }
    }

    pub fn inner(&self) -> &LapinConnection {
        &self.inner
    }

    pub async fn create_channel(&self) -> Result<Channel> {
        let mut last_used = self.last_used.write().await;
        *last_used = Instant::now();
        Ok(self.inner.create_channel().await?)
    }

    pub fn is_connected(&self) -> bool {
        self.inner.status().connected()
    }

    pub async fn last_used(&self) -> Instant {
        *self.last_used.read().await
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}

/// Connection manager with pooling and health monitoring
#[derive(Debug, Clone)]
pub struct ConnectionManager {
    pub config: RabbitConfig,
    connections: Arc<RwLock<Vec<Arc<Connection>>>>,
    #[allow(dead_code)] // Will be used for connection tracking in future versions
    connection_counter: Arc<Mutex<usize>>,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub async fn new(config: RabbitConfig) -> Result<Self> {
        let manager = Self {
            config,
            connections: Arc::new(RwLock::new(Vec::new())),
            connection_counter: Arc::new(Mutex::new(0)),
        };

        // Initialize minimum connections
        manager.ensure_min_connections().await?;

        // Start background health monitoring
        if manager.config.health_check.enabled {
            manager.start_health_monitoring().await;
        }

        Ok(manager)
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<Arc<Connection>> {
        let connections = self.connections.read().await;

        // Find a healthy connection
        for conn in connections.iter() {
            if conn.is_connected() {
                return Ok(conn.clone());
            }
        }

        // No healthy connections found, drop the read lock and create new one
        drop(connections);

        self.create_new_connection().await
    }

    /// Create a new connection with retry mechanism
    async fn create_new_connection(&self) -> Result<Arc<Connection>> {
        let mut retry_count = 0;
        let mut delay = self.config.retry_config.initial_delay;

        loop {
            match self.establish_connection().await {
                Ok(connection) => {
                    let conn = Arc::new(Connection::new(connection));

                    // Add to pool if not at max capacity
                    let mut connections = self.connections.write().await;
                    if connections.len() < self.config.pool_config.max_connections {
                        connections.push(conn.clone());
                    }

                    info!("Successfully established new RabbitMQ connection");
                    return Ok(conn);
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count > self.config.retry_config.max_retries {
                        error!(
                            "Failed to establish connection after {} retries: {}",
                            retry_count, e
                        );
                        return Err(RabbitError::RetryExhausted(format!(
                            "Connection failed after {} retries",
                            retry_count
                        )));
                    }

                    warn!(
                        "Connection attempt {} failed: {}. Retrying in {:?}",
                        retry_count, e, delay
                    );

                    sleep(delay).await;
                    delay = self.calculate_next_delay(delay);
                }
            }
        }
    }

    /// Establish a raw connection to RabbitMQ
    async fn establish_connection(&self) -> Result<LapinConnection> {
        let connection_future = LapinConnection::connect(
            &self.config.connection_string,
            ConnectionProperties::default(),
        );

        if let Some(timeout_duration) = self.config.connection_timeout {
            timeout(timeout_duration, connection_future)
                .await
                .map_err(|_| RabbitError::Timeout("Connection timeout".to_string()))?
                .map_err(RabbitError::Connection)
        } else {
            connection_future.await.map_err(RabbitError::Connection)
        }
    }

    /// Calculate the next retry delay with exponential backoff and jitter
    fn calculate_next_delay(&self, current_delay: Duration) -> Duration {
        let base_delay = Duration::from_millis(
            (current_delay.as_millis() as f64 * self.config.retry_config.backoff_multiplier) as u64,
        );

        let max_delay = self.config.retry_config.max_delay;
        let delay = if base_delay > max_delay {
            max_delay
        } else {
            base_delay
        };

        // Add jitter
        if self.config.retry_config.jitter > 0.0 {
            let jitter_amount = (delay.as_millis() as f64 * self.config.retry_config.jitter) as u64;
            let jitter = fastrand::u64(0..=jitter_amount);
            Duration::from_millis(delay.as_millis() as u64 + jitter)
        } else {
            delay
        }
    }

    /// Ensure minimum number of connections are available
    async fn ensure_min_connections(&self) -> Result<()> {
        let connections = self.connections.read().await;
        let healthy_count = connections.iter().filter(|c| c.is_connected()).count();

        if healthy_count >= self.config.pool_config.min_connections {
            return Ok(());
        }

        drop(connections);

        let needed = self.config.pool_config.min_connections - healthy_count;
        debug!(
            "Creating {} connections to meet minimum requirement",
            needed
        );

        for _ in 0..needed {
            if let Err(e) = self.create_new_connection().await {
                warn!("Failed to create minimum connection: {}", e);
            }
        }

        Ok(())
    }

    /// Start background health monitoring
    async fn start_health_monitoring(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager.config.health_check.check_interval);

            loop {
                interval.tick().await;
                manager.perform_health_check().await;
            }
        });
    }

    /// Perform health check on all connections
    async fn perform_health_check(&self) {
        let mut connections = self.connections.write().await;
        let mut unhealthy_indices = Vec::new();

        for (i, conn) in connections.iter().enumerate() {
            if !conn.is_connected() {
                debug!("Connection {} is unhealthy, marking for removal", i);
                unhealthy_indices.push(i);
            }
        }

        // Remove unhealthy connections (in reverse order to maintain indices)
        for &i in unhealthy_indices.iter().rev() {
            connections.remove(i);
        }

        if !unhealthy_indices.is_empty() {
            info!("Removed {} unhealthy connections", unhealthy_indices.len());
        }

        drop(connections);

        // Ensure we have minimum connections
        if let Err(e) = self.ensure_min_connections().await {
            warn!(
                "Failed to ensure minimum connections during health check: {}",
                e
            );
        }
    }

    /// Get connection statistics
    pub async fn get_stats(&self) -> ConnectionStats {
        let connections = self.connections.read().await;
        let total = connections.len();
        let healthy = connections.iter().filter(|c| c.is_connected()).count();

        ConnectionStats {
            total_connections: total,
            healthy_connections: healthy,
            unhealthy_connections: total - healthy,
        }
    }

    /// Close all connections
    pub async fn close(&self) -> Result<()> {
        let mut connections = self.connections.write().await;

        for conn in connections.drain(..) {
            if let Err(e) = conn.inner().close(0, "Shutdown").await {
                warn!("Error closing connection: {}", e);
            }
        }

        info!("All connections closed");
        Ok(())
    }
}

/// Connection pool statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionStats {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub unhealthy_connections: usize,
}

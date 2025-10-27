use crate::{
    config::HealthCheckConfig,
    connection::{ConnectionManager, ConnectionStats},
    error::{RabbitError, Result},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

/// Connection status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Connection is healthy and operational
    Healthy,
    /// Connection is degraded but still functional
    Degraded,
    /// Connection is unhealthy and may not work properly
    Unhealthy,
    /// Connection is completely down
    Down,
}

impl ConnectionStatus {
    /// Check if the status indicates a healthy connection
    pub fn is_healthy(&self) -> bool {
        matches!(self, ConnectionStatus::Healthy)
    }

    /// Check if the status indicates an operational connection
    pub fn is_operational(&self) -> bool {
        matches!(self, ConnectionStatus::Healthy | ConnectionStatus::Degraded)
    }
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall connection status
    pub status: ConnectionStatus,
    /// Timestamp of the health check
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Connection statistics
    pub connection_stats: ConnectionStats,
    /// Response time for the health check
    pub response_time: Duration,
    /// Details about the health check
    pub details: String,
    /// Any errors encountered during the health check
    pub errors: Vec<String>,
}

/// Health checker for monitoring RabbitMQ connection status
#[derive(Debug, Clone)]
pub struct HealthChecker {
    connection_manager: ConnectionManager,
    config: HealthCheckConfig,
    last_result: Arc<RwLock<Option<HealthCheckResult>>>,
    monitoring_started: Arc<RwLock<bool>>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(connection_manager: ConnectionManager) -> Self {
        let config = connection_manager.config.health_check.clone();

        Self {
            connection_manager,
            config,
            last_result: Arc::new(RwLock::new(None)),
            monitoring_started: Arc::new(RwLock::new(false)),
        }
    }

    /// Start continuous health monitoring in the background
    pub async fn start_monitoring(&self) -> Result<()> {
        let mut started = self.monitoring_started.write().await;
        if *started {
            warn!("Health monitoring is already started");
            return Ok(());
        }
        *started = true;
        drop(started);

        if !self.config.enabled {
            info!("Health monitoring is disabled in configuration");
            return Ok(());
        }

        let checker = self.clone();
        tokio::spawn(async move {
            checker.monitoring_loop().await;
        });

        info!(
            "Health monitoring started with interval: {:?}",
            self.config.check_interval
        );
        Ok(())
    }

    /// Stop health monitoring
    pub async fn stop_monitoring(&self) {
        let mut started = self.monitoring_started.write().await;
        *started = false;
        info!("Health monitoring stopped");
    }

    /// Perform a single health check
    pub async fn check_health(&self) -> Result<HealthCheckResult> {
        let start_time = Instant::now();
        let mut errors = Vec::new();
        let mut status = ConnectionStatus::Healthy;
        let mut details = String::new();

        // Get connection statistics
        let connection_stats = self.connection_manager.get_stats().await;

        // Check if we have any healthy connections
        if connection_stats.healthy_connections == 0 {
            status = ConnectionStatus::Down;
            errors.push("No healthy connections available".to_string());
            details.push_str("All connections are down. ");
        } else if connection_stats.unhealthy_connections > 0 {
            // We have some healthy connections but also some unhealthy ones
            let unhealthy_ratio = connection_stats.unhealthy_connections as f64
                / connection_stats.total_connections as f64;

            if unhealthy_ratio > 0.5 {
                status = ConnectionStatus::Degraded;
                details.push_str(&format!(
                    "More than 50% of connections are unhealthy ({}/{}). ",
                    connection_stats.unhealthy_connections, connection_stats.total_connections
                ));
            } else {
                status = ConnectionStatus::Healthy;
                details.push_str(&format!(
                    "Some connections are unhealthy ({}/{}). ",
                    connection_stats.unhealthy_connections, connection_stats.total_connections
                ));
            }
        }

        // Try to get a connection and perform a basic operation
        match tokio::time::timeout(self.config.check_timeout, self.test_connection_operation())
            .await
        {
            Ok(Ok(_)) => {
                if status == ConnectionStatus::Healthy {
                    details.push_str("Connection test successful. ");
                }
            }
            Ok(Err(e)) => {
                status = ConnectionStatus::Unhealthy;
                errors.push(format!("Connection test failed: {}", e));
                details.push_str("Failed to perform connection test. ");
            }
            Err(_) => {
                status = ConnectionStatus::Unhealthy;
                errors.push("Connection test timed out".to_string());
                details.push_str("Connection test timed out. ");
            }
        }

        let response_time = start_time.elapsed();

        // Additional checks based on response time
        if response_time > Duration::from_secs(5) {
            if status == ConnectionStatus::Healthy {
                status = ConnectionStatus::Degraded;
            }
            details.push_str("Slow response time detected. ");
        }

        let result = HealthCheckResult {
            status,
            timestamp: chrono::Utc::now(),
            connection_stats,
            response_time,
            details: details.trim().to_string(),
            errors,
        };

        // Store the result
        let mut last_result = self.last_result.write().await;
        *last_result = Some(result.clone());

        debug!(
            "Health check completed: {:?} in {:?}",
            result.status, result.response_time
        );
        Ok(result)
    }

    /// Get the last health check result
    pub async fn get_last_result(&self) -> Option<HealthCheckResult> {
        self.last_result.read().await.clone()
    }

    /// Check if the connection is currently healthy
    pub async fn is_healthy(&self) -> bool {
        match self.get_last_result().await {
            Some(result) => result.status.is_healthy(),
            None => {
                // No previous health check, perform one now
                match self.check_health().await {
                    Ok(result) => result.status.is_healthy(),
                    Err(_) => false,
                }
            }
        }
    }

    /// Check if the connection is operational (healthy or degraded)
    pub async fn is_operational(&self) -> bool {
        match self.get_last_result().await {
            Some(result) => result.status.is_operational(),
            None => {
                // No previous health check, perform one now
                match self.check_health().await {
                    Ok(result) => result.status.is_operational(),
                    Err(_) => false,
                }
            }
        }
    }

    /// Wait for the connection to become healthy
    pub async fn wait_for_healthy(&self, timeout: Option<Duration>) -> Result<()> {
        let start = Instant::now();
        let timeout_duration = timeout.unwrap_or(Duration::from_secs(60));

        loop {
            if self.is_healthy().await {
                return Ok(());
            }

            if start.elapsed() > timeout_duration {
                return Err(RabbitError::HealthCheck(
                    "Timeout waiting for healthy connection".to_string(),
                ));
            }

            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Get health status summary
    pub async fn get_health_summary(&self) -> HealthSummary {
        let last_result = self.get_last_result().await;
        let connection_stats = self.connection_manager.get_stats().await;

        HealthSummary {
            status: last_result
                .as_ref()
                .map(|r| r.status)
                .unwrap_or(ConnectionStatus::Down),
            last_check: last_result.as_ref().map(|r| r.timestamp),
            total_connections: connection_stats.total_connections,
            healthy_connections: connection_stats.healthy_connections,
            unhealthy_connections: connection_stats.unhealthy_connections,
            monitoring_enabled: self.config.enabled,
            check_interval: self.config.check_interval,
        }
    }

    /// Internal monitoring loop
    async fn monitoring_loop(&self) {
        let mut interval = interval(self.config.check_interval);

        loop {
            // Check if monitoring should continue
            {
                let started = self.monitoring_started.read().await;
                if !*started {
                    break;
                }
            }

            interval.tick().await;

            if let Err(e) = self.check_health().await {
                error!("Health check failed: {}", e);
            }
        }

        info!("Health monitoring loop ended");
    }

    /// Test basic connection operation
    async fn test_connection_operation(&self) -> Result<()> {
        let connection = self.connection_manager.get_connection().await?;
        let channel = connection.create_channel().await?;

        // Perform a simple operation to test the connection
        // We'll declare a temporary queue and then delete it
        let test_queue_name = format!("health-check-{}", uuid::Uuid::new_v4());

        channel
            .queue_declare(
                &test_queue_name,
                lapin::options::QueueDeclareOptions {
                    passive: false,
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    nowait: false,
                },
                lapin::types::FieldTable::default(),
            )
            .await?;

        // Delete the test queue
        channel
            .queue_delete(
                &test_queue_name,
                lapin::options::QueueDeleteOptions {
                    if_unused: false,
                    if_empty: false,
                    nowait: false,
                },
            )
            .await?;

        Ok(())
    }
}

/// Health summary information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    /// Current connection status
    pub status: ConnectionStatus,
    /// Timestamp of last health check
    pub last_check: Option<chrono::DateTime<chrono::Utc>>,
    /// Total number of connections
    pub total_connections: usize,
    /// Number of healthy connections
    pub healthy_connections: usize,
    /// Number of unhealthy connections
    pub unhealthy_connections: usize,
    /// Whether monitoring is enabled
    pub monitoring_enabled: bool,
    /// Health check interval
    pub check_interval: Duration,
}

/// Health metrics for monitoring systems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    /// Connection uptime percentage
    pub uptime_percentage: f64,
    /// Average response time
    pub average_response_time: Duration,
    /// Number of failed health checks in the last hour
    pub failed_checks_last_hour: u32,
    /// Number of successful health checks in the last hour
    pub successful_checks_last_hour: u32,
    /// Last error message
    pub last_error: Option<String>,
}

// Extension trait for adding health check configuration
pub trait HealthCheckConfigExt {
    /// Create a conservative health check configuration
    fn conservative() -> HealthCheckConfig;

    /// Create an aggressive health check configuration
    fn aggressive() -> HealthCheckConfig;

    /// Create a minimal health check configuration
    fn minimal() -> HealthCheckConfig;
}

impl HealthCheckConfigExt for HealthCheckConfig {
    fn conservative() -> HealthCheckConfig {
        HealthCheckConfig {
            check_interval: Duration::from_secs(60),
            check_timeout: Duration::from_secs(10),
            enabled: true,
        }
    }

    fn aggressive() -> HealthCheckConfig {
        HealthCheckConfig {
            check_interval: Duration::from_secs(10),
            check_timeout: Duration::from_secs(3),
            enabled: true,
        }
    }

    fn minimal() -> HealthCheckConfig {
        HealthCheckConfig {
            check_interval: Duration::from_secs(300), // 5 minutes
            check_timeout: Duration::from_secs(15),
            enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_status() {
        assert!(ConnectionStatus::Healthy.is_healthy());
        assert!(ConnectionStatus::Healthy.is_operational());

        assert!(!ConnectionStatus::Degraded.is_healthy());
        assert!(ConnectionStatus::Degraded.is_operational());

        assert!(!ConnectionStatus::Unhealthy.is_healthy());
        assert!(!ConnectionStatus::Unhealthy.is_operational());

        assert!(!ConnectionStatus::Down.is_healthy());
        assert!(!ConnectionStatus::Down.is_operational());
    }

    #[test]
    fn test_health_check_config_presets() {
        let conservative = HealthCheckConfig::conservative();
        assert_eq!(conservative.check_interval, Duration::from_secs(60));
        assert_eq!(conservative.check_timeout, Duration::from_secs(10));
        assert!(conservative.enabled);

        let aggressive = HealthCheckConfig::aggressive();
        assert_eq!(aggressive.check_interval, Duration::from_secs(10));
        assert_eq!(aggressive.check_timeout, Duration::from_secs(3));
        assert!(aggressive.enabled);

        let minimal = HealthCheckConfig::minimal();
        assert_eq!(minimal.check_interval, Duration::from_secs(300));
        assert_eq!(minimal.check_timeout, Duration::from_secs(15));
        assert!(minimal.enabled);
    }
}

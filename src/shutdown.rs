//! Graceful shutdown handling for RustRabbit
//!
//! This module provides mechanisms for cleanly shutting down all RustRabbit components:
//! - Connection pool cleanup
//! - Consumer graceful stopping
//! - Batch flushing before exit
//! - Health monitoring shutdown

use crate::error::{RabbitError, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

/// Shutdown signal types
#[derive(Debug, Clone)]
pub enum ShutdownSignal {
    /// Graceful shutdown requested
    Graceful,
    /// Immediate shutdown requested
    Immediate,
    /// Shutdown due to error
    Error(String),
}

/// Shutdown configuration
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Maximum time to wait for graceful shutdown
    pub graceful_timeout: Duration,
    /// Time to wait between shutdown phases
    pub phase_delay: Duration,
    /// Whether to flush pending messages during shutdown
    pub flush_pending: bool,
    /// Maximum time to wait for pending operations
    pub pending_timeout: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            graceful_timeout: Duration::from_secs(30),
            phase_delay: Duration::from_millis(100),
            flush_pending: true,
            pending_timeout: Duration::from_secs(5),
        }
    }
}

/// Shutdown manager for coordinating graceful shutdown
pub struct ShutdownManager {
    config: ShutdownConfig,
    shutdown_sender: broadcast::Sender<ShutdownSignal>,
    #[allow(dead_code)]
    shutdown_receiver: Arc<Mutex<broadcast::Receiver<ShutdownSignal>>>,
    shutdown_in_progress: Arc<RwLock<bool>>,
    registered_components: Arc<Mutex<Vec<Arc<dyn ShutdownHandler>>>>,
}

impl ShutdownManager {
    /// Create a new shutdown manager
    pub fn new(config: ShutdownConfig) -> Self {
        let (sender, receiver) = broadcast::channel(100);

        Self {
            config,
            shutdown_sender: sender,
            shutdown_receiver: Arc::new(Mutex::new(receiver)),
            shutdown_in_progress: Arc::new(RwLock::new(false)),
            registered_components: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a component for shutdown notifications
    pub async fn register_component(&self, component: Arc<dyn ShutdownHandler>) {
        let mut components = self.registered_components.lock().await;
        components.push(component);
        debug!("Registered component for shutdown handling");
    }

    /// Get a shutdown signal receiver
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_sender.subscribe()
    }

    /// Initiate graceful shutdown
    pub async fn shutdown(&self, signal: ShutdownSignal) -> Result<()> {
        let mut shutdown_in_progress = self.shutdown_in_progress.write().await;
        if *shutdown_in_progress {
            debug!("Shutdown already in progress, ignoring duplicate signal");
            return Ok(());
        }

        *shutdown_in_progress = true;
        drop(shutdown_in_progress); // Release the lock

        info!("🛑 Initiating shutdown: {:?}", signal);

        // Send shutdown signal to all subscribers
        if let Err(e) = self.shutdown_sender.send(signal.clone()) {
            warn!("Failed to send shutdown signal: {}", e);
        }

        // Execute shutdown based on signal type
        match signal {
            ShutdownSignal::Graceful => self.execute_graceful_shutdown().await,
            ShutdownSignal::Immediate => self.execute_immediate_shutdown().await,
            ShutdownSignal::Error(ref msg) => {
                error!("Shutdown due to error: {}", msg);
                self.execute_immediate_shutdown().await
            }
        }
    }

    /// Check if shutdown is in progress
    pub async fn is_shutdown_in_progress(&self) -> bool {
        *self.shutdown_in_progress.read().await
    }

    /// Execute graceful shutdown sequence
    async fn execute_graceful_shutdown(&self) -> Result<()> {
        info!("🔄 Starting graceful shutdown sequence");

        let components = self.registered_components.lock().await.clone();

        // Phase 1: Prepare for shutdown
        info!("📋 Phase 1: Preparing components for shutdown");
        for (i, component) in components.iter().enumerate() {
            debug!("Preparing component {} for shutdown", i);

            if let Err(e) = timeout(self.config.pending_timeout, component.prepare_shutdown()).await
            {
                warn!("Component {} prepare_shutdown timed out: {:?}", i, e);
            } else if let Err(e) = component.prepare_shutdown().await {
                warn!("Component {} prepare_shutdown failed: {}", i, e);
            }
        }

        sleep(self.config.phase_delay).await;

        // Phase 2: Stop accepting new work
        info!("⏹️ Phase 2: Stopping new work acceptance");
        for (i, component) in components.iter().enumerate() {
            debug!("Stopping component {} from accepting new work", i);

            if let Err(e) =
                timeout(self.config.pending_timeout, component.stop_accepting_work()).await
            {
                warn!("Component {} stop_accepting_work timed out: {:?}", i, e);
            } else if let Err(e) = component.stop_accepting_work().await {
                warn!("Component {} stop_accepting_work failed: {}", i, e);
            }
        }

        sleep(self.config.phase_delay).await;

        // Phase 3: Wait for pending work to complete
        info!("⏳ Phase 3: Waiting for pending work to complete");
        for (i, component) in components.iter().enumerate() {
            debug!("Waiting for component {} pending work", i);

            if let Err(e) =
                timeout(self.config.pending_timeout, component.wait_for_completion()).await
            {
                warn!("Component {} wait_for_completion timed out: {:?}", i, e);
            } else if let Err(e) = component.wait_for_completion().await {
                warn!("Component {} wait_for_completion failed: {}", i, e);
            }
        }

        sleep(self.config.phase_delay).await;

        // Phase 4: Final cleanup
        info!("🧹 Phase 4: Final cleanup");
        for (i, component) in components.iter().enumerate() {
            debug!("Performing final cleanup for component {}", i);

            if let Err(e) = timeout(self.config.pending_timeout, component.cleanup()).await {
                warn!("Component {} cleanup timed out: {:?}", i, e);
            } else if let Err(e) = component.cleanup().await {
                warn!("Component {} cleanup failed: {}", i, e);
            }
        }

        info!("✅ Graceful shutdown completed successfully");
        Ok(())
    }

    /// Execute immediate shutdown sequence
    async fn execute_immediate_shutdown(&self) -> Result<()> {
        info!("⚡ Starting immediate shutdown sequence");

        let components = self.registered_components.lock().await.clone();

        // Force immediate cleanup of all components
        for (i, component) in components.iter().enumerate() {
            debug!("Force cleaning up component {}", i);

            if let Err(e) = timeout(Duration::from_secs(2), component.force_shutdown()).await {
                error!("Component {} force_shutdown timed out: {:?}", i, e);
            } else if let Err(e) = component.force_shutdown().await {
                error!("Component {} force_shutdown failed: {}", i, e);
            }
        }

        info!("⚡ Immediate shutdown completed");
        Ok(())
    }
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new(ShutdownConfig::default())
    }
}

/// Trait for components that need shutdown handling
#[async_trait::async_trait]
pub trait ShutdownHandler: Send + Sync {
    /// Prepare for shutdown (e.g., set shutdown flags)
    async fn prepare_shutdown(&self) -> Result<()> {
        Ok(())
    }

    /// Stop accepting new work
    async fn stop_accepting_work(&self) -> Result<()> {
        Ok(())
    }

    /// Wait for pending work to complete
    async fn wait_for_completion(&self) -> Result<()> {
        Ok(())
    }

    /// Perform final cleanup
    async fn cleanup(&self) -> Result<()> {
        Ok(())
    }

    /// Force immediate shutdown (emergency)
    async fn force_shutdown(&self) -> Result<()> {
        self.cleanup().await
    }
}

/// CTRL+C signal handler setup
pub async fn setup_signal_handling(shutdown_manager: Arc<ShutdownManager>) -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).map_err(|e| {
            RabbitError::Configuration(format!("Failed to setup SIGTERM handler: {}", e))
        })?;
        let mut sigint = signal(SignalKind::interrupt()).map_err(|e| {
            RabbitError::Configuration(format!("Failed to setup SIGINT handler: {}", e))
        })?;

        let shutdown_manager_clone = shutdown_manager.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown");
                    if let Err(e) = shutdown_manager_clone.shutdown(ShutdownSignal::Graceful).await {
                        error!("Failed to execute graceful shutdown: {}", e);
                    }
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                    if let Err(e) = shutdown_manager_clone.shutdown(ShutdownSignal::Graceful).await {
                        error!("Failed to execute graceful shutdown: {}", e);
                    }
                }
            }
        });
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows;

        let mut ctrl_c = windows::ctrl_c().map_err(|e| {
            RabbitError::Configuration(format!("Failed to setup Ctrl+C handler: {}", e))
        })?;
        let mut ctrl_break = windows::ctrl_break().map_err(|e| {
            RabbitError::Configuration(format!("Failed to setup Ctrl+Break handler: {}", e))
        })?;
        let mut ctrl_close = windows::ctrl_close().map_err(|e| {
            RabbitError::Configuration(format!("Failed to setup Ctrl+Close handler: {}", e))
        })?;

        let shutdown_manager_clone = shutdown_manager.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = ctrl_c.recv() => {
                    info!("Received Ctrl+C, initiating graceful shutdown");
                    if let Err(e) = shutdown_manager_clone.shutdown(ShutdownSignal::Graceful).await {
                        error!("Failed to execute graceful shutdown: {}", e);
                    }
                }
                _ = ctrl_break.recv() => {
                    info!("Received Ctrl+Break, initiating immediate shutdown");
                    if let Err(e) = shutdown_manager_clone.shutdown(ShutdownSignal::Immediate).await {
                        error!("Failed to execute immediate shutdown: {}", e);
                    }
                }
                _ = ctrl_close.recv() => {
                    info!("Received close signal, initiating graceful shutdown");
                    if let Err(e) = shutdown_manager_clone.shutdown(ShutdownSignal::Graceful).await {
                        error!("Failed to execute graceful shutdown: {}", e);
                    }
                }
            }
        });
    }

    info!("📡 Signal handlers setup complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct MockComponent {
        shutdown_called: Arc<AtomicBool>,
    }

    impl MockComponent {
        fn new() -> Self {
            Self {
                shutdown_called: Arc::new(AtomicBool::new(false)),
            }
        }

        fn was_shutdown_called(&self) -> bool {
            self.shutdown_called.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl ShutdownHandler for MockComponent {
        async fn cleanup(&self) -> Result<()> {
            self.shutdown_called.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_shutdown_manager_creation() {
        let config = ShutdownConfig::default();
        let manager = ShutdownManager::new(config);

        assert!(!manager.is_shutdown_in_progress().await);
    }

    #[tokio::test]
    async fn test_component_registration() {
        let manager = ShutdownManager::default();
        let component = Arc::new(MockComponent::new());

        manager.register_component(component.clone()).await;

        // Trigger shutdown
        let _ = manager.shutdown(ShutdownSignal::Graceful).await;

        // Give some time for shutdown to complete
        sleep(Duration::from_millis(100)).await;

        assert!(component.was_shutdown_called());
    }

    #[tokio::test]
    async fn test_shutdown_signal_subscription() {
        let manager = ShutdownManager::default();
        let mut receiver = manager.subscribe();

        // Send shutdown signal
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            let _ = manager.shutdown(ShutdownSignal::Graceful).await;
        });

        // Receive the signal
        let signal = receiver.recv().await.unwrap();
        matches!(signal, ShutdownSignal::Graceful);
    }
}

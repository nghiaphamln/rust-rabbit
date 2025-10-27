//! Circuit breaker implementation for connection resilience
//!
//! This module provides a circuit breaker pattern to handle connection failures gracefully.
//! When a connection repeatedly fails, the circuit breaker will "open" and prevent further
//! connection attempts for a configured period, allowing the system to recover.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Circuit breaker states
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    /// Normal operation - requests are allowed through
    Closed,
    /// Circuit is open - requests are blocked for a timeout period
    Open,
    /// Limited requests are allowed to test if the service has recovered
    HalfOpen,
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Time window for counting failures (sliding window)
    pub failure_window: Duration,
    /// How long to wait before attempting recovery (half-open state)
    pub recovery_timeout: Duration,
    /// Number of successful requests needed in half-open state to close circuit
    pub success_threshold: u32,
    /// Maximum number of requests allowed in half-open state
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 3,
            half_open_max_requests: 5,
        }
    }
}

/// Circuit breaker for managing connection failures
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    half_open_requests: AtomicU64,
}

#[derive(Debug)]
struct CircuitBreakerState {
    current_state: CircuitState,
    last_failure_time: Option<Instant>,
    last_state_change: Instant,
    failure_times: Vec<Instant>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        let state = CircuitBreakerState {
            current_state: CircuitState::Closed,
            last_failure_time: None,
            last_state_change: Instant::now(),
            failure_times: Vec::new(),
        };

        Self {
            config,
            state: Arc::new(RwLock::new(state)),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            half_open_requests: AtomicU64::new(0),
        }
    }

    /// Check if a request should be allowed through the circuit breaker
    pub async fn allow_request(&self) -> bool {
        let mut state = self.state.write().await;
        let now = Instant::now();

        // Clean up old failure times outside the window
        state
            .failure_times
            .retain(|&time| now.duration_since(time) <= self.config.failure_window);

        match state.current_state {
            CircuitState::Closed => {
                // Normal operation - allow all requests
                true
            }
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last_failure) = state.last_failure_time {
                    if now.duration_since(last_failure) >= self.config.recovery_timeout {
                        debug!("Circuit breaker transitioning from Open to HalfOpen");
                        state.current_state = CircuitState::HalfOpen;
                        state.last_state_change = now;
                        self.half_open_requests.store(0, Ordering::Relaxed);
                        self.success_count.store(0, Ordering::Relaxed);
                        true
                    } else {
                        // Still in timeout period
                        false
                    }
                } else {
                    // No last failure time, transition to half-open
                    state.current_state = CircuitState::HalfOpen;
                    state.last_state_change = now;
                    true
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests to test recovery
                let current_requests = self.half_open_requests.load(Ordering::Relaxed);
                if current_requests < self.config.half_open_max_requests as u64 {
                    self.half_open_requests.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful operation
    pub async fn record_success(&self) {
        let mut state = self.state.write().await;

        match state.current_state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
                state.failure_times.clear();
            }
            CircuitState::HalfOpen => {
                let success_count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

                if success_count >= self.config.success_threshold as u64 {
                    info!(
                        "Circuit breaker transitioning from HalfOpen to Closed after {} successes",
                        success_count
                    );
                    state.current_state = CircuitState::Closed;
                    state.last_state_change = Instant::now();
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                    self.half_open_requests.store(0, Ordering::Relaxed);
                    state.failure_times.clear();
                }
            }
            CircuitState::Open => {
                // Should not happen, but reset if it does
                debug!("Unexpected success in Open state, resetting circuit breaker");
                state.current_state = CircuitState::Closed;
                state.last_state_change = Instant::now();
                self.failure_count.store(0, Ordering::Relaxed);
                state.failure_times.clear();
            }
        }
    }

    /// Record a failed operation
    pub async fn record_failure(&self) {
        let mut state = self.state.write().await;
        let now = Instant::now();

        state.failure_times.push(now);
        state.last_failure_time = Some(now);

        // Clean up old failures outside the window
        state
            .failure_times
            .retain(|&time| now.duration_since(time) <= self.config.failure_window);

        let failure_count = state.failure_times.len() as u32;

        match state.current_state {
            CircuitState::Closed => {
                if failure_count >= self.config.failure_threshold {
                    warn!(
                        "Circuit breaker opening due to {} failures in {:?}",
                        failure_count, self.config.failure_window
                    );
                    state.current_state = CircuitState::Open;
                    state.last_state_change = now;
                }
            }
            CircuitState::HalfOpen => {
                warn!("Circuit breaker transitioning from HalfOpen to Open due to failure");
                state.current_state = CircuitState::Open;
                state.last_state_change = now;
                self.success_count.store(0, Ordering::Relaxed);
                self.half_open_requests.store(0, Ordering::Relaxed);
            }
            CircuitState::Open => {
                // Already open, just update the failure time
            }
        }
    }

    /// Get current circuit breaker state
    pub async fn get_state(&self) -> CircuitState {
        let state = self.state.read().await;
        state.current_state.clone()
    }

    /// Get circuit breaker statistics
    pub async fn get_stats(&self) -> CircuitBreakerStats {
        let state = self.state.read().await;
        CircuitBreakerStats {
            current_state: state.current_state.clone(),
            failure_count: state.failure_times.len() as u32,
            success_count: self.success_count.load(Ordering::Relaxed) as u32,
            half_open_requests: self.half_open_requests.load(Ordering::Relaxed) as u32,
            time_since_last_state_change: Instant::now().duration_since(state.last_state_change),
            last_failure_time: state.last_failure_time,
        }
    }

    /// Force the circuit breaker to a specific state (for testing)
    #[cfg(test)]
    pub async fn force_state(&self, new_state: CircuitState) {
        let mut state = self.state.write().await;
        state.current_state = new_state;
        state.last_state_change = Instant::now();
    }
}

/// Statistics about circuit breaker state
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub current_state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub half_open_requests: u32,
    pub time_since_last_state_change: Duration,
    pub last_failure_time: Option<Instant>,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_circuit_breaker_closed_state() {
        let cb = CircuitBreaker::new();

        // Should allow requests in closed state
        assert!(cb.allow_request().await);

        // Record success should keep it closed
        cb.record_success().await;
        assert_eq!(cb.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_secs(10),
            success_threshold: 2,
            half_open_max_requests: 3,
        };

        let cb = CircuitBreaker::with_config(config);

        // Record failures to trigger opening
        for i in 0..3 {
            cb.record_failure().await;
            if i < 2 {
                assert_eq!(cb.get_state().await, CircuitState::Closed);
            }
        }

        // Should be open now
        assert_eq!(cb.get_state().await, CircuitState::Open);
        assert!(!cb.allow_request().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_transition() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_millis(50),
            success_threshold: 2,
            half_open_max_requests: 3,
        };

        let cb = CircuitBreaker::with_config(config);

        // Open the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.get_state().await, CircuitState::Open);

        // Wait for recovery timeout
        sleep(Duration::from_millis(60)).await;

        // Should transition to half-open
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_millis(50),
            success_threshold: 2,
            half_open_max_requests: 3,
        };

        let cb = CircuitBreaker::with_config(config);

        // Open the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.get_state().await, CircuitState::Open);

        // Wait for recovery timeout
        sleep(Duration::from_millis(60)).await;

        // Transition to half-open
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::HalfOpen);

        // Record successes to close circuit
        cb.record_success().await;
        cb.record_success().await;

        // Should be closed now
        assert_eq!(cb.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_stats() {
        let cb = CircuitBreaker::new();

        cb.record_failure().await;
        cb.record_success().await;

        let stats = cb.get_stats().await;
        assert_eq!(stats.current_state, CircuitState::Closed);
        assert!(stats.time_since_last_state_change > Duration::ZERO);
    }
}

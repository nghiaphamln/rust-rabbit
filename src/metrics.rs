//! Metrics and observability module for RustRabbit
//!
//! This module provides Prometheus metrics integration for monitoring:
//! - Message throughput (published/consumed)
//! - Connection health and pool status
//! - Error rates and retry attempts
//! - Processing latency and queue depths

use prometheus::{
    Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics collector for RustRabbit operations
#[derive(Debug, Clone)]
pub struct RustRabbitMetrics {
    registry: Arc<Registry>,

    // Message metrics
    messages_published_total: IntCounterVec,
    messages_consumed_total: IntCounterVec,
    messages_failed_total: IntCounterVec,
    messages_retried_total: IntCounterVec,

    // Processing metrics
    message_processing_duration: HistogramVec,
    message_publish_duration: HistogramVec,

    // Connection metrics
    connections_total: IntGauge,
    connections_healthy: IntGauge,
    connections_unhealthy: IntGauge,
    connection_attempts_total: IntCounter,
    connection_failures_total: IntCounter,

    // Queue metrics
    queue_depth: IntGaugeVec,
    consumer_count: IntGaugeVec,

    // Health metrics
    health_check_duration: Histogram,
    last_health_check_timestamp: Gauge,
}

impl RustRabbitMetrics {
    /// Create a new metrics instance with default registry
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Arc::new(Registry::new());
        Self::with_registry(registry)
    }

    /// Create a new metrics instance with custom registry
    pub fn with_registry(registry: Arc<Registry>) -> Result<Self, prometheus::Error> {
        let messages_published_total = IntCounterVec::new(
            prometheus::Opts::new(
                "rustrabbit_messages_published_total",
                "Total number of messages published",
            ),
            &["queue", "exchange", "routing_key"],
        )?;

        let messages_consumed_total = IntCounterVec::new(
            prometheus::Opts::new(
                "rustrabbit_messages_consumed_total",
                "Total number of messages consumed",
            ),
            &["queue", "consumer_tag", "status"],
        )?;

        let messages_failed_total = IntCounterVec::new(
            prometheus::Opts::new(
                "rustrabbit_messages_failed_total",
                "Total number of failed message processing attempts",
            ),
            &["queue", "error_type", "retry_attempt"],
        )?;

        let messages_retried_total = IntCounterVec::new(
            prometheus::Opts::new(
                "rustrabbit_messages_retried_total",
                "Total number of message retry attempts",
            ),
            &["queue", "retry_reason"],
        )?;

        let message_processing_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "rustrabbit_message_processing_duration_seconds",
                "Time spent processing messages",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["queue", "consumer_tag"],
        )?;

        let message_publish_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "rustrabbit_message_publish_duration_seconds",
                "Time spent publishing messages",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
            &["queue", "exchange"],
        )?;

        let connections_total = IntGauge::with_opts(prometheus::Opts::new(
            "rustrabbit_connections_total",
            "Total number of connections in the pool",
        ))?;

        let connections_healthy = IntGauge::with_opts(prometheus::Opts::new(
            "rustrabbit_connections_healthy",
            "Number of healthy connections",
        ))?;

        let connections_unhealthy = IntGauge::with_opts(prometheus::Opts::new(
            "rustrabbit_connections_unhealthy",
            "Number of unhealthy connections",
        ))?;

        let connection_attempts_total = IntCounter::with_opts(prometheus::Opts::new(
            "rustrabbit_connection_attempts_total",
            "Total number of connection attempts",
        ))?;

        let connection_failures_total = IntCounter::with_opts(prometheus::Opts::new(
            "rustrabbit_connection_failures_total",
            "Total number of connection failures",
        ))?;

        let queue_depth = IntGaugeVec::new(
            prometheus::Opts::new(
                "rustrabbit_queue_depth",
                "Number of messages waiting in queue",
            ),
            &["queue"],
        )?;

        let consumer_count = IntGaugeVec::new(
            prometheus::Opts::new(
                "rustrabbit_consumer_count",
                "Number of active consumers per queue",
            ),
            &["queue"],
        )?;

        let health_check_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "rustrabbit_health_check_duration_seconds",
                "Time spent performing health checks",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        )?;

        let last_health_check_timestamp = Gauge::with_opts(prometheus::Opts::new(
            "rustrabbit_last_health_check_timestamp",
            "Timestamp of last health check (Unix seconds)",
        ))?;

        // Register all metrics
        registry.register(Box::new(messages_published_total.clone()))?;
        registry.register(Box::new(messages_consumed_total.clone()))?;
        registry.register(Box::new(messages_failed_total.clone()))?;
        registry.register(Box::new(messages_retried_total.clone()))?;
        registry.register(Box::new(message_processing_duration.clone()))?;
        registry.register(Box::new(message_publish_duration.clone()))?;
        registry.register(Box::new(connections_total.clone()))?;
        registry.register(Box::new(connections_healthy.clone()))?;
        registry.register(Box::new(connections_unhealthy.clone()))?;
        registry.register(Box::new(connection_attempts_total.clone()))?;
        registry.register(Box::new(connection_failures_total.clone()))?;
        registry.register(Box::new(queue_depth.clone()))?;
        registry.register(Box::new(consumer_count.clone()))?;
        registry.register(Box::new(health_check_duration.clone()))?;
        registry.register(Box::new(last_health_check_timestamp.clone()))?;

        Ok(Self {
            registry,
            messages_published_total,
            messages_consumed_total,
            messages_failed_total,
            messages_retried_total,
            message_processing_duration,
            message_publish_duration,
            connections_total,
            connections_healthy,
            connections_unhealthy,
            connection_attempts_total,
            connection_failures_total,
            queue_depth,
            consumer_count,
            health_check_duration,
            last_health_check_timestamp,
        })
    }

    /// Get the Prometheus registry for exposing metrics
    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    // Message metrics

    /// Record a published message
    pub fn record_message_published(&self, queue: &str, exchange: &str, routing_key: &str) {
        self.messages_published_total
            .with_label_values(&[queue, exchange, routing_key])
            .inc();
    }

    /// Record a consumed message
    pub fn record_message_consumed(&self, queue: &str, consumer_tag: &str, success: bool) {
        let status = if success { "success" } else { "failed" };
        self.messages_consumed_total
            .with_label_values(&[queue, consumer_tag, status])
            .inc();
    }

    /// Record a failed message processing
    pub fn record_message_failed(&self, queue: &str, error_type: &str, retry_attempt: u32) {
        self.messages_failed_total
            .with_label_values(&[queue, error_type, &retry_attempt.to_string()])
            .inc();
    }

    /// Record a message retry
    pub fn record_message_retry(&self, queue: &str, retry_reason: &str) {
        self.messages_retried_total
            .with_label_values(&[queue, retry_reason])
            .inc();
    }

    /// Record message processing duration
    pub fn record_processing_duration(&self, queue: &str, consumer_tag: &str, duration: Duration) {
        self.message_processing_duration
            .with_label_values(&[queue, consumer_tag])
            .observe(duration.as_secs_f64());
    }

    /// Record message publish duration
    pub fn record_publish_duration(&self, queue: &str, exchange: &str, duration: Duration) {
        self.message_publish_duration
            .with_label_values(&[queue, exchange])
            .observe(duration.as_secs_f64());
    }

    // Connection metrics

    /// Update connection pool metrics
    pub fn update_connection_pool(&self, total: i64, healthy: i64, unhealthy: i64) {
        self.connections_total.set(total);
        self.connections_healthy.set(healthy);
        self.connections_unhealthy.set(unhealthy);
    }

    /// Record a connection attempt
    pub fn record_connection_attempt(&self) {
        self.connection_attempts_total.inc();
    }

    /// Record a connection failure
    pub fn record_connection_failure(&self) {
        self.connection_failures_total.inc();
    }

    // Queue metrics

    /// Update queue depth
    pub fn update_queue_depth(&self, queue: &str, depth: i64) {
        self.queue_depth.with_label_values(&[queue]).set(depth);
    }

    /// Update consumer count
    pub fn update_consumer_count(&self, queue: &str, count: i64) {
        self.consumer_count.with_label_values(&[queue]).set(count);
    }

    // Health metrics

    /// Record health check duration
    pub fn record_health_check_duration(&self, duration: Duration) {
        self.health_check_duration.observe(duration.as_secs_f64());
        self.last_health_check_timestamp.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        );
    }
}

impl Default for RustRabbitMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to create default metrics")
    }
}

/// Timer helper for measuring operation duration
pub struct MetricsTimer {
    start: Instant,
}

impl Default for MetricsTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTimer {
    /// Start a new timer
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed duration since timer started
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = RustRabbitMetrics::new().unwrap();
        assert!(!metrics.registry().gather().is_empty());
    }

    #[test]
    fn test_message_metrics() {
        let metrics = RustRabbitMetrics::new().unwrap();

        metrics.record_message_published("test-queue", "test-exchange", "test-key");
        metrics.record_message_consumed("test-queue", "test-consumer", true);
        metrics.record_message_failed("test-queue", "timeout", 1);
        metrics.record_message_retry("test-queue", "transient-error");

        let metric_families = metrics.registry().gather();
        assert!(!metric_families.is_empty());
    }

    #[test]
    fn test_connection_metrics() {
        let metrics = RustRabbitMetrics::new().unwrap();

        metrics.update_connection_pool(5, 4, 1);
        metrics.record_connection_attempt();
        metrics.record_connection_failure();

        let metric_families = metrics.registry().gather();
        assert!(!metric_families.is_empty());
    }

    #[test]
    fn test_timer() {
        let timer = MetricsTimer::new();
        std::thread::sleep(Duration::from_millis(1));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(1));
    }
}

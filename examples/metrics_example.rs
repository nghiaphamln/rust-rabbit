//! Example demonstrating Prometheus metrics integration with RustRabbit
//!
//! This example shows how to:
//! - Enable metrics collection
//! - Expose metrics via HTTP endpoint
//! - Monitor message throughput and connection health

use rust_rabbit::{
    config::RabbitConfig, consumer::MessageHandler, error::Result, metrics::RustRabbitMetrics,
    publisher::PublishOptions, RustRabbit,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

#[derive(Debug, Serialize, Deserialize)]
struct MetricsTestMessage {
    id: u64,
    content: String,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct MetricsTestHandler {
    processed_count: Arc<std::sync::atomic::AtomicU64>,
}

impl MetricsTestHandler {
    fn new() -> Self {
        Self {
            processed_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn get_processed_count(&self) -> u64 {
        self.processed_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl MessageHandler<MetricsTestMessage> for MetricsTestHandler {
    async fn handle(
        &self,
        message: MetricsTestMessage,
        _context: rust_rabbit::consumer::MessageContext,
    ) -> rust_rabbit::consumer::MessageResult {
        // Simulate processing time
        sleep(Duration::from_millis(10)).await;

        // Simulate some failures for testing error metrics
        if message.id % 10 == 0 {
            error!("Simulated processing failure for message {}", message.id);
            return rust_rabbit::consumer::MessageResult::Retry;
        }

        let count = self
            .processed_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 100 == 0 {
            info!("Processed {} messages", count + 1);
        }

        rust_rabbit::consumer::MessageResult::Ack
    }
}

async fn start_metrics_server(metrics: RustRabbitMetrics) -> Result<()> {
    use prometheus::{Encoder, TextEncoder};
    use warp::Filter;

    let metrics_route = warp::path("metrics").map(move || {
        let encoder = TextEncoder::new();
        let metric_families = metrics.registry().gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    });

    info!("🔍 Metrics server starting on http://localhost:9090/metrics");

    // Start the metrics server
    tokio::spawn(async move {
        warp::serve(metrics_route).run(([127, 0, 0, 1], 9090)).await;
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Starting RustRabbit Metrics Example");

    // Create RabbitMQ configuration
    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .build();

    // Create metrics instance
    let metrics = RustRabbitMetrics::new().map_err(|_| {
        rust_rabbit::error::RabbitError::Configuration("Failed to create metrics".to_string())
    })?;

    // Start metrics HTTP server
    start_metrics_server(metrics.clone()).await?;

    // Create RustRabbit instance with metrics
    let rabbit = RustRabbit::with_metrics(config, metrics.clone()).await?;

    // Create publisher
    let publisher = rabbit.publisher();

    // Create consumer
    let queue_name = "metrics-test-queue";
    let consumer_options = rust_rabbit::consumer::ConsumerOptions::builder(queue_name)
        .consumer_tag("metrics-test-consumer".to_string())
        .auto_ack()
        .concurrency(10)
        .build();

    let handler = MetricsTestHandler::new();
    let consumer = rabbit.consumer(consumer_options).await?;

    // Start consumer in background
    let handler_clone = Arc::new(handler.clone());
    tokio::spawn(async move {
        if let Err(e) = consumer.consume(handler_clone).await {
            error!("Consumer error: {}", e);
        }
    });

    // Start health monitoring
    let health_checker = rabbit.health_checker();
    tokio::spawn(async move {
        let _ = health_checker.start_monitoring().await;
    });

    info!("📊 Starting message publishing and metrics collection...");
    info!("🌐 View metrics at: http://localhost:9090/metrics");
    info!("📈 Example metrics to watch:");
    info!("   - rustrabbit_messages_published_total");
    info!("   - rustrabbit_messages_consumed_total");
    info!("   - rustrabbit_message_processing_duration_seconds");
    info!("   - rustrabbit_connections_healthy");

    // Publish messages continuously
    let mut message_id = 1u64;
    loop {
        // Publish a batch of messages
        for _i in 0..50 {
            let message = MetricsTestMessage {
                id: message_id,
                content: format!("Test message {} - batch processing", message_id),
                timestamp: chrono::Utc::now(),
            };

            let publish_options = PublishOptions::builder().auto_declare_queue().build();

            if let Err(e) = publisher
                .publish_to_queue(queue_name, &message, Some(publish_options))
                .await
            {
                error!("Failed to publish message {}: {}", message_id, e);
            }

            message_id += 1;
        }

        // Wait before next batch
        sleep(Duration::from_secs(2)).await;

        // Print some stats
        let processed_count = handler.get_processed_count();
        info!(
            "📊 Status: Published {} messages, Processed {} messages",
            message_id - 1,
            processed_count
        );

        // Print current metrics sample
        if message_id % 200 == 0 {
            info!("🔍 Check metrics at http://localhost:9090/metrics for detailed stats");

            // Show some key metrics values (simplified - avoid complex API)
            info!("   Metrics are being collected - check the HTTP endpoint");
        }

        // Stop after 1000 messages for demo
        if message_id > 1000 {
            info!("🎯 Demo completed! Published 1000 messages");
            break;
        }
    }

    // Wait a bit for final processing
    sleep(Duration::from_secs(5)).await;

    info!("📈 Final metrics summary:");
    let processed_count = handler.get_processed_count();
    info!("   - Total published: {}", message_id - 1);
    info!("   - Total processed: {}", processed_count);
    info!(
        "   - Success rate: {:.1}%",
        (processed_count as f64 / (message_id - 1) as f64) * 100.0
    );
    info!("🌐 Metrics still available at: http://localhost:9090/metrics");

    // Keep metrics server running for a bit
    info!("⏰ Keeping metrics server running for 30 seconds...");
    sleep(Duration::from_secs(30)).await;

    // Cleanup
    rabbit.close().await?;
    info!("✅ RustRabbit Metrics Example completed successfully!");

    Ok(())
}

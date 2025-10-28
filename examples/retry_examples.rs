//! Retry Examples
//!
//! This example demonstrates different retry configurations and patterns
//! available in rust-rabbit.

use rust_rabbit::{Connection, Consumer, RetryConfig};
use serde::Deserialize;
use std::time::Duration;
use tracing::{error, info, warn, Level};

#[derive(Deserialize, Debug)]
struct ProcessingTask {
    id: u32,
    task_type: String,
    data: String,
    difficulty: u8, // 1-10, affects failure probability
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting retry examples");

    // Connect to RabbitMQ
    let connection = Connection::new("amqp://guest:guest@localhost:5672").await?;

    // Example 1: Exponential Retry (Default)
    info!("Starting exponential retry consumer...");

    let exponential_consumer = Consumer::builder(connection.clone(), "exponential_tasks")
        .retry(RetryConfig::exponential_default()) // 1s→2s→4s→8s→16s (5 retries)
        .concurrency(3)
        .build()
        .await?;

    let exp_handle = tokio::spawn(async move {
        exponential_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Exponential retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "exponential").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(
                            "Exponential task {} failed: {} (will retry with exponential backoff)",
                            task.id, e
                        );
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 2: Linear Retry
    info!("Starting linear retry consumer...");

    let linear_consumer = Consumer::builder(connection.clone(), "linear_tasks")
        .retry(RetryConfig::linear(4, Duration::from_secs(10))) // 4 retries, 10s each
        .concurrency(2)
        .build()
        .await?;

    let linear_handle = tokio::spawn(async move {
        linear_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Linear retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "linear").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(
                            "Linear task {} failed: {} (will retry in 10 seconds)",
                            task.id, e
                        );
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 3: Custom Retry Delays
    info!("Starting custom retry consumer...");

    let custom_retry = RetryConfig::custom(vec![
        Duration::from_secs(1),     // Quick first retry
        Duration::from_secs(5),     // Medium wait
        Duration::from_secs(30),    // Longer wait
        Duration::from_minutes(2),  // Even longer wait
        Duration::from_minutes(10), // Final attempt after 10 minutes
    ]);

    let custom_consumer = Consumer::builder(connection.clone(), "custom_tasks")
        .retry(custom_retry)
        .concurrency(1) // Sequential processing for custom retry
        .build()
        .await?;

    let custom_handle = tokio::spawn(async move {
        custom_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Custom retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "custom").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(
                            "Custom task {} failed: {} (will retry with custom delays)",
                            task.id, e
                        );
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 4: Fast Retry with Short Delays
    info!("Starting fast retry consumer...");

    let fast_retry = RetryConfig::exponential(
        8,                          // 8 retries
        Duration::from_millis(100), // Start with 100ms
        Duration::from_secs(5),     // Cap at 5 seconds
    );

    let fast_consumer = Consumer::builder(connection.clone(), "fast_tasks")
        .retry(fast_retry)
        .concurrency(5)
        .build()
        .await?;

    let fast_handle = tokio::spawn(async move {
        fast_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Fast retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "fast").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!("Fast task {} failed: {} (will retry quickly)", task.id, e);
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 5: Conservative Retry (Few attempts, long delays)
    info!("Starting conservative retry consumer...");

    let conservative_retry = RetryConfig::exponential(
        2,                          // Only 2 retries
        Duration::from_secs(30),    // Start with 30 seconds
        Duration::from_minutes(15), // Max 15 minutes
    );

    let conservative_consumer = Consumer::builder(connection.clone(), "conservative_tasks")
        .retry(conservative_retry)
        .concurrency(1)
        .build()
        .await?;

    let conservative_handle = tokio::spawn(async move {
        conservative_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Conservative retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "conservative").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(
                            "Conservative task {} failed: {} (will retry after long delay)",
                            task.id, e
                        );
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 6: No Retry (Immediate failure)
    info!("Starting no-retry consumer...");

    let no_retry_consumer = Consumer::builder(connection.clone(), "no_retry_tasks")
        .retry(RetryConfig::no_retry()) // Failed messages go directly to DLQ
        .concurrency(3)
        .build()
        .await?;

    let no_retry_handle = tokio::spawn(async move {
        no_retry_consumer
            .consume(|task: ProcessingTask| async move {
                info!("No retry - Processing task {}: {}", task.id, task.task_type);

                match simulate_task_processing(&task, "no_retry").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("No retry task {} failed: {} (going to DLQ)", task.id, e);
                        Err(e)
                    }
                }
            })
            .await
    });

    // Example 7: Selective Retry (Different strategies based on error type)
    info!("Starting selective retry consumer...");

    let selective_consumer = Consumer::builder(connection.clone(), "selective_tasks")
        .retry(RetryConfig::exponential_default())
        .concurrency(3)
        .build()
        .await?;

    let selective_handle = tokio::spawn(async move {
        selective_consumer
            .consume(|task: ProcessingTask| async move {
                info!(
                    "Selective retry - Processing task {}: {}",
                    task.id, task.task_type
                );

                match simulate_task_processing(&task, "selective").await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // Classify errors to decide retry strategy
                        if is_retryable_error(&e) {
                            warn!(
                                "Selective task {} failed with retryable error: {}",
                                task.id, e
                            );
                            Err(e) // Will retry
                        } else {
                            error!(
                                "Selective task {} failed with permanent error: {}",
                                task.id, e
                            );
                            Ok(()) // Don't retry, but ACK to avoid infinite loop
                        }
                    }
                }
            })
            .await
    });

    info!("All retry consumers started. Press Ctrl+C to stop...");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping consumers...");

    // Stop all consumers
    exp_handle.abort();
    linear_handle.abort();
    custom_handle.abort();
    fast_handle.abort();
    conservative_handle.abort();
    no_retry_handle.abort();
    selective_handle.abort();

    info!("Retry examples completed!");
    Ok(())
}

// Simulate task processing with different failure patterns
async fn simulate_task_processing(
    task: &ProcessingTask,
    retry_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simulate processing time
    let processing_time = Duration::from_millis(50 + (task.difficulty as u64 * 20));
    tokio::time::sleep(processing_time).await;

    // Simulate different types of failures based on task properties
    let failure_probability = (task.difficulty as f32) / 10.0 * 0.4; // Max 40% failure rate
    let random = fastrand::f32();

    if random < failure_probability {
        // Generate different types of errors
        match task.task_type.as_str() {
            "network_call" => {
                if random < 0.1 {
                    return Err("Network timeout".into());
                } else if random < 0.2 {
                    return Err("Connection refused".into());
                } else {
                    return Err("Service temporarily unavailable".into());
                }
            }
            "database_query" => {
                if random < 0.1 {
                    return Err("Database connection lost".into());
                } else if random < 0.15 {
                    return Err("Query timeout".into());
                } else {
                    return Err("Database temporarily overloaded".into());
                }
            }
            "file_processing" => {
                if random < 0.05 {
                    return Err("File not found".into()); // Permanent error
                } else if random < 0.1 {
                    return Err("Disk full".into());
                } else {
                    return Err("File locked by another process".into());
                }
            }
            "api_call" => {
                if random < 0.1 {
                    return Err("Rate limit exceeded".into());
                } else if random < 0.15 {
                    return Err("API server error".into());
                } else if random < 0.17 {
                    return Err("Invalid API key".into()); // Permanent error
                } else {
                    return Err("API temporarily unavailable".into());
                }
            }
            "validation" => {
                if random < 0.1 {
                    return Err("Invalid data format".into()); // Permanent error
                } else {
                    return Err("Validation service unreachable".into());
                }
            }
            _ => {
                return Err("Unknown processing error".into());
            }
        }
    }

    // Success case
    info!(
        "Task {} ({}) completed successfully with {} retry strategy",
        task.id, task.task_type, retry_type
    );
    Ok(())
}

// Classify errors as retryable or permanent
fn is_retryable_error(error: &Box<dyn std::error::Error + Send + Sync>) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // Permanent errors that shouldn't be retried
    let permanent_errors = [
        "file not found",
        "invalid data format",
        "invalid api key",
        "access denied",
        "forbidden",
        "bad request",
        "malformed",
    ];

    for permanent in &permanent_errors {
        if error_msg.contains(permanent) {
            return false;
        }
    }

    // Transient errors that should be retried
    let retryable_errors = [
        "timeout",
        "network",
        "connection",
        "temporarily",
        "rate limit",
        "server error",
        "overloaded",
        "unavailable",
        "locked",
        "busy",
    ];

    for retryable in &retryable_errors {
        if error_msg.contains(retryable) {
            return true;
        }
    }

    // Default to retryable for unknown errors
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_creation() {
        // Test different retry configurations
        let exponential = RetryConfig::exponential_default();
        assert_eq!(exponential.max_retries, 5);

        let linear = RetryConfig::linear(3, Duration::from_secs(5));
        assert_eq!(linear.max_retries, 3);

        let custom = RetryConfig::custom(vec![Duration::from_secs(1), Duration::from_secs(10)]);
        assert_eq!(custom.max_retries, 2);

        let no_retry = RetryConfig::no_retry();
        assert_eq!(no_retry.max_retries, 0);
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(30));

        // Test exponential delays
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4)));
        assert_eq!(config.calculate_delay(5), None); // Max retries exceeded

        let linear_config = RetryConfig::linear(3, Duration::from_secs(10));
        assert_eq!(
            linear_config.calculate_delay(0),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            linear_config.calculate_delay(1),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            linear_config.calculate_delay(2),
            Some(Duration::from_secs(10))
        );
        assert_eq!(linear_config.calculate_delay(3), None);
    }

    #[test]
    fn test_error_classification() {
        // Test retryable errors
        let retryable: Box<dyn std::error::Error + Send + Sync> = "Network timeout".into();
        assert!(is_retryable_error(&retryable));

        let rate_limit: Box<dyn std::error::Error + Send + Sync> = "Rate limit exceeded".into();
        assert!(is_retryable_error(&rate_limit));

        // Test permanent errors
        let permanent: Box<dyn std::error::Error + Send + Sync> = "Invalid data format".into();
        assert!(!is_retryable_error(&permanent));

        let not_found: Box<dyn std::error::Error + Send + Sync> = "File not found".into();
        assert!(!is_retryable_error(&not_found));
    }

    #[tokio::test]
    async fn test_task_processing_simulation() {
        let easy_task = ProcessingTask {
            id: 1,
            task_type: "network_call".to_string(),
            data: "test".to_string(),
            difficulty: 1, // Low difficulty = low failure rate
        };

        // Should have high success rate
        let mut successes = 0;
        for _ in 0..10 {
            if simulate_task_processing(&easy_task, "test").await.is_ok() {
                successes += 1;
            }
        }
        assert!(successes >= 5); // Should succeed more often than fail

        let hard_task = ProcessingTask {
            id: 2,
            task_type: "validation".to_string(),
            data: "test".to_string(),
            difficulty: 10, // High difficulty = high failure rate
        };

        // Should have lower success rate
        let mut failures = 0;
        for _ in 0..10 {
            if simulate_task_processing(&hard_task, "test").await.is_err() {
                failures += 1;
            }
        }
        assert!(failures >= 2); // Should fail sometimes
    }
}

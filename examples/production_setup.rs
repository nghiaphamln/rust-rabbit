//! Production Setup Example
//! 
//! This example demonstrates a production-ready configuration of rust-rabbit
//! with proper error handling, monitoring, graceful shutdown, and best practices.

use rust_rabbit::{Connection, Consumer, Publisher, RetryConfig, PublishOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug, Level};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderEvent {
    order_id: String,
    user_id: String,
    amount: f64,
    currency: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    event_type: OrderEventType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum OrderEventType {
    Created,
    Updated,
    Cancelled,
    Completed,
    Refunded,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NotificationTask {
    recipient: String,
    message: String,
    notification_type: NotificationType,
    priority: Priority,
    retry_count: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum NotificationType {
    Email,
    SMS,
    Push,
    InApp,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

// Production application state
#[derive(Clone)]
struct AppState {
    connection: Connection,
    publisher: Arc<Publisher>,
    stats: Arc<RwLock<AppStats>>,
    shutdown: Arc<tokio::sync::Notify>,
}

#[derive(Default, Debug)]
struct AppStats {
    orders_processed: u64,
    notifications_sent: u64,
    errors_encountered: u64,
    last_activity: Option<chrono::DateTime<chrono::Utc>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging for production
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .json() // Use JSON format for production logs
        .init();

    info!("Starting production RabbitMQ application");

    // Initialize application state
    let app_state = initialize_app().await?;
    
    // Start health monitoring
    let health_handle = start_health_monitoring(app_state.clone());
    
    // Start metrics reporting
    let metrics_handle = start_metrics_reporting(app_state.clone());
    
    // Start message producers
    let producer_handle = start_message_producer(app_state.clone());
    
    // Start order processing consumer
    let order_consumer_handle = start_order_consumer(app_state.clone()).await?;
    
    // Start notification consumer  
    let notification_consumer_handle = start_notification_consumer(app_state.clone()).await?;
    
    info!("All services started successfully");
    
    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown");
        }
        _ = app_state.shutdown.notified() => {
            info!("Received internal shutdown signal");
        }
    }
    
    // Graceful shutdown
    info!("Starting graceful shutdown sequence");
    
    // Stop accepting new work
    producer_handle.abort();
    
    // Allow consumers to finish current work (with timeout)
    let shutdown_timeout = Duration::from_secs(30);
    tokio::select! {
        _ = order_consumer_handle => info!("Order consumer stopped"),
        _ = tokio::time::sleep(shutdown_timeout) => warn!("Order consumer shutdown timeout"),
    }
    
    tokio::select! {
        _ = notification_consumer_handle => info!("Notification consumer stopped"),
        _ = tokio::time::sleep(shutdown_timeout) => warn!("Notification consumer shutdown timeout"),
    }
    
    // Stop monitoring services
    health_handle.abort();
    metrics_handle.abort();
    
    // Print final statistics
    let stats = app_state.stats.read().await;
    info!("Final statistics: {:?}", *stats);
    
    info!("Application shutdown complete");
    Ok(())
}

async fn initialize_app() -> Result<AppState, Box<dyn std::error::Error>> {
    info!("Initializing application state");
    
    // Read configuration from environment with defaults
    let rabbitmq_url = std::env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string());
    
    let connection_timeout = std::env::var("CONNECTION_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse::<u64>()
        .unwrap_or(30);
    
    info!("Connecting to RabbitMQ at: {}", rabbitmq_url);
    
    // Create connection with retry logic
    let connection = create_connection_with_retry(&rabbitmq_url, connection_timeout).await?;
    
    // Create publisher
    let publisher = Arc::new(Publisher::new(connection.clone()).await?);
    
    // Initialize statistics
    let stats = Arc::new(RwLock::new(AppStats::default()));
    
    // Create shutdown notifier
    let shutdown = Arc::new(tokio::sync::Notify::new());
    
    info!("Application state initialized successfully");
    
    Ok(AppState {
        connection,
        publisher,
        stats,
        shutdown,
    })
}

async fn create_connection_with_retry(
    url: &str,
    timeout: u64,
) -> Result<Connection, Box<dyn std::error::Error>> {
    let max_retries = 5;
    let mut current_retry = 0;
    
    loop {
        match tokio::time::timeout(
            Duration::from_secs(timeout),
            Connection::new(url)
        ).await {
            Ok(Ok(connection)) => {
                info!("Successfully connected to RabbitMQ");
                return Ok(connection);
            }
            Ok(Err(e)) => {
                current_retry += 1;
                if current_retry >= max_retries {
                    error!("Failed to connect to RabbitMQ after {} retries: {}", max_retries, e);
                    return Err(e.into());
                }
                let delay = Duration::from_secs(2_u64.pow(current_retry));
                warn!("Connection attempt {} failed: {}. Retrying in {:?}", current_retry, e, delay);
                tokio::time::sleep(delay).await;
            }
            Err(_) => {
                current_retry += 1;
                if current_retry >= max_retries {
                    error!("Connection timeout after {} retries", max_retries);
                    return Err("Connection timeout".into());
                }
                let delay = Duration::from_secs(2_u64.pow(current_retry));
                warn!("Connection timeout on attempt {}. Retrying in {:?}", current_retry, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn start_order_consumer(app_state: AppState) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
    info!("Starting order processing consumer");
    
    // Production retry configuration for orders
    let retry_config = RetryConfig::exponential(
        3,                               // 3 retries max
        Duration::from_secs(5),         // Start with 5 seconds
        Duration::from_minutes(5)       // Cap at 5 minutes
    );
    
    let consumer = Consumer::builder(app_state.connection.clone(), "order_events")
        .retry(retry_config)
        .concurrency(10) // Process up to 10 orders concurrently
        .prefetch(20)    // Prefetch 20 messages for better throughput
        .build()
        .await?;
    
    let state = app_state.clone();
    let handle = tokio::spawn(async move {
        let result = consumer.consume(move |order: OrderEvent| {
            let state = state.clone();
            async move {
                debug!("Processing order event: {}", order.order_id);
                
                match process_order_event(&order, &state).await {
                    Ok(_) => {
                        // Update statistics
                        let mut stats = state.stats.write().await;
                        stats.orders_processed += 1;
                        stats.last_activity = Some(chrono::Utc::now());
                        
                        info!("Successfully processed order {}", order.order_id);
                        Ok(())
                    }
                    Err(e) => {
                        // Update error statistics
                        let mut stats = state.stats.write().await;
                        stats.errors_encountered += 1;
                        
                        error!("Failed to process order {}: {}", order.order_id, e);
                        Err(e)
                    }
                }
            }
        }).await;
        
        if let Err(e) = result {
            error!("Order consumer stopped with error: {}", e);
            app_state.shutdown.notify_one();
        }
    });
    
    info!("Order consumer started successfully");
    Ok(handle)
}

async fn start_notification_consumer(app_state: AppState) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
    info!("Starting notification consumer");
    
    // Different retry strategy for notifications
    let retry_config = RetryConfig::custom(vec![
        Duration::from_secs(1),      // Quick retry for transient issues
        Duration::from_secs(10),     // Medium delay
        Duration::from_minutes(1),   // Longer delay
        Duration::from_minutes(5),   // Even longer
        Duration::from_minutes(30),  // Final attempt after 30 minutes
    ]);
    
    let consumer = Consumer::builder(app_state.connection.clone(), "notifications")
        .retry(retry_config)
        .concurrency(20) // High concurrency for notifications
        .prefetch(50)
        .build()
        .await?;
    
    let state = app_state.clone();
    let handle = tokio::spawn(async move {
        let result = consumer.consume(move |notification: NotificationTask| {
            let state = state.clone();
            async move {
                debug!("Processing notification for: {}", notification.recipient);
                
                match send_notification(&notification, &state).await {
                    Ok(_) => {
                        // Update statistics
                        let mut stats = state.stats.write().await;
                        stats.notifications_sent += 1;
                        stats.last_activity = Some(chrono::Utc::now());
                        
                        info!("Successfully sent notification to {}", notification.recipient);
                        Ok(())
                    }
                    Err(e) => {
                        // Update error statistics
                        let mut stats = state.stats.write().await;
                        stats.errors_encountered += 1;
                        
                        error!("Failed to send notification to {}: {}", notification.recipient, e);
                        Err(e)
                    }
                }
            }
        }).await;
        
        if let Err(e) = result {
            error!("Notification consumer stopped with error: {}", e);
            app_state.shutdown.notify_one();
        }
    });
    
    info!("Notification consumer started successfully");
    Ok(handle)
}

fn start_message_producer(app_state: AppState) -> tokio::task::JoinHandle<()> {
    info!("Starting message producer");
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut order_counter = 1;
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Simulate incoming orders
                    if let Err(e) = simulate_order_creation(&app_state, &mut order_counter).await {
                        error!("Failed to create simulated order: {}", e);
                    }
                }
                _ = app_state.shutdown.notified() => {
                    info!("Message producer shutting down");
                    break;
                }
            }
        }
    })
}

async fn simulate_order_creation(
    app_state: &AppState,
    counter: &mut u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let order = OrderEvent {
        order_id: format!("ORDER-{:06}", counter),
        user_id: format!("USER-{}", fastrand::u32(1..=1000)),
        amount: fastrand::f64() * 1000.0,
        currency: "USD".to_string(),
        timestamp: chrono::Utc::now(),
        event_type: OrderEventType::Created,
    };
    
    // Publish with production settings
    let options = PublishOptions::builder()
        .persistent(true)                    // Persist messages to disk
        .mandatory(true)                     // Ensure message is routed
        .immediate(false)                    // Don't require immediate consumer
        .expiration(Duration::from_hours(24)) // Expire after 24 hours
        .build();
    
    app_state.publisher
        .publish_to_queue("order_events", &order, options)
        .await?;
    
    info!("Published order event: {}", order.order_id);
    *counter += 1;
    
    Ok(())
}

async fn process_order_event(
    order: &OrderEvent,
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simulate order processing logic
    debug!("Processing order {} for user {}", order.order_id, order.user_id);
    
    // Simulate some processing time
    tokio::time::sleep(Duration::from_millis(fastrand::u64(100..=500))).await;
    
    // Simulate occasional failures (10% failure rate)
    if fastrand::f32() < 0.1 {
        return Err("Simulated order processing failure".into());
    }
    
    // Create notifications based on order event
    match order.event_type {
        OrderEventType::Created => {
            let notification = NotificationTask {
                recipient: order.user_id.clone(),
                message: format!("Your order {} has been created for ${:.2}", order.order_id, order.amount),
                notification_type: NotificationType::Email,
                priority: Priority::Normal,
                retry_count: 0,
            };
            
            app_state.publisher
                .publish_to_queue("notifications", &notification, PublishOptions::default())
                .await?;
                
            info!("Sent order creation notification for {}", order.order_id);
        }
        OrderEventType::Completed => {
            let notification = NotificationTask {
                recipient: order.user_id.clone(),
                message: format!("Your order {} has been completed!", order.order_id),
                notification_type: NotificationType::Push,
                priority: Priority::High,
                retry_count: 0,
            };
            
            app_state.publisher
                .publish_to_queue("notifications", &notification, PublishOptions::default())
                .await?;
                
            info!("Sent order completion notification for {}", order.order_id);
        }
        _ => {
            debug!("No notification needed for order event type: {:?}", order.event_type);
        }
    }
    
    Ok(())
}

async fn send_notification(
    notification: &NotificationTask,
    _app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simulate notification sending logic
    debug!("Sending {:?} notification to {}", notification.notification_type, notification.recipient);
    
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(fastrand::u64(50..=200))).await;
    
    // Simulate failures based on notification type
    let failure_rate = match notification.notification_type {
        NotificationType::Email => 0.05,    // 5% failure rate
        NotificationType::SMS => 0.1,       // 10% failure rate  
        NotificationType::Push => 0.15,     // 15% failure rate
        NotificationType::InApp => 0.02,    // 2% failure rate
    };
    
    if fastrand::f32() < failure_rate {
        return Err(format!("Simulated {:?} notification failure", notification.notification_type).into());
    }
    
    Ok(())
}

fn start_health_monitoring(app_state: AppState) -> tokio::task::JoinHandle<()> {
    info!("Starting health monitoring");
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = app_state.stats.read().await;
                    
                    // Check if we're still processing messages
                    let is_healthy = if let Some(last_activity) = stats.last_activity {
                        chrono::Utc::now().signed_duration_since(last_activity).num_seconds() < 300 // 5 minutes
                    } else {
                        true // No activity yet is OK
                    };
                    
                    if is_healthy {
                        info!("Health check: OK - Orders: {}, Notifications: {}, Errors: {}", 
                             stats.orders_processed, stats.notifications_sent, stats.errors_encountered);
                    } else {
                        warn!("Health check: WARNING - No activity for 5+ minutes");
                    }
                }
                _ = app_state.shutdown.notified() => {
                    info!("Health monitoring shutting down");
                    break;
                }
            }
        }
    })
}

fn start_metrics_reporting(app_state: AppState) -> tokio::task::JoinHandle<()> {
    info!("Starting metrics reporting");
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let mut last_stats = AppStats::default();
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let current_stats = app_state.stats.read().await.clone();
                    
                    // Calculate rates since last report
                    let orders_rate = current_stats.orders_processed.saturating_sub(last_stats.orders_processed);
                    let notifications_rate = current_stats.notifications_sent.saturating_sub(last_stats.notifications_sent);
                    let errors_rate = current_stats.errors_encountered.saturating_sub(last_stats.errors_encountered);
                    
                    info!(
                        "Metrics Report - Orders/min: {}, Notifications/min: {}, Errors/min: {}, Total Orders: {}, Total Notifications: {}, Total Errors: {}",
                        orders_rate, notifications_rate, errors_rate,
                        current_stats.orders_processed, current_stats.notifications_sent, current_stats.errors_encountered
                    );
                    
                    last_stats = current_stats;
                }
                _ = app_state.shutdown.notified() => {
                    info!("Metrics reporting shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_order_event_serialization() {
        let order = OrderEvent {
            order_id: "TEST-001".to_string(),
            user_id: "USER-123".to_string(),
            amount: 99.99,
            currency: "USD".to_string(),
            timestamp: chrono::Utc::now(),
            event_type: OrderEventType::Created,
        };
        
        let serialized = serde_json::to_string(&order).unwrap();
        let deserialized: OrderEvent = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(order.order_id, deserialized.order_id);
        assert_eq!(order.amount, deserialized.amount);
    }

    #[tokio::test]
    async fn test_notification_task_serialization() {
        let notification = NotificationTask {
            recipient: "user@example.com".to_string(),
            message: "Test message".to_string(),
            notification_type: NotificationType::Email,
            priority: Priority::High,
            retry_count: 0,
        };
        
        let serialized = serde_json::to_string(&notification).unwrap();
        let deserialized: NotificationTask = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(notification.recipient, deserialized.recipient);
        assert_eq!(notification.message, deserialized.message);
    }

    #[test]
    fn test_app_stats_default() {
        let stats = AppStats::default();
        assert_eq!(stats.orders_processed, 0);
        assert_eq!(stats.notifications_sent, 0);
        assert_eq!(stats.errors_encountered, 0);
        assert!(stats.last_activity.is_none());
    }
}
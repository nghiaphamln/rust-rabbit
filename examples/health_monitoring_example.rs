use rust_rabbit::{
    RustRabbit, RabbitConfig, 
    health::HealthCheckConfigExt,
    config::HealthCheckConfig,
};
use std::time::Duration;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create configuration with aggressive health checking
    let mut config = RabbitConfig {
        connection_string: "amqp://localhost:5672".to_string(),
        virtual_host: Some("/".to_string()),
        ..Default::default()
    };
    
    // Use aggressive health check configuration
    config.health_check = HealthCheckConfig::aggressive();

    // Create RustRabbit instance
    let rabbit = RustRabbit::new(config).await?;
    let health_checker = rabbit.health_checker();

    // Start health monitoring
    health_checker.start_monitoring().await?;
    info!("Health monitoring started");

    // Monitor health for a while
    for i in 0..20 {
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Get current health status
        let is_healthy = health_checker.is_healthy().await;
        let is_operational = health_checker.is_operational().await;
        
        info!("Health check {}: healthy={}, operational={}", i + 1, is_healthy, is_operational);

        // Get detailed health summary
        let summary = health_checker.get_health_summary().await;
        info!("Health summary: {:?}", summary);

        // Perform manual health check
        if i % 5 == 0 {
            match health_checker.check_health().await {
                Ok(result) => {
                    info!("Manual health check result: {:?}", result.status);
                    info!("Response time: {:?}", result.response_time);
                    info!("Connection stats: {:?}", result.connection_stats);
                    if !result.errors.is_empty() {
                        warn!("Health check errors: {:?}", result.errors);
                    }
                }
                Err(e) => {
                    warn!("Health check failed: {}", e);
                }
            }
        }
    }

    // Test waiting for healthy connection
    info!("Testing wait_for_healthy...");
    match health_checker.wait_for_healthy(Some(Duration::from_secs(10))).await {
        Ok(_) => info!("Connection is healthy!"),
        Err(e) => warn!("Failed to wait for healthy connection: {}", e),
    }

    // Stop health monitoring
    health_checker.stop_monitoring().await;
    info!("Health monitoring stopped");

    // Close connections
    rabbit.close().await?;

    Ok(())
}
use rust_rabbit::{
    config::RabbitConfig,
    connection::ConnectionManager,
    consumer::{Consumer, ConsumerOptions},
    retry::RetryPolicy,
};
use serde_json;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // 🚀 SETUP SIÊU NHANH - CHỈ 3 BƯỚC!

    // Bước 1: Config connection (thay URL này)
    let config = RabbitConfig::builder()
        .connection_string("amqp://admin:password@localhost:5672/test")
        .build();
    let connection = ConnectionManager::new(config).await?;

    // Bước 2: Config consumer với retry (1 dòng!)
    let options = ConsumerOptions {
        auto_ack: false,                         // Bắt buộc cho retry
        prefetch_count: Some(10),                // Xử lý 10 msg đồng thời
        retry_policy: Some(RetryPolicy::fast()), // Fast retry preset
        ..Default::default()
    };

    // Bước 3: Tạo consumer và start!
    let consumer = Consumer::new(connection, options).await?;

    info!("🎯 Consumer với retry đã sẵn sàng!");

    // Ví dụ cách sử dụng (uncomment để test với RabbitMQ):
    /*
    consumer.consume("my_queue", |delivery| async move {
        match process_your_message(&delivery.data).await {
            Ok(_) => {
                info!("✅ Processed successfully");
                delivery.ack(Default::default()).await?;
            }
            Err(e) if should_retry(&e) => {
                warn!("⚠️ Retryable error: {}, will retry", e);
                delivery.nack(Default::default()).await?;
            }
            Err(e) => {
                error!("❌ Fatal error: {}, sending to DLQ", e);
                delivery.reject(Default::default()).await?;
            }
        }
        Ok(())
    }).await?;
    */

    Ok(())
}

// ========== CÁC PRESET RETRY KHÁC ==========

#[allow(dead_code)]
fn retry_examples() {
    // Fast retry (mặc định): 5 retries, 200ms→300ms→450ms...
    let _fast = RetryPolicy::fast();

    // Fast cho queue cụ thể
    let _fast_custom = RetryPolicy::fast_for_queue("user_orders");

    // Slow retry: 3 retries, 1s→2s→4s...
    let _slow = RetryPolicy::slow();

    // Custom với builder
    let _custom = RetryPolicy::builder()
        .fast_preset() // Dùng preset làm base
        .max_retries(3) // Override số lần retry
        .build();

    // Ultra custom
    let _ultra = RetryPolicy::builder()
        .max_retries(2)
        .initial_delay(std::time::Duration::from_millis(500))
        .backoff_multiplier(3.0)
        .jitter(0.2)
        .dead_letter_exchange("custom.dlx")
        .build();
}

// ========== MESSAGE PROCESSING LOGIC ==========

#[allow(dead_code)]
async fn process_your_message(data: &[u8]) -> Result<(), AppError> {
    // Parse JSON message
    let message: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| AppError::ParseError(e.to_string()))?;

    // Extract user_id (example)
    let user_id = message["user_id"]
        .as_str()
        .ok_or(AppError::MissingField("user_id"))?;

    // Simulate business logic
    match process_user_action(user_id).await {
        Ok(_) => {
            info!("User {} processed successfully", user_id);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[allow(dead_code)]
async fn process_user_action(_user_id: &str) -> Result<(), AppError> {
    // Simulate API call that might fail
    if fastrand::f32() < 0.3 {
        // 30% failure rate
        return Err(AppError::ExternalApiError("Timeout".to_string()));
    }
    Ok(())
}

#[allow(dead_code)]
fn should_retry(error: &AppError) -> bool {
    match error {
        AppError::ExternalApiError(_) => true, // API errors: retry
        AppError::NetworkError(_) => true,     // Network errors: retry
        AppError::ParseError(_) => false,      // Parse errors: don't retry
        AppError::MissingField(_) => false,    // Validation errors: don't retry
    }
}

// ========== ERROR TYPES ==========

#[derive(Debug)]
#[allow(dead_code)]
enum AppError {
    ParseError(String),
    MissingField(&'static str),
    ExternalApiError(String),
    NetworkError(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AppError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            AppError::MissingField(field) => write!(f, "Missing field: {}", field),
            AppError::ExternalApiError(msg) => write!(f, "External API error: {}", msg),
            AppError::NetworkError(msg) => write!(f, "Network error: {}", msg),
        }
    }
}

impl std::error::Error for AppError {}

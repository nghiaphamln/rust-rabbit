//! Basic unit tests for rust-rabbit core functionality

use rust_rabbit::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    id: u32,
    content: String,
    amount: Option<f64>,
}

impl TestMessage {
    fn new(id: u32, content: &str) -> Self {
        Self {
            id,
            content: content.to_string(),
            amount: None,
        }
    }
    
    fn with_amount(mut self, amount: f64) -> Self {
        self.amount = Some(amount);
        self
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn test_exponential_retry() {
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(60));
        
        // Test delay calculations
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));  // 1s
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2)));  // 2s  
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4)));  // 4s
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(8)));  // 8s
        assert_eq!(config.calculate_delay(4), Some(Duration::from_secs(16))); // 16s
        assert_eq!(config.calculate_delay(5), None); // No more retries
        
        // Test max retries
        assert_eq!(config.max_retries, 5);
    }
    
    #[test]
    fn test_exponential_retry_with_cap() {
        let config = RetryConfig::exponential(10, Duration::from_secs(1), Duration::from_secs(5));
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1))); // 1s
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2))); // 2s
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4))); // 4s
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(5))); // 5s (capped)
        assert_eq!(config.calculate_delay(4), Some(Duration::from_secs(5))); // 5s (capped)
    }
    
    #[test]
    fn test_linear_retry() {
        let config = RetryConfig::linear(3, Duration::from_secs(5));
        
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(3), None); // No more retries
    }
    
    #[test]
    fn test_custom_retry() {
        let delays = vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
        ];
        let config = RetryConfig::custom(delays.clone());
        
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(30)));
        assert_eq!(config.calculate_delay(3), None); // No more retries
    }
    
    #[test]
    fn test_no_retry() {
        let config = RetryConfig::no_retry();
        
        assert_eq!(config.max_retries, 0);
        assert_eq!(config.calculate_delay(0), None); // No retries at all
    }
    
    #[test]
    fn test_dead_letter_names() {
        let config = RetryConfig::exponential_default();
        
        assert_eq!(config.get_dead_letter_exchange("orders"), "orders.dlx");
        assert_eq!(config.get_dead_letter_queue("orders"), "orders.dlq");
        
        let config_custom = config.with_dead_letter("custom.dlx".to_string(), "custom.dlq".to_string());
        assert_eq!(config_custom.get_dead_letter_exchange("orders"), "custom.dlx");
        assert_eq!(config_custom.get_dead_letter_queue("orders"), "custom.dlq");
    }
    
    #[test]
    fn test_retry_queue_names() {
        let config = RetryConfig::exponential_default();
        
        assert_eq!(config.get_retry_queue_name("orders", 0), "orders.retry.1");
        assert_eq!(config.get_retry_queue_name("orders", 1), "orders.retry.2");
        assert_eq!(config.get_retry_queue_name("orders", 4), "orders.retry.5");
    }
    
    #[test]
    fn test_retry_mechanism_types() {
        let exponential = RetryMechanism::Exponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        };
        
        let linear = RetryMechanism::Linear {
            delay: Duration::from_secs(5),
        };
        
        let custom = RetryMechanism::Custom {
            delays: vec![Duration::from_secs(1), Duration::from_secs(10)],
        };
        
        // Test that mechanisms can be created and cloned
        let _exp_clone = exponential.clone();
        let _lin_clone = linear.clone();
        let _cust_clone = custom.clone();
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn test_error_retryable_classification() {
        let connection_error = RustRabbitError::Connection("connection lost".to_string());
        assert!(connection_error.is_retryable());
        assert!(connection_error.is_connection_error());
        
        let serialization_error = RustRabbitError::Serialization("invalid json".to_string());
        assert!(!serialization_error.is_retryable());
        assert!(!serialization_error.is_connection_error());
        
        let config_error = RustRabbitError::Configuration("invalid config".to_string());
        assert!(!config_error.is_retryable());
        assert!(!config_error.is_connection_error());
        
        let consumer_error = RustRabbitError::Consumer("processing failed".to_string());
        assert!(consumer_error.is_retryable());
        assert!(!consumer_error.is_connection_error());
        
        let retry_error = RustRabbitError::Retry("retry failed".to_string());
        assert!(!retry_error.is_retryable()); // Avoid infinite retry loops
        assert!(!retry_error.is_connection_error());
    }
    
    #[test]
    fn test_error_user_messages() {
        let errors = vec![
            RustRabbitError::Connection("technical details".to_string()),
            RustRabbitError::Serialization("technical details".to_string()),
            RustRabbitError::Configuration("technical details".to_string()),
            RustRabbitError::Consumer("technical details".to_string()),
            RustRabbitError::Publisher("technical details".to_string()),
            RustRabbitError::Retry("technical details".to_string()),
        ];
        
        for error in errors {
            let user_message = error.user_message();
            assert!(!user_message.is_empty());
            assert!(!user_message.contains("technical details")); // Should not leak technical details
            assert!(user_message.len() > 10); // Should be descriptive
        }
    }
    
    #[test]
    fn test_error_conversion_from_serde() {
        let json_error = serde_json::from_str::<TestMessage>("invalid json").unwrap_err();
        let rabbit_error = RustRabbitError::from(json_error);
        
        assert!(matches!(rabbit_error, RustRabbitError::Serialization(_)));
        assert!(!rabbit_error.is_retryable());
    }
    
    #[test]
    fn test_error_conversion_from_url() {
        let url_error = url::Url::parse("invalid-url").unwrap_err();
        let rabbit_error = RustRabbitError::from(url_error);
        
        assert!(matches!(rabbit_error, RustRabbitError::Configuration(_)));
        assert!(!rabbit_error.is_retryable());
    }
}

#[cfg(test)]
mod publish_options_tests {
    use super::*;

    #[test]
    fn test_publish_options_default() {
        let options = PublishOptions::default();
        
        assert!(options.persistent); // Should default to persistent
        assert_eq!(options.priority, None);
        assert_eq!(options.ttl, None);
        assert!(options.headers.is_empty());
    }
    
    #[test]
    fn test_publish_options_builder() {
        let options = PublishOptions::new()
            .persistent(false)
            .priority(10)
            .ttl(Duration::from_secs(60))
            .header("source", "test-service")
            .header("version", "1.0")
            .header("priority", "high");
        
        assert!(!options.persistent);
        assert_eq!(options.priority, Some(10));
        assert_eq!(options.ttl, Some(Duration::from_secs(60)));
        assert_eq!(options.headers.len(), 3);
        assert_eq!(options.headers.get("source"), Some(&"test-service".to_string()));
        assert_eq!(options.headers.get("version"), Some(&"1.0".to_string()));
        assert_eq!(options.headers.get("priority"), Some(&"high".to_string()));
    }
    
    #[test]
    fn test_publish_options_chaining() {
        let options = PublishOptions::new()
            .persistent(true)
            .priority(5)
            .header("key1", "value1")
            .header("key2", "value2")
            .ttl(Duration::from_millis(500));
        
        assert!(options.persistent);
        assert_eq!(options.priority, Some(5));
        assert_eq!(options.ttl, Some(Duration::from_millis(500)));
        assert_eq!(options.headers.len(), 2);
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    #[test]
    fn test_connection_config() {
        let config = ConnectionConfig::new("amqp://localhost:5672")
            .connection_timeout(60)
            .heartbeat(30);
        
        assert_eq!(config.url, "amqp://localhost:5672");
        assert_eq!(config.connection_timeout, 60);
        assert_eq!(config.heartbeat, 30);
    }
    
    #[test]
    fn test_connection_builder() {
        let builder = ConnectionBuilder::new("amqp://user:pass@localhost:5672/vhost")
            .connection_timeout(45)
            .heartbeat(20);
        
        assert_eq!(builder.config.url, "amqp://user:pass@localhost:5672/vhost");
        assert_eq!(builder.config.connection_timeout, 45);
        assert_eq!(builder.config.heartbeat, 20);
    }
    
    #[test]
    fn test_connection_config_defaults() {
        let config = ConnectionConfig::new("amqp://localhost:5672");
        
        assert_eq!(config.connection_timeout, 30); // Default 30 seconds
        assert_eq!(config.heartbeat, 60); // Default 60 seconds
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let message = TestMessage::new(123, "test content").with_amount(99.99);
        
        // Test serialization
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("123"));
        assert!(json.contains("test content"));
        assert!(json.contains("99.99"));
        
        // Test deserialization
        let deserialized: TestMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, message);
    }
    
    #[test]
    fn test_message_serialization_optional_fields() {
        let message = TestMessage::new(456, "content without amount");
        
        let json = serde_json::to_string(&message).unwrap();
        let deserialized: TestMessage = serde_json::from_str(&json).unwrap();
        
        assert_eq!(deserialized.id, 456);
        assert_eq!(deserialized.content, "content without amount");
        assert_eq!(deserialized.amount, None);
    }
    
    #[test]
    fn test_invalid_json_handling() {
        let invalid_json = r#"{"id": "not_a_number", "content": "test"}"#;
        
        let result = serde_json::from_str::<TestMessage>(invalid_json);
        assert!(result.is_err());
        
        let error = RustRabbitError::from(result.unwrap_err());
        assert!(matches!(error, RustRabbitError::Serialization(_)));
    }
}

#[cfg(test)]
mod integration_api_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_api_ergonomics() {
        // This test ensures the API is ergonomic and compiles correctly
        // It doesn't require a real RabbitMQ connection
        
        // Test that we can create configurations without connection
        let _retry_config = RetryConfig::exponential_default();
        let _publish_options = PublishOptions::new().persistent(true);
        
        // Test that error handling works
        let error = RustRabbitError::Connection("test".to_string());
        assert!(error.is_retryable());
        
        // Test message creation
        let message = TestMessage::new(1, "test");
        let _json = serde_json::to_string(&message).unwrap();
    }
    
    #[test]
    fn test_prelude_imports() {
        // Test that prelude provides all necessary types
        use rust_rabbit::prelude::*;
        
        let _config = RetryConfig::exponential_default();
        let _options = PublishOptions::new();
        let _error = RustRabbitError::Connection("test".to_string());
    }
}

// Mock tests for API design validation
#[cfg(test)]
mod mock_api_tests {
    use super::*;
    
    #[test]
    fn test_consumer_builder_api() {
        // Test that the Consumer builder API is ergonomic
        // This just tests compilation, not actual functionality
        
        async fn mock_consumer_setup() -> Result<(), RustRabbitError> {
            // This would fail at runtime without a real connection
            // but tests the API design
            let connection = Arc::new(
                ConnectionBuilder::new("amqp://localhost:5672")
                    .connection_timeout(30)
                    .heartbeat(60)
                    .connect()
                    .await?
            );
            
            let _consumer = Consumer::builder(connection, "test_queue")
                .retry(RetryConfig::exponential_default())
                .bind_to_exchange("test_exchange")
                .routing_key("test.route")
                .concurrency(5)
                .build()
                .await?;
            
            Ok(())
        }
        
        // Just test that it compiles
        let _ = mock_consumer_setup();
    }
    
    #[test]
    fn test_publisher_api() {
        // Test that the Publisher API is simple and ergonomic
        
        async fn mock_publisher_usage() -> Result<(), RustRabbitError> {
            let connection = Arc::new(
                Connection::new("amqp://localhost:5672").await?
            );
            
            let publisher = Publisher::new(connection);
            let message = TestMessage::new(1, "test");
            let options = PublishOptions::new().persistent(true).priority(5);
            
            // These would fail at runtime without a real connection
            // but test the API design
            publisher.publish_to_exchange("exchange", "routing.key", &message, Some(options.clone())).await?;
            publisher.publish_to_queue("queue", &message, Some(options)).await?;
            
            Ok(())
        }
        
        // Just test that it compiles
        let _ = mock_publisher_usage();
    }
}
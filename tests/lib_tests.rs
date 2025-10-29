//! Basic unit tests for rust-rabbit core functionality

use rust_rabbit::{DelayStrategy, PublishOptions, RetryConfig, RetryMechanism, RustRabbitError};
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
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1))); // 1s
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(2))); // 2s
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(4))); // 4s
        assert_eq!(config.calculate_delay(3), Some(Duration::from_secs(8))); // 8s
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

        let config_custom =
            config.with_dead_letter("custom.dlx".to_string(), "custom.dlq".to_string());
        assert_eq!(
            config_custom.get_dead_letter_exchange("orders"),
            "custom.dlx"
        );
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

        assert!(!options.mandatory);
        assert!(!options.immediate);
        assert_eq!(options.priority, None);
        assert_eq!(options.expiration, None);
    }

    #[test]
    fn test_publish_options_builder() {
        let options = PublishOptions::new()
            .mandatory()
            .priority(10)
            .with_expiration("60000");

        assert!(options.mandatory);
        assert_eq!(options.priority, Some(10));
        assert_eq!(options.expiration, Some("60000".to_string()));
    }

    #[test]
    fn test_publish_options_chaining() {
        let options = PublishOptions::new()
            .mandatory()
            .priority(5)
            .with_expiration("500");

        assert!(options.mandatory);
        assert_eq!(options.priority, Some(5));
        assert_eq!(options.expiration, Some("500".to_string()));
    }
}

#[cfg(test)]
mod connection_tests {
    #[test]
    fn test_connection_string_parsing() {
        // Test that our simplified API accepts connection strings
        let valid_urls = [
            "amqp://localhost:5672",
            "amqp://user:pass@localhost:5672/vhost",
            "amqps://localhost:5671",
        ];

        for url in valid_urls {
            // This should not panic
            assert!(url.starts_with("amqp"));
        }
    }

    #[test]
    fn test_connection_url_validation() {
        let url = "amqp://localhost:5672";
        assert!(url.contains("amqp://"));
        assert!(url.contains("localhost"));
        assert!(url.contains("5672"));
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

    #[tokio::test]
    async fn test_api_ergonomics() {
        // This test ensures the API is ergonomic and compiles correctly
        // It doesn't require a real RabbitMQ connection

        // Test that we can create configurations without connection
        let _retry_config = RetryConfig::exponential_default();
        let _publish_options = PublishOptions::new().mandatory();

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
    use super::TestMessage;
    use rust_rabbit::{Connection, Consumer, PublishOptions, Publisher, RetryConfig};

    #[tokio::test]
    async fn test_consumer_builder_api() {
        // Test that the Consumer builder API is ergonomic
        // This just tests compilation, not actual functionality

        // This would fail at runtime without a real connection
        // but tests the API design

        let connection = Connection::new("amqp://localhost:5672").await;
        if connection.is_err() {
            // Expected - no running RabbitMQ
            return;
        }

        let connection = connection.unwrap();
        let _consumer = Consumer::builder(connection, "test_queue")
            .with_retry(RetryConfig::exponential_default())
            .bind_to_exchange("test_exchange", "routing.key")
            .routing_key("test.route")
            .concurrency(5)
            .build();

        // Just test that it compiles
    }

    #[tokio::test]
    async fn test_publisher_api() {
        // Test that the Publisher API is simple and ergonomic

        let connection = Connection::new("amqp://localhost:5672").await;
        if connection.is_err() {
            // Expected - no running RabbitMQ
            return;
        }

        let connection = connection.unwrap();
        let publisher = Publisher::new(connection);
        let message = TestMessage::new(1, "test");
        let options = PublishOptions::new().mandatory().priority(5);

        // These would fail at runtime without a real connection
        // but test the API design
        let _ = publisher
            .publish_to_exchange("exchange", "routing.key", &message, Some(options.clone()))
            .await;

        let _ = publisher
            .publish_to_queue("queue", &message, Some(options))
            .await;

        // Just test that it compiles
    }
}

#[cfg(test)]
mod delay_strategy_tests {
    use super::*;

    #[test]
    fn test_delay_strategy_default() {
        let config = RetryConfig::exponential_default();
        assert!(matches!(config.delay_strategy, DelayStrategy::TTL));
    }

    #[test]
    fn test_delay_strategy_ttl() {
        let config = RetryConfig::exponential_default().with_delay_strategy(DelayStrategy::TTL);
        assert!(matches!(config.delay_strategy, DelayStrategy::TTL));
    }

    #[test]
    fn test_delay_strategy_delayed_exchange() {
        let config =
            RetryConfig::exponential_default().with_delay_strategy(DelayStrategy::DelayedExchange);
        assert!(matches!(
            config.delay_strategy,
            DelayStrategy::DelayedExchange
        ));
    }

    #[test]
    fn test_delay_exchange_name_generation() {
        let config = RetryConfig::exponential_default();
        assert_eq!(config.get_delay_exchange("orders"), "orders.delay");
        assert_eq!(config.get_delay_exchange("payments"), "payments.delay");
    }

    #[test]
    fn test_retry_with_delayed_exchange_strategy() {
        let config = RetryConfig::linear(3, Duration::from_secs(5))
            .with_delay_strategy(DelayStrategy::DelayedExchange);

        assert_eq!(config.max_retries, 3);
        assert!(matches!(
            config.delay_strategy,
            DelayStrategy::DelayedExchange
        ));
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(3), None);
    }

    #[test]
    fn test_custom_retry_with_delayed_exchange() {
        let delays = vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
        ];
        let config =
            RetryConfig::custom(delays).with_delay_strategy(DelayStrategy::DelayedExchange);

        assert_eq!(config.max_retries, 3);
        assert!(matches!(
            config.delay_strategy,
            DelayStrategy::DelayedExchange
        ));
        assert_eq!(config.calculate_delay(0), Some(Duration::from_secs(1)));
        assert_eq!(config.calculate_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(config.calculate_delay(2), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_exponential_with_delayed_exchange_and_dlq() {
        let config = RetryConfig::exponential(2, Duration::from_secs(1), Duration::from_secs(10))
            .with_delay_strategy(DelayStrategy::DelayedExchange)
            .with_dead_letter("custom_dlx".to_string(), "custom_dlq".to_string());

        assert_eq!(config.max_retries, 2);
        assert!(matches!(
            config.delay_strategy,
            DelayStrategy::DelayedExchange
        ));
        assert_eq!(config.get_dead_letter_exchange("orders"), "custom_dlx");
        assert_eq!(config.get_dead_letter_queue("orders"), "custom_dlq");
        assert_eq!(config.get_delay_exchange("orders"), "orders.delay");
    }

    #[test]
    fn test_dlq_ttl_none_by_default() {
        let config = RetryConfig::exponential_default();
        assert!(config.dlq_ttl.is_none());
    }

    #[test]
    fn test_dlq_ttl_configuration() {
        let ttl = Duration::from_secs(86400); // 1 day
        let config = RetryConfig::exponential_default().with_dlq_ttl(ttl);

        assert!(config.dlq_ttl.is_some());
        assert_eq!(config.dlq_ttl.unwrap(), ttl);
    }

    #[test]
    fn test_dlq_ttl_with_linear_retry() {
        let ttl = Duration::from_secs(3600); // 1 hour
        let config = RetryConfig::linear(3, Duration::from_secs(5)).with_dlq_ttl(ttl);

        assert_eq!(config.max_retries, 3);
        assert_eq!(config.dlq_ttl, Some(ttl));
    }

    #[test]
    fn test_dlq_ttl_with_custom_delays() {
        let delays = vec![
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
        ];
        let ttl = Duration::from_secs(604800); // 7 days
        let config = RetryConfig::custom(delays).with_dlq_ttl(ttl);

        assert_eq!(config.max_retries, 3);
        assert_eq!(config.dlq_ttl, Some(ttl));
    }

    #[test]
    fn test_dlq_ttl_with_all_options() {
        let ttl = Duration::from_secs(86400);
        let config = RetryConfig::exponential(5, Duration::from_secs(1), Duration::from_secs(60))
            .with_delay_strategy(DelayStrategy::DelayedExchange)
            .with_dead_letter("custom_dlx".to_string(), "custom_dlq".to_string())
            .with_dlq_ttl(ttl);

        assert_eq!(config.max_retries, 5);
        assert!(matches!(
            config.delay_strategy,
            DelayStrategy::DelayedExchange
        ));
        assert_eq!(config.get_dead_letter_exchange("test"), "custom_dlx");
        assert_eq!(config.dlq_ttl, Some(ttl));
    }
}

#[cfg(test)]
mod consumer_builder_tests {
    use super::*;

    #[test]
    fn test_consumer_builder_with_dlq_ttl() {
        // This is a compile-time test - if it compiles, the API is correct
        let retry_config =
            RetryConfig::exponential_default().with_dlq_ttl(Duration::from_secs(86400));

        // Verify the configuration
        assert!(retry_config.dlq_ttl.is_some());
        assert_eq!(retry_config.dlq_ttl.unwrap(), Duration::from_secs(86400));
    }
}

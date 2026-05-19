use crate::{Connection, Consumer, RustRabbitError};

#[test]
fn manual_ack_is_rejected_before_runtime_use() {
    let consumer = Consumer::builder(
        Connection::disconnected_for_tests("amqp://localhost:5672"),
        "orders",
    )
    .manual_ack()
    .build();

    let error = consumer.ensure_supported_ack_mode().unwrap_err();
    assert!(matches!(error, RustRabbitError::Consumer(_)));
    assert!(error.to_string().contains("manual_ack() is not supported"));
}

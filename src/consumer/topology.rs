use super::Consumer;
use crate::error::RustRabbitError;
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel,
};
use tracing::debug;

impl Consumer {
    pub(super) async fn create_retry_queue(
        &self,
        channel: &Channel,
        retry_attempt: u32,
        delay: std::time::Duration,
    ) -> Result<String, RustRabbitError> {
        let retry_queue_name = format!("{}.retry.{}", self.queue_name, retry_attempt);
        let delay_ms = delay.as_millis() as i64;

        let mut args = FieldTable::default();
        args.insert("x-message-ttl".into(), AMQPValue::LongLongInt(delay_ms));
        args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("".into()),
        );
        args.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(self.queue_name.clone().into()),
        );

        channel
            .queue_declare(
                retry_queue_name.clone().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await?;

        debug!(
            "Created retry queue: {} with TTL: {}ms",
            retry_queue_name, delay_ms
        );
        Ok(retry_queue_name)
    }

    pub(super) async fn create_dlq(&self, channel: &Channel) -> Result<String, RustRabbitError> {
        let dlq_name = format!("{}.dlq", self.queue_name);
        let mut args = FieldTable::default();

        if let Some(retry_config) = &self.retry_config {
            if let Some(ttl) = &retry_config.dlq_ttl {
                let ttl_ms = ttl.as_millis() as i64;
                args.insert("x-message-ttl".into(), AMQPValue::LongLongInt(ttl_ms));
                debug!("DLQ TTL: {}ms", ttl_ms);
            }
        }

        channel
            .queue_declare(
                dlq_name.clone().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await?;

        debug!("Created DLQ: {}", dlq_name);
        Ok(dlq_name)
    }

    pub(super) async fn send_to_retry_queue(
        &self,
        channel: &Channel,
        message_data: &[u8],
        retry_attempt: u32,
        delay: std::time::Duration,
    ) -> Result<(), RustRabbitError> {
        let retry_queue_name = self
            .create_retry_queue(channel, retry_attempt, delay)
            .await?;

        channel
            .basic_publish(
                "".into(),
                retry_queue_name.clone().into(),
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await?
            .await?;

        debug!("Sent message to retry queue: {}", retry_queue_name);
        Ok(())
    }

    pub(super) async fn send_to_retry_queue_with_headers(
        &self,
        channel: &Channel,
        message_data: &[u8],
        retry_attempt: u32,
        delay: std::time::Duration,
        headers: FieldTable,
    ) -> Result<(), RustRabbitError> {
        let retry_queue_name = self
            .create_retry_queue(channel, retry_attempt, delay)
            .await?;

        channel
            .basic_publish(
                "".into(),
                retry_queue_name.clone().into(),
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2)
                    .with_headers(headers),
            )
            .await?
            .await?;

        debug!(
            "Sent message to retry queue with headers: {}",
            retry_queue_name
        );
        Ok(())
    }

    pub(super) async fn send_to_dlq_simple(
        &self,
        channel: &Channel,
        message_data: &[u8],
    ) -> Result<(), RustRabbitError> {
        let dlq_name = self.create_dlq(channel).await?;

        channel
            .basic_publish(
                "".into(),
                dlq_name.clone().into(),
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await?
            .await?;

        debug!("Sent message to DLQ: {}", dlq_name);
        Ok(())
    }

    pub(super) async fn create_delay_exchange(
        &self,
        channel: &Channel,
    ) -> Result<String, RustRabbitError> {
        if let Some(retry_config) = &self.retry_config {
            let delay_exchange = retry_config.get_delay_exchange(&self.queue_name);

            let mut args = FieldTable::default();
            args.insert(
                "x-delayed-type".into(),
                AMQPValue::LongString("direct".into()),
            );

            channel
                .exchange_declare(
                    delay_exchange.clone().into(),
                    lapin::ExchangeKind::Custom("x-delayed-message".to_string()),
                    lapin::options::ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    args,
                )
                .await?;

            debug!(
                "Created delay exchange: {} (x-delayed-message type)",
                delay_exchange
            );
            Ok(delay_exchange)
        } else {
            Err(RustRabbitError::Retry(
                "Retry config not configured".to_string(),
            ))
        }
    }

    pub(super) async fn send_to_delay_exchange(
        &self,
        channel: &Channel,
        message_data: &[u8],
        delay: std::time::Duration,
    ) -> Result<(), RustRabbitError> {
        let delay_exchange = self.create_delay_exchange(channel).await?;
        let delay_ms = delay.as_millis() as i64;

        channel
            .basic_publish(
                delay_exchange.clone().into(),
                self.queue_name.clone().into(),
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2)
                    .with_headers({
                        let mut headers = FieldTable::default();
                        headers.insert("x-delay".into(), AMQPValue::LongLongInt(delay_ms));
                        headers
                    }),
            )
            .await?
            .await?;

        debug!(
            "Sent message to delay exchange: {} with delay: {}ms",
            delay_exchange, delay_ms
        );
        Ok(())
    }

    pub(super) async fn send_to_delay_exchange_with_headers(
        &self,
        channel: &Channel,
        message_data: &[u8],
        delay: std::time::Duration,
        mut headers: FieldTable,
    ) -> Result<(), RustRabbitError> {
        let delay_exchange = self.create_delay_exchange(channel).await?;
        let delay_ms = delay.as_millis() as i64;
        headers.insert("x-delay".into(), AMQPValue::LongLongInt(delay_ms));

        channel
            .basic_publish(
                delay_exchange.clone().into(),
                self.queue_name.clone().into(),
                BasicPublishOptions::default(),
                message_data,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2)
                    .with_headers(headers),
            )
            .await?
            .await?;

        debug!(
            "Sent message to delay exchange with headers: {} with delay: {}ms",
            delay_exchange, delay_ms
        );
        Ok(())
    }

    pub(super) async fn setup_infrastructure(
        &self,
        channel: &Channel,
    ) -> Result<(), RustRabbitError> {
        channel
            .queue_declare(
                self.queue_name.clone().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        if let (Some(exchange), Some(routing_key)) = (&self.exchange_name, &self.routing_key) {
            channel
                .queue_bind(
                    self.queue_name.clone().into(),
                    exchange.clone().into(),
                    routing_key.clone().into(),
                    lapin::options::QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
        }

        if let Some(retry_config) = &self.retry_config {
            if matches!(
                retry_config.delay_strategy,
                crate::retry::DelayStrategy::DelayedExchange
            ) {
                let delay_exchange = self.create_delay_exchange(channel).await?;
                channel
                    .queue_bind(
                        self.queue_name.clone().into(),
                        delay_exchange.clone().into(),
                        self.queue_name.clone().into(),
                        lapin::options::QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;

                debug!(
                    "Bound queue {} to delay exchange {}",
                    self.queue_name, delay_exchange
                );
            }
        }

        Ok(())
    }
}

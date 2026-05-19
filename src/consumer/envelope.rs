use super::{headers::retry_delay_index, Consumer};
use crate::{
    connection::Connection,
    error::RustRabbitError,
    message::{ErrorType, MessageEnvelope},
    retry::RetryConfig,
};
use futures_lite::stream::StreamExt;
use lapin::{options::QueueDeclareOptions, types::FieldTable};
use serde::de::DeserializeOwned;
use std::{future::Future, sync::Arc};
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

impl Consumer {
    /// Start consuming message envelopes with full retry support
    pub async fn consume_envelopes<T, H, Fut>(&self, handler: H) -> Result<(), RustRabbitError>
    where
        T: DeserializeOwned + Send + Clone + Sync + 'static + serde::Serialize,
        H: Fn(MessageEnvelope<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        self.ensure_supported_ack_mode()?;

        let retry_config = self.retry_config.clone();
        let (channel, mut consumer) = self
            .start_consumer_channel("rust-rabbit-envelope-consumer")
            .await?;

        let semaphore = Arc::new(Semaphore::new(self.prefetch_count as usize));
        debug!(
            "Started consuming envelopes from queue: {}",
            self.queue_name
        );

        while let Some(delivery_result) = consumer.next().await {
            let delivery = delivery_result?;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let handler_clone = handler.clone();
            let auto_ack = self.auto_ack;
            let channel_clone = Arc::new(channel.clone());
            let retry_config_clone = retry_config.clone();
            let queue_name = self.queue_name.clone();
            let connection = self.connection.clone();
            let consumer_self = self.runtime_clone();

            tokio::spawn(async move {
                let _permit = permit;

                match serde_json::from_slice::<MessageEnvelope<T>>(&delivery.data) {
                    Ok(mut envelope) => {
                        debug!(
                            "Processing envelope {} (attempt {}/{})",
                            envelope.metadata.message_id,
                            envelope.metadata.retry_attempt + 1,
                            envelope.metadata.max_retries + 1
                        );

                        match handler_clone(envelope.clone()).await {
                            Ok(()) => {
                                if auto_ack {
                                    consumer_self
                                        .ack_delivery(
                                            &channel_clone,
                                            delivery.delivery_tag,
                                            "successful envelope handling",
                                        )
                                        .await;
                                }
                                debug!(
                                    "Envelope {} processed successfully",
                                    envelope.metadata.message_id
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Handler error for envelope {}: {}",
                                    envelope.metadata.message_id, e
                                );

                                let error_type = classify_error(e.as_ref());
                                envelope = envelope.with_error(
                                    &e.to_string(),
                                    error_type,
                                    Some(&format!("Queue: {}", queue_name)),
                                );

                                if auto_ack {
                                    if let Some(retry_cfg) = &retry_config_clone {
                                        if !envelope.is_retry_exhausted() {
                                            if let Some(delay) = retry_cfg.calculate_delay(
                                                retry_delay_index(envelope.metadata.retry_attempt),
                                            ) {
                                                warn!(
                                                    "Scheduling retry {} for envelope {} with delay {:?}",
                                                    envelope.metadata.retry_attempt + 1,
                                                    envelope.metadata.message_id,
                                                    delay
                                                );

                                                match serde_json::to_vec(&envelope) {
                                                    Ok(retry_payload) => {
                                                        let consumer_self = Consumer {
                                                            connection: connection.clone(),
                                                            queue_name: queue_name.clone(),
                                                            exchange_name: None,
                                                            routing_key: None,
                                                            retry_config: retry_config_clone
                                                                .clone(),
                                                            prefetch_count: 10,
                                                            auto_ack: true,
                                                        };

                                                        let send_result = if matches!(
                                                            retry_config_clone
                                                                .as_ref()
                                                                .map(|c| c.delay_strategy),
                                                            Some(crate::retry::DelayStrategy::DelayedExchange)
                                                        ) {
                                                            consumer_self
                                                                .send_to_delay_exchange(
                                                                    &channel_clone,
                                                                    &retry_payload,
                                                                    delay,
                                                                )
                                                                .await
                                                        } else {
                                                            consumer_self
                                                                .send_to_retry_queue(
                                                                    &channel_clone,
                                                                    &retry_payload,
                                                                    envelope.metadata.retry_attempt,
                                                                    delay,
                                                                )
                                                                .await
                                                        };

                                                        if let Err(e) = send_result {
                                                            error!("Failed to send envelope for retry: {}", e);
                                                            consumer_self
                                                                .nack_delivery(
                                                                    &channel_clone,
                                                                    delivery.delivery_tag,
                                                                    "envelope retry publish failure",
                                                                )
                                                                .await;
                                                            return;
                                                        }

                                                        consumer_self
                                                            .ack_delivery(
                                                                &channel_clone,
                                                                delivery.delivery_tag,
                                                                "envelope retry enqueue",
                                                            )
                                                            .await
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "Failed to serialize envelope for retry: {}",
                                                            e
                                                        );
                                                        consumer_self
                                                            .nack_delivery(
                                                                &channel_clone,
                                                                delivery.delivery_tag,
                                                                "envelope retry serialization",
                                                            )
                                                            .await
                                                    }
                                                }
                                            } else {
                                                Self::send_to_dlq(
                                                    &envelope,
                                                    retry_cfg,
                                                    &connection,
                                                    &queue_name,
                                                )
                                                .await;

                                                consumer_self
                                                    .ack_delivery(
                                                        &channel_clone,
                                                        delivery.delivery_tag,
                                                        "envelope DLQ send",
                                                    )
                                                    .await;
                                            }
                                        } else {
                                            warn!(
                                                "Retry exhausted for envelope {}",
                                                envelope.metadata.message_id
                                            );
                                            Self::send_to_dlq(
                                                &envelope,
                                                retry_cfg,
                                                &connection,
                                                &queue_name,
                                            )
                                            .await;

                                            consumer_self
                                                .ack_delivery(
                                                    &channel_clone,
                                                    delivery.delivery_tag,
                                                    "envelope DLQ send",
                                                )
                                                .await;
                                        }
                                    } else {
                                        consumer_self
                                            .nack_delivery(
                                                &channel_clone,
                                                delivery.delivery_tag,
                                                "envelope handler failure without retry config",
                                            )
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize message envelope: {}", e);
                        if auto_ack {
                            consumer_self
                                .nack_delivery(
                                    &channel_clone,
                                    delivery.delivery_tag,
                                    "envelope deserialization",
                                )
                                .await
                        }
                    }
                }
            });
        }

        Ok(())
    }

    pub(super) async fn send_to_dlq<T>(
        envelope: &MessageEnvelope<T>,
        retry_config: &RetryConfig,
        connection: &Arc<Connection>,
        queue_name: &str,
    ) where
        T: serde::Serialize,
    {
        match connection.create_channel().await {
            Ok(dlq_channel) => {
                let dlq_name = retry_config.get_dead_letter_queue(queue_name);

                if let Err(e) = dlq_channel
                    .queue_declare(
                        dlq_name.clone().into(),
                        QueueDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                {
                    error!("Failed to declare DLQ {}: {}", dlq_name, e);
                    return;
                }

                let failure_summary = envelope.get_failure_summary();
                let dlq_payload = serde_json::json!({
                    "envelope": envelope,
                    "failure_summary": failure_summary,
                    "sent_to_dlq_at": chrono::Utc::now(),
                });

                if let Ok(payload_bytes) = serde_json::to_vec(&dlq_payload) {
                    if let Err(e) = dlq_channel
                        .basic_publish(
                            "".into(),
                            dlq_name.clone().into(),
                            lapin::options::BasicPublishOptions::default(),
                            &payload_bytes,
                            lapin::BasicProperties::default(),
                        )
                        .await
                    {
                        error!("Failed to publish to DLQ {}: {}", dlq_name, e);
                    } else {
                        warn!(
                            "Sent envelope {} to DLQ: {}",
                            envelope.metadata.message_id, failure_summary
                        );
                    }
                }
            }
            Err(e) => {
                error!("Failed to create DLQ channel: {}", e);
            }
        }
    }
}

pub(crate) fn classify_error(error: &(dyn std::error::Error + Send + Sync)) -> ErrorType {
    let error_msg = error.to_string().to_lowercase();

    if error_msg.contains("timeout")
        || error_msg.contains("connection")
        || error_msg.contains("network")
        || error_msg.contains("temporary")
    {
        ErrorType::Transient
    } else if error_msg.contains("rate limit")
        || error_msg.contains("quota")
        || error_msg.contains("resource")
    {
        ErrorType::Resource
    } else if error_msg.contains("validation")
        || error_msg.contains("authentication")
        || error_msg.contains("authorization")
        || error_msg.contains("invalid")
        || error_msg.contains("bad request")
    {
        ErrorType::Permanent
    } else {
        ErrorType::Unknown
    }
}

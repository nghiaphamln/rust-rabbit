use super::{
    headers::{
        read_correlation_id, read_retry_attempt, update_headers_with_retry, HEADER_CORRELATION_ID,
    },
    Consumer,
};
use crate::{error::RustRabbitError, message::MassTransitEnvelope};
use futures_lite::stream::StreamExt;
use lapin::types::AMQPValue;
use serde::{de::DeserializeOwned, Serialize};
use std::{future::Future, sync::Arc};
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

impl Consumer {
    /// Start consuming messages with smart MassTransit detection
    /// Handler receives just the payload type T (no wrapper)
    pub async fn consume<T, H, Fut>(&self, handler: H) -> Result<(), RustRabbitError>
    where
        T: DeserializeOwned + Send + Clone + Sync + 'static + Serialize,
        H: Fn(T) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        self.ensure_supported_ack_mode()?;

        let (channel, mut consumer) = self.start_consumer_channel("").await?;

        let semaphore = Arc::new(Semaphore::new(self.prefetch_count as usize));
        debug!("Started consuming from queue: {}", self.queue_name);

        while let Some(delivery_result) = consumer.next().await {
            let delivery = delivery_result?;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let handler_clone = handler.clone();
            let auto_ack = self.auto_ack;
            let channel_clone = Arc::new(channel.clone());
            let retry_config = self.retry_config.clone();
            let consumer_self = self.runtime_clone();

            tokio::spawn(async move {
                let _permit = permit;
                let delivery_tag = delivery.delivery_tag;
                let properties = delivery.properties;
                let message_data = delivery.data;

                let (payload, correlation_id_from_mt) =
                    match MassTransitEnvelope::from_slice(&message_data) {
                        Ok(mt_envelope) => match mt_envelope.extract_message::<T>() {
                            Ok(data) => {
                                debug!("Detected MassTransit format, extracted payload");
                                (data, mt_envelope.correlation_id().map(|s| s.to_string()))
                            }
                            Err(e) => {
                                error!(
                                    "Failed to extract payload from MassTransit envelope: {}",
                                    e
                                );
                                if auto_ack {
                                    consumer_self
                                        .nack_delivery(
                                            &channel_clone,
                                            delivery_tag,
                                            "malformed MassTransit payload",
                                        )
                                        .await;
                                }
                                return;
                            }
                        },
                        Err(_) => match serde_json::from_slice::<T>(&message_data) {
                            Ok(data) => {
                                debug!("Direct format detected");
                                (data, None)
                            }
                            Err(e) => {
                                error!("Failed to deserialize message: {}", e);
                                if auto_ack {
                                    consumer_self
                                        .nack_delivery(
                                            &channel_clone,
                                            delivery_tag,
                                            "raw payload deserialization",
                                        )
                                        .await;
                                }
                                return;
                            }
                        },
                    };

                let retry_attempt = read_retry_attempt(&properties);
                let correlation_id =
                    correlation_id_from_mt.or_else(|| read_correlation_id(&properties));

                match handler_clone(payload).await {
                    Ok(()) => {
                        if auto_ack {
                            consumer_self
                                .ack_delivery(&channel_clone, delivery_tag, "successful handling")
                                .await;
                        }
                        debug!("Message processed successfully");
                    }
                    Err(e) => {
                        error!("Handler error: {}", e);
                        if auto_ack {
                            if let Some(retry_cfg) = &retry_config {
                                if retry_attempt < retry_cfg.max_retries {
                                    if let Some(delay) = retry_cfg.calculate_delay(retry_attempt) {
                                        warn!(
                                            "Scheduling retry {} with delay {:?} for message",
                                            retry_attempt + 1,
                                            delay
                                        );

                                        let mut updated_headers = update_headers_with_retry(
                                            properties.headers().as_ref(),
                                            retry_attempt + 1,
                                        );

                                        if let Some(corr_id) = &correlation_id {
                                            updated_headers.insert(
                                                HEADER_CORRELATION_ID.into(),
                                                AMQPValue::LongString(corr_id.clone().into()),
                                            );
                                        }

                                        let send_result = if matches!(
                                            retry_cfg.delay_strategy,
                                            crate::retry::DelayStrategy::DelayedExchange
                                        ) {
                                            consumer_self
                                                .send_to_delay_exchange_with_headers(
                                                    &channel_clone,
                                                    &message_data,
                                                    delay,
                                                    updated_headers,
                                                )
                                                .await
                                        } else {
                                            consumer_self
                                                .send_to_retry_queue_with_headers(
                                                    &channel_clone,
                                                    &message_data,
                                                    retry_attempt + 1,
                                                    delay,
                                                    updated_headers,
                                                )
                                                .await
                                        };

                                        if let Err(e) = send_result {
                                            error!("Failed to send retry message: {}", e);
                                            consumer_self
                                                .nack_delivery(
                                                    &channel_clone,
                                                    delivery_tag,
                                                    "retry publish failure",
                                                )
                                                .await;
                                            return;
                                        }

                                        consumer_self
                                            .ack_delivery(
                                                &channel_clone,
                                                delivery_tag,
                                                "retry enqueue",
                                            )
                                            .await
                                    } else {
                                        warn!("Retry exhausted, sending to DLQ");
                                        if let Err(e) = consumer_self
                                            .send_to_dlq_simple(&channel_clone, &message_data)
                                            .await
                                        {
                                            error!("Failed to send to DLQ: {}", e);
                                        }
                                        consumer_self
                                            .ack_delivery(&channel_clone, delivery_tag, "DLQ send")
                                            .await;
                                    }
                                } else {
                                    warn!("Max retries reached, sending to DLQ");
                                    if let Err(e) = consumer_self
                                        .send_to_dlq_simple(&channel_clone, &message_data)
                                        .await
                                    {
                                        error!("Failed to send to DLQ: {}", e);
                                    }
                                    consumer_self
                                        .ack_delivery(&channel_clone, delivery_tag, "DLQ send")
                                        .await;
                                }
                            } else {
                                consumer_self
                                    .nack_delivery(
                                        &channel_clone,
                                        delivery_tag,
                                        "handler failure without retry config",
                                    )
                                    .await;
                            }
                        }
                    }
                }
            });
        }

        Ok(())
    }
}

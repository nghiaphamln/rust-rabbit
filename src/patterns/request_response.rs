use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::RustRabbitError;

/// Correlation ID for request-response tracking
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Create a new unique correlation ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create from existing string
    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    /// Get the correlation ID as string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Request message with correlation ID and timeout
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMessage {
    pub correlation_id: CorrelationId,
    pub reply_to: String,
    pub payload: Vec<u8>,
    pub timeout: Duration,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl RequestMessage {
    pub fn new(payload: Vec<u8>, reply_to: String, timeout: Duration) -> Self {
        Self {
            correlation_id: CorrelationId::new(),
            reply_to,
            payload,
            timeout,
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = correlation_id;
        self
    }
}

/// Response message with correlation ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub correlation_id: CorrelationId,
    pub payload: Vec<u8>,
    pub success: bool,
    pub error_message: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ResponseMessage {
    pub fn success(correlation_id: CorrelationId, payload: Vec<u8>) -> Self {
        Self {
            correlation_id,
            payload,
            success: true,
            error_message: None,
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn error(correlation_id: CorrelationId, error: String) -> Self {
        Self {
            correlation_id,
            payload: Vec::new(),
            success: false,
            error_message: Some(error),
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Pending request tracker
#[derive(Debug)]
struct PendingRequest {
    sender: oneshot::Sender<ResponseMessage>,
    created_at: Instant,
    timeout: Duration,
}

/// Request-Response client for handling RPC-style messaging
#[derive(Debug)]
pub struct RequestResponseClient {
    pending_requests: Arc<Mutex<HashMap<CorrelationId, PendingRequest>>>,
    default_timeout: Duration,
}

impl RequestResponseClient {
    pub fn new(default_timeout: Duration) -> Self {
        let client = Self {
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
        };

        // Start cleanup task for expired requests
        let pending_requests = client.pending_requests.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                Self::cleanup_expired_requests(&pending_requests).await;
            }
        });

        client
    }

    /// Send a request and wait for response
    pub async fn send_request(
        &self,
        payload: Vec<u8>,
        reply_to: String,
        timeout: Option<Duration>,
    ) -> Result<ResponseMessage> {
        let timeout = timeout.unwrap_or(self.default_timeout);
        let request = RequestMessage::new(payload, reply_to, timeout);
        let correlation_id = request.correlation_id.clone();

        let (sender, receiver) = oneshot::channel();
        let pending_request = PendingRequest {
            sender,
            created_at: Instant::now(),
            timeout,
        };

        // Store pending request
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(correlation_id.clone(), pending_request);
        }

        debug!(
            correlation_id = %correlation_id,
            timeout_ms = timeout.as_millis(),
            "Registered pending request"
        );

        // TODO: Send actual request message via RabbitMQ
        // This will be integrated with the main RustRabbit client

        // Wait for response with timeout
        tokio::select! {
            result = receiver => {
                match result {
                    Ok(response) => {
                        info!(
                            correlation_id = %correlation_id,
                            success = response.success,
                            "Received response"
                        );
                        Ok(response)
                    }
                    Err(_) => {
                        warn!(correlation_id = %correlation_id, "Response channel closed");
                        Err(RustRabbitError::RequestTimeout.into())
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                // Remove from pending requests on timeout
                {
                    let mut pending = self.pending_requests.lock().unwrap();
                    pending.remove(&correlation_id);
                }
                error!(correlation_id = %correlation_id, "Request timeout");
                Err(RustRabbitError::RequestTimeout.into())
            }
        }
    }

    /// Handle incoming response message
    pub async fn handle_response(&self, response: ResponseMessage) -> Result<()> {
        let correlation_id = response.correlation_id.clone();

        let sender = {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.remove(&correlation_id)
        };

        if let Some(pending_request) = sender {
            debug!(
                correlation_id = %correlation_id,
                "Forwarding response to pending request"
            );

            if pending_request.sender.send(response).is_err() {
                warn!(
                    correlation_id = %correlation_id,
                    "Failed to send response - receiver dropped"
                );
            }
        } else {
            warn!(
                correlation_id = %correlation_id,
                "Received response for unknown correlation ID"
            );
        }

        Ok(())
    }

    /// Get pending request count (for monitoring)
    pub fn pending_count(&self) -> usize {
        self.pending_requests.lock().unwrap().len()
    }

    /// Cleanup expired requests
    async fn cleanup_expired_requests(
        pending_requests: &Arc<Mutex<HashMap<CorrelationId, PendingRequest>>>,
    ) {
        let now = Instant::now();
        let mut expired_ids = Vec::new();

        {
            let pending = pending_requests.lock().unwrap();
            for (correlation_id, request) in pending.iter() {
                if now.duration_since(request.created_at) > request.timeout {
                    expired_ids.push(correlation_id.clone());
                }
            }
        }

        if !expired_ids.is_empty() {
            let mut pending = pending_requests.lock().unwrap();
            for correlation_id in expired_ids {
                if let Some(expired_request) = pending.remove(&correlation_id) {
                    let _ = expired_request.sender.send(ResponseMessage::error(
                        correlation_id.clone(),
                        "Request timeout".to_string(),
                    ));

                    warn!(
                        correlation_id = %correlation_id,
                        "Cleaned up expired request"
                    );
                }
            }
        }
    }
}

/// Request-Response server for handling incoming requests
pub struct RequestResponseServer {
    handler: Arc<dyn RequestHandler + Send + Sync>,
}

/// Trait for handling incoming requests
#[async_trait::async_trait]
pub trait RequestHandler {
    async fn handle_request(&self, request: RequestMessage) -> Result<ResponseMessage>;
}

impl RequestResponseServer {
    pub fn new(handler: Arc<dyn RequestHandler + Send + Sync>) -> Self {
        Self { handler }
    }

    /// Process incoming request and generate response
    pub async fn process_request(&self, request: RequestMessage) -> Result<ResponseMessage> {
        let correlation_id = request.correlation_id.clone();

        debug!(
            correlation_id = %correlation_id,
            "Processing incoming request"
        );

        let start_time = Instant::now();
        let response = self.handler.handle_request(request).await;
        let processing_time = start_time.elapsed();

        match &response {
            Ok(resp) => {
                info!(
                    correlation_id = %correlation_id,
                    processing_time_ms = processing_time.as_millis(),
                    success = resp.success,
                    "Request processed"
                );
            }
            Err(err) => {
                error!(
                    correlation_id = %correlation_id,
                    processing_time_ms = processing_time.as_millis(),
                    error = %err,
                    "Request processing failed"
                );
                return Ok(ResponseMessage::error(correlation_id, err.to_string()));
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    struct TestHandler;

    #[async_trait::async_trait]
    impl RequestHandler for TestHandler {
        async fn handle_request(&self, request: RequestMessage) -> Result<ResponseMessage> {
            let payload = format!("Echo: {}", String::from_utf8_lossy(&request.payload));
            Ok(ResponseMessage::success(
                request.correlation_id,
                payload.into_bytes(),
            ))
        }
    }

    #[tokio::test]
    async fn test_correlation_id_generation() {
        let id1 = CorrelationId::new();
        let id2 = CorrelationId::new();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn test_request_message_creation() {
        let payload = b"test message".to_vec();
        let request = RequestMessage::new(
            payload.clone(),
            "reply.queue".to_string(),
            Duration::from_secs(30),
        );

        assert_eq!(request.payload, payload);
        assert_eq!(request.reply_to, "reply.queue");
        assert_eq!(request.timeout, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_response_creation() {
        let correlation_id = CorrelationId::new();
        let payload = b"response".to_vec();

        let success_response = ResponseMessage::success(correlation_id.clone(), payload.clone());
        assert!(success_response.success);
        assert_eq!(success_response.correlation_id, correlation_id);
        assert_eq!(success_response.payload, payload);

        let error_response = ResponseMessage::error(correlation_id.clone(), "Error".to_string());
        assert!(!error_response.success);
        assert_eq!(error_response.error_message, Some("Error".to_string()));
    }

    #[tokio::test]
    async fn test_request_response_server() {
        let handler = Arc::new(TestHandler);
        let server = RequestResponseServer::new(handler);

        let request = RequestMessage::new(
            b"hello".to_vec(),
            "reply.queue".to_string(),
            Duration::from_secs(30),
        );
        let correlation_id = request.correlation_id.clone();

        let response = server.process_request(request).await.unwrap();
        assert_eq!(response.correlation_id, correlation_id);
        assert!(response.success);
        assert_eq!(String::from_utf8_lossy(&response.payload), "Echo: hello");
    }

    #[tokio::test]
    async fn test_pending_requests_cleanup() {
        let client = RequestResponseClient::new(Duration::from_millis(100));

        // Send request with short timeout
        let result = client
            .send_request(
                b"test".to_vec(),
                "reply.queue".to_string(),
                Some(Duration::from_millis(50)),
            )
            .await;

        // Should timeout
        assert!(result.is_err());

        // Wait for cleanup
        sleep(Duration::from_millis(200)).await;

        // Should have 0 pending requests
        assert_eq!(client.pending_count(), 0);
    }
}

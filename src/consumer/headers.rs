use lapin::{
    types::{AMQPValue, FieldTable},
    BasicProperties,
};

pub(super) const HEADER_RETRY_ATTEMPT: &str = "x-retry-attempt";
pub(super) const HEADER_CORRELATION_ID: &str = "x-correlation-id";

pub(super) fn read_retry_attempt(properties: &BasicProperties) -> u32 {
    if let Some(headers) = properties.headers() {
        for (header_key, value) in headers {
            if header_key.as_str() == HEADER_RETRY_ATTEMPT {
                if let AMQPValue::LongLongInt(attempt) = value {
                    return *attempt as u32;
                }
            }
        }
    }
    0
}

pub(super) fn read_correlation_id(properties: &BasicProperties) -> Option<String> {
    if let Some(headers) = properties.headers() {
        for (header_key, value) in headers {
            if header_key.as_str() == HEADER_CORRELATION_ID {
                if let AMQPValue::LongString(corr_id) = value {
                    return Some(corr_id.to_string());
                }
            }
        }
    }
    None
}

pub(super) fn update_headers_with_retry(
    existing_headers: Option<&FieldTable>,
    retry_attempt: u32,
) -> FieldTable {
    let mut headers = match existing_headers {
        Some(h) => h.clone(),
        None => FieldTable::default(),
    };
    headers.insert(
        HEADER_RETRY_ATTEMPT.into(),
        AMQPValue::LongLongInt(retry_attempt as i64),
    );
    headers
}

pub(super) fn retry_delay_index(retry_attempt: u32) -> u32 {
    retry_attempt.saturating_sub(1)
}

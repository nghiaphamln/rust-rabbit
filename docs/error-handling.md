# Error Handling

`rust-rabbit` uses `RustRabbitError` for library errors and treats handler errors as retry input when a consumer has `RetryConfig`.

## Library Errors

```rust
use rust_rabbit::RustRabbitError;
```

Main categories:

- `Connection`
- `Protocol`
- `Serialization`
- `Configuration`
- `Consumer`
- `Publisher`
- `Retry`
- `Io`

Helpers:

- `error.is_retryable()`
- `error.is_connection_error()`
- `error.user_message()`

## Consumer Errors

Your handler controls the message outcome:

```rust,no_run
consumer.consume(|order: Order| async move {
    if order.amount <= 0.0 {
        return Err("invalid order amount".into());
    }

    Ok(())
}).await?;
```

- Return `Ok(())` to acknowledge success.
- Return `Err(...)` to trigger retry or DLQ, depending on `RetryConfig`.

## What to Retry

Retry:

- temporary network failures
- downstream timeouts
- broker or dependency outages

Do not retry:

- invalid payloads
- missing required fields
- permanent business rule failures

## Recommended Pattern

Add enough context for logs and DLQ inspection, but keep messages short:

```rust,no_run
consumer.consume(|order: Order| async move {
    process_order(&order)
        .await
        .map_err(|e| format!("order {} failed: {}", order.id, e).into())
}).await?;
```

## Envelope Consumers

Use `consume_envelopes()` when you want retry history and failure metadata in the payload itself. `MessageEnvelope<T>` keeps `retry_attempt`, timestamps, headers, and error history.

# Best Practices

## Keep the API Narrow

Reuse one `Connection` and create publishers from it:

```rust,no_run
let connection = Connection::new("amqp://localhost:5672").await?;
let publisher = Publisher::new(connection.clone());
```

This matches how the library is designed and keeps connection management predictable.

## Pick the Right Consumer Style

- Use `consume()` for plain payloads.
- Use `consume_envelopes()` when the application needs retry metadata and error history.

Do not build business logic around `manual_ack()`. The method exists on the builder, but current runtime behavior rejects it because handlers do not receive an ack handle.

## Keep Handlers Small

Handlers should validate input, call business logic, and return a clear result:

```rust,no_run
consumer.consume(|order: Order| async move {
    validate(&order)?;
    save(&order).await?;
    Ok(())
}).await?;
```

Move retries for local dependencies inside the business function only when you need more control than queue-level retry.

## Tune With Measurements

- Start with `with_prefetch(5)` or `with_prefetch(10)`.
- Increase only after measuring throughput.
- Use exponential retry by default.

## Operational Defaults

- Enable tracing with `init_tracing()` in applications that do not already configure `tracing`.
- Keep examples and docs aligned with actual API behavior.
- Run `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, and `cargo audit` before release work.

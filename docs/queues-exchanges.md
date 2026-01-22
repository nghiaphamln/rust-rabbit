# Queues and Exchanges Guide

This guide covers queue and exchange management in rust-rabbit, including automatic declaration, binding strategies, and routing patterns.

## Table of Contents

- [Overview](#overview)
- [Automatic Declaration](#automatic-declaration)
- [Exchange Types](#exchange-types)
- [Routing Patterns](#routing-patterns)
- [Queue Configuration](#queue-configuration)
- [Best Practices](#best-practices)

## Overview

rust-rabbit automatically handles queue and exchange setup while providing flexibility for custom configurations. Understanding RabbitMQ's exchange types and routing helps you design effective messaging patterns.

## Automatic Declaration

rust-rabbit automatically creates queues and exchanges when needed.

### Publisher Auto-Declaration

```rust
use rust_rabbit::Publisher;

let publisher = Publisher::new(connection);

// Auto-creates "order_queue" if it doesn't exist
publisher.publish_to_queue("order_queue", &message, None).await?;

// Uses existing exchange or fails if not found
publisher.publish_to_exchange("order_exchange", "new.order", &message, None).await?;
```

### Consumer Auto-Declaration

```rust
use rust_rabbit::Consumer;

// Auto-creates queue and exchange, binds them together
let consumer = Consumer::builder(connection, "order_queue")
    .bind_to_exchange("order_exchange", "new.order")
    .build();
```

## Exchange Types

### Direct Exchange (Default)

Best for simple routing where messages go to queues with exact routing key matches.

```rust
// Publisher sends to specific routing key
publisher.publish_to_exchange("direct_exchange", "order.new", &message, None).await?;

// Consumer binds queue to specific routing key
let consumer = Consumer::builder(connection, "new_orders")
    .bind_to_exchange("direct_exchange", "order.new")
    .build();
```

Use cases:
- Simple point-to-point messaging
- Task distribution to specific workers
- Direct command routing

### Topic Exchange

Pattern-based routing using wildcards. Messages route based on pattern matching.

```rust
// Publisher sends with routing key
publisher.publish_to_exchange("topic_exchange", "order.created.eu", &message, None).await?;

// Consumer binds with pattern
// * matches exactly one word
// # matches zero or more words
let consumer = Consumer::builder(connection, "eu_orders")
    .bind_to_exchange("topic_exchange", "order.*.eu")
    .build();

let consumer2 = Consumer::builder(connection, "all_orders")
    .bind_to_exchange("topic_exchange", "order.#")
    .build();
```

Wildcards:
- `*` matches exactly one word
- `#` matches zero or more words

Use cases:
- Event-driven architectures
- Multi-criteria routing
- Regional or category-based distribution

### Fanout Exchange

Broadcasts messages to all bound queues. Ignores routing keys.

```rust
// Publisher ignores routing key (can be empty)
publisher.publish_to_exchange("fanout_exchange", "", &message, None).await?;

// All consumers receive the message
let consumer1 = Consumer::builder(connection, "logger_queue")
    .bind_to_exchange("fanout_exchange", "")
    .build();

let consumer2 = Consumer::builder(connection, "analytics_queue")
    .bind_to_exchange("fanout_exchange", "")
    .build();
```

Use cases:
- Broadcasting events
- Logging and monitoring
- Cache invalidation
- Real-time notifications

## Routing Patterns

### Single Consumer Pattern

One queue, one consumer. Simple point-to-point messaging.

```rust
// Producer
publisher.publish_to_queue("orders", &order, None).await?;

// Consumer
let consumer = Consumer::builder(connection, "orders")
    .build();
```

### Multiple Consumer Pattern (Work Queue)

Multiple consumers on same queue. RabbitMQ distributes messages round-robin.

```rust
// Consumer 1
let consumer1 = Consumer::builder(connection, "orders")
    .with_prefetch(5)
    .build();

// Consumer 2 (same queue)
let consumer2 = Consumer::builder(connection, "orders")
    .with_prefetch(5)
    .build();
```

### Publish-Subscribe Pattern

One message goes to multiple consumers via fanout exchange.

```rust
// Publisher
publisher.publish_to_exchange("events", "", &event, None).await?;

// Multiple subscribers
let logger = Consumer::builder(connection, "log_queue")
    .bind_to_exchange("events", "")
    .build();

let analytics = Consumer::builder(connection, "analytics_queue")
    .bind_to_exchange("events", "")
    .build();
```

### Routing Pattern

Messages route to specific queues based on routing key.

```rust
// Publisher with routing key
publisher.publish_to_exchange("logs", "error", &error_log, None).await?;
publisher.publish_to_exchange("logs", "info", &info_log, None).await?;

// Consumer for errors only
let error_consumer = Consumer::builder(connection, "error_logs")
    .bind_to_exchange("logs", "error")
    .build();

// Consumer for all logs
let all_consumer = Consumer::builder(connection, "all_logs")
    .bind_to_exchange("logs", "#")
    .build();
```

## Queue Configuration

### Prefetch Count

Controls how many unacknowledged messages a consumer can have.

```rust
let consumer = Consumer::builder(connection, "orders")
    .with_prefetch(10)  // Process up to 10 messages concurrently
    .build();
```

Guidelines:
- Low prefetch (1-5): Fair distribution, lower throughput
- Medium prefetch (10-50): Balanced performance
- High prefetch (100+): Maximum throughput, uneven distribution

### Durable Queues

rust-rabbit creates durable queues by default. Messages survive broker restarts.

```rust
// Queues are durable by default
let consumer = Consumer::builder(connection, "orders")
    .build();
// Queue "orders" will be durable
```

## Best Practices

### Exchange Naming

Use clear, hierarchical names:

```rust
// Good
"order.events"
"user.commands"
"payment.notifications"

// Avoid
"exchange1"
"temp"
"test"
```

### Queue Naming

Use descriptive names that indicate purpose:

```rust
// Good
"order_processor"
"email_sender"
"payment_validator"

// Avoid
"queue1"
"worker"
"temp_queue"
```

### Routing Key Patterns

Use structured, dot-separated keys:

```rust
// Good
"order.created.us"
"user.updated.premium"
"payment.failed.retry"

// Avoid
"orderCreatedUS"
"user_update"
"payment-fail"
```

### Multiple Bindings

Bind queue to multiple routing keys:

```rust
let consumer = Consumer::builder(connection, "urgent_orders")
    .bind_to_exchange("orders", "order.urgent.#")
    .bind_to_exchange("orders", "order.*.high_value")
    .build();
```

### Error Handling

Always configure retry and DLQ:

```rust
let consumer = Consumer::builder(connection, "orders")
    .with_retry(RetryConfig::exponential_default())
    .bind_to_exchange("orders", "order.new")
    .build();
```

## Common Patterns

### Event Sourcing

```rust
// Publish events to topic exchange
publisher.publish_to_exchange(
    "domain.events",
    "order.created",
    &event,
    None
).await?;

// Consumers subscribe to event patterns
let order_consumer = Consumer::builder(connection, "order_service")
    .bind_to_exchange("domain.events", "order.#")
    .build();

let analytics_consumer = Consumer::builder(connection, "analytics")
    .bind_to_exchange("domain.events", "#")
    .build();
```

### MassTransit Interoperability

Consume messages from C# MassTransit services. rust-rabbit automatically detects and unwraps MassTransit envelopes:

```rust
// Consumer automatically unwraps MassTransit envelope
let consumer = Consumer::builder(connection, "order_queue")
    .bind_to_exchange("order-exchange", "order.created")
    .with_retry(RetryConfig::exponential_default())
    .build();

consumer.consume(|msg: OrderMessage| async move {
    // msg is the unwrapped payload, not the envelope
    println!("Order ID: {}", msg.order_id);
    println!("Amount: ${:.2}", msg.amount);
    
    // Process order
    save_to_database(&msg).await?;
    Ok(())
}).await?;
```

For access to envelope metadata (correlation ID, timestamps, etc.), use `consume_envelopes()`:

```rust
use rust_rabbit::MessageEnvelope;

consumer.consume_envelopes(|envelope: MessageEnvelope<OrderMessage>| async move {
    println!("Message ID: {}", envelope.metadata.message_id);
    println!("Correlation ID: {:?}", envelope.metadata.correlation_id);
    println!("Timestamp: {:?}", envelope.metadata.timestamp);
    
    // Access payload
    let order = envelope.payload;
    process_order(&order).await?;
    Ok(())
}).await?;
```

### Command Queue

```rust
// Send commands to specific service
publisher.publish_to_queue("order_service.commands", &command, None).await?;

// Service processes commands
let consumer = Consumer::builder(connection, "order_service.commands")
    .with_prefetch(5)
    .build();
```

### Multiple Queue Bindings

Bind a single queue to multiple routing keys for flexible message routing:

```rust
// Single queue receives messages from multiple patterns
let consumer = Consumer::builder(connection, "priority_orders")
    .bind_to_exchange("orders", "order.urgent.*")
    .bind_to_exchange("orders", "order.*.high_value")
    .bind_to_exchange("orders", "order.vip.#")
    .with_prefetch(10)
    .build();

consumer.consume(|msg: Order| async move {
    println!("Processing priority order: {}", msg.id);
    Ok(())
}).await?;
```

Note: Call `bind_to_exchange()` multiple times on the same builder to add multiple bindings.

## Troubleshooting

### Messages not routing

- Check exchange type matches routing pattern
- Verify routing key spelling
- Confirm queue is bound to exchange
- Check exchange exists before publishing

### Queue not created

- Verify connection is established
- Check RabbitMQ logs for declaration errors
- Ensure no conflicting queue declarations
- Verify permissions

### Uneven message distribution

- Check prefetch count on consumers
- Verify consumers are actively processing
- Review message processing time
- Consider adjusting prefetch values

## See Also

- [Retry Configuration Guide](retry-guide.md) - Retry and DLQ configuration
- [Error Handling Guide](error-handling.md) - Error handling strategies
- [Best Practices Guide](best-practices.md) - Production deployment patterns

## Quick Reference

### Exchange Types Quick Guide

```rust
// Direct Exchange - exact routing key match
publisher.publish_to_exchange("direct_ex", "order.new", &msg, None).await?;
consumer.bind_to_exchange("direct_ex", "order.new")

// Topic Exchange - pattern matching with wildcards
publisher.publish_to_exchange("topic_ex", "order.created.us", &msg, None).await?;
consumer.bind_to_exchange("topic_ex", "order.*.us")     // * = one word
consumer.bind_to_exchange("topic_ex", "order.#")        // # = zero or more words

// Fanout Exchange - broadcast to all queues
publisher.publish_to_exchange("fanout_ex", "", &msg, None).await?;
consumer.bind_to_exchange("fanout_ex", "")
```

### Multiple Bindings Pattern

```rust
// Single queue, multiple routing patterns
Consumer::builder(connection, "priority_queue")
    .bind_to_exchange("orders", "order.urgent.*")
    .bind_to_exchange("orders", "order.*.high_value")
    .bind_to_exchange("orders", "order.vip.#")
    .build()
```

### Common Prefetch Values

- **1-5**: Fair distribution, slow processing
- **10-20**: Balanced, general purpose (recommended default)
- **50-100**: High throughput, fast processing
- **100+**: Maximum throughput, may cause uneven distribution

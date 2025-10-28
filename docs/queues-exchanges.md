# Exchange & Queue Management Guide

This guide covers queue and exchange management in rust-rabbit, including automatic declaration, binding strategies, and best practices.

## Overview

rust-rabbit automatically handles queue and exchange setup, but provides flexibility when you need custom configurations. Understanding RabbitMQ's exchange types and routing helps you design effective messaging patterns.

## Automatic Declaration

By default, rust-rabbit automatically creates queues and exchanges when needed.

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
    .bind_to_exchange("order_exchange", "new.order")    // Creates exchange if needed
    .build();
```

## Exchange Types and Patterns

### 1. Direct Exchange (Default)

Best for simple routing where messages go to queues with exact routing key matches.

```rust
// Publisher sends to specific routing key
publisher.publish_to_exchange("direct_exchange", "order.new", &message, None).await?;

// Consumer binds queue to specific routing key
let consumer = Consumer::builder(connection, "new_orders")
    .bind_to_exchange("direct_exchange")
    .routing_key("order.new")              // Only receives "order.new" messages
    .build()
    .await?;
```

**Use cases:**
- Task distribution
- Simple request routing
- Microservice communication

### 2. Topic Exchange

Enables pattern-based routing with wildcards (`*` = one word, `#` = zero or more words).

```rust
// Publisher can send various order events
publisher.publish_to_exchange("topic_exchange", "order.created.urgent", &message, None).await?;
publisher.publish_to_exchange("topic_exchange", "order.updated.normal", &message, None).await?;

// Consumer 1: All urgent orders
let urgent_consumer = Consumer::builder(connection, "urgent_orders")
    .bind_to_exchange("topic_exchange")
    .routing_key("*.*.urgent")             // Matches any urgent messages
    .build()
    .await?;

// Consumer 2: All order events  
let all_orders_consumer = Consumer::builder(connection, "all_orders")
    .bind_to_exchange("topic_exchange")
    .routing_key("order.#")                // Matches all order.* messages
    .build()
    .await?;
```

**Use cases:**
- Event-driven architectures
- Log routing
- Multi-tenant systems

### 3. Fanout Exchange

Broadcasts messages to all bound queues, ignoring routing keys.

```rust
// Publisher broadcasts to all subscribers
publisher.publish_to_exchange("fanout_exchange", "", &message, None).await?; // Routing key ignored

// Multiple consumers receive the same message
let consumer1 = Consumer::builder(connection, "audit_queue")
    .bind_to_exchange("fanout_exchange")
    .build()
    .await?;

let consumer2 = Consumer::builder(connection, "analytics_queue")  
    .bind_to_exchange("fanout_exchange")
    .build()
    .await?;
```

**Use cases:**
- Event broadcasting
- Audit logging
- Real-time analytics

## Queue Configuration

### Durable vs Transient Queues

```rust
// Durable queue (survives broker restart) - DEFAULT
let consumer = Consumer::builder(connection, "persistent_queue")
    .build()
    .await?;

// For manual configuration, queues are durable by default
// Transient queues would need manual declaration
```

### Queue Binding Strategies

```rust
// Simple binding: queue name = routing key
let consumer = Consumer::builder(connection, "orders")
    .bind_to_exchange("order_exchange")
    // routing_key defaults to queue name: "orders"
    .build()
    .await?;

// Custom routing key
let consumer = Consumer::builder(connection, "urgent_orders")
    .bind_to_exchange("order_exchange")
    .routing_key("order.urgent")           // Different from queue name
    .build()
    .await?;

// Multiple bindings (manual setup required)
// This requires custom setup outside rust-rabbit for now
```

## Common Patterns

### 1. Work Queue Pattern

Distribute tasks among multiple workers.

```rust
// Producer
let publisher = Publisher::new(connection);
for task in tasks {
    publisher.publish_to_queue("task_queue", &task, None).await?;
}

// Multiple workers compete for tasks
let worker1 = Consumer::builder(connection, "task_queue")
    .concurrency(1)                        // One task at a time per worker
    .build()
    .await?;

let worker2 = Consumer::builder(connection, "task_queue")
    .concurrency(1)
    .build()
    .await?;
```

### 2. Publish/Subscribe Pattern

Broadcast events to multiple subscribers.

```rust
// Publisher broadcasts events
let publisher = Publisher::new(connection);
publisher.publish_to_exchange("events", "user.created", &event, None).await?;

// Multiple subscribers
let email_service = Consumer::builder(connection, "email_notifications")
    .bind_to_exchange("events")
    .routing_key("user.#")                 // All user events
    .build()
    .await?;

let analytics_service = Consumer::builder(connection, "user_analytics")
    .bind_to_exchange("events")  
    .routing_key("user.created")           // Only creation events
    .build()
    .await?;
```

### 3. RPC Pattern (Request/Response)

Simple request-response using separate queues.

```rust
// Request sender
let publisher = Publisher::new(connection);
let request_id = uuid::Uuid::new_v4().to_string();

let options = PublishOptions::new()
    .header("reply_to", "response_queue")
    .header("correlation_id", &request_id);

publisher.publish_to_queue("rpc_queue", &request, Some(options)).await?;

// Response handler
let response_consumer = Consumer::builder(connection, "response_queue")
    .build()
    .await?;

// RPC server
let rpc_consumer = Consumer::builder(connection, "rpc_queue")
    .build()
    .await?;

rpc_consumer.consume(|msg: rust_rabbit::Message<RpcRequest>| async move {
    let response = process_request(request).await?;
    
    // Send response back (would need publisher access in real implementation)
    publisher.publish_to_queue("response_queue", &response, None).await?;
    Ok(())
}).await?;
```

## Manual Queue Management

For advanced use cases, you may need manual queue management:

```rust
use lapin::{Channel, options::*, types::FieldTable, ExchangeKind};

async fn setup_custom_topology(connection: &Connection) -> Result<(), Error> {
    let channel = connection.create_channel().await?;
    
    // Create custom exchange
    channel.exchange_declare(
        "custom_exchange",
        ExchangeKind::Topic,
        ExchangeDeclareOptions {
            durable: true,
            auto_delete: false,
            internal: false,
            passive: false,
            nowait: false,
        },
        FieldTable::default(),
    ).await?;
    
    // Create custom queue
    channel.queue_declare(
        "custom_queue",
        QueueDeclareOptions {
            durable: true,
            exclusive: false,
            auto_delete: false,
            passive: false,
            nowait: false,
        },
        FieldTable::default(),
    ).await?;
    
    // Bind queue to exchange
    channel.queue_bind(
        "custom_queue",
        "custom_exchange", 
        "custom.routing.key",
        QueueBindOptions::default(),
        FieldTable::default(),
    ).await?;
    
    Ok(())
}

// Use with Consumer
let consumer = Consumer::builder(connection, "custom_queue")
    .manual_declare()                      // Skip auto-declaration
    .build()
    .await?;
```

## Best Practices

### 1. Naming Conventions

```rust
// Good naming patterns
"orders"                   // Simple queue names
"order.created"           // Event-style routing keys  
"user.service.queue"      // Service-specific queues
"analytics.events.dlq"    // Dead letter queues

// Avoid
"Queue1"                  // Non-descriptive
"super_long_queue_name_that_is_hard_to_read"  // Too long
"queue-with-dashes"       // Use dots or underscores
```

### 2. Exchange Design

```rust
// Organize by domain
"order.events"            // Order-related events
"user.events"             // User-related events
"payment.commands"        // Payment commands

// Use appropriate exchange types
let events = "events";     // Fanout for broadcasting
let commands = "commands"; // Direct for specific routing
let logs = "logs";         // Topic for pattern matching
```

### 3. Queue Durability

```rust
// Production: Always use durable queues (default)
let consumer = Consumer::builder(connection, "production_queue")
    .build()
    .await?;

// Development: Consider non-durable for testing
// (requires manual setup for now)
```

### 4. Dead Letter Handling

```rust
// Automatic DLQ setup with retry
let consumer = Consumer::builder(connection, "orders")
    .with_retry(RetryConfig::exponential_default())  // Auto-creates orders.dlx/orders.dlq
    .build()
    .await?;

// Monitor DLQs
let dlq_consumer = Consumer::builder(connection, "orders.dlq")
    .manual_declare()                           // DLQ already exists
    .build()
    .await?;

dlq_consumer.consume(|msg: rust_rabbit::Message<FailedMessage>| async move {
    log::error!("Failed message: {:?}", failed_message);
    // Send alert, store for investigation, etc.
    Ok(())
}).await?;
```

## Performance Considerations

### 1. Queue Distribution

```rust
// Distribute load across multiple queues
for i in 0..4 {
    let queue_name = format!("worker_queue_{}", i);
    let consumer = Consumer::builder(connection.clone(), &queue_name)
        .concurrency(10)
        .build()
        .await?;
    
    tokio::spawn(async move {
        consumer.consume(|msg: rust_rabbit::Message<Task>| async move {
            process_task(message).await
        }).await
    });
}
```

### 2. Prefetch and Concurrency

```rust
// High throughput: Higher concurrency
let high_throughput = Consumer::builder(connection, "fast_queue")
    .concurrency(50)                       // Process many messages in parallel
    .build()
    .await?;

// Reliable processing: Lower concurrency  
let reliable = Consumer::builder(connection, "important_queue")
    .concurrency(1)                        // One at a time for reliability
    .build()
    .await?;
```

### 3. Message Size Considerations

```rust
// For large messages, consider:
// 1. Store in external storage, send reference
#[derive(Serialize)]
struct LargeMessageRef {
    id: String,
    storage_url: String,
    checksum: String,
}

// 2. Use message compression (implement custom serialization)
// 3. Split into smaller chunks
```

## Troubleshooting

### Common Issues

1. **Queue not found errors**
   ```rust
   // Ensure auto-declaration is enabled (default)
   let consumer = Consumer::builder(connection, "queue")
       .build()  // auto_declare is true by default
       .await?;
   ```

2. **Messages not routing**
   ```rust
   // Check exchange type and routing key
   let consumer = Consumer::builder(connection, "queue")
       .bind_to_exchange("exchange")
       .routing_key("exact.routing.key")      // Must match publisher
       .build()
       .await?;
   ```

3. **Duplicate message processing**
   ```rust
   // Ensure proper concurrency settings
   let consumer = Consumer::builder(connection, "queue")
       .concurrency(1)                        // Process one at a time if needed
       .build()
       .await?;
   ```

### Monitoring

```rust
// Monitor queue lengths, message rates, etc.
// This requires external monitoring tools like:
// - RabbitMQ Management UI (http://localhost:15672)
// - Prometheus metrics
// - Custom monitoring scripts
```

For more information, see:
- [Retry Configuration Guide](retry-guide.md)
- [Error Handling](error-handling.md)
- [Best Practices](best-practices.md)

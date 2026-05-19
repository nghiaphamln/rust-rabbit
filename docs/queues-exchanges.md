# Queues and Exchanges

`rust-rabbit` keeps RabbitMQ setup simple:

- `publish_to_queue()` declares a durable queue and publishes through the default exchange.
- `publish_to_exchange()` declares a durable topic exchange and publishes with your routing key.
- `Consumer::builder(...).bind_to_exchange(...)` declares the queue and binding on startup.

## Queue-Only Flow

```rust,no_run
publisher.publish_to_queue("orders", &order, None).await?;

let consumer = Consumer::builder(connection, "orders").build();
```

Use this for simple worker queues.

## Exchange Flow

```rust,no_run
publisher
    .publish_to_exchange("domain.events", "order.created", &order, None)
    .await?;

let consumer = Consumer::builder(connection, "billing")
    .bind_to_exchange("domain.events", "order.*")
    .build();
```

Use this for fan-out by routing key pattern.

## Routing Notes

This library declares exchanges as `topic`. Use routing keys accordingly:

- `order.created`
- `order.cancelled`
- `payment.failed`

Useful patterns:

- `order.*` matches one segment
- `order.#` matches any depth below `order`

## Prefetch

```rust,no_run
let consumer = Consumer::builder(connection, "orders")
    .with_prefetch(10)
    .build();
```

- `1-5` for slow handlers
- `10-50` for balanced workloads
- `50+` only after measuring throughput and fairness

## Naming

Prefer stable, explicit names:

- queues: `orders`, `billing.commands`, `notifications.email`
- exchanges: `domain.events`, `integration.events`
- DLQ defaults: `{queue}.dlq`
- retry queues: `{queue}.retry.{attempt}`

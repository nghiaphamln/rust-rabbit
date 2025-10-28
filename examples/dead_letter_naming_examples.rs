use rust_rabbit::retry::RetryPolicy;
use std::time::Duration;

fn main() {
    println!("🏷️  Dead Letter Exchange/Queue Naming Examples\n");

    // 1. Generic preset names (not recommended for production)
    println!("1️⃣  Generic preset names (for development/testing):");

    let generic_fast = RetryPolicy::fast();
    println!(
        "   Fast policy DLX: {:?}",
        generic_fast.dead_letter_exchange
    );
    println!("   Fast policy DLQ: {:?}", generic_fast.dead_letter_queue);

    let generic_minutes = RetryPolicy::minutes_exponential();
    println!(
        "   Minutes policy DLX: {:?}",
        generic_minutes.dead_letter_exchange
    );
    println!(
        "   Minutes policy DLQ: {:?}",
        generic_minutes.dead_letter_queue
    );
    println!();

    // 2. Queue-specific names (recommended)
    println!("2️⃣  Queue-specific names (recommended for production):");

    let orders_policy = RetryPolicy::fast_for_queue("orders.processing");
    println!("   Orders DLX: {:?}", orders_policy.dead_letter_exchange);
    println!("   Orders DLQ: {:?}", orders_policy.dead_letter_queue);

    let notifications_policy = RetryPolicy::minutes_exponential_for_queue("notifications.email");
    println!(
        "   Notifications DLX: {:?}",
        notifications_policy.dead_letter_exchange
    );
    println!(
        "   Notifications DLQ: {:?}",
        notifications_policy.dead_letter_queue
    );

    let payments_policy = RetryPolicy::slow_for_queue("payments.processing");
    println!(
        "   Payments DLX: {:?}",
        payments_policy.dead_letter_exchange
    );
    println!("   Payments DLQ: {:?}", payments_policy.dead_letter_queue);
    println!();

    // 3. Custom builder names
    println!("3️⃣  Custom builder names (maximum flexibility):");

    // Service-based naming
    let user_service_policy = RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60))
        .backoff_multiplier(2.0)
        .dead_letter_exchange("user-service.dlx")
        .dead_letter_queue("user-service.dlq")
        .build();
    println!(
        "   User Service DLX: {:?}",
        user_service_policy.dead_letter_exchange
    );
    println!(
        "   User Service DLQ: {:?}",
        user_service_policy.dead_letter_queue
    );

    // Environment-based naming
    let prod_policy = RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60))
        .backoff_multiplier(2.0)
        .dead_letter_exchange("prod.orders.dlx")
        .dead_letter_queue("prod.orders.dlq")
        .build();
    println!("   Production DLX: {:?}", prod_policy.dead_letter_exchange);
    println!("   Production DLQ: {:?}", prod_policy.dead_letter_queue);

    // Application-based naming
    let app_policy = RetryPolicy::builder()
        .max_retries(3)
        .initial_delay(Duration::from_secs(2))
        .backoff_multiplier(2.0)
        .dead_letter_exchange("ecommerce.failed")
        .dead_letter_queue("ecommerce.failed.queue")
        .build();
    println!(
        "   Ecommerce App DLX: {:?}",
        app_policy.dead_letter_exchange
    );
    println!("   Ecommerce App DLQ: {:?}", app_policy.dead_letter_queue);

    // No dead letter (discard after retries)
    let no_dlx_policy = RetryPolicy::builder()
        .max_retries(3)
        .initial_delay(Duration::from_secs(1))
        .no_dead_letter() // Messages will be discarded after max retries
        .build();
    println!(
        "   No DLX Policy DLX: {:?}",
        no_dlx_policy.dead_letter_exchange
    );
    println!(
        "   No DLX Policy DLQ: {:?}",
        no_dlx_policy.dead_letter_queue
    );
    println!();

    // 4. Real-world examples
    println!("4️⃣  Real-world naming examples:");

    let examples = vec![
        // E-commerce
        (
            "orders.processing",
            "orders.processing.dlx",
            "orders.processing.dlq",
        ),
        (
            "payments.stripe",
            "payments.stripe.dlx",
            "payments.stripe.dlq",
        ),
        (
            "inventory.updates",
            "inventory.updates.dlx",
            "inventory.updates.dlq",
        ),
        // Notifications
        (
            "notifications.email",
            "notifications.email.dlx",
            "notifications.email.dlq",
        ),
        (
            "notifications.sms",
            "notifications.sms.dlx",
            "notifications.sms.dlq",
        ),
        (
            "notifications.push",
            "notifications.push.dlx",
            "notifications.push.dlq",
        ),
        // User management
        (
            "users.registration",
            "users.registration.dlx",
            "users.registration.dlq",
        ),
        (
            "users.password-reset",
            "users.password-reset.dlx",
            "users.password-reset.dlq",
        ),
        (
            "users.profile-updates",
            "users.profile-updates.dlx",
            "users.profile-updates.dlq",
        ),
        // Analytics
        (
            "analytics.events",
            "analytics.events.dlx",
            "analytics.events.dlq",
        ),
        (
            "analytics.reports",
            "analytics.reports.dlx",
            "analytics.reports.dlq",
        ),
        // File processing
        (
            "files.image-resize",
            "files.image-resize.dlx",
            "files.image-resize.dlq",
        ),
        (
            "files.pdf-generation",
            "files.pdf-generation.dlx",
            "files.pdf-generation.dlq",
        ),
    ];

    for (queue, dlx, dlq) in examples {
        println!("   Queue: {} → DLX: {}, DLQ: {}", queue, dlx, dlq);
    }
    println!();

    // 5. Environment-based examples
    println!("5️⃣  Environment-based naming:");

    let environments = vec![
        ("dev", "orders.processing"),
        ("staging", "orders.processing"),
        ("prod", "orders.processing"),
    ];

    for (env, queue) in environments {
        println!(
            "   {} → DLX: {}.{}.dlx, DLQ: {}.{}.dlq",
            env, env, queue, env, queue
        );
    }
    println!();

    // 6. Best practices summary
    println!("📝 Best Practices:");
    println!("   ✅ Use queue name as base: 'orders.processing' → 'orders.processing.dlx'");
    println!("   ✅ Include environment: 'prod.orders.processing.dlx'");
    println!("   ✅ Keep consistent naming convention across project");
    println!("   ✅ Use descriptive names that indicate purpose");
    println!("   ✅ Consider using project/service prefix");
    println!("   ❌ Don't use generic names like 'my.dlx' in production");
    println!("   ❌ Don't make names too long or complex");
    println!("   ❌ Don't forget to set up monitoring for DLQ");
}

/*
Code examples for different scenarios:

1. Simple queue-based naming:
```rust
let retry_policy = RetryPolicy::fast_for_queue("orders.processing");
// Creates: orders.processing.dlx, orders.processing.dlq
```

2. Environment-aware naming:
```rust
let retry_policy = RetryPolicy::builder()
    .fast_preset()
    .dead_letter_exchange(format!("{}.orders.dlx", env))
    .dead_letter_queue(format!("{}.orders.dlq", env))
    .build();
```

3. Service-based naming:
```rust
let retry_policy = RetryPolicy::builder()
    .minutes_exponential()
    .dead_letter_exchange("user-service.failed")
    .dead_letter_queue("user-service.failed.queue")
    .build();
```

4. Function to generate policy for any queue:
```rust
fn create_retry_policy_for_queue(queue_name: &str, env: &str) -> RetryPolicy {
    RetryPolicy::builder()
        .max_retries(5)
        .initial_delay(Duration::from_secs(60))
        .backoff_multiplier(2.0)
        .dead_letter_exchange(format!("{}.{}.dlx", env, queue_name))
        .dead_letter_queue(format!("{}.{}.dlq", env, queue_name))
        .build()
}
```
*/

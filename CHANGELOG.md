# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.2] - 2026-01-22

### Changed

- Removed all emoji from source code for professional, clean codebase
- Updated all examples and documentation to use plain text formatting
- Improved code readability and maintainability

## [1.2.1] - 2026-01-22

### Added

- **`init_tracing()` Helper Function**: Convenient tracing setup with recommended defaults
  - Automatically filters lapin logs to WARN+ level (eliminates spurious io_loop ERROR logs)
  - Respects `RUST_LOG` environment variable for custom configuration
  - Available via `rust_rabbit::init_tracing()` or in prelude
  - Optional feature flag `tracing` (enabled by default)

### Changed

- All examples now use `rust_rabbit::init_tracing()` for cleaner setup
- Improved documentation examples with better imports and context

### Fixed

- Fixed 3 ignored doc tests - all doc tests now compile and run successfully
- Better error context in doc test examples

## [1.2.0] - 2026-01-22

### Added

- **MassTransit Integration**: Full support for MassTransit message format
  - `MassTransitEnvelope<T>` for envelope structure
  - `MassTransitOptions` configuration with correlation ID, source/destination addresses
  - URN-based message type specification
  - Automatic MassTransit message detection and unwrapping
  - Seamless interoperability with C# MassTransit applications
  - `consume_envelopes()` method to access envelope metadata
- **Advanced Delay Strategies**: Flexible retry timing mechanisms
  - `DelayStrategy` enum with `TTL` and `DelayedExchange` variants
  - TTL-based delays using message expiration (default, no plugin required)
  - x-delayed-message exchange support for precise timing
  - Configurable delay strategy per retry configuration
- **Dead Letter Queue Enhancements**
  - `with_dlq_ttl()` method for automatic DLQ message cleanup
  - Improved DLQ configuration and handling
  - Better poison message detection
- **Documentation Improvements**
  - Complete restructure of README.md (removed emoji, simplified language)
  - New comprehensive user guides:
    - Retry Configuration Guide
    - Queues and Exchanges Guide
    - Error Handling Guide
    - Best Practices Guide
  - Added Quick Reference sections to all guides
  - Multiple Queue Bindings examples
  - MassTransit consumption patterns
  - DelayedExchange plugin setup instructions

### Changed

- Updated documentation to use plain text formatting (no emoji)
- Improved code examples with more practical use cases
- Enhanced error messages for better debugging

### Fixed

- Compatibility with lapin 3.7.2 (Error type changed from enum to struct)
- Fixed error handling to use `error.kind()` method
- Corrected doctest examples with proper type names and field access

## [1.1.0] - 2025-12-15

### Added

- Enhanced documentation with clearer structure
- Comprehensive test coverage across all modules
- API stability improvements

### Changed

- Stabilized core Publisher/Consumer APIs
- Improved error classification and retry logic
- Better error messages and context

### Fixed

- Various clippy warnings addressed
- Code quality improvements
- Minor bug fixes in error handling

## [1.0.0] - 2025-10-01

### Added

- **Simple Publisher API**: Two-method interface (`publish_to_exchange`, `publish_to_queue`)
- **Auto-declaring Consumer**: Automatic queue/exchange setup with retry support
- **Flexible Retry System**: Exponential, linear, and custom delay patterns
  - `RetryConfig::exponential_default()` for exponential backoff
  - `RetryConfig::linear()` for consistent delays
  - `RetryConfig::custom()` for custom delay sequences
  - `RetryConfig::no_retry()` for immediate failure
- **Basic Connection Management**: Simple connection with auto-reconnection
- **Consumer Builder Pattern**: Fluent API for consumer configuration
  - `with_retry()` for retry configuration
  - `bind_to_exchange()` for exchange binding
  - `with_prefetch()` for concurrency control
  - `manual_ack()` for manual acknowledgment
- **Publisher Options**: Configurable message publishing
  - `mandatory()` for mandatory routing
  - `priority()` for message priority
  - `with_expiration()` for TTL
- **Error Handling**: Comprehensive error types and classification
  - `RustRabbitError` with multiple variants
  - `is_retryable()` method for error classification
  - `is_connection_error()` for connection issues
- **Production Features**
  - Persistent messages by default
  - Durable queues and exchanges
  - Proper ACK/NACK handling
  - Built-in retry mechanisms
  - Dead Letter Queue (DLQ) support
- **Examples**: Complete working examples for common scenarios
  - Basic publisher and consumer
  - Retry configurations
  - Production setup
  - DLQ with TTL

### Changed

- Complete library rewrite with simplified API
- Reduced dependencies from 15+ to 7 essential crates
- Focus on simplicity and ease of use

### Removed

- Complex enterprise features (simplified for easier use)
- Advanced event sourcing capabilities
- Multi-broker support (focus on RabbitMQ only)

## [0.x.x] - Legacy Versions

Previous versions before the major simplification revamp. These versions contained complex enterprise features and have been superseded by the simplified 1.x series.

---

## Upgrade Guide

### From 1.2.0 to 1.2.1

No breaking changes. Optionally use the new `init_tracing()` helper:

```rust
// Old way (still works)
tracing_subscriber::fmt().init();

// New way (recommended - filters lapin noise)
rust_rabbit::init_tracing();
```

### From 1.1.x to 1.2.x

No breaking changes. All 1.1.x code continues to work. New features are opt-in:

```rust
// Use new MassTransit integration
let options = PublishOptions::new()
    .with_masstransit("Contracts:OrderCreated");

// Use new delay strategy
let retry_config = RetryConfig::exponential_default()
    .with_delay_strategy(DelayStrategy::DelayedExchange);

// Use new DLQ TTL
let retry_config = RetryConfig::exponential_default()
    .with_dlq_ttl(Duration::from_secs(86400));
```

### From 1.0.x to 1.1.0

No breaking changes. All 1.0.x code continues to work without modifications.

### From 0.x to 1.0

Major breaking changes due to complete API redesign. See migration guide in [ROADMAP.md](ROADMAP.md).

---

For detailed documentation, see:
- [README.md](README.md) - Quick start and overview
- [ROADMAP.md](ROADMAP.md) - Future plans and release schedule
- [docs/](docs/) - Comprehensive user guides

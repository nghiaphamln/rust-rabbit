# Rust Rabbit Roadmap

This document outlines the planned evolution of the rust-rabbit library following its major simplification revamp. The roadmap is organized by priority and estimated complexity.

## Version 1.0 - Simplified Foundation (Released)

### Completed
- **Simple Publisher API**: Two-method interface (`publish_to_exchange`, `publish_to_queue`)
- **Auto-declaring Consumer**: Automatic queue/exchange setup with retry support
- **Flexible Retry System**: Exponential, linear, and custom delay patterns
- **Basic Connection Management**: Simple connection with auto-reconnection
- **Clean Dependencies**: Reduced from 15+ to 7 essential dependencies
- **Comprehensive Documentation**: User guides, examples, and API docs
- **Production Examples**: Real-world usage patterns and best practices

### Goals Achieved
- Simplicity: Easy to use with minimal configuration
- Reliability: Built-in retry mechanisms and error handling
- Flexibility: Configurable retry strategies for different use cases
- Performance: Efficient message processing with configurable concurrency

## Version 1.1 - Stability & Documentation (Released)

### Completed Features
- **Enhanced Documentation**: Complete restructure of README and docs guides
- **Error Handling Improvements**: Better error classification and retry logic
- **Code Quality**: Clean clippy warnings, comprehensive test coverage
- **API Stability**: Stabilized core Publisher/Consumer APIs

## Version 1.2.0 (Current - MassTransit & Advanced Retry)

### Completed Features
- **MassTransit Integration**
  - Full MassTransit message format support
  - Message envelope with metadata (correlation ID, source/destination addresses)
  - URN-based message type specification
  - Seamless interoperability with .NET MassTransit applications
  
- **Advanced Delay Strategies**
  - `DelayStrategy` enum with TTL and DelayedExchange options
  - TTL-based delays using message expiration
  - x-delayed-message exchange support for precise timing
  - Configurable delay strategy per retry configuration
  
- **Dead Letter Queue Enhancements**
  - `with_dlq_ttl()` method for TTL-based message expiration
  - Improved DLQ configuration and handling
  - Better poison message detection

### Goals Achieved
- Cross-platform messaging with .NET ecosystems
- Flexible retry timing mechanisms
- Production-ready DLQ handling

## Version 1.3 - Enhanced Observability & Monitoring (Q2 2026)

### Monitoring & Debugging
- **Basic Metrics Collection**
  - Message throughput (messages/second)
  - Retry counts and failure rates
  - Connection health status
  - Queue depth monitoring
  
- **Structured Logging Enhancement**
  - Consistent log format across all components
  - Configurable log levels per component
  - Request correlation IDs for tracing
  - Performance timing logs
  
- **Health Check Endpoint**
  - Simple HTTP endpoint for health monitoring
  - Connection status reporting
  - Queue connectivity verification
  - Optional Prometheus metrics export

### Circuit Breaker Pattern
- Automatic circuit opening on repeated failures
- Configurable failure thresholds
- Exponential backoff for circuit recovery
- Per-queue circuit breaker state

### Configuration Enhancements
- **Environment-based Configuration**
  - Support for `.env` files
  - Environment variable override patterns
  - Configuration validation and defaults
- **Retry Policy Presets**
  - Pre-configured policies for common scenarios
  - Database operation retry patterns
  - API call retry strategies

## Version 1.4 - Performance Optimization (Q3 2026)

### Performance Features
- **Message Batching**
  - Configurable batch sizes for publishing
  - Time-based and size-based batching
  - Batch compression options
  - Atomic batch processing guarantees
  
- **Connection Pooling**
  - Optional connection pools for high-throughput scenarios
  - Configurable pool size and connection reuse
  - Automatic pool health management
  - Load balancing across connections

- **Memory Optimization**
  - Zero-copy message processing where possible
  - Configurable message buffering strategies
  - Memory usage monitoring and alerts
  - Automatic garbage collection tuning

### Benchmarking & Testing
- **Performance Benchmarks**
  - Standardized performance test suite
  - Throughput and latency measurements
  - Memory usage profiling
  - Comparison with other Rust AMQP libraries

## Version 1.5 - Extended Messaging Patterns (Q4 2026)

### Additional Patterns
- **Request-Response Pattern** (Simplified)
  - Correlation ID-based request matching
  - Timeout handling for responses
  - Multiple response handling
  - Load balancing for request handlers
  
- **Priority Queues**
  - Message priority levels (1-255)
  - Priority-based processing order
  - Queue priority configuration
  - Priority overflow handling
  
- **Message Deduplication**
  - Content-based deduplication
  - Time window deduplication
  - Custom deduplication strategies
  - Redis-backed deduplication store

### Integration Support
- **Framework Integrations**
  - Axum/Warp HTTP server integration
  - Tokio task integration helpers
  - Serde serialization optimizations
  - Custom deserializer support

## Version 2.0 - Cloud-Native Features (2027)

### Cloud Platform Support
- **Container Orchestration**
  - Kubernetes deployment examples
  - Docker Compose configurations
  - Health check integrations
  - Graceful shutdown handling
  
- **Service Discovery**
  - Consul integration
  - etcd support
  - Kubernetes service discovery
  - DNS-based discovery

- **Security Enhancements**
  - TLS/SSL connection encryption
  - SASL authentication mechanisms
  - Certificate-based authentication
  - Connection security auditing

### Multi-Protocol Support
- **Protocol Extensions**
  - MQTT bridge support
  - HTTP webhook publishing
  - gRPC service integration
  - WebSocket real-time streaming

## Version 2.1+ - Advanced Enterprise Features

### Enterprise Considerations
- **Message Transformation**
  - Built-in message format converters
  - Schema validation and evolution
  - Content filtering and routing
  - Message enrichment pipelines
  
- **Distributed Tracing**
  - OpenTelemetry integration
  - Jaeger/Zipkin support
  - Correlation ID propagation
  - Performance trace analysis
  
- **Administrative Tools**
  - CLI management tools
  - Queue inspection utilities
  - Message replay capabilities
  - Configuration management tools

## Principles for Future Development

### Core Values
1. **Simplicity First**: Every feature must have a simple, obvious use case
2. **Backward Compatibility**: Major versions will maintain API stability
3. **Performance Conscious**: Features should not significantly impact base performance
4. **Documentation Driven**: Every feature needs comprehensive documentation and examples
5. **Testing Required**: All features must have unit tests and integration examples

### Explicitly Out of Scope
- **Complex Event Sourcing**: Enterprise-grade event sourcing remains outside core library
- **Multi-Broker Support**: Focus remains on RabbitMQ exclusively
- **GUI Tools**: Command-line and programmatic interfaces only
- **Custom Protocol Implementation**: Stick to standard AMQP 0.9.1

## Community & Contribution

### Contribution Guidelines
- **RFC Process**: Major features require RFC discussion
- **Feature Flags**: New features should be behind feature flags initially
- **Benchmark Requirements**: Performance-impacting changes need benchmarks
- **Documentation First**: Features start with documentation, then implementation

### Communication Channels
- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Architecture discussions and questions
- **Documentation Examples**: Real-world usage examples and patterns

## Migration Path

### Upgrading from Complex Version
For users upgrading from the pre-1.0 complex version:

1. **Assessment Phase**: Identify which enterprise features you actually use
2. **Simplification**: Migrate to basic Publisher/Consumer patterns
3. **Retry Migration**: Convert complex retry logic to new retry configurations
4. **Testing**: Verify behavior with simplified APIs
5. **Performance Validation**: Ensure throughput meets requirements

### Migration Tools (Planned for v1.1)
- **Configuration Converter**: Tool to convert old configurations to new format
- **Feature Usage Analyzer**: Identify which enterprise features are actually used
- **Migration Guide**: Step-by-step migration instructions with examples
- **Compatibility Shims**: Temporary compatibility layer for gradual migration

## Release Schedule

### Estimated Timeline
- **v1.0**: Released (Simplified Foundation)
- **v1.1**: Released (Stability & Documentation)
- **v1.2.0**: Current (MassTransit & Advanced Retry)
- **v1.3**: Q2 2026 (Observability & Monitoring)
- **v1.4**: Q3 2026 (Performance Optimization)
- **v1.5**: Q4 2026 (Extended Patterns)
- **v2.0**: 2027 (Cloud-Native)

### Release Process
- **Monthly Patch Releases**: Bug fixes and minor improvements
- **Quarterly Minor Releases**: New features and enhancements
- **Yearly Major Releases**: Breaking changes and major new capabilities

---

*This roadmap is a living document and will be updated based on community feedback, usage patterns, and emerging requirements. Priority and timeline adjustments will be made based on real-world usage and feedback.*

**Last Updated**: January 2026  
**Next Review**: April 2026
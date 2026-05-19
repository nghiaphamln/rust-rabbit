# Repository Guidelines

## Project Structure & Module Organization
`src/lib.rs` is the crate entrypoint and re-exports the public API. Core modules live in `src/connection.rs`, `src/consumer.rs`, `src/publisher.rs`, `src/retry.rs`, `src/message.rs`, and `src/error.rs`. Integration-style tests are in `tests/lib_tests.rs`. Runnable examples live in `examples/` and should stay buildable because they double as API documentation. Longer guides are in `docs/`, and CI is defined in `.github/workflows/ci.yml`.

## Build, Test, and Development Commands
- `cargo build` - build the library.
- `cargo test` - run the default test suite.
- `cargo test --all-targets` - verify lib tests, integration tests, and example test targets.
- `cargo check --examples` - ensure examples still compile.
- `cargo clippy --all-targets --all-features -- -D warnings` - enforce lint-clean code.
- `cargo fmt --all -- --check` - verify formatting.
- `cargo audit` - check dependency advisories before release-oriented changes.

## Coding Style & Naming Conventions
Use standard Rust formatting with `rustfmt`; do not hand-format around it. Follow existing naming: modules and functions in `snake_case`, types and enums in `CamelCase`, builder-style methods like `with_retry(...)` and `with_dlq_ttl(...)`. Keep public API additions minimal and consistent with `lib.rs` re-exports. Prefer small internal helpers over duplicating RabbitMQ publish/retry logic across modules.

## Testing Guidelines
Add tests for behavioral changes, not only compile coverage. Keep unit tests near the module when they validate internal semantics, and use `tests/lib_tests.rs` for crate-level API expectations. Name tests descriptively, for example `test_retry_exhaustion` or `manual_ack_is_rejected_before_runtime_use`. When changing retry, envelope, or MassTransit behavior, run `cargo test --all-targets` and `cargo check --examples`.

## Commit & Pull Request Guidelines
Follow the existing Conventional Commit style seen in history: `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`. Keep subjects short and imperative, for example `fix: preserve wire format on retry`. PRs should describe the user-visible impact, note any RabbitMQ or dependency implications, and list the verification commands you ran. If examples or docs changed, mention that explicitly.

## Security & Configuration Tips
Do not commit real RabbitMQ credentials. Use local URLs such as `amqp://guest:guest@localhost:5672` in examples only. Treat `cargo audit` warnings seriously; if a finding is only transitive, document the upstream source in the PR.

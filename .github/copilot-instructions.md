# GitHub Copilot Custom Instructions for rust-rabbit

## Project Overview
This project is a Rust library for [brief description of rust-rabbit's purpose]. It provides [main features/capabilities] built using Rust with focus on [performance/reliability/safety].

## Coding Standards and Conventions
- **Language:** Rust
- **Style Guide:** Follow Rust API Guidelines and official Rust Style Guide (rustfmt)
- **Naming Conventions:** 
  - Use `snake_case` for functions, variables, and modules
  - Use `PascalCase` for types, traits, and enums
  - Use `SCREAMING_SNAKE_CASE` for constants
- **Comments:** Provide clear and concise comments for complex logic
- **Documentation:** Generate rustdoc-style documentation (`///`) for all public APIs, functions, and types with examples

## Architectural Principles
- **Design Patterns:** Follow Rust idioms and best practices for library design
- **Modularity:** Ensure code is modular with clear separation of concerns
- **Testing:** Write comprehensive unit tests and integration tests for all new features
- **Error Handling:** Use Result types and custom error types; avoid panics in libraries

## Specific Instructions for Copilot

### Feature Implementation
- **Checklist Completion:** Implement ALL items in feature checklists completely. Do NOT use `// TODO` or `todo!()` as a substitute for implementation.
- **No Incomplete TODOs:** Every checklist item must be fully implemented, tested, and documented before marking as done.

### Code Quality Checks
After implementation is complete, MUST run and verify all of the following:
1. **Code Formatting:** `cargo fmt --check` - Ensure all code follows rustfmt standards
2. **Linting:** `cargo clippy -- -D warnings` - Fix all clippy warnings and ensure no new warnings are introduced
3. **Documentation:** `cargo doc --no-deps --document-private-items` - Verify all public APIs have complete rustdoc comments with examples
4. **Security Audit:** `cargo audit` - Check for known security vulnerabilities in dependencies
5. **Tests:** `cargo test` - All tests must pass
6. **Build:** `cargo build --release` - Ensure clean release build

### Documentation
- Update README.md with:
  - Clear feature descriptions and usage examples
  - API overview and key types
  - Any changes to existing functionality
- Remove any redundant or meaningless markdown sections that don't add value to the library documentation
- Ensure documentation reflects actual implementation accurately

### Code Generation
- Prioritize: **readability, correctness, and maintainability**
- Write idiomatic Rust code following community standards
- Include comprehensive examples in documentation comments

### Refactoring
- Aim for: **simplicity, maintainability, and adherence to Rust best practices**
- Consider memory efficiency and zero-cost abstractions
- Maintain backward compatibility where possible

### Explanations
- Provide: **concise summaries with detailed breakdowns** of Rust-specific concepts
- Explain ownership, borrowing, and trait implications when relevant

### Language Preference
- Respond in **Vietnamese** (Tiếng Việt) for explanations and discussions
- Respond in **English** for code comments and documentation

### Tone
- Maintain a **professional, helpful, and thorough** tone
- Be direct about quality requirements and best practices

## Review Checklist Before Completion
Every feature implementation must satisfy:
- ✅ All checklist items fully implemented (no pending TODOs)
- ✅ `cargo fmt` passes
- ✅ `cargo clippy` passes with no warnings
- ✅ `cargo test` passes with 100% test coverage for new code
- ✅ `cargo doc` generates clean documentation with no warnings
- ✅ `cargo audit` shows no vulnerabilities
- ✅ README.md updated with new features/changes
- ✅ No redundant or meaningless documentation
- ✅ Code follows Rust API guidelines

## Examples and Context
- Refer to existing modules in the repository for preferred patterns
- Maintain consistency with current code style and architecture
- When adding features, ensure they integrate seamlessly with existing APIs
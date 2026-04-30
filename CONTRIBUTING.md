# Contributing to HeliosProxy

Thank you for your interest in contributing to HeliosProxy. This document
covers the development workflow, code standards, and how to submit changes.

## Prerequisites

- **Rust 1.75+** (MSRV)
- **cargo** (comes with Rust)
- **Git**

Optional (for integration tests):

- **PostgreSQL 14+** running locally or in Docker
- **Docker & Docker Compose** for containerized testing

## Building

```bash
# Default features only (pool-modes)
cargo build

# With Transaction Replay
cargo build --features ha-tr

# All proxy features (recommended for development)
cargo build --features all-features

# All features including PostgreSQL topology
cargo build --features all-features,postgres-topology

# Release build
cargo build --release --features all-features
```

## Testing

```bash
# Run all unit tests with all features enabled
cargo test --features all-features

# Run tests for a specific module
cargo test --features routing-hints routing::

# Run ignored integration tests (requires running backend)
cargo test --test integration -- --ignored

# Run benchmarks
cargo bench --features all-features
```

### Feature matrix

CI tests the following feature combinations. Make sure your changes pass
all of them before submitting a PR:

| Combination | Command |
|---|---|
| Default | `cargo test` |
| HA + Transaction Replay | `cargo test --features ha-tr` |
| All features | `cargo test --features all-features` |
| All + PostgreSQL topology | `cargo test --features all-features,postgres-topology` |

## Code Style

### Formatting

All code must pass `cargo fmt`. The CI will reject unformatted code.

```bash
# Check formatting
cargo fmt --check

# Auto-format
cargo fmt
```

### Linting

All code must pass `cargo clippy` with no warnings.

```bash
# Lint with all features
cargo clippy --features all-features -- -D warnings
```

### Conventions

- Use `thiserror` for error types in library code.
- Use `anyhow` only in binary/integration test code.
- Prefer `tracing` over `println!` or `log`.
- Document public items with `///` doc comments.
- Add `#[cfg(feature = "...")]` guards for feature-gated modules.
- Keep modules self-contained: each feature module should work
  independently without requiring other optional features.

## Adding a Feature Module

HeliosProxy uses Cargo feature flags to keep the binary lean. To add a
new feature module:

1. **Create the module** under `src/your_module/`:

   ```
   src/your_module/
       mod.rs       # Module root with re-exports
       config.rs    # Configuration types
       metrics.rs   # Metrics (if applicable)
       ...
   ```

2. **Add the feature flag** to `Cargo.toml`:

   ```toml
   [features]
   your-module = []
   ```

   And include it in the `all-features` bundle:

   ```toml
   all-features = [
       ...,
       "your-module",
   ]
   ```

3. **Gate the module** in `src/lib.rs`:

   ```rust
   #[cfg(feature = "your-module")]
   pub mod your_module;
   ```

4. **Add unit tests** inside the module (inline `#[cfg(test)]` blocks).

5. **Add the feature to the CI matrix** in `.github/workflows/ci.yml`
   if it requires special testing.

6. **Document the feature** in the `README.md` feature table and in
   `CHANGELOG.md` under the next release.

## Pull Request Process

1. **Fork** the repository and create a feature branch:

   ```bash
   git checkout -b feat/your-feature
   ```

2. **Make your changes**, ensuring:
   - All tests pass: `cargo test --features all-features`
   - Code is formatted: `cargo fmt`
   - No clippy warnings: `cargo clippy --features all-features -- -D warnings`
   - MSRV is respected: `cargo check` with Rust 1.75

3. **Write tests** for new functionality. Aim for unit tests in the
   module and integration tests in `tests/integration/` if the feature
   involves network I/O.

4. **Update documentation**:
   - Add a CHANGELOG entry under `## [Unreleased]`.
   - Update README if adding a user-facing feature.

5. **Submit a pull request** against the `main` branch with a clear
   description of what changed and why.

6. **Address review feedback** promptly. PRs require at least one
   approval before merging.

## Project Structure

```
src/
    main.rs              # Binary entry point
    lib.rs               # Library root (feature gates)
    config.rs            # Global configuration
    server.rs            # Proxy server (wire protocol)
    protocol.rs          # PostgreSQL protocol codec
    admin.rs             # Admin REST API
    connection_pool.rs   # Core connection pool
    load_balancer.rs     # Load balancing strategies
    health_checker.rs    # Node health monitoring
    failover_controller.rs  # Automatic failover
    pool/                # Pool modes (session/tx/statement)
    routing/             # Query routing hints
    cache/               # Multi-tier query cache
    lag/                 # Lag-aware routing
    rate_limit/          # Rate limiting
    circuit_breaker/     # Circuit breaker
    analytics/           # Query analytics
    multi_tenancy/       # Multi-tenant isolation
    auth/                # Authentication proxy
    rewriter/            # Query rewriting
    plugins/             # WASM plugin system
    graphql/             # GraphQL gateway
    schema_routing/      # Schema-aware routing
    distribcache/        # Distributed caching
benches/
    pooling.rs           # Pool benchmarks (Criterion)
    routing.rs           # Routing benchmarks (Criterion)
tests/
    integration/mod.rs   # Integration test skeleton
```

## License

By contributing, you agree that your contributions will be licensed
under the same license as the project (Apache-2.0).

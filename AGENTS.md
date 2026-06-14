# Repository Guidelines

## Project Structure & Module Organization

HeliosProxy is a Rust 2021 crate (`heliosdb-proxy`) with MSRV 1.75. Core source lives in `src/`: `main.rs` is the binary entry point, `lib.rs` exposes feature-gated modules, and shared systems such as `config.rs`, `server.rs`, `protocol.rs`, `connection_pool.rs`, and `health_checker.rs` support the proxy runtime. Feature modules are organized by domain, for example `src/routing/`, `src/cache/`, `src/pool/`, `src/graphql/`, `src/schema_routing/`, and `src/distribcache/`. Integration and end-to-end tests live under `tests/`; Criterion benchmarks are in `benches/`. User-facing examples, Docker scenarios, demos, and deployment notes are under `examples/`, `docker/`, `demos/`, `docs/`, `operator/`, and `terraform/`.

## Build, Test, and Development Commands

- `cargo build` builds the default feature set (`pool-modes`).
- `cargo build --features all-features` builds all proxy feature modules.
- `cargo build --release --features all-features` creates an optimized release binary.
- `cargo test` runs the default test suite.
- `cargo test --features all-features` runs tests with the full feature bundle.
- `cargo test --test integration -- --ignored` runs backend-dependent integration tests.
- `cargo bench --features all-features` runs Criterion benchmarks.

## Coding Style & Naming Conventions

Run `cargo fmt` before submitting changes; CI enforces `cargo fmt --check`. Run `cargo clippy --features all-features -- -D warnings` and fix warnings instead of suppressing them. Prefer `thiserror` for library errors, reserve `anyhow` for binaries or integration tests, and use `tracing` rather than `println!`. Name Rust modules and feature flags in lowercase snake/kebab style matching existing examples, such as `schema_routing` and `schema-routing`. Public items should have `///` documentation when they form part of the crate API.

## Testing Guidelines

Put focused unit tests near the code in `#[cfg(test)]` modules. Add integration coverage in `tests/integration/` when behavior crosses network, backend, or process boundaries. Feature-gated code must be guarded with `#[cfg(feature = "...")]` and tested with the relevant feature combination. CI checks default, `ha-tr`, `all-features`, and `all-features,postgres-topology`.

## Commit & Pull Request Guidelines

Recent history uses concise conventional prefixes such as `docs(readme): ...`, `release: ...`, `ci: ...`, `license: ...`, and `chore(lib): ...`. Follow that pattern with a short imperative summary. PRs should describe what changed and why, list validation commands run, link related issues, and update `README.md` or `CHANGELOG.md` for user-facing changes. Include screenshots only for UI or documentation rendering changes.

## Security & Configuration Tips

Do not commit local credentials, database URLs, registry tokens, or generated secrets. Keep reusable configuration examples in `config/`, `examples/`, or `tests/docker/`, and prefer documented feature flags over unconditional dependencies.

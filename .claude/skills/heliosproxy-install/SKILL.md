---
name: heliosproxy-install
description: Install HeliosProxy from crates.io or build from source. Pick feature flags. Verify with `--version`. Use when the user says "install heliosproxy", "set up the proxy", "cargo install heliosdb-proxy", or hits a "command not found" on `heliosdb-proxy`.
allowed-tools: Bash(cargo *), Bash(rustc *), Bash(heliosdb-proxy *), Bash(which *), Read
related: [heliosproxy-overview, heliosproxy-start, heliosproxy-config]
---

# Install HeliosProxy

Two paths: install the published binary from crates.io, or clone
the repo and `cargo build` for development. Default features are
minimal; opt in to feature flags explicitly.

## When to use

- Fresh machine: no `heliosdb-proxy` on `$PATH`
- Need a specific build (TLS, plugins, edge proxy, etc.) compiled in
- Reproducing an issue from a specific version
- Working on the proxy itself (developer install)

🔵 Read-only on the system; reversible (`cargo uninstall heliosdb-proxy`)

## Surfaces

| Surface | When to use |
|---|---|
| `cargo install heliosdb-proxy` | Production / operator install — pulls from crates.io |
| `cargo install --git github.com/dimensigon/HDB-HeliosDB-Proxy` | Pre-release / unreleased commit |
| `git clone … && cargo build --release` | Developer install with `--features` matrix |
| `ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1` | Container deploy (Docker/K8s) |

## Recipes

### Recipe 1: Install latest from crates.io (default features)

```bash
cargo install heliosdb-proxy
heliosdb-proxy --version
# heliosdb-proxy 0.4.1
```

Default features: `pool-modes` only. For more, see Recipe 3.

Requires Rust 1.75+ (MSRV). Check with `rustc --version`. If older,
install Rust via `rustup` first.

### Recipe 2: Install a specific version

```bash
cargo install heliosdb-proxy --version 0.4.1
```

Or pin in `Cargo.toml` as a library dep:

```toml
[dependencies]
heliosdb-proxy = "0.4.1"
```

### Recipe 3: Install with all features

```bash
cargo install heliosdb-proxy --features all-features
```

`all-features` turns on every proxy feature (HA-TR, plugins, anomaly,
edge, multi-tenancy, etc.). Topology providers are exclusive — pass
exactly one of `--features postgres-topology` or
`--features heliosdb-topology` separately:

```bash
cargo install heliosdb-proxy --features "all-features postgres-topology"
```

See [`_index/feature-matrix.md`](../_index/feature-matrix.md) for
which feature unlocks which skill.

### Recipe 4: Developer install from source

```bash
git clone git@github.com:dimensigon/HDB-HeliosDB-Proxy.git
cd HDB-HeliosDB-Proxy
cargo build --release --features all-features
./target/release/heliosdb-proxy --version
```

For a faster compile during iteration, drop `--release`:

```bash
cargo build --features wasm-plugins,ha-tr,anomaly-detection
./target/debug/heliosdb-proxy --version
```

### Recipe 5: Docker image

```bash
docker pull ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1
docker run --rm ghcr.io/dimensigon/hdb-heliosdb-proxy:0.4.1 --version
```

The image is published by `.github/workflows/docker.yml` on every
tag push. For a custom-features image, `git clone` and build locally:

```bash
docker build -t my-heliosproxy:dev -f docker/Dockerfile .
```

### Recipe 6: Verify the install end-to-end

```bash
heliosdb-proxy --help                  # subcommands + flags
heliosdb-proxy --version               # 0.4.1
which heliosdb-proxy                   # ~/.cargo/bin/heliosdb-proxy
ls $(dirname $(which heliosdb-proxy))  # confirm it's on PATH
```

## Pitfalls

- **`error: failed to compile heliosdb-proxy v0.4.1, intermediate
  artifacts can be found at …`** — usually MSRV. Confirm
  `rustc --version` is ≥ 1.75. Install via `rustup default stable`.
- **`linker 'cc' not found`** — install build-essential
  (`apt install build-essential` / `xcode-select --install`).
- **`error: failed to download` / network errors during install** —
  check `~/.cargo/config.toml` for proxy / mirror; if behind a
  firewall, use `cargo install --offline` against a vendored copy.
- **`--features wasm-plugins` adds ~3 MB compiled** (wasmtime
  cranelift JIT). Default off so minimal installs stay tiny. Add
  it only if you'll load WASM plugins.
- **`postgres-topology` and `heliosdb-topology` are mutually
  exclusive.** `cargo` won't error if you enable both at build
  time, but at runtime the topology provider that wins is undefined.
  Pick one.
- **AGPL → Apache-2.0 in v0.4.0+.** If you cloned an old branch or
  pinned a pre-0.4.0 release, the LICENSE file differs.

## See also

- `heliosproxy-start` — once installed, daemonize it
- `heliosproxy-config` — the `proxy.toml` you'll point `--config` at
- `heliosproxy-release` — how new versions land on crates.io
- crates.io: <https://crates.io/crates/heliosdb-proxy>
- docs.rs: <https://docs.rs/heliosdb-proxy>
- Container: `ghcr.io/dimensigon/hdb-heliosdb-proxy`
- Code: [`Cargo.toml`](../../Cargo.toml) (feature flag list)

//! End-to-end test that loads a real WASM plugin produced by the
//! `heliosdb-proxy-plugins` workspace and exercises it through the
//! proxy's `WasmPluginRuntime`.
//!
//! Why this lives in `tests/`:
//!   - Reads a `.wasm` file from disk produced by a separate `cargo
//!     build --target wasm32-unknown-unknown --release` in the
//!     plugins workspace.
//!   - Skipped when the artefact is missing so `cargo test` on a
//!     fresh checkout still works (CI builds the plugins first).
//!
//! What it proves:
//!   - The plugin's exported `alloc` / `dealloc` / `memory` / hook
//!     symbols match the runtime's expectations.
//!   - The host's KV import bridge works end-to-end through the
//!     plugin's wrapper functions in `helios-plugin-abi`.
//!   - The plugin's serde shape matches the proxy's wire types.

#![cfg(feature = "wasm-plugins")]

use std::path::PathBuf;
use std::time::Duration;

use heliosdb_proxy::plugins::{
    HookContext, HookType, PluginManifest, PluginRuntimeConfig, QueryContext,
    WasmPluginRuntime,
};

/// Locate a plugin .wasm built by the sibling plugins workspace.
/// Returns `None` if the artefact is missing — caller should skip.
fn find_plugin_wasm(name: &str) -> Option<PathBuf> {
    let here = std::env::current_dir().ok()?;
    let candidate = here
        .join("../heliosdb-proxy-plugins/target/wasm32-unknown-unknown/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

fn manifest_for(name: &str, wasm_path: &PathBuf, hooks: Vec<HookType>) -> PluginManifest {
    let mut m = PluginManifest::default();
    m.name = name.to_string();
    m.version = "0.1.0".to_string();
    m.license = "AGPL-3.0-only".to_string();
    m.hooks = hooks;
    m.path = wasm_path.clone();
    m
}

#[test]
fn cost_governor_plugin_loads_and_runs_pre_query() {
    let Some(wasm_path) = find_plugin_wasm("helios_plugin_cost_governor") else {
        eprintln!(
            "skipping: helios_plugin_cost_governor.wasm not found — \
             run `cargo build -p helios-plugin-cost-governor \
             --target wasm32-unknown-unknown --release` in the plugins workspace first"
        );
        return;
    };
    let wasm_bytes = std::fs::read(&wasm_path).expect("read wasm bytes");

    let mut config = PluginRuntimeConfig::default();
    config.fuel_metering = false;
    config.timeout = Duration::from_secs(5);
    let runtime = WasmPluginRuntime::new(&config).expect("runtime init");

    let manifest = manifest_for(
        "helios-plugin-cost-governor",
        &wasm_path,
        vec![HookType::PreQuery, HookType::PostQuery],
    );
    let plugin = runtime
        .instantiate(&manifest, &wasm_bytes)
        .expect("instantiate plugin");

    // Build a QueryContext with a tenant_id but no budget seeded —
    // cost-governor returns Continue when no budget is configured.
    let mut ctx = QueryContext {
        query: "SELECT 1".to_string(),
        normalized: "SELECT ?".to_string(),
        tables: vec![],
        is_read_only: true,
        hook_context: HookContext::default(),
    };
    ctx.hook_context
        .attributes
        .insert("tenant_id".into(), "acme".into());
    let ctx_json = serde_json::to_vec(&ctx).expect("serialise ctx");

    let response_bytes = runtime
        .call_hook(&plugin, HookType::PreQuery, &ctx_json)
        .expect("pre_query call");

    let v: serde_json::Value =
        serde_json::from_slice(&response_bytes).expect("decode result");
    assert_eq!(v["kind"], "continue", "got {:?}", v);
}

#[test]
fn cost_governor_blocks_when_budget_exhausted_via_kv() {
    let Some(wasm_path) = find_plugin_wasm("helios_plugin_cost_governor") else {
        eprintln!("skipping: cost-governor wasm not found");
        return;
    };
    let wasm_bytes = std::fs::read(&wasm_path).expect("read wasm");

    let mut config = PluginRuntimeConfig::default();
    config.fuel_metering = false;
    let runtime = WasmPluginRuntime::new(&config).unwrap();

    let manifest = manifest_for(
        "helios-plugin-cost-governor",
        &wasm_path,
        vec![HookType::PreQuery],
    );
    let plugin = runtime.instantiate(&manifest, &wasm_bytes).unwrap();

    // Seed budget + over-budget usage directly into the runtime's KV
    // backend, scoped to the cost-governor's plugin name.
    let plugin_ns = "helios-plugin-cost-governor";
    runtime.kv().set(
        plugin_ns,
        b"tenant:acme:budget".to_vec(),
        br#"{"minute":1.0,"hour":10.0,"day":100.0}"#.to_vec(),
    );
    runtime.kv().set(
        plugin_ns,
        b"tenant:acme:usage".to_vec(),
        br#"{"minute":2.0,"hour":2.0,"day":5.0}"#.to_vec(),
    );

    let mut ctx = QueryContext {
        query: "SELECT 1".to_string(),
        normalized: String::new(),
        tables: vec![],
        is_read_only: true,
        hook_context: HookContext::default(),
    };
    ctx.hook_context
        .attributes
        .insert("tenant_id".into(), "acme".into());
    let ctx_json = serde_json::to_vec(&ctx).unwrap();

    let response = runtime
        .call_hook(&plugin, HookType::PreQuery, &ctx_json)
        .expect("pre_query call");
    let v: serde_json::Value = serde_json::from_slice(&response).expect("decode");
    assert_eq!(v["kind"], "block", "expected block, got {:?}", v);
    let reason = v["reason"].as_str().expect("reason field");
    assert!(
        reason.contains("minute"),
        "reason should mention minute window: {}",
        reason
    );
}

#[test]
fn ai_classifier_writes_request_keys_into_kv() {
    let Some(wasm_path) = find_plugin_wasm("helios_plugin_ai_classifier") else {
        eprintln!("skipping: ai-classifier wasm not found");
        return;
    };
    let wasm_bytes = std::fs::read(&wasm_path).unwrap();

    let mut config = PluginRuntimeConfig::default();
    config.fuel_metering = false;
    let runtime = WasmPluginRuntime::new(&config).unwrap();

    let manifest = manifest_for(
        "helios-plugin-ai-classifier",
        &wasm_path,
        vec![HookType::PreQuery],
    );
    let plugin = runtime.instantiate(&manifest, &wasm_bytes).unwrap();

    let mut ctx = QueryContext {
        query: "/* generated by GPT-4 */ SELECT * FROM users".into(),
        normalized: String::new(),
        tables: vec![],
        is_read_only: true,
        hook_context: HookContext {
            request_id: "req-test-1".into(),
            ..Default::default()
        },
    };
    ctx.hook_context
        .attributes
        .insert("application_name".into(), "gpt-shopper".into());
    let ctx_json = serde_json::to_vec(&ctx).unwrap();

    let _ = runtime
        .call_hook(&plugin, HookType::PreQuery, &ctx_json)
        .expect("classifier pre_query");

    let plugin_ns = "helios-plugin-ai-classifier";
    let ai = runtime
        .kv()
        .get(plugin_ns, b"req:req-test-1:ai_traffic")
        .expect("ai_traffic key written");
    assert_eq!(&ai[..], b"true");
    let agent = runtime
        .kv()
        .get(plugin_ns, b"req:req-test-1:agent_id")
        .expect("agent_id key written");
    assert_eq!(&agent[..], b"gpt-shopper");
}

/// Pack a real .wasm into a .tar.gz artefact via the helios-plugin
/// CLI and load it through the proxy's loader. End-user round-trip
/// proving the FU-27 / FU-28 chain works in production wiring.
#[test]
fn proxy_loads_packed_tar_gz_artefact() {
    use heliosdb_proxy::plugins::PluginLoader;

    let Some(wasm_path) = find_plugin_wasm("helios_plugin_cost_governor") else {
        eprintln!("skipping: cost-governor wasm not found");
        return;
    };

    // Build the artefact by reading the wasm and packing inline (no
    // shelling out to the CLI — keeps the test hermetic). Mirrors
    // exactly what `helios-plugin pack` produces.
    let wasm_bytes = std::fs::read(&wasm_path).unwrap();
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(&wasm_bytes);
    let digest = hasher.finalize();
    let mut wasm_sha = String::new();
    for b in digest.iter() {
        wasm_sha.push_str(&format!("{:02x}", b));
    }
    let manifest_json = serde_json::json!({
        "schema_version": "1.0",
        "name": "helios-plugin-cost-governor",
        "version": "0.1.0",
        "description": "test artefact",
        "license": "AGPL-3.0-only",
        "hooks": ["pre_query", "post_query"],
        "wasm_sha256": wasm_sha,
        "packed_at": "2026-04-25T13:00:00Z",
    });
    let manifest_bytes = serde_json::to_vec(&manifest_json).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let artefact_path = tmp.path().join("cost-governor.tar.gz");
    let f = std::fs::File::create(&artefact_path).unwrap();
    let gz = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut tar_b = tar::Builder::new(gz);
    let mut put = |path: &str, body: &[u8]| {
        let mut h = tar::Header::new_gnu();
        h.set_path(path).unwrap();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar_b.append(&h, body).unwrap();
    };
    put("manifest.json", &manifest_bytes);
    put("plugin.wasm", &wasm_bytes);
    let gz = tar_b.into_inner().unwrap();
    gz.finish().unwrap();

    // Now load via the proxy's loader.
    let loader = PluginLoader::new();
    let (manifest, bytes) = loader.load(&artefact_path).expect("load tar.gz");
    assert_eq!(manifest.name, "helios-plugin-cost-governor");
    assert_eq!(bytes.len(), wasm_bytes.len());

    // Instantiate through the runtime — proves the loaded wasm is
    // actually the same bytes that compile cleanly.
    let mut config = PluginRuntimeConfig::default();
    config.fuel_metering = false;
    let runtime = WasmPluginRuntime::new(&config).unwrap();
    runtime.instantiate(&manifest, &bytes).expect("instantiate");
}

#[test]
fn pgvector_router_returns_node_for_top_k_query() {
    let Some(wasm_path) = find_plugin_wasm("helios_plugin_pgvector_router") else {
        eprintln!("skipping: pgvector-router wasm not found");
        return;
    };
    let wasm_bytes = std::fs::read(&wasm_path).unwrap();

    let mut config = PluginRuntimeConfig::default();
    config.fuel_metering = false;
    let runtime = WasmPluginRuntime::new(&config).unwrap();

    let manifest = manifest_for(
        "helios-plugin-pgvector-router",
        &wasm_path,
        vec![HookType::Route],
    );
    let plugin = runtime.instantiate(&manifest, &wasm_bytes).unwrap();

    let mut ctx = QueryContext {
        query: "SELECT id FROM docs ORDER BY embedding <=> $1 LIMIT 5".into(),
        normalized: String::new(),
        tables: vec![],
        is_read_only: true,
        hook_context: HookContext::default(),
    };
    ctx.hook_context
        .attributes
        .insert("helios.vector_node".into(), "vec-replica-1".into());
    let ctx_json = serde_json::to_vec(&ctx).unwrap();

    let response = runtime
        .call_hook(&plugin, HookType::Route, &ctx_json)
        .expect("route call");
    let v: serde_json::Value = serde_json::from_slice(&response).expect("decode");
    assert_eq!(v["action"], "node", "got {:?}", v);
    assert_eq!(v["target"], "vec-replica-1");
}

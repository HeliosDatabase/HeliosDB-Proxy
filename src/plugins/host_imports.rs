//! Wasmtime-side host imports exposed to WASM plugins.
//!
//! Plugins import these from the `env` module:
//!
//! ```wat
//! (import "env" "kv_get"    (func (param i32 i32 i32 i32) (result i32)))
//! (import "env" "kv_set"    (func (param i32 i32 i32 i32) (result i32)))
//! (import "env" "kv_delete" (func (param i32 i32)         (result i32)))
//! ```
//!
//! The KV namespace is per-plugin: each plugin sees only its own
//! key-value store, keyed off `LoadedPlugin.metadata.name`. State
//! survives across calls because the `KvBackend` is owned by the
//! runtime, not the per-call `Store`.
//!
//! Return-value conventions (i32):
//!
//! - `kv_get`: bytes written, or `-1` for missing key, or `-2` if the
//!   caller's output buffer is too small (caller can retry with a
//!   larger buffer; the value is left intact).
//! - `kv_set`: `0` on success, `-1` on internal error. A configured
//!   cap breach (`kv_max_value_bytes` / `kv_max_keys_per_plugin`) is
//!   surfaced through this same `-1` — the write is rejected and the
//!   store is left unchanged.
//! - `kv_delete`: `0` (idempotent — no error if the key was absent).
//!
//! The implementation is in-process and in-memory. A future slice
//! can swap the backend for a persistent store (sled, redb, …)
//! without changing the import surface.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use wasmtime::{Caller, Linker, Memory};

use super::runtime::PluginError;

/// KV store type alias: plugin-name -> (key -> value)
type KvStore = HashMap<String, HashMap<Vec<u8>, Vec<u8>>>;

/// In-memory KV backend, namespaced by plugin name. The outer map
/// is keyed by plugin name; the inner map by user-supplied key.
///
/// Two optional caps bound how much a caller (plugin or the
/// `/admin/kv` endpoint) can store; `0` on either means "unlimited".
/// `new()` / `Default` leave both at `0` so existing callers and
/// tests keep the historical unbounded behaviour; production wires
/// real values via [`KvBackend::with_limits`].
#[derive(Clone, Default)]
pub struct KvBackend {
    inner: Arc<RwLock<KvStore>>,
    /// Max bytes for any single key OR value (`0` = unlimited). BOTH
    /// the user-supplied key and its value are bounded by this cap so
    /// neither axis can grow without limit.
    max_value_bytes: usize,
    /// Max distinct keys per plugin namespace (`0` = unlimited).
    /// Overwriting an existing key never trips this cap.
    max_keys_per_plugin: usize,
    /// Max distinct plugin namespaces / outer-map entries (`0` =
    /// unlimited). Bounds how many namespaces a caller can bring into
    /// existence — notably the `/admin/kv/<plugin>/<key>` endpoint,
    /// which names an arbitrary `<plugin>` and would otherwise let a
    /// token-holder grow memory without bound by writing to
    /// unboundedly-many namespace names. Writing to an already-present
    /// namespace never trips this cap.
    max_plugins: usize,
}

impl KvBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with explicit caps. `0` on any field = unlimited.
    pub fn with_limits(
        max_value_bytes: usize,
        max_keys_per_plugin: usize,
        max_plugins: usize,
    ) -> Self {
        Self {
            max_value_bytes,
            max_keys_per_plugin,
            max_plugins,
            ..Self::default()
        }
    }

    /// The configured single key/value byte cap (`0` = unlimited).
    /// Lets a caller (e.g. the `/admin/kv` PUT handler) fast-reject an
    /// oversized body before allocating an owned copy of it.
    pub fn max_value_bytes(&self) -> usize {
        self.max_value_bytes
    }

    /// Read a value. None if missing.
    pub fn get(&self, plugin: &str, key: &[u8]) -> Option<Vec<u8>> {
        let g = self.inner.read();
        g.get(plugin).and_then(|m| m.get(key).cloned())
    }

    /// Insert / overwrite. Returns `false` (and leaves the store
    /// untouched) when a configured cap would be exceeded:
    /// - the key OR value length exceeds `max_value_bytes`, or
    /// - creating a NEW plugin namespace would push the store past
    ///   `max_plugins`, or
    /// - inserting a NEW key would push the namespace past
    ///   `max_keys_per_plugin`. Overwriting an existing key (or writing
    ///   another key into an already-present namespace) never fails the
    ///   key-count or namespace cap.
    pub fn set(&self, plugin: &str, key: Vec<u8>, value: Vec<u8>) -> bool {
        // Size cap first — cheap, and lets us bail before locking. Both
        // the key and the value are bounded so neither can grow without
        // limit (the admin request line already caps their transport
        // length, but this makes the retained size tunable).
        if self.max_value_bytes != 0
            && (key.len() > self.max_value_bytes || value.len() > self.max_value_bytes)
        {
            return false;
        }
        let mut g = self.inner.write();
        // Namespace cap: refuse to bring a NEW plugin namespace into
        // existence once the outer map is full. Writing to a namespace
        // that already exists is always allowed (the count stays
        // constant), so loaded plugins are never starved. Checked
        // BEFORE `entry().or_default()` so a rejected write never
        // leaves an empty namespace behind.
        if self.max_plugins != 0 && !g.contains_key(plugin) && g.len() >= self.max_plugins {
            return false;
        }
        let m = g.entry(plugin.to_string()).or_default();
        // Key-count cap applies only to genuinely new keys; an
        // overwrite keeps the namespace size constant, so allow it.
        if self.max_keys_per_plugin != 0
            && !m.contains_key(&key)
            && m.len() >= self.max_keys_per_plugin
        {
            return false;
        }
        m.insert(key, value);
        true
    }

    /// Delete; idempotent. Drops the plugin's inner map and its
    /// outer-map slot once the namespace becomes empty, so a
    /// delete-heavy caller actually reclaims memory instead of leaving
    /// zombie namespaces behind — this also keeps the `max_plugins`
    /// namespace count honest (a fully-drained namespace frees a slot).
    pub fn delete(&self, plugin: &str, key: &[u8]) {
        let mut g = self.inner.write();
        // Compute emptiness inside the `&mut m` scope, then drop that
        // borrow before touching `g` again so the outer-map removal
        // never overlaps the inner borrow.
        let now_empty = match g.get_mut(plugin) {
            Some(m) => {
                m.remove(key);
                m.is_empty()
            }
            None => false,
        };
        if now_empty {
            g.remove(plugin);
        }
    }

    /// Returns the number of keys in the plugin's namespace.
    /// Useful for tests and the admin endpoint.
    pub fn len(&self, plugin: &str) -> usize {
        self.inner.read().get(plugin).map(|m| m.len()).unwrap_or(0)
    }

    /// List keys (lossy UTF-8) in a plugin's namespace, optionally
    /// filtered by a byte `prefix` (pass `b""` for all keys). Backs
    /// the `GET /admin/kv/<plugin>/` list endpoint.
    pub fn list_keys(&self, plugin: &str, prefix: &[u8]) -> Vec<String> {
        let g = self.inner.read();
        g.get(plugin)
            .map(|m| {
                m.keys()
                    .filter(|k| k.starts_with(prefix))
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Per-call store data: the plugin name (so host imports route to
/// the right KV namespace) and a clone of the shared KV backend.
/// Carrying the Arc<KvBackend> by value here is cheap (one atomic
/// inc) and lets the import functions call `caller.data()` to
/// retrieve it.
pub struct StoreCtx {
    pub plugin_name: String,
    pub kv: KvBackend,
}

/// Register all host imports under the `env` module against the
/// supplied linker. Idempotent — calling twice replaces prior bindings.
pub fn register_kv_imports(linker: &mut Linker<StoreCtx>) -> Result<(), PluginError> {
    linker
        .func_wrap(
            "env",
            "kv_get",
            |mut caller: Caller<'_, StoreCtx>,
             key_ptr: i32,
             key_len: i32,
             val_out_ptr: i32,
             val_max_len: i32|
             -> i32 {
                let memory = match get_memory(&mut caller) {
                    Some(m) => m,
                    None => return -1,
                };
                let key = match read_bytes(&memory, &caller, key_ptr, key_len) {
                    Some(b) => b,
                    None => return -1,
                };
                let plugin_name = caller.data().plugin_name.clone();
                let kv = caller.data().kv.clone();
                let value = match kv.get(&plugin_name, &key) {
                    Some(v) => v,
                    None => return -1,
                };
                if (value.len() as i32) > val_max_len {
                    return -2;
                }
                if write_bytes(&memory, &mut caller, val_out_ptr, &value).is_err() {
                    return -1;
                }
                value.len() as i32
            },
        )
        .map_err(|e| PluginError::RuntimeError(format!("link kv_get: {}", e)))?;

    linker
        .func_wrap(
            "env",
            "kv_set",
            |mut caller: Caller<'_, StoreCtx>,
             key_ptr: i32,
             key_len: i32,
             val_ptr: i32,
             val_len: i32|
             -> i32 {
                let memory = match get_memory(&mut caller) {
                    Some(m) => m,
                    None => return -1,
                };
                let key = match read_bytes(&memory, &caller, key_ptr, key_len) {
                    Some(b) => b,
                    None => return -1,
                };
                let val = match read_bytes(&memory, &caller, val_ptr, val_len) {
                    Some(b) => b,
                    None => return -1,
                };
                let plugin_name = caller.data().plugin_name.clone();
                let kv = caller.data().kv.clone();
                // A cap breach (value too big / key-count exceeded)
                // returns false → surface as -1 ("internal error"),
                // which is already part of the documented contract.
                if kv.set(&plugin_name, key, val) {
                    0
                } else {
                    -1
                }
            },
        )
        .map_err(|e| PluginError::RuntimeError(format!("link kv_set: {}", e)))?;

    linker
        .func_wrap(
            "env",
            "kv_delete",
            |mut caller: Caller<'_, StoreCtx>, key_ptr: i32, key_len: i32| -> i32 {
                let memory = match get_memory(&mut caller) {
                    Some(m) => m,
                    None => return -1,
                };
                let key = match read_bytes(&memory, &caller, key_ptr, key_len) {
                    Some(b) => b,
                    None => return -1,
                };
                let plugin_name = caller.data().plugin_name.clone();
                let kv = caller.data().kv.clone();
                kv.delete(&plugin_name, &key);
                0
            },
        )
        .map_err(|e| PluginError::RuntimeError(format!("link kv_delete: {}", e)))?;

    Ok(())
}

/// Register the `env.sha256_hex` host import. Plugins call:
///
/// ```text
/// env.sha256_hex(in_ptr: i32, in_len: i32, out_ptr: i32) -> i32
/// ```
///
/// where `out_ptr` must point to at least 64 bytes inside plugin
/// memory (the lower-case hex SHA-256 digest is exactly 64 ASCII
/// chars). Returns 64 on success, -1 on memory error.
///
/// The host computes the digest over the plugin-supplied byte range
/// using the production `sha2` crate; plugins no longer need to
/// embed their own (placeholder) hash and stay small.
pub fn register_crypto_imports(linker: &mut Linker<StoreCtx>) -> Result<(), PluginError> {
    use sha2::{Digest, Sha256};

    linker
        .func_wrap(
            "env",
            "sha256_hex",
            |mut caller: Caller<'_, StoreCtx>, in_ptr: i32, in_len: i32, out_ptr: i32| -> i32 {
                let memory = match get_memory(&mut caller) {
                    Some(m) => m,
                    None => return -1,
                };
                let input = match read_bytes(&memory, &caller, in_ptr, in_len) {
                    Some(b) => b,
                    None => return -1,
                };
                let digest = Sha256::digest(&input);
                // Hex-encode into a fixed 64-byte stack buffer so we
                // don't allocate per call.
                let mut hex = [0u8; 64];
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for (i, b) in digest.iter().enumerate() {
                    hex[i * 2] = HEX[(b >> 4) as usize];
                    hex[i * 2 + 1] = HEX[(b & 0x0f) as usize];
                }
                if write_bytes(&memory, &mut caller, out_ptr, &hex).is_err() {
                    return -1;
                }
                64
            },
        )
        .map_err(|e| PluginError::RuntimeError(format!("link sha256_hex: {}", e)))?;
    Ok(())
}

fn get_memory(caller: &mut Caller<'_, StoreCtx>) -> Option<Memory> {
    caller.get_export("memory").and_then(|e| e.into_memory())
}

fn read_bytes(
    memory: &Memory,
    caller: &Caller<'_, StoreCtx>,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    if len < 0 {
        return None;
    }
    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;
    let data = memory.data(caller);
    data.get(start..end).map(|s| s.to_vec())
}

fn write_bytes(
    memory: &Memory,
    caller: &mut Caller<'_, StoreCtx>,
    ptr: i32,
    bytes: &[u8],
) -> Result<(), ()> {
    let start = ptr as usize;
    let end = start.checked_add(bytes.len()).ok_or(())?;
    let data = memory.data_mut(caller);
    let slot = data.get_mut(start..end).ok_or(())?;
    slot.copy_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_namespaced_per_plugin() {
        let kv = KvBackend::new();
        kv.set("plugin-a", b"k".to_vec(), b"v1".to_vec());
        kv.set("plugin-b", b"k".to_vec(), b"v2".to_vec());
        assert_eq!(kv.get("plugin-a", b"k"), Some(b"v1".to_vec()));
        assert_eq!(kv.get("plugin-b", b"k"), Some(b"v2".to_vec()));
        assert_eq!(kv.get("plugin-c", b"k"), None);
    }

    #[test]
    fn kv_overwrite_is_idempotent() {
        let kv = KvBackend::new();
        kv.set("p", b"k".to_vec(), b"v1".to_vec());
        kv.set("p", b"k".to_vec(), b"v2".to_vec());
        assert_eq!(kv.get("p", b"k"), Some(b"v2".to_vec()));
        assert_eq!(kv.len("p"), 1);
    }

    #[test]
    fn kv_delete_idempotent_on_missing() {
        let kv = KvBackend::new();
        kv.delete("p", b"never-set");
        kv.set("p", b"k".to_vec(), b"v".to_vec());
        kv.delete("p", b"k");
        assert_eq!(kv.get("p", b"k"), None);
    }

    #[test]
    fn kv_list_keys_empty_namespace_is_empty() {
        let kv = KvBackend::new();
        assert!(kv.list_keys("nobody", b"").is_empty());
    }

    #[test]
    fn kv_list_keys_filters_by_prefix() {
        let kv = KvBackend::new();
        kv.set("p", b"budget/a".to_vec(), b"1".to_vec());
        kv.set("p", b"budget/b".to_vec(), b"2".to_vec());
        kv.set("p", b"region_map".to_vec(), b"3".to_vec());

        // No prefix → every key (order-independent).
        let mut all = kv.list_keys("p", b"");
        all.sort();
        assert_eq!(all, vec!["budget/a", "budget/b", "region_map"]);

        // Prefix filter keeps only the matching keys.
        let mut budget = kv.list_keys("p", b"budget/");
        budget.sort();
        assert_eq!(budget, vec!["budget/a", "budget/b"]);
    }

    #[test]
    fn kv_value_cap_rejects_oversized_value() {
        let kv = KvBackend::with_limits(4, 0, 0);
        // 4 bytes is exactly the cap — allowed.
        assert!(kv.set("p", b"k".to_vec(), b"1234".to_vec()));
        // 5 bytes exceeds it — rejected, store unchanged.
        assert!(!kv.set("p", b"k".to_vec(), b"12345".to_vec()));
        assert_eq!(kv.get("p", b"k"), Some(b"1234".to_vec()));
    }

    #[test]
    fn kv_value_cap_also_bounds_key_length() {
        let kv = KvBackend::with_limits(4, 0, 0);
        // A 4-byte key is at the cap — allowed.
        assert!(kv.set("p", b"kkkk".to_vec(), b"v".to_vec()));
        // A 5-byte key exceeds the cap — rejected, store unchanged.
        assert!(!kv.set("p", b"kkkkk".to_vec(), b"v".to_vec()));
        assert_eq!(kv.get("p", b"kkkkk"), None);
        assert_eq!(kv.len("p"), 1);
    }

    #[test]
    fn kv_namespace_cap_blocks_new_plugins_but_allows_existing() {
        let kv = KvBackend::with_limits(0, 0, 2);
        // Two distinct namespaces fit under the cap of 2.
        assert!(kv.set("a", b"k".to_vec(), b"1".to_vec()));
        assert!(kv.set("b", b"k".to_vec(), b"2".to_vec()));
        // A third distinct namespace would exceed it — rejected, and no
        // empty namespace is left behind.
        assert!(!kv.set("c", b"k".to_vec(), b"3".to_vec()));
        assert_eq!(kv.get("c", b"k"), None);
        assert!(kv.list_keys("c", b"").is_empty());
        // Writing MORE keys into an already-present namespace is always
        // allowed — the namespace count stays constant.
        assert!(kv.set("a", b"k2".to_vec(), b"9".to_vec()));
        assert_eq!(kv.len("a"), 2);
    }

    #[test]
    fn kv_delete_reclaims_empty_namespace_slot() {
        // With a namespace cap of 1, draining the sole namespace must
        // free its slot so a different namespace can then be created.
        let kv = KvBackend::with_limits(0, 0, 1);
        assert!(kv.set("a", b"k".to_vec(), b"1".to_vec()));
        // Cap is full — a second namespace is refused.
        assert!(!kv.set("b", b"k".to_vec(), b"2".to_vec()));
        // Drain "a"; its now-empty namespace is dropped, freeing a slot.
        kv.delete("a", b"k");
        assert_eq!(kv.len("a"), 0);
        // The reclaimed slot lets a fresh namespace be created.
        assert!(kv.set("b", b"k".to_vec(), b"2".to_vec()));
        assert_eq!(kv.get("b", b"k"), Some(b"2".to_vec()));
    }

    #[test]
    fn kv_key_count_cap_blocks_new_keys_but_allows_overwrite() {
        let kv = KvBackend::with_limits(0, 2, 0);
        assert!(kv.set("p", b"a".to_vec(), b"1".to_vec()));
        assert!(kv.set("p", b"b".to_vec(), b"2".to_vec()));
        // Third distinct key would exceed the cap of 2 — rejected.
        assert!(!kv.set("p", b"c".to_vec(), b"3".to_vec()));
        assert_eq!(kv.len("p"), 2);
        // Overwriting an existing key under a full cap still succeeds.
        assert!(kv.set("p", b"a".to_vec(), b"updated".to_vec()));
        assert_eq!(kv.get("p", b"a"), Some(b"updated".to_vec()));
        assert_eq!(kv.len("p"), 2);
    }

    #[test]
    fn kv_zero_caps_mean_unlimited() {
        let kv = KvBackend::with_limits(0, 0, 0);
        // A large value and many keys both succeed under 0 = unlimited.
        assert!(kv.set("p", b"big".to_vec(), vec![0u8; 1_000_000]));
        for i in 0..1000u32 {
            assert!(kv.set("p", i.to_le_bytes().to_vec(), b"v".to_vec()));
        }
        assert_eq!(kv.len("p"), 1001);
    }
}

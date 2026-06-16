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
//! - `kv_set`: `0` on success, `-1` on internal error.
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
#[derive(Clone, Default)]
pub struct KvBackend {
    inner: Arc<RwLock<KvStore>>,
}

impl KvBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a value. None if missing.
    pub fn get(&self, plugin: &str, key: &[u8]) -> Option<Vec<u8>> {
        let g = self.inner.read();
        g.get(plugin).and_then(|m| m.get(key).cloned())
    }

    /// Insert / overwrite.
    pub fn set(&self, plugin: &str, key: Vec<u8>, value: Vec<u8>) {
        let mut g = self.inner.write();
        g.entry(plugin.to_string())
            .or_default()
            .insert(key, value);
    }

    /// Delete; idempotent.
    pub fn delete(&self, plugin: &str, key: &[u8]) {
        let mut g = self.inner.write();
        if let Some(m) = g.get_mut(plugin) {
            m.remove(key);
        }
    }

    /// Returns the number of keys in the plugin's namespace.
    /// Useful for tests and the future admin endpoint.
    pub fn len(&self, plugin: &str) -> usize {
        self.inner
            .read()
            .get(plugin)
            .map(|m| m.len())
            .unwrap_or(0)
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
            |mut caller: Caller<'_, StoreCtx>, key_ptr: i32, key_len: i32, val_out_ptr: i32, val_max_len: i32| -> i32 {
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
            |mut caller: Caller<'_, StoreCtx>, key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32| -> i32 {
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
                kv.set(&plugin_name, key, val);
                0
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
                    hex[i * 2]     = HEX[(b >> 4) as usize];
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

fn read_bytes(memory: &Memory, caller: &Caller<'_, StoreCtx>, ptr: i32, len: i32) -> Option<Vec<u8>> {
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
}

//! Plugin Loader
//!
//! Reads a plugin file — a bare `.wasm` module with optional sidecars, or a
//! packed `.tar.gz` artefact ([`crate::plugin_artefact`]) — checks its
//! signature against the trust root when one is configured, and parses and
//! validates its manifest.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::plugin_artefact::{self, ArtefactError, ArtefactKind, SidecarManifest, TrustRoot};

use super::sandbox::Permission;
use super::HookType;

/// Error types for plugin loading
#[derive(Debug, Clone)]
pub enum PluginLoadError {
    /// File not found
    FileNotFound(String),

    /// Invalid file format
    InvalidFormat(String),

    /// Manifest parsing error
    ManifestError(String),

    /// IO error
    IoError(String),

    /// Validation error
    ValidationError(String),

    /// Signature verification failed (Ed25519 over the .wasm bytes
    /// did not match any trusted public key, or the signature blob
    /// itself was malformed).
    SignatureInvalid(String),
}

impl std::fmt::Display for PluginLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginLoadError::FileNotFound(path) => write!(f, "File not found: {}", path),
            PluginLoadError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            PluginLoadError::ManifestError(msg) => write!(f, "Manifest error: {}", msg),
            PluginLoadError::IoError(msg) => write!(f, "IO error: {}", msg),
            PluginLoadError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            PluginLoadError::SignatureInvalid(msg) => {
                write!(f, "Signature verification failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for PluginLoadError {}

impl From<std::io::Error> for PluginLoadError {
    fn from(err: std::io::Error) -> Self {
        PluginLoadError::IoError(err.to_string())
    }
}

impl From<PluginLoadError> for super::runtime::PluginError {
    fn from(err: PluginLoadError) -> Self {
        super::runtime::PluginError::LoadError(err.to_string())
    }
}

impl From<ArtefactError> for PluginLoadError {
    fn from(err: ArtefactError) -> Self {
        match err {
            ArtefactError::Io(m) => PluginLoadError::IoError(m),
            ArtefactError::Format(m) => PluginLoadError::InvalidFormat(m),
            ArtefactError::Manifest(m) => PluginLoadError::ManifestError(m),
            ArtefactError::Key(m) | ArtefactError::Signature(m) => {
                PluginLoadError::SignatureInvalid(m)
            }
        }
    }
}

/// Plugin manifest: from an artefact's `manifest.json`, a bare module's
/// `.yaml` / `.json` sidecar, or the JSON in its `helios_manifest` custom
/// section.
#[derive(Debug, Clone)]
pub struct PluginManifest {
    /// Plugin name
    pub name: String,

    /// Version
    pub version: String,

    /// Description
    pub description: String,

    /// Author
    pub author: String,

    /// License
    pub license: String,

    /// Supported hooks
    pub hooks: Vec<HookType>,

    /// Required permissions
    pub permissions: Vec<Permission>,

    /// Minimum memory requirement
    pub min_memory: usize,

    /// Maximum memory requirement
    pub max_memory: usize,

    /// Configuration schema
    pub config_schema: HashMap<String, ConfigField>,

    /// Plugin file path
    pub path: PathBuf,
}

impl Default for PluginManifest {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            license: String::new(),
            hooks: Vec::new(),
            permissions: Vec::new(),
            min_memory: 1024 * 1024,      // 1MB
            max_memory: 64 * 1024 * 1024, // 64MB
            config_schema: HashMap::new(),
            path: PathBuf::new(),
        }
    }
}

/// Configuration field schema
#[derive(Debug, Clone)]
pub struct ConfigField {
    /// Field type
    pub field_type: ConfigFieldType,

    /// Whether field is required
    pub required: bool,

    /// Default value
    pub default: Option<String>,

    /// Description
    pub description: String,
}

/// Configuration field types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFieldType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
}

/// Name of the WebAssembly custom section a bare module may carry its
/// manifest in (JSON, same fields as a `.json` sidecar).
pub const EMBEDDED_MANIFEST_SECTION: &str = "helios_manifest";

/// Largest memory a manifest may ask for.
const MAX_PLUGIN_MEMORY: usize = 256 * 1024 * 1024;

/// What [`PluginLoader::load_verified`] read and checked.
#[derive(Debug, Clone)]
pub struct VerifiedPlugin {
    /// The validated manifest.
    pub manifest: PluginManifest,
    /// The module bytes.
    pub wasm: Vec<u8>,
    /// The trusted key that signed the module, when a trust root is set.
    pub signed_by: Option<String>,
    /// The trusted key that signed an artefact's `manifest.json`, when it
    /// carries a `manifest.sig`.
    pub manifest_signed_by: Option<String>,
    /// Manifest keys this format does not define (logged by the caller).
    pub unknown_manifest_keys: Vec<String>,
}

/// Plugin loader
pub struct PluginLoader {
    /// Search paths for plugins
    search_paths: Vec<PathBuf>,

    /// Optional Ed25519 trust root. When `Some`, every module must be
    /// signed by one of its keys: a bare `.wasm` through its `.sig`
    /// sidecar, an artefact through its `plugin.sig`. When `None`,
    /// signatures are not checked (the dev-loop ergonomic of dropping
    /// unsigned modules in the plugin dir).
    signature_verifier: Option<SignatureVerifier>,

    /// Upper bound on a plugin file and on an artefact's decompressed
    /// content.
    max_bytes: u64,

    /// With a trust root, refuse anything whose manifest is not signed:
    /// artefacts without a valid `manifest.sig` and every bare `.wasm`.
    require_signed_manifest: bool,
}

/// Ed25519 signature verifier for plugin modules: a
/// [`TrustRoot`](crate::plugin_artefact::TrustRoot) with this module's
/// error type.
///
/// Trust-root format: a directory of `*.pub` files, one key each, a single
/// key file, or 64 hex digits inline. A key is a SubjectPublicKeyInfo PEM
/// (`openssl pkey -pubout`), base64 of the 32 raw bytes, or 64 hex digits.
/// A signature is base64 of the raw 64-byte Ed25519 signature.
#[derive(Debug, Default)]
pub struct SignatureVerifier {
    trust: TrustRoot,
}

impl SignatureVerifier {
    /// Build a verifier from a trust root (see the type docs). A malformed
    /// key, or a trust root without any key, is an error.
    pub fn from_trust_root(dir: &Path) -> Result<Self, PluginLoadError> {
        Ok(Self {
            trust: TrustRoot::load(&dir.to_string_lossy())?,
        })
    }

    /// Verify a signature blob (base64-encoded Ed25519 signature)
    /// against the module bytes. Returns the matching key's label.
    pub fn verify(&self, wasm: &[u8], sig_b64: &str) -> Result<&str, PluginLoadError> {
        Ok(self.trust.verify(wasm, sig_b64)?)
    }

    /// Number of trusted keys.
    pub fn key_count(&self) -> usize {
        self.trust.key_count()
    }

    /// The underlying trust root.
    pub fn trust_root(&self) -> &TrustRoot {
        &self.trust
    }
}

impl PluginLoader {
    /// Create a new plugin loader. Accepts both bare `.wasm` modules (the
    /// dev-loop format) and packed `.tar.gz` artefacts (the distribution
    /// format `helios-plugin pack` produces).
    pub fn new() -> Self {
        Self {
            search_paths: Vec::new(),
            signature_verifier: None,
            max_bytes: plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES,
            require_signed_manifest: false,
        }
    }

    /// Attach a trust-root verifier. Once set, every load requires a
    /// signature made by one of its keys.
    pub fn with_signature_verifier(mut self, verifier: SignatureVerifier) -> Self {
        self.signature_verifier = Some(verifier);
        self
    }

    /// Bound a plugin file's size, and an artefact's decompressed content.
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// With a trust root, also require a signed manifest: only artefacts
    /// with a valid `manifest.sig` load.
    pub fn require_signed_manifest(mut self, required: bool) -> Self {
        self.require_signed_manifest = required;
        self
    }

    /// Add a search path
    pub fn add_search_path(&mut self, path: PathBuf) {
        self.search_paths.push(path);
    }

    /// Load a plugin from a file path. Two accepted shapes:
    ///
    ///   1. Bare `.wasm` — its manifest comes from a `.yaml` or `.json`
    ///      sidecar, else from a `helios_manifest` custom section; with a
    ///      trust root, a `.sig` sidecar must sign the module.
    ///   2. Packed `.tar.gz` artefact — manifest and signatures are inside
    ///      ([`crate::plugin_artefact`]).
    pub fn load(&self, path: &Path) -> Result<(PluginManifest, Vec<u8>), PluginLoadError> {
        self.load_verified(path).map(|p| (p.manifest, p.wasm))
    }

    /// [`load`](Self::load), also reporting who signed what and which
    /// manifest keys were not understood.
    pub fn load_verified(&self, path: &Path) -> Result<VerifiedPlugin, PluginLoadError> {
        if !path.exists() {
            return Err(PluginLoadError::FileNotFound(path.display().to_string()));
        }
        match ArtefactKind::of(path) {
            Some(ArtefactKind::TarGz) => self.load_tar_gz(path),
            Some(ArtefactKind::Wasm) => self.load_wasm(path),
            None => Err(PluginLoadError::InvalidFormat(format!(
                "{} is not a plugin file (expected a name ending .wasm or .tar.gz)",
                path.display()
            ))),
        }
    }

    fn load_wasm(&self, path: &Path) -> Result<VerifiedPlugin, PluginLoadError> {
        let wasm_path = path;
        if self.require_signed_manifest && self.signature_verifier.is_some() {
            return Err(PluginLoadError::SignatureInvalid(format!(
                "{} is a bare module, whose manifest cannot be signed; this proxy \
                 requires signed manifests (pack it with `helios-plugin pack --sign-with`)",
                path.display()
            )));
        }
        let wasm = plugin_artefact::read_bounded(path, self.max_bytes)?;
        if !plugin_artefact::is_wasm(&wasm) {
            return Err(PluginLoadError::InvalidFormat(
                "Invalid WASM file (bad magic number)".to_string(),
            ));
        }
        let signed_by = match &self.signature_verifier {
            Some(verifier) => {
                let sig_path = path.with_extension("sig");
                if !sig_path.exists() {
                    return Err(PluginLoadError::SignatureInvalid(format!(
                        "{} requires a sidecar .sig file (trust root active)",
                        path.display()
                    )));
                }
                let sig_b64 = fs::read_to_string(&sig_path).map_err(|e| {
                    PluginLoadError::IoError(format!("read {}: {}", sig_path.display(), e))
                })?;
                Some(verifier.verify(&wasm, &sig_b64)?.to_string())
            }
            None => None,
        };
        let sidecar = match self.sidecar_manifest(wasm_path, &wasm)? {
            Some(sidecar) => sidecar,
            None => derived_manifest(wasm_path, &wasm)?,
        };
        let unknown = sidecar.unknown_keys.clone();
        let manifest = manifest_from_sidecar(sidecar, path)?;
        Ok(VerifiedPlugin {
            manifest,
            wasm,
            signed_by,
            manifest_signed_by: None,
            unknown_manifest_keys: unknown,
        })
    }

    /// A bare module's manifest: the `.yaml` sidecar, else the `.json`
    /// sidecar, else the module's `helios_manifest` custom section; `None`
    /// when it has none of them.
    fn sidecar_manifest(
        &self,
        wasm_path: &Path,
        wasm: &[u8],
    ) -> Result<Option<SidecarManifest>, PluginLoadError> {
        let yaml_path = wasm_path.with_extension("yaml");
        if yaml_path.exists() {
            let text = fs::read_to_string(&yaml_path)?;
            return plugin_artefact::parse_yaml_manifest(&text)
                .map(Some)
                .map_err(|e| {
                    PluginLoadError::ManifestError(format!("{}: {e}", yaml_path.display()))
                });
        }
        let json_path = wasm_path.with_extension("json");
        if json_path.exists() {
            let text = fs::read_to_string(&json_path)?;
            return plugin_artefact::parse_json_manifest(&text)
                .map(Some)
                .map_err(|e| {
                    PluginLoadError::ManifestError(format!("{}: {e}", json_path.display()))
                });
        }
        if let Some(section) = custom_section(wasm, EMBEDDED_MANIFEST_SECTION)? {
            let text = std::str::from_utf8(section).map_err(|_| {
                PluginLoadError::ManifestError(format!(
                    "the {EMBEDDED_MANIFEST_SECTION} section is not UTF-8"
                ))
            })?;
            return plugin_artefact::parse_json_manifest(text)
                .map(Some)
                .map_err(|e| {
                    PluginLoadError::ManifestError(format!(
                        "{EMBEDDED_MANIFEST_SECTION} section: {e}"
                    ))
                });
        }
        Ok(None)
    }

    /// Load a packed `.tar.gz` artefact: unpack it strictly, check the
    /// module's SHA-256 against the manifest and, with a trust root, its
    /// signatures.
    fn load_tar_gz(&self, path: &Path) -> Result<VerifiedPlugin, PluginLoadError> {
        let artefact = plugin_artefact::read(path, self.max_bytes)?;
        let (signed_by, manifest_signed_by) = match &self.signature_verifier {
            Some(verifier) => {
                let trust = plugin_artefact::verify_artefact(
                    &artefact,
                    verifier.trust_root(),
                    self.require_signed_manifest,
                )?;
                (Some(trust.wasm_signed_by), trust.manifest_signed_by)
            }
            None => (None, None),
        };
        let hooks = parse_hooks(&artefact.manifest.hooks)?;
        let manifest = PluginManifest {
            name: artefact.manifest.name,
            version: artefact.manifest.version,
            description: artefact.manifest.description,
            author: String::new(),
            license: artefact.manifest.license,
            hooks,
            permissions: Vec::new(),
            config_schema: HashMap::new(),
            path: path.to_path_buf(),
            ..PluginManifest::default()
        };
        validate(&manifest)?;
        Ok(VerifiedPlugin {
            manifest,
            wasm: artefact.wasm,
            signed_by,
            manifest_signed_by,
            unknown_manifest_keys: Vec::new(),
        })
    }

    /// Validate manifest
    pub fn validate_manifest(&self, manifest: &PluginManifest) -> Result<(), PluginLoadError> {
        validate(manifest)
    }

    /// The plugin files in the search paths (`.wasm` and `.tar.gz`; hidden
    /// files and symlinks skipped), sorted.
    pub fn discover(&self) -> Result<Vec<PathBuf>, PluginLoadError> {
        let mut plugins = Vec::new();
        for search_path in &self.search_paths {
            if !search_path.is_dir() {
                continue;
            }
            for entry in fs::read_dir(search_path)? {
                let entry = entry?;
                // Symlinks are not followed: a plugin is a real file here.
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let path = entry.path();
                if ArtefactKind::of(&path).is_some() {
                    plugins.push(path);
                }
            }
        }
        plugins.sort();
        Ok(plugins)
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Map manifest hook names to hook types; an unknown name is an error (a
/// plugin whose hook is silently dropped would never run).
fn parse_hooks(names: &[String]) -> Result<Vec<HookType>, PluginLoadError> {
    let mut hooks = Vec::with_capacity(names.len());
    for name in names {
        let hook = HookType::from_str(name.trim())
            .ok_or_else(|| PluginLoadError::ManifestError(format!("unknown hook {name:?}")))?;
        if hooks.contains(&hook) {
            return Err(PluginLoadError::ManifestError(format!(
                "hook {name:?} listed twice"
            )));
        }
        hooks.push(hook);
    }
    Ok(hooks)
}

/// The manifest of a bare module that has none (a development
/// convenience): its file name as the name, version `0.0.0-dev`, and as
/// hooks the hook functions it exports (`pre_query`, `route`, `post_query`,
/// `authenticate`; their signatures are checked when it is instantiated).
fn derived_manifest(wasm_path: &Path, wasm: &[u8]) -> Result<SidecarManifest, PluginLoadError> {
    let name = wasm_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();
    let exports = exported_functions(wasm)?;
    let hooks: Vec<String> = [
        HookType::PreQuery,
        HookType::Route,
        HookType::PostQuery,
        HookType::Authenticate,
    ]
    .iter()
    .map(|h| h.export_name())
    .filter(|name| exports.iter().any(|e| e == name))
    .map(str::to_string)
    .collect();
    tracing::warn!(
        path = %wasm_path.display(),
        plugin = %name,
        hooks = ?hooks,
        "plugin module has no manifest; using its file name and the hooks it exports \
         (development use: pack it with `helios-plugin pack` for production)"
    );
    Ok(SidecarManifest {
        name,
        version: "0.0.0-dev".to_string(),
        description: "manifest derived from the module's exports".to_string(),
        hooks,
        ..SidecarManifest::default()
    })
}

/// The names of the functions a WebAssembly module exports.
pub fn exported_functions(wasm: &[u8]) -> Result<Vec<String>, PluginLoadError> {
    Ok(plugin_artefact::wasm_exported_functions(wasm)?)
}

fn manifest_from_sidecar(
    sidecar: SidecarManifest,
    path: &Path,
) -> Result<PluginManifest, PluginLoadError> {
    let defaults = PluginManifest::default();
    let memory = |value: Option<String>, default: usize| -> Result<usize, PluginLoadError> {
        match value {
            Some(v) => Ok(plugin_artefact::parse_memory_size(&v)?),
            None => Ok(default),
        }
    };
    let manifest = PluginManifest {
        name: sidecar.name,
        version: if sidecar.version.is_empty() {
            defaults.version.clone()
        } else {
            sidecar.version
        },
        description: sidecar.description,
        author: sidecar.author,
        license: sidecar.license,
        hooks: parse_hooks(&sidecar.hooks)?,
        permissions: sidecar
            .permissions
            .iter()
            .filter_map(|p| Permission::from_str(p))
            .collect(),
        min_memory: memory(sidecar.min_memory, defaults.min_memory)?,
        max_memory: memory(sidecar.max_memory, defaults.max_memory)?,
        config_schema: HashMap::new(),
        path: path.to_path_buf(),
    };
    validate(&manifest)?;
    Ok(manifest)
}

fn validate(manifest: &PluginManifest) -> Result<(), PluginLoadError> {
    let invalid = |e: ArtefactError| PluginLoadError::ValidationError(e.to_string());
    if manifest.name.is_empty() {
        return Err(PluginLoadError::ValidationError(
            "Plugin name is required".to_string(),
        ));
    }
    plugin_artefact::validate_name(&manifest.name).map_err(invalid)?;
    plugin_artefact::validate_version(&manifest.version).map_err(invalid)?;
    if manifest.min_memory > manifest.max_memory {
        return Err(PluginLoadError::ValidationError(
            "min_memory cannot exceed max_memory".to_string(),
        ));
    }
    if manifest.max_memory > MAX_PLUGIN_MEMORY {
        return Err(PluginLoadError::ValidationError(
            "max_memory cannot exceed 256MB".to_string(),
        ));
    }
    Ok(())
}

/// The payload of the custom section named `name` in a WebAssembly module,
/// if it has one.
pub fn custom_section<'a>(wasm: &'a [u8], name: &str) -> Result<Option<&'a [u8]>, PluginLoadError> {
    Ok(plugin_artefact::wasm_custom_section(wasm, name)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_load_error_display() {
        let err = PluginLoadError::FileNotFound("/test.wasm".to_string());
        assert!(err.to_string().contains("File not found"));

        let err = PluginLoadError::ManifestError("invalid".to_string());
        assert!(err.to_string().contains("Manifest error"));
    }

    #[test]
    fn test_plugin_manifest_default() {
        let manifest = PluginManifest::default();
        assert!(manifest.name.is_empty());
        assert_eq!(manifest.version, "0.0.0");
        assert!(manifest.hooks.is_empty());
    }

    #[test]
    fn discover_lists_plugin_files_sorted() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "b.wasm",
            "a.tar.gz",
            "a.sig",
            "a.json",
            ".hidden.wasm",
            "c.gz",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        let mut loader = PluginLoader::new();
        assert!(loader.discover().unwrap().is_empty());
        loader.add_search_path(dir.path().to_path_buf());
        let found: Vec<_> = loader
            .discover()
            .unwrap()
            .into_iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(found, ["a.tar.gz", "b.wasm"]);
    }

    #[test]
    fn test_parse_memory_size() {
        let size = crate::plugin_artefact::parse_memory_size;
        assert_eq!(size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(size("1024KB").unwrap(), 1024 * 1024);
        assert_eq!(size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(size("1048576").unwrap(), 1048576);
    }

    #[test]
    fn test_manifest_validation_empty_name() {
        let loader = PluginLoader::new();
        let manifest = PluginManifest::default();

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name is required"));
    }

    #[test]
    fn test_manifest_validation_invalid_memory() {
        let loader = PluginLoader::new();
        let manifest = PluginManifest {
            name: "test-plugin".to_string(),
            min_memory: 100 * 1024 * 1024,
            max_memory: 50 * 1024 * 1024,
            ..Default::default()
        };

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_memory"));
    }

    #[test]
    fn test_manifest_validation_success() {
        let loader = PluginLoader::new();
        let manifest = PluginManifest {
            name: "test-plugin".to_string(),
            ..Default::default()
        };

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_ok());
    }

    /// A bare module with no manifest anywhere gets one derived from it:
    /// its file name, version `0.0.0-dev`, and the hook functions it
    /// exports.
    #[test]
    fn a_bare_module_without_a_manifest_takes_its_hooks_from_its_exports() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("my-plugin.wasm");
        let wasm = wat::parse_str(
            r#"(module (memory (export "memory") 1)
                 (func (export "alloc") (param i32) (result i32) (i32.const 0))
                 (func (export "dealloc") (param i32 i32))
                 (func (export "route") (param i32 i32) (result i64) (i64.const 0))
                 (func (export "post_query") (param i32 i32)))"#,
        )
        .unwrap();
        std::fs::write(&path, &wasm).unwrap();
        let (manifest, _) = PluginLoader::new().load(&path).unwrap();
        assert_eq!(manifest.name, "my-plugin");
        assert_eq!(manifest.version, "0.0.0-dev");
        assert_eq!(
            manifest.hooks,
            [
                crate::plugins::HookType::Route,
                crate::plugins::HookType::PostQuery
            ]
        );
        assert_eq!(
            exported_functions(&wasm).unwrap(),
            ["alloc", "dealloc", "route", "post_query"]
        );
        // A file name that is not a valid plugin name is refused.
        let odd = dir.path().join("my plugin.wasm");
        std::fs::write(&odd, &wasm).unwrap();
        assert!(PluginLoader::new().load(&odd).is_err());
    }

    /// The manifest may live in the module's `helios_manifest` custom
    /// section; a sidecar takes precedence over it.
    #[test]
    fn an_embedded_manifest_section_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let json = br#"{"name": "embedded", "version": "2.1.0", "hooks": ["route"]}"#;
        let name = EMBEDDED_MANIFEST_SECTION.as_bytes();
        let mut body = vec![name.len() as u8];
        body.extend_from_slice(name);
        body.extend_from_slice(json);
        let mut wasm = fake_wasm(b"");
        wasm.push(0); // custom section id
        wasm.push(body.len() as u8);
        wasm.extend_from_slice(&body);
        let path = dir.path().join("whatever.wasm");
        std::fs::write(&path, &wasm).unwrap();
        let (manifest, bytes) = PluginLoader::new().load(&path).unwrap();
        assert_eq!(manifest.name, "embedded");
        assert_eq!(manifest.version, "2.1.0");
        assert_eq!(manifest.hooks, [crate::plugins::HookType::Route]);
        assert_eq!(bytes, wasm);

        std::fs::write(
            dir.path().join("whatever.json"),
            r#"{"name": "sidecar", "hooks": ["pre_query"]}"#,
        )
        .unwrap();
        assert_eq!(PluginLoader::new().load(&path).unwrap().0.name, "sidecar");

        // A section that runs past the module is malformed.
        let mut truncated = fake_wasm(b"");
        truncated.extend_from_slice(&[0, 0x40, 3]);
        assert!(custom_section(&truncated, EMBEDDED_MANIFEST_SECTION).is_err());
    }

    /// Hook names are checked: an unknown one refuses the plugin rather
    /// than being dropped.
    #[test]
    fn an_unknown_hook_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.wasm");
        std::fs::write(&path, fake_wasm(b"")).unwrap();
        std::fs::write(
            dir.path().join("p.yaml"),
            "name: p\nhooks:\n  - pre_query\n  - pre_qeury\n",
        )
        .unwrap();
        let err = PluginLoader::new().load(&path).unwrap_err();
        assert!(err.to_string().contains("unknown hook"), "{err}");
        let tar = pack_tarball_hooks(dir.path(), "t", &fake_wasm(b"t"), None, &["route", "nope"]);
        let err = PluginLoader::new().load(&tar).unwrap_err();
        assert!(err.to_string().contains("unknown hook"), "{err}");
    }

    /// Only `.wasm` and `.tar.gz` names are plugin files.
    #[test]
    fn other_file_names_are_not_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.gz");
        std::fs::write(&path, b"x").unwrap();
        let err = PluginLoader::new().load(&path).unwrap_err();
        assert!(matches!(err, PluginLoadError::InvalidFormat(_)), "{err:?}");
    }

    /// `require_signed_manifest` admits only artefacts whose manifest.sig
    /// verifies: bare modules and schema-1.0 artefacts are refused.
    #[test]
    fn signed_manifests_can_be_required() {
        let dir = tempfile::tempdir().unwrap();
        let trust_dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(trust_dir.path(), "official", &key);
        let loader = || {
            PluginLoader::new()
                .with_signature_verifier(
                    SignatureVerifier::from_trust_root(trust_dir.path()).unwrap(),
                )
                .require_signed_manifest(true)
        };

        let wasm = fake_wasm(b"signed");
        let sig = base64::engine::general_purpose::STANDARD.encode(key.sign(&wasm).to_bytes());
        let legacy = pack_tarball(dir.path(), "legacy", &wasm, Some(sig.as_bytes()));
        let err = loader().load(&legacy).unwrap_err();
        assert!(err.to_string().contains("manifest.sig"), "{err}");

        let bare = dir.path().join("bare.wasm");
        std::fs::write(&bare, &wasm).unwrap();
        std::fs::write(dir.path().join("bare.sig"), &sig).unwrap();
        std::fs::write(
            dir.path().join("bare.json"),
            r#"{"name": "bare", "hooks": ["route"]}"#,
        )
        .unwrap();
        let err = loader().load(&bare).unwrap_err();
        assert!(err.to_string().contains("bare module"), "{err}");

        let input = crate::plugin_artefact::PackInput {
            name: "signed".into(),
            version: "1.0.0".into(),
            description: String::new(),
            license: String::new(),
            hooks: vec!["route".into()],
            wasm,
            wasm_signature: None,
        };
        let (_, bytes) = crate::plugin_artefact::pack(&input, Some(&key)).unwrap();
        let packed = dir.path().join("signed.tar.gz");
        std::fs::write(&packed, bytes).unwrap();
        let verified = loader().load_verified(&packed).unwrap();
        assert_eq!(verified.signed_by.as_deref(), Some("official"));
        assert_eq!(verified.manifest_signed_by.as_deref(), Some("official"));
    }

    #[test]
    fn test_config_field_type() {
        assert_eq!(ConfigFieldType::String, ConfigFieldType::String);
        assert_ne!(ConfigFieldType::String, ConfigFieldType::Integer);
    }

    // -----------------------------------------------------------------
    // SignatureVerifier tests
    //
    // We generate an Ed25519 keypair at runtime, write the public key
    // into a temp trust-root dir, sign a fake .wasm, and check that
    // the loader accepts the signed bytes and rejects tampered ones.
    // -----------------------------------------------------------------

    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};

    /// Helper: write a single .pub file with `key`'s public component
    /// into `dir/<label>.pub`. Returns `dir`.
    fn write_pub_key(dir: &Path, label: &str, key: &SigningKey) {
        let pub_bytes = key.verifying_key().to_bytes();
        let b64 = base64::engine::general_purpose::STANDARD.encode(pub_bytes);
        std::fs::write(dir.join(format!("{label}.pub")), b64).unwrap();
    }

    fn make_signing_key() -> SigningKey {
        // Deterministic seed → reproducible tests.
        let seed = [7u8; 32];
        SigningKey::from_bytes(&seed)
    }

    #[test]
    fn test_signature_verifier_accepts_matching_signature() {
        let dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(dir.path(), "official", &key);

        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();
        assert_eq!(verifier.key_count(), 1);

        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = key.sign(wasm);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let label = verifier.verify(wasm, &sig_b64).unwrap();
        assert_eq!(label, "official");
    }

    #[test]
    fn test_signature_verifier_rejects_tampered_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(dir.path(), "official", &key);
        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();

        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = key.sign(wasm);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let tampered = b"\x00asm\x01\x00\x00\x00pretend-real-wasn"; // 'm' → 'n'
        let err = verifier.verify(tampered, &sig_b64).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_rejects_unknown_signer() {
        let dir = tempfile::tempdir().unwrap();
        let trusted = make_signing_key();
        write_pub_key(dir.path(), "official", &trusted);
        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();

        // Sign with a completely different key.
        let attacker = SigningKey::from_bytes(&[0xAB; 32]);
        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = attacker.sign(wasm);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let err = verifier.verify(wasm, &sig_b64).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_rejects_wrong_length_pubkey() {
        let dir = tempfile::tempdir().unwrap();
        // 31 bytes — invalid Ed25519 length.
        std::fs::write(
            dir.path().join("bad.pub"),
            base64::engine::general_purpose::STANDARD.encode([0u8; 31]),
        )
        .unwrap();
        let err = SignatureVerifier::from_trust_root(dir.path()).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_supports_multiple_keys() {
        let dir = tempfile::tempdir().unwrap();
        let k1 = SigningKey::from_bytes(&[1u8; 32]);
        let k2 = SigningKey::from_bytes(&[2u8; 32]);
        write_pub_key(dir.path(), "publisher-a", &k1);
        write_pub_key(dir.path(), "publisher-b", &k2);

        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();
        assert_eq!(verifier.key_count(), 2);

        let wasm = b"\x00asm\x01\x00\x00\x00abc";
        let sig = k2.sign(wasm); // signed by the SECOND publisher
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let label = verifier.verify(wasm, &sig_b64).unwrap();
        assert_eq!(label, "publisher-b");
    }

    #[test]
    fn test_loader_with_verifier_rejects_unsigned_plugin() {
        let dir = tempfile::tempdir().unwrap();
        let wasm_path = dir.path().join("plugin.wasm");
        std::fs::write(&wasm_path, b"\x00asm\x01\x00\x00\x00body").unwrap();

        let trust_dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(trust_dir.path(), "official", &key);

        let loader = PluginLoader::new()
            .with_signature_verifier(SignatureVerifier::from_trust_root(trust_dir.path()).unwrap());
        let err = loader.load(&wasm_path).unwrap_err();
        assert!(
            matches!(err, PluginLoadError::SignatureInvalid(_)),
            "expected SignatureInvalid for missing .sig, got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------
    // .tar.gz artefact loader tests (FU-28). Manually build a tarball
    // shaped like helios-plugin's output and feed it through load().
    // Avoids a workspace dep on the CLI crate.
    // -----------------------------------------------------------------

    use flate2::write::GzEncoder;
    use flate2::Compression;
    use sha2::{Digest, Sha256};

    fn fake_wasm(extra: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        v.extend_from_slice(extra);
        v
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let d = Sha256::digest(bytes);
        let mut s = String::new();
        for b in d.iter() {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    fn pack_tarball(dir: &Path, name: &str, wasm: &[u8], sig: Option<&[u8]>) -> std::path::PathBuf {
        pack_tarball_hooks(dir, name, wasm, sig, &["pre_query", "post_query"])
    }

    fn pack_tarball_hooks(
        dir: &Path,
        name: &str,
        wasm: &[u8],
        sig: Option<&[u8]>,
        hooks: &[&str],
    ) -> std::path::PathBuf {
        let manifest = serde_json::json!({
            "schema_version": "1.0",
            "name": name,
            "version": "0.1.0",
            "description": "test",
            "license": "Apache-2.0",
            "hooks": hooks,
            "wasm_sha256": sha256_hex(wasm),
            "signature_sha256": sig.map(sha256_hex),
            "signature_algorithm": sig.map(|_| "ed25519"),
            "packed_at": "2026-04-25T13:00:00Z",
        });
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();

        let out_path = dir.join(format!("{}.tar.gz", name));
        let f = std::fs::File::create(&out_path).unwrap();
        let gz = GzEncoder::new(f, Compression::default());
        let mut tar = tar::Builder::new(gz);

        let mut put = |path: &str, body: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_path(path).unwrap();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, body).unwrap();
        };
        put("manifest.json", &manifest_bytes);
        put("plugin.wasm", wasm);
        if let Some(s) = sig {
            put("plugin.sig", s);
        }
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();
        out_path
    }

    #[test]
    fn test_loader_accepts_tar_gz_artefact_without_signature() {
        let dir = tempfile::tempdir().unwrap();
        let wasm = fake_wasm(b"unsigned");
        let path = pack_tarball(dir.path(), "test-plugin", &wasm, None);

        let loader = PluginLoader::new();
        let (manifest, bytes) = loader.load(&path).unwrap();
        assert_eq!(manifest.name, "test-plugin");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(bytes, wasm);
        // Hooks parsed from string array.
        assert!(manifest.hooks.contains(&super::super::HookType::PreQuery));
        assert!(manifest.hooks.contains(&super::super::HookType::PostQuery));
    }

    #[test]
    fn test_loader_rejects_tar_gz_with_wrong_wasm_hash() {
        let dir = tempfile::tempdir().unwrap();
        // Build a tarball where manifest.wasm_sha256 doesn't match
        // the actual wasm bytes.
        let real_wasm = fake_wasm(b"real");
        let manifest = serde_json::json!({
            "schema_version": "1.0",
            "name": "x",
            "version": "0.1.0",
            "description": "",
            "license": "Apache-2.0",
            "hooks": [],
            "wasm_sha256": "deadbeef".repeat(8),  // wrong hash
            "packed_at": "2026-04-25T13:00:00Z",
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let out_path = dir.path().join("bad.tar.gz");
        let f = std::fs::File::create(&out_path).unwrap();
        let gz = GzEncoder::new(f, Compression::default());
        let mut tar = tar::Builder::new(gz);
        let mut put = |path: &str, body: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_path(path).unwrap();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, body).unwrap();
        };
        put("manifest.json", &manifest_bytes);
        put("plugin.wasm", &real_wasm);
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();

        let loader = PluginLoader::new();
        let err = loader.load(&out_path).unwrap_err();
        match err {
            PluginLoadError::InvalidFormat(msg) => {
                assert!(msg.contains("SHA-256"), "got {}", msg)
            }
            other => panic!("expected InvalidFormat, got {:?}", other),
        }
    }

    #[test]
    fn test_loader_rejects_tar_gz_unknown_schema_major() {
        let dir = tempfile::tempdir().unwrap();
        let wasm = fake_wasm(b"x");
        let manifest = serde_json::json!({
            "schema_version": "9.0",
            "name": "x",
            "version": "0.1.0",
            "description": "",
            "license": "Apache-2.0",
            "hooks": [],
            "wasm_sha256": sha256_hex(&wasm),
            "packed_at": "2026-04-25T13:00:00Z",
        });
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let out_path = dir.path().join("future.tar.gz");
        let f = std::fs::File::create(&out_path).unwrap();
        let gz = GzEncoder::new(f, Compression::default());
        let mut tar = tar::Builder::new(gz);
        let mut put = |path: &str, body: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_path(path).unwrap();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, body).unwrap();
        };
        put("manifest.json", &manifest_bytes);
        put("plugin.wasm", &wasm);
        let gz = tar.into_inner().unwrap();
        gz.finish().unwrap();

        let loader = PluginLoader::new();
        let err = loader.load(&out_path).unwrap_err();
        match err {
            PluginLoadError::ManifestError(msg) => {
                assert!(msg.contains("schema version"), "got {}", msg)
            }
            other => panic!("expected ManifestError, got {:?}", other),
        }
    }

    #[test]
    fn test_loader_tar_gz_signature_verifies_against_trust_root() {
        let dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        let wasm = fake_wasm(b"signed-body");

        // Sign the wasm bytes with our test key.
        use ed25519_dalek::Signer;
        let sig = key.sign(&wasm);
        let sig_b64 = base64::engine::general_purpose::STANDARD
            .encode(sig.to_bytes())
            .into_bytes();

        let path = pack_tarball(dir.path(), "signed-plugin", &wasm, Some(&sig_b64));

        let trust_dir = tempfile::tempdir().unwrap();
        write_pub_key(trust_dir.path(), "official", &key);

        let loader = PluginLoader::new()
            .with_signature_verifier(SignatureVerifier::from_trust_root(trust_dir.path()).unwrap());
        let (manifest, bytes) = loader.load(&path).unwrap();
        assert_eq!(manifest.name, "signed-plugin");
        assert_eq!(bytes, wasm);
    }

    #[test]
    fn test_loader_tar_gz_rejects_missing_signature_when_trust_root_active() {
        let dir = tempfile::tempdir().unwrap();
        let wasm = fake_wasm(b"unsigned");
        let path = pack_tarball(dir.path(), "p", &wasm, None);

        let trust_dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(trust_dir.path(), "official", &key);

        let loader = PluginLoader::new()
            .with_signature_verifier(SignatureVerifier::from_trust_root(trust_dir.path()).unwrap());
        let err = loader.load(&path).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }
}

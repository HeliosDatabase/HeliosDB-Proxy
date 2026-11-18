//! `helios-plugin` — build, sign, check and distribute HeliosProxy plugins.
//!
//!   helios-plugin keygen  --out-dir <dir> [--name <key-name>]
//!   helios-plugin pack    --wasm <module> (--manifest <plugin.yaml> | --name <n> --version <v> --hooks <a,b>)
//!                         [--description <d>] [--license <l>] [--sign-with <key> | --sig <file>]
//!                         --output <artefact.tar.gz>
//!   helios-plugin sign    <module.wasm | artefact.tar.gz> --key <key> [--output <file>]
//!   helios-plugin inspect <module.wasm | artefact.tar.gz> [--json]
//!   helios-plugin verify  <module.wasm | artefact.tar.gz> [--trust-root <dir|file|hex> | --pubkey <file>]
//!                         [--sig <file>] [--require-signed-manifest]
//!   helios-plugin install <name> --registry <index.json> --dest <plugins-dir>
//!                         [--trust-root <dir|file|hex>] [--version <v>]
//!   helios-plugin list    --registry <index.json>
//!   helios-plugin new     <name> [--dir <path>]
//!
//! Keys are Ed25519: a signing key is a PKCS#8 PEM file (`keygen`, or
//! `openssl genpkey -algorithm Ed25519`), base64 or hex of the 32-byte seed;
//! a public key is a SubjectPublicKeyInfo PEM file (`openssl pkey -pubout`),
//! base64 or hex of the 32 raw bytes. A trust root is a directory of `*.pub`
//! files, one key file, or one hex key — what `[plugins] trust_root` takes.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use heliosdb_proxy::plugin_artefact::{self, ArtefactKind, PackInput};
use heliosdb_proxy::plugin_registry;

#[derive(Parser)]
#[command(
    name = "helios-plugin",
    version,
    about = "Build, sign, check and distribute HeliosProxy WASM plugins"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate an Ed25519 signing key (`<name>.key`, PKCS#8 PEM, mode 0600)
    /// and its public key (`<name>.pub`, the trust-root format).
    Keygen {
        /// Directory to write the two files into (created if missing).
        #[arg(long)]
        out_dir: PathBuf,
        /// Base name of the files.
        #[arg(long, default_value = "publisher")]
        name: String,
    },
    /// Pack a module into a `.tar.gz` artefact (manifest.json, plugin.wasm,
    /// and with a key plugin.sig and manifest.sig).
    Pack {
        /// The WebAssembly module.
        #[arg(long)]
        wasm: PathBuf,
        /// A `plugin.yaml` / `plugin.json` manifest supplying name, version,
        /// description, license and hooks (flags override it).
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Plugin name.
        #[arg(long)]
        name: Option<String>,
        /// Plugin version.
        #[arg(long)]
        version: Option<String>,
        /// One-line description.
        #[arg(long)]
        description: Option<String>,
        /// SPDX license identifier.
        #[arg(long)]
        license: Option<String>,
        /// Hooks the module implements, comma-separated (pre_query, route,
        /// post_query, authenticate).
        #[arg(long, value_delimiter = ',')]
        hooks: Vec<String>,
        /// Sign the module and the manifest with this key.
        #[arg(long, conflicts_with = "sig")]
        sign_with: Option<PathBuf>,
        /// Embed a ready-made signature over the module (base64), made
        /// elsewhere.
        #[arg(long)]
        sig: Option<PathBuf>,
        /// The artefact to write.
        #[arg(long, alias = "out")]
        output: PathBuf,
    },
    /// Sign a module (writes the `.sig` sidecar) or re-sign an artefact
    /// (plugin.sig and manifest.sig, in place unless --output).
    Sign {
        /// The module or artefact.
        file: PathBuf,
        /// The signing key.
        #[arg(long)]
        key: PathBuf,
        /// Where to write (default: the sidecar, or the artefact itself).
        #[arg(long, alias = "out")]
        output: Option<PathBuf>,
    },
    /// Print an artefact's manifest, entries, sizes and SHA-256s, or a
    /// module's digest, sidecars and exported hooks.
    Inspect {
        /// The module or artefact.
        file: PathBuf,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Verify a module or artefact (integrity, and signatures against a
    /// trust root) without installing it — a pre-deploy / audit check.
    Verify {
        /// The module or artefact.
        #[arg(value_name = "FILE")]
        wasm: PathBuf,
        /// Trust root: a directory of `*.pub` keys, a key file, or a hex key.
        /// Omit to check only integrity and print the SHA-256.
        #[arg(long, alias = "pubkey")]
        trust_root: Option<PathBuf>,
        /// Signature file of a bare module (default: its `.sig` sidecar).
        #[arg(long)]
        sig: Option<PathBuf>,
        /// Require an artefact's manifest to be signed too.
        #[arg(long)]
        require_signed_manifest: bool,
    },
    /// Install a plugin from a registry index into the plugins directory.
    Install {
        /// Plugin name as listed in the registry.
        name: String,
        /// Path to the registry index (JSON).
        #[arg(long)]
        registry: PathBuf,
        /// Destination plugins directory (where the proxy loads from).
        #[arg(long)]
        dest: PathBuf,
        /// Optional Ed25519 trust root. When set, the artefact must carry a
        /// signature and is verified against it.
        #[arg(long)]
        trust_root: Option<PathBuf>,
        /// Pin an exact version (default: first match in the index).
        #[arg(long)]
        version: Option<String>,
    },
    /// List the plugins a registry index offers.
    List {
        #[arg(long)]
        registry: PathBuf,
    },
    /// Scaffold a new plugin crate.
    New {
        /// Plugin name.
        name: String,
        /// Parent directory to create `<name>/` in (default: current dir).
        #[arg(long, default_value = ".")]
        dir: PathBuf,
    },
}

fn read_key(path: &Path) -> Result<ed25519_dalek::SigningKey, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read key {}: {e}", path.display()))?;
    plugin_artefact::parse_signing_key(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn keygen(out_dir: &Path, name: &str) -> Result<(), String> {
    plugin_artefact::validate_name(name).map_err(|e| format!("key name: {e}"))?;
    std::fs::create_dir_all(out_dir).map_err(|e| format!("create {}: {e}", out_dir.display()))?;
    let key_path = out_dir.join(format!("{name}.key"));
    let pub_path = out_dir.join(format!("{name}.pub"));
    for path in [&key_path, &pub_path] {
        if path.exists() {
            return Err(format!(
                "{} already exists; not overwriting it",
                path.display()
            ));
        }
    }
    let key = plugin_artefact::generate_signing_key().map_err(|e| e.to_string())?;
    plugin_artefact::write_atomic(
        &key_path,
        plugin_artefact::signing_key_pem(&key).as_bytes(),
        0o600,
    )
    .map_err(|e| e.to_string())?;
    let public = plugin_artefact::public_key_base64(&key.verifying_key());
    plugin_artefact::write_atomic(&pub_path, format!("{public}\n").as_bytes(), 0o644)
        .map_err(|e| e.to_string())?;
    println!("signing key: {} (keep it secret)", key_path.display());
    println!(
        "public key:  {} (put it in the proxy's trust root)",
        pub_path.display()
    );
    Ok(())
}

/// The `pack` subcommand's arguments.
struct PackArgs {
    wasm: PathBuf,
    manifest: Option<PathBuf>,
    name: Option<String>,
    version: Option<String>,
    description: Option<String>,
    license: Option<String>,
    hooks: Vec<String>,
    sign_with: Option<PathBuf>,
    sig: Option<PathBuf>,
    output: PathBuf,
}

fn pack(args: PackArgs) -> Result<(), String> {
    let PackArgs {
        wasm,
        manifest,
        name,
        version,
        description,
        license,
        hooks,
        sign_with,
        sig,
        output,
    } = args;
    let (wasm, sign_with, sig, output) = (
        wasm.as_path(),
        sign_with.as_deref(),
        sig.as_deref(),
        output.as_path(),
    );
    let base = match manifest.as_deref() {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            let parsed = if path.extension().and_then(|e| e.to_str()) == Some("json") {
                plugin_artefact::parse_json_manifest(&text)
            } else {
                plugin_artefact::parse_yaml_manifest(&text)
            }
            .map_err(|e| format!("{}: {e}", path.display()))?;
            if !parsed.unknown_keys.is_empty() {
                eprintln!(
                    "warning: {} has keys the artefact manifest does not carry: {}",
                    path.display(),
                    parsed.unknown_keys.join(", ")
                );
            }
            parsed
        }
        None => plugin_artefact::SidecarManifest::default(),
    };
    let input = PackInput {
        name: name.unwrap_or(base.name),
        version: version.unwrap_or(base.version),
        description: description.unwrap_or(base.description),
        license: license.unwrap_or(base.license),
        hooks: if hooks.is_empty() { base.hooks } else { hooks },
        wasm: plugin_artefact::read_bounded(wasm, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
            .map_err(|e| e.to_string())?,
        wasm_signature: sig
            .map(|p| {
                std::fs::read_to_string(p)
                    .map(|s| s.trim().to_string())
                    .map_err(|e| format!("read {}: {e}", p.display()))
            })
            .transpose()?,
    };
    if input.hooks.is_empty() {
        return Err("no hooks: pass --hooks or a manifest listing them".to_string());
    }
    let key = sign_with.map(read_key).transpose()?;
    let (manifest, bytes) =
        plugin_artefact::pack(&input, key.as_ref()).map_err(|e| e.to_string())?;
    plugin_artefact::write_atomic(output, &bytes, 0o644).map_err(|e| e.to_string())?;
    println!(
        "packed {} {} -> {} ({} bytes, {})",
        manifest.name,
        manifest.version,
        output.display(),
        bytes.len(),
        if key.is_some() {
            "module and manifest signed"
        } else if input.wasm_signature.is_some() {
            "module signature embedded"
        } else {
            "unsigned"
        }
    );
    Ok(())
}

fn sign(file: &Path, key_path: &Path, output: Option<&Path>) -> Result<(), String> {
    let key = read_key(key_path)?;
    match ArtefactKind::of(file) {
        Some(ArtefactKind::TarGz) => {
            let artefact = plugin_artefact::read(file, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
                .map_err(|e| e.to_string())?;
            let (manifest, bytes) =
                plugin_artefact::resign(&artefact, &key).map_err(|e| e.to_string())?;
            let out = output.unwrap_or(file);
            plugin_artefact::write_atomic(out, &bytes, 0o644).map_err(|e| e.to_string())?;
            println!(
                "signed {} {} -> {} (plugin.sig and manifest.sig)",
                manifest.name,
                manifest.version,
                out.display()
            );
        }
        Some(ArtefactKind::Wasm) => {
            let wasm =
                plugin_artefact::read_bounded(file, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
                    .map_err(|e| e.to_string())?;
            if !plugin_artefact::is_wasm(&wasm) {
                return Err(format!("{} is not a WebAssembly module", file.display()));
            }
            let sig_path = output
                .map(Path::to_path_buf)
                .unwrap_or_else(|| file.with_extension("sig"));
            let sig = plugin_artefact::sign(&key, &wasm);
            plugin_artefact::write_atomic(&sig_path, format!("{sig}\n").as_bytes(), 0o644)
                .map_err(|e| e.to_string())?;
            println!("signed {} -> {}", file.display(), sig_path.display());
        }
        None => {
            return Err(format!(
                "{} is neither a .wasm module nor a .tar.gz artefact",
                file.display()
            ))
        }
    }
    Ok(())
}

fn inspect(file: &Path, json: bool) -> Result<(), String> {
    let bytes = plugin_artefact::read_bounded(file, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
        .map_err(|e| e.to_string())?;
    if plugin_artefact::is_gzip(&bytes) {
        let a = plugin_artefact::unpack(&bytes, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
            .map_err(|e| e.to_string())?;
        let mut entries = vec![
            (plugin_artefact::ENTRY_MANIFEST, a.manifest_json.clone()),
            (plugin_artefact::ENTRY_WASM, a.wasm.clone()),
        ];
        if let Some(sig) = &a.wasm_signature {
            entries.push((plugin_artefact::ENTRY_SIG, sig.clone().into_bytes()));
        }
        if let Some(sig) = &a.manifest_signature {
            entries.push((
                plugin_artefact::ENTRY_MANIFEST_SIG,
                sig.clone().into_bytes(),
            ));
        }
        if json {
            let files: Vec<serde_json::Value> = entries
                .iter()
                .map(|(name, body)| {
                    serde_json::json!({
                        "name": name,
                        "bytes": body.len(),
                        "sha256": plugin_artefact::sha256_hex(body),
                    })
                })
                .collect();
            let out = serde_json::json!({
                "sha256": plugin_artefact::sha256_hex(&bytes),
                "manifest": a.manifest,
                "files": files,
                "signed": a.wasm_signature.is_some(),
                "manifest_signed": a.manifest_signature.is_some(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?
            );
            return Ok(());
        }
        let m = &a.manifest;
        println!("{} {}", m.name, m.version);
        println!("  description: {}", m.description);
        println!("  license:     {}", m.license);
        println!("  hooks:       {}", m.hooks.join(", "));
        println!("  schema:      {}", m.schema_version);
        println!("  packed at:   {}", m.packed_at);
        println!(
            "  signatures:  module {}, manifest {}",
            if a.wasm_signature.is_some() {
                "signed"
            } else {
                "unsigned"
            },
            if a.manifest_signature.is_some() {
                "signed"
            } else {
                "unsigned"
            }
        );
        println!("  files:");
        for (name, body) in &entries {
            println!(
                "    {:<14} {:>9} bytes  sha256={}",
                name,
                body.len(),
                plugin_artefact::sha256_hex(body)
            );
        }
        return Ok(());
    }
    if !plugin_artefact::is_wasm(&bytes) {
        return Err(format!(
            "{} is neither a WebAssembly module nor a .tar.gz artefact",
            file.display()
        ));
    }
    let exports = exported_hooks(&bytes)?;
    let sidecars: Vec<String> = ArtefactKind::Wasm
        .sidecars(file)
        .into_iter()
        .filter(|p| p.exists())
        .map(|p| p.display().to_string())
        .collect();
    if json {
        let out = serde_json::json!({
            "sha256": plugin_artefact::sha256_hex(&bytes),
            "bytes": bytes.len(),
            "exported_hooks": exports,
            "sidecars": sidecars,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&out).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!("{}", file.display());
    println!("  bytes:          {}", bytes.len());
    println!("  sha256:         {}", plugin_artefact::sha256_hex(&bytes));
    println!("  exported hooks: {}", exports.join(", "));
    println!(
        "  sidecars:       {}",
        if sidecars.is_empty() {
            "none".to_string()
        } else {
            sidecars.join(", ")
        }
    );
    Ok(())
}

/// The hook functions a module exports (by name; the proxy checks their
/// signatures when it loads the module).
fn exported_hooks(wasm: &[u8]) -> Result<Vec<String>, String> {
    const HOOKS: [&str; 4] = ["pre_query", "route", "post_query", "authenticate"];
    Ok(plugin_artefact::wasm_exported_functions(wasm)
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|name| HOOKS.contains(&name.as_str()))
        .collect())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.cmd {
        Cmd::Keygen { out_dir, name } => keygen(&out_dir, &name),
        Cmd::Pack {
            wasm,
            manifest,
            name,
            version,
            description,
            license,
            hooks,
            sign_with,
            sig,
            output,
        } => pack(PackArgs {
            wasm,
            manifest,
            name,
            version,
            description,
            license,
            hooks,
            sign_with,
            sig,
            output,
        }),
        Cmd::Sign { file, key, output } => sign(&file, &key, output.as_deref()),
        Cmd::Inspect { file, json } => inspect(&file, json),
        Cmd::Install {
            name,
            registry,
            dest,
            trust_root,
            version,
        } => plugin_registry::install(
            &registry,
            &name,
            version.as_deref(),
            &dest,
            trust_root.as_deref(),
        )
        .map(|r| {
            println!(
                "installed {} v{} -> {}",
                r.name,
                if r.version.is_empty() {
                    "?"
                } else {
                    &r.version
                },
                r.wasm_path.display()
            );
            println!("  sha256: {}", r.sha256);
            match r.signed_by {
                Some(k) => println!("  signature: verified by '{k}'"),
                None => println!("  signature: not checked (no trust root)"),
            }
        }),
        Cmd::List { registry } => plugin_registry::load_index(&registry).map(|idx| {
            for e in &idx.plugins {
                let sig = if e.signature.is_some() {
                    "signed"
                } else {
                    "unsigned"
                };
                println!(
                    "{:<24} {:<10} {:<8} {}",
                    e.name, e.version, sig, e.description
                );
            }
        }),
        Cmd::Verify {
            wasm,
            trust_root,
            sig,
            require_signed_manifest,
        } => plugin_registry::verify_with(
            &wasm,
            trust_root.as_deref(),
            sig.as_deref(),
            require_signed_manifest,
        )
        .map(|r| {
            println!("{}", wasm.display());
            println!("  sha256: {}", r.sha256);
            match (&r.signed_by, &r.manifest_signed_by) {
                (Some(k), Some(m)) => {
                    println!("  OK: module signed by '{k}', manifest signed by '{m}'")
                }
                (Some(k), None) => println!("  OK: signature valid for '{k}'"),
                _ => println!("  integrity OK; signature not checked (no trust root)"),
            }
        }),
        Cmd::New { name, dir } => plugin_registry::scaffold(&name, &dir).map(|root| {
            println!("scaffolded plugin at {}", root.display());
        }),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

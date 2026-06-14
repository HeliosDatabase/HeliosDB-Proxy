//! `helios-plugin` — plugin registry CLI (Batch H, item 78).
//!
//! Distribution verbs for the WASM plugin platform: resolve a signed artefact
//! from a registry index, verify it, and drop it where the proxy's hot-reload
//! watcher picks it up — plus a scaffold for new plugins.
//!
//!   helios-plugin install <name> --registry <index.json> --dest <plugins-dir> \
//!       [--trust-root <dir>] [--version <v>]
//!   helios-plugin list    --registry <index.json>
//!   helios-plugin new     <name> [--dir <path>]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use heliosdb_proxy::plugin_registry;

#[derive(Parser)]
#[command(name = "helios-plugin", about = "HeliosProxy plugin registry + install")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Install a plugin from a registry index into the plugins directory.
    Install {
        /// Plugin name as listed in the registry.
        name: String,
        /// Path to the registry index (JSON).
        #[arg(long)]
        registry: PathBuf,
        /// Destination plugins directory (where the proxy hot-reloads from).
        #[arg(long)]
        dest: PathBuf,
        /// Optional Ed25519 trust root (dir of `*.pub` keys). When set, the
        /// artefact must carry a signature and is verified against it.
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
    /// Verify a local plugin artefact (SHA-256, and signature against a trust
    /// root) without installing it — a pre-deploy / audit check.
    Verify {
        /// Path to the `.wasm` artefact.
        wasm: PathBuf,
        /// Ed25519 trust root (dir of `*.pub` keys). Omit to print only the
        /// SHA-256 digest.
        #[arg(long)]
        trust_root: Option<PathBuf>,
        /// Signature file (base64 Ed25519). Default: a `<name>.sig` sidecar.
        #[arg(long)]
        sig: Option<PathBuf>,
    },
    /// Scaffold a new plugin source skeleton.
    New {
        /// Plugin name.
        name: String,
        /// Parent directory to create `<name>/` in (default: current dir).
        #[arg(long, default_value = ".")]
        dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.cmd {
        Cmd::Install { name, registry, dest, trust_root, version } => {
            match plugin_registry::install(
                &registry,
                &name,
                version.as_deref(),
                &dest,
                trust_root.as_deref(),
            ) {
                Ok(r) => {
                    println!(
                        "installed {} v{} -> {}",
                        r.name,
                        if r.version.is_empty() { "?" } else { &r.version },
                        r.wasm_path.display()
                    );
                    println!("  sha256: {}", r.sha256);
                    match r.signed_by {
                        Some(k) => println!("  signature: verified by '{k}'"),
                        None if trust_root.is_some() => unreachable!(),
                        None => println!("  signature: not checked (no trust root)"),
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        Cmd::List { registry } => plugin_registry::load_index(&registry).map(|idx| {
            for e in &idx.plugins {
                let sig = if e.signature.is_some() { "signed" } else { "unsigned" };
                println!("{:<24} {:<10} {:<8} {}", e.name, e.version, sig, e.description);
            }
        }),
        Cmd::Verify { wasm, trust_root, sig } => {
            plugin_registry::verify(&wasm, trust_root.as_deref(), sig.as_deref()).map(|r| {
                println!("{}", wasm.display());
                println!("  sha256: {}", r.sha256);
                match r.signed_by {
                    Some(k) => println!("  signature: verified by '{k}'"),
                    None if trust_root.is_some() => unreachable!(),
                    None => println!("  signature: not checked (no trust root)"),
                }
            })
        }
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

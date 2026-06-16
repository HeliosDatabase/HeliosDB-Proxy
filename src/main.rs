//! HeliosDB Proxy - Main Entry Point
//!
//! Standalone proxy binary for HeliosDB-Lite connection routing.

use clap::{Args, Parser, Subcommand, ValueEnum};
use heliosdb_proxy::{
    config::ProxyConfig,
    server::ProxyServer,
    skills::{self, InstallMode, InstallTarget},
    Result, VERSION,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// HeliosDB Proxy - Connection Router and Failover Manager
#[derive(Parser, Debug)]
#[command(name = "heliosdb-proxy")]
#[command(version = VERSION)]
#[command(about = "HeliosDB Proxy - Intelligent connection router for HeliosDB-Lite")]
#[command(arg_required_else_help = false)]
struct Cli {
    /// Subcommand. When omitted, runs the proxy daemon with the
    /// flags below.
    #[command(subcommand)]
    command: Option<Command>,

    // ── Daemon-mode flags (used when `command` is None) ──────────

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Listen address
    #[arg(short, long, default_value = "0.0.0.0:5432")]
    listen: String,

    /// Admin API address
    #[arg(long, default_value = "0.0.0.0:9090")]
    admin: String,

    /// Primary node (host:port)
    #[arg(long)]
    primary: Option<String>,

    /// Standby nodes (can be specified multiple times)
    #[arg(long)]
    standby: Vec<String>,

    /// Enable TR (Transaction Replay)
    #[arg(long, default_value = "true")]
    tr: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable JSON logging
    #[arg(long)]
    json_logs: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Install bundled resources from this binary into the user's environment.
    Install {
        #[command(subcommand)]
        what: InstallWhat,
    },
}

#[derive(Subcommand, Debug)]
enum InstallWhat {
    /// Deploy the embedded operator skill bundle (~/.claude/skills, ~/.codex/skills).
    Skills(SkillsArgs),
}

#[derive(Args, Debug)]
struct SkillsArgs {
    /// Where to install. `both` writes to whichever of ~/.claude and ~/.codex exists.
    #[arg(long, value_enum, default_value_t = SkillTargetCli::Both)]
    target: SkillTargetCli,

    /// Symlink the skills into the user's directory instead of copying. The
    /// embedded bundle is extracted to ~/.local/share/heliosdb-proxy/skills/
    /// once; the per-target entries point at it. Re-running this command
    /// after a binary upgrade re-extracts and refreshes the cache, so the
    /// existing symlinks pick up the new content automatically.
    #[arg(long)]
    symlink: bool,

    /// Overwrite pre-existing heliosproxy-* skills at the target. Without this
    /// flag, existing entries are skipped and reported.
    #[arg(long)]
    force: bool,

    /// Print the planned actions without writing anything.
    #[arg(long)]
    dry_run: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum SkillTargetCli {
    Claude,
    Codex,
    Both,
}

impl From<SkillTargetCli> for InstallTarget {
    fn from(t: SkillTargetCli) -> Self {
        match t {
            SkillTargetCli::Claude => InstallTarget::Claude,
            SkillTargetCli::Codex => InstallTarget::Codex,
            SkillTargetCli::Both => InstallTarget::Both,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(Command::Install { what }) = cli.command {
        // Subcommands set up minimal logging and bypass the daemon path.
        init_logging(&cli.log_level, cli.json_logs);
        return match what {
            InstallWhat::Skills(args) => run_install_skills(args),
        };
    }

    // Daemon mode (existing behaviour).
    init_logging(&cli.log_level, cli.json_logs);

    tracing::info!("HeliosDB Proxy v{} starting...", VERSION);

    let config = load_config(&cli)?;
    // Retain the config path so SIGHUP can re-read it for a live reload.
    let server = ProxyServer::new(config)?.with_config_path(cli.config.clone());

    tracing::info!("Starting proxy server on {}", cli.listen);
    server.run().await?;

    tracing::info!("Proxy server stopped");
    Ok(())
}

fn run_install_skills(args: SkillsArgs) -> Result<()> {
    let mode = if args.symlink {
        InstallMode::Symlink
    } else {
        InstallMode::Copy
    };
    let target: InstallTarget = args.target.into();

    let report = skills::install_skills(target, mode, args.force, args.dry_run)
        .map_err(|e| heliosdb_proxy::ProxyError::Internal(format!("install skills: {}", e)))?;

    let prefix = if args.dry_run { "[dry-run] " } else { "" };
    println!(
        "{}heliosproxy skill bundle v{} — {} mode",
        prefix,
        VERSION,
        if args.symlink { "symlink" } else { "copy" }
    );
    if !report.installed.is_empty() {
        println!(
            "{}{} entries {}:",
            prefix,
            report.installed.len(),
            if args.dry_run { "would be installed" } else { "installed" }
        );
        for p in &report.installed {
            println!("  + {}", p.display());
        }
    }
    if !report.overwrote.is_empty() {
        println!(
            "{}{} entries {}:",
            prefix,
            report.overwrote.len(),
            if args.dry_run { "would be overwritten" } else { "overwritten" }
        );
        for p in &report.overwrote {
            println!("  ~ {}", p.display());
        }
    }
    if !report.skipped.is_empty() {
        println!(
            "{}{} entries skipped (pass --force to overwrite):",
            prefix,
            report.skipped.len()
        );
        for p in &report.skipped {
            println!("  = {}", p.display());
        }
    }
    if !report.errors.is_empty() {
        println!("{}{} errors:", prefix, report.errors.len());
        for (p, e) in &report.errors {
            println!("  ! {}: {}", p.display(), e);
        }
    }
    Ok(())
}

fn init_logging(level: &str, json: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));

    let subscriber = tracing_subscriber::registry().with(filter);

    if json {
        subscriber
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        subscriber
            .with(tracing_subscriber::fmt::layer())
            .init();
    }
}

fn load_config(cli: &Cli) -> Result<ProxyConfig> {
    if let Some(ref path) = cli.config {
        return ProxyConfig::from_file(path);
    }

    let mut config = ProxyConfig {
        listen_address: cli.listen.clone(),
        admin_address: cli.admin.clone(),
        tr_enabled: cli.tr,
        ..ProxyConfig::default()
    };

    if let Some(ref primary) = cli.primary {
        config.add_node(primary, "primary")?;
    }
    for standby in &cli.standby {
        config.add_node(standby, "standby")?;
    }

    config.validate()?;
    Ok(config)
}

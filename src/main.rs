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

    /// Admin API address. Defaults to loopback: the admin API is privileged, so
    /// the proxy refuses a non-loopback bind without an admin token (set one in
    /// a config file, or set admin_allow_insecure).
    #[arg(long, default_value = "127.0.0.1:9090")]
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
            if args.dry_run {
                "would be installed"
            } else {
                "installed"
            }
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
            if args.dry_run {
                "would be overwritten"
            } else {
                "overwritten"
            }
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
        subscriber.with(tracing_subscriber::fmt::layer()).init();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_defaults_when_no_args() {
        let cli = Cli::try_parse_from(["heliosdb-proxy"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.config, None);
        assert_eq!(cli.listen, "0.0.0.0:5432");
        assert_eq!(cli.admin, "127.0.0.1:9090");
        assert_eq!(cli.primary, None);
        assert!(cli.standby.is_empty());
        assert!(cli.tr);
        assert_eq!(cli.log_level, "info");
        assert!(!cli.json_logs);
    }

    #[test]
    fn parses_config_flag_short_and_long() {
        let short = Cli::try_parse_from(["heliosdb-proxy", "-c", "proxy.toml"]).unwrap();
        assert_eq!(short.config, Some("proxy.toml".to_string()));

        let long = Cli::try_parse_from(["heliosdb-proxy", "--config", "proxy.toml"]).unwrap();
        assert_eq!(long.config, Some("proxy.toml".to_string()));
    }

    #[test]
    fn parses_listen_flag_short_and_long() {
        let short = Cli::try_parse_from(["heliosdb-proxy", "-l", "0.0.0.0:6000"]).unwrap();
        assert_eq!(short.listen, "0.0.0.0:6000");

        let long = Cli::try_parse_from(["heliosdb-proxy", "--listen", "0.0.0.0:6001"]).unwrap();
        assert_eq!(long.listen, "0.0.0.0:6001");
    }

    #[test]
    fn parses_admin_flag() {
        let cli = Cli::try_parse_from(["heliosdb-proxy", "--admin", "10.0.0.5:9999"]).unwrap();
        assert_eq!(cli.admin, "10.0.0.5:9999");
    }

    #[test]
    fn parses_primary_flag() {
        let cli = Cli::try_parse_from(["heliosdb-proxy", "--primary", "10.0.0.1:5432"]).unwrap();
        assert_eq!(cli.primary, Some("10.0.0.1:5432".to_string()));
    }

    #[test]
    fn parses_repeated_standby_flags() {
        let cli = Cli::try_parse_from([
            "heliosdb-proxy",
            "--standby",
            "10.0.0.2:5432",
            "--standby",
            "10.0.0.3:5432",
        ])
        .unwrap();
        assert_eq!(
            cli.standby,
            vec!["10.0.0.2:5432".to_string(), "10.0.0.3:5432".to_string()]
        );
    }

    #[test]
    fn tr_flag_is_a_presence_switch_not_a_valued_option() {
        // `#[arg(long, default_value = "true")]` on a `bool` field still gets
        // clap's automatic `ArgAction::SetTrue` for the type: `--tr` takes NO
        // value. Passing `--tr false` does not set `tr = false` — "false" is
        // left over and clap tries (and fails) to match it against the
        // `Option<Command>` subcommand slot instead. This means there is
        // currently no CLI-level way to disable TR by passing an explicit
        // value; only omitting `--tr` (leaving it at its `true` default)
        // or passing the bare `--tr` flag are meaningful, and both leave
        // `tr == true`. Documented here rather than "fixed" — production
        // behaviour must not change.
        let cli = Cli::try_parse_from(["heliosdb-proxy", "--tr"]).unwrap();
        assert!(cli.tr);

        let err = Cli::try_parse_from(["heliosdb-proxy", "--tr", "false"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn parses_log_level_and_json_logs() {
        let cli =
            Cli::try_parse_from(["heliosdb-proxy", "--log-level", "debug", "--json-logs"]).unwrap();
        assert_eq!(cli.log_level, "debug");
        assert!(cli.json_logs);
    }

    #[test]
    fn parses_install_skills_target_both_dry_run() {
        let cli = Cli::try_parse_from([
            "heliosdb-proxy",
            "install",
            "skills",
            "--target",
            "both",
            "--dry-run",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Install {
                what: InstallWhat::Skills(args),
            }) => {
                assert!(matches!(args.target, SkillTargetCli::Both));
                assert!(args.dry_run);
                assert!(!args.symlink);
                assert!(!args.force);
            }
            other => panic!("expected Install{{Skills}} subcommand, got {:?}", other),
        }
    }

    #[test]
    fn rejects_unknown_flag() {
        assert!(Cli::try_parse_from(["heliosdb-proxy", "--not-a-real-flag"]).is_err());
    }
}

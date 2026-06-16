//! Embedded HeliosProxy skill-bundle deployer.
//!
//! Lets users who installed via `cargo install heliosdb-proxy`
//! (no git clone, no repo on disk) deploy the `.claude/skills/`
//! bundle into their Claude Code or Codex environment via:
//!
//! ```text
//! heliosdb-proxy install skills              # copy into both ~/.claude and ~/.codex
//! heliosdb-proxy install skills --symlink    # symlink (refreshes on next run after upgrade)
//! heliosdb-proxy install skills --target claude --dry-run
//! ```
//!
//! ## Modes
//!
//! - **Copy** (default): every skill file is written under
//!   `<target>/skills/heliosproxy-<name>/SKILL.md`. Stable across
//!   binary uninstalls.
//! - **Symlink**: the bundle is first extracted to
//!   `~/.local/share/heliosdb-proxy/skills/`, then each
//!   `<target>/skills/heliosproxy-<name>` is a symlink into that
//!   cache. Re-running after a binary upgrade overwrites the cache;
//!   the symlinks resolve to the fresh content.

use include_dir::{include_dir, Dir, DirEntry};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// The 22-skill bundle, embedded at compile time.
///
/// Resolved relative to `CARGO_MANIFEST_DIR`. The directory must
/// exist at build time. If you change the bundle layout, both this
/// const and the deployer below need an audit.
pub static EMBEDDED_SKILLS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/.claude/skills");

/// Where to deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallTarget {
    /// `~/.claude/skills/`
    Claude,
    /// `~/.codex/skills/`
    Codex,
    /// Both — install to whichever target dir(s) exist.
    Both,
}

/// How to deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallMode {
    /// Each skill is a real directory tree owned by the target.
    Copy,
    /// Each skill is a symlink into the binary-managed cache.
    Symlink,
}

/// Outcome of an install run.
#[derive(Debug, Default)]
pub struct InstallReport {
    /// New entries created.
    pub installed: Vec<PathBuf>,
    /// Pre-existing entries left untouched (no `--force`).
    pub skipped: Vec<PathBuf>,
    /// Pre-existing entries replaced (with `--force`).
    pub overwrote: Vec<PathBuf>,
    /// Per-entry errors (used when one entry fails but others succeed).
    pub errors: Vec<(PathBuf, String)>,
}

impl InstallReport {
    /// Total entries acted on (installed + overwrote).
    pub fn changes(&self) -> usize {
        self.installed.len() + self.overwrote.len()
    }
}

/// Errors that abort the whole run.
#[derive(Debug, Error)]
pub enum InstallError {
    #[error("$HOME is not set")]
    NoHome,
    #[error(
        "no valid install target — neither {claude} nor {codex} exists; \
         create the parent directory (`mkdir -p ~/.claude` or `~/.codex`) and retry"
    )]
    NoTargetDir { claude: String, codex: String },
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// Install the embedded bundle into the user's Claude / Codex skills directory.
///
/// Returns a [`InstallReport`] summarising what happened. On
/// `dry_run = true`, no filesystem writes occur but the report
/// reflects what would have been done.
pub fn install_skills(
    target: InstallTarget,
    mode: InstallMode,
    force: bool,
    dry_run: bool,
) -> Result<InstallReport, InstallError> {
    let home = std::env::var("HOME").map_err(|_| InstallError::NoHome)?;
    install_skills_at(&PathBuf::from(home), target, mode, force, dry_run)
}

/// Same as [`install_skills`] but takes the home directory explicitly —
/// mostly for tests, which can't rely on the process-global `$HOME`.
pub fn install_skills_at(
    home: &Path,
    target: InstallTarget,
    mode: InstallMode,
    force: bool,
    dry_run: bool,
) -> Result<InstallReport, InstallError> {
    let dirs = resolve_targets(home, target)?;

    // For symlink mode, extract the bundle into a stable on-disk
    // cache once, then point every per-target symlink at it.
    let cache_dir = if mode == InstallMode::Symlink {
        let cache = home.join(".local/share/heliosdb-proxy/skills");
        if !dry_run {
            extract_bundle_to(&cache)?;
        }
        Some(cache)
    } else {
        None
    };

    let mut report = InstallReport::default();
    for dest_root in dirs {
        deploy_to(
            &dest_root,
            cache_dir.as_deref(),
            mode,
            force,
            dry_run,
            &mut report,
        )?;
    }

    Ok(report)
}

/// Resolve the requested install target into concrete `<dir>/skills` paths.
fn resolve_targets(home: &Path, target: InstallTarget) -> Result<Vec<PathBuf>, InstallError> {
    let claude_root = home.join(".claude");
    let codex_root = home.join(".codex");

    let want_claude = matches!(target, InstallTarget::Claude | InstallTarget::Both);
    let want_codex = matches!(target, InstallTarget::Codex | InstallTarget::Both);

    let mut out = Vec::new();
    if want_claude && claude_root.exists() {
        out.push(claude_root.join("skills"));
    }
    if want_codex && codex_root.exists() {
        out.push(codex_root.join("skills"));
    }

    if out.is_empty() {
        return Err(InstallError::NoTargetDir {
            claude: claude_root.display().to_string(),
            codex: codex_root.display().to_string(),
        });
    }
    Ok(out)
}

/// Deploy every top-level entry from the embedded bundle into `dest_root`.
fn deploy_to(
    dest_root: &Path,
    cache_dir: Option<&Path>,
    mode: InstallMode,
    force: bool,
    dry_run: bool,
    report: &mut InstallReport,
) -> Result<(), InstallError> {
    if !dry_run {
        fs::create_dir_all(dest_root)?;
    }

    for entry in EMBEDDED_SKILLS.entries() {
        let name = match entry.path().file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        let dest = dest_root.join(name);

        let pre_exists = dest.exists() || dest.is_symlink();
        if pre_exists && !force {
            report.skipped.push(dest);
            continue;
        }
        if pre_exists {
            if !dry_run {
                remove_path(&dest)?;
            }
            report.overwrote.push(dest.clone());
        }

        match mode {
            InstallMode::Copy => {
                if !dry_run {
                    copy_entry(entry, &dest)?;
                }
            }
            InstallMode::Symlink => {
                let cache = cache_dir.expect("cache_dir set when symlink mode");
                let src = cache.join(name);
                if !dry_run {
                    create_symlink(&src, &dest)?;
                }
            }
        }
        report.installed.push(dest);
    }

    Ok(())
}

/// Remove a file, directory, or symlink uniformly.
fn remove_path(p: &Path) -> io::Result<()> {
    let meta = fs::symlink_metadata(p)?;
    if meta.file_type().is_dir() {
        fs::remove_dir_all(p)
    } else {
        fs::remove_file(p)
    }
}

/// Recursively materialise an embedded entry to disk.
fn copy_entry(entry: &DirEntry<'_>, dest: &Path) -> io::Result<()> {
    match entry {
        DirEntry::Dir(d) => {
            fs::create_dir_all(dest)?;
            for child in d.entries() {
                let child_name = child.path().file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing file name")
                })?;
                copy_entry(child, &dest.join(child_name))?;
            }
        }
        DirEntry::File(f) => {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(dest, f.contents())?;
        }
    }
    Ok(())
}

/// Extract the entire embedded bundle to `target`, replacing any
/// existing content. Used by symlink mode as the symlink target.
fn extract_bundle_to(target: &Path) -> io::Result<()> {
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    fs::create_dir_all(target)?;
    EMBEDDED_SKILLS.extract(target)?;
    Ok(())
}

#[cfg(unix)]
fn create_symlink(src: &Path, dst: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(src, dst)
}

#[cfg(windows)]
fn create_symlink(src: &Path, dst: &Path) -> io::Result<()> {
    if src.is_dir() {
        std::os::windows::fs::symlink_dir(src, dst)
    } else {
        std::os::windows::fs::symlink_file(src, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn embedded_bundle_has_overview_and_template() {
        // Sanity: the include_dir!() macro picked up real content.
        assert!(EMBEDDED_SKILLS.get_dir("heliosproxy-overview").is_some());
        assert!(EMBEDDED_SKILLS.get_file("_template.md").is_some());
        assert!(EMBEDDED_SKILLS.get_file("_index/verb-map.md").is_some());
    }

    #[test]
    fn embedded_bundle_has_22_skills() {
        let n = EMBEDDED_SKILLS
            .entries()
            .iter()
            .filter(|e| matches!(e, DirEntry::Dir(d) if d.path().file_name().and_then(|f| f.to_str()).map(|n| n.starts_with("heliosproxy-")).unwrap_or(false)))
            .count();
        assert_eq!(
            n, 22,
            "expected 22 heliosproxy-* skill directories in the bundle"
        );
    }

    #[test]
    fn resolve_targets_errors_when_no_dirs_exist() {
        let tmp = TempDir::new().unwrap();
        let err = resolve_targets(tmp.path(), InstallTarget::Both).unwrap_err();
        assert!(matches!(err, InstallError::NoTargetDir { .. }));
    }

    #[test]
    fn resolve_targets_picks_existing_dirs() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude")).unwrap();
        let dirs = resolve_targets(tmp.path(), InstallTarget::Both).unwrap();
        assert_eq!(dirs, vec![tmp.path().join(".claude/skills")]);
    }

    #[test]
    fn install_copy_mode_writes_skill_files() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude")).unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Copy,
            false,
            false,
        )
        .unwrap();
        assert!(report.changes() >= 22);
        let f = tmp
            .path()
            .join(".claude/skills/heliosproxy-overview/SKILL.md");
        assert!(f.exists());
        let body = fs::read_to_string(&f).unwrap();
        assert!(body.contains("HeliosProxy"));
    }

    #[test]
    fn install_skips_existing_without_force() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude/skills/heliosproxy-overview")).unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Copy,
            false,
            false,
        )
        .unwrap();
        assert!(report
            .skipped
            .iter()
            .any(|p| p.ends_with("heliosproxy-overview")));
    }

    #[test]
    fn install_force_overwrites() {
        let tmp = TempDir::new().unwrap();
        let pre = tmp.path().join(".claude/skills/heliosproxy-overview");
        fs::create_dir_all(&pre).unwrap();
        fs::write(pre.join("stale.txt"), b"old").unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Copy,
            true,
            false,
        )
        .unwrap();
        assert!(report
            .overwrote
            .iter()
            .any(|p| p.ends_with("heliosproxy-overview")));
        assert!(!pre.join("stale.txt").exists());
        assert!(pre.join("SKILL.md").exists());
    }

    #[test]
    fn dry_run_writes_nothing() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude")).unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Copy,
            false,
            true,
        )
        .unwrap();
        assert!(report.changes() >= 22);
        assert!(!tmp
            .path()
            .join(".claude/skills/heliosproxy-overview")
            .exists());
    }

    #[cfg(unix)]
    #[test]
    fn install_symlink_mode_creates_symlinks() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude")).unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Symlink,
            false,
            false,
        )
        .unwrap();
        assert!(report.changes() >= 22);
        let link = tmp.path().join(".claude/skills/heliosproxy-overview");
        let meta = fs::symlink_metadata(&link).unwrap();
        assert!(meta.file_type().is_symlink());
        let target = fs::read_link(&link).unwrap();
        assert!(
            target
                .to_string_lossy()
                .contains(".local/share/heliosdb-proxy/skills"),
            "symlink target unexpected: {}",
            target.display()
        );
        let cache = tmp
            .path()
            .join(".local/share/heliosdb-proxy/skills/heliosproxy-overview/SKILL.md");
        assert!(cache.exists());
    }

    #[cfg(unix)]
    #[test]
    fn install_symlink_then_force_replaces_link() {
        // Re-run scenario: simulate the binary being upgraded — operator
        // re-runs the command, and the prior symlink should be replaced
        // and pointed at the freshly extracted cache.
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join(".claude")).unwrap();
        install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Symlink,
            false,
            false,
        )
        .unwrap();
        let report = install_skills_at(
            tmp.path(),
            InstallTarget::Claude,
            InstallMode::Symlink,
            true, // force on the second run
            false,
        )
        .unwrap();
        assert!(report.changes() >= 22);
    }
}

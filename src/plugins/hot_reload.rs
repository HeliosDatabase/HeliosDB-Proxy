//! Hot Reload Support
//!
//! Change detection for the plugin directory.
//!
//! [`DirWatch`] is what the [`PluginManager`](super::PluginManager) builds
//! its directory reconcile on: every scan fingerprints each plugin file
//! (`.wasm` with its `.sig` / `.yaml` / `.json` sidecars, or `.tar.gz`) and
//! the trust root, and reports a change only once it has been stable for the
//! debounce period, so a file still being written is never loaded.
//!
//! A fingerprint is the files' stamps (size, modification time, inode) plus
//! a SHA-256 of their content. The content is hashed when a stamp changes
//! and, while a file is younger than [`RACY_WINDOW`], on every scan: two
//! writes of the same size within one timestamp tick leave the stamp
//! unchanged, and only the content shows the second one.
//!
//! [`HotReloader`] is the event-style interface over the same scanner
//! (`Added` / `Modified` / `Removed`), for embedders that drive reloads
//! themselves.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;

use super::runtime::PluginError;
use crate::plugin_artefact::ArtefactKind;

/// Default time a change must be stable before it is acted on.
pub const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(500);

/// Default minimum interval between two [`HotReloader::check`] scans.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// How long after a file's modification time its stamp may still hide a
/// same-size rewrite: the coarsest common file-system timestamp granularity
/// (FAT's two seconds).
pub const RACY_WINDOW: Duration = Duration::from_secs(2);

/// What identifies one version of a file without reading it: size,
/// modification time and (on Unix) inode, so an atomic rename of a new
/// file over an old one is a change even with an equal size and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    inode: u64,
}

impl FileStamp {
    /// The stamp of the file at `path`, or `None` when there is none.
    pub fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        if !meta.is_file() {
            return None;
        }
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(&meta);
        #[cfg(not(unix))]
        let inode = 0;
        Some(Self {
            len: meta.len(),
            modified: meta.modified().ok(),
            inode,
        })
    }
}

/// The version of a set of files: their stamps and a SHA-256 of their
/// content (absent files count as empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    stamps: Vec<Option<FileStamp>>,
    digest: String,
}

impl Fingerprint {
    /// Fingerprint the plugin file at `path` and its sidecars.
    pub fn of_plugin(path: &Path) -> Self {
        Self::of_files(&Self::plugin_files(path))
    }

    /// Fingerprint a trust root as given in the configuration: a directory
    /// (its `*.pub` files), a key file, or inline hex (constant).
    pub fn of_trust_root(spec: &str) -> Self {
        let path = Path::new(spec);
        let files = if path.is_dir() {
            let mut keys: Vec<PathBuf> = std::fs::read_dir(path)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok().map(|e| e.path()))
                        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("pub"))
                        .collect()
                })
                .unwrap_or_default();
            keys.sort();
            keys
        } else {
            vec![path.to_path_buf()]
        };
        Self::of_files(&files)
    }

    /// A plugin file followed by its sidecars.
    fn plugin_files(path: &Path) -> Vec<PathBuf> {
        let mut files = vec![path.to_path_buf()];
        if let Some(kind) = ArtefactKind::of(path) {
            files.extend(kind.sidecars(path));
        }
        files
    }

    /// Fingerprint `files`: stamp and hash them.
    fn of_files(files: &[PathBuf]) -> Self {
        Self {
            stamps: Self::stamps(files),
            digest: Self::digest(files),
        }
    }

    fn stamps(files: &[PathBuf]) -> Vec<Option<FileStamp>> {
        files.iter().map(|p| FileStamp::of(p)).collect()
    }

    /// Whether a digest taken at `hashed_at` of files with these stamps
    /// still identifies their content: every file was older than
    /// [`RACY_WINDOW`] then, so a later write must change a stamp.
    fn digest_holds(stamps: &[Option<FileStamp>], hashed_at: SystemTime) -> bool {
        stamps.iter().flatten().all(|s| {
            s.modified.is_some_and(|m| {
                hashed_at
                    .duration_since(m)
                    .is_ok_and(|age| age > RACY_WINDOW)
            })
        })
    }

    fn digest(files: &[PathBuf]) -> String {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        for path in files {
            // Each file is framed so content cannot shift between files.
            hasher.update(path.as_os_str().as_encoded_bytes());
            hasher.update([0u8]);
            match std::fs::File::open(path) {
                Ok(mut file) => loop {
                    match file.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => hasher.update(&buf[..n]),
                        Err(_) => {
                            hasher.update(b"\xffunreadable");
                            break;
                        }
                    }
                },
                Err(_) => hasher.update(b"\xffabsent"),
            }
            hasher.update([0xffu8]);
        }
        crate::plugin_artefact::sha256_hex(&hasher.finalize())
    }

    /// Whether the plugin file itself exists.
    pub fn present(&self) -> bool {
        self.stamps.first().is_some_and(Option::is_some)
    }
}

/// A fingerprint, when it was first observed, and when its digest was
/// taken (to reuse it while the files are unchanged and old enough).
#[derive(Debug, Clone)]
struct Observed {
    fingerprint: Fingerprint,
    since: Instant,
    hashed_at: SystemTime,
}

impl Observed {
    fn new(fingerprint: Fingerprint, now: Instant) -> Self {
        Self {
            fingerprint,
            since: now,
            hashed_at: SystemTime::now(),
        }
    }

    /// Re-fingerprint `files`: the stamps every time, the content only
    /// when a stamp changed or the last digest was taken while a file was
    /// younger than [`RACY_WINDOW`].
    fn refresh(&mut self, files: &[PathBuf], now: Instant) {
        let stamps = Fingerprint::stamps(files);
        if stamps == self.fingerprint.stamps && Fingerprint::digest_holds(&stamps, self.hashed_at) {
            return;
        }
        self.hashed_at = SystemTime::now();
        let fingerprint = Fingerprint {
            digest: Fingerprint::digest(files),
            stamps,
        };
        if fingerprint != self.fingerprint {
            self.fingerprint = fingerprint;
            self.since = now;
        }
    }

    fn settled(&self, now: Instant, debounce: Duration, force: bool) -> bool {
        force || now.saturating_duration_since(self.since) >= debounce
    }
}

/// One plugin file in a [`DirScan`].
#[derive(Debug, Clone)]
pub struct ScannedFile {
    /// The file.
    pub path: PathBuf,
    /// Its current version.
    pub fingerprint: Fingerprint,
    /// Whether that version has been stable for the debounce period.
    pub settled: bool,
}

/// What a [`DirWatch::scan`] found.
#[derive(Debug, Clone, Default)]
pub struct DirScan {
    /// Plugin files present, sorted by path.
    pub files: Vec<ScannedFile>,
    /// Plugin files seen before whose absence has settled.
    pub removed: Vec<PathBuf>,
    /// The trust root's settled fingerprint, when one is configured and has
    /// settled (an unsettled change keeps the previous one).
    pub trust: Option<Fingerprint>,
    /// Why the directory could not be listed, if it could not (a missing
    /// directory is not an error: it holds no plugins).
    pub unreadable: Option<String>,
}

/// Change detection for one plugin directory (and its trust root).
#[derive(Debug)]
pub struct DirWatch {
    dir: PathBuf,
    trust_root: Option<String>,
    debounce: Duration,
    files: BTreeMap<PathBuf, Observed>,
    trust: Option<Observed>,
    trust_settled: Option<Fingerprint>,
}

impl DirWatch {
    /// Watch `dir`, and `trust_root` (as configured) when given.
    pub fn new(dir: &Path, trust_root: Option<&str>, debounce: Duration) -> Self {
        Self {
            dir: dir.to_path_buf(),
            trust_root: trust_root.map(str::to_string),
            debounce,
            files: BTreeMap::new(),
            trust: None,
            trust_settled: None,
        }
    }

    /// The watched directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Change the debounce period.
    pub fn set_debounce(&mut self, debounce: Duration) {
        self.debounce = debounce;
    }

    /// The debounce period.
    pub fn debounce(&self) -> Duration {
        self.debounce
    }

    /// Scan the directory. With `force` every change counts as settled
    /// (startup, an explicit rescan).
    pub fn scan(&mut self, now: Instant, force: bool) -> DirScan {
        let mut scan = DirScan::default();
        let mut present: Vec<PathBuf> = match std::fs::read_dir(&self.dir) {
            // Regular files only: symlinks are not followed, so a plugin
            // is always a file of this directory.
            Ok(entries) => entries
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
                .map(|e| e.path())
                .filter(|p| ArtefactKind::of(p).is_some())
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                scan.unreadable = Some(format!("{}: {e}", self.dir.display()));
                // An unreadable directory is not evidence that its files
                // are gone: keep every known file as it was.
                self.files.keys().cloned().collect()
            }
        };
        present.sort();

        for path in &present {
            let files = Fingerprint::plugin_files(path);
            let observed = match self.files.entry(path.clone()) {
                std::collections::btree_map::Entry::Occupied(entry) => {
                    let observed = entry.into_mut();
                    observed.refresh(&files, now);
                    observed
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(Observed::new(Fingerprint::of_files(&files), now))
                }
            };
            let settled = observed.settled(now, self.debounce, force);
            if observed.fingerprint.present() {
                scan.files.push(ScannedFile {
                    path: path.clone(),
                    fingerprint: observed.fingerprint.clone(),
                    settled,
                });
            }
        }
        // Files no longer listed: absent from now on; gone once that has
        // settled (a delete-then-write replacement is not a removal).
        let missing: Vec<PathBuf> = self
            .files
            .keys()
            .filter(|p| present.binary_search(p).is_err())
            .cloned()
            .collect();
        for path in missing {
            if FileStamp::of(&path).is_some() {
                // Listed nowhere but still there (a race with the
                // listing): treat it as unchanged until the next scan.
                continue;
            }
            let observed = self.files.get_mut(&path).expect("known file");
            observed.refresh(&Fingerprint::plugin_files(&path), now);
            if observed.settled(now, self.debounce, force) {
                self.files.remove(&path);
                scan.removed.push(path);
            }
        }

        if let Some(spec) = &self.trust_root {
            let fingerprint = Fingerprint::of_trust_root(spec);
            let observed = self
                .trust
                .get_or_insert_with(|| Observed::new(fingerprint.clone(), now));
            if fingerprint != observed.fingerprint {
                observed.fingerprint = fingerprint;
                observed.since = now;
            }
            if observed.settled(now, self.debounce, force) || self.trust_settled.is_none() {
                self.trust_settled = Some(observed.fingerprint.clone());
            }
            scan.trust = self.trust_settled.clone();
        }
        scan
    }
}

/// Hot reloader for plugins: event-style change detection over a plugin
/// directory, for embedders that load and unload plugins themselves.
///
/// Register the plugins you loaded ([`register`](Self::register)); each
/// [`check`](Self::check) then reports, once per settled change:
/// [`ReloadEvent::Added`] for a plugin file not registered (new, or
/// changed since it was last reported), [`ReloadEvent::Modified`] for a
/// registered file whose content (or sidecar) changed, and
/// [`ReloadEvent::Removed`] for a registered file that is gone.
pub struct HotReloader {
    watch: Mutex<DirWatch>,
    /// Plugin name ↔ file, and the version registered or last reported.
    registered: Mutex<HashMap<String, PathBuf>>,
    reported: Mutex<HashMap<PathBuf, Fingerprint>>,
    last_check: Mutex<Option<Instant>>,
    check_interval: Duration,
}

impl HotReloader {
    /// Create a new hot reloader. A directory that does not exist (yet) is
    /// watched until it appears.
    pub fn new(watch_dir: &Path) -> Result<Self, PluginError> {
        if watch_dir.exists() && !watch_dir.is_dir() {
            return Err(PluginError::LoadError(format!(
                "Watch path is not a directory: {}",
                watch_dir.display()
            )));
        }
        Ok(Self {
            watch: Mutex::new(DirWatch::new(watch_dir, None, DEFAULT_DEBOUNCE)),
            registered: Mutex::new(HashMap::new()),
            reported: Mutex::new(HashMap::new()),
            last_check: Mutex::new(None),
            check_interval: DEFAULT_CHECK_INTERVAL,
        })
    }

    /// Register a loaded plugin's file: its current version is the
    /// baseline later changes are reported against.
    pub fn register(&self, plugin_name: &str, path: &Path) {
        self.registered
            .lock()
            .insert(plugin_name.to_string(), path.to_path_buf());
        self.reported
            .lock()
            .insert(path.to_path_buf(), Fingerprint::of_plugin(path));
    }

    /// Unregister a plugin file
    pub fn unregister(&self, plugin_name: &str) {
        if let Some(path) = self.registered.lock().remove(plugin_name) {
            self.reported.lock().remove(&path);
        }
    }

    /// Check for file changes (at most once per check interval).
    pub fn check(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        let now = Instant::now();
        {
            let mut last = self.last_check.lock();
            if last.is_some_and(|t| now.saturating_duration_since(t) < self.check_interval) {
                return Ok(Vec::new());
            }
            *last = Some(now);
        }
        let scan = self.watch.lock().scan(now, false);
        if let Some(reason) = scan.unreadable {
            return Err(PluginError::RuntimeError(reason));
        }
        let names: HashMap<PathBuf, String> = self
            .registered
            .lock()
            .iter()
            .map(|(name, path)| (path.clone(), name.clone()))
            .collect();
        let mut reported = self.reported.lock();
        let mut events = Vec::new();
        for file in scan.files.into_iter().filter(|f| f.settled) {
            if reported.get(&file.path) == Some(&file.fingerprint) {
                continue;
            }
            events.push(match names.get(&file.path) {
                Some(name) => ReloadEvent::Modified(name.clone()),
                None => ReloadEvent::Added(file.path.clone()),
            });
            reported.insert(file.path, file.fingerprint);
        }
        for path in scan.removed {
            reported.remove(&path);
            if let Some(name) = names.get(&path) {
                events.push(ReloadEvent::Removed(name.clone()));
            }
        }
        Ok(events)
    }

    /// Set debounce duration
    pub fn set_debounce(&mut self, duration: Duration) {
        self.watch.get_mut().set_debounce(duration);
    }

    /// Set check interval
    pub fn set_check_interval(&mut self, interval: Duration) {
        self.check_interval = interval;
    }

    /// Get watch directory
    pub fn watch_dir(&self) -> PathBuf {
        self.watch.lock().dir().to_path_buf()
    }

    /// Get registered plugin count
    pub fn plugin_count(&self) -> usize {
        self.registered.lock().len()
    }
}

/// Reload event
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadEvent {
    /// Plugin file was modified
    Modified(String),

    /// Plugin file was removed
    Removed(String),

    /// New plugin file was added
    Added(PathBuf),
}

/// Reload error
#[derive(Debug, Clone)]
pub enum ReloadError {
    /// File system error
    FileSystemError(String),

    /// Plugin load error
    LoadError(String),

    /// Plugin unload error
    UnloadError(String),
}

impl std::fmt::Display for ReloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReloadError::FileSystemError(msg) => write!(f, "File system error: {}", msg),
            ReloadError::LoadError(msg) => write!(f, "Load error: {}", msg),
            ReloadError::UnloadError(msg) => write!(f, "Unload error: {}", msg),
        }
    }
}

impl std::error::Error for ReloadError {}

/// Background thread that runs [`HotReloader::check`] every `interval` and
/// hands non-empty event batches to a callback.
pub struct HotReloadWatcher {
    /// Reloader
    reloader: Arc<HotReloader>,

    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,

    /// Poll interval
    interval: Duration,
}

impl HotReloadWatcher {
    /// Create a new watcher polling every [`DEFAULT_CHECK_INTERVAL`].
    pub fn new(reloader: Arc<HotReloader>) -> Self {
        Self::with_interval(reloader, DEFAULT_CHECK_INTERVAL)
    }

    /// Create a new watcher polling every `interval`.
    pub fn with_interval(reloader: Arc<HotReloader>, interval: Duration) -> Self {
        Self {
            reloader,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            interval,
        }
    }

    /// Start watching (returns immediately, runs in background). Scan
    /// errors are logged and the watch continues.
    pub fn start<F>(&self, callback: F)
    where
        F: Fn(Vec<ReloadEvent>) + Send + 'static,
    {
        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let reloader = self.reloader.clone();
        let running = self.running.clone();
        let interval = self.interval;

        std::thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                match reloader.check() {
                    Ok(events) if !events.is_empty() => callback(events),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "plugin directory scan failed"),
                }
                std::thread::sleep(interval);
            }
        });
    }

    /// Stop watching
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const WASM: &[u8] = b"\x00asm\x01\x00\x00\x00";

    /// A file write that changes the fingerprint even within one
    /// modification-time tick: a different length.
    fn write(path: &Path, extra: usize) {
        let mut bytes = WASM.to_vec();
        bytes.resize(WASM.len() + extra, 0);
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn a_missing_directory_is_watched_not_refused() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("later");
        let reloader = HotReloader::new(&dir).unwrap();
        assert!(reloader.check().unwrap().is_empty());
        let file = base.path().join("file");
        fs::write(&file, b"x").unwrap();
        assert!(
            HotReloader::new(&file).is_err(),
            "a file is not a directory"
        );
    }

    #[test]
    fn changes_settle_before_they_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let mut watch = DirWatch::new(dir.path(), None, Duration::from_millis(500));
        let t0 = Instant::now();
        let a = dir.path().join("a.wasm");
        write(&a, 0);
        let scan = watch.scan(t0, false);
        assert_eq!(scan.files.len(), 1);
        assert!(!scan.files[0].settled, "just written");
        assert!(watch.scan(t0 + Duration::from_millis(600), false).files[0].settled);

        // A sidecar is part of the plugin's version.
        let before = watch.scan(t0 + Duration::from_millis(700), false).files[0]
            .fingerprint
            .clone();
        fs::write(dir.path().join("a.sig"), b"sig").unwrap();
        let after = watch.scan(t0 + Duration::from_millis(800), false);
        assert_ne!(after.files[0].fingerprint, before);
        assert!(!after.files[0].settled);

        // A removal settles too; `force` settles at once.
        fs::remove_file(&a).unwrap();
        let t1 = t0 + Duration::from_millis(900);
        assert!(watch.scan(t1, false).removed.is_empty());
        assert_eq!(watch.scan(t1, true).removed, [a]);
        assert!(watch.scan(t1, true).removed.is_empty(), "reported once");
    }

    /// Two writes of the same size within one timestamp tick are still
    /// two versions: the content digest tells them apart.
    #[test]
    fn a_same_size_rewrite_is_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.wasm");
        fs::write(&path, b"\x00asm\x01\x00\x00\x00AAAA").unwrap();
        let mut watch = DirWatch::new(dir.path(), None, Duration::ZERO);
        let first = watch.scan(Instant::now(), true).files[0]
            .fingerprint
            .clone();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let modified = file.metadata().unwrap().modified().unwrap();
        fs::write(&path, b"\x00asm\x01\x00\x00\x00BBBB").unwrap();
        // Pin the old modification time: only the content differs.
        file.set_modified(modified).unwrap();
        let second = watch.scan(Instant::now(), true).files[0]
            .fingerprint
            .clone();
        assert_ne!(first, second);
        assert_eq!(Fingerprint::of_plugin(&path), second);
    }

    #[test]
    fn only_plugin_files_are_watched() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "a.wasm",
            "b.tar.gz",
            "a.json",
            "notes.txt",
            ".c.wasm",
            "d.gz",
        ] {
            write(&dir.path().join(name), 0);
        }
        fs::create_dir(dir.path().join("e.wasm")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("a.wasm"), dir.path().join("f.wasm")).unwrap();
        let mut watch = DirWatch::new(dir.path(), None, Duration::ZERO);
        let names: Vec<_> = watch
            .scan(Instant::now(), true)
            .files
            .into_iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["a.wasm", "b.tar.gz"]);
    }

    #[test]
    fn the_trust_root_is_fingerprinted() {
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let spec = trust.path().to_string_lossy().into_owned();
        let mut watch = DirWatch::new(dir.path(), Some(&spec), Duration::ZERO);
        let first = watch.scan(Instant::now(), false).trust.unwrap();
        fs::write(trust.path().join("k.pub"), "key").unwrap();
        let second = watch.scan(Instant::now(), false).trust.unwrap();
        assert_ne!(first, second);
        fs::write(trust.path().join("ignored.txt"), "x").unwrap();
        assert_eq!(watch.scan(Instant::now(), false).trust.unwrap(), second);
    }

    #[test]
    fn hot_reloader_reports_each_settled_change_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut reloader = HotReloader::new(dir.path()).unwrap();
        reloader.set_debounce(Duration::ZERO);
        reloader.set_check_interval(Duration::ZERO);

        let a = dir.path().join("a.wasm");
        write(&a, 0);
        assert_eq!(reloader.check().unwrap(), [ReloadEvent::Added(a.clone())]);
        assert!(reloader.check().unwrap().is_empty(), "reported once");

        reloader.register("plugin-a", &a);
        assert_eq!(reloader.plugin_count(), 1);
        assert!(reloader.check().unwrap().is_empty());
        write(&a, 3);
        assert_eq!(
            reloader.check().unwrap(),
            [ReloadEvent::Modified("plugin-a".into())]
        );
        fs::remove_file(&a).unwrap();
        assert_eq!(
            reloader.check().unwrap(),
            [ReloadEvent::Removed("plugin-a".into())]
        );
        reloader.unregister("plugin-a");
        assert_eq!(reloader.plugin_count(), 0);
    }

    #[test]
    fn checks_are_rate_limited() {
        let dir = tempfile::tempdir().unwrap();
        let mut reloader = HotReloader::new(dir.path()).unwrap();
        reloader.set_debounce(Duration::ZERO);
        reloader.set_check_interval(Duration::from_secs(3600));
        assert!(reloader.check().unwrap().is_empty());
        write(&dir.path().join("a.wasm"), 0);
        assert!(reloader.check().unwrap().is_empty(), "within the interval");
    }

    #[test]
    fn test_reload_error_display() {
        let err = ReloadError::FileSystemError("test".to_string());
        assert!(err.to_string().contains("File system error"));

        let err = ReloadError::LoadError("test".to_string());
        assert!(err.to_string().contains("Load error"));
    }

    #[test]
    fn the_watcher_thread_delivers_events_and_stops() {
        let dir = tempfile::tempdir().unwrap();
        let mut reloader = HotReloader::new(dir.path()).unwrap();
        reloader.set_debounce(Duration::ZERO);
        reloader.set_check_interval(Duration::ZERO);
        let watcher =
            HotReloadWatcher::with_interval(Arc::new(reloader), Duration::from_millis(10));
        assert!(!watcher.is_running());
        let (tx, rx) = std::sync::mpsc::channel();
        watcher.start(move |events| {
            let _ = tx.send(events);
        });
        assert!(watcher.is_running());
        write(&dir.path().join("a.wasm"), 0);
        let events = rx.recv_timeout(Duration::from_secs(5)).expect("an event");
        assert!(
            matches!(events.as_slice(), [ReloadEvent::Added(_)]),
            "{events:?}"
        );
        watcher.stop();
        assert!(!watcher.is_running());
    }
}

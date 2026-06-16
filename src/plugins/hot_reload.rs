//! Hot Reload Support
//!
//! File watching and automatic plugin reloading.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;

use super::runtime::PluginError;

/// Hot reloader for plugins
pub struct HotReloader {
    /// Watch directory
    watch_dir: PathBuf,

    /// File modification times
    file_times: RwLock<HashMap<PathBuf, SystemTime>>,

    /// Plugin name to file mapping
    plugin_files: RwLock<HashMap<String, PathBuf>>,

    /// Debounce duration (ignore rapid changes)
    debounce: Duration,

    /// Last check time
    last_check: RwLock<Instant>,

    /// Minimum interval between checks
    check_interval: Duration,

    /// Pending events (debounced)
    pending_events: RwLock<HashMap<PathBuf, (ReloadEventType, Instant)>>,
}

impl HotReloader {
    /// Create a new hot reloader
    pub fn new(watch_dir: &Path) -> Result<Self, PluginError> {
        if !watch_dir.exists() {
            return Err(PluginError::LoadError(format!(
                "Watch directory does not exist: {}",
                watch_dir.display()
            )));
        }

        Ok(Self {
            watch_dir: watch_dir.to_path_buf(),
            file_times: RwLock::new(HashMap::new()),
            plugin_files: RwLock::new(HashMap::new()),
            debounce: Duration::from_millis(500),
            last_check: RwLock::new(Instant::now()),
            check_interval: Duration::from_millis(100),
            pending_events: RwLock::new(HashMap::new()),
        })
    }

    /// Register a plugin file
    pub fn register(&self, plugin_name: &str, path: &Path) {
        let mut plugin_files = self.plugin_files.write();
        plugin_files.insert(plugin_name.to_string(), path.to_path_buf());

        // Record initial modification time
        if let Ok(metadata) = std::fs::metadata(path) {
            if let Ok(modified) = metadata.modified() {
                let mut file_times = self.file_times.write();
                file_times.insert(path.to_path_buf(), modified);
            }
        }
    }

    /// Unregister a plugin file
    pub fn unregister(&self, plugin_name: &str) {
        let mut plugin_files = self.plugin_files.write();
        if let Some(path) = plugin_files.remove(plugin_name) {
            let mut file_times = self.file_times.write();
            file_times.remove(&path);
        }
    }

    /// Check for file changes
    pub fn check(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        let now = Instant::now();

        // Rate limit checks
        {
            let last = *self.last_check.read();
            if now.duration_since(last) < self.check_interval {
                return Ok(Vec::new());
            }
            *self.last_check.write() = now;
        }

        let mut events = Vec::new();

        // Scan watch directory for new/removed files
        events.extend(self.scan_directory()?);

        // Check registered files for modifications
        events.extend(self.check_modifications()?);

        // Process pending events (apply debouncing)
        events.extend(self.process_pending_events(now)?);

        Ok(events)
    }

    /// Scan directory for new/removed files
    fn scan_directory(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        let mut events = Vec::new();

        if !self.watch_dir.exists() {
            return Ok(events);
        }

        let entries = std::fs::read_dir(&self.watch_dir)
            .map_err(|e| PluginError::RuntimeError(e.to_string()))?;

        let mut current_files: HashMap<PathBuf, SystemTime> = HashMap::new();

        for entry in entries.flatten() {
            let path = entry.path();

            // Only watch .wasm files
            if path.extension().map(|e| e != "wasm").unwrap_or(true) {
                continue;
            }

            if let Ok(metadata) = std::fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    current_files.insert(path, modified);
                }
            }
        }

        // Check for new files
        let file_times = self.file_times.read();
        for path in current_files.keys() {
            if !file_times.contains_key(path) {
                // New file detected - add to pending
                self.add_pending_event(path.clone(), ReloadEventType::Added);
            }
        }

        // Check for removed files
        for path in file_times.keys() {
            if path.starts_with(&self.watch_dir) && !current_files.contains_key(path) {
                // File removed
                if let Some(name) = self.get_plugin_name(path) {
                    events.push(ReloadEvent::Removed(name));
                }
            }
        }

        Ok(events)
    }

    /// Check registered files for modifications
    fn check_modifications(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        let plugin_files = self.plugin_files.read();
        let file_times = self.file_times.read();

        for (_plugin_name, path) in plugin_files.iter() {
            if let Ok(metadata) = std::fs::metadata(path) {
                if let Ok(modified) = metadata.modified() {
                    if let Some(old_time) = file_times.get(path) {
                        if modified > *old_time {
                            // File modified - add to pending
                            self.add_pending_event(path.clone(), ReloadEventType::Modified);
                        }
                    }
                }
            }
        }

        Ok(Vec::new())
    }

    /// Add a pending event (for debouncing)
    fn add_pending_event(&self, path: PathBuf, event_type: ReloadEventType) {
        let mut pending = self.pending_events.write();
        pending.insert(path, (event_type, Instant::now()));
    }

    /// Process pending events after debounce period
    fn process_pending_events(&self, now: Instant) -> Result<Vec<ReloadEvent>, PluginError> {
        let mut events = Vec::new();
        let mut to_remove = Vec::new();

        {
            let pending = self.pending_events.read();
            for (path, (event_type, timestamp)) in pending.iter() {
                if now.duration_since(*timestamp) >= self.debounce {
                    match event_type {
                        ReloadEventType::Modified => {
                            if let Some(name) = self.get_plugin_name(path) {
                                events.push(ReloadEvent::Modified(name));
                            }
                        }
                        ReloadEventType::Added => {
                            events.push(ReloadEvent::Added(path.clone()));
                        }
                        ReloadEventType::Removed => {
                            if let Some(name) = self.get_plugin_name(path) {
                                events.push(ReloadEvent::Removed(name));
                            }
                        }
                    }
                    to_remove.push(path.clone());
                }
            }
        }

        // Remove processed events and update file times
        {
            let mut pending = self.pending_events.write();
            let mut file_times = self.file_times.write();

            for path in to_remove {
                pending.remove(&path);

                // Update file time
                if let Ok(metadata) = std::fs::metadata(&path) {
                    if let Ok(modified) = metadata.modified() {
                        file_times.insert(path, modified);
                    }
                }
            }
        }

        Ok(events)
    }

    /// Get plugin name for a path
    fn get_plugin_name(&self, path: &Path) -> Option<String> {
        let plugin_files = self.plugin_files.read();
        for (name, p) in plugin_files.iter() {
            if p == path {
                return Some(name.clone());
            }
        }

        // Fall back to filename
        path.file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
    }

    /// Set debounce duration
    pub fn set_debounce(&mut self, duration: Duration) {
        self.debounce = duration;
    }

    /// Set check interval
    pub fn set_check_interval(&mut self, interval: Duration) {
        self.check_interval = interval;
    }

    /// Get watch directory
    pub fn watch_dir(&self) -> &Path {
        &self.watch_dir
    }

    /// Get registered plugin count
    pub fn plugin_count(&self) -> usize {
        self.plugin_files.read().len()
    }
}

/// Reload event type (internal)
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReloadEventType {
    Modified,
    Added,
    #[allow(dead_code)]
    Removed,
}

/// Reload event
#[derive(Debug, Clone)]
pub enum ReloadEvent {
    /// Plugin file was modified
    Modified(String),

    /// Plugin file was removed
    #[allow(dead_code)]
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

/// Hot reload watcher (for async watching)
pub struct HotReloadWatcher {
    /// Reloader
    reloader: Arc<HotReloader>,

    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl HotReloadWatcher {
    /// Create a new watcher
    pub fn new(reloader: Arc<HotReloader>) -> Self {
        Self {
            reloader,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Start watching (returns immediately, runs in background)
    pub fn start<F>(&self, callback: F)
    where
        F: Fn(Vec<ReloadEvent>) + Send + 'static,
    {
        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let reloader = self.reloader.clone();
        let running = self.running.clone();

        std::thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                if let Ok(events) = reloader.check() {
                    if !events.is_empty() {
                        callback(events);
                    }
                }

                std::thread::sleep(Duration::from_millis(100));
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

    #[test]
    fn test_hot_reloader_new() {
        let temp_dir = std::env::temp_dir().join("hot_reload_test");
        fs::create_dir_all(&temp_dir).unwrap();

        let reloader = HotReloader::new(&temp_dir);
        assert!(reloader.is_ok());

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_hot_reloader_nonexistent_dir() {
        let path = PathBuf::from("/nonexistent/path/to/plugins");
        let reloader = HotReloader::new(&path);
        assert!(reloader.is_err());
    }

    #[test]
    fn test_hot_reloader_register() {
        let temp_dir = std::env::temp_dir().join("hot_reload_register_test");
        fs::create_dir_all(&temp_dir).unwrap();

        let reloader = HotReloader::new(&temp_dir).unwrap();

        let plugin_path = temp_dir.join("test-plugin.wasm");
        fs::write(&plugin_path, b"\x00asm\x01\x00\x00\x00").unwrap();

        reloader.register("test-plugin", &plugin_path);
        assert_eq!(reloader.plugin_count(), 1);

        reloader.unregister("test-plugin");
        assert_eq!(reloader.plugin_count(), 0);

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_reload_event() {
        let event = ReloadEvent::Modified("test".to_string());
        assert!(matches!(event, ReloadEvent::Modified(_)));

        let event = ReloadEvent::Added(PathBuf::from("/test.wasm"));
        assert!(matches!(event, ReloadEvent::Added(_)));

        let event = ReloadEvent::Removed("test".to_string());
        assert!(matches!(event, ReloadEvent::Removed(_)));
    }

    #[test]
    fn test_reload_error_display() {
        let err = ReloadError::FileSystemError("test".to_string());
        assert!(err.to_string().contains("File system error"));

        let err = ReloadError::LoadError("test".to_string());
        assert!(err.to_string().contains("Load error"));
    }

    #[test]
    fn test_hot_reloader_check() {
        let temp_dir = std::env::temp_dir().join("hot_reload_check_test");
        fs::create_dir_all(&temp_dir).unwrap();

        let reloader = HotReloader::new(&temp_dir).unwrap();

        // Initial check should return empty
        let events = reloader.check().unwrap();
        assert!(events.is_empty());

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_hot_reload_watcher() {
        let temp_dir = std::env::temp_dir().join("hot_reload_watcher_test");
        fs::create_dir_all(&temp_dir).unwrap();

        let reloader = Arc::new(HotReloader::new(&temp_dir).unwrap());
        let watcher = HotReloadWatcher::new(reloader);

        assert!(!watcher.is_running());

        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_debounce_setting() {
        let temp_dir = std::env::temp_dir().join("hot_reload_debounce_test");
        fs::create_dir_all(&temp_dir).unwrap();

        let mut reloader = HotReloader::new(&temp_dir).unwrap();
        reloader.set_debounce(Duration::from_secs(1));
        reloader.set_check_interval(Duration::from_millis(50));

        assert_eq!(reloader.debounce, Duration::from_secs(1));
        assert_eq!(reloader.check_interval, Duration::from_millis(50));

        fs::remove_dir_all(&temp_dir).ok();
    }
}

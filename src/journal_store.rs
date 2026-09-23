//! Durable, segmented on-disk store for committed transactions (TR-07).
//!
//! Every transaction the journal commits is appended, in commit order, to the
//! current segment file `journal-<first-commit-seq>.log` under
//! `[journal] dir`. A segment is closed once it reaches `segment_bytes`; the
//! oldest whole segments are deleted once the directory exceeds
//! `retain_bytes`. At startup the segments are read back (a torn or corrupt
//! tail is truncated at the last intact record) and the newest transactions
//! are reloaded into the journal's committed store, with commit numbering
//! continuing after the highest recovered sequence.
//!
//! Records are written by one dedicated writer thread fed through a bounded
//! queue, so the data path never blocks on disk: when the queue is full the
//! record is dropped and counted (`journal_dropped_total`,
//! `coverage.dropped_transactions`) instead of stalling a client. The fsync
//! policy (`commit` / `interval` / `none`) decides when appended records
//! reach stable storage.
//!
//! Record layout: `MAGIC(4) | len(4, BE) | crc32(4, BE) | postcard(payload)`,
//! MAGIC = `HJ02`. `HJ01` (bincode 1 payloads) was only ever written by
//! pre-release builds of 1.9.0; a segment that starts with it is refused at
//! startup with an explicit error instead of being truncated as corrupt.
//! Boundary: the journal is written **after** the backend reported the commit
//! (the proxy is not a participant in the backend's commit), so a crash
//! between the two loses that record; the store is a faithful log of what
//! this proxy observed, not a WAL that the backend waits on (D-02).

use crate::config::{JournalFsync, JournalToml};
use crate::transaction_journal::{JournalSink, TransactionJournalEntry};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAGIC: [u8; 4] = *b"HJ02";
/// Record magic of the pre-release bincode format (never shipped in a release).
const LEGACY_MAGIC: [u8; 4] = *b"HJ01";
const HEADER: usize = 12;
const SEGMENT_PREFIX: &str = "journal-";
const SEGMENT_SUFFIX: &str = ".log";

/// CRC-32 (IEEE 802.3), table-driven; no extra dependency.
fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Store settings (from `[journal]`).
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub dir: PathBuf,
    pub segment_bytes: u64,
    pub retain_bytes: u64,
    pub fsync: JournalFsync,
    pub fsync_interval: Duration,
    pub queue: usize,
}

impl StoreConfig {
    /// Settings from a `[journal]` section with a configured `dir`.
    pub fn from_toml(j: &JournalToml) -> Option<Self> {
        let dir = j.dir()?;
        Some(Self {
            dir: PathBuf::from(dir),
            segment_bytes: j.segment_bytes.max(1),
            retain_bytes: j.retain_bytes.max(1),
            fsync: j.fsync_policy(),
            fsync_interval: Duration::from_millis(j.fsync_interval_ms.max(1)),
            queue: j.writer_queue.max(1),
        })
    }
}

/// What startup recovery found.
#[derive(Debug, Default)]
pub struct Recovered {
    /// The newest transactions, oldest first, bounded by the caller's caps.
    pub transactions: Vec<TransactionJournalEntry>,
    /// Records read across all segments (including ones not retained).
    pub records_read: u64,
    /// Highest commit sequence seen (`0` = none).
    pub commit_seq_high: u64,
    /// Bytes truncated from a torn/corrupt segment tail.
    pub truncated_bytes: u64,
    /// Segments found.
    pub segments: usize,
}

enum Msg {
    Record(Arc<TransactionJournalEntry>),
    Flush(mpsc::Sender<()>),
}

/// The `JournalSink` half: hands records to the writer thread.
pub struct SegmentSink {
    tx: SyncSender<Msg>,
    dropped: AtomicU64,
    healthy: Arc<AtomicBool>,
    dir: PathBuf,
}

impl SegmentSink {
    /// Wait until every record accepted so far is written (and fsync'ed under
    /// the `commit`/`interval` policies). Used by tests and shutdown.
    pub fn flush(&self) {
        let (tx, rx) = mpsc::channel();
        if self.tx.send(Msg::Flush(tx)).is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(30));
        }
    }

    /// Records the queue refused (full or writer gone).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Directory the segments live in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl JournalSink for SegmentSink {
    fn append(&self, tx: &Arc<TransactionJournalEntry>) -> bool {
        match self.tx.try_send(Msg::Record(tx.clone())) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    fn durable(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}

/// The segmented store: recovery at open, then a writer thread.
pub struct SegmentStore {
    cfg: StoreConfig,
}

fn segment_path(dir: &Path, first_seq: u64) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{first_seq:020}{SEGMENT_SUFFIX}"))
}

/// Sorted `(first_seq, path, len)` of the segments in `dir`.
fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf, u64)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(stem) = name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|s| s.strip_suffix(SEGMENT_SUFFIX))
        else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        let len = entry.metadata()?.len();
        out.push((seq, entry.path(), len));
    }
    out.sort_by_key(|(seq, _, _)| *seq);
    Ok(out)
}

/// Read one record at the reader's position. `Ok(None)` at a clean end,
/// `Err` on a torn or corrupt record.
fn read_record(r: &mut BufReader<File>) -> io::Result<Option<TransactionJournalEntry>> {
    let mut header = [0u8; HEADER];
    match r.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    if header[..4] == LEGACY_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "segment written by a pre-release build (record format HJ01, bincode); \
             this version reads HJ02 only: move the journal directory aside to start fresh",
        ));
    }
    if header[..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad record magic",
        ));
    }
    let len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let crc = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    if len == 0 || len > 256 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("implausible record length {len}"),
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)
        .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))?;
    if crc32(&payload) != crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record crc mismatch",
        ));
    }
    postcard::from_bytes::<TransactionJournalEntry>(&payload)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

impl SegmentStore {
    /// Open (creating the directory) and recover the newest transactions,
    /// bounded by `max_txs` / `max_bytes` (the journal's committed caps).
    pub fn open(
        cfg: StoreConfig,
        max_txs: usize,
        max_bytes: usize,
    ) -> io::Result<(Self, Recovered)> {
        fs::create_dir_all(&cfg.dir)?;
        let mut rec = Recovered::default();
        let segments = list_segments(&cfg.dir)?;
        rec.segments = segments.len();
        let mut ring: VecDeque<TransactionJournalEntry> = VecDeque::new();
        let mut ring_bytes = 0usize;
        let last_idx = segments.len().saturating_sub(1);
        for (i, (_, path, _)) in segments.iter().enumerate() {
            let file = File::open(path)?;
            let mut reader = BufReader::new(file);
            let mut good_end: u64 = 0;
            loop {
                match read_record(&mut reader) {
                    Ok(Some(tx)) => {
                        good_end = reader.stream_position()?;
                        rec.records_read += 1;
                        if let Some(seq) = tx.commit_seq {
                            rec.commit_seq_high = rec.commit_seq_high.max(seq);
                        }
                        ring_bytes += tx.total_size();
                        ring.push_back(tx);
                        while ring.len() > max_txs.max(1)
                            || (ring_bytes > max_bytes && ring.len() > 1)
                        {
                            if let Some(old) = ring.pop_front() {
                                ring_bytes = ring_bytes.saturating_sub(old.total_size());
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) if e.kind() == io::ErrorKind::Unsupported => {
                        // A format this build cannot read is not corruption:
                        // refuse to start rather than truncate someone's log.
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            format!("{}: {}", path.display(), e),
                        ));
                    }
                    Err(e) => {
                        let total = fs::metadata(path)?.len();
                        let torn = total.saturating_sub(good_end);
                        tracing::warn!(
                            segment = %path.display(),
                            offset = good_end,
                            bytes = torn,
                            error = %e,
                            "journal segment ends in a torn/corrupt record; {}",
                            if i == last_idx { "truncating" } else { "ignoring the tail" }
                        );
                        rec.truncated_bytes += torn;
                        if i == last_idx {
                            let f = OpenOptions::new().write(true).open(path)?;
                            f.set_len(good_end)?;
                            f.sync_all()?;
                        }
                        break;
                    }
                }
            }
        }
        rec.transactions = ring.into_iter().collect();
        Ok((Self { cfg }, rec))
    }

    /// Start the writer thread and return the sink to attach to the journal.
    pub fn spawn_writer(self) -> io::Result<Arc<SegmentSink>> {
        let (tx, rx) = mpsc::sync_channel::<Msg>(self.cfg.queue);
        let healthy = Arc::new(AtomicBool::new(true));
        let sink = Arc::new(SegmentSink {
            tx,
            dropped: AtomicU64::new(0),
            healthy: healthy.clone(),
            dir: self.cfg.dir.clone(),
        });
        let cfg = self.cfg;
        let mut writer = Writer::open(cfg.clone())?;
        std::thread::Builder::new()
            .name("helios-journal-writer".into())
            .spawn(move || {
                let mut dirty = false;
                let mut last_sync = Instant::now();
                loop {
                    let wait = match cfg.fsync {
                        JournalFsync::Interval => cfg.fsync_interval,
                        _ => Duration::from_secs(1),
                    };
                    match rx.recv_timeout(wait) {
                        Ok(Msg::Record(tx)) => match writer.append(&tx) {
                            Ok(()) => {
                                dirty = true;
                                if cfg.fsync == JournalFsync::Commit {
                                    if let Err(e) = writer.sync() {
                                        tracing::error!(error = %e, "journal fsync failed");
                                        healthy.store(false, Ordering::Relaxed);
                                    }
                                    dirty = false;
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "journal append failed");
                                healthy.store(false, Ordering::Relaxed);
                            }
                        },
                        Ok(Msg::Flush(done)) => {
                            if let Err(e) = writer.flush_all() {
                                tracing::error!(error = %e, "journal flush failed");
                                healthy.store(false, Ordering::Relaxed);
                            }
                            dirty = false;
                            last_sync = Instant::now();
                            let _ = done.send(());
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            let _ = writer.flush_all();
                            return;
                        }
                    }
                    if dirty
                        && cfg.fsync == JournalFsync::Interval
                        && last_sync.elapsed() >= cfg.fsync_interval
                    {
                        if let Err(e) = writer.sync() {
                            tracing::error!(error = %e, "journal fsync failed");
                            healthy.store(false, Ordering::Relaxed);
                        }
                        dirty = false;
                        last_sync = Instant::now();
                    }
                }
            })?;
        Ok(sink)
    }
}

/// The writer thread's file state.
struct Writer {
    cfg: StoreConfig,
    file: File,
    path: PathBuf,
    len: u64,
}

impl Writer {
    fn open(cfg: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(&cfg.dir)?;
        let segments = list_segments(&cfg.dir)?;
        // Continue the newest segment when it has room; else start a new one
        // named after the next sequence (unknown here: use high+1 of the
        // file names, refined on the first append).
        if let Some((_, path, len)) = segments.last() {
            if *len < cfg.segment_bytes {
                let file = OpenOptions::new().append(true).open(path)?;
                return Ok(Self {
                    cfg,
                    file,
                    path: path.clone(),
                    len: *len,
                });
            }
        }
        let next = segments.last().map(|(s, _, _)| s + 1).unwrap_or(1);
        let path = segment_path(&cfg.dir, next);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            cfg,
            file,
            path,
            len: 0,
        })
    }

    fn rotate(&mut self, first_seq: u64) -> io::Result<()> {
        self.file.sync_all()?;
        let path = segment_path(&self.cfg.dir, first_seq);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        self.file = file;
        self.path = path;
        self.len = 0;
        self.enforce_retention()
    }

    fn enforce_retention(&self) -> io::Result<()> {
        let mut segments = list_segments(&self.cfg.dir)?;
        let mut total: u64 = segments.iter().map(|(_, _, l)| *l).sum();
        while total > self.cfg.retain_bytes && segments.len() > 1 {
            let (_, path, len) = segments.remove(0);
            if path == self.path {
                break;
            }
            match fs::remove_file(&path) {
                Ok(()) => {
                    tracing::info!(segment = %path.display(), "journal segment retired (retain_bytes)");
                    total = total.saturating_sub(len);
                }
                Err(e) => {
                    tracing::warn!(segment = %path.display(), error = %e, "journal segment retire failed");
                    break;
                }
            }
        }
        Ok(())
    }

    fn append(&mut self, tx: &TransactionJournalEntry) -> io::Result<()> {
        let payload = postcard::to_stdvec(tx)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        if self.len >= self.cfg.segment_bytes && self.len > 0 {
            self.rotate(tx.commit_seq.unwrap_or(0))?;
        } else if self.len == 0 {
            // A fresh segment carries the first record's sequence in its name.
            let want = segment_path(&self.cfg.dir, tx.commit_seq.unwrap_or(0));
            if want != self.path && !want.exists() && fs::rename(&self.path, &want).is_ok() {
                self.path = want;
            }
        }
        let mut buf = Vec::with_capacity(HEADER + payload.len());
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&crc32(&payload).to_be_bytes());
        buf.extend_from_slice(&payload);
        self.file.write_all(&buf)?;
        self.len += buf.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn flush_all(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if self.cfg.fsync != JournalFsync::None {
            self.file.sync_data()?;
        }
        Ok(())
    }
}

/// Truncate a file to `len` (test helper for torn-tail scenarios).
#[cfg(test)]
fn truncate_file(path: &Path, len: u64) -> io::Result<()> {
    let mut f = OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    f.seek(std::io::SeekFrom::End(0)).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_journal::{
        JournalValue, NewEntry, SourceIdentity, StatementOutcome, TransactionJournal,
        TransactionJournalEntry, WireProtocol,
    };
    use crate::NodeId;
    use uuid::Uuid;

    fn cfg(dir: &Path) -> StoreConfig {
        StoreConfig {
            dir: dir.to_path_buf(),
            segment_bytes: 64 * 1024 * 1024,
            retain_bytes: 1024 * 1024 * 1024,
            fsync: JournalFsync::None,
            fsync_interval: Duration::from_millis(10),
            queue: 64,
        }
    }

    fn tx(seq: u64, sql: &str) -> TransactionJournalEntry {
        let mut t = TransactionJournalEntry::new(Uuid::new_v4(), Uuid::new_v4(), NodeId::new(), 0)
            .with_source(SourceIdentity {
                client_addr: "10.0.0.1:1".into(),
                user: "u".into(),
                database: "d".into(),
                backend: "b:5432".into(),
                tenant: Some("t".into()),
            });
        t.add_entry(
            NewEntry {
                statement: sql.to_string(),
                parameters: vec![
                    JournalValue::Text("x".into()),
                    JournalValue::Binary(vec![0, 1, 2]),
                    JournalValue::Null,
                ],
                param_types: vec![25, 17, 0],
                result_checksum: None,
                rows_affected: Some(1),
                duration_ms: 3,
                outcome: StatementOutcome::Succeeded {
                    tag: "INSERT 0 1".into(),
                },
                protocol: WireProtocol::Extended,
            }
            .into_journal_entry(1),
        );
        t.commit_seq = Some(seq);
        t.committed_at = Some(chrono::Utc::now());
        t.commit_tag = Some("COMMIT".into());
        t
    }

    /// Every shape a journaled transaction can take survives the segment codec
    /// unchanged (postcard is not self-describing, so a serde attribute that
    /// needs `deserialize_any` would fail here, not in production).
    #[test]
    fn every_journal_shape_round_trips_through_the_codec() {
        let mut t = tx(7, "update t set a = $1 where b = any($2)");
        t.add_entry(
            NewEntry {
                statement: "insert into u values ($1, $2, $3, $4, $5, $6)".into(),
                parameters: vec![
                    JournalValue::Bool(true),
                    JournalValue::Int64(i64::MIN),
                    JournalValue::Float64(-1.5e300),
                    JournalValue::Bytes(vec![0xde, 0xad]),
                    JournalValue::Array(vec![
                        JournalValue::Int64(1),
                        JournalValue::Array(vec![
                            JournalValue::Null,
                            JournalValue::Text("é".into()),
                        ]),
                    ]),
                    JournalValue::TextRaw(vec![0xff, 0xfe]),
                ],
                param_types: vec![16, 20, 701, 17, 1016, 25],
                result_checksum: Some(u64::MAX),
                rows_affected: None,
                duration_ms: 0,
                outcome: StatementOutcome::Failed(Box::new(
                    crate::transaction_journal::StatementFailure {
                        sqlstate: "23505".into(),
                        message: "duplicate key".into(),
                    },
                )),
                protocol: WireProtocol::Simple,
            }
            .into_journal_entry(2),
        );
        t.add_entry(
            NewEntry {
                statement: "select 1".into(),
                parameters: Vec::new(),
                param_types: Vec::new(),
                result_checksum: None,
                rows_affected: None,
                duration_ms: 1,
                outcome: StatementOutcome::Unobserved,
                protocol: WireProtocol::Extended,
            }
            .into_journal_entry(3),
        );
        t.create_savepoint("s1".into());
        t.source.tenant = None;
        t.commit_tag = Some(std::borrow::Cow::Owned("COMMIT PREPARED".into()));
        t.mark_incomplete("COPY FROM STDIN");

        let bytes = postcard::to_stdvec(&t).unwrap();
        let back: TransactionJournalEntry = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(&t).unwrap(),
            serde_json::to_value(&back).unwrap()
        );
    }

    /// A segment in the pre-release bincode format (`HJ01`) is refused with
    /// an explicit error and left untouched, never truncated as corrupt.
    #[test]
    fn legacy_format_segment_is_refused_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 1);
        let payload = b"not a postcard record";
        let mut rec = Vec::new();
        rec.extend_from_slice(&LEGACY_MAGIC);
        rec.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        rec.extend_from_slice(&crc32(payload).to_be_bytes());
        rec.extend_from_slice(payload);
        fs::write(&path, &rec).unwrap();

        let e = match SegmentStore::open(cfg(dir.path()), 1000, usize::MAX) {
            Ok(_) => panic!("a legacy segment must not open"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);
        let msg = e.to_string();
        assert!(
            msg.contains("HJ01") && msg.contains("move the journal directory aside"),
            "{msg}"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            rec,
            "the legacy segment must be left intact"
        );
    }

    #[test]
    fn crc32_matches_reference_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn records_round_trip_through_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        let (store, rec) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        assert_eq!(rec.records_read, 0);
        let sink = store.spawn_writer().unwrap();
        for i in 1..=5 {
            assert!(sink.append(&Arc::new(tx(i, &format!("insert {i}")))));
        }
        sink.flush();
        assert!(sink.durable());
        drop(sink);

        let (_, rec) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        assert_eq!(rec.records_read, 5);
        assert_eq!(rec.commit_seq_high, 5);
        assert_eq!(rec.segments, 1);
        assert_eq!(rec.truncated_bytes, 0);
        let seqs: Vec<u64> = rec
            .transactions
            .iter()
            .map(|t| t.commit_seq.unwrap())
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        let t = &rec.transactions[2];
        assert_eq!(t.entries[0].statement, "insert 3");
        assert_eq!(
            t.entries[0].parameters[1],
            JournalValue::Binary(vec![0, 1, 2])
        );
        assert_eq!(t.entries[0].param_types, vec![25, 17, 0]);
        assert_eq!(t.source.tenant.as_deref(), Some("t"));
        assert_eq!(
            t.entries[0].outcome,
            StatementOutcome::Succeeded {
                tag: "INSERT 0 1".into()
            }
        );
        assert_eq!(t.entries[0].protocol, WireProtocol::Extended);

        // Reload into a journal: numbering continues after the recovered high.
        let journal = TransactionJournal::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(journal.load_committed(rec.transactions));
        assert_eq!(journal.commit_seq_high(), 5);
        let after = rt.block_on(journal.committed_after(3));
        assert_eq!(after.len(), 2);
    }

    #[test]
    fn recovery_keeps_only_the_newest_within_caps() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        let sink = store.spawn_writer().unwrap();
        for i in 1..=10 {
            sink.append(&Arc::new(tx(i, "insert")));
        }
        sink.flush();
        drop(sink);
        let (_, rec) = SegmentStore::open(cfg(dir.path()), 3, usize::MAX).unwrap();
        assert_eq!(rec.records_read, 10);
        let seqs: Vec<u64> = rec
            .transactions
            .iter()
            .map(|t| t.commit_seq.unwrap())
            .collect();
        assert_eq!(seqs, vec![8, 9, 10]);
        assert_eq!(rec.commit_seq_high, 10);
    }

    #[test]
    fn torn_tail_is_truncated_and_earlier_records_survive() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        let sink = store.spawn_writer().unwrap();
        for i in 1..=3 {
            sink.append(&Arc::new(tx(i, "insert")));
        }
        sink.flush();
        drop(sink);
        let (_, path, len) = list_segments(dir.path()).unwrap().pop().unwrap();
        // Chop the last record in half.
        truncate_file(&path, len - 7).unwrap();
        let (_, rec) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        assert_eq!(rec.records_read, 2);
        assert!(rec.truncated_bytes > 0);
        let fixed = fs::metadata(&path).unwrap().len();
        assert!(fixed < len - 7, "torn record removed");
        // Appending after truncation continues cleanly.
        let (store, _) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        let sink = store.spawn_writer().unwrap();
        sink.append(&Arc::new(tx(3, "insert again")));
        sink.flush();
        drop(sink);
        let (_, rec) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        assert_eq!(rec.records_read, 3);
        assert_eq!(rec.truncated_bytes, 0);

        // A corrupted byte in the middle: crc mismatch, tail ignored.
        let (_, path, _) = list_segments(dir.path()).unwrap().pop().unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let mid = HEADER + 5;
        bytes[mid] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let (_, rec) = SegmentStore::open(cfg(dir.path()), 1000, usize::MAX).unwrap();
        assert_eq!(
            rec.records_read, 0,
            "first record corrupt → nothing after it is trusted"
        );
    }

    #[test]
    fn segments_rotate_and_retention_retires_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg(dir.path());
        c.segment_bytes = 400; // a few records per segment
        c.retain_bytes = 1200;
        let (store, _) = SegmentStore::open(c.clone(), 1000, usize::MAX).unwrap();
        let sink = store.spawn_writer().unwrap();
        for i in 1..=40 {
            sink.append(&Arc::new(tx(i, "insert into t values (1)")));
        }
        sink.flush();
        drop(sink);
        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.len() > 1, "rotation happened: {:?}", segments);
        let total: u64 = segments.iter().map(|(_, _, l)| *l).sum();
        assert!(
            total <= c.retain_bytes + 2 * c.segment_bytes,
            "retention bounded: {total}"
        );
        let (_, rec) = SegmentStore::open(c, 1000, usize::MAX).unwrap();
        assert_eq!(rec.commit_seq_high, 40);
        let first = rec.transactions.first().unwrap().commit_seq.unwrap();
        assert!(
            first > 1,
            "oldest segments were retired; first kept seq = {first}"
        );
        let seqs: Vec<u64> = rec
            .transactions
            .iter()
            .map(|t| t.commit_seq.unwrap())
            .collect();
        assert!(
            seqs.windows(2).all(|w| w[0] < w[1]),
            "commit order preserved"
        );
    }

    #[test]
    fn full_queue_drops_and_counts_instead_of_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg(dir.path());
        c.queue = 1;
        // Never spawn the writer: the queue holds one record, the next drops.
        let (tx_ch, _rx) = mpsc::sync_channel::<Msg>(1);
        let sink = SegmentSink {
            tx: tx_ch,
            dropped: AtomicU64::new(0),
            healthy: Arc::new(AtomicBool::new(true)),
            dir: c.dir,
        };
        assert!(sink.append(&Arc::new(tx(1, "a"))));
        assert!(!sink.append(&Arc::new(tx(2, "b"))));
        assert_eq!(sink.dropped(), 1);
    }
}

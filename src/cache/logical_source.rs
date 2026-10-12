//! Logical-decoding invalidation source (C-02 slice 2).
//!
//! The query cache's commit-aware invalidation sees only writes that pass
//! through this proxy. With `[cache] invalidation_source = "logical"` the
//! proxy also follows the database's own committed changes: it polls a
//! `pgoutput` logical replication slot with
//! `pg_logical_slot_get_binary_changes` over an ordinary connection (no
//! streaming replication protocol), maps each committed transaction's
//! relations to table names and moves their cache generations
//! ([`QueryCache::mark_written`]). Writes from other proxies, direct
//! connections, cron jobs and triggers are then invalidated within about one
//! `logical_feedback_ms`.
//!
//! Anything this source cannot account for invalidates the whole cache
//! ([`QueryCache::invalidate_all`]) rather than serve possibly stale results:
//! a relation it has no name for, a `TRUNCATE` it cannot resolve, a lost
//! connection or slot (changes may have been consumed and not applied), and a
//! slot lagging beyond `logical_max_slot_lag_bytes`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{
    tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
};

use super::QueryCache;

/// Connection settings parsed from a libpq-style `key=value` DSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDsn {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub dbname: Option<String>,
}

impl LogicalDsn {
    /// Parse `host=... port=... user=... password=... dbname=...`. Values may
    /// be single-quoted (`password='a b'`, `\'` and `\\` escapes). `host` and
    /// `user` are required; `port` defaults to 5432.
    pub fn parse(dsn: &str) -> Result<Self, String> {
        let mut fields: HashMap<String, String> = HashMap::new();
        let mut chars = dsn.chars().peekable();
        loop {
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            if chars.peek().is_none() {
                break;
            }
            let key: String =
                std::iter::from_fn(|| chars.next_if(|c| *c != '=' && !c.is_whitespace())).collect();
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            if chars.next() != Some('=') {
                return Err(format!("expected `=` after `{key}`"));
            }
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            let mut value = String::new();
            if chars.peek() == Some(&'\'') {
                chars.next();
                loop {
                    match chars.next() {
                        Some('\\') => match chars.next() {
                            Some(c) => value.push(c),
                            None => return Err("unterminated escape".into()),
                        },
                        Some('\'') => break,
                        Some(c) => value.push(c),
                        None => return Err(format!("unterminated quoted value for `{key}`")),
                    }
                }
            } else {
                value.extend(std::iter::from_fn(|| chars.next_if(|c| !c.is_whitespace())));
            }
            fields.insert(key, value);
        }
        let host = fields.remove("host").ok_or("missing host")?;
        let user = fields.remove("user").ok_or("missing user")?;
        let port = match fields.remove("port") {
            Some(p) => p.parse().map_err(|_| format!("bad port `{p}`"))?,
            None => 5432,
        };
        let dsn = Self {
            host,
            port,
            user,
            password: fields.remove("password"),
            dbname: fields.remove("dbname"),
        };
        if let Some(unknown) = fields.keys().next() {
            return Err(format!("unsupported key `{unknown}`"));
        }
        Ok(dsn)
    }
}

/// Whether `name` is a plain lower-case SQL identifier, safe to inline as a
/// string literal (slot and publication names).
pub fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
        && name.len() <= 63
}

/// What one committed transaction asks of the cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invalidation {
    /// These tables were written.
    Tables(Vec<String>),
    /// Something this source cannot name was written: invalidate everything.
    All,
}

/// Incremental `pgoutput` (protocol version 1) decoder: relation messages
/// name the oids, data messages collect them per transaction, and the commit
/// message releases the transaction's [`Invalidation`].
#[derive(Debug, Default)]
pub struct PgOutputDecoder {
    relations: HashMap<u32, String>,
    open: Option<OpenTx>,
}

#[derive(Debug, Default)]
struct OpenTx {
    tables: Vec<String>,
    unknown: bool,
}

impl PgOutputDecoder {
    /// Feed one message (one row of `pg_logical_slot_get_binary_changes`).
    /// Returns the invalidation of a transaction at its commit; a malformed
    /// message invalidates everything.
    pub fn feed(&mut self, msg: &[u8]) -> Option<Invalidation> {
        let Some((&tag, body)) = msg.split_first() else {
            return Some(Invalidation::All);
        };
        match tag {
            b'B' => {
                self.open = Some(OpenTx::default());
                None
            }
            b'R' => match parse_relation(body) {
                Some((oid, name)) => {
                    self.relations.insert(oid, name);
                    None
                }
                None => self.taint(),
            },
            b'I' | b'U' | b'D' => match read_u32(body, 0) {
                Some(oid) => {
                    self.touch(oid);
                    None
                }
                None => self.taint(),
            },
            b'T' => {
                // Int32 relation count, Int8 options, then Int32 oids.
                let Some(n) = read_u32(body, 0) else {
                    return self.taint();
                };
                for i in 0..n as usize {
                    match read_u32(body, 5 + 4 * i) {
                        Some(oid) => self.touch(oid),
                        None => return self.taint(),
                    }
                }
                None
            }
            b'C' => {
                let tx = self.open.take().unwrap_or_default();
                if tx.unknown {
                    Some(Invalidation::All)
                } else if tx.tables.is_empty() {
                    None
                } else {
                    Some(Invalidation::Tables(tx.tables))
                }
            }
            // Origin, Type, logical Message: no table data.
            _ => None,
        }
    }

    fn touch(&mut self, oid: u32) {
        let open = self.open.get_or_insert_with(OpenTx::default);
        match self.relations.get(&oid) {
            Some(name) if !open.tables.contains(name) => open.tables.push(name.clone()),
            Some(_) => {}
            None => open.unknown = true,
        }
    }

    fn taint(&mut self) -> Option<Invalidation> {
        match &mut self.open {
            Some(open) => {
                open.unknown = true;
                None
            }
            None => Some(Invalidation::All),
        }
    }
}

fn read_u32(b: &[u8], at: usize) -> Option<u32> {
    let s = b.get(at..at + 4)?;
    Some(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_cstr(b: &[u8], at: usize) -> Option<(&str, usize)> {
    let rest = b.get(at..)?;
    let end = rest.iter().position(|&c| c == 0)?;
    Some((std::str::from_utf8(&rest[..end]).ok()?, at + end + 1))
}

/// `R`: Int32 oid, String namespace, String relname, ... The cache names a
/// table by its lower-cased relation name without schema (the normalizer's
/// convention), so that is what this returns.
fn parse_relation(body: &[u8]) -> Option<(u32, String)> {
    let oid = read_u32(body, 0)?;
    let (_namespace, next) = read_cstr(body, 4)?;
    let (relname, _) = read_cstr(body, next)?;
    Some((oid, relname.to_lowercase()))
}

/// Decode PostgreSQL's text form of `bytea` (`\x0a1b...`).
pub fn decode_bytea_hex(text: &str) -> Option<Vec<u8>> {
    let hex = text.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Counters for `/metrics` (`heliosdb_proxy_cache_logical_*`).
#[derive(Debug, Default)]
pub struct LogicalStats {
    pub transactions_applied: AtomicU64,
    pub full_invalidations: AtomicU64,
    pub reconnects: AtomicU64,
    /// The slot's lag behind the primary's WAL position at the last poll.
    pub lag_bytes: AtomicU64,
    /// 1 while the source is attached and polling, 0 otherwise.
    pub connected: AtomicU64,
}

/// `[cache]` settings the source needs, resolved at startup.
#[derive(Debug, Clone)]
pub struct LogicalSettings {
    pub dsn: LogicalDsn,
    pub slot: String,
    pub publication: String,
    pub feedback: Duration,
    pub max_slot_lag_bytes: u64,
    pub max_changes_per_poll: u64,
    pub connect_timeout: Duration,
    /// Bound on one poll's queries (`[limits] backend_read_timeout_secs`).
    pub query_timeout: Duration,
}

/// The polling source: one dedicated connection, separate from the data path.
pub struct LogicalSource {
    settings: LogicalSettings,
    stats: Arc<LogicalStats>,
    client: Option<BackendClient>,
    decoder: PgOutputDecoder,
}

impl LogicalSource {
    pub fn new(settings: LogicalSettings, stats: Arc<LogicalStats>) -> Self {
        Self {
            settings,
            stats,
            client: None,
            decoder: PgOutputDecoder::default(),
        }
    }

    fn backend_config(&self) -> BackendConfig {
        let d = &self.settings.dsn;
        BackendConfig {
            host: d.host.clone(),
            port: d.port,
            user: d.user.clone(),
            password: d.password.clone(),
            database: d.dbname.clone(),
            application_name: Some("heliosdb-proxy-cache-logical".into()),
            tls_mode: TlsMode::Prefer,
            connect_timeout: self.settings.connect_timeout,
            query_timeout: self.settings.query_timeout,
            tls_config: default_client_config(),
        }
    }

    /// Connect, check the publication exists and create the slot if it does
    /// not. Errors name what is missing; the daemon refuses to start on one.
    pub async fn attach(&mut self) -> Result<(), String> {
        let mut client = BackendClient::connect(&self.backend_config())
            .await
            .map_err(|e| {
                format!(
                    "connect {}:{}: {e}",
                    self.settings.dsn.host, self.settings.dsn.port
                )
            })?;
        let (slot, publication) = (&self.settings.slot, &self.settings.publication);
        let exists = |r: &crate::backend::QueryResult| !r.rows.is_empty();
        let pubs = client
            .simple_query(&format!(
                "SELECT 1 FROM pg_publication WHERE pubname = '{publication}'"
            ))
            .await
            .map_err(|e| format!("query pg_publication: {e}"))?;
        if !exists(&pubs) {
            return Err(format!(
                "publication {publication} does not exist; create it on the primary, e.g. \
                 CREATE PUBLICATION {publication} FOR ALL TABLES"
            ));
        }
        let slots = client
            .simple_query(&format!(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = '{slot}'"
            ))
            .await
            .map_err(|e| format!("query pg_replication_slots: {e}"))?;
        match slots.rows.first().and_then(|r| r.first()) {
            Some(TextValue::Text(plugin)) if plugin == "pgoutput" => {}
            Some(other) => {
                return Err(format!(
                    "replication slot {slot} exists but is not a pgoutput slot ({other:?})"
                ))
            }
            None => {
                client
                    .simple_query(&format!(
                        "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
                    ))
                    .await
                    .map_err(|e| format!("create logical slot {slot}: {e}"))?;
                tracing::info!(%slot, "cache logical source: created replication slot");
            }
        }
        self.client = Some(client);
        // A fresh decoding session resends relation messages before data.
        self.decoder = PgOutputDecoder::default();
        self.stats.connected.store(1, Ordering::Relaxed);
        Ok(())
    }

    /// One poll: fetch the slot's pending committed changes (up to
    /// `max_changes_per_poll`, whole transactions), apply them, sample the
    /// slot lag. Returns whether the cap was reached (poll again at once).
    pub async fn poll_once(&mut self, cache: &QueryCache) -> Result<bool, String> {
        let s = &self.settings;
        let client = self.client.as_mut().ok_or("not attached")?;
        let changes = client
            .simple_query(&format!(
                "SELECT data FROM pg_logical_slot_get_binary_changes('{}', NULL, {}, \
                 'proto_version', '1', 'publication_names', '{}')",
                s.slot, s.max_changes_per_poll, s.publication
            ))
            .await
            .map_err(|e| format!("read slot {}: {e}", s.slot))?;
        let rows = changes.rows.len() as u64;
        for row in &changes.rows {
            let bytes = match row.first() {
                Some(TextValue::Text(t)) => decode_bytea_hex(t),
                _ => None,
            };
            let action = match bytes {
                Some(b) => self.decoder.feed(&b),
                None => Some(Invalidation::All),
            };
            match action {
                Some(Invalidation::Tables(tables)) => {
                    cache.mark_written(&tables);
                    self.stats
                        .transactions_applied
                        .fetch_add(1, Ordering::Relaxed);
                }
                Some(Invalidation::All) => {
                    cache.invalidate_all();
                    self.stats
                        .full_invalidations
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .transactions_applied
                        .fetch_add(1, Ordering::Relaxed);
                }
                None => {}
            }
        }
        let lag = client
            .simple_query(&format!(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint \
                 FROM pg_replication_slots WHERE slot_name = '{}'",
                s.slot
            ))
            .await
            .map_err(|e| format!("read slot lag: {e}"))?;
        match lag.rows.first().and_then(|r| r.first()) {
            Some(TextValue::Text(v)) => {
                let bytes = v.parse::<i64>().unwrap_or(0).max(0) as u64;
                self.stats.lag_bytes.store(bytes, Ordering::Relaxed);
                if bytes > s.max_slot_lag_bytes {
                    tracing::error!(
                        slot = %s.slot,
                        lag_bytes = bytes,
                        limit = s.max_slot_lag_bytes,
                        "cache logical source: slot lags beyond logical_max_slot_lag_bytes; \
                         invalidating the whole cache"
                    );
                    cache.invalidate_all();
                    self.stats
                        .full_invalidations
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => return Err(format!("replication slot {} disappeared", s.slot)),
        }
        Ok(rows >= s.max_changes_per_poll)
    }

    /// Poll until `shutdown` resolves. A failed poll drops the connection and
    /// reattaches with backoff; whatever happened meanwhile cannot be known,
    /// so the first successful reattach invalidates the whole cache.
    pub async fn run(
        mut self,
        cache: Arc<QueryCache>,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        let mut gap = false;
        loop {
            if self.client.is_none() {
                match self.attach().await {
                    Ok(()) => {
                        if gap {
                            tracing::warn!(
                                "cache logical source: reattached after a gap; invalidating the whole cache"
                            );
                            cache.invalidate_all();
                            self.stats
                                .full_invalidations
                                .fetch_add(1, Ordering::Relaxed);
                            gap = false;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "cache logical source: attach failed"),
                }
            }
            let mut again = false;
            if self.client.is_some() {
                match self.poll_once(&cache).await {
                    Ok(full) => again = full,
                    Err(e) => {
                        tracing::warn!(error = %e, "cache logical source: poll failed; reattaching");
                        if let Some(c) = self.client.take() {
                            c.close().await;
                        }
                        self.stats.connected.store(0, Ordering::Relaxed);
                        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                        gap = true;
                    }
                }
            }
            if again {
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(self.settings.feedback) => {}
                _ = shutdown.recv() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation(oid: u32, ns: &str, name: &str) -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend_from_slice(&oid.to_be_bytes());
        m.extend_from_slice(ns.as_bytes());
        m.push(0);
        m.extend_from_slice(name.as_bytes());
        m.push(0);
        m.extend_from_slice(&[b'd', 0, 0]); // replica identity + 0 columns
        m
    }

    fn data(tag: u8, oid: u32) -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&oid.to_be_bytes());
        m.extend_from_slice(b"N\0\0");
        m
    }

    fn truncate(oids: &[u32]) -> Vec<u8> {
        let mut m = vec![b'T'];
        m.extend_from_slice(&(oids.len() as u32).to_be_bytes());
        m.push(0);
        for o in oids {
            m.extend_from_slice(&o.to_be_bytes());
        }
        m
    }

    const BEGIN: &[u8] = b"B";
    const COMMIT: &[u8] = b"C";

    #[test]
    fn committed_writes_name_their_tables_once() {
        let mut d = PgOutputDecoder::default();
        assert_eq!(d.feed(BEGIN), None);
        assert_eq!(d.feed(&relation(16384, "public", "Accounts")), None);
        assert_eq!(d.feed(&relation(16390, "app", "history")), None);
        for msg in [data(b'U', 16384), data(b'I', 16390), data(b'D', 16384)] {
            assert_eq!(d.feed(&msg), None);
        }
        assert_eq!(
            d.feed(COMMIT),
            Some(Invalidation::Tables(vec![
                "accounts".into(),
                "history".into()
            ]))
        );
        // The relation cache survives across transactions.
        d.feed(BEGIN);
        d.feed(&data(b'I', 16390));
        assert_eq!(
            d.feed(COMMIT),
            Some(Invalidation::Tables(vec!["history".into()]))
        );
    }

    #[test]
    fn truncate_and_unknown_relations() {
        let mut d = PgOutputDecoder::default();
        d.feed(&relation(1, "public", "a"));
        d.feed(&relation(2, "public", "b"));
        d.feed(BEGIN);
        d.feed(&truncate(&[1, 2]));
        assert_eq!(
            d.feed(COMMIT),
            Some(Invalidation::Tables(vec!["a".into(), "b".into()]))
        );
        d.feed(BEGIN);
        d.feed(&data(b'I', 99));
        assert_eq!(d.feed(COMMIT), Some(Invalidation::All), "unknown oid");
        d.feed(BEGIN);
        assert_eq!(d.feed(COMMIT), None, "an empty transaction changes nothing");
        assert_eq!(d.feed(b"O\0\0\0\0\0\0\0\0x\0"), None, "origin ignored");
    }

    #[test]
    fn malformed_messages_fail_safe() {
        let mut d = PgOutputDecoder::default();
        assert_eq!(d.feed(b""), Some(Invalidation::All));
        assert_eq!(
            d.feed(b"I\x00"),
            Some(Invalidation::All),
            "outside a transaction"
        );
        d.feed(BEGIN);
        d.feed(b"R\x00\x00\x00\x01broken");
        assert_eq!(d.feed(COMMIT), Some(Invalidation::All));
    }

    #[test]
    fn bytea_hex_and_identifiers() {
        assert_eq!(decode_bytea_hex("\\x42ff00"), Some(vec![0x42, 0xff, 0]));
        assert_eq!(decode_bytea_hex("42ff"), None);
        assert_eq!(decode_bytea_hex("\\x4"), None);
        assert!(is_plain_identifier("heliosdb_proxy_cache"));
        assert!(!is_plain_identifier("Bad"));
        assert!(!is_plain_identifier("x'; DROP"));
        assert!(!is_plain_identifier("1abc"));
        assert!(!is_plain_identifier(""));
    }

    #[test]
    fn dsn_parsing() {
        let d = LogicalDsn::parse("host=pg-a port=5433 user=repl password='p w\\'d' dbname=app")
            .unwrap();
        assert_eq!(
            d,
            LogicalDsn {
                host: "pg-a".into(),
                port: 5433,
                user: "repl".into(),
                password: Some("p w'd".into()),
                dbname: Some("app".into()),
            }
        );
        assert_eq!(LogicalDsn::parse("host=h user=u").unwrap().port, 5432);
        assert!(LogicalDsn::parse("user=u").is_err());
        assert!(LogicalDsn::parse("host=h user=u sslmode=require").is_err());
        assert!(LogicalDsn::parse("host=h user=u port=x").is_err());
        assert!(LogicalDsn::parse("host user=u").is_err());
    }
}

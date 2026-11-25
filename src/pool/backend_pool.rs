//! Data-path backend connection pool for Transaction / Statement pooling modes.
//!
//! This is the *raw-stream* pool that actually multiplexes clients onto a
//! bounded set of backend connections — the piece that makes the
//! `pool-modes` feature do real work on the wire. It is deliberately distinct
//! from [`crate::pool::manager::ConnectionPoolManager`], which models pooling
//! over the higher-level `BackendClient` message API; the proxy data path
//! forwards raw PostgreSQL-wire bytes, so it needs a pool of authenticated
//! `TcpStream`s.
//!
//! ## Identity keying (why this is safe)
//!
//! HeliosProxy authenticates backend connections by **passing the client's own
//! credentials through** to PostgreSQL (the client SCRAM handshake is relayed).
//! A parked connection is therefore authenticated as a specific
//! `(user, database)` principal. The pool keys idle connections by
//! `node\0user\0database`, so a connection is only ever handed to a client that
//! connected with the *same* identity — and that client independently
//! authenticated before it could reach the pool. This is exactly PgBouncer's
//! per-(user,db) pooling model; it does not multiplex distinct users onto one
//! backend identity (that would need proxy-terminated auth with a shared
//! backend credential, which is a separate, larger change). A tenant with pool
//! settings of its own (`[[multi_tenancy.tenant_settings]] pool_max_idle` /
//! `pool_max_connections`) adds `\0t=<tenant>` to its keys, so its connections
//! are pooled apart and capped on their own.
//!
//! ## Cleanliness
//!
//! A connection is `DISCARD ALL`-reset by the caller before it is parked
//! (see the release path in `server/backend_conn.rs`), so the next borrower — possibly a
//! *different* client of the same identity — never inherits GUCs, temp tables,
//! prepared statements, or advisory locks. On checkout the connection is
//! liveness-probed (`[pool] test_on_acquire`) so a peer that closed the socket
//! while idle is dropped rather than handed out.
//!
//! ## Lifetime and ahead-of-use connections
//!
//! A connection remembers when it was opened: past `max_lifetime` it is
//! closed instead of being parked, dropped at checkout and reaped
//! ([`BackendIdlePool::reap`]). The reaper keeps the newest `min_idle`
//! connections of each key through the idle TTL; the [`DialSpec`]s recorded
//! at check-in tell it how to open the ones missing (`[pool_mode] min_idle`,
//! `[pool] min_connections`).

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// Build the pool key for a `(node, user, database)` triple. NUL-delimited so
/// the three components can never collide across boundaries.
pub fn pool_key(node: &str, user: &str, database: &str) -> String {
    format!("{}\0{}\0{}", node, user, database)
}

/// The BackendKeyData (process id, secret key) the backend behind a
/// connection reported at its startup: what a CancelRequest for a statement
/// running on that connection names. `None` when the backend sent none.
pub type BackendKey = Option<(u32, u32)>;

/// A settings proof taken on a backend connection: the result cache's
/// namespace for it (`[cache]`), computed from the connection's `SHOW ALL`
/// and the startup parameters of the session that proved it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingsProof {
    /// The proven namespace.
    pub namespace: [u8; 32],
    /// The startup-parameter digest the namespace was computed with. A
    /// borrower with a different digest proves the connection again.
    pub startup: [u8; 32],
    /// When the proof was taken (the borrower applies its own age limit).
    pub proven_at: Instant,
}

/// What a borrower may take as known about a parked connection's session
/// settings without asking the backend again. The default knows nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParkedSettings {
    /// The connection holds its reset settings (those it started with):
    /// nothing changed them since it was opened, or a reset that restores
    /// them (`DISCARD ALL`, `RESET ALL`) ran before it was parked.
    pub baseline: bool,
    /// A proof that still describes the connection's settings.
    pub proof: Option<SettingsProof>,
    /// The proof kept under `[cache] backend_context_immutable`, which holds
    /// for the physical connection's life.
    pub immutable_proof: Option<SettingsProof>,
}

/// A connection handed to or taken from the pool with what goes with it.
pub struct PooledConn {
    /// The authenticated backend socket.
    pub stream: TcpStream,
    /// The connection's node slot (H-05), kept while it is parked.
    pub slot: crate::node_slots::Slot,
    /// Handed back with the connection, so the borrower can cancel a
    /// statement it abandons on it.
    pub backend_key: BackendKey,
    /// What is known about its settings ([`ParkedSettings`]).
    pub settings: ParkedSettings,
    /// The connection's slot under its tenant's `pool_max_connections`.
    pub tenant_slot: crate::node_slots::Slot,
    /// When the backend connection was opened (its lifetime starts there).
    pub opened_at: Instant,
}

impl PooledConn {
    /// A connection opened just now, with no slot, no key and nothing known
    /// about its settings.
    pub fn fresh(stream: TcpStream) -> Self {
        Self {
            stream,
            slot: None,
            backend_key: None,
            settings: ParkedSettings::default(),
            tenant_slot: None,
            opened_at: Instant::now(),
        }
    }

    /// Whether the connection outlived `max_lifetime` (`None`: no limit).
    pub fn expired(&self, max_lifetime: Option<Duration>) -> bool {
        max_lifetime.is_some_and(|max| self.opened_at.elapsed() >= max)
    }
}

/// One parked connection.
struct Parked {
    conn: PooledConn,
    parked_at: Instant,
}

/// How [`BackendIdlePool::checkout_conn_with`] leases.
#[derive(Debug, Clone, Copy)]
pub struct LeaseOptions {
    /// Probe the connection for liveness before handing it out (`[pool]
    /// test_on_acquire`).
    pub probe: bool,
    /// Connections older than this are closed instead of handed out.
    pub max_lifetime: Option<Duration>,
}

impl LeaseOptions {
    /// Probe every lease; no lifetime limit.
    pub const PROBED: LeaseOptions = LeaseOptions {
        probe: true,
        max_lifetime: None,
    };
}

/// How the idle reaper opens a connection for one pool identity on its own
/// (`[pool_mode] min_idle`, `[pool] min_connections`), recorded when a
/// session parks a connection under that identity.
#[derive(Debug, Clone)]
pub struct DialSpec {
    /// The node, `host:port`.
    pub node: String,
    /// The startup packet the session's connections were opened with.
    pub startup: Arc<[u8]>,
    /// The user the backend authenticates (the proxy answers its challenges
    /// only with a credential of its own, `[auth] auth_file`).
    pub user: String,
    /// The tenant pool whose cap the identity's connections count against.
    pub tenant_pool: Option<Arc<crate::tenant_slots::TenantPool>>,
    /// The identity's idle cap.
    pub max_idle: usize,
    /// When a session last parked a connection of the identity.
    pub last_used: Instant,
    /// The backend asked for a password the proxy does not hold (pass-through
    /// auth): the identity is never opened ahead of use.
    pub no_credential: bool,
}

/// A bounded set of idle, authenticated backend connections, partitioned by
/// connection identity. Cheap to clone-share behind an `Arc`.
pub struct BackendIdlePool {
    /// identity-key -> stack of parked connections (newest last).
    idle: DashMap<String, Vec<Parked>>,
    /// Hard cap on idle connections parked per identity key.
    max_idle_per_key: usize,
    /// Hard cap on idle connections parked across ALL identities — bounds total
    /// file descriptors / memory regardless of how many distinct
    /// `(node,user,db)` identities appear.
    max_total_idle: usize,
    /// Live count of parked connections (kept in step with `idle`) so the
    /// global-cap check and `idle_count()` are O(1) instead of O(keys).
    total_idle: AtomicUsize,
    /// How to open each recently used identity ahead of use; only recorded
    /// while `min_idle` or `min_connections` asks for it. At most
    /// `max_total_idle` entries.
    specs: DashMap<String, DialSpec>,
    /// Checkout hits — a parked connection was reused.
    reuses: AtomicU64,
    /// Connections parked (checked in) successfully.
    parked: AtomicU64,
    /// Check-ins refused because an idle cap (per-key or global) was reached —
    /// the connection is closed by the caller dropping it.
    over_capacity: AtomicU64,
    /// Parked connections dropped at checkout because the peer had closed
    /// them (or left unexpected bytes) while idle.
    stale_evicted: AtomicU64,
    /// Parked connections dropped by the idle reaper for exceeding the TTL.
    reaped: AtomicU64,
    /// Connections parked WITHOUT running the reset query because they were
    /// provably clean: the `skip_clean_reset` conditional-reset optimisation,
    /// or a lease that served only a cache hit.
    resets_skipped: AtomicU64,
    /// Idle connections closed to make room on a backend that refused a new
    /// connection with 53300 (`too_many_connections`).
    capacity_evictions: AtomicU64,
    /// Connections closed for outliving the pool's max lifetime: at release
    /// instead of parking, at checkout, or by the reaper.
    lifetime_expired: AtomicU64,
    /// Requests refused with 53300 because no backend connection could be
    /// opened within `[pool_mode] acquire_timeout_secs`.
    acquire_timeouts: AtomicU64,
    /// Connections leased without the liveness probe (`[pool] test_on_acquire
    /// = false`) that refused their first write, replaced by a fresh dial.
    stale_lease_redials: AtomicU64,
    /// Connections the reaper opened ahead of use (`min_idle`,
    /// `min_connections`).
    topped_up: AtomicU64,
    /// Ahead-of-use dials that failed (connect, authentication).
    topup_failures: AtomicU64,
    /// Released connections closed instead of parked because a session was
    /// waiting for their node's or tenant's slot.
    handed_over: AtomicU64,
    /// The no-credential warning was logged.
    no_credential_warned: std::sync::atomic::AtomicBool,
}

impl BackendIdlePool {
    /// Create a pool that parks at most `max_idle_per_key` connections per
    /// `(node,user,db)` identity and `max_total_idle` across all identities.
    /// A floor of 1 is enforced on each so the pool always retains at least one
    /// reusable connection.
    pub fn new(max_idle_per_key: usize, max_total_idle: usize) -> Self {
        Self {
            idle: DashMap::new(),
            max_idle_per_key: max_idle_per_key.max(1),
            max_total_idle: max_total_idle.max(1),
            total_idle: AtomicUsize::new(0),
            specs: DashMap::new(),
            reuses: AtomicU64::new(0),
            parked: AtomicU64::new(0),
            over_capacity: AtomicU64::new(0),
            stale_evicted: AtomicU64::new(0),
            reaped: AtomicU64::new(0),
            resets_skipped: AtomicU64::new(0),
            capacity_evictions: AtomicU64::new(0),
            lifetime_expired: AtomicU64::new(0),
            acquire_timeouts: AtomicU64::new(0),
            stale_lease_redials: AtomicU64::new(0),
            topped_up: AtomicU64::new(0),
            topup_failures: AtomicU64::new(0),
            handed_over: AtomicU64::new(0),
            no_credential_warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The idle cap per identity (`[pool_mode] max_pool_size`, at least 1).
    pub fn max_idle_per_key(&self) -> usize {
        self.max_idle_per_key
    }

    /// Record that a provably-clean connection was parked without a reset.
    pub fn note_reset_skipped(&self) {
        self.resets_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Connections parked without a reset because they were provably clean.
    pub fn resets_skipped(&self) -> u64 {
        self.resets_skipped.load(Ordering::Relaxed)
    }

    /// Take a live idle connection for `key`, or `None` if the pool has no
    /// usable one (caller then dials a fresh connection). Dead/stale parked
    /// connections are evicted in passing.
    pub fn checkout(&self, key: &str) -> Option<TcpStream> {
        self.checkout_conn_with(key, LeaseOptions::PROBED)
            .map(|conn| conn.stream)
    }

    /// [`Self::checkout`], also handing back the connection's node slot
    /// (H-05) so it stays held while the connection is in use.
    pub fn checkout_with_slot(&self, key: &str) -> Option<(TcpStream, crate::node_slots::Slot)> {
        self.checkout_conn_with(key, LeaseOptions::PROBED)
            .map(|conn| (conn.stream, conn.slot))
    }

    /// [`Self::checkout_with_slot`], also handing back the BackendKeyData
    /// the connection was parked with ([`Self::checkin_keyed`]).
    pub fn checkout_keyed(
        &self,
        key: &str,
    ) -> Option<(TcpStream, crate::node_slots::Slot, BackendKey)> {
        self.checkout_conn_with(key, LeaseOptions::PROBED)
            .map(|conn| (conn.stream, conn.slot, conn.backend_key))
    }

    /// [`Self::checkout_keyed`], also handing back what is known about the
    /// connection's settings: the newest live connection of `key`, probed
    /// for liveness, with no lifetime limit ([`LeaseOptions::PROBED`]).
    pub fn checkout_conn(&self, key: &str) -> Option<PooledConn> {
        self.checkout_conn_with(key, LeaseOptions::PROBED)
    }

    /// Take the newest parked connection of `key` that is usable under
    /// `opts`: one past `opts.max_lifetime` is closed (counted in
    /// [`Self::lifetime_expired`]); with `opts.probe`, one whose peer closed
    /// it (or sent anything) while it was parked is closed too (counted in
    /// [`Self::stale_evicted`]). `None` when nothing usable is parked.
    pub fn checkout_conn_with(&self, key: &str, opts: LeaseOptions) -> Option<PooledConn> {
        let mut guard = self.idle.get_mut(key)?;
        while let Some(Parked { conn, .. }) = guard.pop() {
            self.total_idle.fetch_sub(1, Ordering::Relaxed);
            if conn.expired(opts.max_lifetime) {
                self.lifetime_expired.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if !opts.probe || Self::probe_alive(&conn.stream) {
                self.reuses.fetch_add(1, Ordering::Relaxed);
                return Some(conn);
            }
            // Peer closed (or desynced) while idle — drop it and try the next.
            self.stale_evicted.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Park a (freshly reset) connection for reuse under `key`. Returns `false`
    /// when an idle cap (per-key OR global) is already reached — in that case
    /// the connection is dropped (closed) by being moved in and discarded,
    /// shedding excess capacity.
    pub fn checkin(&self, key: &str, stream: TcpStream) -> bool {
        self.checkin_with_slot(key, stream, None)
    }

    /// [`Self::checkin`] keeping the connection's node slot (H-05) parked
    /// with it; a refused check-in drops (releases) the slot with the socket.
    pub fn checkin_with_slot(
        &self,
        key: &str,
        stream: TcpStream,
        slot: crate::node_slots::Slot,
    ) -> bool {
        self.checkin_keyed(key, stream, slot, None)
    }

    /// [`Self::checkin_with_slot`] keeping the connection's BackendKeyData
    /// (`backend_key`) with it, for [`Self::checkout_keyed`] to hand back.
    pub fn checkin_keyed(
        &self,
        key: &str,
        stream: TcpStream,
        slot: crate::node_slots::Slot,
        backend_key: BackendKey,
    ) -> bool {
        self.checkin_conn_capped(
            key,
            PooledConn {
                stream,
                slot,
                backend_key,
                settings: ParkedSettings::default(),
                tenant_slot: None,
                opened_at: Instant::now(),
            },
            self.max_idle_per_key,
        )
    }

    /// [`Self::checkin_keyed`] with what is known about the connection's
    /// settings, at most the pool's per-key idle cap
    /// ([`Self::checkin_conn_capped`] takes another cap).
    pub fn checkin_conn(&self, key: &str, conn: PooledConn) -> bool {
        self.checkin_conn_capped(key, conn, self.max_idle_per_key)
    }

    /// Park `conn` under `key`, with what is known about its settings for
    /// [`Self::checkout_conn_with`] to hand back, at most `max_idle` per key (at
    /// least 1) and `max_total_idle` across all keys. `false` when a cap is
    /// reached: the connection, its slots with it, is dropped (closed).
    pub fn checkin_conn_capped(&self, key: &str, conn: PooledConn, max_idle: usize) -> bool {
        // Global ceiling first — bounds total FDs across all identities. Reserve
        // the slot atomically (fetch_add, inspect the prior value) rather than
        // load-then-act: a plain load outside the lock lets N concurrent
        // check-ins to distinct keys each observe `cap - 1` and all push,
        // overshooting the ceiling. With a reservation only one racer sees a
        // prior value below the cap; the rest roll their increment back.
        if self.total_idle.fetch_add(1, Ordering::Relaxed) >= self.max_total_idle {
            self.total_idle.fetch_sub(1, Ordering::Relaxed);
            self.over_capacity.fetch_add(1, Ordering::Relaxed);
            return false; // `conn` dropped here → socket closed.
        }
        let mut entry = self.idle.entry(key.to_string()).or_default();
        if entry.len() >= max_idle.max(1) {
            // Per-key cap reached — release the global slot we reserved.
            self.total_idle.fetch_sub(1, Ordering::Relaxed);
            self.over_capacity.fetch_add(1, Ordering::Relaxed);
            return false; // `conn` dropped here → socket closed.
        }
        entry.push(Parked {
            conn,
            parked_at: Instant::now(),
        });
        self.parked.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Drop parked connections that have been idle longer than `max_age` so a
    /// connection the backend has (or will) close on its own idle timeout is
    /// not handed out stale, and idle capacity is released back to the OS.
    /// Returns the number reaped. Intended to be called periodically by a
    /// background task.
    pub fn reap_idle(&self, max_age: Duration) -> usize {
        self.reap(Some(max_age), None, 0, None)
    }

    /// The idle reaper's pass: close parked connections idle longer than
    /// `idle_ttl` (`None`: never), except the newest `keep_newest` of each key
    /// (`[pool_mode] min_idle`) and, with `closable`, beyond the number of
    /// idle closes each node may still take (its open connections above
    /// `[pool] min_connections`; a node not listed has no floor); and every
    /// one older than `max_lifetime` (`None`: no limit), kept or not.
    /// Returns how many were closed.
    pub fn reap(
        &self,
        idle_ttl: Option<Duration>,
        max_lifetime: Option<Duration>,
        keep_newest: usize,
        mut closable: Option<&mut std::collections::HashMap<String, usize>>,
    ) -> usize {
        if idle_ttl.is_none() && max_lifetime.is_none() {
            return 0;
        }
        let (mut idle_closed, mut expired) = (0usize, 0usize);
        for mut entry in self.idle.iter_mut() {
            let node = entry
                .key()
                .split('\0')
                .next()
                .unwrap_or_default()
                .to_string();
            let stack = entry.value_mut();
            // Newest last: the last `keep_newest` are the floor.
            let floor_from = stack.len().saturating_sub(keep_newest);
            let mut index = 0usize;
            stack.retain(|p| {
                let kept_by_floor = index >= floor_from;
                index += 1;
                if p.conn.expired(max_lifetime) {
                    expired += 1;
                    return false;
                }
                let idle_expired = idle_ttl.is_some_and(|ttl| p.parked_at.elapsed() >= ttl);
                if !idle_expired || kept_by_floor {
                    return true;
                }
                // The node's floor: close only while it stays above it.
                if let Some(budget) = closable.as_deref_mut().and_then(|c| c.get_mut(&node)) {
                    if *budget == 0 {
                        return true;
                    }
                    *budget -= 1;
                }
                idle_closed += 1;
                false
            });
        }
        let closed = idle_closed + expired;
        if closed > 0 {
            self.total_idle.fetch_sub(closed, Ordering::Relaxed);
            self.reaped.fetch_add(idle_closed as u64, Ordering::Relaxed);
            self.lifetime_expired
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
        closed
    }

    /// Liveness probe for an idle parked connection: a clean idle backend has
    /// no pending bytes, so a non-blocking read should report `WouldBlock`.
    /// `Ok(0)` means the peer closed; `Ok(n>0)` means unexpected data (protocol
    /// desync) — both are treated as dead.
    fn probe_alive(stream: &TcpStream) -> bool {
        let mut probe = [0u8; 1];
        matches!(
            stream.try_read(&mut probe),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
        )
    }

    /// Total idle connections currently parked across all identities (O(1)).
    pub fn idle_count(&self) -> usize {
        self.total_idle.load(Ordering::Relaxed)
    }

    /// Idle connections parked under `key`.
    pub fn idle_count_for(&self, key: &str) -> usize {
        self.idle.get(key).map_or(0, |e| e.len())
    }

    /// Idle connections parked for `node`, any identity (O(keys)).
    pub fn idle_count_for_node(&self, node: &str) -> usize {
        let prefix = format!("{node}\0");
        self.idle
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| e.value().len())
            .sum()
    }

    /// Global ceiling on parked idle connections.
    pub fn max_total_idle(&self) -> usize {
        self.max_total_idle
    }

    /// Number of parked connections dropped by the idle reaper (TTL).
    pub fn reaped(&self) -> u64 {
        self.reaped.load(Ordering::Relaxed)
    }

    /// Number of checkout hits (connections reused rather than dialed fresh).
    pub fn reuses(&self) -> u64 {
        self.reuses.load(Ordering::Relaxed)
    }

    /// Number of successful check-ins.
    pub fn parked(&self) -> u64 {
        self.parked.load(Ordering::Relaxed)
    }

    /// Number of check-ins refused for exceeding the per-key idle cap.
    pub fn over_capacity(&self) -> u64 {
        self.over_capacity.load(Ordering::Relaxed)
    }

    /// Number of stale connections evicted at checkout.
    pub fn stale_evicted(&self) -> u64 {
        self.stale_evicted.load(Ordering::Relaxed)
    }

    /// Count a connection closed at release for outliving the lifetime.
    pub fn note_lifetime_expired(&self) {
        self.lifetime_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Connections closed for outliving the max lifetime.
    pub fn lifetime_expired(&self) -> u64 {
        self.lifetime_expired.load(Ordering::Relaxed)
    }

    /// Count a request refused at `[pool_mode] acquire_timeout_secs`.
    pub fn note_acquire_timeout(&self) {
        self.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Requests refused with 53300 at `[pool_mode] acquire_timeout_secs`.
    pub fn acquire_timeouts(&self) -> u64 {
        self.acquire_timeouts.load(Ordering::Relaxed)
    }

    /// Count an unprobed lease replaced after its first write failed.
    pub fn note_stale_lease_redial(&self) {
        self.stale_lease_redials.fetch_add(1, Ordering::Relaxed);
    }

    /// Unprobed leases replaced by a fresh dial.
    pub fn stale_lease_redials(&self) -> u64 {
        self.stale_lease_redials.load(Ordering::Relaxed)
    }

    /// Count an ahead-of-use dial: parked (`true`) or failed.
    pub fn note_topup(&self, parked: bool) {
        if parked {
            self.topped_up.fetch_add(1, Ordering::Relaxed);
        } else {
            self.topup_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Connections opened ahead of use and parked.
    pub fn topped_up(&self) -> u64 {
        self.topped_up.load(Ordering::Relaxed)
    }

    /// Ahead-of-use dials that failed.
    pub fn topup_failures(&self) -> u64 {
        self.topup_failures.load(Ordering::Relaxed)
    }

    /// Count a released connection closed for a session waiting for its
    /// slot.
    pub fn note_handed_over(&self) {
        self.handed_over.fetch_add(1, Ordering::Relaxed);
    }

    /// Released connections closed so that a waiting session got their
    /// node's or tenant's slot.
    pub fn handed_over(&self) -> u64 {
        self.handed_over.load(Ordering::Relaxed)
    }

    /// `true` the first time only: the warning that an identity cannot be
    /// opened ahead of use (the proxy holds no credential for it) is logged
    /// once per process.
    pub fn first_no_credential_warning(&self) -> bool {
        !self
            .no_credential_warned
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    /// Close the longest-parked idle connection to `node` (any identity), to
    /// hand its backend slot to a new connection the backend just refused with
    /// `53300 too_many_connections`. Parked connections hold backend capacity
    /// that is idle by definition, and a pass-through client cannot borrow one
    /// for its own authentication. Returns `false` when nothing to `node` is
    /// parked.
    pub fn evict_one_idle_for_node(&self, node: &str) -> bool {
        let prefix = format!("{}\0", node);
        self.evict_one_where(|key| key.starts_with(&prefix))
    }

    /// Close the longest-parked idle connection of the tenant whose pool keys
    /// end in `suffix` (`TenantPlan::pool_suffix`), to hand its tenant slot
    /// to a new connection of the tenant. `false` when none is parked.
    pub fn evict_one_idle_for_tenant(&self, suffix: &str) -> bool {
        self.evict_one_where(|key| key.ends_with(suffix))
    }

    fn evict_one_where(&self, matches: impl Fn(&str) -> bool) -> bool {
        for mut entry in self.idle.iter_mut() {
            if !matches(entry.key()) || entry.value().is_empty() {
                continue;
            }
            // Oldest first: index 0 was parked earliest (checkin pushes).
            drop(entry.value_mut().remove(0));
            self.total_idle.fetch_sub(1, Ordering::Relaxed);
            self.capacity_evictions.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Close every idle connection parked for `node` (any identity) and
    /// return how many were closed. Used when `node` stops being the write
    /// leader: its HA manager typically restarts a demoted server, so the
    /// parked sockets are likely dead or about to be.
    pub fn evict_idle_for_node(&self, node: &str) -> usize {
        let prefix = format!("{}\0", node);
        let mut closed = 0;
        for mut entry in self.idle.iter_mut() {
            if entry.key().starts_with(&prefix) {
                closed += entry.value().len();
                entry.value_mut().clear();
            }
        }
        self.total_idle.fetch_sub(closed, Ordering::Relaxed);
        closed
    }

    /// Idle connections closed by [`Self::evict_one_idle_for_node`].
    pub fn capacity_evictions(&self) -> u64 {
        self.capacity_evictions.load(Ordering::Relaxed)
    }

    /// Note that a session parked a connection of `key` now: the reaper may
    /// open the identity ahead of use. `make` builds the spec the first time
    /// (the only time the startup packet is built). A full table first drops
    /// its least recently used spec.
    pub fn note_use(&self, key: &str, now: Instant, make: impl FnOnce() -> DialSpec) {
        if let Some(mut spec) = self.specs.get_mut(key) {
            spec.last_used = now;
            return;
        }
        if self.specs.len() >= self.max_total_idle {
            let oldest = self
                .specs
                .iter()
                .min_by_key(|e| e.value().last_used)
                .map(|e| e.key().clone());
            if let Some(oldest) = oldest {
                self.specs.remove(&oldest);
            }
        }
        let mut spec = make();
        spec.last_used = now;
        self.specs.insert(key.to_string(), spec);
    }

    /// Forget the identities no session used within `ttl` (`None`: keep).
    pub fn expire_specs(&self, ttl: Option<Duration>) {
        if let Some(ttl) = ttl {
            self.specs.retain(|_, spec| spec.last_used.elapsed() < ttl);
        }
    }

    /// The identities the reaper may open ahead of use, with their keys.
    pub fn specs(&self) -> Vec<(String, DialSpec)> {
        self.specs
            .iter()
            .filter(|e| !e.value().no_credential)
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// The most recently used identity of `node` that may be opened ahead
    /// of use.
    pub fn latest_spec_for(&self, node: &str) -> Option<(String, DialSpec)> {
        self.specs
            .iter()
            .filter(|e| e.value().node == node && !e.value().no_credential)
            .max_by_key(|e| e.value().last_used)
            .map(|e| (e.key().clone(), e.value().clone()))
    }

    /// The backend asked `key`'s identity for a password the proxy does not
    /// hold: it is not opened ahead of use again.
    pub fn mark_no_credential(&self, key: &str) {
        if let Some(mut spec) = self.specs.get_mut(key) {
            spec.no_credential = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    /// Open a connected TcpStream pair against a throwaway loopback listener so
    /// tests can exercise the pool's bookkeeping with real (live) sockets.
    async fn live_stream(listener: &TcpListener) -> TcpStream {
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, _server) = tokio::join!(connect, accept);
        // Keep the server side alive by leaking it into a long-lived holder via
        // the caller; here we just return the client side. The accepted half is
        // dropped, which is fine for liveness tests that re-accept per stream.
        client.unwrap()
    }

    /// Both halves of a loopback connection, so the parked side stays alive.
    async fn live_pair(listener: &TcpListener) -> (TcpStream, TcpStream) {
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (client.unwrap(), accepted.unwrap().0)
    }

    #[tokio::test]
    async fn capacity_eviction_closes_the_oldest_idle_connection_of_that_node_only() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Keep the accepted halves open so parked connections stay alive for
        // the checkout probe below.
        let mut server_halves = Vec::new();
        let pool = BackendIdlePool::new(4, 1000);
        let a1 = pool_key("10.0.0.1:5432", "u1", "d");
        let a2 = pool_key("10.0.0.1:5432", "u2", "d");
        // Same host, different port: must not match the node prefix.
        let b = pool_key("10.0.0.1:54320", "u1", "d");
        let mut park = |key: &str, pair: (TcpStream, TcpStream)| {
            let addr = pair.0.local_addr().unwrap();
            server_halves.push(pair.1);
            assert!(pool.checkin(key, pair.0));
            addr
        };
        let first_addr = park(&a1, live_pair(&listener).await);
        park(&a1, live_pair(&listener).await);
        park(&b, live_pair(&listener).await);
        assert_eq!(pool.idle_count(), 3);

        assert!(pool.evict_one_idle_for_node("10.0.0.1:5432"));
        assert_eq!(pool.idle_count(), 2);
        assert_eq!(pool.capacity_evictions(), 1);
        // The oldest one went; the remaining a1 connection is the newer one.
        let left = pool.checkout(&a1).expect("one a1 connection remains");
        assert_ne!(left.local_addr().unwrap(), first_addr);

        // Nothing parked for the node any more (a2 never had one), b untouched.
        assert!(pool.checkout(&a2).is_none());
        assert!(!pool.evict_one_idle_for_node("10.0.0.1:5432"));
        assert!(!pool.evict_one_idle_for_node("10.9.9.9:5432"));
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.capacity_evictions(), 1);
    }

    #[tokio::test]
    async fn leader_move_eviction_closes_every_idle_connection_of_that_node_only() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut server_halves = Vec::new();
        let pool = BackendIdlePool::new(4, 1000);
        let a1 = pool_key("10.0.0.1:5432", "u1", "d");
        let a2 = pool_key("10.0.0.1:5432", "u2", "d");
        // Same host, different port: must not match the node prefix.
        let b = pool_key("10.0.0.1:54320", "u1", "d");
        for key in [&a1, &a1, &a2, &b] {
            let (client, accepted) = live_pair(&listener).await;
            server_halves.push(accepted);
            assert!(pool.checkin(key, client));
        }
        assert_eq!(pool.idle_count(), 4);

        assert_eq!(pool.evict_idle_for_node("10.0.0.1:5432"), 3);
        assert_eq!(pool.idle_count(), 1);
        assert!(pool.checkout(&a1).is_none());
        assert!(pool.checkout(&a2).is_none());
        assert_eq!(pool.evict_idle_for_node("10.0.0.1:5432"), 0);
        assert!(
            pool.checkout(&b).is_some(),
            "other nodes keep their connections"
        );
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.capacity_evictions(), 0, "a separate counter");
    }

    /// H-05: a parked connection keeps its node slot; checkout hands it back
    /// and a refused check-in releases it with the socket.
    #[tokio::test]
    async fn parked_connections_keep_their_node_slot() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let pool = BackendIdlePool::new(1, 1000);
        let key = pool_key("db:5432", "u", "d");
        let (c1, _s1) = live_pair(&listener).await;
        let (c2, _s2) = live_pair(&listener).await;

        assert!(pool.checkin_with_slot(&key, c1, Some(sem.clone().try_acquire_owned().unwrap())));
        assert_eq!(
            sem.available_permits(),
            1,
            "the parked connection holds its slot"
        );
        // Per-key cap 1: refused, and its slot goes with the dropped socket.
        assert!(!pool.checkin_with_slot(&key, c2, Some(sem.clone().try_acquire_owned().unwrap())));
        assert_eq!(sem.available_permits(), 1);

        let (_stream, slot) = pool.checkout_with_slot(&key).expect("parked connection");
        assert!(slot.is_some());
        assert_eq!(sem.available_permits(), 1, "still held while in use");
        drop(slot);
        assert_eq!(sem.available_permits(), 2);
    }

    /// ac858d61400b: a parked connection keeps the BackendKeyData of the
    /// backend behind it, so the session that borrows it next can cancel a
    /// statement it abandons there; one parked without a key has none.
    #[tokio::test]
    async fn parked_connections_keep_their_backend_key() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 1000);
        let key = pool_key("db:5432", "u", "d");
        let (c1, _s1) = live_pair(&listener).await;
        let (c2, _s2) = live_pair(&listener).await;
        let keyed_addr = c1.local_addr().unwrap();
        assert!(pool.checkin_keyed(&key, c1, None, Some((7, 1007))));
        assert!(pool.checkin_with_slot(&key, c2, None));

        // Newest first: the one parked without a key, then the keyed one.
        let (_, _, none) = pool.checkout_keyed(&key).expect("parked");
        assert_eq!(none, None);
        let (stream, _, backend_key) = pool.checkout_keyed(&key).expect("parked");
        assert_eq!(stream.local_addr().unwrap(), keyed_addr);
        assert_eq!(backend_key, Some((7, 1007)));
        assert!(pool.checkout_keyed(&key).is_none());
    }

    #[tokio::test]
    async fn parked_connections_keep_what_is_known_about_their_settings() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 1000);
        let key = pool_key("db:5432", "u", "d");
        let (c1, _s1) = live_pair(&listener).await;
        let (c2, _s2) = live_pair(&listener).await;
        let proof = SettingsProof {
            namespace: [1; 32],
            startup: [2; 32],
            proven_at: Instant::now(),
        };
        let settings = ParkedSettings {
            baseline: true,
            proof: Some(proof),
            immutable_proof: None,
        };
        assert!(pool.checkin_conn_capped(
            &key,
            PooledConn {
                stream: c1,
                slot: None,
                backend_key: Some((3, 4)),
                settings,
                tenant_slot: None,
                opened_at: Instant::now(),
            },
            8,
        ));
        // The keyed API parks with nothing known.
        assert!(pool.checkin_keyed(&key, c2, None, None));

        let plain = pool
            .checkout_conn_with(&key, LeaseOptions::PROBED)
            .expect("parked");
        assert_eq!(plain.settings, ParkedSettings::default());
        assert!(!plain.settings.baseline);
        let proven = pool
            .checkout_conn_with(&key, LeaseOptions::PROBED)
            .expect("parked");
        assert_eq!(proven.settings, settings);
        assert_eq!(proven.backend_key, Some((3, 4)));
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn pool_key_is_nul_delimited_and_distinct() {
        assert_eq!(pool_key("n", "u", "d"), "n\0u\0d");
        assert_ne!(pool_key("n", "ud", ""), pool_key("n", "u", "d"));
    }

    #[tokio::test]
    async fn checkin_then_checkout_reuses_same_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 1000);
        let key = pool_key("127.0.0.1:5432", "bench", "benchdb");

        // Park a live connection, then check it back out.
        let s = live_stream(&listener).await;
        let parked_addr = s.local_addr().unwrap();
        assert!(pool.checkin(&key, s));
        assert_eq!(pool.idle_count(), 1);

        let got = pool
            .checkout(&key)
            .expect("a parked connection is reusable");
        assert_eq!(got.local_addr().unwrap(), parked_addr, "same socket reused");
        assert_eq!(pool.reuses(), 1);
        assert_eq!(pool.idle_count(), 0);

        // Empty pool → miss.
        assert!(pool.checkout(&key).is_none());
    }

    #[tokio::test]
    async fn distinct_identities_do_not_share() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 1000);
        let alice = pool_key("n", "alice", "db");
        let bob = pool_key("n", "bob", "db");

        pool.checkin(&alice, live_stream(&listener).await);
        // Bob must NOT see alice's connection.
        assert!(pool.checkout(&bob).is_none());
        assert!(pool.checkout(&alice).is_some());
    }

    #[tokio::test]
    async fn per_key_cap_sheds_excess() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(2, 1000);
        let key = pool_key("n", "u", "d");

        assert!(pool.checkin(&key, live_stream(&listener).await));
        assert!(pool.checkin(&key, live_stream(&listener).await));
        // Third exceeds the cap of 2 → refused (and dropped/closed).
        assert!(!pool.checkin(&key, live_stream(&listener).await));
        assert_eq!(pool.over_capacity(), 1);
        assert_eq!(pool.idle_count(), 2);
    }

    #[tokio::test]
    async fn checkout_evicts_a_closed_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 1000);
        let key = pool_key("n", "u", "d");

        // Park a connection, then close the server side so the parked socket is
        // dead.
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        pool.checkin(&key, client);
        drop(server); // peer closes
                      // Give the close a moment to propagate.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Checkout must not hand out the dead connection.
        assert!(pool.checkout(&key).is_none());
        assert_eq!(pool.stale_evicted(), 1);
    }

    #[tokio::test]
    async fn global_cap_sheds_across_distinct_identities() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Per-key cap is generous (10) but the GLOBAL cap is 2.
        let pool = BackendIdlePool::new(10, 2);
        // Three different identities, one connection each.
        assert!(pool.checkin(&pool_key("n", "a", "d"), live_stream(&listener).await));
        assert!(pool.checkin(&pool_key("n", "b", "d"), live_stream(&listener).await));
        // Third exceeds the global ceiling even though its per-key bucket is empty.
        assert!(!pool.checkin(&pool_key("n", "c", "d"), live_stream(&listener).await));
        assert_eq!(pool.idle_count(), 2);
        assert_eq!(pool.over_capacity(), 1);
    }

    #[tokio::test]
    async fn reaper_drops_aged_idle_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 100);
        let key = pool_key("n", "u", "d");
        pool.checkin(&key, live_stream(&listener).await);
        assert_eq!(pool.idle_count(), 1);

        // Nothing reaped while within the TTL.
        assert_eq!(pool.reap_idle(std::time::Duration::from_secs(60)), 0);
        assert_eq!(pool.idle_count(), 1);

        // Let it age, then reap with a tiny TTL.
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        assert_eq!(pool.reap_idle(std::time::Duration::from_millis(5)), 1);
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.reaped(), 1);
        // A subsequent checkout misses (it was reaped).
        assert!(pool.checkout(&key).is_none());
    }

    /// A parked connection opened `age` ago.
    fn aged(stream: TcpStream, age: Duration) -> PooledConn {
        PooledConn {
            opened_at: Instant::now() - age,
            ..PooledConn::fresh(stream)
        }
    }

    /// W02 max lifetime: a connection past it is never handed out (closed
    /// at checkout), and the reaper closes it even within the idle TTL and
    /// the min_idle floor; younger ones are untouched.
    #[tokio::test]
    async fn lifetime_closes_old_connections_at_checkout_and_in_the_reaper() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(8, 100);
        let key = pool_key("n", "u", "d");
        let life = Some(Duration::from_secs(60));
        let (old, _s1) = live_pair(&listener).await;
        let (young, _s2) = live_pair(&listener).await;
        let young_addr = young.local_addr().unwrap();
        assert!(pool.checkin_conn_capped(&key, aged(young, Duration::from_secs(1)), 8));
        // Newest last: the old one is handed out first, and refused.
        assert!(pool.checkin_conn_capped(&key, aged(old, Duration::from_secs(120)), 8));
        let got = pool
            .checkout_conn_with(
                &key,
                LeaseOptions {
                    probe: true,
                    max_lifetime: life,
                },
            )
            .expect("the young one");
        assert_eq!(got.stream.local_addr().unwrap(), young_addr);
        assert_eq!(pool.lifetime_expired(), 1);
        assert_eq!(pool.idle_count(), 0);
        // Without a lifetime the same old connection is fine.
        let (old, _s3) = live_pair(&listener).await;
        assert!(pool.checkin_conn_capped(&key, aged(old, Duration::from_secs(120)), 8));
        assert!(pool
            .checkout_conn_with(&key, LeaseOptions::PROBED)
            .is_some());

        // The reaper: the floor keeps idle connections, not old ones.
        let (old, _s4) = live_pair(&listener).await;
        let (young, _s5) = live_pair(&listener).await;
        assert!(pool.checkin_conn_capped(&key, aged(old, Duration::from_secs(120)), 8));
        assert!(pool.checkin_conn_capped(&key, aged(young, Duration::from_secs(1)), 8));
        assert_eq!(pool.reap(Some(Duration::from_secs(600)), life, 2, None), 1);
        assert_eq!(pool.lifetime_expired(), 2);
        assert_eq!(pool.reaped(), 0, "a lifetime close is not an idle reap");
        assert_eq!(pool.idle_count(), 1);
    }

    /// W02 min_idle: the reaper's idle TTL spares the newest `keep_newest`
    /// connections of each key; with no TTL and no lifetime it closes
    /// nothing.
    #[tokio::test]
    async fn reaper_keeps_the_newest_min_idle_of_each_key() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(8, 100);
        let (a, b) = (pool_key("n", "a", "d"), pool_key("n", "b", "d"));
        let mut halves = Vec::new();
        for key in [&a, &a, &a, &b] {
            let (c, s) = live_pair(&listener).await;
            halves.push(s);
            assert!(pool.checkin(key, c));
        }
        let newest_a = {
            let (c, s) = live_pair(&listener).await;
            halves.push(s);
            let addr = c.local_addr().unwrap();
            assert!(pool.checkin(&a, c));
            addr
        };
        assert_eq!(pool.reap(None, None, 0, None), 0, "nothing to reap by");
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(pool.reap(Some(Duration::from_millis(5)), None, 2, None), 2);
        assert_eq!(pool.idle_count_for(&a), 2);
        assert_eq!(pool.idle_count_for(&b), 1);
        assert_eq!(pool.idle_count_for_node("n"), 3);
        assert_eq!(pool.idle_count_for_node("m"), 0);
        let kept = pool.checkout(&a).unwrap();
        assert_eq!(kept.local_addr().unwrap(), newest_a, "the newest stayed");
        // Without a floor the TTL closes the rest.
        assert_eq!(pool.reap(Some(Duration::from_millis(5)), None, 0, None), 2);
        assert_eq!(pool.idle_count(), 0);
    }

    /// W02 min_connections: with a per-node budget the idle TTL closes a
    /// node's connections only while it stays at its floor; a node not
    /// listed has none.
    #[tokio::test]
    async fn reaper_keeps_each_nodes_min_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(8, 100);
        let mut halves = Vec::new();
        for key in [
            pool_key("n", "a", "d"),
            pool_key("n", "b", "d"),
            pool_key("n", "b", "d"),
            pool_key("m", "a", "d"),
        ] {
            let (c, s) = live_pair(&listener).await;
            halves.push(s);
            assert!(pool.checkin(&key, c));
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
        // `n` may lose one more connection; `m` has no floor.
        let mut closable = std::collections::HashMap::from([("n".to_string(), 1usize)]);
        let ttl = Some(Duration::from_millis(5));
        assert_eq!(pool.reap(ttl, None, 0, Some(&mut closable)), 2);
        assert_eq!(closable["n"], 0);
        assert_eq!(pool.idle_count_for_node("n"), 2);
        assert_eq!(pool.idle_count_for_node("m"), 0);
        assert_eq!(
            pool.reap(ttl, None, 0, Some(&mut closable)),
            0,
            "at the floor"
        );
        assert_eq!(pool.idle_count(), 2);
        assert_eq!(pool.reaped(), 2);
    }

    /// W02 test_on_acquire = false: a checkout without the probe hands out a
    /// connection whose peer closed while it was parked (the probe would have
    /// dropped it).
    #[tokio::test]
    async fn an_unprobed_checkout_skips_the_liveness_probe() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(4, 100);
        let key = pool_key("n", "u", "d");
        for probe in [true, false] {
            let (client, server) = live_pair(&listener).await;
            assert!(pool.checkin(&key, client));
            drop(server);
            tokio::time::sleep(Duration::from_millis(20)).await;
            let got = pool.checkout_conn_with(
                &key,
                LeaseOptions {
                    probe,
                    max_lifetime: None,
                },
            );
            assert_eq!(got.is_some(), !probe, "probe = {probe}");
        }
        assert_eq!(pool.stale_evicted(), 1);
    }

    /// A per-key idle cap below the global one (a tenant's `pool_max_idle`),
    /// and the eviction of one idle connection of a tenant by its key
    /// suffix.
    #[tokio::test]
    async fn per_key_idle_caps_and_tenant_eviction() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = BackendIdlePool::new(10, 100);
        let tenant = format!("{}\0t=acme", pool_key("n", "u", "d"));
        let other = pool_key("n", "u", "d");
        let mut halves = Vec::new();
        for expect in [true, true, false] {
            let (c, s) = live_pair(&listener).await;
            halves.push(s);
            assert_eq!(
                pool.checkin_conn_capped(&tenant, PooledConn::fresh(c), 2),
                expect
            );
        }
        let (c, s) = live_pair(&listener).await;
        halves.push(s);
        assert!(pool.checkin_conn_capped(&other, PooledConn::fresh(c), 10));
        assert_eq!(pool.over_capacity(), 1);
        assert!(pool.evict_one_idle_for_tenant("\0t=acme"));
        assert_eq!(pool.idle_count_for(&tenant), 1);
        assert_eq!(pool.idle_count_for(&other), 1, "other keys untouched");
        assert!(pool.evict_one_idle_for_tenant("\0t=acme"));
        assert!(!pool.evict_one_idle_for_tenant("\0t=acme"));
        assert!(!pool.evict_one_idle_for_tenant("\0t=globex"));
    }

    fn spec(node: &str) -> DialSpec {
        DialSpec {
            node: node.to_string(),
            startup: Arc::from(&b"startup"[..]),
            user: "u".to_string(),
            tenant_pool: None,
            max_idle: 4,
            last_used: Instant::now(),
            no_credential: false,
        }
    }

    /// The top-up specs: recorded once per key (later uses only refresh
    /// it), the newest per node found, expired after the TTL, never offered
    /// once marked as needing a credential, and bounded by the global idle
    /// cap.
    #[test]
    fn dial_specs_are_recorded_once_refreshed_and_expired() {
        let pool = BackendIdlePool::new(4, 2);
        let t0 = Instant::now() - Duration::from_secs(10);
        let mut built = 0;
        pool.note_use("a", t0, || {
            built += 1;
            spec("n1")
        });
        pool.note_use("a", Instant::now(), || {
            built += 1;
            spec("n1")
        });
        assert_eq!(built, 1, "the startup packet is built once");
        pool.note_use("b", t0, || spec("n1"));
        assert_eq!(pool.latest_spec_for("n1").unwrap().0, "a");
        assert!(pool.latest_spec_for("n2").is_none());
        // The table is full (2 = max_total_idle): the least recent goes.
        pool.note_use("c", Instant::now(), || spec("n2"));
        let mut keys: Vec<String> = pool.specs().into_iter().map(|(k, _)| k).collect();
        keys.sort();
        assert_eq!(keys, ["a", "c"]);
        pool.mark_no_credential("a");
        assert!(pool.latest_spec_for("n1").is_none());
        assert_eq!(pool.specs().len(), 1);
        pool.note_use("old", t0, || spec("n3"));
        pool.expire_specs(Some(Duration::from_secs(5)));
        assert!(pool.latest_spec_for("n3").is_none());
        assert!(pool.latest_spec_for("n2").is_some());
        pool.expire_specs(None);
        assert!(pool.latest_spec_for("n2").is_some());
    }
}

//! L3 Distributed Cache - peer cache mesh over TCP
//!
//! Features:
//! - Consistent hashing for key distribution
//! - Replication to the `replication_factor` owners of each key
//! - TCP-based peer-to-peer communication
//!
//! Peers are configured explicitly ([`DistributedCache::new`],
//! [`DistributedCache::add_peer`]); there is no peer discovery. Replication
//! and invalidation are sent to each peer and awaited: every call returns a
//! [`PeerOpReport`] naming the peers that acknowledged and the ones that did
//! not (with the reason), and failures are counted in
//! [`DistributedCache::replication_failures`] /
//! [`DistributedCache::invalidation_failures`]. A peer that missed an
//! invalidation can serve a stale entry until its TTL expires, which is why
//! those failures are surfaced rather than dropped.

use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{CacheEntry, TierStats};
use crate::distribcache::QueryFingerprint;

/// Cache protocol message types
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum MessageType {
    Get = 1,
    GetResponse = 2,
    Put = 3,
    PutResponse = 4,
    Invalidate = 5,
    Ping = 6,
    Pong = 7,
    InvalidateResponse = 8,
    InvalidateTable = 9,
}

/// `PutResponse` status: the entry was stored.
const PUT_STORED: u32 = 0;
/// `PutResponse` status: the payload did not decode; nothing was stored.
const PUT_REJECTED: u32 = 1;

/// Outcome of an operation sent to several peers (replication or
/// invalidation).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerOpReport {
    /// Peers that acknowledged the operation.
    pub acknowledged: Vec<SocketAddr>,
    /// Peers that did not, with the reason (unreachable, rejected, or
    /// marked unhealthy and therefore not attempted).
    pub failed: Vec<(SocketAddr, String)>,
}

impl PeerOpReport {
    /// `true` when every targeted peer acknowledged.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Outcome of [`DistributedCache::insert`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationReport {
    /// Whether this node is an owner of the key and stored it locally.
    pub stored_locally: bool,
    /// Per-peer outcome of replicating to the remote owners.
    pub peers: PeerOpReport,
}

impl ReplicationReport {
    /// Whether at least one owner (local or remote) holds the entry.
    pub fn stored_anywhere(&self) -> bool {
        self.stored_locally || !self.peers.acknowledged.is_empty()
    }
}

/// A mesh-wide invalidation to broadcast.
#[derive(Debug, Clone)]
enum Invalidation {
    Entry(QueryFingerprint),
    Table(String),
}

impl TryFrom<u8> for MessageType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MessageType::Get),
            2 => Ok(MessageType::GetResponse),
            3 => Ok(MessageType::Put),
            4 => Ok(MessageType::PutResponse),
            5 => Ok(MessageType::Invalidate),
            6 => Ok(MessageType::Ping),
            7 => Ok(MessageType::Pong),
            8 => Ok(MessageType::InvalidateResponse),
            9 => Ok(MessageType::InvalidateTable),
            _ => Err(()),
        }
    }
}

/// Peer identifier
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PeerId(pub u64);

impl PeerId {
    pub fn new(addr: &SocketAddr) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        addr.hash(&mut hasher);
        Self(hasher.finish())
    }

    pub fn local() -> Self {
        Self(0)
    }
}

/// Consistent hash ring for key distribution
struct HashRing {
    /// Ring nodes (virtual nodes -> peer)
    ring: BTreeMap<u64, PeerId>,
    /// Number of virtual nodes per peer
    virtual_nodes: usize,
}

impl HashRing {
    fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    fn add_peer(&mut self, peer: PeerId) {
        for i in 0..self.virtual_nodes {
            let hash = Self::hash_peer(peer, i);
            self.ring.insert(hash, peer);
        }
    }

    fn remove_peer(&mut self, peer: PeerId) {
        self.ring.retain(|_, p| *p != peer);
    }

    fn get_nodes(&self, key: &[u8], count: u32) -> Vec<PeerId> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let key_hash = Self::hash_key(key);
        let mut nodes = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Find first node >= key_hash
        let iter = self
            .ring
            .range(key_hash..)
            .chain(self.ring.range(..key_hash));

        for (_, peer) in iter {
            if !seen.contains(peer) {
                seen.insert(*peer);
                nodes.push(*peer);
                if nodes.len() >= count as usize {
                    break;
                }
            }
        }

        nodes
    }

    fn hash_peer(peer: PeerId, vnode: usize) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        peer.0.hash(&mut hasher);
        vnode.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_key(key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

/// Peer connection state
#[derive(Debug)]
pub struct PeerConnection {
    /// Peer address
    pub addr: SocketAddr,
    /// Connection healthy
    pub healthy: bool,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Round-trip time in microseconds
    pub rtt_us: u64,
    /// Connection timeout in milliseconds
    timeout_ms: u64,
}

impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            healthy: self.healthy,
            last_seen: self.last_seen,
            rtt_us: self.rtt_us,
            timeout_ms: self.timeout_ms,
        }
    }
}

impl PeerConnection {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            healthy: true,
            last_seen: 0,
            rtt_us: 0,
            timeout_ms: 5000, // 5 second timeout
        }
    }

    /// Get entry from peer via TCP
    pub async fn get(&self, fingerprint: &QueryFingerprint) -> Result<CacheEntry, &'static str> {
        let _start = std::time::Instant::now();

        // Try to connect with timeout
        let stream = match tokio::time::timeout(
            std::time::Duration::from_millis(self.timeout_ms),
            TcpStream::connect(self.addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(_)) => return Err("Connection failed"),
            Err(_) => return Err("Connection timeout"),
        };

        // Build request message
        let fp_bytes = match postcard::to_stdvec(fingerprint) {
            Ok(b) => b,
            Err(_) => return Err("Serialization failed"),
        };

        // Send GET request
        let (mut reader, mut writer) = stream.into_split();

        // Message format: [type: u8][length: u32][data: bytes]
        let mut header = vec![MessageType::Get as u8];
        header.extend_from_slice(&(fp_bytes.len() as u32).to_le_bytes());

        if writer.write_all(&header).await.is_err() {
            return Err("Failed to write header");
        }
        if writer.write_all(&fp_bytes).await.is_err() {
            return Err("Failed to write data");
        }

        // Read response
        let mut resp_header = [0u8; 5];
        if reader.read_exact(&mut resp_header).await.is_err() {
            return Err("Failed to read response header");
        }

        if !matches!(
            MessageType::try_from(resp_header[0]),
            Ok(MessageType::GetResponse)
        ) {
            return Err("Unexpected response type");
        }
        let length = u32::from_le_bytes([
            resp_header[1],
            resp_header[2],
            resp_header[3],
            resp_header[4],
        ]) as usize;

        if length == 0 {
            return Err("Entry not found");
        }

        let mut data = vec![0u8; length];
        if reader.read_exact(&mut data).await.is_err() {
            return Err("Failed to read response data");
        }

        // Deserialize entry
        let entry: CacheEntry =
            postcard::from_bytes(&data).map_err(|_| "Deserialization failed")?;

        Ok(entry)
    }

    /// Insert entry to peer via TCP
    pub async fn insert(
        &self,
        fingerprint: QueryFingerprint,
        entry: CacheEntry,
    ) -> Result<(), &'static str> {
        // Try to connect with timeout
        let stream = match tokio::time::timeout(
            std::time::Duration::from_millis(self.timeout_ms),
            TcpStream::connect(self.addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(_)) => return Err("Connection failed"),
            Err(_) => return Err("Connection timeout"),
        };

        // Serialize fingerprint and entry
        let fp_bytes = postcard::to_stdvec(&fingerprint).map_err(|_| "FP serialization failed")?;
        let entry_bytes = postcard::to_stdvec(&entry).map_err(|_| "Entry serialization failed")?;

        // Build message: [type: u8][fp_len: u32][entry_len: u32][fp_data][entry_data]
        let mut message = Vec::with_capacity(1 + 4 + 4 + fp_bytes.len() + entry_bytes.len());
        message.push(MessageType::Put as u8);
        message.extend_from_slice(&(fp_bytes.len() as u32).to_le_bytes());
        message.extend_from_slice(&(entry_bytes.len() as u32).to_le_bytes());
        message.extend_from_slice(&fp_bytes);
        message.extend_from_slice(&entry_bytes);

        let (mut reader, mut writer) = stream.into_split();

        if writer.write_all(&message).await.is_err() {
            return Err("Failed to write");
        }

        // Read the ack: [PutResponse][status: u32]
        match read_response(&mut reader, MessageType::PutResponse).await? {
            PUT_STORED => Ok(()),
            PUT_REJECTED => Err("Peer rejected the entry"),
            _ => Err("Unknown PUT status"),
        }
    }

    /// Ping peer to check health
    #[cfg(test)]
    pub async fn ping(&self) -> bool {
        let _start = std::time::Instant::now();

        let stream = match tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            TcpStream::connect(self.addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            _ => return false,
        };

        let (mut reader, mut writer) = stream.into_split();

        // Send ping
        let ping_msg = [MessageType::Ping as u8, 0, 0, 0, 0];
        if writer.write_all(&ping_msg).await.is_err() {
            return false;
        }

        // Wait for pong
        let mut resp = [0u8; 5];
        match tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            reader.read_exact(&mut resp),
        )
        .await
        {
            Ok(Ok(_)) => resp[0] == MessageType::Pong as u8,
            _ => false,
        }
    }

    /// Send an entry invalidation to the peer and wait for its ack.
    pub async fn invalidate(&self, fingerprint: &QueryFingerprint) -> Result<(), &'static str> {
        let fp_bytes = postcard::to_stdvec(fingerprint).map_err(|_| "Serialization failed")?;
        self.send_invalidation(MessageType::Invalidate, &fp_bytes)
            .await
            .map(|_| ())
    }

    /// Send a table invalidation to the peer and wait for its ack. Returns
    /// the number of entries the peer removed.
    pub async fn invalidate_table(&self, table: &str) -> Result<u32, &'static str> {
        self.send_invalidation(MessageType::InvalidateTable, table.as_bytes())
            .await
    }

    async fn send_invalidation(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<u32, &'static str> {
        let stream = match tokio::time::timeout(
            std::time::Duration::from_millis(self.timeout_ms),
            TcpStream::connect(self.addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(_)) => return Err("Connection failed"),
            Err(_) => return Err("Connection timeout"),
        };

        let mut message = vec![msg_type as u8];
        message.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        message.extend_from_slice(payload);

        let (mut reader, mut writer) = stream.into_split();
        writer
            .write_all(&message)
            .await
            .map_err(|_| "Write failed")?;

        tokio::time::timeout(
            std::time::Duration::from_millis(self.timeout_ms),
            read_response(&mut reader, MessageType::InvalidateResponse),
        )
        .await
        .map_err(|_| "Ack timeout")?
    }
}

/// Read a `[type: u8][value: u32]` response and check its type.
async fn read_response(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    expected: MessageType,
) -> Result<u32, &'static str> {
    let mut header = [0u8; 5];
    if reader.read_exact(&mut header).await.is_err() {
        return Err("Failed to read ack");
    }
    if header[0] != expected as u8 {
        return Err("Unexpected response type");
    }
    Ok(u32::from_le_bytes([
        header[1], header[2], header[3], header[4],
    ]))
}

/// L3 Distributed Cache - Cache mesh with consistent hashing
pub struct DistributedCache {
    /// Local peer ID
    local_peer_id: PeerId,

    /// Consistent hash ring
    hash_ring: std::sync::RwLock<HashRing>,

    /// Peer connections
    peers: DashMap<PeerId, PeerConnection>,

    /// Local cache for owned keys
    local: DashMap<u64, CacheEntry>,

    /// Replication factor
    replication_factor: u32,

    /// Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    remote_hits: AtomicU64,
    healthy_peers: AtomicU32,
    /// Owner replications that were not acknowledged
    replication_failures: AtomicU64,
    /// Peer invalidations that were not acknowledged
    invalidation_failures: AtomicU64,
}

impl DistributedCache {
    /// Create a new distributed cache
    pub fn new(replication_factor: u32, peer_addrs: Vec<SocketAddr>) -> Self {
        let local_peer_id = PeerId::local();

        let mut hash_ring = HashRing::new(100); // 100 virtual nodes per peer
        hash_ring.add_peer(local_peer_id);

        let peers = DashMap::new();
        for addr in &peer_addrs {
            let peer_id = PeerId::new(addr);
            hash_ring.add_peer(peer_id);
            peers.insert(peer_id, PeerConnection::new(*addr));
        }

        Self {
            local_peer_id,
            hash_ring: std::sync::RwLock::new(hash_ring),
            peers,
            local: DashMap::new(),
            replication_factor,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            remote_hits: AtomicU64::new(0),
            healthy_peers: AtomicU32::new(peer_addrs.len() as u32),
            replication_failures: AtomicU64::new(0),
            invalidation_failures: AtomicU64::new(0),
        }
    }

    /// Get an entry from the distributed cache
    pub async fn get(&self, fingerprint: &QueryFingerprint) -> Option<CacheEntry> {
        let key = self.fingerprint_to_hash(fingerprint);
        let key_bytes = key.to_le_bytes();

        // Determine owners
        let owners = {
            let ring = self.hash_ring.read().ok()?;
            ring.get_nodes(&key_bytes, self.replication_factor)
        };

        // Check local first if we own it
        if owners.contains(&self.local_peer_id) {
            if let Some(entry) = self.local.get(&key) {
                if !entry.is_expired() {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(entry.clone());
                } else {
                    drop(entry);
                    self.local.remove(&key);
                }
            }
        }

        // Query remote peers
        for owner in owners {
            if owner == self.local_peer_id {
                continue;
            }

            let peer = self.peers.get(&owner).map(|p| p.value().clone());
            if let Some(peer) = peer {
                if peer.healthy {
                    if let Ok(entry) = peer.get(fingerprint).await {
                        // Cache locally
                        self.local.insert(key, entry.clone());
                        self.remote_hits.fetch_add(1, Ordering::Relaxed);
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Some(entry);
                    }
                }
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert an entry into the distributed cache.
    ///
    /// Stores it locally when this node is one of the key's owners and sends
    /// it to every remote owner, awaiting each acknowledgement. The report
    /// lists the remote owners that stored it and the ones that did not; an
    /// owner marked unhealthy is reported as failed without being contacted.
    /// Each failure also increments [`Self::replication_failures`].
    pub async fn insert(
        &self,
        fingerprint: QueryFingerprint,
        entry: CacheEntry,
    ) -> ReplicationReport {
        let key = self.fingerprint_to_hash(&fingerprint);
        let key_bytes = key.to_le_bytes();

        // Determine owners
        let owners = {
            let ring = self
                .hash_ring
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ring.get_nodes(&key_bytes, self.replication_factor)
        };

        // Insert locally if we own it
        let stored_locally = owners.contains(&self.local_peer_id);
        if stored_locally {
            self.local.insert(key, entry.clone());
        }

        let mut report = PeerOpReport::default();
        for owner in owners {
            if owner == self.local_peer_id {
                continue;
            }
            let Some(peer) = self.peers.get(&owner).map(|p| p.value().clone()) else {
                continue;
            };
            if !peer.healthy {
                report.failed.push((
                    peer.addr,
                    "peer marked unhealthy; replication not attempted".to_string(),
                ));
                continue;
            }
            match peer.insert(fingerprint.clone(), entry.clone()).await {
                Ok(()) => report.acknowledged.push(peer.addr),
                Err(e) => report.failed.push((peer.addr, e.to_string())),
            }
        }

        if !report.failed.is_empty() {
            self.replication_failures
                .fetch_add(report.failed.len() as u64, Ordering::Relaxed);
            tracing::warn!(
                failed = ?report.failed,
                "L3 replication incomplete"
            );
        }
        ReplicationReport {
            stored_locally,
            peers: report,
        }
    }

    /// Owner replications that were not acknowledged since creation.
    pub fn replication_failures(&self) -> u64 {
        self.replication_failures.load(Ordering::Relaxed)
    }

    /// Peer invalidations that were not acknowledged since creation.
    pub fn invalidation_failures(&self) -> u64 {
        self.invalidation_failures.load(Ordering::Relaxed)
    }

    /// Add a peer to the cache mesh
    pub fn add_peer(&self, addr: SocketAddr) {
        let peer_id = PeerId::new(&addr);

        if let Ok(mut ring) = self.hash_ring.write() {
            ring.add_peer(peer_id);
        }

        self.peers.insert(peer_id, PeerConnection::new(addr));
        self.healthy_peers.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a peer from the cache mesh
    pub fn remove_peer(&self, addr: &SocketAddr) {
        let peer_id = PeerId::new(addr);

        if let Ok(mut ring) = self.hash_ring.write() {
            ring.remove_peer(peer_id);
        }

        // Only a healthy peer is counted in `healthy_peers`.
        if let Some((_, peer)) = self.peers.remove(&peer_id) {
            if peer.healthy {
                self.healthy_peers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Mark peer as unhealthy
    pub fn mark_unhealthy(&self, addr: &SocketAddr) {
        let peer_id = PeerId::new(addr);

        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            if peer.healthy {
                peer.healthy = false;
                self.healthy_peers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Mark peer as healthy
    pub fn mark_healthy(&self, addr: &SocketAddr) {
        let peer_id = PeerId::new(addr);

        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            if !peer.healthy {
                peer.healthy = true;
                self.healthy_peers.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Invalidate an entry across the mesh.
    ///
    /// Removes it locally and sends the invalidation to every peer
    /// concurrently, awaiting each acknowledgement. See [`PeerOpReport`].
    pub async fn invalidate(&self, fingerprint: &QueryFingerprint) -> PeerOpReport {
        let key = self.fingerprint_to_hash(fingerprint);
        self.local.remove(&key);
        self.broadcast_invalidation(Invalidation::Entry(fingerprint.clone()))
            .await
    }

    /// Invalidate every entry that reads `table` (ASCII case-insensitive),
    /// locally and on every peer, awaiting each acknowledgement.
    pub async fn invalidate_table(&self, table: &str) -> PeerOpReport {
        self.remove_local_table(table);
        self.broadcast_invalidation(Invalidation::Table(table.to_string()))
            .await
    }

    /// Remove local entries that read `table`; returns how many.
    fn remove_local_table(&self, table: &str) -> u32 {
        let before = self.local.len();
        self.local
            .retain(|_, entry| !entry.tables.iter().any(|t| t.eq_ignore_ascii_case(table)));
        before.saturating_sub(self.local.len()) as u32
    }

    async fn broadcast_invalidation(&self, invalidation: Invalidation) -> PeerOpReport {
        let mut report = PeerOpReport::default();
        let mut sends = tokio::task::JoinSet::new();
        let peers: Vec<PeerConnection> = self.peers.iter().map(|p| p.value().clone()).collect();
        for peer in peers {
            if !peer.healthy {
                report.failed.push((
                    peer.addr,
                    "peer marked unhealthy; invalidation not attempted".to_string(),
                ));
                continue;
            }
            let invalidation = invalidation.clone();
            sends.spawn(async move {
                let result = match &invalidation {
                    Invalidation::Entry(fp) => peer.invalidate(fp).await,
                    Invalidation::Table(table) => peer.invalidate_table(table).await.map(|_| ()),
                };
                (peer.addr, result)
            });
        }
        while let Some(joined) = sends.join_next().await {
            match joined {
                Ok((addr, Ok(()))) => report.acknowledged.push(addr),
                Ok((addr, Err(e))) => report.failed.push((addr, e.to_string())),
                Err(e) => tracing::error!(error = %e, "L3 invalidation task failed"),
            }
        }

        if !report.failed.is_empty() {
            self.invalidation_failures
                .fetch_add(report.failed.len() as u64, Ordering::Relaxed);
            tracing::warn!(
                failed = ?report.failed,
                "L3 invalidation incomplete: peers that missed it may serve stale entries \
                 until their TTL expires"
            );
        }
        report
    }

    /// Convert fingerprint to hash key
    fn fingerprint_to_hash(&self, fingerprint: &QueryFingerprint) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        fingerprint.template.hash(&mut hasher);
        if let Some(param) = fingerprint.param_hash {
            param.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Get cache statistics
    pub fn stats(&self) -> TierStats {
        let local_size: usize = self.local.iter().map(|e| e.value().size()).sum();

        TierStats {
            size_bytes: local_size as u64,
            max_size_bytes: 0, // Distributed, no single max
            entry_count: self.local.len() as u64,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: 0,
            compression_ratio: None,
            peer_count: Some(self.peers.len() as u32 + 1), // +1 for local
            healthy_peers: Some(self.healthy_peers.load(Ordering::Relaxed) + 1),
        }
    }

    /// Get peer addresses
    pub fn peer_addrs(&self) -> Vec<SocketAddr> {
        self.peers.iter().map(|p| p.value().addr).collect()
    }

    /// Copy valid entries to another cache (for branch merging)
    pub fn copy_valid_entries_to(&self, target: &DistributedCache) {
        for entry in self.local.iter() {
            if !entry.value().is_expired() {
                target.local.insert(*entry.key(), entry.value().clone());
            }
        }
    }

    /// Run the peer server on `addr`: accept connections from other mesh nodes
    /// and serve the GET / PUT / PING / INVALIDATE wire protocol against this
    /// node's owned-key map. This is the server counterpart to
    /// [`PeerConnection`] — without it, replication PUTs and remote GETs from
    /// peers have nothing to answer them, so the L3 mesh cannot actually
    /// replicate or serve cross-node reads. Returns when binding fails; loops
    /// forever otherwise (spawn it as a background task).
    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_on(listener).await;
        Ok(())
    }

    /// Like [`serve`](Self::serve) but takes an already-bound listener — used by
    /// tests that bind `127.0.0.1:0` and need the OS-assigned port first.
    pub async fn serve_on(self: Arc<Self>, listener: TcpListener) {
        while let Ok((stream, _peer)) = listener.accept().await {
            let cache = Arc::clone(&self);
            tokio::spawn(async move {
                let _ = cache.handle_peer_conn(stream).await;
            });
        }
    }

    /// Handle one inbound peer connection: read the framed request and reply
    /// with the matching response, mirroring [`PeerConnection`]'s framing.
    async fn handle_peer_conn(&self, stream: TcpStream) -> std::io::Result<()> {
        let (mut reader, mut writer) = stream.into_split();

        let mut type_byte = [0u8; 1];
        if reader.read_exact(&mut type_byte).await.is_err() {
            return Ok(());
        }
        let msg_type = match MessageType::try_from(type_byte[0]) {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };

        match msg_type {
            MessageType::Get => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf).await?;
                let fp_len = u32::from_le_bytes(len_buf) as usize;
                let mut fp_bytes = vec![0u8; fp_len];
                reader.read_exact(&mut fp_bytes).await?;

                // Look up the requested fingerprint in the local owned map.
                let payload = match postcard::from_bytes::<QueryFingerprint>(&fp_bytes) {
                    Ok(fp) => {
                        let key = self.fingerprint_to_hash(&fp);
                        self.local.get(&key).and_then(|e| {
                            if e.is_expired() {
                                None
                            } else {
                                postcard::to_stdvec(e.value()).ok()
                            }
                        })
                    }
                    Err(_) => None,
                };

                let mut out = vec![MessageType::GetResponse as u8];
                match payload {
                    // length 0 signals "not found" to the client.
                    Some(bytes) => {
                        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        out.extend_from_slice(&bytes);
                    }
                    None => out.extend_from_slice(&0u32.to_le_bytes()),
                }
                writer.write_all(&out).await?;
            }
            MessageType::Put => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf).await?;
                let fp_len = u32::from_le_bytes(len_buf) as usize;
                reader.read_exact(&mut len_buf).await?;
                let entry_len = u32::from_le_bytes(len_buf) as usize;

                let mut fp_bytes = vec![0u8; fp_len];
                reader.read_exact(&mut fp_bytes).await?;
                let mut entry_bytes = vec![0u8; entry_len];
                reader.read_exact(&mut entry_bytes).await?;

                let status = match (
                    postcard::from_bytes::<QueryFingerprint>(&fp_bytes),
                    postcard::from_bytes::<CacheEntry>(&entry_bytes),
                ) {
                    (Ok(fp), Ok(entry)) => {
                        let key = self.fingerprint_to_hash(&fp);
                        self.local.insert(key, entry);
                        PUT_STORED
                    }
                    _ => PUT_REJECTED,
                };

                // [PutResponse][status: u32]
                let mut out = vec![MessageType::PutResponse as u8];
                out.extend_from_slice(&status.to_le_bytes());
                writer.write_all(&out).await?;
            }
            MessageType::Ping => {
                let mut len_buf = [0u8; 4];
                let _ = reader.read_exact(&mut len_buf).await;
                let mut out = vec![MessageType::Pong as u8];
                out.extend_from_slice(&0u32.to_le_bytes());
                writer.write_all(&out).await?;
            }
            MessageType::Invalidate => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf).await?;
                let fp_len = u32::from_le_bytes(len_buf) as usize;
                let mut fp_bytes = vec![0u8; fp_len];
                reader.read_exact(&mut fp_bytes).await?;
                // An undecodable fingerprint is not acknowledged: the
                // sender reports the invalidation as failed.
                let fp = match postcard::from_bytes::<QueryFingerprint>(&fp_bytes) {
                    Ok(fp) => fp,
                    Err(_) => return Ok(()),
                };
                let key = self.fingerprint_to_hash(&fp);
                let removed = u32::from(self.local.remove(&key).is_some());
                let mut out = vec![MessageType::InvalidateResponse as u8];
                out.extend_from_slice(&removed.to_le_bytes());
                writer.write_all(&out).await?;
            }
            MessageType::InvalidateTable => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf).await?;
                let name_len = u32::from_le_bytes(len_buf) as usize;
                let mut name = vec![0u8; name_len];
                reader.read_exact(&mut name).await?;
                let table = match String::from_utf8(name) {
                    Ok(t) => t,
                    Err(_) => return Ok(()),
                };
                let removed = self.remove_local_table(&table);
                let mut out = vec![MessageType::InvalidateResponse as u8];
                out.extend_from_slice(&removed.to_le_bytes());
                writer.write_all(&out).await?;
            }
            // Response-type frames are never received server-side.
            MessageType::GetResponse
            | MessageType::PutResponse
            | MessageType::Pong
            | MessageType::InvalidateResponse => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_hash_ring_distribution() {
        let mut ring = HashRing::new(10);

        let peer1 = PeerId(1);
        let peer2 = PeerId(2);
        let peer3 = PeerId(3);

        ring.add_peer(peer1);
        ring.add_peer(peer2);
        ring.add_peer(peer3);

        // Test key distribution
        let key1 = b"test-key-1";
        let key2 = b"test-key-2";
        let key3 = b"test-key-3";

        let nodes1 = ring.get_nodes(key1, 2);
        let nodes2 = ring.get_nodes(key2, 2);
        let nodes3 = ring.get_nodes(key3, 2);

        // Each should return 2 nodes
        assert_eq!(nodes1.len(), 2);
        assert_eq!(nodes2.len(), 2);
        assert_eq!(nodes3.len(), 2);
    }

    #[test]
    fn test_hash_ring_replication() {
        let mut ring = HashRing::new(10);

        let peer1 = PeerId(1);
        let peer2 = PeerId(2);

        ring.add_peer(peer1);
        ring.add_peer(peer2);

        let key = b"replicated-key";
        let nodes = ring.get_nodes(key, 2);

        // Should return both peers
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&peer1));
        assert!(nodes.contains(&peer2));
    }

    #[tokio::test]
    async fn test_distributed_cache_local_insert_get() {
        let cache = DistributedCache::new(1, Vec::new());

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let entry = CacheEntry::new(vec![1, 2, 3], vec!["users".to_string()], 1)
            .with_ttl(Duration::from_secs(300));

        let report = cache.insert(fp.clone(), entry).await;
        assert!(report.stored_locally);
        assert!(report.peers.is_complete());

        let result = cache.get(&fp).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3]);
    }

    #[test]
    fn test_distributed_cache_peer_management() {
        let cache = DistributedCache::new(2, Vec::new());

        let addr1: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9101".parse().unwrap();

        cache.add_peer(addr1);
        cache.add_peer(addr2);

        assert_eq!(cache.stats().peer_count, Some(3)); // 2 remote + 1 local

        cache.mark_unhealthy(&addr1);
        assert_eq!(cache.stats().healthy_peers, Some(2)); // 1 remote + 1 local

        cache.remove_peer(&addr1);
        assert_eq!(cache.stats().peer_count, Some(2)); // 1 remote + 1 local
                                                       // Removing an already-unhealthy peer must not decrement again.
        assert_eq!(cache.stats().healthy_peers, Some(2));
    }

    #[tokio::test]
    async fn test_distributed_cache_stats() {
        let cache = DistributedCache::new(1, Vec::new());

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");

        cache
            .insert(
                fp1.clone(),
                CacheEntry::new(vec![1], vec![], 1).with_ttl(Duration::from_secs(300)),
            )
            .await;

        cache.get(&fp1).await; // Hit
        cache.get(&fp2).await; // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entry_count, 1);
    }

    // End-to-end proof that the peer server actually answers the wire protocol:
    // a PeerConnection client PUTs an entry over real TCP, the server stores it
    // in its local map, and a subsequent GET round-trips the same bytes back.
    // Before the server existed, replication PUTs reached nothing and remote
    // GETs always failed to connect.
    #[tokio::test]
    async fn test_peer_server_put_get_roundtrip() {
        // Bind the server to an OS-assigned port and start serving.
        let server = Arc::new(DistributedCache::new(1, Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(Arc::clone(&server).serve_on(listener));

        let client = PeerConnection::new(addr);
        let fp = QueryFingerprint::from_query("SELECT * FROM accounts WHERE id = $1");
        let entry = CacheEntry::new(vec![9, 8, 7], vec!["accounts".to_string()], 1)
            .with_ttl(Duration::from_secs(300));

        // PUT over TCP — must be acked, and must land in the server's local map.
        client.insert(fp.clone(), entry.clone()).await.unwrap();
        let key = server.fingerprint_to_hash(&fp);
        assert!(
            server.local.contains_key(&key),
            "PUT did not land on server"
        );

        // GET over TCP — must return the same bytes the client stored.
        let got = client.get(&fp).await.expect("remote GET failed");
        assert_eq!(got.data, vec![9, 8, 7]);

        // PING must get a Pong.
        assert!(client.ping().await, "peer did not answer ping");

        // INVALIDATE must remove it server-side (acknowledged before the
        // call returns); the next GET then misses.
        client.invalidate(&fp).await.unwrap();
        assert!(
            !server.local.contains_key(&key),
            "INVALIDATE did not remove entry"
        );
        assert!(
            client.get(&fp).await.is_err(),
            "GET should miss after invalidate"
        );
    }

    /// Replication to an owner that is down is reported per owner and
    /// counted, not dropped.
    #[tokio::test]
    async fn test_replication_failure_is_reported_and_counted() {
        // A port with nothing listening: connect is refused.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);

        // Replication factor 2 with one remote peer: that peer owns every key.
        let cache = DistributedCache::new(2, vec![dead_addr]);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let entry = CacheEntry::new(vec![1], vec!["users".to_string()], 1)
            .with_ttl(Duration::from_secs(60));

        let report = cache.insert(fp, entry).await.peers;
        assert!(!report.is_complete());
        assert!(report.acknowledged.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].0, dead_addr);
        assert_eq!(cache.replication_failures(), 1);

        // An owner marked unhealthy is reported as not attempted.
        cache.mark_unhealthy(&dead_addr);
        let report = cache
            .insert(
                QueryFingerprint::from_query("SELECT * FROM orders"),
                CacheEntry::new(vec![2], vec![], 1),
            )
            .await
            .peers;
        assert!(report.failed[0].1.contains("not attempted"), "{report:?}");
        assert_eq!(cache.replication_failures(), 2);
    }

    /// Replication to a live owner is acknowledged and lands on the peer.
    #[tokio::test]
    async fn test_replication_to_live_peer_is_acknowledged() {
        let server = Arc::new(DistributedCache::new(1, Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(Arc::clone(&server).serve_on(listener));

        let cache = DistributedCache::new(2, vec![addr]);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let report = cache
            .insert(
                fp.clone(),
                CacheEntry::new(vec![5], vec!["users".to_string()], 1),
            )
            .await
            .peers;
        assert_eq!(report.acknowledged, vec![addr]);
        assert!(report.is_complete());
        assert_eq!(cache.replication_failures(), 0);
        assert!(server.local.contains_key(&server.fingerprint_to_hash(&fp)));
    }

    /// A PUT whose payload the peer cannot decode is rejected, not acked.
    #[tokio::test]
    async fn test_peer_rejects_undecodable_put() {
        let server = Arc::new(DistributedCache::new(1, Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(Arc::clone(&server).serve_on(listener));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut msg = vec![MessageType::Put as u8];
        msg.extend_from_slice(&2u32.to_le_bytes());
        msg.extend_from_slice(&2u32.to_le_bytes());
        msg.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        stream.write_all(&msg).await.unwrap();
        let mut resp = [0u8; 5];
        stream.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp[0], MessageType::PutResponse as u8);
        assert_eq!(
            u32::from_le_bytes([resp[1], resp[2], resp[3], resp[4]]),
            PUT_REJECTED
        );
        assert!(server.local.is_empty());
    }

    /// Table invalidation reaches peers, is acknowledged, and an unreachable
    /// peer is reported and counted.
    #[tokio::test]
    async fn test_invalidate_table_across_mesh_reports_each_peer() {
        let server = Arc::new(DistributedCache::new(1, Vec::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live = listener.local_addr().unwrap();
        tokio::spawn(Arc::clone(&server).serve_on(listener));
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);

        // Seed the live peer with a "users" entry and an "orders" entry.
        let client = PeerConnection::new(live);
        let users = QueryFingerprint::from_query("SELECT * FROM users");
        let orders = QueryFingerprint::from_query("SELECT * FROM orders");
        client
            .insert(
                users.clone(),
                CacheEntry::new(vec![1], vec!["users".to_string()], 1),
            )
            .await
            .unwrap();
        client
            .insert(
                orders.clone(),
                CacheEntry::new(vec![2], vec!["orders".to_string()], 1),
            )
            .await
            .unwrap();

        let cache = DistributedCache::new(1, vec![live, dead_addr]);
        let report = cache.invalidate_table("USERS").await;
        assert_eq!(report.acknowledged, vec![live]);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].0, dead_addr);
        assert_eq!(cache.invalidation_failures(), 1);

        assert!(!server
            .local
            .contains_key(&server.fingerprint_to_hash(&users)));
        assert!(server
            .local
            .contains_key(&server.fingerprint_to_hash(&orders)));
    }
}

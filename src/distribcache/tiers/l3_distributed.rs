//! L3 Distributed Cache - Cache mesh with <10ms access time
//!
//! Features:
//! - Consistent hashing for key distribution
//! - Replication for availability
//! - TCP-based peer-to-peer communication
//! - Gossip protocol for peer discovery (planned)

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
        let fp_bytes = match bincode::serialize(fingerprint) {
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

        let _msg_type =
            MessageType::try_from(resp_header[0]).map_err(|_| "Invalid message type")?;
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
            bincode::deserialize(&data).map_err(|_| "Deserialization failed")?;

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
        let fp_bytes = bincode::serialize(&fingerprint).map_err(|_| "FP serialization failed")?;
        let entry_bytes = bincode::serialize(&entry).map_err(|_| "Entry serialization failed")?;

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

        // Read response (ack)
        let mut resp_header = [0u8; 5];
        if reader.read_exact(&mut resp_header).await.is_err() {
            return Err("Failed to read ack");
        }

        Ok(())
    }

    /// Ping peer to check health
    #[allow(dead_code)]
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

    /// Send invalidation message to peer
    pub async fn invalidate(&self, fingerprint: &QueryFingerprint) -> Result<(), &'static str> {
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

        let fp_bytes = bincode::serialize(fingerprint).map_err(|_| "Serialization failed")?;

        let mut message = vec![MessageType::Invalidate as u8];
        message.extend_from_slice(&(fp_bytes.len() as u32).to_le_bytes());
        message.extend_from_slice(&fp_bytes);

        let (_, mut writer) = stream.into_split();
        writer
            .write_all(&message)
            .await
            .map_err(|_| "Write failed")?;

        Ok(())
    }
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
    #[allow(dead_code)]
    replication_lag_ms: AtomicU64,
    healthy_peers: AtomicU32,
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
            replication_lag_ms: AtomicU64::new(0),
            healthy_peers: AtomicU32::new(peer_addrs.len() as u32),
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

            if let Some(peer) = self.peers.get(&owner) {
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

    /// Insert an entry into the distributed cache
    pub async fn insert(&self, fingerprint: QueryFingerprint, entry: CacheEntry) {
        let key = self.fingerprint_to_hash(&fingerprint);
        let key_bytes = key.to_le_bytes();

        // Determine owners
        let owners = {
            let ring = self.hash_ring.read().unwrap();
            ring.get_nodes(&key_bytes, self.replication_factor)
        };

        // Insert locally if we own it
        if owners.contains(&self.local_peer_id) {
            self.local.insert(key, entry.clone());
        }

        // Replicate to other owners (fire and forget for now)
        for owner in owners {
            if owner == self.local_peer_id {
                continue;
            }

            if let Some(peer) = self.peers.get(&owner) {
                if peer.healthy {
                    let fp = fingerprint.clone();
                    let e = entry.clone();
                    let _ = peer.insert(fp, e).await;
                }
            }
        }
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

        if self.peers.remove(&peer_id).is_some() {
            self.healthy_peers.fetch_sub(1, Ordering::Relaxed);
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

    /// Invalidate an entry across the mesh
    pub async fn invalidate(&self, fingerprint: &QueryFingerprint) {
        let key = self.fingerprint_to_hash(fingerprint);

        // Remove locally
        self.local.remove(&key);

        // Broadcast invalidation to all healthy peers
        for peer_ref in self.peers.iter() {
            let peer = peer_ref.value();
            if peer.healthy {
                // Fire and forget - don't wait for ack
                let fp = fingerprint.clone();
                let peer_clone = peer.clone();
                tokio::spawn(async move {
                    let _ = peer_clone.invalidate(&fp).await;
                });
            }
        }
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
                let payload = match bincode::deserialize::<QueryFingerprint>(&fp_bytes) {
                    Ok(fp) => {
                        let key = self.fingerprint_to_hash(&fp);
                        self.local.get(&key).and_then(|e| {
                            if e.is_expired() {
                                None
                            } else {
                                bincode::serialize(e.value()).ok()
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

                if let (Ok(fp), Ok(entry)) = (
                    bincode::deserialize::<QueryFingerprint>(&fp_bytes),
                    bincode::deserialize::<CacheEntry>(&entry_bytes),
                ) {
                    let key = self.fingerprint_to_hash(&fp);
                    self.local.insert(key, entry);
                }

                // Acknowledge (the client reads a 5-byte response header).
                let mut out = vec![MessageType::PutResponse as u8];
                out.extend_from_slice(&0u32.to_le_bytes());
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
                if let Ok(fp) = bincode::deserialize::<QueryFingerprint>(&fp_bytes) {
                    let key = self.fingerprint_to_hash(&fp);
                    self.local.remove(&key);
                }
                // Invalidate is fire-and-forget; no response expected.
            }
            // Response-type frames are never received server-side.
            MessageType::GetResponse | MessageType::PutResponse | MessageType::Pong => {}
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

        cache.insert(fp.clone(), entry).await;

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
        assert!(server.local.contains_key(&key), "PUT did not land on server");

        // GET over TCP — must return the same bytes the client stored.
        let got = client.get(&fp).await.expect("remote GET failed");
        assert_eq!(got.data, vec![9, 8, 7]);

        // PING must get a Pong.
        assert!(client.ping().await, "peer did not answer ping");

        // INVALIDATE must remove it server-side; the next GET then misses.
        client.invalidate(&fp).await.unwrap();
        // brief await so the fire-and-forget invalidate is processed
        for _ in 0..50 {
            if !server.local.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !server.local.contains_key(&key),
            "INVALIDATE did not remove entry"
        );
        assert!(client.get(&fp).await.is_err(), "GET should miss after invalidate");
    }
}

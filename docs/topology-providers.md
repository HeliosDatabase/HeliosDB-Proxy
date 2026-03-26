# HeliosProxy Topology Providers

HeliosProxy discovers and tracks the current primary node through a pluggable topology provider abstraction. This document describes the available providers, their configuration, and failover detection behavior.

---

## Overview

The `PrimaryTracker` is the component responsible for knowing which backend node is the current primary at all times. It consumes topology change events and exposes the current primary to the load balancer, failover controller, and switchover buffer.

Three topology provider implementations are available:

| Provider | Feature Flag | Discovery Method | Latency |
|----------|-------------|------------------|---------|
| PostgreSQL | `postgres-topology` | Polls `pg_is_in_recovery()` on each node | Polling interval (default 2s) |
| HeliosDB | `heliosdb-topology` | Subscribes to internal topology events | Event-driven (near-instant) |
| Manual / API | *(none -- always available)* | Explicit `set_primary()` / `clear_primary()` calls | Depends on external orchestration |

If no topology provider is configured, the proxy starts in **standalone mode** and expects primary management through the Admin API or direct programmatic calls.

---

## PostgreSQL Provider (`postgres-topology`)

The PostgreSQL topology provider discovers the primary by polling the built-in `pg_is_in_recovery()` function on every configured node. The node that returns `false` is the primary; all nodes returning `true` are standbys or replicas.

### How It Works

```
  +-----------+     pg_is_in_recovery()     +-----------+
  | HeliosProxy| ────────────────────────> | Node A     |
  | Topology   |      returns: false        | (Primary)  |
  | Poller     |                            +-----------+
  |            |     pg_is_in_recovery()     +-----------+
  |            | ────────────────────────> | Node B     |
  |            |      returns: true          | (Standby)  |
  |            |                            +-----------+
  |            |     pg_is_in_recovery()     +-----------+
  |            | ────────────────────────> | Node C     |
  |            |      returns: true          | (Replica)  |
  +-----------+                            +-----------+
```

On each polling interval:

1. The provider connects to every configured node (or reuses an existing connection).
2. Executes `SELECT pg_is_in_recovery()`.
3. The node returning `false` is identified as the primary.
4. If the primary has changed since the last poll, a `TopologyEvent::PrimaryChanged` event is broadcast.
5. If a node fails to respond, a `TopologyEvent::HealthChanged` event is broadcast.

### Configuration

The PostgreSQL provider is configured through the standard `[[nodes]]` entries in `config.toml`. Each node requires connection credentials for the polling queries.

```toml
# Enable postgres-topology at build time:
# cargo build --release --features "postgres-topology"

[[nodes]]
host = "pg-primary.internal"
port = 5432
role = "primary"
name = "pg-primary"

[[nodes]]
host = "pg-standby-1.internal"
port = 5432
role = "standby"
name = "pg-standby-1"

[[nodes]]
host = "pg-standby-2.internal"
port = 5432
role = "standby"
name = "pg-standby-2"
```

### Polling Interval

The default polling interval is **2 seconds**. This means failover detection latency is at most 2 seconds (one polling cycle) plus the health check `failure_threshold` cycles.

The polling interval can be configured programmatically:

```rust
use heliosdb_proxy::primary_tracker::PostgresTopologyProvider;
use std::time::Duration;

let provider = PostgresTopologyProvider::new(nodes)
    .with_poll_interval(Duration::from_secs(1));  // Poll every second
```

### Compatible HA Solutions

The PostgreSQL provider works with any HA solution that uses standard PostgreSQL streaming replication, because the detection relies solely on `pg_is_in_recovery()`:

| HA Solution | Compatibility | Notes |
|-------------|--------------|-------|
| Native streaming replication | Fully compatible | Standard `pg_basebackup` + streaming replication. |
| Patroni | Fully compatible | Patroni manages promotion; the proxy detects the new primary via polling. |
| pg_auto_failover | Fully compatible | Citus-managed automatic failover. |
| Stolon | Fully compatible | Cloud-native PostgreSQL HA. |
| repmgr | Fully compatible | Community replication manager. |
| AWS RDS | Fully compatible | Managed PostgreSQL. The proxy polls read replicas to distinguish them from the primary. |
| AWS Aurora | Fully compatible | Aurora PostgreSQL-compatible edition. |
| Google Cloud SQL | Fully compatible | Managed PostgreSQL with read replicas. |
| Azure Database for PostgreSQL | Fully compatible | Managed PostgreSQL with read replicas. |

### Failover Detection Sequence

When a PostgreSQL primary fails and a standby is promoted:

```
Time 0s    Primary fails (stops responding to pg_is_in_recovery)
           |
Time 0-2s  Next polling cycle detects failure
           TopologyEvent::HealthChanged { node_id: old_primary, is_healthy: false }
           |
Time 2-4s  Standby is promoted by external HA manager (Patroni, etc.)
           Promoted standby returns pg_is_in_recovery() = false
           |
Time 4-6s  Next polling cycle detects new primary
           TopologyEvent::PrimaryChanged { old_primary, new_primary }
           |
Time 6s    PrimaryTracker updates, switchover buffer drains,
           queries resume on new primary
```

Total failover detection time: **2-6 seconds** with default settings (2s poll interval).

---

## HeliosDB Provider (`heliosdb-topology`)

The HeliosDB topology provider bridges the proxy into the HeliosDB internal replication system. Instead of polling, it subscribes to the `TopologyManager` event stream for instant, zero-latency failover detection.

### How It Works

```
  +-----------+     TopologyEvent stream     +-----------+
  | HeliosProxy| <────────────────────────── | HeliosDB   |
  | Topology   |  PrimaryChanged event       | Topology   |
  | Subscriber |                             | Manager    |
  +-----------+                              +-----------+
```

The HeliosDB provider implements the `TopologyProvider` trait through a bridge interface (`HeliosTopologyBridge`). This avoids a hard compile-time dependency on the HeliosDB replication crate, allowing the standalone proxy to compile without the HeliosDB workspace.

### Architecture

```rust
// Bridge trait -- implemented by the HeliosDB replication crate
pub trait HeliosTopologyBridge: Send + Sync + 'static {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;
    fn get_primary(&self) -> Option<TopologyNodeInfo>;
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;
}

// Wrapper that adapts the bridge to TopologyProvider
pub struct HeliosTopologyProvider<T: HeliosTopologyBridge> {
    inner: Arc<T>,
}
```

### Configuration

The HeliosDB provider requires no additional configuration beyond the `heliosdb-topology` feature flag. It is initialized programmatically when the proxy is started within the HeliosDB workspace.

```bash
# Build within the HeliosDB workspace:
cargo build --release --features "heliosdb-topology"
```

### Failover Detection

Failover detection is event-driven. When the HeliosDB replication subsystem detects a primary change (through its internal consensus mechanism), it immediately emits a `TopologyEvent::PrimaryChanged` event. The proxy processes this event within milliseconds.

```
Time 0ms    Primary fails
            |
Time ~100ms HeliosDB replication detects failure and promotes standby
            TopologyEvent::PrimaryChanged emitted
            |
Time ~101ms PrimaryTracker updates, queries resume on new primary
```

Total failover detection time: **sub-second** (limited only by the HeliosDB replication protocol).

---

## Manual / Standalone Provider

When no topology feature flag is enabled, the proxy starts in standalone mode. The primary is managed through explicit API calls.

### How It Works

```
  +-----------+     set_primary()     +-----------+
  | External  | ──────────────────> | HeliosProxy|
  | Manager   |     Admin API        | Primary    |
  | (Patroni, |     or programmatic  | Tracker    |
  |  scripts) |                      +-----------+
  +-----------+
```

### Programmatic API

```rust
use heliosdb_proxy::primary_tracker::PrimaryTracker;

// Create a standalone tracker
let tracker = PrimaryTracker::new_standalone();

// Set the primary (e.g., after discovering it externally)
let node_id = uuid::Uuid::new_v4();
tracker.set_primary(node_id, "pg-primary.local:5432".to_string());

// Confirm the primary (after verifying it is accepting writes)
tracker.confirm_primary();

// Handle failover -- clear the old primary
tracker.clear_primary();

// Set the new primary
let new_node_id = uuid::Uuid::new_v4();
tracker.set_primary(new_node_id, "pg-standby.local:5432".to_string());
tracker.confirm_primary();
```

### Event Subscription

Regardless of the provider mode, components can subscribe to primary change events:

```rust
let mut rx = tracker.subscribe();

loop {
    match rx.recv().await {
        Ok(PrimaryChangeEvent::Changed { old, new, address }) => {
            println!("Primary changed: {:?} -> {} at {}", old, new, address);
        }
        Ok(PrimaryChangeEvent::Lost { old }) => {
            println!("Primary lost: {}", old);
        }
        Ok(PrimaryChangeEvent::Confirmed { node_id }) => {
            println!("Primary confirmed: {}", node_id);
        }
        Err(_) => break,
    }
}
```

### Use Cases for Standalone Mode

| Use Case | Integration Approach |
|----------|---------------------|
| External failover manager (Patroni, pg_auto_failover) | The manager calls the proxy Admin API to update the primary after promotion. |
| Manual failover scripts | Operator runs `curl -X POST .../nodes/{addr}/enable` after promoting a standby. |
| Custom orchestration | Application code calls `PrimaryTracker::set_primary()` directly. |
| Testing and development | Primary is set at startup and remains static. |

---

## Primary Lifecycle

All three providers follow the same primary lifecycle:

```
                              set_primary()
  ┌──────┐                     ┌─────────┐
  │ NONE │ ──────────────────> │ PENDING │
  └──────┘                     └────┬────┘
      ^                             │
      │    clear_primary()          │ confirm_primary()
      │                             v
      │                        ┌──────────┐
      └─────────────────────── │CONFIRMED │
         node lost / unhealthy └──────────┘
```

| State | Meaning |
|-------|---------|
| NONE | No primary is known. Write queries are buffered (up to `write_timeout_secs`) or rejected. |
| PENDING | A primary has been identified but not yet confirmed. The proxy begins routing writes to the pending primary. |
| CONFIRMED | The primary is verified and accepting writes. Normal operation. |

### Topology Events

| Event | Trigger |
|-------|---------|
| `PrimaryChanged { old, new }` | The primary role moved from one node to another. |
| `NodeLeft { node_id }` | A node is no longer reachable and has been removed from the topology. |
| `HealthChanged { node_id, is_healthy }` | A node's health status changed. |

### Primary Change Events (Internal)

| Event | Trigger |
|-------|---------|
| `Changed { old, new, address }` | A new primary has been set (either via provider event or manual call). |
| `Lost { old }` | The current primary was cleared (node failure, manual clear). |
| `Confirmed { node_id }` | The pending primary has been confirmed. |

---

## Custom Topology Providers

The `TopologyProvider` trait can be implemented for any custom topology source. This enables integration with proprietary HA solutions, service meshes, or cloud-native orchestration systems.

### Trait Definition

```rust
pub trait TopologyProvider: Send + Sync + 'static {
    /// Subscribe to topology change events.
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;

    /// Get the current primary node, if one exists.
    fn get_primary(&self) -> Option<TopologyNodeInfo>;

    /// Look up a node by its UUID.
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;
}
```

### Example: Consul-Based Provider

```rust
struct ConsulTopologyProvider {
    consul_url: String,
    service_name: String,
    event_tx: broadcast::Sender<TopologyEvent>,
    primary: RwLock<Option<TopologyNodeInfo>>,
}

impl TopologyProvider for ConsulTopologyProvider {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
        self.event_tx.subscribe()
    }

    fn get_primary(&self) -> Option<TopologyNodeInfo> {
        self.primary.read().clone()
    }

    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
        self.primary.read()
            .as_ref()
            .filter(|n| n.node_id == id)
            .cloned()
    }
}
```

Register the custom provider with the primary tracker:

```rust
let provider = Arc::new(ConsulTopologyProvider::new(
    "http://consul:8500",
    "postgresql",
));

let tracker = PrimaryTracker::with_provider(provider);
```

---

## Provider Comparison

| Capability | PostgreSQL | HeliosDB | Manual |
|-----------|-----------|----------|--------|
| Automatic detection | Yes (polling) | Yes (event-driven) | No |
| Detection latency | 2-6 seconds | Sub-second | Depends on external |
| External dependencies | None | HeliosDB workspace | External manager |
| Configuration complexity | Low | None (automatic) | Lowest |
| Feature flag required | `postgres-topology` | `heliosdb-topology` | None |
| Suitable for production | Yes | Yes | Yes (with external manager) |

---

## See Also

- [Architecture](architecture.md) -- System overview and module map
- [Configuration Reference](configuration.md) -- Node configuration details
- [Deployment Guides](deployment/) -- Standalone, Docker, and Kubernetes deployment

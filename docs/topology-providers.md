# HeliosProxy Topology Providers

HeliosProxy needs to know which backend node is the current primary so it can route
writes correctly and buffer them across a failover. There are two distinct layers to this,
and this document keeps them separate because they behave differently:

1. **The standalone daemon** determines the primary from static `[[nodes]]` roles plus
   live health checks, and reports it at the admin `/topology` endpoint.
2. **The `TopologyProvider` library abstraction** (`PrimaryTracker` plus pluggable
   providers) is a programmatic/embedded interface — used by unit tests and by the
   HeliosDB-workspace build — for automatic, event-driven primary tracking.

Every concrete claim below is grounded in `src/primary_tracker.rs`,
`src/admin.rs` (`compute_topology`, `TopologyResponse`), and the node/config types in
`src/config.rs`. Where the library abstraction is *not* wired into the shipped daemon, the
document says so.

**Last verified against commit `9c5ff9b`.**

---

## Layer 1 — How the Standalone Daemon Tracks the Primary

In the running `heliosdb-proxy` daemon, the primary is **not** discovered by polling
`pg_is_in_recovery()`. It is the configured `[[nodes]]` entry whose `role = "primary"`,
that is `enabled`, and whose health check is currently passing.

### Determining the current primary

The write path (`select_primary_with_timeout` in `src/server.rs`) looks for
`n.role == NodeRole::Primary && n.enabled` and a passing health entry. If that node is
healthy, writes go to it. If it is not, the proxy buffers the write, polling health every
100 ms for up to `write_timeout_secs` (default 30) for a healthy primary to appear; on
timeout it increments the `failovers` metric and returns `NoHealthyNodes`. (See
[transaction-replay.md](transaction-replay.md#what-happens-on-the-live-write-path).)

The admin view (`compute_topology` in `src/admin.rs`) uses the same rule: `currentPrimary`
is the address of the first node with `role = "primary"` (case-insensitive) whose health
entry is `healthy = true`. `None` is the correct answer while a failover is in progress and
no primary-role node is healthy.

### The `/topology` response

`GET /topology` returns `TopologyResponse` (camelCase to map cleanly into the Kubernetes
operator CRD status):

| Field | Meaning |
|-------|---------|
| `currentPrimary` | Address of the first healthy `primary`-role node, or `null`. |
| `healthyNodes` | Count of nodes with a passing health check. |
| `unhealthyNodes` | Count of nodes with a failing health check. |
| `totalNodes` | Number of configured `[[nodes]]`. |
| `lastFailoverAt` | RFC 3339 timestamp of the last observed primary change; `null` when none has been observed since boot (currently always `null` in `compute_topology`). |

### Node configuration and manual control

Nodes are declared with `[[nodes]]` entries (`role = "primary" | "standby" | "replica"`,
plus `host`/`port`/`name`/`enabled`). See [configuration.md](configuration.md) for the
full node schema.

Because the daemon derives the primary from role + health, primary changes are driven by:

- **Health checks** flipping a node's `healthy` flag (a failing primary drops out;
  `currentPrimary` becomes `null` until a `primary`-role node is healthy again).
- **`POST /nodes/{addr}/enable` / `/disable`** — operator control over which nodes are
  in rotation.
- **`POST /api/chaos`** — force a node unhealthy (or restore it) to exercise the failover
  path without external tooling.

To promote a standby in this model, an external HA manager (Patroni, pg_auto_failover,
etc.) performs the promotion, and the proxy's `[[nodes]]` roles are updated (config reload
via SIGHUP) or the failed primary node recovers under the same address.

---

## Layer 2 — The `TopologyProvider` Library Abstraction

`src/primary_tracker.rs` defines a provider abstraction for **automatic** primary
tracking. These types are compiled into the crate and covered by unit tests, and are the
intended integration surface for embedding the proxy or building it inside the HeliosDB
workspace. As of `9c5ff9b` they are **not** instantiated by the standalone daemon's
forwarding loop — `PrimaryTracker`, `PostgresTopologyProvider`, and the HeliosDB bridge
appear in the runtime only through the crate's test suite.

### The `PrimaryTracker`

`PrimaryTracker` holds an optional `Arc<dyn TopologyProvider>` and the current
`PrimaryInfo` (`node_id`, `address`, `became_primary_at`, `is_confirmed`). It can run in
three modes:

1. **Provider-backed** — `PrimaryTracker::with_provider(provider)`; `run()` subscribes to
   the provider's event stream and updates on each `TopologyEvent`.
2. **Standalone** — `PrimaryTracker::new_standalone()`; the primary is set/cleared
   explicitly via `set_primary` / `confirm_primary` / `clear_primary`.
3. **PostgreSQL** — pass a `PostgresTopologyProvider` (feature `postgres-topology`) to
   `with_provider`.

### The `TopologyProvider` trait

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

`TopologyNodeInfo` carries `node_id: Uuid`, `client_addr: String`, and `is_healthy: bool`.

### Provider overview

| Provider | Feature Flag | Discovery Method | Latency |
|----------|-------------|------------------|---------|
| PostgreSQL | `postgres-topology` | Polls `pg_is_in_recovery()` on each node | Poll interval (default 2 s) |
| HeliosDB | `heliosdb-topology` | Subscribes to the internal `TopologyManager` event stream | Event-driven |
| Manual / standalone | *(none — always available)* | Explicit `set_primary()` / `clear_primary()` | Depends on the caller |

---

## PostgreSQL Provider (`postgres-topology`)

`PostgresTopologyProvider` (feature `postgres-topology`) discovers the primary by polling
`SELECT pg_is_in_recovery()` on each configured node: the node returning `false` is the
primary; those returning `true` are standbys/replicas.

### How it works

`poll_nodes` runs each poll interval:

1. `probe_recovery` opens a `BackendClient` to each node and runs
   `SELECT pg_is_in_recovery()`.
2. The first node reporting `false` becomes the candidate primary. (Taking the *first*
   keeps the choice deterministic if a brief split-brain shows two.)
3. If the primary UUID changed since the previous poll, a
   `TopologyEvent::PrimaryChanged { old_primary, new_primary }` is broadcast.
4. If a probe errors, `TopologyEvent::HealthChanged { node_id, is_healthy: false }` is
   broadcast for that node.

### Construction (programmatic)

The provider is constructed from an explicit `Vec<PostgresNode>`, **not** from `proxy.toml`
`[[nodes]]` — there is no config wiring that builds a `PostgresTopologyProvider` in the
daemon. `PostgresNode` carries `node_id`, `host`, `port`, `user`, `password`, `database`.

```rust
use heliosdb_proxy::primary_tracker::{PostgresNode, PostgresTopologyProvider};
use std::time::Duration;

let provider = PostgresTopologyProvider::new(nodes)   // Vec<PostgresNode>
    .with_poll_interval(Duration::from_secs(1))       // default is 2s
    .with_tls_mode(heliosdb_proxy::backend::TlsMode::Prefer);
```

Probe connections are built with a rustls client config from the Mozilla root set;
`with_tls_mode` sets the TLS policy (default `Prefer`), `connect_timeout` is the poll
interval capped at 5 s, and `application_name` is `helios-topology`.

### Default polling interval

The default poll interval is **2 seconds** (`Duration::from_secs(2)`), tunable with
`with_poll_interval`. Detection latency is therefore roughly one poll cycle for a lost
primary, plus another cycle after an external HA manager promotes a standby to `false`.

### Compatible HA solutions

Because detection relies solely on `pg_is_in_recovery()`, the provider works with any HA
solution built on standard PostgreSQL streaming replication — the external manager performs
promotion; the provider detects the resulting change. This includes native streaming
replication, Patroni, pg_auto_failover, Stolon, repmgr, and managed offerings (AWS
RDS/Aurora, Google Cloud SQL, Azure Database for PostgreSQL) that expose replicas answering
`pg_is_in_recovery()`.

---

## HeliosDB Provider (`heliosdb-topology`)

`HeliosTopologyProvider<T>` (feature `heliosdb-topology`, in the `heliosdb_provider`
module) bridges the proxy into HeliosDB's internal replication `TopologyManager` instead of
polling. It is defined behind a bridge trait so the standalone proxy can compile without a
hard dependency on the replication crate:

```rust
// Implemented by the HeliosDB replication crate.
pub trait HeliosTopologyBridge: Send + Sync + 'static {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;
    fn get_primary(&self) -> Option<TopologyNodeInfo>;
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;
}

// Adapts the bridge to `TopologyProvider`.
pub struct HeliosTopologyProvider<T: HeliosTopologyBridge> {
    inner: Arc<T>,
}
```

Detection is event-driven: when the replication subsystem promotes a standby it emits
`TopologyEvent::PrimaryChanged`, which `PrimaryTracker::run` applies immediately. This
provider requires only the feature flag; it is initialized programmatically when the proxy
is built within the HeliosDB workspace.

---

## Manual / Standalone Tracking

With no provider, `PrimaryTracker::new_standalone()` tracks a primary set entirely through
explicit calls:

```rust
use heliosdb_proxy::primary_tracker::PrimaryTracker;

let tracker = PrimaryTracker::new_standalone();

tracker.set_primary(node_id, "pg-primary.local:5432".to_string()); // is_confirmed = false
tracker.confirm_primary();                                          // is_confirmed = true

// On failover:
tracker.clear_primary();                                           // emits Lost
tracker.set_primary(new_node_id, "pg-standby.local:5432".to_string());
tracker.confirm_primary();
```

This is the mode an external orchestrator would drive: promote out of band, then tell the
tracker the new primary's address.

---

## Primary Lifecycle

`PrimaryInfo::is_confirmed` encodes a two-phase promotion so writes can begin against a
pending primary before it is fully verified:

```
                  set_primary()          confirm_primary()
   ┌──────┐  ─────────────────────▶ ┌─────────┐ ───────────────▶ ┌───────────┐
   │ none │                         │ pending │                  │ confirmed │
   └──────┘  ◀───────────────────── └─────────┘ ◀─────────────── └───────────┘
                 clear_primary()  (primary lost / node unhealthy)
```

| State | `PrimaryInfo` | Meaning |
|-------|---------------|---------|
| none | `None` | No primary known. |
| pending | `Some { is_confirmed: false }` | Primary set (e.g. mid-switchover), not yet verified. |
| confirmed | `Some { is_confirmed: true }` | Primary verified and serving. |

### Events

**`TopologyEvent`** (provider → tracker):

| Event | Trigger |
|-------|---------|
| `PrimaryChanged { old_primary, new_primary }` | The primary role moved between nodes. |
| `NodeLeft { node_id }` | A node left the cluster. |
| `HealthChanged { node_id, is_healthy }` | A node's health status changed. |

**`PrimaryChangeEvent`** (tracker → subscribers, via `PrimaryTracker::subscribe`):

| Event | Trigger |
|-------|---------|
| `Changed { old, new, address }` | A new primary was set (provider event or manual call). |
| `Lost { old }` | The current primary was cleared. |
| `Confirmed { node_id }` | The pending primary was confirmed. |

---

## Custom Topology Providers

Any custom HA source can implement `TopologyProvider` and be handed to
`PrimaryTracker::with_provider`. The trait is exactly the three methods above, so a
Consul/Patroni/service-mesh integration only needs to translate its own leader-election
signal into `TopologyEvent`s and answer `get_primary` / `get_node`:

```rust
struct ConsulTopologyProvider {
    event_tx: broadcast::Sender<TopologyEvent>,
    primary: RwLock<Option<TopologyNodeInfo>>,
    /* consul client, service name, … */
}

impl TopologyProvider for ConsulTopologyProvider {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
        self.event_tx.subscribe()
    }
    fn get_primary(&self) -> Option<TopologyNodeInfo> {
        self.primary.read().clone()
    }
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
        self.primary.read().as_ref().filter(|n| n.node_id == id).cloned()
    }
}

let tracker = PrimaryTracker::with_provider(Arc::new(consul_provider));
```

The crate's own tests cover a mock provider and a `PatroniProvider`-shaped custom
implementation, demonstrating the same pattern.

---

## Provider Comparison

| Capability | PostgreSQL | HeliosDB | Manual |
|-----------|-----------|----------|--------|
| Automatic detection | Yes (polling) | Yes (event-driven) | No |
| Detection cadence | Poll interval (default 2 s) | Event-driven | Caller-driven |
| External dependencies | Reachable PG nodes | HeliosDB workspace | External manager |
| Feature flag | `postgres-topology` | `heliosdb-topology` | None |
| Wired into standalone daemon | No (programmatic) | No (workspace build) | No (programmatic) |

> The standalone daemon's own primary tracking (Layer 1) is independent of these
> providers: it uses static `[[nodes]]` roles + health checks and reports through
> `/topology`.

---

## See Also

- [Configuration Reference](configuration.md) — `[[nodes]]` schema and `write_timeout_secs`.
- [Transaction Replay](transaction-replay.md) — how the primary is used on the write path.
- [Admin API Reference](admin-api.md) — `/topology`, `/nodes/{addr}/enable|disable`, `/api/chaos`.
- [Architecture](architecture.md) — system overview and module map.
</content>

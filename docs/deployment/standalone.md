# Standalone Deployment

This guide covers installing and running HeliosProxy as a standalone binary on a Linux server.

---

## Prerequisites

- Linux (x86_64 or aarch64), macOS, or Windows
- Rust 1.86+ (for building from source; not needed for the prebuilt binaries)
- One or more PostgreSQL-compatible backends

---

## Prebuilt Binaries and crates.io

Each [GitHub release](https://github.com/HeliosDatabase/HeliosDB-Proxy/releases) carries
`heliosdb-proxy-{linux,macos}-{x86_64,aarch64}.tar.gz` with a `.sha256` file, built with
`all-features,postgres-topology`:

```bash
curl -LO https://github.com/HeliosDatabase/HeliosDB-Proxy/releases/download/v3.0.0/heliosdb-proxy-linux-x86_64.tar.gz
curl -LO https://github.com/HeliosDatabase/HeliosDB-Proxy/releases/download/v3.0.0/heliosdb-proxy-linux-x86_64.tar.gz.sha256
sha256sum -c heliosdb-proxy-linux-x86_64.tar.gz.sha256
tar xzf heliosdb-proxy-linux-x86_64.tar.gz
sudo install -m 0755 heliosdb-proxy-linux-x86_64 /usr/local/bin/heliosdb-proxy
```

Or from crates.io (this also installs the `helios-plugin` CLI):

```bash
cargo install heliosdb-proxy --version 3.0.0 --locked --features "all-features,postgres-topology"
```

## Building from Source

### Default Build (Connection Pooling Only)

```bash
git clone https://github.com/HeliosDatabase/HeliosDB-Proxy.git
cd HeliosDB-Proxy
cargo build --release
```

The binary is written to `target/release/heliosdb-proxy`.

### Production Build (All Features + PostgreSQL Topology)

```bash
cargo build --release --features "all-features,postgres-topology"
```

(`observability` is a reserved no-op; adding it changes nothing.)

### Lightweight HA Build

```bash
cargo build --release --features "pool-modes,ha-tr,postgres-topology"
```

See [Feature Flags](../feature-flags.md) for all available build options.

---

## Installation

### Copy the Binary

```bash
sudo install -m 0755 target/release/heliosdb-proxy /usr/local/bin/heliosdb-proxy
```

### Verify Installation

```bash
heliosdb-proxy --version
```

---

## Configuration File

Create a configuration directory and file:

```bash
sudo mkdir -p /etc/heliosproxy
```

Create `/etc/heliosproxy/config.toml`:

```toml
listen_address = "0.0.0.0:6432"
admin_address = "0.0.0.0:9090"
# Required: a non-loopback admin bind without a token is refused at startup.
admin_token = "replace-with-a-secret"
tr_enabled = true
tr_mode = "session"
write_timeout_secs = 30

[pool_mode]
mode = "transaction"
max_pool_size = 100
min_idle = 10
idle_timeout_secs = 600
max_lifetime_secs = 3600
acquire_timeout_secs = 5
reset_query = "DISCARD ALL"
prepared_statement_mode = "track"

[pool]
min_connections = 5
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 30
test_on_acquire = true

[load_balancer]
read_strategy = "least_connections"
read_write_split = true
latency_threshold_ms = 100

[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"

[[nodes]]
host = "db-primary.internal"
port = 5432
role = "primary"
weight = 100
enabled = true
name = "primary"

[[nodes]]
host = "db-standby-1.internal"
port = 5432
role = "standby"
weight = 100
enabled = true
name = "standby-1"
```

Set appropriate file permissions:

```bash
sudo chmod 640 /etc/heliosproxy/config.toml
sudo chown root:heliosproxy /etc/heliosproxy/config.toml
```

---

## Running Manually

### With Configuration File

```bash
heliosdb-proxy --config /etc/heliosproxy/config.toml
```

### With Command-Line Arguments

```bash
heliosdb-proxy \
  --listen 0.0.0.0:6432 \
  --admin 0.0.0.0:9090 \
  --primary db-primary:5432 \
  --standby db-standby-1:5432 \
  --standby db-standby-2:5432 \
  --log-level info
```

### With Debug Logging

```bash
heliosdb-proxy --config /etc/heliosproxy/config.toml --log-level debug
```

### With JSON Structured Logging

```bash
heliosdb-proxy --config /etc/heliosproxy/config.toml --json-logs
```

### With Environment Variable Log Control

```bash
RUST_LOG=heliosdb_proxy=debug heliosdb-proxy --config /etc/heliosproxy/config.toml
```

---

## Systemd Service

### Create a Service User

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin heliosproxy
```

### Create the Unit File

Create `/etc/systemd/system/heliosproxy.service`:

```ini
[Unit]
Description=HeliosProxy - Intelligent Database Connection Router
Documentation=https://github.com/HeliosDatabase/HeliosDB-Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=heliosproxy
Group=heliosproxy

ExecStart=/usr/local/bin/heliosdb-proxy --config /etc/heliosproxy/config.toml
ExecReload=/bin/kill -HUP $MAINPID

Restart=always
RestartSec=5
TimeoutStartSec=30
# SIGTERM (systemd's default stop signal) drains for up to
# shutdown_drain_timeout_secs (default 60): keep this longer than that.
TimeoutStopSec=75

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
ReadOnlyPaths=/etc/heliosproxy

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=heliosproxy

# Environment
Environment="RUST_LOG=heliosdb_proxy=info"

[Install]
WantedBy=multi-user.target
```

### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable heliosproxy
sudo systemctl start heliosproxy
```

### Verify Status

```bash
sudo systemctl status heliosproxy
```

### View Logs

```bash
# Follow logs
sudo journalctl -u heliosproxy -f

# View last 100 lines
sudo journalctl -u heliosproxy -n 100

# View logs since last boot
sudo journalctl -u heliosproxy -b
```

---

## File Layout

After installation, the recommended file layout is:

```
/usr/local/bin/heliosdb-proxy        # Binary
/etc/heliosproxy/config.toml         # Configuration
/etc/heliosproxy/server.crt          # TLS certificate (optional)
/etc/heliosproxy/server.key          # TLS private key (optional)
/etc/heliosproxy/ca.crt              # CA certificate (optional)
/etc/systemd/system/heliosproxy.service  # Systemd unit
```

---

## Health Verification

After starting the proxy, verify it is healthy:

```bash
# Check liveness
curl http://localhost:9090/health

# Check readiness (backend connectivity)
curl http://localhost:9090/health/ready

# View backend node status
curl http://localhost:9090/nodes | jq .

# Test a PostgreSQL connection through the proxy
psql -h localhost -p 6432 -U myuser -d mydb -c "SELECT 1"
```

---

## Upgrades

To upgrade HeliosProxy to a new version:

```bash
# 1. Build the new version
cd HeliosDB-Proxy
git pull
cargo build --release --features "all-features,postgres-topology"

# 2. Stop the service
sudo systemctl stop heliosproxy

# 3. Replace the binary
sudo install -m 0755 target/release/heliosdb-proxy /usr/local/bin/heliosdb-proxy

# 4. Start the service
sudo systemctl start heliosproxy

# 5. Verify
curl http://localhost:9090/version
curl http://localhost:9090/health/ready
```

`systemctl stop` sends SIGTERM: the proxy stops accepting, `/health/ready` answers 503,
and each session is ended with FATAL `57P01` once nothing of it is in flight (idle
sessions at once), so clients reconnect without losing a statement; the drain lasts at
most `shutdown_drain_timeout_secs`. Clients cannot connect between stop and start. For a
zero-downtime upgrade, start the new binary on the same addresses first and send
`SIGUSR2` to the old process, which drains the same way while the new one accepts (see
[Signals](../configuration.md#signals)); or run several instances behind a TCP load
balancer and restart them one at a time.

---

## Firewall Configuration

The proxy requires the following ports:

| Port | Protocol | Purpose | Exposure |
|------|----------|---------|----------|
| 6432 (configurable) | TCP | PostgreSQL client connections | Clients / application servers |
| 9090 (configurable) | TCP | Admin API, health checks, metrics | Monitoring systems, operators |

Example `firewalld` configuration:

```bash
sudo firewall-cmd --permanent --add-port=6432/tcp
sudo firewall-cmd --permanent --add-port=9090/tcp
sudo firewall-cmd --reload
```

Example `iptables` configuration:

```bash
sudo iptables -A INPUT -p tcp --dport 6432 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j ACCEPT
```

---

## See Also

- [Docker Deployment](docker.md)
- [Kubernetes Deployment](kubernetes.md)
- [Configuration Reference](../configuration.md)

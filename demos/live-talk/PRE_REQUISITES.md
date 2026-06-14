# Live Talk Demo Prerequisites

The demo kit does not install system dependencies. It prepares
`~/HDB/Proxy-Demogrounds`, copies scripts/assets, and uses Docker images for
PostgreSQL client fallbacks, but the host still needs the tools below.

## Required Host Tools

- Docker Engine with Docker Compose v2: `docker compose version`
- Git with SSH access to `git@github.com:HeliosDatabase/HeliosDB-Proxy.git`
- `curl` and `jq` for admin API calls and JSON formatting
- `tmux` for the side-by-side presentation launcher
- `ss` from `iproute2` for port collision checks; without it, checks are best effort
- `setsid` from `util-linux` so Nano daemon mode survives noninteractive launch

Recommended quick check:

```bash
for c in docker git curl jq tmux ss setsid; do command -v "$c" || echo "missing: $c"; done
docker compose version
```

## Repository Layout

Expected local paths:

```text
~/HDB/Proxy                 HeliosProxy repo
~/HDB/Nano                  HeliosDB-Nano repo or binary source
~/HDB/Proxy-Demogrounds     Generated local demo state
```

Run setup from the proxy repo:

```bash
cd ~/HDB/Proxy
./demos/live-talk/bin/prepare-demoground.sh
```

## HeliosDB-Nano Binary

The OLTP comparison expects an executable Nano binary at:

```bash
~/HDB/Nano/target/release/heliosdb-nano
```

If your binary is elsewhere, edit `~/HDB/Proxy-Demogrounds/.env`:

```bash
NANO_BIN=/absolute/path/to/heliosdb-nano
```

The demo scripts do not build Nano automatically because `/home/gpc/HDB/Nano`
is treated as a canonical checkout. Build or download Nano separately before
the talk.

## PostgreSQL Client Tools

Host `psql` and `pgbench` are optional on Linux. If absent, the scripts run
`postgres:16-alpine` client containers with `--network host`.

For macOS or environments where Docker host networking is unavailable, install
PostgreSQL client tools locally and confirm:

```bash
psql --version
pgbench --version
```

## Docker Images and Network

The demos may pull or build these images:

- `postgres:14-alpine`, `postgres:16-alpine`, `postgres:17-alpine`
- `ghcr.io/heliosdatabase/hdb-heliosdb-proxy:*` for existing v0.4 demos
- A local HeliosProxy image built from `docker/Dockerfile` for integration demos

Have network access and enough local Docker resources. Practical minimum:
4 CPU cores, 8 GB RAM, and 10 GB free disk.

## Ports

Default ports must be free unless changed in `~/HDB/Proxy-Demogrounds/.env`:

| Purpose | Port |
|---|---:|
| Side-by-side PostgreSQL | `55432` |
| Side-by-side Nano PG wire | `16432` |
| Side-by-side Nano HTTP | `18180` |
| Side-by-side Nano replication | `19432` |
| Proxy PG wire for v0.4 demos | `6432` |
| Proxy admin API | `9090` |
| Upgrade matrix proxy | `59001` |
| Upgrade matrix admin | `59002` |
| Upgrade matrix PG 14/15/16/17 | `55014`-`55017` |

Check active listeners:

```bash
ss -ltn | grep -E ':(55432|16432|18180|19432|6432|9090|59001|59002|55014|55015|55016|55017)\b'
```

## WASM Plugin Tour

The `plugins` command is a talk-track tour by default. Running plugin demos
11-18 requires either prebuilt `.wasm` files in each demo's `plugins/`
directory or the companion plugin workspace plus Rust WASM target:

```bash
rustup target add wasm32-unknown-unknown
```

Some plugin-signature walkthroughs also use `openssl`.

## Presentation Preflight

```bash
cd ~/HDB/Proxy
./demos/live-talk/bin/prepare-demoground.sh
~/HDB/Proxy-Demogrounds/bin/oltp-race.sh up
~/HDB/Proxy-Demogrounds/bin/oltp-race.sh init
DURATION=5 CLIENTS=2 JOBS=2 NANO_CLIENTS=1 NANO_JOBS=1 \
  ~/HDB/Proxy-Demogrounds/bin/oltp-race.sh run
```

If Nano is not reachable after setup, confirm `setsid` is installed and restart
local services with:

```bash
~/HDB/Proxy-Demogrounds/bin/oltp-race.sh up
```

---
name: heliosproxy-demo-down
description: Tear down any v0.4.0 demo cleanly. Volume cleanup, port liberation, log collection before stop. Use when the user says "tear down", "demo.sh down", "stuck on port", "clean up after demo", "where are the logs".
allowed-tools: Bash(./demo.sh *), Bash(docker *), Bash(docker compose *), Bash(lsof *), Bash(cd *), Read
related: [heliosproxy-overview, heliosproxy-demo-up]
---

# Tear down a demo

Each demo's `demo.sh down` runs `docker compose down -v` —
containers, networks, AND volumes are removed. State is wiped
clean. If you want to keep state (logs, journal data, plugin KV),
collect it before tearing down.

🟠 Mutating — destroys local state.

## When to use

- Done with a demo, want to free ports / disk
- Switching demos: stop one before starting another (port conflict)
- Cleanup after a CI run
- Recovering from a stuck `./demo.sh up` (containers in error state)

## Recipes

### Recipe 1: Standard tear-down

```bash
cd demos/v0.4.0/02-edge-proxy
./demo.sh down
```

Equivalent to:

```bash
docker compose down -v
```

`-v` removes named volumes — the demo's PG data, the proxy's
plugin/journal/cache state. Without `-v`, volumes survive and
the next `up` reuses the old data.

### Recipe 2: Tear down ALL demos at once

```bash
cd demos/v0.4.0
for d in [0-9][0-9]-*; do
  echo "tearing down $d"
  (cd "$d" && ./demo.sh down 2>/dev/null || true)
done
```

Useful in CI / cleanup scripts.

### Recipe 3: Collect logs before tear-down

```bash
cd demos/v0.4.0/01-anomaly-detection

# proxy logs
docker compose logs proxy > /tmp/anomaly-proxy.log
docker compose logs postgres > /tmp/anomaly-postgres.log

# specific endpoint snapshots
curl -s http://localhost:9090/anomalies?limit=1024 > /tmp/anomaly-events.json
curl -s http://localhost:9090/topology > /tmp/anomaly-topology.json
curl -s http://localhost:9090/metrics > /tmp/anomaly-metrics.json

# now tear down
./demo.sh down
```

For plugin demos, also grab the plugin KV state if relevant:

```bash
curl -s "http://localhost:9090/admin/kv/<plugin-name>/" > /tmp/plugin-kv.txt
```

### Recipe 4: Recover from a stuck demo

If `./demo.sh up` hangs or fails partway, containers may be left
in error state. Force-remove them:

```bash
cd demos/v0.4.0/02-edge-proxy
docker compose down -v --remove-orphans
docker compose ps   # confirm nothing is left
```

For globally stuck containers (a previous demo's leftover):

```bash
docker ps -a --filter 'name=heliosdb-proxy-' --format '{{.ID}}' \
  | xargs -r docker rm -f
docker network prune -f
docker volume prune -f
```

The last two are aggressive — they remove all unused networks and
volumes globally. Safe when no other Docker work is in progress.

### Recipe 5: Free a stuck port

```bash
sudo lsof -i :6432
# heliosdb 12345 ... LISTEN
sudo kill 12345
# OR escalate
sudo kill -9 12345
```

Common after a `kill -9` on the proxy host process; the OS holds
the port in `TIME_WAIT` for ~60 s on Linux.

```bash
# wait it out with a watch
watch -n 2 'lsof -i :6432 2>/dev/null || echo "free"'
```

### Recipe 6: Reset volumes between demo runs

If demos are re-using the same volume names and you want a fresh
start without `--remove-orphans`:

```bash
cd demos/v0.4.0/<demo>
docker compose down
docker volume ls --format '{{.Name}}' | grep "$(basename $PWD)" | xargs -r docker volume rm
docker compose up -d
```

## Pitfalls

- **`./demo.sh down` removes volumes by default** (via the `-v`
  flag inside the script). If you wanted to keep journal / KV /
  audit-chain state, you should have collected it first.
- **`docker compose down` (without `-v`) keeps volumes.** Then the
  next `up` resumes with old data — sometimes useful, sometimes
  surprising. Always check the demo's own `demo.sh` to see which
  it does.
- **TIME_WAIT on Linux.** A killed proxy holds 6432 / 9090 for
  ~60 s. `./demo.sh up` immediately after `down` may fail to bind.
  Wait 60 s or pick different ports.
- **Plugin demos leave a `plugins/` subdirectory with cached
  `.wasm`.** Tear-down doesn't remove these (they're under your
  workdir, not in a Docker volume). Cleanup with
  `rm -rf plugins/*.wasm` if you want a fresh plugin build.
- **The `09-admin-ui` demo's container holds `/var/cache` and
  `/var/log` in named volumes.** A `down -v` clears them; the next
  `up` rebuilds. If the dashboard looks weirdly empty, that's why.
- **Don't `docker volume prune -a` mid-demo on a shared dev
  machine.** It nukes volumes for unrelated projects.

## See also

- `heliosproxy-demo-up` — what tear-down is undoing
- `heliosproxy-shutdown` — for the proxy itself outside demos
- [`demos/v0.4.0/`](../../demos/v0.4.0/) — top-level demo index
- [`demos/v0.4.0/_shared/`](../../demos/v0.4.0/_shared/) — shared scaffolding

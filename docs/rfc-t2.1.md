# RFC — Zero-downtime PostgreSQL major-version upgrades via HeliosProxy

**Status**: draft for r/PostgreSQL + Postgres Slack `#general` post.
**Audience**: PG DBAs, platform engineers, SREs running production
PostgreSQL clusters.
**Goal**: collect ≥ 50 "interested" signals (replies, upvotes, sign-ups)
before the T2.1 flagship lands.

## TL;DR

We're building an open-source proxy that upgrades a live PostgreSQL
cluster across major versions (14 → 17, 15 → 17, etc.) **without
dropping a single client connection**. The proxy is
[HeliosProxy](https://github.com/dimensigon/HDB-HeliosDB-Proxy) (AGPL-3.0).
Looking for early operators willing to validate the workflow against
their staging clusters.

## Why

`pg_upgrade` requires downtime, or careful manual blue/green setup
with bespoke replication scripting. Logical replication-based
upgrades work but force the application team to handle connection
draining, write fencing, and post-cutover validation themselves.
Every PG operator below v16 is on a deferred upgrade path; the cost
of a clean upgrade is the operational reason the cluster stays on
the old version.

## What we're building

A six-stage orchestrator running inside an existing
PostgreSQL-wire-compatible proxy. The proxy already handles client
connection pooling, transaction-level write routing, and failover
replay. The upgrade flow extends that:

```
Stage 1  Spin up new-version standby; create logical-replication slot.
Stage 2  Wait until standby's replay LSN catches the source's commit LSN.
Stage 3  Shadow execute writes on both — orchestrator measures drift.
Stage 4  Quiesce client writes via the proxy's switchover buffer; promote target.
Stage 5  Replay any in-flight transactions against new primary (TR engine).
Stage 6  Disable old node; emit completion event.
```

What's different from rolling-your-own:

- **Connection state preserves.** Active sessions stay attached; the
  proxy holds them through stage 4-5 and they observe a brief latency
  spike rather than a connection error.
- **Cursor and prepared-statement state migrates.** The session-migrate
  + cursor-restore engines re-DECLARE / re-PREPARE on the new
  primary so client-side resume code "just works".
- **Sample validation is built in.** Stage 3 runs row-count + per-row
  hash on a deterministic sample so the cutover doesn't ship until
  source ≡ target byte-for-byte (configurable strictness).
- **Rollback is one button.** Stage 4 buffers writes; if validation
  fails, the old primary stays primary and the upgrade aborts with
  zero impact.

## Why a proxy

The orchestration runs *outside* the PostgreSQL processes — it's the
proxy that knows where the in-flight transactions are, that holds
the client connections, and that already knows how to route writes.
Standalone tools (`pg_upgrade`, custom scripts) don't have that
vantage point and end up reinventing connection management.

## Status

- The proxy itself is at v0.3.1, [private repo,
  AGPL-3.0](https://github.com/dimensigon/HDB-HeliosDB-Proxy).
- 1 184 unit + integration tests passing.
- T0 foundation complete: real PG client, TR stubs filled, plugin
  hooks wired, docker test harness with fault injection.
- T2.1 orchestrator state machine landed; per-stage side effects are
  stubs that compile to no-ops. Real-cluster wiring lands on top.
- We have a docker-compose harness with PG 14/15/16/17 in parallel
  containers; CI runs the full 6-pair upgrade matrix end-to-end on
  every commit.

## What we need from you

1. **Interest signal.** Reply or DM if "zero-dropped-connection
   PG major upgrade" would change your operational posture. We're
   measuring demand before we lock the API.
2. **Validation partners.** 3-5 production operators willing to run
   the workflow against staging clusters (with non-trivial write
   workloads) once stage bodies are real. We provide hand-holding
   and fix-on-first-bug; you get an upgrade story for free.
3. **Edge cases.** What's the gnarliest thing about your last PG
   major upgrade? Things we're already chasing: connection-pooled
   apps, prepared-statement-heavy workloads, large temp tables,
   cursor `WITH HOLD`. What else?

## Anti-questions

- ❌ "Why not just use logical replication directly?" — we do, but
  the replication is half the problem. The other half is preserving
  client state across the cutover.
- ❌ "Why not use pg_auto_failover / Patroni?" — those manage
  cluster membership, not version transitions. Complementary, not
  overlapping.
- ❌ "Why not pgbouncer + manual cutover?" — pgbouncer drops the
  connection on a backend swap. The fix is the proxy holds the
  client side through the swap.

## Where

- Repo: <https://github.com/dimensigon/HDB-HeliosDB-Proxy>
- Discussion: this thread (please reply)
- Demo target: a pgbench-continuous demo recording landing
  alongside the v0.4 release tag once stage bodies are wired.

## Timeline

- **Week 4**: Stage bodies wired against the upgrade-matrix harness.
- **Week 5**: First passing 14 → 17 run on a 16-thread pgbench
  load.
- **Week 6**: Recorded demo + this thread's mid-point update.
- **Week 8**: Public preview with a packaged operator.

If your team would benefit from the validation-partner program,
ping us. The waitlist is real and the slots are first-come.

— HeliosProxy contributors

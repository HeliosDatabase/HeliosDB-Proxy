# Website Brief — HeliosProxy v0.3.1

**Purpose:** Hand-off from the Proxy-repo agent to the website agent. Describes what changed in v0.3.1, what's defensible to claim publicly, what isn't, and which pages likely need updates. Read this before editing marketing copy; act on the sections flagged "action."

**Release:** v0.3.1 — 2026-04-22 — commit `23a451c` on `main` of `github.com/HeliosDatabase/HeliosDB-Proxy` (private).
**Previous release:** v0.3.0 — 2026-03-26 — commit `8c43de9`.
**Type:** patch release — correctness fixes + hot-path performance. No API removals. No new feature modules (still 24). Feature flags, module list, CLI, admin API, Docker image surface are unchanged.

---

## TL;DR for the homepage / release-notes reader

1. **Connection-pool checkout is 28-37 % faster** (single-threaded, measured).
2. **Pool metrics reads are 3.1× faster** (lock-free, measured).
3. **Protocol parser no longer allocates per prepared-statement parameter** (not benchmarked, but visible in code and covered by regression tests).
4. **L1 cache hits are lock-free** — many threads can hit the same key in parallel (covered by a 16-thread × 500-iter regression test; not benchmarked).
5. Under-the-hood reliability fixes: `max_connections` is now actually enforced against in-use connections; unterminated C-strings in protocol input return an error instead of silently consuming the buffer.

---

## What changed — what to communicate vs. what belongs in the CHANGELOG only

### Website-worthy (existing claims get stronger)

These improve things we already market. Keep the framing neutral and specific — no "N× faster than X" vs. competitors unless we actually benchmarked that.

#### Connection pooling / checkout throughput
- **Claim you can make:** *"Per-node connection checkout doesn't serialize through a global lock. Idle-connection pop and semaphore acquire happen outside the map lock; per-node state is held behind a cheap cloneable `Arc<Semaphore>`."*
- **Number you can quote (with caveats):** *"Single-threaded `acquire_release` benchmark improved 30 % (472 ns → 330 ns) between v0.3.0 and v0.3.1 on the same machine."* — this is the criterion `--quick` harness, not a production workload.
- Goes on: pool section of `heliosproxy.md`, wherever existing copy talks about "connection pooling" or "checkout latency."

#### Pool observability / metrics
- **Claim:** *"Pool counters (acquires, timeouts, connections created/closed, recycles, validation failures) are atomic. Reading the full metrics snapshot is sub-15 ns and never contends with checkouts."*
- **Number:** *"`pool.metrics()` read: 35.9 ns → 11.5 ns (3.1× faster)."*
- Goes on: observability / metrics section.

#### PostgreSQL wire parsing
- **Claim:** *"Prepared-statement parameter values are handled as reference-counted slices into the original protocol buffer — no per-parameter heap allocation during parse. C-string reads in the parser avoid incremental buffer growth via a single scan."*
- **No benchmark number yet.** Do not put a number on this. If you want a line, use: *"Wire-protocol parsing now avoids per-parameter allocation."*
- Goes on: protocol / PostgreSQL compatibility section.

#### L1 hot cache
- **Claim:** *"L1 cache hits take only a read lock; the per-entry access counter is atomic, so many threads can hit the same cached query in parallel without serialising."*
- **Evidence (not a number):** *"Exercised by a 16-thread × 500-iteration regression test against the same key."*
- Goes on: query-cache section, wherever L1/L2/L3 tiers are described.

### CHANGELOG-only — do NOT broadcast

These are bug fixes. Broadcasting them signals the previous version had the bug. Keep them in CHANGELOG but out of blog/homepage copy.

- Semaphore permit leak that allowed the pool to overshoot `max_connections`. The public line in CHANGELOG is *"`max_connections` now enforced against in-use connections."* That's fine in release notes; it doesn't belong on the pricing or features page.
- Unterminated C-string handling in the protocol parser (silently consumed buffer → now returns a protocol error).

---

## Benchmark table — defensible numbers

Source: `cargo bench --bench pooling -- --quick`, same machine, v0.3.0 (`8c43de9`) vs. v0.3.1 (`23a451c`). Criterion quick mode — lower sample count than a full run, but directionally reliable.

| Benchmark                               | v0.3.0     | v0.3.1     | Change         |
|-----------------------------------------|------------|------------|----------------|
| `pool/acquire_release/single`           | 471.81 ns  | 329.83 ns  | **−30 %**      |
| `pool/throughput/sequential_acquire/1`  | 490.36 ns  | 353.88 ns  | **−28 %**      |
| `pool/throughput/sequential_acquire/10` | 5.354 µs   | 3.351 µs   | **−37 %**      |
| `pool/throughput/sequential_acquire/50` | 25.36 µs   | 16.95 µs   | **−33 %**      |
| `pool/metrics/read_metrics`             | 35.90 ns   | 11.51 ns   | **−68 %** (3.1×) |

**When citing these:** always note "single-threaded, sequential, criterion `--quick`." Don't extrapolate to queries-per-second of a full proxy — the pool is one component on the request path.

---

## Action: where on the website to update

Paths below are relative to `/home/app/Helios/Docs-Public/Lite/docs/website/` (based on the last site structure known to this repo — confirm with a directory listing before editing):

1. **`heliosproxy.md`** (product page)
   - Pool / routing section: add the "checkout doesn't serialise per-node" line.
   - Observability section: add the "lock-free atomic counters" line.
   - PostgreSQL compatibility section: add the "no per-parameter allocation" line.
   - Query-cache section: add the "concurrent L1 hits" line.
   - Version badge / header: bump to `v0.3.1` if the page surfaces a version.

2. **`features.md`** — if this page lists per-module bullets, each of the four items above can become a sub-bullet under the relevant module. No wholesale re-write needed.

3. **`index.md`** — only if there's a "what's new" / release-highlight widget. If so, one sentence: *"v0.3.1 ships a 30 % faster pool checkout and lock-free cache hits. Release notes →"*.

4. **`_nav.md` / nav structure** — no changes expected.

5. **Release-notes / changelog page (if the site has one)** — mirror the content of `CHANGELOG.md` §[0.3.1] verbatim. The canonical CHANGELOG lives at `https://github.com/HeliosDatabase/HeliosDB-Proxy/blob/main/CHANGELOG.md` (private repo — link only works for authenticated users).

6. **Download / install page** — bump the latest version string to `0.3.1`. Docker tag: `ghcr.io/heliosdatabase/heliosdb-proxy:0.3.1` (once the release workflow runs; verify the tag exists before publishing the copy).

---

## What NOT to claim yet

- **Do not** put "2-4× faster" on the homepage. The audit document speculates that; the benches we've actually run show 28-37 % on single-threaded pool checkout. Mixed-workload / multi-threaded numbers require new benches.
- **Do not** quote throughput numbers on protocol parsing or L1 cache — neither has a dedicated bench yet. Claims about those must be qualitative or cite the regression tests, not a ns figure.
- **Do not** compare against PgBouncer / Pgpool in v0.3.1 copy. The existing v0.1.0 PgBouncer comparison bench may be out of date — don't cite it unless someone re-runs it on the same hardware with v0.3.1.

---

## Tone reminder

HeliosProxy copy is **formal, commercial, protocol-level**:

- All 24 feature modules work against any PostgreSQL-wire-compatible backend (PostgreSQL 12+, HeliosDB-Lite/Full, CockroachDB, YugabyteDB, TimescaleDB, Citus, AlloyDB). Reinforce this in any new copy where the word "HeliosDB" might creep in.
- Prefer "connection router and failover manager" over "proxy" when describing positioning; "proxy" is fine in technical sections.
- No emojis. No exclamation marks. No "blazing" / "lightning-fast" language.
- Benchmark numbers must include the method ("criterion `--quick`, single-threaded, same machine") — never bare "30 % faster."

---

## Source-of-truth links for the website agent

- Audit (internal): `/home/app/Helios/Proxy/docs/audit-2026-04-21.md`
- CHANGELOG: `/home/app/Helios/Proxy/CHANGELOG.md` §[0.3.1]
- Fix commits:
  - `d80c673` — pool semaphore permit + lock scope
  - `634c641` — pool metrics → atomics
  - `c34b579` — protocol zero-copy
  - `6da0817` — L1 cache read-path
  - `23a451c` — release commit (CHANGELOG + version bump)
- Regression tests demonstrating the claims:
  - `src/connection_pool.rs::tests::test_max_connections_enforced_while_in_use`
  - `src/connection_pool.rs::tests::test_return_then_reacquire_reuses_permit`
  - `src/protocol.rs::tests::test_read_cstring_unterminated`
  - `src/protocol.rs::tests::test_bind_message_param_values_are_bytes`
  - `src/cache/l1_hot.rs::tests::test_concurrent_hits_read_lock_only`

---

## Open items (not blocking website update, but worth tracking)

- LICENSE is now Apache-2.0 (was SSPL-1.0, transitioned through AGPL-3.0). `Cargo.toml` carries `license = "Apache-2.0"` and `license-file = "LICENSE"`.
- No benchmark exists for the protocol-parser or L1-cache improvements. If marketing wants numbers for those, we need to author benches first.

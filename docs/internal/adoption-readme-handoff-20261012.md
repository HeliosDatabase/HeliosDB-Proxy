# README adoption pass: handoff (2026-10-12)

Scope: HeliosProxy section of `/home/gpc/HDB/heliosdb-adoption-plan.md` (issue 0.2 and the README items).

## Done in this branch (README.md only)
- Positioning now reads "replaces HAProxy + PgBouncer in front of a Patroni-managed or RDS/Aurora cluster"; says explicitly it does not replace Patroni and does not promote or fence by default.
- Hero, badges (CI, crates.io, license, stars), four-core-jobs table, 60-second quickstart, "What happens on failover", comparison table, star CTA.
- Audit/verification paragraph moved unchanged into "Status & guarantees", linked from the top.
- "46 modules" lead-in removed; module tables kept below as optional.

## Needs external work (not done here)
- Failover GIF: no committed demo configures a promotion authority. Record one against Patroni (or pg_auto_failover) once a compose file exists; do not reuse `demos/chaos-failover` as a promotion demo.
- Quickstart was checked for CLI/startup only (`--primary/--listen` start, `/health` answers). The Docker-backed end-to-end run was not executed (needs owner approval per run).
- Comparison table: PgBouncer/HAProxy cells are from upstream docs recalled, not re-fetched; PgCat, Supavisor, Odyssey rows omitted until verified.
- Docker pulls badge and any `docker run` quickstart: no verified public image (docker.yml pushes to GHCR on tags; docs reference unpublished names). Release tarballs for linux/macOS x86_64/aarch64 do exist on v2.0.0.
- Prebuilt release binaries use `all-features` without `postgres-topology`, so the postgres/patroni providers are missing from them. Consider adding it to `release.yml`.
- GitHub settings (owner action): repo description "PgBouncer + HAProxy replacement for Postgres with failover-safe sessions, written in Rust" (adjust to the accurate wording above); topics `postgresql, pgbouncer, connection-pooler, proxy, high-availability, rust, failover`; social preview image; Discussions; "good first issue" labels.
- Cargo.toml `description` still says "failover manager"; consider aligning (changes crate metadata, so left alone).
- If promotion is later wired into the daemon (branch `feat/auto-promote-20261011`, unmerged, opt-in), revisit the "does not replace Patroni" wording only after owner decision and evidence.

## Coordination with Nano
Nano's README items are theirs. Dependency: Nano README's "Ecosystem"/Proxy mentions should use the same wording (no "replaces Patroni"). Note: Nano has no fenced promote primitive.

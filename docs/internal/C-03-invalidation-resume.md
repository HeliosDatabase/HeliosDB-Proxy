# C-03 — Invalidation-stream cursor resumption

Audit item C-03 ("recover gaps in invalidation delivery"), tracking issue #54.
Goal: an edge that loses its SSE invalidation stream (network blip, home
restart) must not silently serve pre-gap data, and a short drop must not force
a full cold cache when the home can replay the missed tail.

## Protocol

- Every broadcast home-side gets a monotonic `seq` (`InvalidationEvent.seq`,
  serde default `0` = unsequenced/pre-C-03 home).
- The home keeps a bounded replay ring of the last 256 invalidations
  (`REGISTRY_HISTORY_CAP`), so replay memory is O(1), never proportional to
  write rate.
- Each home boot has a `boot_id` (micros since UNIX epoch). Cursors are opaque
  `boot:seq` pairs; a cursor from another boot can never be mistaken for a
  position in this boot's stream.
- `GET /api/edge/subscribe?last_event_id=<boot>:<seq>` calls
  `EdgeRegistry::register_at`, which returns `Resume::Warm { replayed, upto }`
  (missed tail queued into the edge channel before it goes live) or
  `Resume::Gap { upto }` (unknown/foreign/evicted cursor, or nothing streamed).
- SSE marker written before any event: `: resume warm boot=B upto=N replayed=K`
  or `: resume gap boot=B upto=N`. Invalidation frames carry `id: <seq>`.
- Gap semantics: the wildcard hello (`up_to_version = current_version`,
  empty tables) still flushes the edge. Warm semantics: the hello carries
  `up_to_version = 0` — epoch check only, no wildcard drop — and the edge
  applies the replayed/live events incrementally.

## Edge client

- Tracks `cursor: Option<(boot, seq)>` across reconnects, sends
  `last_event_id` on reconnect, and updates `seq` from `id:` lines.
- On the marker: `warm` keeps the cache; `gap` calls `flush_all()`.
- No cursor (first connect for this process, or a pre-C-03 home): cold-start
  flush, preserving the old conservative behaviour.
- Pre-C-03 homes ignore the unknown query key, emit no marker/`id:`, so the
  client keeps flushing on every reconnect exactly as before.

## Status (2026-09-17)

- `82d940e` edge cache byte budget (C-01, adjacent).
- `4271bad` edge flush-on-connect (`gap_flushes` counter) — interim safety net.
- `4fd4544` home-side seq/replay/register_at + markers + `id:` frames; gates
  `1.8.1-c03b` PASS, CI green.
- Client half implemented (parser `id:`/`: resume`, cursor send, warm/gap
  handling) but uncommitted; verification gate `1.8.1-c03c` queued.
- Remaining: land the client half behind a green gate, then a live
  reconnect drill (warm vs gap) and/or an SSE-path integration test.
- Handover: sprinter item `752317ee59d4`, assignee Fable 5.1.

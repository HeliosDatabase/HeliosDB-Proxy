# Group 3B — Flush/NOTIFY idle relay (main-loop backend watch)

The deferred half of Group 3. Both fixes share one mechanism: while the session
loop waits for the client's next message, it also **watches the current backend
connection** and relays whatever it produces. That single change fixes idle
LISTEN/NOTIFY (3.C) and, because it delivers late `Flush` output, lets the
post-Flush relay stop waiting (3.B) — no separate client-readability probe
needed.

## 3.C — idle sessions were deaf to async backend traffic

While parked on `stream.read_buf()` waiting for the *client*, the proxy never
read the backend socket, so `LISTEN`/`NOTIFY` notifications, `NoticeResponse`,
and `ParameterStatus` sat unread until the client's next query (LISTEN through
the proxy was effectively broken for idle clients), and a backend that died
while the session was idle went unnoticed.

**Fix.** At the top of the loop the backend is always quiescent (every query
response is fully drained before returning here), so any bytes it produces are
out-of-band. The read is now a `tokio::select!` over: (a) the client read, and
(b) a read on the *current* backend connection. Backend bytes → relayed
verbatim to the client, keep watching. Backend EOF/error while idle → drop the
cached connection (next query redials), keep the client session alive. A
mid-COPY backend is excluded (it legitimately awaits CopyData). Only the current
node is watched (a session's LISTEN pins it to one connection in practice).

## 3.B — Flush 200 ms stall

`stream_flush` returned only after the backend had been silent for **200 ms**,
so a driver doing `Parse`+`Flush` → wait `ParseComplete` → `Bind`/`Execute`
paid up to +200 ms per prepare cycle: the proxy relayed `ParseComplete` but then
sat in the 200 ms wait, not reading the `Bind`/`Execute` the client had already
sent.

**Fix.** `stream_flush` now drains only what is *instantly* available
(non-blocking `try_read` loop) and returns immediately; any Flush output that
lands later is delivered by the 3.C backend watch. No fixed stall.

## Results
- **Correctness/latency — new tests, each discriminates (pass on fix, fail on baseline main):**
  - `notify-test.sh`: a psycopg2 listener `LISTEN`s then idle-waits in `select()`;
    a separate connection `NOTIFY`s. Fix: delivered while idle (2/2). Baseline:
    listener times out.
  - `flush-latency-test.sh`: a raw-protocol client (inline SCRAM-SHA-256) times
    `Parse`+`Flush` → `ParseComplete` → `Bind`/`Execute`/`Sync` → result. Fix:
    **0.6 ms**. Baseline: **202 ms**.
- **No regression:** regression 9/9; unnamed-parse 8/8; prepared-stmt 4/4;
  copy 3/3; copy-poolmode 4/4; cancel PASS. Session-mode throughput within noise
  of baseline (the watch's per-idle-wait `select` is free). clippy `-D warnings`
  ×4; 270 default + 1407 all-features lib tests.

## Deferred
Watching *all* cached backend connections (not just the current node) for the
rare multi-connection LISTEN case; a client-readability fast-path if profiling
ever shows the watch matters on the hot path (it does not today).

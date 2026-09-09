# Full backlog implementation progress

The active user goal is to solve **all** audit backlog items, including newly
confirmed findings. A patch batch is a validation checkpoint, not the end of that
goal. Items close only after their implementation and acceptance evidence are
complete. Global guarantees must remain consistent with actual backend capabilities.

## Current work

| Item | State | Remaining work |
|---|---|---|
| TR-01 | Commit-boundary safeguards passed initial gates; held-Parse/Flush follow-ups queued | Acknowledged cross-cycle statement/portal identity; validate follow-ups and complete acceptance evidence |
| TR-05 | Conservative partial-write handling passed functional/lint/MSRV gates | Investigate per-benchmark regressions before final acceptance |
| TR-02 | Streaming guard and recorder follow-ups passed targeted validation; the uncovered backend-watch publication path found in review is now frame-aligned | Optional bounded response buffering and performance/memory acceptance |
| H-07 | Frame-header validation applied on EVERY backend streaming relay (`stream_until_ready`, capture, replay drain, pool reset, re-prepare reader, idle watch) via `backend_frame_len` and the new `[limits] max_backend_frame_bytes` (default 100 MiB); malformed (< 4) or oversize headers close the backend immediately; the re-prepare reader no longer allocates the advertised body size | Slow-drip: a backend dripping bytes inside one frame resets the per-read `backend_read_timeout` each time — a whole-response deadline is TR-06's "one recovery deadline"; track there |
| Other items | Open | Work through the dependency order in IMPROVEMENTS.md |

## Independent review, 2026-09-09 (Claude)

Three adversarial reviewers (one per lens: commit classification, response
publication, delivery/general safety) read the whole diff. TR-05 could not be
refuted. TR-01 passed with caveats. TR-02 failed on a publication path the fix
did not cover. All findings acted on are listed below; the fixes and their tests
are in the working tree and the full matrix was re-run on the final state.

| Finding | Severity | Resolution |
|---|---|---|
| Idle backend-watch relay published raw chunks that can end mid-frame, with no progress accounting: a partial async frame followed by a backend death authorized a full re-execution appended inside that frame | high | Relay now forwards only COMPLETE frames and reassembles the remainder (`complete_frame_prefix`), bounded by `[limits] max_pending_bytes`; a length below the 4-byte minimum is rejected. Discriminating tests added |
| `ROLLBACK; <DML>` classified as a commit promoted a rolled-back transaction's `SET`s into the post-failover restore | medium | `Boundaries::ends_tx` exposed; GUC promotion now requires a durable commit. Test fails on the unfixed tree |
| Blanket refusal of backslash-in-literal stripped `select`/`transaction` replay from regexes, Windows paths and JSON escapes | medium | The scan runs under both backslash readings and refuses only when they disagree — exactly the semicolon-hiding case. No session state needed |
| Uncertain autocommit write with no replacement primary reported `08006` | low | Reports `08007` with the same verify-the-outcome guidance as an uncertain COMMIT |
| Held unnamed `Parse` write was the last unbounded backend write | low | Bounded by `[limits] backend_write_timeout_secs` |
| `tr_response_action` close rule depended on `bytes > 0` | low | `raw`/`terminal` now decide independently of the byte count |

One reviewer recommendation was **rejected**: relaxing the guard so a published
`CommandComplete` no longer closes the session. It looks like an availability win,
but injecting an `ErrorResponse` after a `CommandComplete` reports failure for a
statement the backend actually finished, and a client that retries that `INSERT`
double-applies it. The synthetic harness caught the regression when it was tried;
closing remains the only report a client cannot misread.

Two harness expectations were updated to match the stricter relay: a `fragment`
case now requires the client to receive NO trailing partial-frame bytes, where it
previously asserted the 3 raw bytes the old relay forwarded.

## Validation state

- The original audit's evidence files remain unchanged.
- A fresh, complete 107-case pre-change Criterion baseline is recorded in
  `benches/BASELINE.md` and `evidence/tr01-benchmarks-before.json`.
- The isolated SQL lexer tests passed (2 tests); format and Python syntax checks pass.
- TR-01/TR-05 validation uses a frozen candidate at
  `/tmp/proxy-tr01-20260908/candidate`, with Rust source hashes in
  `/tmp/proxy-tr01-20260908/candidate-source.json`. Subsequent working-tree edits
  are **not** included in that candidate's results.
- The validation driver is `/tmp/proxy-tr01-20260908/gates.sh`; logs are under
  `/tmp/proxy-tr01-20260908/`. Heavy work runs under the shared fleet build lock
  with MemoryMax=24G and two Cargo jobs, sequentially.
- The frozen candidate passed default, `ha-tr`, `all-features`, and
  `all-features,postgres-topology` test commands. Backend-dependent tests that
  return early without a backend do not establish live integration coverage.
  The separately run disposable PostgreSQL fixture passed all seven commit-outcome
  cases (`evidence/tr01-postgres.json`); the original binary failed four of those
  seven (`evidence/tr01-postgres-before.json`). Five focused daemon protocol probes
  also passed (`evidence/tr01-default.json`).
- After the interruption, the prior validation process was absent from the host.
  On September 9, `/tmp/proxy-tr01-20260908/resume2-gates.sh` was queued to resume
  at the all-features/postgres-topology binary build. Earlier passing gates are
  retained; no benchmark result is inferred from the interrupted run.
- The resumed candidate also passed no-default tests, the four clippy profiles,
  the Rust 1.86 MSRV check, and five daemon probes each on all-pg and no-default
  binaries. Its full benchmark command completed, but the performance acceptance
  gate did **not** pass: 39 of 107 sample-median confidence intervals show separated
  regressions, with a largest median delta of +22.89%. The arithmetic mean of
  percentage deltas is +0.086% and the geometric mean is -0.202%; those aggregates
  do not excuse the individual regressions. See `evidence/tr01-benchmarks.json`.
  A controlled comparison is required to distinguish candidate effects from
  run-to-run/host variation. No release or merge acceptance is implied.
- `/tmp/proxy-tr02-20260909/gates.sh` uses the same shared lock and memory bound.
  Its separate candidate includes the streaming guard, 21 daemon cases, 13 real
  PostgreSQL streaming cases with an old-binary negative control, and the
  September 9 held-Parse ordering, pre-Sync BEGIN capture, and Flush budget fixes.
  Source hashes are in that directory's `candidate-source.json`. The candidate was
  updated before its first gate started; no result from TR-01 applies to it.
- Its first attempt passed format checking and reproduced duplicate rows/errors
  on the old binary, then classified an old-binary partial-frame hang as a harness
  timeout. Logs are preserved in `/tmp/proxy-tr02-20260909/attempt1/`. The corrected
  fixture records response timeout as an invariant failure only after confirming
  that its intended fault boundary was reached. A rerun is queued; Rust checks now
  precede external fixture gates. No TR-02 functional result is claimed yet.
- The corrected rerun completed successfully: 416 default, 1711 all-pg and 335
  no-default library tests passed; all-features clippy passed. All 21 synthetic
  probes passed on each of default, all-pg and no-default binaries. All 13 real
  PostgreSQL streaming probes passed; the original binary failed 12 of 13.
  See `evidence/tr02-validation.json` and `evidence/tr02-*.json`.
- The initial benchmark candidate's four executables were preserved under
  `/tmp/proxy-tr01-20260908/bench-control/`. A serialized baseline rebuild and
  binary/text comparison (`build-control.sh`, exec session 66934) is queued/running
  to test whether the measured paths contain changed machine code. Its eventual
  evidence is `tr01-benchmark-binary-control.json`; do not infer equality or a
  performance pass before inspecting the result.
- Initial Docker port publication failed because the host DOCKER NAT chain is
  absent. The fixture now uses host networking with PostgreSQL bound exclusively
  to a dedicated `127.0.0.1` port, retaining its private data and resource limits.
  No host networking repair or existing-service changes are part of this work.

No implementation item has been declared fully accepted yet. Keep updating this
file and the per-item evidence as the full backlog is resolved.

## User-requested stop and handoff — 2026-09-09

The user requested a progress/direction summary for tmux `Proxy`, then a stop.
The full summary is in [HANDOFF-2026-09-09.md](HANDOFF-2026-09-09.md). All validation
jobs started by Codex have finished; no new implementation work is scheduled.

The benchmark binary control completed: all four rebuilt baseline executables are
byte-for-byte identical to the preserved TR-01 candidate executables, including
their executable text. See `evidence/tr01-benchmark-binary-control.json`. This
rules out changed code in those binaries as the cause of the observed timing
differences, while leaving the daemon relay performance coverage gap open. The
original 107 measurements remain recorded. The full backlog is not complete.

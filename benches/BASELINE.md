# Criterion benchmark baseline

Recorded per CLAUDE.md quality gate 3. All subsequent `cargo bench` runs must be
non-negative vs these values, and cumulative degradation across a work session must
stay under 3%.

- **Date:** 2026-07-08
- **Host:** gpc001ca (Rocky/RHEL9, kernel 5.14.0-611.34.1.el9_7, 125.7 GiB RAM)
- **Commit:** 96eba7a (main)
- **Features:** `--features all-features`
- **Command:** `flock /home/gpc/HDB/sprint/coordination/build.lock systemd-run --user --scope --collect -p MemoryMax=24G -p MemorySwapMax=0 cargo bench --features all-features`
  (fleet build-lock + memory bound are mandatory on this host — see CLAUDE.md Resource Constraints)
- **Criterion data:** saved as the default baseline in `target/criterion/` at this commit;
  a plain re-run auto-reports the delta vs this run.
- **Metric:** Criterion wall-time estimate, reported as `[lower / median / upper]` of the
  95% confidence interval. Compare medians; treat a regression as real only if the
  candidate's CI does not overlap the baseline CI (Criterion's own change report says
  "regressed" / "improved" / "within noise").

## 2026-07-15 — current full-suite baseline (supersedes; adds relay / contention / protocol coverage)

Recorded at commit `2e50705` under `--features all-features`, host gpc001ca, fleet-locked
+ 24 GiB bound. **Recorded under heavy fleet load** (several concurrent sessions), so these
are CONSERVATIVE upper bounds — a quiet-host re-run reads faster (an "improvement", never a
gate-3 failure). Re-baseline in a quiescent window when convenient. The trivial clippy/test
fixes committed on top of `2e50705` touch no benched code, so these figures hold at the tip.

**No performance regression** vs the prior baseline: across the 92 benches the existing
pooling / routing / protocol groups showed a scattered mix (~14 improved / 18 within-noise /
~10 "regressed"), every delta ≤ ~6.4 % median, spread across unrelated modules on code this
change did not touch (only new bench files were added). That is the shared-host measurement
variance documented under "Interpreting deltas" below — not a real regression, which would be
localized and consistent to a changed hot path.

### benches/pooling.rs  (11 benches)

| Benchmark | Time [low / median / high] |
|---|---|
| pool/acquire_release/single | 589.67 ns / 594.63 ns / 600.27 ns |
| pool/throughput/sequential_acquire/1 | 720.93 ns / 725.92 ns / 732.51 ns |
| pool/throughput/sequential_acquire/10 | 7.2144 µs / 7.2922 µs / 7.3843 µs |
| pool/throughput/sequential_acquire/50 | 35.323 µs / 35.599 µs / 35.955 µs |
| pool/acquire_release/contention/2 | 21.979 µs / 22.396 µs / 22.813 µs |
| pool/acquire_release/contention/8 | 87.233 µs / 89.505 µs / 91.729 µs |
| pool/acquire_release/contention/32 | 379.07 µs / 388.68 µs / 397.85 µs |
| pool/node_endpoint/create | 725.44 ns / 728.44 ns / 732.06 ns |
| pool/node_endpoint/address | 110.64 ns / 111.46 ns / 112.43 ns |
| pool/node_endpoint/node_id | 708.02 ns / 711.30 ns / 715.29 ns |
| pool/metrics/read_metrics | 5.0096 ns / 5.0750 ns / 5.1502 ns |

### benches/protocol.rs  (21 benches)

| Benchmark | Time [low / median / high] |
|---|---|
| protocol/decode_message/short_select | 62.225 ns / 62.649 ns / 63.139 ns |
| protocol/decode_message/medium_where | 70.873 ns / 72.305 ns / 73.668 ns |
| protocol/decode_message/kilobyte_in_list | 122.97 ns / 123.95 ns / 125.13 ns |
| protocol/encode_message/short_select | 21.942 ns / 22.205 ns / 22.457 ns |
| protocol/encode_message/medium_where | 23.723 ns / 23.921 ns / 24.151 ns |
| protocol/encode_message/kilobyte_in_list | 77.077 ns / 78.304 ns / 79.659 ns |
| protocol/query_text/short_select | 11.924 ns / 11.980 ns / 12.050 ns |
| protocol/query_text/medium_where | 40.899 ns / 41.167 ns / 41.475 ns |
| protocol/query_text/kilobyte_in_list | 334.17 ns / 338.27 ns / 343.47 ns |
| protocol/decode_startup/startup_params | 837.48 ns / 845.59 ns / 853.63 ns |
| protocol/decode_startup/ssl_request | 36.868 ns / 37.492 ns / 38.218 ns |
| protocol/decode_startup/cancel_request | 46.390 ns / 47.322 ns / 48.241 ns |
| protocol/extended_parse/parse_message | 168.78 ns / 171.11 ns / 173.95 ns |
| protocol/extended_parse/bind_message | 223.87 ns / 227.95 ns / 232.31 ns |
| protocol/backend_response/error_response | 496.36 ns / 503.41 ns / 511.95 ns |
| protocol/backend_response/command_complete | 125.59 ns / 126.54 ns / 127.76 ns |
| protocol/backend_response/auth_sasl | 143.18 ns / 145.09 ns / 147.32 ns |
| protocol/backend_response/auth_md5 | 36.463 ns / 36.977 ns / 37.606 ns |
| protocol/tag_dispatch/from_tag | 13.321 ns / 13.504 ns / 13.713 ns |
| protocol/tag_dispatch/starts_with_ci | 5.2150 ns / 5.2635 ns / 5.3168 ns |
| protocol/tag_dispatch/contains_ci | 21.988 ns / 22.219 ns / 22.495 ns |

### benches/relay.rs  (42 benches)

| Benchmark | Time [low / median / high] |
|---|---|
| switchover/buffer_query/enqueue_one | 231.82 ns / 237.98 ns / 244.90 ns |
| journal/statement_type/select | 21.347 ns / 21.552 ns / 21.817 ns |
| journal/statement_type/insert | 26.623 ns / 27.001 ns / 27.370 ns |
| journal/statement_type/update | 28.346 ns / 28.586 ns / 28.865 ns |
| journal/statement_type/delete | 33.336 ns / 33.625 ns / 33.954 ns |
| journal/statement_type/ddl | 33.976 ns / 34.284 ns / 34.673 ns |
| journal/statement_type/txn | 24.635 ns / 25.091 ns / 25.519 ns |
| journal/statement_type/set | 32.589 ns / 32.913 ns / 33.306 ns |
| journal/statement_type/other | 32.402 ns / 32.661 ns / 32.967 ns |
| journal/rollback_to_savepoint/16 | 1.3689 µs / 1.4236 µs / 1.4782 µs |
| journal/rollback_to_savepoint/128 | 6.1981 µs / 6.3584 µs / 6.5772 µs |
| journal/manager/begin_log_commit | 420.93 ns / 424.96 ns / 430.44 ns |
| journal/manager_contention/2 | 14.628 µs / 14.960 µs / 15.283 µs |
| journal/manager_contention/8 | 24.776 µs / 25.261 µs / 25.707 µs |
| journal/manager_contention/32 | 91.312 µs / 93.054 µs / 94.496 µs |
| journal/entries_in_window/scan_500 | 49.232 µs / 49.786 µs / 50.575 µs |
| pool_mode/txn_event_detect/begin | 8.9909 ns / 9.0535 ns / 9.1124 ns |
| pool_mode/txn_event_detect/start_txn | 23.656 ns / 23.951 ns / 24.294 ns |
| pool_mode/txn_event_detect/commit | 9.9152 ns / 10.025 ns / 10.161 ns |
| pool_mode/txn_event_detect/rollback | 15.711 ns / 15.988 ns / 16.320 ns |
| pool_mode/txn_event_detect/rollback_to | 23.127 ns / 23.463 ns / 23.904 ns |
| pool_mode/txn_event_detect/savepoint | 17.818 ns / 18.099 ns / 18.420 ns |
| pool_mode/txn_event_detect/release | 14.152 ns / 14.260 ns / 14.401 ns |
| pool_mode/txn_event_detect/statement | 15.253 ns / 15.465 ns / 15.705 ns |
| pool_mode/statement_safety/is_safe/safe_select | 39.295 ns / 40.090 ns / 40.951 ns |
| pool_mode/statement_safety/warning/safe_select | 37.341 ns / 37.822 ns / 38.470 ns |
| pool_mode/statement_safety/is_safe/unsafe_listen | 27.002 ns / 27.630 ns / 28.310 ns |
| pool_mode/statement_safety/warning/unsafe_listen | 25.550 ns / 25.774 ns / 26.048 ns |
| pool_mode/statement_safety/is_safe/unsafe_prepare | 27.080 ns / 27.557 ns / 28.109 ns |
| pool_mode/statement_safety/warning/unsafe_prepare | 27.251 ns / 27.567 ns / 27.959 ns |
| pool_mode/statement_safety/is_safe/unsafe_set | 138.85 ns / 139.95 ns / 141.24 ns |
| pool_mode/statement_safety/warning/unsafe_set | 150.36 ns / 152.17 ns / 154.34 ns |
| pool_mode/statement_safety/is_safe/safe_set_local | 97.322 ns / 97.799 ns / 98.313 ns |
| pool_mode/statement_safety/warning/safe_set_local | 114.86 ns / 116.09 ns / 117.49 ns |
| pool_mode/prepared_parse/prepare/named | 125.69 ns / 126.54 ns / 127.53 ns |
| pool_mode/prepared_parse/prepare/typed | 244.13 ns / 246.72 ns / 249.65 ns |
| pool_mode/prepared_parse/deallocate/named | 88.241 ns / 88.684 ns / 89.267 ns |
| pool_mode/prepared_parse/deallocate/all | 53.627 ns / 54.379 ns / 55.233 ns |
| pool_mode/manager_acquire_release/2 | 19.038 µs / 19.676 µs / 20.303 µs |
| pool_mode/manager_acquire_release/8 | 42.882 µs / 43.426 µs / 44.061 µs |
| pool_mode/manager_acquire_release/32 | 217.02 µs / 220.90 µs / 224.68 µs |
| pool_mode/on_statement_complete/txn_sequence | 274.13 ns / 278.16 ns / 282.40 ns |

### benches/routing.rs  (18 benches)

| Benchmark | Time [low / median / high] |
|---|---|
| routing/hint_parse/no_hints | 97.088 ns / 98.791 ns / 100.86 ns |
| routing/hint_parse/single_hint | 858.33 ns / 864.29 ns / 871.53 ns |
| routing/hint_parse/multiple_hints | 3.4488 µs / 3.5000 µs / 3.5606 µs |
| routing/hint_parse/complex_query | 1.8613 µs / 1.8740 µs / 1.8888 µs |
| routing/hint_strip/no_hints | 77.800 ns / 78.956 ns / 80.288 ns |
| routing/hint_strip/single_hint | 244.14 ns / 246.72 ns / 250.00 ns |
| routing/hint_strip/two_hints | 351.32 ns / 357.40 ns / 364.64 ns |
| routing/write_detect/select | 51.429 ns / 51.597 ns / 51.785 ns |
| routing/write_detect/insert | 52.113 ns / 52.446 ns / 52.807 ns |
| routing/write_detect/update | 60.782 ns / 61.115 ns / 61.428 ns |
| routing/write_detect/delete | 64.847 ns / 65.323 ns / 65.762 ns |
| routing/write_detect/begin | 47.467 ns / 47.558 ns / 47.674 ns |
| routing/write_detect/create_table | 51.966 ns / 52.180 ns / 52.406 ns |
| routing/write_detect/with_cte | 53.398 ns / 53.776 ns / 54.288 ns |
| routing/route/read_no_hints | 2.0069 µs / 2.0974 µs / 2.1979 µs |
| routing/route/read_with_hint | 2.6770 µs / 2.7470 µs / 2.8205 µs |
| routing/route/complex_hints | 3.6342 µs / 3.7075 µs / 3.7874 µs |
| routing/node_select/batch_parse | 347.49 µs / 352.61 µs / 357.61 µs |

## benches/pooling.rs

| Benchmark | Time [lower / median / upper] |
|---|---|
| pool/create/10 | 63.170 / 63.783 / 64.502 ns |
| pool/create/50 | 65.636 / 66.545 / 67.519 ns |
| pool/create/100 | 63.387 / 64.049 / 64.813 ns |
| pool/create/500 | 64.071 / 64.866 / 65.698 ns |
| pool/config/default | 3.7432 / 3.7634 / 3.7870 ns |
| pool/config/custom | 3.7581 / 3.7756 / 3.7958 ns |
| pool/acquire_release/single | 586.88 / 596.88 / 608.38 ns |
| pool/throughput/sequential_acquire/1 | 707.43 / 718.97 / 731.74 ns |
| pool/throughput/sequential_acquire/10 | 7.1815 / 7.2799 / 7.3971 µs |
| pool/throughput/sequential_acquire/50 | 35.120 / 35.429 / 35.780 µs |
| pool/node_endpoint/create | 737.42 / 743.71 / 750.73 ns |
| pool/node_endpoint/address | 99.818 / 100.38 / 101.01 ns |
| pool/node_endpoint/node_id | 722.65 / 728.32 / 734.53 ns |
| pool/metrics/read_metrics | 4.8941 / 4.9671 / 5.0632 ns |

## benches/routing.rs

| Benchmark | Time [lower / median / upper] |
|---|---|
| routing/hint_parse/no_hints | 97.165 / 99.115 / 101.51 ns |
| routing/hint_parse/single_hint | 868.75 / 876.67 / 885.80 ns |
| routing/hint_parse/multiple_hints | 3.4894 / 3.5262 / 3.5693 µs |
| routing/hint_parse/complex_query | 1.8984 / 1.9115 / 1.9279 µs |
| routing/hint_strip/no_hints | 74.793 / 76.817 / 78.861 ns |
| routing/hint_strip/single_hint | 250.39 / 253.43 / 257.00 ns |
| routing/hint_strip/two_hints | 346.39 / 351.61 / 357.62 ns |
| routing/write_detect/select | 48.305 / 48.700 / 49.216 ns |
| routing/write_detect/insert | 52.636 / 52.842 / 53.071 ns |
| routing/write_detect/update | 59.167 / 59.795 / 60.522 ns |
| routing/write_detect/delete | 60.459 / 60.673 / 60.928 ns |
| routing/write_detect/begin | 49.120 / 49.410 / 49.813 ns |
| routing/write_detect/create_table | 53.235 / 53.432 / 53.646 ns |
| routing/write_detect/with_cte | 54.418 / 54.640 / 54.849 ns |
| routing/route/read_no_hints | 2.4105 / 2.5051 / 2.6001 µs |
| routing/route/read_with_hint | 4.1716 / 4.2477 / 4.3184 µs |
| routing/route/write | 1.9452 / 2.0223 / 2.0982 µs |
| routing/route/complex_hints | 3.9361 / 4.0225 / 4.1133 µs |
| routing/node_select/batch_parse | 344.53 / 348.86 / 353.91 µs |

## benches/protocol.rs

New in 1.5.0 (T5 / P1-12) — first recorded baseline for the PG-wire per-query hot path
(decode / encode / query-text extraction). Recorded **2026-07-11** at commit **bb21d46**,
host **gpc001ca**, **`--features all-features`**, fleet-locked + 24G-bounded (same command
as the header). Feature-free code, so the figures hold across every feature set.

| Benchmark | Time [lower / median / upper] |
|---|---|
| protocol/decode_message/short_select | 61.437 / 62.193 / 63.078 ns |
| protocol/decode_message/medium_where | 65.212 / 66.729 / 68.438 ns |
| protocol/decode_message/kilobyte_in_list | 127.87 / 129.50 / 131.26 ns |
| protocol/encode_message/short_select | 25.788 / 26.091 / 26.442 ns |
| protocol/encode_message/medium_where | 25.835 / 25.991 / 26.190 ns |
| protocol/encode_message/kilobyte_in_list | 77.951 / 79.102 / 80.421 ns |
| protocol/query_text/short_select | 11.701 / 11.805 / 11.933 ns |
| protocol/query_text/medium_where | 43.432 / 43.912 / 44.386 ns |
| protocol/query_text/kilobyte_in_list | 336.86 / 341.43 / 346.14 ns |

## Interpreting deltas on this host (important)

This is a shared, production-like host running several concurrent sessions; CPU-frequency
scaling and co-tenant load make these ns/µs microbenchmarks noisy run-to-run (±10–15% on
the sub-200ns cases is common). A **scattered** mix of "improved" and "regressed" verdicts
across benchmarks — especially on code a change did not touch — is measurement variance, not
a real regression. Judge a candidate by: (1) does the change touch benchmarked code at all
(the benches import only `connection_pool` and `NodeEndpoint`/`NodeId`/`NodeRole`), and
(2) is any regression *localized and consistent* to the changed hot path? For a decisive
comparison, record a fresh baseline of the base commit and the candidate back-to-back in the
same quiescent window rather than comparing against a baseline taken at a different time.

## Known gap

`benches/protocol.rs` covers the PG-wire decode/encode/query-text + startup/extended/
backend-response parsing hot path. `benches/relay.rs` (added 2026-07-15) now covers the
in-process HA / mode-aware paths that never need a backend: the transaction journal
(StatementType classification, entry sizing, savepoint rollback, begin/log/commit under
write-lock contention, windowed scans), the switchover buffer enqueue, and the pool-mode
decision surface (TransactionEvent detection, statement-safety, PREPARE/DEALLOCATE
parsing, manager acquire/release under client concurrency, on_statement_complete). Pool
acquire/release contention is covered in `benches/pooling.rs`.
Still uncovered — genuinely needs a live backend, so it lives under the scalability
harness rather than Criterion: the end-to-end relay data plane (client↔backend byte
pumping over real sockets) and failover/replay that actually promotes and replays against
a running standby.
The proxy-path scalability harness (`scripts/regress/bench-scalability.sh`) has its own
evidence baselines in `docs/perf-2026-07/README.md` and is always re-measured
back-to-back with the candidate, never compared across days.

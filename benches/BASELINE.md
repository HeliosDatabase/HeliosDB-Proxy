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

## 2026-09-04 — 1.6.0 post-release measurement (main `ce73809`, code = tag `v1.6.0`)

Full-suite run of the released 1.6.0 (21 perf/stability fixes + F3 tr_mode failover) on this host —
107 benches (15 added since the 2026-08-29 baseline, recorded here for the first time),
`cargo bench --features all-features`, bounded (`systemd-run MemoryMax=24G`) under the fleet build
lock, own target dir. **Gate 3 vs the 2026-08-29 baseline: 92/92 matched, mean median delta +1.59%
(cumulative budget 3%) — PASS.** Host was noticeably busier than on 2026-08-29 (co-tenant Nano
release gate ran back-to-back): the largest single swings are on code the batch did not touch
(`routing/route/read_no_hints` +37%, `routing/route/read_with_hint` +37%, `pool/metrics/read_metrics`
+14% on a 5 ns bench) and the same runs show −22% / −30% on untouched siblings — variance, per the
interpretation notes below. Localized, reproducible (main-vs-candidate back-to-back) deltas that ARE
attributable: `journal/manager/begin_log_commit` +11–13% (O5 insertion-order index on the legacy
3-call API; the daemon uses the new single-lock `begin_and_log`), legacy `pool/acquire_release/*`
+7–18% (S10 idle-checkout validation, `test_on_acquire`-gated, not the data-path pool),
`pool_mode/txn_event_detect/start_txn` +78% (24→44 ns) on untouched `src/pool` code with
`statement`/`savepoint` siblings −16%/−22% — codegen-layout shift under thin-LTO.
This section is the reference for the next gate-3 comparison.

### benches/pooling.rs  (17 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| pool/create/10 | 70.712 ns / 71.726 ns / 72.787 ns |
| pool/create/50 | 71.214 ns / 72.403 ns / 73.689 ns |
| pool/create/100 | 72.232 ns / 73.052 ns / 73.939 ns |
| pool/create/500 | 71.302 ns / 71.838 ns / 72.460 ns |
| pool/config/default | 3.9565 ns / 3.9879 ns / 4.0274 ns |
| pool/config/custom | 3.8723 ns / 3.8861 ns / 3.8981 ns |
| pool/acquire_release/single | 688.38 ns / 701.49 ns / 717.18 ns |
| pool/throughput/sequential_acquire/1 | 803.82 ns / 831.52 ns / 861.87 ns |
| pool/throughput/sequential_acquire/10 | 8.4934 µs / 8.7279 µs / 8.9847 µs |
| pool/throughput/sequential_acquire/50 | 38.908 µs / 39.243 µs / 39.605 µs |
| pool/acquire_release/contention/2 | 19.072 µs / 19.529 µs / 20.093 µs |
| pool/acquire_release/contention/8 | 94.372 µs / 95.939 µs / 97.412 µs |
| pool/acquire_release/contention/32 | 363.00 µs / 370.39 µs / 377.17 µs |
| pool/node_endpoint/create | 729.12 ns / 733.62 ns / 738.18 ns |
| pool/node_endpoint/address | 120.81 ns / 121.79 ns / 122.86 ns |
| pool/node_endpoint/node_id | 737.19 ns / 743.21 ns / 749.42 ns |
| pool/metrics/read_metrics | 5.6832 ns / 5.9179 ns / 6.1470 ns |

### benches/protocol.rs  (21 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| protocol/decode_message/short_select | 62.758 ns / 63.410 ns / 64.230 ns |
| protocol/decode_message/medium_where | 68.615 ns / 70.059 ns / 71.433 ns |
| protocol/decode_message/kilobyte_in_list | 124.66 ns / 125.77 ns / 127.17 ns |
| protocol/encode_message/short_select | 22.014 ns / 22.214 ns / 22.435 ns |
| protocol/encode_message/medium_where | 23.518 ns / 23.752 ns / 24.042 ns |
| protocol/encode_message/kilobyte_in_list | 87.044 ns / 88.547 ns / 90.114 ns |
| protocol/query_text/short_select | 11.575 ns / 11.635 ns / 11.707 ns |
| protocol/query_text/medium_where | 43.419 ns / 43.921 ns / 44.425 ns |
| protocol/query_text/kilobyte_in_list | 350.90 ns / 355.19 ns / 360.29 ns |
| protocol/decode_startup/startup_params | 836.72 ns / 844.19 ns / 852.53 ns |
| protocol/decode_startup/ssl_request | 26.461 ns / 26.994 ns / 27.610 ns |
| protocol/decode_startup/cancel_request | 44.262 ns / 44.781 ns / 45.368 ns |
| protocol/extended_parse/parse_message | 164.56 ns / 166.13 ns / 167.90 ns |
| protocol/extended_parse/bind_message | 215.65 ns / 217.85 ns / 220.37 ns |
| protocol/backend_response/error_response | 499.21 ns / 506.01 ns / 513.90 ns |
| protocol/backend_response/command_complete | 134.55 ns / 135.57 ns / 136.75 ns |
| protocol/backend_response/auth_sasl | 147.82 ns / 150.20 ns / 152.97 ns |
| protocol/backend_response/auth_md5 | 41.888 ns / 42.262 ns / 42.643 ns |
| protocol/tag_dispatch/from_tag | 13.076 ns / 13.273 ns / 13.505 ns |
| protocol/tag_dispatch/starts_with_ci | 5.1950 ns / 5.2459 ns / 5.2999 ns |
| protocol/tag_dispatch/contains_ci | 20.859 ns / 21.266 ns / 21.695 ns |

### benches/relay.rs  (50 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| switchover/buffer_query/enqueue_one | 251.53 ns / 259.69 ns / 268.23 ns |
| switchover/drain/1 | 407.73 ns / 419.45 ns / 429.59 ns |
| switchover/drain/16 | 1.9262 µs / 1.9399 µs / 1.9548 µs |
| switchover/drain/64 | 6.5023 µs / 6.5694 µs / 6.6434 µs |
| journal/statement_type/select | 23.631 ns / 24.134 ns / 24.655 ns |
| journal/statement_type/insert | 25.698 ns / 25.972 ns / 26.292 ns |
| journal/statement_type/update | 29.866 ns / 30.322 ns / 30.817 ns |
| journal/statement_type/delete | 32.845 ns / 33.045 ns / 33.275 ns |
| journal/statement_type/ddl | 34.599 ns / 34.959 ns / 35.359 ns |
| journal/statement_type/txn | 23.698 ns / 23.865 ns / 24.059 ns |
| journal/statement_type/set | 32.397 ns / 32.764 ns / 33.278 ns |
| journal/statement_type/other | 32.445 ns / 32.717 ns / 33.045 ns |
| journal/total_size/1 | 6.1654 ns / 6.1890 ns / 6.2172 ns |
| journal/total_size/16 | 102.13 ns / 102.79 ns / 103.59 ns |
| journal/total_size/128 | 763.17 ns / 769.66 ns / 777.46 ns |
| journal/add_entry/push | 120.47 ns / 122.24 ns / 124.31 ns |
| journal/rollback_to_savepoint/16 | 1.3574 µs / 1.4048 µs / 1.4470 µs |
| journal/rollback_to_savepoint/128 | 7.9521 µs / 8.1095 µs / 8.2560 µs |
| journal/manager/begin_log_commit | 481.11 ns / 485.60 ns / 490.83 ns |
| journal/manager_contention/2 | 12.971 µs / 13.465 µs / 13.975 µs |
| journal/manager_contention/8 | 26.169 µs / 26.753 µs / 27.283 µs |
| journal/manager_contention/32 | 112.09 µs / 115.97 µs / 120.41 µs |
| journal/entries_in_window/scan_500 | 47.185 µs / 47.814 µs / 48.610 µs |
| pool_mode/txn_event_detect/begin | 8.6681 ns / 8.7492 ns / 8.8553 ns |
| pool_mode/txn_event_detect/start_txn | 43.709 ns / 44.284 ns / 44.954 ns |
| pool_mode/txn_event_detect/commit | 11.349 ns / 11.386 ns / 11.430 ns |
| pool_mode/txn_event_detect/rollback | 18.305 ns / 18.632 ns / 19.013 ns |
| pool_mode/txn_event_detect/rollback_to | 23.451 ns / 23.630 ns / 23.870 ns |
| pool_mode/txn_event_detect/savepoint | 14.562 ns / 14.712 ns / 14.902 ns |
| pool_mode/txn_event_detect/release | 15.148 ns / 15.332 ns / 15.541 ns |
| pool_mode/txn_event_detect/statement | 14.554 ns / 14.919 ns / 15.241 ns |
| pool_mode/pool_key | 157.52 ns / 159.07 ns / 161.01 ns |
| pool_mode/statement_safety/is_safe/safe_select | 40.150 ns / 40.829 ns / 41.509 ns |
| pool_mode/statement_safety/warning/safe_select | 36.634 ns / 36.969 ns / 37.408 ns |
| pool_mode/statement_safety/is_safe/unsafe_listen | 27.053 ns / 27.877 ns / 28.761 ns |
| pool_mode/statement_safety/warning/unsafe_listen | 25.487 ns / 25.630 ns / 25.825 ns |
| pool_mode/statement_safety/is_safe/unsafe_prepare | 26.615 ns / 26.895 ns / 27.187 ns |
| pool_mode/statement_safety/warning/unsafe_prepare | 27.387 ns / 27.749 ns / 28.207 ns |
| pool_mode/statement_safety/is_safe/unsafe_set | 155.16 ns / 158.28 ns / 161.13 ns |
| pool_mode/statement_safety/warning/unsafe_set | 143.43 ns / 144.31 ns / 145.40 ns |
| pool_mode/statement_safety/is_safe/safe_set_local | 106.95 ns / 108.14 ns / 109.55 ns |
| pool_mode/statement_safety/warning/safe_set_local | 102.12 ns / 103.44 ns / 105.09 ns |
| pool_mode/prepared_parse/prepare/named | 119.38 ns / 120.24 ns / 121.11 ns |
| pool_mode/prepared_parse/prepare/typed | 222.62 ns / 225.32 ns / 228.55 ns |
| pool_mode/prepared_parse/deallocate/named | 61.475 ns / 62.005 ns / 62.653 ns |
| pool_mode/prepared_parse/deallocate/all | 54.207 ns / 55.351 ns / 56.651 ns |
| pool_mode/manager_acquire_release/2 | 19.233 µs / 19.675 µs / 20.081 µs |
| pool_mode/manager_acquire_release/8 | 49.402 µs / 49.879 µs / 50.420 µs |
| pool_mode/manager_acquire_release/32 | 182.92 µs / 185.68 µs / 189.04 µs |
| pool_mode/on_statement_complete/txn_sequence | 284.16 ns / 287.04 ns / 290.59 ns |

### benches/routing.rs  (19 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| routing/hint_parse/no_hints | 95.451 ns / 96.709 ns / 98.094 ns |
| routing/hint_parse/single_hint | 865.45 ns / 882.61 ns / 902.92 ns |
| routing/hint_parse/multiple_hints | 3.4120 µs / 3.4365 µs / 3.4630 µs |
| routing/hint_parse/complex_query | 1.9430 µs / 1.9745 µs / 2.0098 µs |
| routing/hint_strip/no_hints | 76.252 ns / 77.242 ns / 78.479 ns |
| routing/hint_strip/single_hint | 252.56 ns / 257.06 ns / 262.21 ns |
| routing/hint_strip/two_hints | 351.99 ns / 359.59 ns / 368.58 ns |
| routing/write_detect/select | 51.517 ns / 51.928 ns / 52.390 ns |
| routing/write_detect/insert | 50.490 ns / 50.760 ns / 51.145 ns |
| routing/write_detect/update | 58.031 ns / 58.353 ns / 58.682 ns |
| routing/write_detect/delete | 58.417 ns / 58.677 ns / 59.023 ns |
| routing/write_detect/begin | 48.284 ns / 48.492 ns / 48.693 ns |
| routing/write_detect/create_table | 51.363 ns / 51.566 ns / 51.811 ns |
| routing/write_detect/with_cte | 53.119 ns / 53.376 ns / 53.637 ns |
| routing/route/read_no_hints | 2.4895 µs / 2.6418 µs / 2.7761 µs |
| routing/route/read_with_hint | 2.7294 µs / 2.7992 µs / 2.8725 µs |
| routing/route/write | 2.2714 µs / 2.3551 µs / 2.4344 µs |
| routing/route/complex_hints | 4.3332 µs / 4.4416 µs / 4.5497 µs |
| routing/node_select/batch_parse | 340.13 µs / 344.67 µs / 349.82 µs |

## 2026-08-29 — full-suite re-baseline (current tip `a6b47d1`)

Recorded per CLAUDE.md quality gate 3 at commit `a6b47d1` (main tip), host **gpc001ca**, `--features all-features`, fleet-locked + 24 GiB-bounded (same command as the header). Toolchain rustc/cargo **1.95.0**. This supersedes the 2026-07-15 numbers as the active baseline.

**Command:** `flock /home/gpc/HDB/sprint/coordination/build.lock systemd-run --user --scope --collect -p MemoryMax=24G -p MemorySwapMax=0 cargo bench --features all-features`

**No performance regression (gate-3 PASS).** No source or benched hot-path code changed between the prior baseline and `a6b47d1` (every intervening commit is demos/docs). Criterion's change report vs the stored run was the documented shared-host scatter: **28 improved / 40 within-noise / 20 regressed**, every delta small and spread across unrelated modules rather than localized to a changed path. Two full-run outliers — `journal/statement_type/other` (spiked to 122 ns) and `pool_mode/txn_event_detect/statement` (spiked to 152 ns) — were isolated and **re-run back-to-back**, both returning to baseline (32.4 ns / 15.8 ns); confirmed co-tenant CPU-spike artifacts, and the corrected values are recorded below. Host load rose 2.1 to 3.0 during the run, the direct cause of the scatter.

### benches/pooling.rs  (11 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| pool/acquire_release/contention/2 | 21.155 µs / 21.494 µs / 21.789 µs |
| pool/acquire_release/contention/32 | 339.95 µs / 350.41 µs / 363.36 µs |
| pool/acquire_release/contention/8 | 80.761 µs / 82.772 µs / 85.124 µs |
| pool/acquire_release/single | 577.45 ns / 584.81 ns / 593.58 ns |
| pool/metrics/read_metrics | 4.8980 ns / 4.9689 ns / 5.0498 ns |
| pool/node_endpoint/address | 116.97 ns / 118.09 ns / 119.43 ns |
| pool/node_endpoint/create | 724.89 ns / 728.27 ns / 731.94 ns |
| pool/node_endpoint/node_id | 711.91 ns / 716.64 ns / 721.75 ns |
| pool/throughput/sequential_acquire/1 | 727.34 ns / 735.42 ns / 744.57 ns |
| pool/throughput/sequential_acquire/10 | 6.9759 µs / 7.0210 µs / 7.0765 µs |
| pool/throughput/sequential_acquire/50 | 35.333 µs / 35.730 µs / 36.218 µs |

### benches/protocol.rs  (21 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| protocol/backend_response/auth_md5 | 38.018 ns / 38.828 ns / 39.801 ns |
| protocol/backend_response/auth_sasl | 143.81 ns / 145.29 ns / 147.00 ns |
| protocol/backend_response/command_complete | 134.42 ns / 136.98 ns / 139.74 ns |
| protocol/backend_response/error_response | 489.84 ns / 493.98 ns / 498.73 ns |
| protocol/decode_message/kilobyte_in_list | 128.50 ns / 130.18 ns / 131.90 ns |
| protocol/decode_message/medium_where | 63.214 ns / 63.755 ns / 64.401 ns |
| protocol/decode_message/short_select | 63.372 ns / 64.150 ns / 65.053 ns |
| protocol/decode_startup/cancel_request | 35.179 ns / 36.089 ns / 37.110 ns |
| protocol/decode_startup/ssl_request | 25.856 ns / 26.080 ns / 26.354 ns |
| protocol/decode_startup/startup_params | 805.55 ns / 819.22 ns / 834.44 ns |
| protocol/encode_message/kilobyte_in_list | 76.013 ns / 77.081 ns / 78.304 ns |
| protocol/encode_message/medium_where | 24.187 ns / 24.420 ns / 24.664 ns |
| protocol/encode_message/short_select | 20.907 ns / 21.252 ns / 21.666 ns |
| protocol/extended_parse/bind_message | 218.14 ns / 222.99 ns / 228.54 ns |
| protocol/extended_parse/parse_message | 168.71 ns / 170.73 ns / 172.96 ns |
| protocol/query_text/kilobyte_in_list | 337.83 ns / 342.06 ns / 346.81 ns |
| protocol/query_text/medium_where | 41.237 ns / 41.667 ns / 42.150 ns |
| protocol/query_text/short_select | 11.912 ns / 11.964 ns / 12.028 ns |
| protocol/tag_dispatch/contains_ci | 21.863 ns / 22.003 ns / 22.182 ns |
| protocol/tag_dispatch/from_tag | 12.831 ns / 12.918 ns / 13.020 ns |
| protocol/tag_dispatch/starts_with_ci | 5.2622 ns / 5.3341 ns / 5.4222 ns |

### benches/relay.rs  (42 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| journal/entries_in_window/scan_500 | 45.452 µs / 46.193 µs / 47.113 µs |
| journal/manager/begin_log_commit | 426.06 ns / 431.50 ns / 437.92 ns |
| journal/manager_contention/2 | 14.526 µs / 15.407 µs / 16.407 µs |
| journal/manager_contention/32 | 116.68 µs / 120.08 µs / 123.31 µs |
| journal/manager_contention/8 | 24.899 µs / 25.755 µs / 26.659 µs |
| journal/rollback_to_savepoint/128 | 6.9538 µs / 7.1360 µs / 7.3274 µs |
| journal/rollback_to_savepoint/16 | 1.2131 µs / 1.2516 µs / 1.2865 µs |
| journal/statement_type/ddl | 35.527 ns / 36.088 ns / 36.610 ns |
| journal/statement_type/delete | 33.979 ns / 34.334 ns / 34.764 ns |
| journal/statement_type/insert | 25.660 ns / 25.905 ns / 26.201 ns |
| journal/statement_type/other | 32.255 ns / 32.403 ns / 32.599 ns |
| journal/statement_type/select | 21.466 ns / 21.731 ns / 22.065 ns |
| journal/statement_type/set | 32.387 ns / 32.689 ns / 33.036 ns |
| journal/statement_type/txn | 24.410 ns / 24.831 ns / 25.300 ns |
| journal/statement_type/update | 28.883 ns / 29.335 ns / 29.866 ns |
| pool_mode/manager_acquire_release/2 | 18.843 µs / 19.414 µs / 19.958 µs |
| pool_mode/manager_acquire_release/32 | 177.88 µs / 178.66 µs / 179.33 µs |
| pool_mode/manager_acquire_release/8 | 44.686 µs / 45.736 µs / 46.930 µs |
| pool_mode/on_statement_complete/txn_sequence | 268.19 ns / 270.15 ns / 272.58 ns |
| pool_mode/prepared_parse/deallocate/all | 52.751 ns / 53.483 ns / 54.366 ns |
| pool_mode/prepared_parse/deallocate/named | 88.060 ns / 88.469 ns / 88.976 ns |
| pool_mode/prepared_parse/prepare/named | 123.70 ns / 124.70 ns / 125.81 ns |
| pool_mode/prepared_parse/prepare/typed | 243.64 ns / 246.95 ns / 250.77 ns |
| pool_mode/statement_safety/is_safe/safe_select | 37.367 ns / 37.760 ns / 38.202 ns |
| pool_mode/statement_safety/is_safe/safe_set_local | 97.796 ns / 98.757 ns / 99.734 ns |
| pool_mode/statement_safety/is_safe/unsafe_listen | 25.400 ns / 25.751 ns / 26.157 ns |
| pool_mode/statement_safety/is_safe/unsafe_prepare | 26.347 ns / 26.635 ns / 26.995 ns |
| pool_mode/statement_safety/is_safe/unsafe_set | 139.52 ns / 141.28 ns / 143.20 ns |
| pool_mode/statement_safety/warning/safe_select | 37.462 ns / 37.855 ns / 38.344 ns |
| pool_mode/statement_safety/warning/safe_set_local | 115.99 ns / 117.53 ns / 119.42 ns |
| pool_mode/statement_safety/warning/unsafe_listen | 25.506 ns / 25.770 ns / 26.093 ns |
| pool_mode/statement_safety/warning/unsafe_prepare | 27.362 ns / 27.720 ns / 28.137 ns |
| pool_mode/statement_safety/warning/unsafe_set | 148.99 ns / 150.47 ns / 152.06 ns |
| pool_mode/txn_event_detect/begin | 8.8294 ns / 8.8951 ns / 8.9907 ns |
| pool_mode/txn_event_detect/commit | 9.9030 ns / 9.9581 ns / 10.023 ns |
| pool_mode/txn_event_detect/release | 14.258 ns / 14.377 ns / 14.518 ns |
| pool_mode/txn_event_detect/rollback | 15.783 ns / 16.001 ns / 16.260 ns |
| pool_mode/txn_event_detect/rollback_to | 23.197 ns / 23.381 ns / 23.612 ns |
| pool_mode/txn_event_detect/savepoint | 18.379 ns / 18.661 ns / 18.965 ns |
| pool_mode/txn_event_detect/start_txn | 24.019 ns / 24.529 ns / 25.188 ns |
| pool_mode/txn_event_detect/statement | 15.723 ns / 15.782 ns / 15.857 ns |
| switchover/buffer_query/enqueue_one | 6.7398 µs / 6.8143 µs / 6.9012 µs |

### benches/routing.rs  (18 benches)

| Benchmark | Time [lower / median / upper] |
|---|---|
| routing/hint_parse/complex_query | 1.9147 µs / 1.9328 µs / 1.9549 µs |
| routing/hint_parse/multiple_hints | 3.4589 µs / 3.4977 µs / 3.5451 µs |
| routing/hint_parse/no_hints | 95.934 ns / 97.099 ns / 98.596 ns |
| routing/hint_parse/single_hint | 952.20 ns / 976.01 ns / 997.16 ns |
| routing/hint_strip/no_hints | 76.320 ns / 77.424 ns / 78.768 ns |
| routing/hint_strip/single_hint | 247.13 ns / 250.76 ns / 254.59 ns |
| routing/hint_strip/two_hints | 340.34 ns / 344.82 ns / 350.11 ns |
| routing/node_select/batch_parse | 356.43 µs / 362.42 µs / 369.07 µs |
| routing/route/complex_hints | 4.0418 µs / 4.0772 µs / 4.1170 µs |
| routing/route/read_no_hints | 1.7537 µs / 1.8204 µs / 1.8923 µs |
| routing/route/read_with_hint | 1.8846 µs / 1.9990 µs / 2.1378 µs |
| routing/write_detect/begin | 47.651 ns / 47.865 ns / 48.190 ns |
| routing/write_detect/create_table | 51.768 ns / 51.963 ns / 52.183 ns |
| routing/write_detect/delete | 60.065 ns / 60.398 ns / 60.749 ns |
| routing/write_detect/insert | 51.221 ns / 51.666 ns / 52.192 ns |
| routing/write_detect/select | 51.324 ns / 51.526 ns / 51.731 ns |
| routing/write_detect/update | 57.175 ns / 57.457 ns / 57.884 ns |
| routing/write_detect/with_cte | 53.046 ns / 53.216 ns / 53.414 ns |

_Total: 92 benches across the 4 harnesses._


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

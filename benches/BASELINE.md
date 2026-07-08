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

This suite covers the pool skeleton and routing-hint paths only — none of the PG-wire
protocol / relay hot path that the 2026-07 perf program optimized. Backlog item P1-12
adds `benches/protocol.rs` for that; extend this file in the same session it lands.
The proxy-path scalability harness (`scripts/regress/bench-scalability.sh`) has its own
evidence baselines in `docs/perf-2026-07/README.md` and is always re-measured
back-to-back with the candidate, never compared across days.

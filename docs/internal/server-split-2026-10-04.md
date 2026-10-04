# Server module split: final validation

Sprinter item: `577025a06e45`. This is a move-only refactor of `src/server.rs`
into `src/server/`, with public `crate::server::` paths preserved. Twenty commits
separate the module moves and test moves. Runtime behavior, feature defaults and
configuration are unchanged. The final documentation commit updates the current
source references and changelog; historical reports retain their original paths.

## Source and evidence

- Baseline: `721e0d4be65a759f6002163da75d04bf391d91f7`.
- Tested refactor: `0e7ee896a180c8002851ce869dd5ac287ef4c571`.
- Candidate worktree: `/home/gpc/HDB/Proxy-wt-server-split-20261002`.
- Baseline worktree: `/home/gpc/HDB/Proxy-base-server-split-20261002`.
- Evidence root: `/home/gpc/HDB/sprint/baselines/proxy/server-split-20261002`.
- Original final gate: `final-gate.sh`, `final/results.jsonl`, and the matching
  logs under `final/`. The candidate was clean at both recorded boundaries:
  2026-10-03 18:07:06 UTC through 20:43:58 UTC.
- Per-move proof material: `proofs/`, `split_move.py`, `split_methods.py`,
  `split_test_modules.py`, and `steps/results.jsonl`.
- Takeover and independent review: `takeover-20261004/` under the evidence root.

The original gate ran under the shared fleet lock with a 24 GiB memory limit,
no swap, two Cargo jobs and separate target directories. Its wrapper records every
exit code rather than stopping on the first failed step; all 24 exit-bearing
stages actually returned zero. The product classification is recorded separately.

The takeover source audit passed 34 comparisons covering the extracted modules,
test bodies and reconstructed test declarations. All 169 test attributes remain.
The current history contains 20 refactor commits; the intermediate ledger records
passing check/Clippy/test stages for commits 2–19, with the initial limits gate
and full final gate covering the boundary steps. Text normalization permits
formatting and inserted visibility; it supplements review and runtime tests,
rather than constituting a formal equivalence proof.

## Test and build results

Check, Clippy with `--tests -- -D warnings`, and tests passed on Rust 1.99.0 for
all five profiles. Library counts below distinguish unit coverage from auxiliary
suite totals.

| Profile | Library passed | Main / config passed | Ordinary integration passed / ignored |
|---|---:|---:|---:|
| no default features | 541 | 11 / 4 | 47 / 3 |
| default | 625 | 11 / 4 | 47 / 3 |
| ha-tr | 625 | 11 / 4 | 47 / 3 |
| all-features | 1872 | 11 / 4 | 48 / 3 |
| all-features,postgres-topology | 1875 | 11 / 4 | 48 / 3 |

Formatting, the Rust 1.86 locked MSRV check, and benchmark compilation passed.
There is one passing doctest per profile; ignored doctests number 0/1/1/12/12
in table order. Three ordinary integration placeholders remain ignored. Five
optional external-WASM test functions reported success in full-feature builds,
but can return early without sibling plugin artifacts; those counts are not
proof of five plugin executions. The independent daemon WASM fixtures below
supply actual runtime evidence. The plugin binary has no unit tests.

The dedicated live gate built its daemon with Rust 1.98.1 and used two owned,
disposable PostgreSQL 18.4 containers:

- Strict live integration: 48 passed, zero failures or ignored tests; three empty
  infrastructure placeholders explicitly filtered.
- Live regression battery: 9 passed.
- Daemon fixtures: 2 topology, 5 deployment CLI, and 3 WASM cases passed.
- Real PostgreSQL TR commit-outcome suites: 7 commit and 13 stream cases passed.
- Docker-free TR boundary suite: 26 passed.
- Cleanup evidence records removal of both owned PostgreSQL containers.

## Performance and move-only scope

The full four-target Criterion gate used the recorded default Rust 1.95.0
compiler, three interleaved rounds per arm, and the unchanged 3% aggregate / 2%
separated-case thresholds. All **118 cases** are present in every round:
**708 observations**, with **2,124 raw estimate/metadata/sample JSON files**.
Independent review checked full-ID/directory bindings, raw sample medians,
50/100 sample counts, eight executable hashes and all executed paths.

The recomputed verdict is **PASS**: mean per-case change **−0.6645099398%**,
median **−0.5955184992%**, **zero separated regressions**, and six separated
improvements. No cases were removed or timing thresholds changed.

The release product binaries have 20,229 matching normalized function bodies
and no unmatched bodies. This classification removes address-dependent operands
and compares FDE-body multisets: it corroborates the source-move proofs; it is
not byte identity or a formal whole-program equivalence proof. The benchmark
executables are also classified as body-identical.

The item's explicit acceptance condition says no additional user-path run is
needed when the move-only gate is clean. That condition applies here; no new
user-path performance result is claimed. Releases and future write-path behavior
changes retain the repository's ordinary user-path gate requirement.

## Documentation completion after the gate

The final changes after the tested refactor are Markdown and Rust/shell comments.
Every changed non-Markdown line was independently checked to be a comment, with
line counts and executable lines unchanged. The embedded `.claude/skills` bundle
is byte-for-byte unchanged from the tested refactor, as are Cargo manifests,
lockfile, test code, benchmarks and build configuration. The optional embedded
skill-source-link refresh is preserved separately as
`takeover-20261004/embedded-skill-links-followup.patch`; it is outside this refactor.

Fresh `cargo +stable fmt --check`, `git diff --check`, and shell syntax checks
passed after the documentation changes. No large gate was repeated for these
non-executable edits. The original evidence and failed exploratory attempts are
retained rather than overwritten.

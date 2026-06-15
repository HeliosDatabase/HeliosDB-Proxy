# BATCH D — WASM plugin runtime performance

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Make plugin dispatch production-grade: pre-instantiated modules, bounded execution, single parse per query, lock-free metrics.

**Parallel-execution compatibility:** SOLO-friendly; can run in PARALLEL with E/F/G/H, and with A if hook-site edits are coordinated.

**Files touched:** `src/plugins/runtime.rs`, `src/plugins/mod.rs`, `src/plugins/metrics.rs` (+ call sites in `src/server.rs` only for signature changes)

**Conflicts with:** Touches `src/server.rs` only at hook call sites — coordinate with whoever holds A/B/C or execute after them. Internally independent of all other batches.

**Acceptance criteria:**

- Per-hook dispatch cost drops to Store::new + instantiate_pre (measure with `benches/` or a microbench: target <10µs per no-op hook).
- A plugin that loops forever is killed at its configured `timeout` (epoch ticker enforced) without blocking the reactor.
- `cargo test --features wasm-plugins` and `tests/wasm_plugin_e2e.rs` green.

---

### Full module re-instantiation, fresh Store, fresh Linker, and host-import re-registration on every hook call

- **Location:** `src/plugins/runtime.rs:431`
- **Severity / category:** high / architecture
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let mut linker: Linker<StoreCtx> = Linker::new(&self.engine);
        register_kv_imports(&mut linker)?;
        register_crypto_imports(&mut linker)?;
        let instance = linker.instantiate(&mut store, &plugin.module)
```

**Impact:** Every hook invocation on every query (pre_query, route, post_query — up to 3 per query per plugin) builds a new Store, allocates a new Linker, re-wraps 4 host functions via func_wrap, instantiates the module (linear-memory allocation + table/global init), then does two get_typed_func lookups for alloc/dealloc plus one for the hook. This is the dominant per-dispatch cost — tens of microseconds plus several heap allocations per hook call, versus ~1µs for a pre-instantiated call. It also clones plugin.metadata.name into StoreCtx (runtime.rs:409) per call.

**Fix:** Build the Linker once per runtime (it is Send+Sync) and compute a wasmtime::InstancePre per plugin at load time via linker.instantiate_pre(&module); store it on LoadedPlugin. Per call, only Store::new + instance_pre.instantiate remain (wasmtime fast-path, no export-name hash lookups for imports). Cache the TypedFunc lookups' export indices, or go further with a small per-plugin pool of (Store, Instance) reset between calls. Expected: order-of-magnitude reduction in per-hook dispatch latency and allocation count.

**Verifier correction/nuance:** Two minor refinements: (1) the per-call cost is conditional — zero when wasm-plugins is off or no plugins are loaded, and pre-query/route hooks only inspect simple-query (Query) messages, not extended protocol (Parse/Bind/Execute pass through, server.rs:1679-1681); (2) TypedFunc handles cannot be cached across Stores directly — the right fix is caching Module export indices, which the suggestion already implies. Also, observer-ABI hooks pay an extra failed typed-func probe per call (runtime.rs:466→487), making the finding slightly understated for that case.

<details><summary>Verifier reasoning</summary>

Confirmed line-by-line. runtime.rs call_hook builds a fresh Store (line 412, with plugin.metadata.name.clone() at 409), a fresh Linker (431), re-registers host imports every call (432-433; exactly 4 func_wrap sites in host_imports.rs at lines 95/125/150/190), instantiates the module per call (434-435), and does per-call get_typed lookups for alloc (450), dealloc (451), and the hook export (466) — with an additional wasted failed-probe + second lookup (487) for observer-ABI hooks, slightly worse than the finding states. LoadedPlugin (runtime.rs:114-142) caches only the compiled Module; grep confirms no InstancePre/Linker reuse anywhere. Hot path confirmed: server.rs:789 (apply_pre_query_hook in the per-message client loop), 1137 (apply_route_hook), 1959 (execute_post_query) fan out per plugin through mod.rs:471/615/534 into call_hook — up to 3 instantiations per query per plugin, as claimed. The suggestion is valid for wasmtime 26: Linker::instantiate_pre/InstancePre is Send+Sync and exists in that version; the fresh per-call Store (the actual isolation/fuel boundary per the code's own comments at 365-379) is retained, so correctness is preserved; hot-reload recreates LoadedPlugin (mod.rs:449-459) so a cached InstancePre would be rebuilt correctly. Caveats that scope but do not refute: the cost only exists when the wasm-plugins feature is compiled, a plugin manager is configured, and plugins are loaded; and pre-query/route hooks fire only for simple-protocol Query messages (extended protocol bypasses, server.rs:1664-1666). Within the plugins-wasm subsystem — which ships 8 first-party query-path plugins — this is genuine per-query overhead and fixing it would materially cut per-hook dispatch latency and allocations.

</details>


### Synchronous WASM execution blocks tokio worker threads with epoch deadline effectively disabled and configured timeout never enforced

- **Location:** `src/plugins/runtime.rs:426`
- **Severity / category:** high / async
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
store.set_epoch_deadline(u64::MAX);
```

**Impact:** call_hook is a synchronous function executed directly inside the async client-session loop (server.rs:789 apply_pre_query_hook, server.rs:1137 apply_route_hook, server.rs:847 fire_post_query_hook). The epoch deadline is set to u64::MAX (the code comment admits real wall-clock enforcement is deferred), so the only bound is fuel — and if an operator sets fuel_metering=false there is no bound at all. PluginRuntimeConfig.timeout (default 100ms, config.rs) is copied into the sandbox but never enforced anywhere in call_hook. A slow or looping plugin stalls the tokio worker thread, adding tail latency to every other connection multiplexed on that worker; at default fuel_limit=1_000_000 a plugin can still legally burn ~hundreds of microseconds to milliseconds per call on the reactor thread.

**Fix:** Spawn a background epoch ticker (engine.increment_epoch() every ~1ms) and set store.set_epoch_deadline(timeout_ticks) so config.timeout is actually enforced. Either keep hook calls inline but bounded to sub-millisecond epochs, or wrap call_hook in tokio::task::spawn_blocking (or use wasmtime async support with epoch_deadline_async_yield_and_update) so plugin CPU time never blocks the reactor. Benefit: bounded p99 under misbehaving plugins and no cross-connection head-of-line blocking.

**Verifier correction/nuance:** Finding is essentially fully correct. Minor precision points: (a) the hooks only fire for simple-protocol Query messages (extended protocol Parse/Bind/Execute bypasses them, server.rs:1665-1681), so prepared-statement-heavy workloads are unaffected today; (b) with default fuel metering on, a runaway plugin traps after ~1M fuel rather than stalling indefinitely — the indefinite stall requires fuel_metering=false; (c) additional supporting overhead not in the finding: call_hook re-instantiates the module via Linker::instantiate on every invocation (runtime.rs:434), compounding the synchronous per-query cost.

<details><summary>Verifier reasoning</summary>

Every element of the finding verified against the code. (1) runtime.rs:426 contains `store.set_epoch_deadline(u64::MAX);` and the adjacent comment (lines 419-425) explicitly admits wall-clock enforcement is deferred to a future commit; epoch_interruption(true) is set at engine init (line 253) but no `increment_epoch` call exists anywhere in src/, so epoch interruption is fully inert. (2) call_hook (runtime.rs:380) is a synchronous fn with no spawn_blocking anywhere in the crate; it additionally does a fresh Store + linker.instantiate per call (line 434). It is invoked per simple-Query message from the async session loop: server.rs:789 (apply_pre_query_hook), :847 (fire_post_query_hook), :1137 (apply_route_hook), via sync helpers at server.rs:1667/1879/1924 and plugin-manager fan-out loops in plugins/mod.rs. main.rs:117 uses #[tokio::main] (multi-thread runtime), so plugin CPU runs on shared reactor workers — per-query hot path, not cold path. (3) config.timeout (default 100ms, config.rs:66; TOML timeout_ms at config.rs:91) is copied into ResourceLimits.max_execution_time (runtime.rs:329) and SecurityPolicy (runtime.rs:263) but grep confirms max_execution_time is never read for enforcement; no tokio::time::timeout wraps any hook call (all timeout wrappers in server.rs are network I/O). (4) fuel_metering defaults true with fuel_limit=1_000_000 (config.rs:67-68) but is TOML-disableable (config.rs:95), and runtime.rs:413 only sets fuel when metering is on — so the unbounded-when-disabled claim is correct. (5) The suggestion (epoch ticker + deadline derived from config.timeout, or spawn_blocking/async wasmtime) is the standard wasmtime pattern, is not implemented anywhere else, and is correctness-safe because hook errors already fail open (pre-query errors logged and treated as Continue, mod.rs:485-498). Could not refute. Caveat: impact requires the wasm-plugins feature plus loaded plugins registering these hooks, and with default fuel metering the per-call stall is bounded to roughly sub-millisecond scale per plugin per query; the unbounded worker-thread hang requires an operator to set fuel_metering=false — the finding states both conditions accurately. Since the project ships eight first-party plugins as a headline feature, fixing this would plausibly improve p99 latency and prevent cross-connection head-of-line blocking in plugin-enabled deployments.

</details>


### Same query parsed and QueryContext rebuilt three times per query; post-query JSON payload re-serialized once per plugin inside the fan-out loop

- **Location:** `src/plugins/mod.rs:522`
- **Severity / category:** medium / allocation
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
for plugin_name in plugin_names {
            if let Some(plugin) = self.plugins.get(&plugin_name) {
                ...
                let payload = match serde_json::to_vec(&(ctx, outcome)) {
```

**Impact:** With at least one plugin loaded, each Query message triggers three independent QueryMessage::parse + build_query_context invocations (pre-query at server.rs:1688, route at server.rs:1897, post-query at server.rs:1942), each copying the full SQL twice and generating a fresh UUID — so the three hook stages see three different request_ids for the same query, defeating correlation as well as wasting allocations. In execute_post_query the serde_json::to_vec of (ctx, outcome) sits inside the per-plugin loop, so N post-query plugins cause N identical serializations. call_pre_query/call_route also re-serialize the same ctx JSON per plugin (runtime.rs:526, 574).

**Fix:** Build one QueryContext (and its serialized JSON bytes) per query message up front and pass it by reference through all three hook stages; hoist the serde_json::to_vec out of the plugin loops. Consider serializing once into a reusable per-session buffer. Benefit: removes 2 parses, 4 SQL copies, 2 UUIDs and N-1 JSON serializations per query, and gives all hooks a consistent request_id.

**Verifier correction/nuance:** Three minor refinements: (1) PreQueryResult::Rewrite (server.rs:1693-1696) can replace the SQL between the pre stage and route/post stages, so a single shared QueryContext must refresh its query/normalized fields (and invalidate any cached JSON) after a rewrite while keeping the request_id stable — the suggestion as written ('pass it by reference through all three stages') needs this caveat. (2) The waste is slightly broader than 'with at least one plugin loaded': the parse + context build + UUID happen whenever plugin_manager is Some, even if zero plugins registered the specific hook, since execute_pre_query/execute_route/execute_post_query check the hooks map only after the ctx is built. (3) For pre-query and route stages, 'N serializations' is an upper bound because those loops short-circuit on the first non-Continue/non-Default result; only post-query always fans out to all N plugins.

<details><summary>Verifier reasoning</summary>

Every factual claim verified in code. (1) Triple parse/context: server.rs:1683+1688 (apply_pre_query_hook), 1893+1897 (apply_route_hook), 1938+1942 (fire_post_query_hook) each independently run QueryMessage::parse(msg.payload.clone()) + build_query_context; all three are on the same per-Query hot path (main client loop: line 789 pre, 837->1137 route, 847 post). QueryMessage::parse (protocol.rs:362) allocates a full String of the SQL and the call sites deep-copy the payload via BytesMut::clone, so the finding actually undercounts copies. build_query_context (server.rs:1793-1804) makes two more SQL String copies and calls HookContext::default(), which generates uuid::Uuid::new_v4().to_string() (mod.rs:209) — confirmed three distinct request_ids per query, and the custom Serialize impl (runtime.rs:626+) ships hook_context to plugins, so cross-hook correlation is genuinely broken. (2) Per-plugin re-serialization: mod.rs:522 serde_json::to_vec(&(ctx, outcome)) is inside the plugin loop (516-558) with ctx/outcome immutable, so N post-query plugins do N identical serializations; call_pre_query (runtime.rs:526) and call_route (runtime.rs:574) likewise serialize ctx once per plugin from loops at mod.rs:471/615. The serialized QueryContext embeds the SQL twice (query + normalized are identical full copies), so for large queries the redundant bytes multiply. Hot path: fires for every simple-Query message whenever plugin_manager is Some (wasm-plugins enabled), even if no plugin registered that hook. The suggestion is valid: hoisting to_vec out of the loops is trivially safe; sharing one ctx across stages works with one caveat (see correction). matters=true but modest: each hook call creates a fresh wasmtime Store+Linker+instantiate per invocation (runtime.rs:405-441), which dominates per-plugin cost, so absolute gains are bounded — but removing 2 parses, ~4-6 SQL-sized copies, 2 UUIDs, and (N-1) JSON serializations per query per stage is a real hot-path allocation reduction, measurable at high QPS with multiple plugins and large SQL texts; the request_id unification is also a genuine observability fix.

</details>


### Two global RwLock write acquisitions in PluginMetrics::record_hook_call serialize all connections on every hook call

- **Location:** `src/plugins/metrics.rs:55`
- **Severity / category:** medium / locking
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
let mut stats = self.plugin_stats.write();
            let entry = stats
                .entry(plugin_name.to_string())
                .or_insert_with(PluginStatsInner::new);
```

**Impact:** record_hook_call runs after every hook invocation on every query (mod.rs:473, 534, 573, 617) and takes a process-global parking_lot::RwLock write on plugin_stats and a second one on hook_latencies (metrics.rs:89), plus a plugin_name.to_string() allocation per call for the entry lookup. Under multi-core load with plugins enabled, all client sessions funnel through these two write locks per hook stage — a contention point that scales inversely with QPS. LoadedPlugin::record_invocation adds a third per-call write lock (runtime.rs:211 last_invoked), and call_hook ends with two more separate instance_data.write() acquisitions (runtime.rs:510, 513).

**Fix:** Replace the locked HashMaps with a DashMap keyed by plugin name holding atomic counters (calls, errors, latency-sum, min/max via fetch_update), or pre-resolve an Arc<PluginStatsInner-with-atomics> per plugin at load time so the hot path is lock-free atomics only. Merge the two instance_data writes in call_hook into one. Benefit: removes 4-5 lock acquisitions and a String allocation per hook call.

**Verifier correction/nuance:** Two details need adjustment: (1) "two more separate instance_data.write() acquisitions (runtime.rs:510, 513)" — line 510 only executes when config.fuel_metering is enabled; without fuel metering there is one unconditional write (line 513). (2) The finding misses the most expensive part: inside the hook_latencies write lock, LatencyHistogram::record sorts a Vec of up to 10,000 Durations on every call (metrics.rs:379), making the serial section microseconds rather than nanoseconds; any fix must replace this with a bucketed/atomic histogram, not just atomic counters. Also note per-call latency is dominated by per-call wasmtime Store/Linker creation and instantiation in call_hook, so the benefit of the lock fix is throughput/scalability under concurrent load, not per-call latency.

<details><summary>Verifier reasoning</summary>

Every cited line checks out. metrics.rs:55-58 takes a global parking_lot RwLock write on plugin_stats with a plugin_name.to_string() allocation per call; metrics.rs:89-93 takes a second global write on hook_latencies; runtime.rs:211 (record_invocation, called from call_hook at runtime.rs:403) adds a third lock write; runtime.rs:510/513 add instance_data writes. Callers confirmed at mod.rs:473/486 (pre-query), 536/544 (post-query), 573/586 (authenticate), 617/630 (route), reached per client Query message from server.rs:789, 1898, 1959 and per connection from server.rs:1846 — so this is genuinely on the per-query hot path whenever wasm-plugins is built and at least one plugin is loaded (the proxy ships 8 first-party plugins; with zero plugins the executors exit before touching the locks, cost is zero). The finding actually UNDERSTATES the problem: the second lock's critical section includes LatencyHistogram::record (metrics.rs:370-385), which calls self.latencies.sort() on a Vec of up to 10,000 entries on every hook call while holding the global write lock — a multi-microsecond exclusive serial section that dwarfs the lock acquisition cost and is the real contention driver. The suggestion is valid: the locks guard only metric aggregates (readers are the Prometheus exporter and admin endpoints at metrics.rs:130-182, mod.rs:655), so atomics/DashMap introduce no correctness hazard; the percentile histogram would additionally need a bucketed atomic design, which the suggestion does not address. Caveat on magnitude: per-call latency is dominated by call_hook's fresh Store + Linker + linker.instantiate per invocation (runtime.rs:408-441) plus serde_json marshalling, so the fix improves multi-core throughput/scalability under load rather than single-query latency. The instantiation work parallelizes across cores; the global write locks do not — that asymmetry is exactly why this matters at scale.

</details>


### Fuel metering on by default plus always-on epoch interruption double-instruments all plugin code while epochs provide no benefit

- **Location:** `src/plugins/runtime.rs:253`
- **Severity / category:** medium / build
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=True confidence=high

**Evidence (verbatim from code at audit time):**

```
engine_config.epoch_interruption(true);
```

**Impact:** The engine is built with epoch_interruption(true) unconditionally and consume_fuel(true) when fuel_metering is on (default true, fuel_limit 1_000_000 in config.rs defaults). Both options add per-loop-header / per-block instrumentation to every compiled plugin function — wasmtime documents fuel as the more expensive of the two (typically 10-30% combined slowdown on compute-heavy code). Since the epoch deadline is immediately set to u64::MAX (runtime.rs:426), the proxy pays the epoch-check cost on every plugin instruction stream and gets zero enforcement from it: all cost, no benefit.

**Fix:** Pick one mechanism: use epoch interruption with a real ticker for wall-clock timeouts (cheapest) and make fuel metering opt-in for deterministic-budget use cases, or drop epoch_interruption(true) until the deadline is actually wired. Recompiled plugin code will run measurably faster (single-digit to ~20% depending on workload) with identical observable behavior today.

**Verifier correction/nuance:** The finding is correct but the magnitude estimate needs tempering. call_hook creates a fresh Store, builds a new Linker, registers imports, and re-instantiates the module on every hook invocation (runtime.rs:412-441); for typical short hooks that per-call setup likely dominates total cost, so removing one instrumentation pass will land at the low single-digit end, not ~20%. Also, dropping epoch_interruption alone removes the cheaper of the two instrumentations (fuel is the expensive one and stays on by default), so the larger win would come from the fuel-vs-epoch choice the suggestion describes, not from the epoch removal by itself. The upper-bound numbers apply only to compute-heavy guest code (e.g., ai-classifier/column-mask style plugins). The per-call instantiation overhead is arguably the bigger per-query performance issue in this same function.

<details><summary>Verifier reasoning</summary>

Every factual claim checks out. runtime.rs:253 unconditionally enables epoch_interruption(true) on the shared wasmtime engine; runtime.rs:248-250 enables consume_fuel(true) when fuel_metering is on, and the default is true with fuel_limit 1_000_000 (src/config.rs:293-294, src/plugins/config.rs:68-69). runtime.rs:426 sets store.set_epoch_deadline(u64::MAX) on every call, and the adjacent comment (lines 419-425) concedes the deadline 'effectively disables the interrupt' and that 'real time enforcement happens via fuel.' Grep confirms no increment_epoch call exists anywhere in the repo — no ticker, so epoch checks can never trip. In wasmtime 26 both options are compile-time instrumentation on the shared Engine: epoch checks at function entries/loop headers plus per-block fuel accounting are baked into every plugin module, so all plugin guest code is double-instrumented while epochs enforce nothing. This is on the per-query path: server.rs:789/1690 (PreQuery), server.rs:847/1959 (PostQuery), plus Route/Authenticate hooks (runtime.rs:531-579) run inline in query dispatch when wasm-plugins is enabled, and the eight first-party plugins use these hooks. The suggestion is safe: removing epoch_interruption today is behavior-identical (fuel still traps runaway loops), and the epoch-ticker-plus-opt-in-fuel alternative matches wasmtime's documented guidance. Matters=true because plugin guest execution contributes directly to query latency for deployments using plugins, though only those deployments benefit (feature-gated, plugin_manager is Option).

</details>


### Hot-reload path recompiles synchronously on every reload because the module cache is write-only, re-reads the trust root per load, and has an unload-before-compile visibility gap

- **Location:** `src/plugins/runtime.rs:354`
- **Severity / category:** medium / io
- **Found by:** `plugins-wasm` auditor
- **Adversarial verification:** isReal=True matters=False confidence=high

**Evidence (verbatim from code at audit time):**

```
// Cache the compiled Module (cheap clone on hit).
        {
            let mut cache = self.module_cache.write();
            cache.insert(manifest.path.clone(), module.clone());
        }
```

**Impact:** module_cache is inserted into at instantiate but never consulted — the only read is .len() in stats() (runtime.rs:595) — so the comment 'Avoids re-compiling the same .wasm on every load' (runtime.rs:232) is false: every load/reload runs Module::from_binary (full cranelift compile, easily 50-500ms for a real plugin) on the calling thread. PluginManager::reload_plugin (mod.rs:449-459) unloads first, then loads: during the entire file-read + Ed25519 verify + SHA-256 + compile window, queries silently skip the plugin (policy plugins like column-mask or llm-guardrail stop enforcing). load_plugin also rebuilds SignatureVerifier::from_trust_root from disk on every single load (mod.rs:395-398). If check_updates is driven from an async context, the compile blocks that executor thread; note also that check_updates currently has no callers outside src/plugins, so directory-watch hot reload is effectively unwired.

**Fix:** Key the module cache by content hash and check it before compiling; compile and verify the replacement module fully before removing the old plugin, then atomically swap the DashMap entry (load-then-swap instead of unload-then-load) so there is no enforcement gap. Cache the SignatureVerifier on the manager. Run compilation via spawn_blocking. Benefit: zero-downtime reloads and no executor stalls during plugin updates.

**Verifier correction/nuance:** Severity should be low (latent/dead-path), not medium: the entire hot-reload path is unreachable because check_updates has no callers, and startup loads each plugin once so neither the recompile nor the verifier rebuild ever repeats in production. Also, the suggestion's phrase "check it before compiling" is unsafe as stated for the existing path-keyed cache: a reload fires because the file at the same path changed, so a path-keyed cache hit would serve the stale old module — the content-hash keying is mandatory, and even then it only helps the unchanged-file case since changed content must compile anyway. The valuable parts of the suggestion are load-then-swap (closing the enforcement gap) and spawn_blocking, both of which only become relevant once check_updates is actually wired to a background task or admin endpoint.

<details><summary>Verifier reasoning</summary>

Every factual claim verifies against the code: module_cache (runtime.rs:233) is insert-only at runtime.rs:353-354 with its sole read being .len() in stats() at runtime.rs:595; Module::from_binary at runtime.rs:347 runs unconditionally on every instantiate, making the comment at runtime.rs:231-232 false. reload_plugin (mod.rs:449-459) unloads (DashMap remove at mod.rs:430, hooks pruned at 432-435) before load_plugin re-reads/verifies/compiles, so execute_pre_query (mod.rs:462-503) would silently skip the plugin during the window. SignatureVerifier::from_trust_root is rebuilt from disk per load (mod.rs:395-398, loader.rs:219-249). However, it does not matter for production performance: check_updates (mod.rs:681) has zero callers repo-wide (src/, tests/, benches/), so hot reload is dead code; the only wired caller of load_plugin is startup preload (server.rs:352), which loads each path exactly once — a cache would get zero hits and the trust-root re-read is a one-time trivial cost. There is no admin reload endpoint (admin.rs:406 only exposes read-only GET /plugins). The per-query hook path (call_hook at runtime.rs:435) uses plugin.module from LoadedPlugin and never touches module_cache, so query latency/throughput are unaffected. Net: a real dead-code/latent-design bug worth fixing before hot reload is ever wired (the docs advertise hot reload), but fixing it today changes no production latency, throughput, or scalability metric, and the enforcement-gap risk is unreachable in the shipped binary.

</details>


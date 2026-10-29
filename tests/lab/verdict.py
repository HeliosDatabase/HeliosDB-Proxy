#!/usr/bin/env python3
"""Judge one HA-lab run from its output directory (check.json, outcomes.tsv,
promoted_ms). Prints one line per property and PASS/FAIL; exits 1 on FAIL.

Properties (every scenario):
  balance_conserved      sum(balance) on the new primary equals the start total
  no_acknowledged_loss   every transfer the client saw COMMIT for is present
  no_phantom_write       no transfer the client saw fail (ERROR / not sent) is present
  writes_resume_<proxy>  both proxies committed transfers that started after the
                         promotion (writes followed the new primary, no config edit)
stop-old-primary and chaos-force-leader also:
  no_split_brain_ack     the returned (or never stopped), unfenced former primary
                         holds no acknowledged transfer the new primary lacks
chaos-force-leader also:
  override_dropped_leader_<proxy>  right after POST /api/chaos force_unhealthy,
                         that proxy's /topology named no primary
drain-restart also (promoted_ms = the instant proxy-a served again):
  drain_completed        proxy-a logged the drain as complete and exited
  drain_no_unknown       no transfer through proxy-a ended `unknown` (a drain finishes
                         in-flight work; new connections are refused, never cut)
stale-sighup also (promoted_ms = the instant after the second reload):
  broken_reload_refused  the unparsable file was refused and /nodes kept 3 nodes
  stale_reload_applied   the stale file was applied: /nodes dropped to 2 and the
                         provider's leader stayed pg-a
tls-rotation also (promoted_ms = the instant after the rotation):
  certificate_rotated    the fingerprint a new client sees changed with SIGHUP and
                         equals the new file's
edge-resume (C-03) instead: warm resume after a network cut replays the missed
invalidation and keeps the rest of the cache; a home restart flushes the edge.
reconnect-wave (H-05) instead:
  small_tenant_served        the small tenant's readers complete transactions in
                             every second before the kill and from RECOVER_S after
                             the promotion to the end
  small_tenant_no_aborts     no small-tenant client was aborted
  big_tenant_recovers        the big tenant reaches half its pre-kill throughput
                             within 30 s of the promotion
  reconnects_within_limit    the most reconnects one node had in progress at once
                             (heliosdb_proxy_backend_reconnects_peak) is above 0
                             (the wave went through the limit) and at most
                             max_concurrent_reconnects_per_node
  rss_bounded                proxy RSS at the end is within 64 MiB of before the kill
  rss_flat_after_failover    from RECOVER_S after the promotion to the end the
                             proxy's RSS varies by less than 16 MiB
  with a heavy tenant (pgbench-heavy.log, WAVE_HEAVY > 0; ac858d61400b) also:
  heavy_queries_within_cap   the heavy tenant's sampled active-query permits
                             peaked above 0 and at most its active-query limit
  heavy_cap_engaged          some heavy request waited at the cap
  heavy_replay_within_budget its sampled replay bytes peaked above one 64 KiB
                             statement and at most its replay-byte budget
  heavy_replay_budget_engaged the budget marked some transaction non-replayable
  heavy_replay_released      the final scrape shows 0 replay bytes held
  (heavy aborts are reported, not judged: pgbench aborts a client on 53300/57P01)
"""
import json, pathlib, re, sys

RECOVER_S = 15
PROGRESS = re.compile(r"progress: ([0-9.]+) s, ([0-9.]+) tps")


def tps_series(path):
    """{second: tps} from pgbench -P 1 output."""
    return {round(float(t)): float(tps) for t, tps in PROGRESS.findall(path.read_text())}


def reconnect_wave(out):
    start = int(out.joinpath("wave_start_ms").read_text())
    promoted = int(out.joinpath("promoted_ms").read_text())
    secs = int(out.joinpath("wave_secs").read_text())
    kill_file = out.joinpath("kill_ms")
    if kill_file.exists():  # recorded just before the kill was issued
        kill_s = (int(kill_file.read_text()) - start) / 1000
    else:  # older evidence: the kill came at least 2 s before the promotion
        kill_s = (promoted - start) / 1000 - 2
    prom_s = (promoted - start) / 1000
    big = tps_series(out.joinpath("pgbench-big.log"))
    small = tps_series(out.joinpath("pgbench-small.log"))
    heavy_log = out.joinpath("pgbench-heavy.log")
    tenants = ("big", "small", "heavy") if heavy_log.exists() else ("big", "small")
    aborted = {t: len(re.findall(r"aborted", out.joinpath(f"pgbench-{t}.log").read_text()))
               for t in tenants}
    before = list(range(2, int(kill_s) - 1))
    after = list(range(int(prom_s) + RECOVER_S, secs))
    pre_big = sum(big.get(t, 0) for t in before) / max(len(before), 1)
    recovery = next((t - prom_s for t in sorted(big) if t > prom_s and big[t] >= pre_big / 2), None)
    samples = []
    for line in out.joinpath("wave-samples.tsv").read_text().splitlines():
        if line[:1].isdigit():
            samples.append(dict(zip(("ms", "in_progress", "waits", "refusals", "failovers",
                                     "rss_kib", "heavy_active", "heavy_replay"),
                                    map(int, line.split("\t")))))
    prom = json.loads(out.joinpath("metrics-prometheus-proxy-a.prom").read_text())["text"]
    def gauge_opt(name):
        return next((int(float(line.split()[1])) for line in prom.splitlines()
                     if line.startswith(name + " ")), None)

    def gauge(name):
        value = gauge_opt(name)
        return 0 if value is None else value
    limit = gauge("heliosdb_proxy_backend_reconnect_limit")
    peak = gauge("heliosdb_proxy_backend_reconnects_peak")
    rss_before = max((s["rss_kib"] for s in samples if s["ms"] < promoted - 2000), default=-1)
    rss_end = samples[-1]["rss_kib"] if samples else -1
    max_in_progress = max((s["in_progress"] for s in samples), default=-1)
    late = [s["rss_kib"] for s in samples if s["ms"] >= promoted + RECOVER_S * 1000]
    journal = json.loads(out.joinpath("metrics-proxy-a.json").read_text())
    last = samples[-1] if samples else {}
    checks = [
        ("small_tenant_served", all(small.get(t, 0) > 0 for t in before + after)),
        ("small_tenant_no_aborts", aborted["small"] == 0),
        ("big_tenant_recovers", recovery is not None and recovery <= 30),
        ("reconnects_within_limit", limit > 0 and 0 < peak <= limit),
        ("rss_bounded", rss_before > 0 and rss_end - rss_before <= 64 * 1024),
        ("rss_flat_after_failover", len(late) > 1 and max(late) - min(late) < 16 * 1024),
    ]
    heavy = {}
    if heavy_log.exists():
        def tenant(name):
            return gauge_opt(name + '{tenant="heavy"}')
        active = max((s.get("heavy_active", -1) for s in samples), default=-1)
        replay = max((s.get("heavy_replay", -1) for s in samples), default=-1)
        query_limit = tenant("heliosdb_proxy_tenant_active_query_limit")
        byte_limit = tenant("heliosdb_proxy_tenant_replay_byte_limit")
        waits = tenant("heliosdb_proxy_tenant_query_waits_total")
        refusals = tenant("heliosdb_proxy_tenant_replay_budget_refusals_total")
        final_replay = tenant("heliosdb_proxy_tenant_replay_bytes")
        checks += [
            ("heavy_queries_within_cap",
             query_limit is not None and 0 < active <= query_limit),
            ("heavy_cap_engaged", (waits or 0) > 0),
            ("heavy_replay_within_budget",
             byte_limit is not None and 65536 < replay <= byte_limit),
            ("heavy_replay_budget_engaged", (refusals or 0) > 0),
            ("heavy_replay_released", final_replay == 0),
        ]
        heavy = {
            "heavy_tps": round(sum(tps_series(heavy_log).values()) / max(secs, 1), 1),
            "heavy_max_active_sampled": active, "heavy_active_query_limit": query_limit,
            "heavy_query_waits": waits,
            "heavy_query_refusals": tenant("heliosdb_proxy_tenant_query_refusals_total"),
            "heavy_max_replay_bytes_sampled": replay, "heavy_replay_byte_limit": byte_limit,
            "heavy_replay_budget_refusals": refusals, "heavy_final_replay_bytes": final_replay,
        }
    return checks, {**heavy, **{
        "pre_kill_big_tps": round(pre_big, 1),
        "big_recovery_s": None if recovery is None else round(recovery, 1),
        "small_zero_tps_seconds": [t for t in range(1, secs) if small.get(t, 0) == 0],
        "big_zero_tps_seconds": [t for t in range(1, secs) if big.get(t, 0) == 0],
        "aborted_clients": aborted,
        "reconnect_limit_per_node": limit,
        "peak_reconnects_per_node": peak,
        "max_reconnects_in_progress_sampled": max_in_progress,
        "reconnect_waits": last.get("waits"), "reconnect_refusals": last.get("refusals"),
        "tr_failovers": last.get("failovers"),
        "rss_kib_before_kill": rss_before, "rss_kib_end": rss_end,
        "rss_kib_late_min_max": [min(late, default=-1), max(late, default=-1)],
        "journal_committed_total": journal.get("journal_committed_total"),
        "kill_s": round(kill_s, 1), "promotion_s": round(prom_s, 1),
    }}



def edge_hits(out, name):
    return json.loads(out.joinpath(name).read_text())["cache"]["hits"]


def edge_gap_flushes(out, name):
    return json.loads(out.joinpath(name).read_text())["cache"]["gap_flushes"]


def edge_resume(out):
    drill = json.loads(out.joinpath("edge-drill.json").read_text())
    checks = [
        ("edge_cache_warmed", edge_hits(out, "edge-0-warmed.json") >= 2),
        ("warm_resume_after_cut", drill["resumed"] == "warm"),
        ("missed_invalidation_replayed", drill["read1"] == "11"),
        ("warm_kept_other_entries",
         edge_hits(out, "edge-3-after-id2.json") > edge_hits(out, "edge-2-after-id1.json")
         and drill["read2"] == "20"),
        ("no_flush_on_warm_resume",
         edge_gap_flushes(out, "edge-3-after-id2.json") == edge_gap_flushes(out, "edge-0-warmed.json")),
        ("home_restart_flushes_edge", drill["home_restart"] == "flushed"),
        ("read_after_flush_misses",
         edge_hits(out, "edge-5-after-id2.json") == edge_hits(out, "edge-4-home-restarted.json")
         and drill["read3"] == "20"),
    ]
    return checks, drill


def cache_logical(out):
    drill = json.loads(out.joinpath("cache-drill.json").read_text())
    m0 = json.loads(out.joinpath("metrics-0-warmed.json").read_text())["query_cache"]
    m1 = json.loads(out.joinpath("metrics-1-end.json").read_text())["query_cache"]
    hits = lambda m: sum(m["hits"].values())
    checks = [
        ("cache_was_serving_hits", hits(m0) >= 2),
        ("logical_source_attached", (m1.get("logical_source") or {}).get("connected") is True),
        ("write_via_other_proxy_seen", drill["after_proxy_b"] == {"kv": "11", "audit": "1"}),
        ("direct_write_seen", drill["after_direct"] == {"kv": "12", "audit": "2"}),
        ("trigger_side_effect_seen", drill["after_trigger"] == {"audit": "3"}),
        ("transactions_applied",
         (m1.get("logical_source") or {}).get("transactions_applied", 0) >= 3),
    ]
    return checks, {"drill": drill, "logical_source": m1.get("logical_source"),
                    "hits_warm": m0["hits"], "hits_end": m1["hits"]}


def main(scenario, out):
    out = pathlib.Path(out)
    if scenario in ("edge-resume", "cache-logical", "reconnect-wave"):
        checks, detail = {"edge-resume": edge_resume, "cache-logical": cache_logical,
                          "reconnect-wave": reconnect_wave}[scenario](out)
        for name, ok in checks:
            print(f"{name:30} {'ok' if ok else 'FAILED'}")
        print(json.dumps(detail))
        verdict = all(ok for _, ok in checks)
        print("PASS" if verdict else "FAIL")
        return 0 if verdict else 1
    docs = [json.loads(line) for line in out.joinpath("check.json").read_text().splitlines()
            if line.startswith("{")]
    main_doc = next(d for d in docs if "balance_sum" in d)
    promoted = int(out.joinpath("promoted_ms").read_text())
    after = {}
    for line in out.joinpath("outcomes.tsv").read_text().splitlines():
        _id, outcome, _state, via, t0, _t1 = line.split("\t")
        if outcome == "committed" and int(t0) > promoted:
            after[via] = after.get(via, 0) + 1
    checks = [
        ("balance_conserved", main_doc["balance_sum"] == main_doc["balance_sum_expected"]),
        ("no_acknowledged_loss", main_doc["committed_missing"] == 0),
        ("no_phantom_write", main_doc["failed_but_present"] == 0),
        ("writes_resume_proxy-a", after.get("proxy-a", 0) > 0),
        ("writes_resume_proxy-b", after.get("proxy-b", 0) > 0),
    ]
    if scenario in ("stop-old-primary", "chaos-force-leader"):
        other = next((d for d in docs if "other_node" in d), {})
        checks.append(("no_split_brain_ack", other.get("acknowledged_only_on_other") == 0))
    if scenario == "chaos-force-leader":
        for proxy in ("proxy-a", "proxy-b"):
            forced = json.loads(out.joinpath(f"topology-forced-{proxy}.json").read_text())
            checks.append((f"override_dropped_leader_{proxy}", forced.get("currentPrimary") is None))
    if scenario == "drain-restart":
        compose = out.joinpath("proxy-a.log").read_text(errors="replace")
        drained = [l for l in compose.splitlines() if "proxy-a" in l and "drain complete" in l]
        unknown_a = sum(1 for line in out.joinpath("outcomes.tsv").read_text().splitlines()
                        if line.split("\t")[1] == "unknown" and line.split("\t")[3] == "proxy-a")
        checks.append(("drain_completed", bool(drained) and out.joinpath("exited_ms").exists()))
        checks.append(("drain_no_unknown", unknown_a == 0))
    if scenario == "stale-sighup":
        compose = out.joinpath("proxy-a.log").read_text(errors="replace")
        refused = any("proxy-a" in l and "SIGHUP reload failed to parse" in l for l in compose.splitlines())
        after_broken = json.loads(out.joinpath("nodes-after-broken-reload.json").read_text())
        after_stale = json.loads(out.joinpath("nodes-after-stale-reload.json").read_text())
        topo = json.loads(out.joinpath("topology-after-stale-reload.json").read_text())
        checks.append(("broken_reload_refused", refused and len(after_broken) == 3))
        checks.append(("stale_reload_applied", len(after_stale) == 2
                       and all(n["address"] != "127.0.0.1:27703" for n in after_stale)
                       and topo.get("currentPrimary") == "127.0.0.1:27701"))
    if scenario == "tls-rotation":
        fp = json.loads(out.joinpath("tls-fingerprints.json").read_text())
        checks.append(("certificate_rotated", bool(fp["before"]) and bool(fp["after"])
                       and fp["before"] != fp["after"] and fp["after"] == fp["file"]))
    for name, ok in checks:
        print(f"{name:24} {'ok' if ok else 'FAILED'}")
    print(json.dumps({"committed_after_promotion": after, **main_doc}))
    verdict = all(ok for _, ok in checks)
    print("PASS" if verdict else "FAIL")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))

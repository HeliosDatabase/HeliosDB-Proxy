#!/usr/bin/env python3
"""Judge one HA-lab run from its output directory (check.json, outcomes.tsv,
promoted_ms). Prints one line per property and PASS/FAIL; exits 1 on FAIL.

Properties (every scenario):
  balance_conserved      sum(balance) on the new primary equals the start total
  no_acknowledged_loss   every transfer the client saw COMMIT for is present
  no_phantom_write       no transfer the client saw fail (ERROR / not sent) is present
  writes_resume_<proxy>  both proxies committed transfers that started after the
                         promotion (writes followed the new primary, no config edit)
stop-old-primary also:
  no_split_brain_ack     the returned, unfenced former primary holds no
                         acknowledged transfer the new primary lacks
edge-resume (C-03) instead: warm resume after a network cut replays the missed
invalidation and keeps the rest of the cache; a home restart flushes the edge.
reconnect-wave (H-05) instead:
  small_tenant_served        the small tenant's readers complete transactions in
                             every second before the kill and from RECOVER_S after
                             the promotion to the end
  small_tenant_no_aborts     no small-tenant client was aborted
  big_tenant_recovers        the big tenant reaches half its pre-kill throughput
                             within 30 s of the promotion
  reconnects_within_limit    backend reconnects in progress never exceeded
                             max_concurrent_reconnects_per_node x nodes
  rss_bounded                proxy RSS at the end is within 64 MiB of before the kill
  rss_flat_after_failover    from RECOVER_S after the promotion to the end the
                             proxy's RSS varies by less than 16 MiB
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
    kill_s = (promoted - start) / 1000 - 2  # the kill came 2 s before the promotion
    prom_s = (promoted - start) / 1000
    big = tps_series(out.joinpath("pgbench-big.log"))
    small = tps_series(out.joinpath("pgbench-small.log"))
    aborted = {t: len(re.findall(r"aborted", out.joinpath(f"pgbench-{t}.log").read_text()))
               for t in ("big", "small")}
    before = list(range(2, int(kill_s) - 1))
    after = list(range(int(prom_s) + RECOVER_S, secs))
    pre_big = sum(big.get(t, 0) for t in before) / max(len(before), 1)
    recovery = next((t - prom_s for t in sorted(big) if t > prom_s and big[t] >= pre_big / 2), None)
    samples = []
    for line in out.joinpath("wave-samples.tsv").read_text().splitlines():
        if line[:1].isdigit():
            samples.append(dict(zip(("ms", "in_progress", "waits", "refusals", "failovers",
                                     "rss_kib"), map(int, line.split("\t")))))
    prom = json.loads(out.joinpath("metrics-prometheus-proxy-a.prom").read_text())["text"]
    limit = next((int(float(line.split()[1])) for line in prom.splitlines()
                  if line.startswith("heliosdb_proxy_backend_reconnect_limit ")), 0)
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
        ("reconnects_within_limit", limit > 0 and 0 <= max_in_progress <= limit * 3),
        ("rss_bounded", rss_before > 0 and rss_end - rss_before <= 64 * 1024),
        ("rss_flat_after_failover", len(late) > 1 and max(late) - min(late) < 16 * 1024),
    ]
    return checks, {
        "pre_kill_big_tps": round(pre_big, 1),
        "big_recovery_s": None if recovery is None else round(recovery, 1),
        "small_zero_tps_seconds": [t for t in range(1, secs) if small.get(t, 0) == 0],
        "big_zero_tps_seconds": [t for t in range(1, secs) if big.get(t, 0) == 0],
        "aborted_clients": aborted,
        "reconnect_limit_per_node": limit,
        "max_reconnects_in_progress": max_in_progress,
        "reconnect_waits": last.get("waits"), "reconnect_refusals": last.get("refusals"),
        "tr_failovers": last.get("failovers"),
        "rss_kib_before_kill": rss_before, "rss_kib_end": rss_end,
        "rss_kib_late_min_max": [min(late, default=-1), max(late, default=-1)],
        "journal_committed_total": journal.get("journal_committed_total"),
        "kill_s": round(kill_s, 1), "promotion_s": round(prom_s, 1),
    }



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
    if scenario == "stop-old-primary":
        other = next((d for d in docs if "other_node" in d), {})
        checks.append(("no_split_brain_ack", other.get("acknowledged_only_on_other") == 0))
    for name, ok in checks:
        print(f"{name:24} {'ok' if ok else 'FAILED'}")
    print(json.dumps({"committed_after_promotion": after, **main_doc}))
    verdict = all(ok for _, ok in checks)
    print("PASS" if verdict else "FAIL")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))

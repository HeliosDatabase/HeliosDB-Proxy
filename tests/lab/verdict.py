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
"""
import json, pathlib, sys


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
    if scenario in ("edge-resume", "cache-logical"):
        checks, detail = edge_resume(out) if scenario == "edge-resume" else cache_logical(out)
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

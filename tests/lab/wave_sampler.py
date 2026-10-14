#!/usr/bin/env python3
"""Sample a lab proxy during the reconnect-wave drill until STOP_FILE exists.

Usage: wave_sampler.py ADMIN_URL TOKEN PROXY_PID OUT_TSV STOP_FILE
Writes one line per 250 ms: epoch_ms, backend reconnects in progress, reconnect
waits, reconnect refusals, TR failovers, proxy RSS (KiB). Standard library only.
"""
import json, pathlib, sys, time, urllib.request

METRICS = {
    "heliosdb_proxy_backend_reconnects_in_progress": "in_progress",
    "heliosdb_proxy_backend_reconnect_waits_total": "waits",
    "heliosdb_proxy_backend_reconnect_refusals_total": "refusals",
    "heliosdb_proxy_tr_failovers_total": "failovers",
}


def scrape(url, token):
    req = urllib.request.Request(url + "/metrics/prometheus",
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=2) as resp:
        text = json.loads(resp.read())["text"]
    found = {}
    for line in text.splitlines():
        name, _, value = line.partition(" ")
        if name in METRICS:
            found[METRICS[name]] = int(float(value))
    return found


def rss_kib(pid):
    for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return -1


def main(url, token, pid, out, stop):
    stop = pathlib.Path(stop)
    with open(out, "w") as f:
        f.write("ms\tin_progress\twaits\trefusals\tfailovers\trss_kib\n")
        while not stop.exists():
            try:
                m = scrape(url, token)
                row = [int(time.time() * 1000)] + [m.get(k, -1) for k in
                       ("in_progress", "waits", "refusals", "failovers")] + [rss_kib(pid)]
                f.write("\t".join(map(str, row)) + "\n")
                f.flush()
            except Exception as e:  # keep sampling through a slow scrape
                f.write(f"# {e!r}\n")
            time.sleep(0.25)


if __name__ == "__main__":
    main(*sys.argv[1:6])

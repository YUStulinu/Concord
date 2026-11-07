"""How long a cluster is unavailable after its leader dies.

    python -m concord_chaos.recovery [--trials 50] [--target concord|etcd]

A client writes continuously (one put at a time, to whichever server answers as leader). The
leader is killed; recovery time is from the kill to the end of the first write started after
the kill that succeeds, as the client sees it. The client gives up on a server after 250 ms (on
Windows a connection to a dead port is not refused at once, so that timeout is part of what a
real client would see). For Concord, the time until a new leader shows in /v1/status is also
recorded. Each trial uses a fresh 5-server cluster.
"""
from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from pathlib import Path

from .cluster import ROOT, Cluster, request


def trial(seed: int) -> dict:
    c = Cluster(size=5, base_port=21000, workdir=ROOT / "data" / "recovery")
    c.start()
    time.sleep(0.5)
    successes: list[tuple[float, float]] = []  # (started, finished)
    stop = threading.Event()

    def writer():
        n = 0
        target = c.leader()
        while not stop.is_set():
            n += 1
            started = time.perf_counter()
            try:
                code, reply = request(c.servers[target].http, "POST", "/v1/kv", {"op": "put", "key": "k", "value": str(n)}, timeout=0.25)
                if code == 200:
                    successes.append((started, time.perf_counter()))
                    continue
                if code == 421 and reply.get("leader"):
                    target = next(i for i, s in c.servers.items() if s.http == reply["leader"])
                    continue
            except (OSError, StopIteration):
                pass
            # Try another server.
            alive = c.alive()
            target = alive[n % len(alive)]
            time.sleep(0.005)

    t = threading.Thread(target=writer, daemon=True)
    t.start()
    time.sleep(1.0)
    old = c.leader()
    killed_at = time.perf_counter()
    c.kill(old)
    new_leader_at = None
    while time.perf_counter() - killed_at < 10:
        l = c.leader()
        if l is not None and l != old:
            new_leader_at = time.perf_counter()
            break
        time.sleep(0.005)
    time.sleep(1.0)
    stop.set()
    t.join()
    c.stop()
    first_ok = next((end for start, end in successes if start > killed_at), None)
    return {
        "leader_elected_ms": None if new_leader_at is None else round((new_leader_at - killed_at) * 1000, 1),
        "writes_resume_ms": None if first_ok is None else round((first_ok - killed_at) * 1000, 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=50)
    ap.add_argument("--out", default=str(ROOT / "results" / "recovery.json"))
    args = ap.parse_args()
    trials = []
    for i in range(args.trials):
        r = trial(i)
        trials.append(r)
        print(i, r, flush=True)
    def stats(key):
        v = sorted(x[key] for x in trials if x[key] is not None)
        return {"median": statistics.median(v), "p90": v[int(0.9 * (len(v) - 1))], "max": v[-1], "min": v[0], "n": len(v)}
    out = {"servers": 5, "tick_ms": 10, "election_timeout_ms": "150-300 (randomised)",
           "leader_elected_ms": stats("leader_elected_ms"), "writes_resume_ms": stats("writes_resume_ms"), "trials": trials}
    Path(args.out).write_text(json.dumps(out, indent=1))
    print(json.dumps({k: v for k, v in out.items() if k != "trials"}, indent=1))


if __name__ == "__main__":
    main()

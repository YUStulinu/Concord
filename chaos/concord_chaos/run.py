"""Runs fault scenarios and checks every history for linearizability.

    python -m concord_chaos.run [--runs 300] [--seconds 8] [--unsafe-reads] [--out results/chaos.json]

Each run starts a fresh 5-server cluster, runs 8 clients for --seconds while a fault schedule
works on the cluster, heals everything, lets the clients go on briefly, and hands the recorded
history to concord-check. Schedules rotate through every kind of fault.
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

from .client import run_clients
from .cluster import ROOT, Cluster
from . import nemesis
from .nemesis import SCHEDULES

CHECKER = ROOT / "bin" / ("concord-check.exe" if sys.platform == "win32" else "concord-check")


def check(history: list, path: Path) -> dict:
    path.write_text(json.dumps(history))
    p = subprocess.run([str(CHECKER), str(path)], capture_output=True, text=True)
    try:
        out = json.loads(p.stdout)
    except json.JSONDecodeError:
        out = {"error": p.stderr[-500:]}
    out["exit"] = p.returncode
    return out


def one_run(schedule: str, seed: int, seconds: float, unsafe: bool, workdir: Path) -> dict:
    rng = random.Random(seed)
    extra = ["--snapshot-every", "300"]
    if unsafe:
        extra.append("--unsafe-reads")
    c = Cluster(size=5, base_port=17000 + 20 * (seed % 40), workdir=workdir, extra_args=extra)
    t0 = time.time()
    c.start()
    history: list = []
    nemesis.ACTIONS.clear()
    stop = time.time() + seconds
    faulter = threading.Thread(target=SCHEDULES[schedule], args=(c, rng, stop), daemon=True)
    faulter.start()
    run_clients(c, seconds, n=8, seed=seed, history=history, any_node=unsafe)
    faulter.join(timeout=10)
    c.heal()
    for i in c.servers:
        if c.servers[i].member and i not in c.alive():
            c.restart(i)
    # After healing, the cluster must recover and keep serving.
    run_clients(c, 1.5, n=4, seed=seed + 7_000_000, history=history, any_node=unsafe)
    c.stop()
    verdict = check(history, workdir / f"history-{seed}.json")
    ops = collections.Counter(e["op"] for e in history)
    return {
        "schedule": schedule,
        "seed": seed,
        "unsafe_reads": unsafe,
        "ops": len(history),
        "by_op": dict(ops),
        "unknown": sum(1 for e in history if e["ret"] is None),
        "linearizable": verdict.get("linearizable"),
        "gave_up": verdict.get("gave_up"),
        "check_seconds": round(verdict.get("seconds", 0), 3),
        "violation": next((k for k in verdict.get("keys", []) if isinstance(k.get("verdict"), dict)), None),
        "faults": dict(nemesis.ACTIONS),
        "seconds": round(time.time() - t0, 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=300)
    ap.add_argument("--seconds", type=float, default=8)
    ap.add_argument("--unsafe-reads", action="store_true")
    ap.add_argument("--schedules", default=",".join(s for s in SCHEDULES))
    ap.add_argument("--out", default=str(ROOT / "results" / "chaos.json"))
    args = ap.parse_args()
    schedules = args.schedules.split(",")
    workdir = ROOT / "data" / ("chaos-unsafe" if args.unsafe_reads else "chaos")
    runs = []
    for i in range(args.runs):
        sched = schedules[i % len(schedules)]
        r = one_run(sched, i, args.seconds, args.unsafe_reads, workdir)
        runs.append(r)
        flag = "ok" if r["linearizable"] else ("GAVE UP" if r["gave_up"] else "NOT LINEARIZABLE")
        print(f"{i:4d} {sched:15s} {r['ops']:6d} ops ({r['unknown']:3d} unknown)  {flag}  [{r['seconds']}s]", flush=True)
        summary = summarise(runs)
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps({"summary": summary, "runs": runs}, indent=1))
    print(json.dumps(summarise(runs), indent=2))


def summarise(runs: list) -> dict:
    by = collections.defaultdict(lambda: {"runs": 0, "linearizable": 0, "not_linearizable": 0, "gave_up": 0, "ops": 0, "unknown": 0})
    for r in runs:
        b = by[r["schedule"]]
        b["runs"] += 1
        b["ops"] += r["ops"]
        b["unknown"] += r["unknown"]
        if r["linearizable"]:
            b["linearizable"] += 1
        elif r["gave_up"]:
            b["gave_up"] += 1
        else:
            b["not_linearizable"] += 1
    total = {k: sum(b[k] for b in by.values()) for k in ["runs", "linearizable", "not_linearizable", "gave_up", "ops", "unknown"]}
    return {"total": total, "by_schedule": dict(by)}


if __name__ == "__main__":
    main()

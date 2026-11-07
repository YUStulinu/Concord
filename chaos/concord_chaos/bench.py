"""Concord against etcd: the same three-server setup on this machine, the same load generator.

    python -m concord_chaos.bench [--duration 10] [--out results/bench.json]

etcd 3.7.3 runs with its defaults (fsync on every commit, snapshots every 100,000 entries in
3.7); Concord with its defaults. Both clusters run on localhost, one at a time.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .cluster import ROOT, Cluster

BENCH = ROOT / "bin" / ("concord-bench.exe" if sys.platform == "win32" else "concord-bench")
ETCD = next((ROOT / "third_party" / "etcd").glob("*/etcd*"), None)


class Etcd:
    def __init__(self, size: int = 3, base: int = 23000):
        self.size, self.base = size, base
        self.dir = ROOT / "data" / "etcd"
        self.procs: list[subprocess.Popen] = []

    def client_urls(self) -> list[str]:
        return [f"127.0.0.1:{self.base + 10 * i}" for i in range(1, self.size + 1)]

    def start(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        peers = ",".join(f"e{i}=http://127.0.0.1:{self.base + 10 * i + 1}" for i in range(1, self.size + 1))
        for i in range(1, self.size + 1):
            c, p = self.base + 10 * i, self.base + 10 * i + 1
            args = [str(ETCD), "--name", f"e{i}", "--data-dir", str(self.dir / f"e{i}"),
                    "--listen-client-urls", f"http://127.0.0.1:{c}", "--advertise-client-urls", f"http://127.0.0.1:{c}",
                    "--listen-peer-urls", f"http://127.0.0.1:{p}", "--initial-advertise-peer-urls", f"http://127.0.0.1:{p}",
                    "--initial-cluster", peers, "--initial-cluster-state", "new", "--log-level", "error"]
            self.procs.append(subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(self.dir / f"e{i}.log", "ab")))
        time.sleep(4)

    def stop(self):
        for p in self.procs:
            p.kill()
            p.wait()
        self.procs = []


def run(target: str, endpoints: list[str], workload: str, clients: int, duration: float, out: Path) -> dict:
    args = [str(BENCH), "--target", target, "--endpoints", ",".join(endpoints), "--workload", workload,
            "--clients", str(clients), "--duration", f"{duration}s", "--json", str(out)]
    p = subprocess.run(args, capture_output=True, text=True, timeout=duration * 6 + 120)
    if p.returncode != 0:
        raise RuntimeError(p.stderr[-800:])
    return json.loads(p.stdout.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--clients", default="1,8,32,128")
    ap.add_argument("--out", default=str(ROOT / "results" / "bench.json"))
    args = ap.parse_args()
    lines = ROOT / "data" / "bench-lines.jsonl"
    lines.unlink(missing_ok=True)
    results = []
    clients = [int(c) for c in args.clients.split(",")]
    for target in ["concord", "etcd"]:
        for workload in ["write", "read", "mixed"]:
            for n in clients:
                # A fresh cluster for each measurement.
                if target == "concord":
                    c = Cluster(size=3, base_port=19000, workdir=ROOT / "data" / "bench-cluster")
                    c.start()
                    time.sleep(1)
                    endpoints = [s.http for s in c.servers.values()]
                else:
                    c = Etcd()
                    c.start()
                    endpoints = c.client_urls()
                try:
                    r = run(target, endpoints, workload, n, args.duration, lines)
                finally:
                    c.stop()
                print(f"{target:8s} {workload:6s} {n:4d} clients: {r['ops_per_s']:9.0f} ops/s  p50 {r['p50_ms']:7.2f} ms  p99 {r['p99_ms']:7.2f} ms  errors {r['errors']}", flush=True)
                results.append(r)
    Path(args.out).write_text(json.dumps({"servers": 3, "etcd": "3.7.3", "duration_s": args.duration, "results": results}, indent=1))


if __name__ == "__main__":
    main()

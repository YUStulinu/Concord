"""The harness end to end: a real cluster, clients, a fault, and the checker's verdict.

Needs bin/concord and bin/concord-check (CI builds them first); skipped otherwise.
"""
import json
import random
import threading
import time

import pytest

from concord_chaos.client import run_clients
from concord_chaos.cluster import BIN, Cluster
from concord_chaos.nemesis import SCHEDULES, isolate_leader
from concord_chaos.run import CHECKER, check, summarise

needs_binaries = pytest.mark.skipif(not BIN.exists() or not CHECKER.exists(), reason="build bin/concord and bin/concord-check first")


def test_summary_counts_outcomes():
    runs = [
        {"schedule": "kill", "ops": 10, "unknown": 1, "linearizable": True, "gave_up": False},
        {"schedule": "kill", "ops": 5, "unknown": 0, "linearizable": False, "gave_up": False},
        {"schedule": "none", "ops": 7, "unknown": 0, "linearizable": False, "gave_up": True},
    ]
    s = summarise(runs)
    assert s["total"] == {"runs": 3, "linearizable": 1, "not_linearizable": 1, "gave_up": 1, "ops": 22, "unknown": 1}
    assert s["by_schedule"]["kill"]["not_linearizable"] == 1
    assert set(SCHEDULES) >= {"partition", "kill-leader", "clock-skew", "membership"}


@needs_binaries
def test_cluster_stays_linearizable_while_its_leader_is_isolated(tmp_path):
    c = Cluster(size=3, base_port=25000, workdir=tmp_path / "cluster")
    c.start()
    try:
        history: list = []
        stop = time.time() + 3
        t = threading.Thread(target=isolate_leader, args=(c, random.Random(1), stop), daemon=True)
        t.start()
        run_clients(c, 3, n=4, seed=1, history=history)
        t.join()
        c.heal()
        assert len(history) > 50
        verdict = check(history, tmp_path / "h.json")
        assert verdict["linearizable"], json.dumps(verdict)[:2000]
    finally:
        c.stop()


@needs_binaries
def test_the_checker_catches_stale_reads(tmp_path):
    c = Cluster(size=3, base_port=25100, workdir=tmp_path / "cluster", extra_args=["--unsafe-reads"])
    c.start()
    try:
        history = run_clients(c, 3, n=6, seed=2, any_node=True)
        verdict = check(history, tmp_path / "h.json")
        assert verdict["linearizable"] is False, "serving reads from followers must be caught"
    finally:
        c.stop()

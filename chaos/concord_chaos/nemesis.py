"""Fault schedules. Each runs alongside the clients, alternating a fault and a short heal."""
from __future__ import annotations

import random
import shutil
import time

from .cluster import Cluster

# How many times each fault was injected in the current run (reset by the runner).
ACTIONS: dict[str, int] = {}


def did(what: str):
    ACTIONS[what] = ACTIONS.get(what, 0) + 1


def _sleep(stop: float, seconds: float):
    time.sleep(max(0.0, min(seconds, stop - time.time())))


def partition_halves(c: Cluster, rng: random.Random, stop: float):
    ids = list(c.servers)
    while time.time() < stop:
        rng.shuffle(ids)
        k = rng.randint(1, len(ids) // 2)
        c.partition(ids[:k], ids[k:])
        did("partition")
        _sleep(stop, rng.uniform(0.8, 2.0))
        c.heal()
        _sleep(stop, rng.uniform(0.3, 0.8))


def isolate_leader(c: Cluster, rng: random.Random, stop: float):
    while time.time() < stop:
        l = c.leader()
        if l is not None:
            c.partition([l], [i for i in c.servers if i != l])
            did("isolate leader")
        _sleep(stop, rng.uniform(0.8, 2.0))
        c.heal()
        _sleep(stop, rng.uniform(0.3, 0.8))


def bridge(c: Cluster, rng: random.Random, stop: float):
    """Two halves that can each reach one server in the middle (a majority-overlap partition)."""
    ids = list(c.servers)
    while time.time() < stop:
        rng.shuffle(ids)
        mid, a, b = ids[0], ids[1 : 1 + len(ids) // 2], ids[1 + len(ids) // 2 :]
        for i in a:
            c.set_faults(i, blocked=b)
        for i in b:
            c.set_faults(i, blocked=a)
        c.set_faults(mid, blocked=[])
        did("bridge")
        _sleep(stop, rng.uniform(0.8, 2.0))
        c.heal()
        _sleep(stop, rng.uniform(0.3, 0.8))


def kill_random(c: Cluster, rng: random.Random, stop: float):
    while time.time() < stop:
        victims = rng.sample(list(c.servers), rng.randint(1, len(c.servers) // 2))
        for v in victims:
            c.kill(v)
            did("kill")
        _sleep(stop, rng.uniform(0.5, 1.5))
        for v in victims:
            c.restart(v)
        _sleep(stop, rng.uniform(0.3, 0.8))


def kill_leader(c: Cluster, rng: random.Random, stop: float):
    while time.time() < stop:
        l = c.leader()
        if l is not None:
            c.kill(l)
            did("kill leader")
            _sleep(stop, rng.uniform(0.4, 1.2))
            c.restart(l)
        _sleep(stop, rng.uniform(0.5, 1.0))


def packet_loss(c: Cluster, rng: random.Random, stop: float):
    while time.time() < stop:
        for i in c.servers:
            c.set_faults(i, drop=rng.uniform(0.1, 0.4))
        did("packet loss")
        _sleep(stop, rng.uniform(1.0, 2.0))
        c.heal()
        _sleep(stop, 0.4)


def delays(c: Cluster, rng: random.Random, stop: float):
    while time.time() < stop:
        for i in c.servers:
            c.set_faults(i, delay_ms=rng.randint(0, 80), jitter_ms=rng.randint(0, 80))
        did("delay")
        _sleep(stop, rng.uniform(1.0, 2.0))
        c.heal()
        _sleep(stop, 0.4)


def clock_skew(c: Cluster, rng: random.Random, stop: float):
    """Some servers' timers run up to 4x fast or slow: elections time out early or late."""
    while time.time() < stop:
        for i in c.servers:
            c.set_clock(i, rng.choice([0.25, 0.5, 1.0, 2.0, 4.0]))
        did("clock skew")
        _sleep(stop, rng.uniform(1.0, 2.5))
        c.heal()
        _sleep(stop, 0.4)


def membership(c: Cluster, rng: random.Random, stop: float):
    """Remove a server, wipe it, add it back as a new member that must catch up from scratch."""
    while time.time() < stop:
        l = c.leader()
        candidates = [i for i in c.servers if i != l and c.servers[i].member]
        if l is None or not candidates:
            _sleep(stop, 0.3)
            continue
        v = rng.choice(candidates)
        if c.change_member("remove", v):
            did("member removed")
            c.kill(v)
            shutil.rmtree(c.servers[v].data, ignore_errors=True)
            _sleep(stop, rng.uniform(0.3, 1.0))
            c._spawn(c.servers[v], join=True)
            for _ in range(20):
                if c.change_member("add", v):
                    did("member added")
                    break
                time.sleep(0.2)
        _sleep(stop, rng.uniform(0.5, 1.0))


def mixed(c: Cluster, rng: random.Random, stop: float):
    faults = [partition_halves, isolate_leader, kill_random, kill_leader, packet_loss, clock_skew, bridge]
    while time.time() < stop:
        f = rng.choice(faults)
        f(c, rng, min(stop, time.time() + rng.uniform(1.5, 3.0)))


def none(c: Cluster, rng: random.Random, stop: float):
    _sleep(stop, stop - time.time())


SCHEDULES = {
    "none": none,
    "partition": partition_halves,
    "isolate-leader": isolate_leader,
    "bridge": bridge,
    "kill": kill_random,
    "kill-leader": kill_leader,
    "packet-loss": packet_loss,
    "delay": delays,
    "clock-skew": clock_skew,
    "membership": membership,
    "mixed": mixed,
}

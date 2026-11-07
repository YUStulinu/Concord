"""Clients that record a history for the linearizability checker.

Each operation is written as {process, op, key, value, expect, call, ret, ok, result}. `ret` is
null when the outcome is unknown (a timeout, or an error that does not prove the operation
failed): it may have taken effect at any time after the call, so the checker may place it
anywhere after `call`, or nowhere. As in Jepsen, a client whose operation is left unknown
continues as a new process, since the old operation could still be in flight.
"""
from __future__ import annotations

import http.client
import itertools
import json
import random
import threading
import time

from .cluster import Cluster

_client_ids = itertools.count(1)


def now() -> int:
    return time.monotonic_ns()


class Client:
    def __init__(self, cluster: Cluster, history: list, lock: threading.Lock, rng: random.Random,
                 keys: int = 6, timeout: float = 1.0, any_node: bool = False):
        self.cluster, self.history, self.lock, self.rng = cluster, history, lock, rng
        self.keys, self.timeout, self.any_node = keys, timeout, any_node
        self.process = next(_client_ids)
        self.seq = 0
        self.target: int | None = None
        self.counter = 0
        self.conn: http.client.HTTPConnection | None = None

    def _send(self, body: dict) -> tuple[int, dict]:
        # With any_node, reads go to a random server (for the unsafe-reads experiment).
        if self.target is None or self.target not in self.cluster.alive() or (self.any_node and body["op"] == "get"):
            alive = self.cluster.alive()
            if not alive:
                raise OSError("no servers")
            self.target = self.rng.choice(alive)
            self.conn = None
        if self.conn is None:
            host, port = self.cluster.servers[self.target].http.split(":")
            self.conn = http.client.HTTPConnection(host, int(port), timeout=self.timeout)
        try:
            self.conn.request("POST", "/v1/kv", body=json.dumps(body), headers={"Content-Type": "application/json"})
            r = self.conn.getresponse()
            return r.status, json.loads(r.read() or b"{}")
        except Exception:
            self.conn = None
            raise

    def _follow(self, reply: dict):
        """Point at the leader the server named, or anywhere else."""
        leader = reply.get("leader")
        self.conn = None
        self.target = None
        if leader:
            for i, s in self.cluster.servers.items():
                if s.http == leader and i in self.cluster.alive():
                    self.target = i

    def step(self):
        key = f"k{self.rng.randrange(self.keys)}"
        r = self.rng.random()
        self.counter += 1
        value = f"{self.process}-{self.counter}"
        if r < 0.45:
            op = {"op": "get", "key": key}
        elif r < 0.8:
            op = {"op": "put", "key": key, "value": value}
        elif r < 0.9:
            op = {"op": "delete", "key": key}
        else:
            expect = self._last_seen.get(key)
            op = {"op": "cas", "key": key, "value": value, "expect": expect}
        self.seq += 1
        body = dict(op, client=self.process, seq=self.seq)
        entry = {"process": self.process, **op, "call": now(), "ret": None, "ok": None, "result": None}
        # Retry on not_leader (a definite failure: nothing happened) a few times, quickly.
        for attempt in range(6):
            try:
                code, reply = self._send(body)
            except Exception:
                # Connection trouble: the request may have arrived. Unknown.
                break
            if code == 421:
                self._follow(reply)
                time.sleep(0.02 * (attempt + 1))
                continue
            if code == 200:
                entry["ret"] = now()
                entry["ok"] = reply.get("ok", False)
                entry["result"] = {"value": reply.get("value"), "found": reply.get("found", False)}
                if op["op"] == "get" and reply.get("found"):
                    self._last_seen[key] = reply.get("value")
                elif op["op"] == "get":
                    self._last_seen[key] = None
            elif code == 409:
                entry["ret"] = now()  # "lost": definitely did not take effect
                entry["ok"] = False
                entry["failed"] = True
            break
        if entry["ret"] is None and entry["ok"] is None and op["op"] != "get":
            # Unknown outcome: record it open and continue as a fresh process.
            with self.lock:
                self.history.append(entry)
            self.process = next(_client_ids)
            self.seq = 0
            self._last_seen = {}
            return
        if entry["ret"] is None:
            return  # an unknown read constrains nothing
        if entry.get("failed"):
            return  # definitely not applied: leave it out
        with self.lock:
            self.history.append(entry)

    _last_seen: dict


def run_clients(cluster: Cluster, seconds: float, n: int = 8, seed: int = 0, keys: int = 6,
                any_node: bool = False, history: list | None = None) -> list:
    history = history if history is not None else []
    lock = threading.Lock()
    stop = time.time() + seconds

    def loop(k: int):
        c = Client(cluster, history, lock, random.Random(seed * 1000 + k), keys=keys, any_node=any_node)
        c._last_seen = {}
        while time.time() < stop:
            try:
                c.step()
            except Exception:
                time.sleep(0.05)

    threads = [threading.Thread(target=loop, args=(k,), daemon=True) for k in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return history

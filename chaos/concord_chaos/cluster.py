"""Running a Concord cluster as real processes, and breaking it.

Every server is a separate `concord` process with its own data directory. Faults:

* kill: TerminateProcess / SIGKILL, then restart from the data directory;
* partition: each server is told which peers to drop messages to and from (no iptables on
  Windows, so partitions are made in the servers' transport layer, on every link both ways);
* packet loss and delay, per server;
* clock skew: a server's timers run faster or slower than real time;
* membership: servers removed from and added to the cluster while it runs.
"""
from __future__ import annotations

import http.client
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIN = ROOT / "bin" / ("concord.exe" if sys.platform == "win32" else "concord")


def request(addr: str, method: str, path: str, body: dict | None = None, timeout: float = 2.0) -> tuple[int, dict]:
    host, port = addr.split(":")
    c = http.client.HTTPConnection(host, int(port), timeout=timeout)
    try:
        payload = json.dumps(body) if body is not None else None
        c.request(method, path, body=payload, headers={"Content-Type": "application/json"})
        r = c.getresponse()
        data = r.read()
        return r.status, (json.loads(data) if data else {})
    finally:
        c.close()


@dataclass
class Server:
    id: int
    raft: str
    http: str
    data: Path
    proc: subprocess.Popen | None = None
    log: Path | None = None
    member: bool = True

    @property
    def spec(self) -> str:
        return f"{self.id}={self.raft}@{self.http}"


@dataclass
class Cluster:
    size: int = 5
    base_port: int = 17000
    workdir: Path = ROOT / "data" / "cluster"
    extra_args: list[str] = field(default_factory=list)
    servers: dict[int, Server] = field(default_factory=dict)

    def __post_init__(self):
        for i in range(1, self.size + 1):
            self.servers[i] = self._server(i)

    def _server(self, i: int) -> Server:
        return Server(i, f"127.0.0.1:{self.base_port + i}", f"127.0.0.1:{self.base_port + 100 + i}", self.workdir / f"n{i}")

    # ---- processes ---------------------------------------------------------------------------

    def start(self):
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
        self.workdir.mkdir(parents=True)
        for s in self.servers.values():
            self._spawn(s)
        self.wait_for_leader()

    def _spawn(self, s: Server, join: bool = False):
        cluster = ",".join(x.spec for x in self.servers.values() if x.member and x.id <= self.size)
        args = [str(BIN), "--id", str(s.id), "--raft", s.raft, "--http", s.http, "--data", str(s.data), *self.extra_args]
        args += ["--join"] if join else ["--cluster", cluster]
        s.log = self.workdir / f"n{s.id}.log"
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        s.proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=open(s.log, "ab"), creationflags=flags)
        for _ in range(100):
            try:
                request(s.http, "GET", "/v1/status", timeout=0.5)
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"server {s.id} did not start; see {s.log}")

    def kill(self, i: int):
        s = self.servers[i]
        if s.proc and s.proc.poll() is None:
            s.proc.kill()  # TerminateProcess / SIGKILL: no clean shutdown
            s.proc.wait()
        s.proc = None

    def restart(self, i: int):
        s = self.servers[i]
        if s.proc is None or s.proc.poll() is not None:
            self._spawn(s, join=not os.path.exists(s.data / "log") and i > self.size)

    def alive(self) -> list[int]:
        return [i for i, s in self.servers.items() if s.proc and s.proc.poll() is None]

    def stop(self):
        for i in list(self.servers):
            self.kill(i)

    # ---- inspection --------------------------------------------------------------------------

    def status(self, i: int) -> dict | None:
        try:
            code, body = request(self.servers[i].http, "GET", "/v1/status", timeout=0.5)
            return body if code == 200 else None
        except OSError:
            return None

    def leader(self) -> int | None:
        best = None
        for i in self.alive():
            st = self.status(i)
            if st and st.get("role") == "leader" and (best is None or st["term"] > best[1]):
                best = (i, st["term"])
        return best[0] if best else None

    def wait_for_leader(self, timeout: float = 10.0) -> int:
        end = time.time() + timeout
        while time.time() < end:
            if (l := self.leader()) is not None:
                return l
            time.sleep(0.05)
        raise TimeoutError("no leader")

    # ---- faults ------------------------------------------------------------------------------

    def set_faults(self, i: int, blocked=(), drop: float = 0.0, delay_ms: int = 0, jitter_ms: int = 0):
        if i in self.alive():
            try:
                request(self.servers[i].http, "POST", "/v1/admin/faults",
                        {"blocked": list(blocked), "drop": drop, "delay_ms": delay_ms, "jitter_ms": jitter_ms}, timeout=1)
            except OSError:
                pass

    def partition(self, *groups: list[int]):
        where = {i: g for g, ids in enumerate(groups) for i in ids}
        for i in self.servers:
            self.set_faults(i, blocked=[j for j in self.servers if where.get(j, -1) != where.get(i, -2)])

    def heal(self):
        for i in self.servers:
            self.set_faults(i)
            self.set_clock(i, 1.0)

    def set_clock(self, i: int, rate: float):
        if i in self.alive():
            try:
                request(self.servers[i].http, "POST", "/v1/admin/clock", {"rate": rate}, timeout=1)
            except OSError:
                pass

    def change_member(self, op: str, i: int) -> bool:
        """Adds or removes server i through the leader; True if the change was accepted."""
        s = self.servers[i]
        l = self.leader()
        if l is None:
            return False
        try:
            code, _ = request(self.servers[l].http, "POST", "/v1/admin/members",
                              {"op": op, "id": i, "raft": s.raft, "http": s.http}, timeout=3)
        except OSError:
            return False
        if code == 200:
            s.member = op == "add"
        return code == 200

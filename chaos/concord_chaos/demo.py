"""A five-server cluster for the dashboard: servers 1-5 on http://127.0.0.1:8001-8005.

    python -m concord_chaos.demo        (Ctrl+C stops it)
"""
import time

from .cluster import ROOT, Cluster


def main():
    c = Cluster(size=5, base_port=7000, workdir=ROOT / "data" / "demo", extra_args=["--snapshot-every", "500"])
    # Clients on 8001-8005, Raft on 7001-7005.
    for i, s in c.servers.items():
        s.http = f"127.0.0.1:{8000 + i}"
    c.start()
    print("cluster up: http://127.0.0.1:8001 ... 8005 -- open the dashboard (cd dashboard && npm run dev)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        c.stop()


if __name__ == "__main__":
    main()

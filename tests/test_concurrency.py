"""Many parallel clients on one resource: never two holders, strict FIFO, no lost tickets."""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from conftest import popen_client


def check_event_log(api, n, res):
    """From the audit log: grants/releases alternate (never two holders) and every ticket was served."""
    ev = [e for e in api.events(res) if e["kind"] in ("enqueue", "acquire", "release", "expire",
                                                       "drop", "abandon", "cancel")]
    holder = None
    granted = []
    for e in ev:
        if e["kind"] == "acquire":
            assert holder is None, f"ticket {e['ticket']} granted while {holder} holds"
            holder = e["ticket"]
            granted.append(e["ticket"])
        elif e["kind"] == "release":
            assert holder == e["ticket"]
            holder = None
        else:
            assert e["kind"] == "enqueue", f"unexpected {e}"
    enqueued = [e["ticket"] for e in ev if e["kind"] == "enqueue"]
    assert len(enqueued) == n and len(granted) == n
    assert granted == sorted(enqueued), "grants must follow ticket (arrival) order"
    return granted


def test_50_parallel_http_clients(api):
    n = 50
    barrier = threading.Barrier(n)
    inside = []
    inside_lock = threading.Lock()
    overlaps = []
    order = []
    first_ticket = {}

    def client(i):
        owner = f"c{i:02d}"
        barrier.wait()
        st, out = api.acquire("jetson", owner, wait=0)
        first_ticket[owner] = out["ticket"]
        while out["state"] != "acquired":
            st, out = api.acquire("jetson", owner, ticket=out["ticket"], wait=20)
            assert st in (200, 202), out
        assert out["ticket"] == first_ticket[owner], "ticket must not change while waiting"
        with inside_lock:
            if inside:
                overlaps.append((owner, list(inside)))
            inside.append(owner)
            order.append(out["ticket"])
        time.sleep(0.01)
        with inside_lock:
            inside.remove(owner)
        st, rel = api.post("/r/jetson/release", {"owner": owner})
        assert st == 200, rel
        return owner

    with ThreadPoolExecutor(n) as pool:
        done = list(pool.map(client, range(n)))
    assert len(done) == n
    assert overlaps == []
    assert order == sorted(first_ticket.values()), "strict FIFO by ticket id"
    granted = check_event_log(api, n, "jetson")
    assert granted == order
    st = api.status("jetson")
    assert st["free"] and st["queue"] == []


def test_parallel_cli_run_never_overlaps(server, tmp_path):
    n = 12
    log = tmp_path / "log.txt"
    script = ("import sys,time;f=open(sys.argv[1],'a');f.write('start %s\\n'%sys.argv[2]);f.flush();"
              "time.sleep(0.05);f.write('end %s\\n'%sys.argv[2]);f.close()")
    procs = [popen_client(server, "run", "rig", f"p{i}", "cli test", "--", sys.executable, "-c", script,
                          str(log), f"p{i}") for i in range(n)]
    for p in procs:
        p.wait(120)
        assert p.returncode == 0, p.stderr.read()
    lines = log.read_text().split()
    pairs = list(zip(lines[0::2], lines[1::2]))
    assert len(pairs) == 2 * n
    for (k1, who1), (k2, who2) in zip(pairs[0::2], pairs[1::2]):
        assert (k1, k2, who1) == ("start", "end", who2)
    check_event_log(server.api, n, "rig")

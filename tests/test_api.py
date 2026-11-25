"""HTTP API tests against a real server subprocess (fresh port + DB per test)."""

import threading
import time

import pytest

from conftest import wait_for


def test_health_and_404(api):
    assert api.get("/health") == (200, {"ok": True, "version": "0.1.0"})
    assert api.get("/nope")[0] == 404


def test_acquire_release_cycle_and_metadata(api):
    st, out = api.acquire("jetson", "agent-1", reason="deploy", host="pc", pid=7, lease=60)
    assert st == 200 and out["state"] == "acquired"
    h = api.status("jetson")["holder"]
    assert (h["owner"], h["reason"], h["host"], h["pid"], h["lease_s"]) == ("agent-1", "deploy", "pc", 7, 60)
    assert 59 < h["lease_left_s"] <= 60 and h["since"] > 0
    st, out = api.acquire("jetson", "agent-2", wait=0)
    assert st == 202 and out["position"] == 1 and out["holder"]["owner"] == "agent-1"
    assert api.post("/r/jetson/release", {"owner": "agent-2"})[0] == 409
    assert api.post("/r/jetson/renew", {"owner": "agent-2"})[0] == 409
    assert api.post("/r/jetson/renew", {"owner": "agent-1", "lease": 120})[0] == 200
    assert api.post("/r/jetson/release", {"owner": "agent-1"}) == (200, {"state": "released", "resource": "jetson"})
    st, out2 = api.acquire("jetson", "agent-2", ticket=out["ticket"])
    assert st == 200 and out2["ticket"] == out["ticket"]


def test_reacquire_is_idempotent(api):
    api.acquire("rig", "me")
    st, out = api.acquire("rig", "me")
    assert st == 200 and out["reacquired"] is True
    assert api.status("rig")["queue"] == []


def test_bad_input(api, server):
    assert api.acquire("jetson", "has space")[0] == 400
    assert api.acquire("jetson", "a", lease=10 ** 9)[0] == 400
    assert api.acquire("jetson", None)[0] == 400
    assert api.req("POST", "/r/jetson/acquire", None)[0] == 400  # no body -> no owner
    assert api.acquire("jetson", "a", ticket=999)[0] == 410


def test_malformed_values_are_400_not_500(api, server):
    for bad in ({"wait": [1]}, {"wait": {"a": 1}}, {"wait": "abc"}, {"pid": 10 ** 30}, {"pid": [1]},
                {"ticket": 1e300}, {"ticket": 10 ** 30}, {"lease": [5]}, {"lease": "nan"}):
        st, out = api.acquire("jetson", "a", **bad)
        assert st == 400, (bad, st, out)
    assert api.acquire("jetson", "a", wait="nan")[0] == 200  # NaN wait counts as 0
    assert api.post("/r/jetson/renew", {"owner": "a", "lease": {}})[0] == 400
    assert api.post("/r/jetson/cancel", {"ticket": 1e300})[0] == 400
    assert "internal error" not in server.log.read_text()
    # a negative Content-Length must be rejected, not read until the peer closes
    import socket
    with socket.create_connection(("127.0.0.1", server.port), timeout=5) as s:
        s.sendall(b"POST /r/jetson/acquire HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n")
        assert s.recv(100).startswith(b"HTTP/1.1 400")


def test_holder_reacquire_cannot_delete_another_owners_ticket(api):
    api.acquire("jetson", "a")
    t = api.acquire("jetson", "b", wait=0)[1]["ticket"]
    assert api.acquire("jetson", "a", ticket=t)[0] == 200
    assert [q["ticket"] for q in api.status("jetson")["queue"]] == [t]


def test_status_lists_resources_and_unknown_resource_is_free(api):
    api.acquire("a-res", "x")
    api.acquire("b-res", "y")
    names = [r["resource"] for r in api.status()["resources"]]
    assert names == ["a-res", "b-res"]
    assert api.status("unknown") ["free"] is True
    assert "unknown" not in [r["resource"] for r in api.status()["resources"]]


def test_events_endpoint(api):
    api.acquire("jetson", "a")
    api.post("/r/jetson/release", {"owner": "a"})
    ev = api.events("jetson")
    assert [e["kind"] for e in ev] == ["create", "enqueue", "acquire", "release"]
    after = api.get(f"/events?after={ev[1]['id']}&resource=jetson")[1]["events"]
    assert [e["kind"] for e in after] == ["acquire", "release"]


def test_force_release(api):
    api.acquire("jetson", "stuck")
    st, out = api.post("/r/jetson/force-release", {"by": "marcel", "reason": "agent died"})
    assert st == 200 and out["previous_holder"] == "stuck"
    assert api.status("jetson")["free"]
    e = api.events("jetson", {"force_release"})[0]
    assert e["detail"]["by"] == "marcel" and e["owner"] == "stuck"


def test_html_page(api):
    api.acquire("jetson", "agent-1", reason="<script>alert(1)</script>")
    st, page = api.get("/")
    assert st == 200 and "<title>hwlock</title>" in page and 'http-equiv=refresh' in page
    assert "agent-1" in page and "<script>alert" not in page and "&lt;script&gt;" in page


def test_long_poll_holds_request_and_wakes_on_release(api):
    api.acquire("jetson", "a")
    st, w = api.acquire("jetson", "b", wait=0)
    result = {}

    def waiter():
        t0 = time.monotonic()
        result["resp"] = api.acquire("jetson", "b", ticket=w["ticket"], wait=20)
        result["dt"] = time.monotonic() - t0

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(1.0)
    assert th.is_alive(), "server must hold the long-poll request open"
    api.post("/r/jetson/release", {"owner": "a"})
    th.join(5)
    assert result["resp"][0] == 200 and result["resp"][1]["state"] == "acquired"
    assert 0.9 < result["dt"] < 2.5


def test_long_poll_times_out_with_202_and_is_capped(make_server):
    srv = make_server(HWLOCK_POLL_S="1", HWLOCK_HEARTBEAT_S="6")
    srv.api.acquire("jetson", "a")
    t0 = time.monotonic()
    st, out = srv.api.acquire("jetson", "b", wait=60)  # server caps at HWLOCK_POLL_S
    dt = time.monotonic() - t0
    assert st == 202 and out["state"] == "waiting" and 0.9 < dt < 2.5


def test_lease_expiry_evicts_holder_and_wakes_waiter(api):
    api.acquire("jetson", "short", lease=1)
    t0 = time.monotonic()
    st, out = api.acquire("jetson", "next", wait=10)
    assert st == 200 and out["state"] == "acquired"
    assert 0.8 < time.monotonic() - t0 < 3
    e = api.events("jetson", {"expire"})[0]
    assert e["owner"] == "short"


def test_waiting_ticket_without_heartbeat_is_dropped(make_server):
    srv = make_server(HWLOCK_POLL_S="1", HWLOCK_HEARTBEAT_S="6")
    api = srv.api
    api.acquire("jetson", "holder")
    st, w = api.acquire("jetson", "ghost", wait=0)
    assert st == 202
    wait_for(lambda: api.status("jetson")["queue"] == [], timeout=10)
    assert api.events("jetson", {"drop"})[0]["owner"] == "ghost"
    assert api.acquire("jetson", "ghost", ticket=w["ticket"])[0] == 410


def test_heartbeating_waiter_is_kept(make_server):
    srv = make_server(HWLOCK_POLL_S="1", HWLOCK_HEARTBEAT_S="6")
    api = srv.api
    api.acquire("jetson", "holder")
    _, w = api.acquire("jetson", "patient", wait=0)
    t0 = time.monotonic()
    while time.monotonic() - t0 < 9:  # longer than the heartbeat window, long-polling throughout
        st, out = api.acquire("jetson", "patient", ticket=w["ticket"], wait=1)
        assert st == 202
    assert [q["owner"] for q in api.status("jetson")["queue"]] == ["patient"]


def test_token_auth(make_server):
    srv = make_server(HWLOCK_TOKEN="s3cret")
    api = srv.api
    assert api.get("/health", token=None)[0] == 200
    assert api.get("/status", token=None)[0] == 401
    assert api.get("/status", token="wrong")[0] == 401
    assert api.acquire("jetson", "a", token=None) == (401, {"error": "unauthorized",
                                                            "message": "missing or wrong token"})
    assert api.get("/", token=None)[0] == 401
    assert api.get("/?token=s3cret", token=None)[0] == 200
    assert api.get("/status")[0] == 200
    assert api.acquire("jetson", "a")[0] == 200


def test_admin_token_guards_force_release(make_server):
    srv = make_server(HWLOCK_TOKEN="user", HWLOCK_ADMIN_TOKEN="admin")
    api = srv.api
    api.acquire("jetson", "a")
    assert api.post("/r/jetson/force-release", {"by": "x"})[0] == 401
    assert api.post("/r/jetson/force-release", {"by": "x"}, token="admin")[0] == 200


def test_restart_persists_holder_queue_and_lease_clock(make_server):
    srv = make_server()
    api = srv.api
    api.acquire("jetson", "holder", lease=30)
    tickets = [api.acquire("jetson", f"w{i}", wait=0)[1]["ticket"] for i in range(3)]
    before = api.status("jetson")
    srv.kill()  # SIGKILL mid-queue
    time.sleep(2)
    srv.start()
    after = api.status("jetson")
    assert after["holder"]["owner"] == "holder"
    assert after["holder"]["expires"] == before["holder"]["expires"]  # leases kept counting
    assert after["holder"]["lease_left_s"] < before["holder"]["lease_left_s"] - 1.5
    assert [q["ticket"] for q in after["queue"]] == tickets
    # waiters re-attach by ticket id and are served in their original order
    for i, t in enumerate(tickets):
        st, out = api.acquire("jetson", f"w{i}", ticket=t, wait=0)
        assert st == 202 and out["position"] == 1
        prev = "holder" if i == 0 else f"w{i - 1}"
        api.post("/r/jetson/release", {"owner": prev})
        st, out = api.acquire("jetson", f"w{i}", ticket=t, wait=0)
        assert st == 200, out
    kinds = [e["kind"] for e in api.events()]
    assert kinds.count("server_start") == 2


def test_peer_check_works_for_high_file_descriptors():
    """Connections past fd 1024 (a busy server) must not all look dead (select.select fails there)."""
    import resource
    import socket

    from hwlock_server.web import _peer_alive

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 1200:
        import pytest
        pytest.skip("fd limit too low")
    resource.setrlimit(resource.RLIMIT_NOFILE, (max(soft, 1200), hard))
    pad = [open("/dev/null") for _ in range(1050)]
    try:
        a, b = socket.socketpair()
        assert a.fileno() >= 1024
        assert _peer_alive(a)
        b.sendall(b"x")
        assert _peer_alive(a)
        b.close()
        a.recv(1)
        assert not _peer_alive(a)
        a.close()
    finally:
        for f in pad:
            f.close()
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


GUARD = {"HWLOCK_RESOURCE_MAX_HOLD_S": "jetson=2", "HWLOCK_RESOURCE_HOLD_GRACE_S": "jetson=2"}


def test_renew_over_max_hold_is_409_with_the_deadline_and_shown_in_status(make_server):
    srv = make_server(HWLOCK_MAX_HOLD_S="0", **GUARD)
    api = srv.api
    assert api.acquire("jetson", "hog", lease=60)[0] == 200
    assert api.acquire("gpu", "free-rider", lease=60)[0] == 200
    st, out = api.acquire("jetson", "next", wait=0)
    assert st == 202
    g = api.status("jetson")["holder"]["guard"]
    assert g["max_hold_s"] == 2 and g["grace_s"] == 2 and g["waiting"] == 1 and 0 < g["refuse_in_s"] <= 2
    assert api.status("gpu")["holder"]["guard"] is None  # global 0: only jetson is guarded
    page = api.get("/")[1]
    assert "max hold 2s" in page and "evicted in" in page
    time.sleep(2.2)
    st, err = api.post("/r/jetson/renew", {"owner": "hog"})
    assert st == 409 and err["error"] == "over_max_hold", err
    assert err["waiting"] == 1 and err["max_hold_s"] == 2 and err["grace_s"] == 2 and 0 < err["evict_in_s"] <= 2
    assert "renew refused" in api.get("/")[1]
    st, err = api.acquire("jetson", "hog")  # a re-acquire is refused the same way
    assert st == 409 and err["error"] == "over_max_hold"
    # nobody keeps asking for "next" here, but its ticket is alive (heartbeat 120 s): eviction at since + 4 s
    wait_for(lambda: api.status("jetson")["holder"] is None, timeout=5)
    ev = api.events("jetson", {"renew_refused", "evict_max_hold"})
    assert [e["kind"] for e in ev] == ["renew_refused", "renew_refused", "evict_max_hold"]
    assert ev[-1]["owner"] == "hog" and 4 <= ev[-1]["detail"]["held_s"] < 5
    assert api.acquire("jetson", "next", ticket=out["ticket"], wait=0)[0] == 200


def test_acquire_max_hold_over_http(make_server):
    api = make_server(HWLOCK_MAX_HOLD_S="600").api
    st, out = api.acquire("jetson", "a", max_hold=60)
    assert st == 200 and out["holder"]["guard"]["max_hold_s"] == 60
    assert api.acquire("gpu", "b", max_hold=10**9)[1]["holder"]["guard"]["max_hold_s"] == 600
    for bad in (0, -1, "soon", True):
        st, err = api.acquire("rig", "c", max_hold=bad)
        assert st == 400 and "max_hold" in err["message"], err


def test_guard_since_survives_a_restart(make_server):
    srv = make_server(HWLOCK_MAX_HOLD_S="0", HWLOCK_RESOURCE_MAX_HOLD_S="jetson=3",
                      HWLOCK_RESOURCE_HOLD_GRACE_S="jetson=2")
    api = srv.api
    api.acquire("jetson", "hog", lease=60)
    ticket = api.acquire("jetson", "next", wait=0)[1]["ticket"]
    before = api.status("jetson")["holder"]
    srv.kill()
    time.sleep(1.5)
    srv.start()
    after = api.status("jetson")["holder"]
    assert after["since"] == before["since"] and after["guard"]["evict_at"] == before["guard"]["evict_at"]
    assert after["held_s"] >= 1.5
    t0 = time.time()
    st = 202
    while st == 202 and time.time() - t0 < 10:
        st, out = api.acquire("jetson", "next", ticket=ticket, wait=1)
    assert st == 200, out
    # evicted 3 + 2 s after it was taken, the restart not counting as a new start
    evict = api.events("jetson", {"evict_max_hold"})
    assert len(evict) == 1 and 5 <= evict[0]["detail"]["held_s"] < 6, evict


def test_guard_config_parsing():
    from hwlock_server.web import Config

    c = Config(resource_max_hold_s=" jetson=7200, rig=0 ", resource_hold_grace_s="jetson=300")
    assert c.max_hold_s == 7200 and c.hold_grace_s == 300  # defaults: jetson-lock's
    assert c.resource_max_hold_s == {"jetson": 7200, "rig": 0} and c.resource_hold_grace_s == {"jetson": 300}
    assert Config(max_hold_s=0).max_hold_s == 0
    for bad in ("jetson", "jetson=", "bad name=5", "jetson=x"):
        with pytest.raises(ValueError):
            Config(resource_max_hold_s=bad)
    with pytest.raises(ValueError):
        Config(max_hold_s=-1)


def test_dormant_ticket_is_dropped_after_the_reattach_window(make_server):
    srv = make_server(HWLOCK_REATTACH_S="2")
    api = srv.api
    api.acquire("jetson", "holder")
    t = api.acquire("jetson", "w", wait=0)[1]["ticket"]
    assert api.post("/r/jetson/detach", {"ticket": t, "owner": "w"}) == (200, {
        "state": "dormant", "resource": "jetson", "ticket": t, "window_s": 2.0})
    st = api.status("jetson")
    assert st["queue"] == [] and [d["ticket"] for d in st["dormant"]] == [t]
    page = api.get("/")[1]
    assert "dormant: <b>w</b>" in page and f"ticket {t}" in page
    wait_for(lambda: api.status("jetson")["dormant"] == [], timeout=6)
    assert [(e["owner"], e["ticket"]) for e in api.events("jetson", {"ticket_dropped"})] == [("w", t)]
    assert api.acquire("jetson", "w", ticket=t, wait=0)[0] == 410
    st, out = api.acquire("jetson", "w", wait=0)  # too late: a fresh ticket at the back
    assert st == 202 and out["ticket"] > t and not out.get("reattached")


def test_restart_keeps_dormant_tickets_with_their_remaining_window(make_server):
    srv = make_server(HWLOCK_REATTACH_S="30")
    api = srv.api
    api.acquire("jetson", "holder")
    t1 = api.acquire("jetson", "early", wait=0)[1]["ticket"]
    api.post("/r/jetson/detach", {"ticket": t1, "owner": "early"})
    t2 = api.acquire("jetson", "late", wait=0)[1]["ticket"]
    left = api.status("jetson")["dormant"][0]["dormant_left_s"]
    srv.kill()
    time.sleep(2)
    srv.start()
    st = api.status("jetson")
    assert [d["ticket"] for d in st["dormant"]] == [t1]
    assert left - 6 < st["dormant"][0]["dormant_left_s"] < left - 1.5  # the window kept counting, not reset
    assert [q["ticket"] for q in st["queue"]] == [t2]
    st, out = api.acquire("jetson", "early", wait=0)  # re-attach by owner: its old place, before late
    assert st == 202 and out["ticket"] == t1 and out["position"] == 1 and out["reattached"]
    api.post("/r/jetson/release", {"owner": "holder"})
    assert api.acquire("jetson", "early", ticket=t1, wait=0)[0] == 200


def test_late_detach_or_cancel_of_a_replaced_client_is_superseded(api):
    """Over HTTP: a killed client's late detach/cancel (its own `attach` id) cannot release what the
    re-run of the same acquire (a newer `attach` id) has been granted."""
    assert api.acquire("jetson", "h")[0] == 200
    st, out = api.acquire("jetson", "o", attach="old1")
    t = out["ticket"]
    st, out = api.acquire("jetson", "o", attach="new1")
    assert st == 202 and out["ticket"] == t and out["reattached"]
    api.post("/r/jetson/release", {"owner": "h"})
    assert api.acquire("jetson", "o", ticket=t, attach="new1")[0] == 200
    for op in ("detach", "cancel"):
        st, out = api.post(f"/r/jetson/{op}", {"ticket": t, "owner": "o", "attach": "old1"})
        assert st == 200 and out["state"] == "superseded", out
    assert api.status("jetson")["holder"]["owner"] == "o"
    st, out = api.post("/r/jetson/detach", {"ticket": t, "owner": "o", "attach": "bad id!"})
    assert st == 400
    st, out = api.post("/r/jetson/cancel", {"ticket": t, "owner": "o", "attach": "new1"})
    assert out["state"] == "released" and api.status("jetson")["free"]

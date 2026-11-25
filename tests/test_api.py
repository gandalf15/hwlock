"""HTTP API tests against a real server subprocess (fresh port + DB per test)."""

import threading
import time

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

"""Unit tests of the state machine (hwlock_server.core.Store) with an injected clock."""

import threading
import time

import pytest

from hwlock_server.core import BadRequest, NotHolder, OverMaxHold, Store, TicketGone


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def store(tmp_path, clock):
    s = Store(str(tmp_path / "s.db"), default_lease_s=100, max_lease_s=1000, heartbeat_s=60, clock=clock)
    yield s
    s.close()


def kinds(store, res="gpu"):
    return [e["kind"] for e in store.events(res, limit=1000)["events"]]


def test_free_resource_is_granted_at_once_and_created_on_first_use(store):
    assert store.status()["resources"] == []
    out = store.acquire_step("gpu", "alice", reason="bench", host="pc", pid=42)
    assert out["state"] == "acquired"
    h = store.status("gpu")["holder"]
    assert (h["owner"], h["reason"], h["host"], h["pid"], h["lease_left_s"]) == ("alice", "bench", "pc", 42, 100)
    assert kinds(store) == ["create", "enqueue", "acquire"]


def test_second_owner_waits_in_fifo_order(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")
    c = store.acquire_step("gpu", "c")
    assert (b["state"], b["position"], c["position"], c["queue_length"]) == ("waiting", 1, 2, 2)
    assert b["holder"]["owner"] == "a"
    store.release("gpu", "a")
    # c asks first, but b is ahead: c keeps waiting and must not jump the queue
    assert store.acquire_step("gpu", "c", ticket=c["ticket"])["state"] == "waiting"
    assert store.acquire_step("gpu", "b", ticket=b["ticket"])["state"] == "acquired"
    store.release("gpu", "b")
    assert store.acquire_step("gpu", "c", ticket=c["ticket"])["state"] == "acquired"


def test_reacquire_by_holder_is_idempotent_and_refreshes_lease(store, clock):
    first = store.acquire_step("gpu", "a")
    clock.advance(90)
    again = store.acquire_step("gpu", "a", lease_s=200)
    assert again["state"] == "acquired" and again["reacquired"] is True
    assert again["ticket"] == first["ticket"]
    assert store.status("gpu")["holder"]["lease_left_s"] == 200
    assert store.status("gpu")["queue"] == []


def test_release_and_renew_by_non_holder_are_conflicts(store):
    store.acquire_step("gpu", "a")
    with pytest.raises(NotHolder):
        store.release("gpu", "b")
    with pytest.raises(NotHolder):
        store.renew("gpu", "b")
    with pytest.raises(NotHolder):
        store.release("never-created", "a")
    assert store.status("gpu")["holder"]["owner"] == "a"


def test_renew_extends_lease(store, clock):
    store.acquire_step("gpu", "a", lease_s=10)
    clock.advance(8)
    store.renew("gpu", "a", lease_s=10)
    clock.advance(8)
    assert store.status("gpu")["holder"]["owner"] == "a"
    clock.advance(3)
    assert store.status("gpu")["holder"] is None
    assert "expire" in kinds(store)


def test_expired_holder_is_evicted_and_next_waiter_gets_it(store, clock):
    store.acquire_step("gpu", "a", lease_s=5)
    b = store.acquire_step("gpu", "b")
    clock.advance(6)
    assert store.acquire_step("gpu", "b", ticket=b["ticket"])["state"] == "acquired"
    ev = store.events("gpu")["events"]
    exp = [e for e in ev if e["kind"] == "expire"][0]
    assert exp["owner"] == "a" and exp["detail"]["overdue_s"] == pytest.approx(1)
    with pytest.raises(NotHolder):
        store.renew("gpu", "a")


def test_waiting_ticket_without_heartbeat_is_dropped(store, clock):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")
    c = store.acquire_step("gpu", "c")
    clock.advance(40)
    store.acquire_step("gpu", "c", ticket=c["ticket"])  # c heartbeats, b does not
    clock.advance(30)
    store.sweep()
    assert [q["owner"] for q in store.status("gpu")["queue"]] == ["c"]
    with pytest.raises(TicketGone):
        store.acquire_step("gpu", "b", ticket=b["ticket"])
    store.release("gpu", "a")
    assert store.acquire_step("gpu", "c", ticket=c["ticket"])["state"] == "acquired"
    assert "drop" in kinds(store)


def test_dead_waiter_is_never_granted(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")
    c = store.acquire_step("gpu", "c")
    store.release("gpu", "a")
    out = store.acquire_step("gpu", "b", ticket=b["ticket"], may_grant=lambda: False)
    assert out["state"] == "abandoned"
    assert store.status("gpu")["holder"] is None
    assert store.acquire_step("gpu", "c", ticket=c["ticket"])["state"] == "acquired"


def test_cancel_withdraws_ticket_or_releases_if_already_granted(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")
    assert store.cancel("gpu", b["ticket"])["state"] == "cancelled"
    assert store.status("gpu")["queue"] == []
    a_ticket = store.status("gpu")["holder"]["ticket"]
    assert store.cancel("gpu", a_ticket, "someone-else")["state"] == "unknown"
    assert store.cancel("gpu", a_ticket, "a")["state"] == "released"
    assert store.status("gpu")["holder"] is None


def test_force_release_is_logged(store):
    store.acquire_step("gpu", "a")
    out = store.force_release("gpu", "admin", "stuck job")
    assert out["previous_holder"] == "a"
    e = store.events("gpu")["events"][-1]
    assert (e["kind"], e["owner"], e["detail"]["by"], e["detail"]["reason"]) == (
        "force_release", "a", "admin", "stuck job")
    with pytest.raises(NotHolder):
        store.force_release("gpu", "admin")


def test_lease_limits_and_name_validation(store):
    with pytest.raises(BadRequest):
        store.acquire_step("gpu", "a", lease_s=1001)
    with pytest.raises(BadRequest):
        store.acquire_step("gpu", "a", lease_s=0)
    with pytest.raises(BadRequest):
        store.acquire_step("gpu", "bad owner")
    with pytest.raises(BadRequest):
        store.acquire_step("../etc", "a")
    assert store.status()["resources"] == []


def test_resources_are_independent(store):
    assert store.acquire_step("gpu", "a")["state"] == "acquired"
    assert store.acquire_step("robot", "b")["state"] == "acquired"
    assert store.acquire_step("gpu", "b")["state"] == "waiting"


def test_state_survives_reopen_and_leases_keep_counting(tmp_path, clock):
    path = str(tmp_path / "p.db")
    s = Store(path, default_lease_s=100, heartbeat_s=60, clock=clock)
    s.acquire_step("gpu", "a")
    b = s.acquire_step("gpu", "b")
    c = s.acquire_step("gpu", "c")
    s.close()
    clock.advance(90)  # "down" for 90 s: longer than heartbeat_s, shorter than the lease
    s = Store(path, default_lease_s=100, heartbeat_s=60, clock=clock)
    st = s.status("gpu")
    assert st["holder"]["owner"] == "a" and st["holder"]["lease_left_s"] == 10
    assert [q["ticket"] for q in st["queue"]] == [b["ticket"], c["ticket"]]  # waiters got a fresh window
    clock.advance(11)
    assert s.acquire_step("gpu", "b", ticket=b["ticket"])["state"] == "acquired"


def test_long_poll_wakes_on_release(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")
    result = {}

    def waiter():
        t0 = time.monotonic()
        result["out"] = store.acquire("gpu", "b", ticket=b["ticket"], wait_s=10)
        result["dt"] = time.monotonic() - t0

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.3)
    store.release("gpu", "a")
    th.join(5)
    assert result["out"]["state"] == "acquired"
    assert 0.25 < result["dt"] < 2


def test_long_poll_returns_waiting_after_wait_s(store):
    store.acquire_step("gpu", "a")
    t0 = time.monotonic()
    out = store.acquire("gpu", "b", wait_s=0.5)
    dt = time.monotonic() - t0
    assert out["state"] == "waiting" and 0.45 < dt < 1.5


def test_long_poll_wakes_on_release_that_races_its_wait_timeout(store):
    """The waiter re-checks its socket about once a second. A release landing exactly as one of those
    1 s waits times out must still wake it (Condition.wait() reports such a notify as a timeout)."""
    store.acquire_step("gpu", "a")
    t = store.acquire_step("gpu", "b")["ticket"]
    out = {}

    def waiter():
        t0 = time.monotonic()
        out["res"] = store.acquire("gpu", "b", ticket=t, wait_s=5)
        out["dt"] = time.monotonic() - t0

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(1.0)
    store.release("gpu", "a")
    th.join(10)
    assert out["res"]["state"] == "acquired" and out["dt"] < 2


def test_long_poll_drops_ticket_when_client_hangs_up_before_its_turn(store):
    store.acquire_step("gpu", "a")
    t = store.acquire_step("gpu", "dead")["ticket"]
    alive = {"v": True}
    out = {}
    th = threading.Thread(target=lambda: out.update(
        res=store.acquire("gpu", "dead", ticket=t, wait_s=10, may_grant=lambda: alive["v"])))
    th.start()
    time.sleep(0.3)
    alive["v"] = False
    th.join(3)
    assert out["res"]["state"] == "abandoned"
    assert store.status("gpu")["queue"] == [] and kinds(store)[-1] == "abandon"


def test_starvation_guard_refuses_renew_then_evicts_while_others_wait(tmp_path, clock):
    s = Store(str(tmp_path / "g.db"), default_lease_s=200, max_lease_s=1000, heartbeat_s=10_000,
              max_hold_s=500, hold_grace_s=50, clock=clock)
    s.acquire_step("gpu", "hog")
    clock.advance(90)
    s.renew("gpu", "hog")
    t = s.acquire_step("gpu", "next")["ticket"]  # waits from t=90, before the max hold
    for _ in range(4):
        clock.advance(90)
        s.renew("gpu", "hog")  # held 450 s <= 500 s: fine although someone waits
    clock.advance(90)  # held 540 s
    with pytest.raises(OverMaxHold) as e:
        s.renew("gpu", "hog")
    assert e.value.status == 409 and e.value.code == "over_max_hold"
    x = e.value.extra
    assert (x["waiting"], x["max_hold_s"], x["grace_s"], x["held_s"]) == (1, 500, 50, 540)
    assert x["evict_in_s"] == 10  # since + 500 + 50
    with pytest.raises(OverMaxHold):
        s.acquire_step("gpu", "hog")  # re-acquire is a renewal too
    assert kinds(s)[-1] == "renew_refused"  # the refusal is logged (not rolled back)
    clock.advance(5)  # held 545 s < 550 s: not evicted yet
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "waiting"
    clock.advance(6)  # held 551 s
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "acquired"
    ev = s.events("gpu", limit=1000)["events"]
    evict = [e for e in ev if e["kind"] == "evict_max_hold"]
    assert len(evict) == 1 and evict[0]["owner"] == "hog" and evict[0]["detail"]["held_s"] == 551
    s.close()


def test_holder_alone_past_max_hold_still_gets_the_grace_once_someone_waits(tmp_path, clock):
    s = Store(str(tmp_path / "g.db"), default_lease_s=100, max_lease_s=1000, heartbeat_s=10_000,
              max_hold_s=500, hold_grace_s=50, clock=clock)
    s.acquire_step("gpu", "hog")
    for _ in range(10):  # nobody waiting: no limit
        clock.advance(90)
        s.renew("gpu", "hog")
    assert s.status("gpu")["holder"]["owner"] == "hog"  # held 900 s > 500 s, still fine
    t = s.acquire_step("gpu", "next")["ticket"]  # arrives at 900 s
    g = s.status("gpu")["holder"]["guard"]
    assert g["refuse_in_s"] == -400 and g["evict_in_s"] == 50  # from the waiter's arrival, not since + 550
    with pytest.raises(OverMaxHold):
        s.renew("gpu", "hog")
    clock.advance(49)
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "waiting"
    clock.advance(2)
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "acquired"
    s.close()


def blind_renewer_vs_waiter(s, clock, step=60, until=20_000):
    """A holder that renews blindly every `step` s while a live waiter keeps asking.
    Returns (time the waiter got it, first refusal time) relative to the holder's acquire."""
    s.acquire_step("jetson", "proxy-hold")
    t0 = clock()
    clock.advance(10)
    ticket = s.acquire_step("jetson", "job")["ticket"]
    refused = None
    while clock() - t0 < until:
        clock.advance(step)
        try:
            s.renew("jetson", "proxy-hold")
        except OverMaxHold:
            refused = refused or clock() - t0
        except NotHolder:
            pass
        if s.acquire_step("jetson", "job", ticket=ticket)["state"] == "acquired":
            return clock() - t0, refused
    return None, refused


def test_guard_is_on_by_default_2h_plus_300s_against_a_blind_renewer(tmp_path, clock):
    """The incident: an idle proxy hold renewing blindly while a job waits. Defaults are the
    jetson-lock ones (7200 s max hold, 300 s grace); the waiter keeps heartbeating."""
    s = Store(str(tmp_path / "d.db"), clock=clock)
    got, refused = blind_renewer_vs_waiter(s, clock)
    assert refused is not None and 7200 < refused <= 7200 + 60
    assert got is not None and 7200 + 300 < got <= 7200 + 300 + 60
    assert "evict_max_hold" in kinds(s, "jetson")
    s.close()


def test_no_waiter_means_no_limit(tmp_path, clock):
    s = Store(str(tmp_path / "n.db"), max_hold_s=500, hold_grace_s=50, clock=clock)
    s.acquire_step("gpu", "hog")
    for _ in range(300):  # 30000 s, 60x the max hold
        clock.advance(100)
        s.renew("gpu", "hog")
        s.sweep()
    h = s.status("gpu")["holder"]
    assert h["owner"] == "hog" and h["guard"] == {"max_hold_s": 500, "grace_s": 50}
    assert "renew_refused" not in kinds(s) and "evict_max_hold" not in kinds(s)
    s.close()


def test_max_hold_0_turns_the_guard_off(tmp_path, clock):
    s = Store(str(tmp_path / "o.db"), max_hold_s=0, heartbeat_s=10_000, clock=clock)
    assert blind_renewer_vs_waiter(s, clock) == (None, None)
    assert s.status("jetson")["holder"]["guard"] is None
    s.close()


def test_a_dead_waiter_does_not_evict(tmp_path, clock):
    s = Store(str(tmp_path / "w.db"), max_hold_s=100, hold_grace_s=10, heartbeat_s=60, clock=clock)
    s.acquire_step("gpu", "hog")
    s.acquire_step("gpu", "ghost")  # never asks again
    for _ in range(10):
        clock.advance(30)
        s.renew("gpu", "hog")
    assert s.status("gpu")["holder"]["owner"] == "hog"
    assert "drop" in kinds(s) and "evict_max_hold" not in kinds(s)
    s.close()


def test_per_resource_limits_override_the_global_ones(tmp_path, clock):
    s = Store(str(tmp_path / "r.db"), max_hold_s=0, hold_grace_s=300, heartbeat_s=10_000, clock=clock,
              resource_max_hold_s={"jetson": 100}, resource_hold_grace_s={"jetson": 20})
    got, refused = blind_renewer_vs_waiter(s, clock, step=10, until=1000)
    assert 100 < refused <= 110 and 120 <= got <= 130
    s.acquire_step("gpu", "hog")
    assert s.status("gpu")["holder"]["guard"] is None  # global 0: gpu unguarded
    assert s.limits("jetson") == (100, 20) and s.limits("gpu") == (0, 300)
    s.close()


def test_acquire_max_hold_lowers_the_limit_and_is_capped_by_the_server(tmp_path, clock):
    s = Store(str(tmp_path / "m.db"), max_hold_s=500, hold_grace_s=50, heartbeat_s=10_000, clock=clock)
    out = s.acquire_step("a", "short", max_hold_s=60)
    assert out["holder"]["guard"]["max_hold_s"] == 60
    s.acquire_step("b", "greedy", max_hold_s=99_999)
    assert s.status("b")["holder"]["guard"]["max_hold_s"] == 500  # capped by the server
    for bad in (0, -5, "x", True, float("nan"), float("inf")):
        with pytest.raises(BadRequest):
            s.acquire_step("c", "bad", max_hold_s=bad)
    # a re-acquire cannot raise or reset it
    s.acquire_step("a", "short", max_hold_s=400)
    assert s.status("a")["holder"]["guard"]["max_hold_s"] == 60
    t = s.acquire_step("a", "next")["ticket"]
    clock.advance(61)
    with pytest.raises(OverMaxHold):
        s.renew("a", "short")
    clock.advance(50)
    assert s.acquire_step("a", "next", ticket=t)["state"] == "acquired"
    assert s.status("a")["holder"]["guard"]["max_hold_s"] == 500  # the next holder has the server's
    s.close()
    # no server limit: a holder may still volunteer one
    s = Store(str(tmp_path / "m0.db"), max_hold_s=0, clock=clock)
    assert s.acquire_step("a", "polite", max_hold_s=30)["holder"]["guard"]["max_hold_s"] == 30
    s.close()


def test_since_and_the_guard_survive_a_restart(tmp_path, clock):
    path = str(tmp_path / "p.db")
    s = Store(path, max_hold_s=500, hold_grace_s=50, heartbeat_s=1000, clock=clock)
    s.acquire_step("gpu", "hog", max_hold_s=300)
    since = s.status("gpu")["holder"]["since"]
    clock.advance(100)
    t = s.acquire_step("gpu", "next")["ticket"]
    s.close()
    clock.advance(150)  # down for a while
    s = Store(path, max_hold_s=500, hold_grace_s=50, heartbeat_s=1000, clock=clock)
    h = s.status("gpu")["holder"]
    assert h["since"] == since and h["held_s"] == 250 and h["guard"]["max_hold_s"] == 300
    assert h["guard"]["evict_at"] == since + 300 + 50
    clock.advance(51)  # held 301 s
    with pytest.raises(OverMaxHold):
        s.renew("gpu", "hog")
    clock.advance(50)  # held 351 s
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "acquired"
    s.close()


def test_old_database_gets_the_new_column(tmp_path, clock):
    import sqlite3

    from hwlock_server import core

    path = str(tmp_path / "old.db")
    db = sqlite3.connect(path)
    db.executescript(core.SCHEMA.replace(",\n    holder_max_hold_s REAL", ""))
    db.execute("INSERT INTO resources(name, created, holder_owner, holder_ticket, since, expires, lease_s)"
               " VALUES ('gpu', ?, 'hog', 1, ?, ?, 100)", (clock(), clock() - 10, clock() + 90))
    db.commit()
    assert "holder_max_hold_s" not in {r[1] for r in db.execute("PRAGMA table_info(resources)")}
    db.close()
    for _ in range(2):  # idempotent
        s = Store(path, max_hold_s=500, clock=clock)
        h = s.status("gpu")["holder"]
        assert h["owner"] == "hog" and h["held_s"] == 10 and h["guard"]["max_hold_s"] == 500
        s.close()


def _guarded(tmp_path, clock, name="e.db"):
    return Store(str(tmp_path / name), default_lease_s=900, max_lease_s=1000, heartbeat_s=10_000,
                 max_hold_s=500, hold_grace_s=50, clock=clock)


def test_reacquire_cannot_reset_since_or_raise_the_limit_while_others_wait(tmp_path, clock):
    s = _guarded(tmp_path, clock)
    s.acquire_step("gpu", "hog")
    since = s.status("gpu")["holder"]["since"]
    t = s.acquire_step("gpu", "next")["ticket"]
    for _ in range(5):  # 450 s of re-acquires, each asking for a longer max hold
        clock.advance(90)
        assert s.acquire_step("gpu", "hog", max_hold_s=100_000)["reacquired"]
    h = s.status("gpu")["holder"]
    assert h["since"] == since and h["guard"]["max_hold_s"] == 500
    clock.advance(60)  # held 510 s
    with pytest.raises(OverMaxHold):
        s.acquire_step("gpu", "hog")
    clock.advance(41)  # 551 s > 500 + 50
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "acquired"


def test_waiter_leaving_mid_grace_stops_the_eviction_and_a_new_one_gets_a_full_grace(tmp_path, clock):
    s = _guarded(tmp_path, clock)
    s.acquire_step("gpu", "hog")
    t = s.acquire_step("gpu", "next")["ticket"]
    clock.advance(520)
    with pytest.raises(OverMaxHold):
        s.renew("gpu", "hog")
    s.cancel("gpu", t)  # the only waiter leaves during the grace
    clock.advance(100)  # past the old eviction deadline
    assert s.status("gpu")["holder"]["owner"] == "hog"
    s.renew("gpu", "hog")  # nobody waits: no limit again
    t2 = s.acquire_step("gpu", "late")["ticket"]
    assert s.status("gpu")["holder"]["guard"]["evict_in_s"] == 50  # from the new waiter's arrival
    with pytest.raises(OverMaxHold):
        s.renew("gpu", "hog")
    clock.advance(51)
    assert s.acquire_step("gpu", "late", ticket=t2)["state"] == "acquired"
    assert kinds(s).count("evict_max_hold") == 1


def test_a_restart_across_the_eviction_deadline_evicts_at_once(tmp_path, clock):
    s = _guarded(tmp_path, clock)
    s.acquire_step("gpu", "hog")
    t = s.acquire_step("gpu", "next")["ticket"]
    clock.advance(520)
    s.close()
    clock.advance(100)  # the server was down while the deadline (550 s) passed
    s = _guarded(tmp_path, clock)
    assert s.acquire_step("gpu", "next", ticket=t)["state"] == "acquired"
    assert "evict_max_hold" in kinds(s)
    s.close()


# ---------------------------------------------------------------- dormant tickets (re-attach window)
def _hang_up(s, res, owner, ticket):
    """The waiting client's connection closes while it is first in line (killed, tool timeout)."""
    return s.acquire_step(res, owner, ticket=ticket, may_grant=lambda: False)


def test_hung_up_ticket_is_dormant_and_a_rerun_reattaches_to_its_place(store, clock):
    store.acquire_step("gpu", "holder")
    early = store.acquire_step("gpu", "early")["ticket"]
    assert store.detach("gpu", early, "early")["state"] == "dormant"
    late = store.acquire_step("gpu", "late")["ticket"]
    st = store.status("gpu")
    assert [q["owner"] for q in st["queue"]] == ["late"]
    assert [(d["owner"], d["ticket"], d["dormant_left_s"]) for d in st["dormant"]] == [("early", early, 120)]
    clock.advance(30)
    out = store.acquire_step("gpu", "early", pid=99)  # a new process, no ticket id: re-attach by owner
    assert out["ticket"] == early and out["reattached"] and out["position"] == 1
    assert [q["ticket"] for q in store.status("gpu")["queue"]] == [early, late]
    assert store.status("gpu")["queue"][0]["pid"] == 99
    store.release("gpu", "holder")
    assert store.acquire_step("gpu", "late", ticket=late)["state"] == "waiting"
    assert store.acquire_step("gpu", "early", ticket=early)["state"] == "acquired"
    assert ["ticket_dormant", "ticket_reattached"] == [k for k in kinds(store) if k.startswith("ticket_")]


def test_dormant_ticket_never_blocks_a_free_lock(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")["ticket"]
    c = store.acquire_step("gpu", "c")["ticket"]
    store.release("gpu", "a")
    assert _hang_up(store, "gpu", "b", b)["state"] == "abandoned"  # its turn, but its client is gone
    assert store.status("gpu")["holder"] is None
    assert [d["ticket"] for d in store.status("gpu")["dormant"]] == [b]
    assert store.acquire_step("gpu", "c", ticket=c)["state"] == "acquired"
    store.release("gpu", "c")
    assert store.acquire_step("gpu", "d")["state"] == "acquired"  # only a dormant ticket left: free
    assert kinds(store).count("abandon") == 1 and kinds(store).count("ticket_dormant") == 1


def test_dormant_ticket_is_dropped_after_its_window(store, clock):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")["ticket"]
    store.detach("gpu", b, "b")
    clock.advance(119)
    store.sweep()
    assert [d["ticket"] for d in store.status("gpu")["dormant"]] == [b]
    clock.advance(2)
    store.sweep()
    assert store.status("gpu")["dormant"] == [] and store.status("gpu")["queue"] == []
    assert kinds(store)[-1] == "ticket_dropped"
    with pytest.raises(TicketGone):
        store.acquire_step("gpu", "b", ticket=b)
    out = store.acquire_step("gpu", "b")
    assert out["ticket"] > b and not out["reattached"]


def test_dormant_window_is_configurable_and_0_drops_at_once(tmp_path, clock):
    s = Store(str(tmp_path / "w.db"), reattach_s=10, clock=clock)
    s.acquire_step("gpu", "a")
    b = s.acquire_step("gpu", "b")["ticket"]
    assert s.detach("gpu", b, "b")["window_s"] == 10
    clock.advance(11)
    s.sweep()
    assert s.status("gpu")["dormant"] == []
    s.close()
    s = Store(str(tmp_path / "z.db"), reattach_s=0, clock=clock)
    s.acquire_step("gpu", "a")
    b = s.acquire_step("gpu", "b")["ticket"]
    s.detach("gpu", b, "b")
    st = s.status("gpu")
    assert st["queue"] == [] and st["dormant"] == []
    s.close()


def test_explicit_ticket_id_revives_its_dormant_ticket(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")["ticket"]
    store.detach("gpu", b, "b")
    out = store.acquire_step("gpu", "b", ticket=b)  # a transient disconnect, then the next poll
    assert out["state"] == "waiting" and out["ticket"] == b and out["reattached"]
    assert store.status("gpu")["dormant"] == []


def test_cancel_removes_a_dormant_ticket(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")["ticket"]
    store.detach("gpu", b, "b")
    assert store.cancel("gpu", b, "b")["state"] == "cancelled"
    assert store.status("gpu")["dormant"] == []
    assert not store.acquire_step("gpu", "b")["reattached"]


def test_new_ticket_drops_the_old_one_and_joins_at_the_back(store):
    store.acquire_step("gpu", "a")
    b = store.acquire_step("gpu", "b")["ticket"]
    store.detach("gpu", b, "b")
    c = store.acquire_step("gpu", "c")["ticket"]
    out = store.acquire_step("gpu", "b", new_ticket=True)
    assert out["ticket"] > c and not out["reattached"]
    st = store.status("gpu")
    assert [q["owner"] for q in st["queue"]] == ["c", "b"] and st["dormant"] == []


def test_two_live_waiters_with_one_owner_share_one_ticket(store):
    store.acquire_step("gpu", "a")
    t1 = store.acquire_step("gpu", "same")["ticket"]
    out = store.acquire_step("gpu", "same")
    assert out["ticket"] == t1 and out["reattached"]
    assert [q["ticket"] for q in store.status("gpu")["queue"]] == [t1]


def test_holder_reacquire_stays_immediate_with_a_dormant_ticket_around(store):
    store.acquire_step("gpu", "me")
    o = store.acquire_step("gpu", "other")["ticket"]
    store.detach("gpu", o, "other")
    assert store.acquire_step("gpu", "me")["reacquired"]


def test_detach_of_a_ticket_granted_meanwhile_releases(store):
    t = store.acquire_step("gpu", "a")["ticket"]
    assert store.detach("gpu", t, "a")["state"] == "released"
    assert store.status("gpu")["holder"] is None


def test_restart_keeps_dormant_tickets_with_their_remaining_window(tmp_path, clock):
    path = str(tmp_path / "r.db")
    s = Store(path, heartbeat_s=60, clock=clock)
    s.acquire_step("gpu", "a")
    b = s.acquire_step("gpu", "b")["ticket"]
    s.detach("gpu", b, "b")
    c = s.acquire_step("gpu", "c")["ticket"]
    clock.advance(50)
    s.close()
    clock.advance(30)  # down for 30 s: the window keeps counting (wall clock, like leases)
    s = Store(path, heartbeat_s=60, clock=clock)
    st = s.status("gpu")
    assert [(d["ticket"], d["dormant_left_s"]) for d in st["dormant"]] == [(b, 40)]
    assert [q["ticket"] for q in st["queue"]] == [c]
    assert s.acquire_step("gpu", "b")["ticket"] == b
    s.close()
    clock.advance(10)
    s = Store(path, heartbeat_s=60, clock=clock)
    s.detach("gpu", b, "b")
    s.close()
    clock.advance(200)  # down longer than the window: dropped on the first sweep
    s = Store(path, heartbeat_s=60, clock=clock)
    assert s.status("gpu")["dormant"] == [] and "ticket_dropped" in kinds(s)
    s.close()


def test_dormant_ticket_does_not_count_for_the_guard_and_a_reattach_gets_a_full_grace(tmp_path, clock):
    def guarded():
        return Store(str(tmp_path / "g.db"), default_lease_s=900, max_lease_s=1000, heartbeat_s=10_000,
                     max_hold_s=500, hold_grace_s=50, reattach_s=10_000, clock=clock)

    s = guarded()
    s.acquire_step("gpu", "hog")
    w = s.acquire_step("gpu", "w")["ticket"]
    s.detach("gpu", w, "w")
    clock.advance(600)  # past max hold, but nobody live waits
    s.renew("gpu", "hog")
    assert "waiting" not in s.status("gpu")["holder"]["guard"]
    s.sweep()
    assert s.status("gpu")["holder"]["owner"] == "hog"
    s.close()
    s = guarded()
    assert s.acquire_step("gpu", "w")["reattached"]
    assert s.status("gpu")["holder"]["guard"]["evict_in_s"] == 50  # from the re-attach, not its creation
    s.close()


def test_old_database_gets_the_dormant_columns(tmp_path, clock):
    import sqlite3

    from hwlock_server import core

    path = str(tmp_path / "old2.db")
    db = sqlite3.connect(path)
    db.executescript(core.SCHEMA.replace(",\n    dormant_until REAL", "").replace(",\n    live_since REAL", ""))
    db.execute("INSERT INTO resources(name, created) VALUES ('gpu', ?)", (clock() - 9,))
    db.execute("INSERT INTO tickets(resource, owner, created, last_seen) VALUES ('gpu', 'w', ?, ?)",
               (clock() - 5, clock()))
    db.commit()
    assert "dormant_until" not in {r[1] for r in db.execute("PRAGMA table_info(tickets)")}
    db.close()
    s = Store(path, clock=clock)
    q = s.status("gpu")["queue"]
    assert [x["owner"] for x in q] == ["w"] and q[0]["waiting_s"] == 5
    s.close()
    s = Store(path, clock=clock)  # idempotent
    out = s.acquire_step("gpu", "w")
    assert out["state"] == "acquired" and out["reattached"]
    s.close()


def test_detach_ends_the_clients_own_long_poll_without_reviving_the_ticket(store):
    """SIGTERM: the client detaches while its long-poll is still open on the server. The detach wakes
    that poll; it must not re-attach the ticket it was polling for (only a new request does)."""
    store.acquire_step("gpu", "a")
    t = store.acquire_step("gpu", "b")["ticket"]
    out = {}
    th = threading.Thread(target=lambda: out.update(res=store.acquire("gpu", "b", ticket=t, wait_s=5)))
    th.start()
    time.sleep(0.3)
    store.detach("gpu", t, "b")
    th.join(3)
    assert out["res"]["state"] == "waiting" and out["res"]["dormant"]
    assert [d["ticket"] for d in store.status("gpu")["dormant"]] == [t]
    assert "ticket_reattached" not in kinds(store)

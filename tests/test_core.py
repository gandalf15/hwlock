"""Unit tests of the state machine (hwlock_server.core.Store) with an injected clock."""

import threading
import time

import pytest

from hwlock_server.core import BadRequest, NotHolder, Store, TicketGone


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

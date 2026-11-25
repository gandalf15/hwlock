"""hwlock state machine: named resources, one holder each, a FIFO ticket queue, leases, heartbeats.

All state lives in SQLite (WAL). The server is one process; every state change runs under one
threading lock inside one write transaction, together with its audit event, so changes are atomic
and two holders are impossible (the holder is a set of columns on the resource row).

Time: leases and heartbeats use the server's wall clock (absolute epoch seconds), so a lease keeps
counting while the server is down. Long-poll deadlines use a monotonic clock.

Grant rule (same as the bash prototype): the lock goes only to the FIRST waiting ticket, and only
while that ticket's own client is actively asking for it (an acquire request is in flight and its
connection is still open). A dead waiter therefore never receives the lock; it stops heartbeating and
its ticket is dropped after `heartbeat_s`.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable

NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
MAX_TEXT = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS resources (
    name          TEXT PRIMARY KEY,
    created       REAL NOT NULL,
    holder_owner  TEXT,
    holder_reason TEXT,
    holder_host   TEXT,
    holder_pid    INTEGER,
    holder_ticket INTEGER,
    since         REAL,
    expires       REAL,
    lease_s       REAL,
    holder_max_hold_s REAL
);
CREATE TABLE IF NOT EXISTS tickets (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    resource  TEXT NOT NULL,
    owner     TEXT NOT NULL,
    reason    TEXT,
    host      TEXT,
    pid       INTEGER,
    created   REAL NOT NULL,
    last_seen REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS tickets_resource ON tickets(resource, id);
CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       REAL NOT NULL,
    resource TEXT,
    kind     TEXT NOT NULL,
    owner    TEXT,
    ticket   INTEGER,
    detail   TEXT
);
CREATE INDEX IF NOT EXISTS events_resource ON events(resource, id);
"""


class HwlockError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, **extra: Any):
        super().__init__(message)
        self.message = message
        self.extra = extra

    def payload(self) -> dict:
        return {"error": self.code, "message": self.message, **self.extra}


class BadRequest(HwlockError):
    status, code = 400, "bad_request"


class NotHolder(HwlockError):
    status, code = 409, "not_holder"


class OverMaxHold(HwlockError):
    status, code = 409, "over_max_hold"


class TicketGone(HwlockError):
    status, code = 410, "ticket_gone"


def _name(value: Any, what: str) -> str:
    if not isinstance(value, str) or not NAME_RE.match(value):
        raise BadRequest(f"{what} must match [A-Za-z0-9._-]{{1,64}}: {value!r}")
    return value


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)[:MAX_TEXT]


def _pid(value: Any) -> int | None:
    if value is None:
        return None
    try:
        pid = int(value)
    except (TypeError, ValueError, OverflowError):
        raise BadRequest(f"pid must be an integer: {value!r}") from None
    if not -2**63 <= pid < 2**63:
        raise BadRequest(f"pid out of range: {value!r}")
    return pid


class Store:
    def __init__(
        self,
        path: str,
        *,
        default_lease_s: float = 1800,
        max_lease_s: float = 86400,
        heartbeat_s: float = 120,
        max_hold_s: float = 7200,
        hold_grace_s: float = 300,
        resource_max_hold_s: dict | None = None,
        resource_hold_grace_s: dict | None = None,
        clock: Callable[[], float] = time.time,
        log: Callable[[str], None] | None = None,
    ):
        if default_lease_s > max_lease_s:
            raise ValueError("default lease exceeds max lease")
        self.default_lease_s = default_lease_s
        self.max_lease_s = max_lease_s
        self.heartbeat_s = heartbeat_s
        # Starvation guard (0 = off): while others wait, a holder may keep the lock at most max_hold_s
        # from when it took it; after that renew/re-acquire is refused, and hold_grace_s later it is evicted.
        # Per-resource values (resource_*) override the global ones; see guard().
        self.max_hold_s = max_hold_s
        self.hold_grace_s = hold_grace_s
        self.resource_max_hold_s = dict(resource_max_hold_s or {})
        self.resource_hold_grace_s = dict(resource_hold_grace_s or {})
        self.clock = clock
        self.log = log
        self.lock = threading.RLock()
        self._dirty = False
        self.changes = 0  # bumped on every committed state change (wait()'s return value can miss one)
        self.cond = threading.Condition(self.lock)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        # Databases from before the per-acquire max hold lack its column (ALTER is not idempotent).
        cols = {c["name"] for c in self.db.execute("PRAGMA table_info(resources)")}
        if "holder_max_hold_s" not in cols:
            self.db.execute("ALTER TABLE resources ADD COLUMN holder_max_hold_s REAL")
        with self._tx() as now:
            # Waiting clients could not heartbeat while we were down: give them a fresh window to
            # re-attach by ticket id. Holders keep their absolute expiry (leases keep counting).
            n = self.db.execute("UPDATE tickets SET last_seen=?", (now,)).rowcount
            self._event(now, None, "server_start", detail={"waiting_tickets": n})

    # ------------------------------------------------------------------ plumbing
    @contextmanager
    def _tx(self):
        # Waiters are woken only when an event (= a real state change) was written; a plain
        # heartbeat touch must not wake everybody, or long-polling waiters would wake each other forever.
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            self._dirty = False
            try:
                yield self.clock()
            except HwlockError:
                # A refused request (not holder, over max hold, ...): what was done before the refusal
                # (sweep evictions, the refusal's own event) is valid state, so keep it.
                self._commit()
                raise
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            else:
                self._commit()

    def _commit(self):
        self.db.execute("COMMIT")
        if self._dirty:
            self.changes += 1
            self.cond.notify_all()

    def _event(self, now, resource, kind, owner=None, ticket=None, detail=None):
        self._dirty = True
        self.db.execute(
            "INSERT INTO events(ts, resource, kind, owner, ticket, detail) VALUES (?,?,?,?,?,?)",
            (now, resource, kind, owner, ticket, json.dumps(detail) if detail is not None else None),
        )
        if self.log:
            extra = f" {json.dumps(detail)}" if detail else ""
            self.log(f"{kind} resource={resource} owner={owner} ticket={ticket}{extra}")

    def _lease(self, lease_s: Any) -> float:
        if lease_s is None:
            return float(self.default_lease_s)
        try:
            lease = float(lease_s)
        except (TypeError, ValueError):
            raise BadRequest(f"lease must be a number of seconds: {lease_s!r}") from None
        if not (lease >= 1):
            raise BadRequest("lease must be at least 1 s")
        if lease > self.max_lease_s:
            raise BadRequest(f"lease {lease:g} s exceeds the server maximum {self.max_lease_s:g} s",
                             max_lease_s=self.max_lease_s)
        return lease

    def _resource(self, now, name, create=True):
        row = self.db.execute("SELECT * FROM resources WHERE name=?", (name,)).fetchone()
        if row is None and create:
            self.db.execute("INSERT INTO resources(name, created) VALUES (?,?)", (name, now))
            self._event(now, name, "create")
            row = self.db.execute("SELECT * FROM resources WHERE name=?", (name,)).fetchone()
        return row

    def _sweep(self, now) -> int:
        """Evict expired holders and drop tickets that stopped heartbeating. Returns #changes."""
        changes = 0
        for r in self.db.execute(
            "SELECT * FROM resources WHERE holder_owner IS NOT NULL AND expires <= ?", (now,)
        ).fetchall():
            self.db.execute(
                "UPDATE resources SET holder_owner=NULL, holder_reason=NULL, holder_host=NULL,"
                " holder_pid=NULL, holder_ticket=NULL, since=NULL, expires=NULL, lease_s=NULL,"
                " holder_max_hold_s=NULL"
                " WHERE name=?", (r["name"],))
            self._event(now, r["name"], "expire", r["holder_owner"], r["holder_ticket"],
                        {"held_s": round(now - r["since"], 3), "overdue_s": round(now - r["expires"], 3)})
            changes += 1
        for t in self.db.execute(
            "SELECT * FROM tickets WHERE last_seen < ?", (now - self.heartbeat_s,)
        ).fetchall():
            self.db.execute("DELETE FROM tickets WHERE id=?", (t["id"],))
            self._event(now, t["resource"], "drop", t["owner"], t["id"],
                        {"silent_s": round(now - t["last_seen"], 3)})
            changes += 1
        # after dropping dead tickets: only live waiters can evict a holder
        for r in self.db.execute(
            "SELECT * FROM resources WHERE holder_owner IS NOT NULL"
            " AND EXISTS (SELECT 1 FROM tickets WHERE tickets.resource = resources.name)"
        ).fetchall():
            g = self._guard(r, now)
            if g and g["evict_at"] <= now:
                self._clear(r["name"])
                self._event(now, r["name"], "evict_max_hold", r["holder_owner"], r["holder_ticket"],
                            {"held_s": round(now - r["since"], 3), "max_hold_s": g["max_hold_s"],
                             "grace_s": g["grace_s"], "waiting": g["waiting"]})
                changes += 1
        return changes

    def sweep(self) -> int:
        with self._tx() as now:
            return self._sweep(now)

    def _holder(self, r, now) -> dict | None:
        if r is None or r["holder_owner"] is None:
            return None
        return {
            "owner": r["holder_owner"], "reason": r["holder_reason"], "host": r["holder_host"],
            "pid": r["holder_pid"], "ticket": r["holder_ticket"], "since": r["since"],
            "expires": r["expires"], "lease_s": r["lease_s"],
            "held_s": round(now - r["since"], 3), "lease_left_s": round(r["expires"] - now, 3),
            "guard": self._guard(r, now),
        }

    def limits(self, resource) -> tuple[float, float]:
        """(max_hold_s, grace_s) configured for a resource; max_hold_s 0 = no guard."""
        return (float(self.resource_max_hold_s.get(resource, self.max_hold_s)),
                float(self.resource_hold_grace_s.get(resource, self.hold_grace_s)))

    def _guard(self, r, now) -> dict | None:
        """The starvation guard for the current holder of `r`, or None when it has no limit.

        max_hold_s is the resource's limit, lowered by the holder's own `max_hold` from its acquire.
        While tickets wait: renew/re-acquire is refused from refuse_at = since + max_hold_s, and the
        holder is evicted at evict_at = max(refuse_at, oldest waiter's arrival) + grace_s (so a holder
        that was alone past its max hold still gets the grace period once somebody starts waiting).
        """
        limit, grace = self.limits(r["name"])
        own = r["holder_max_hold_s"]
        if own:
            limit = min(limit, own) if limit > 0 else own
        if limit <= 0:
            return None
        g = {"max_hold_s": limit, "grace_s": grace}
        q = self.db.execute("SELECT COUNT(*), MIN(created) FROM tickets WHERE resource=?",
                            (r["name"],)).fetchone()
        if q[0]:
            refuse_at = r["since"] + limit
            evict_at = max(refuse_at, q[1]) + grace
            g.update(waiting=q[0], refuse_at=refuse_at, evict_at=evict_at,
                     refuse_in_s=round(refuse_at - now, 3), evict_in_s=round(evict_at - now, 3))
        return g

    def _check_max_hold(self, r, now):
        """Refuse to extend a lock held longer than its max hold while others wait (logged)."""
        g = self._guard(r, now)
        if not g or "waiting" not in g or now - r["since"] <= g["max_hold_s"]:
            return
        held = round(now - r["since"], 3)
        self._event(now, r["name"], "renew_refused", r["holder_owner"], r["holder_ticket"],
                    {"held_s": held, "max_hold_s": g["max_hold_s"], "waiting": g["waiting"],
                     "evict_in_s": g["evict_in_s"]})
        raise OverMaxHold(
            f"{r['holder_owner']} has held {r['name']} for {held:.0f} s (max {g['max_hold_s']:g} s while"
            f" others wait) and {g['waiting']} wait: renew refused; release it", resource=r["name"],
            waiting=g["waiting"], held_s=held, max_hold_s=g["max_hold_s"], grace_s=g["grace_s"],
            evict_at=g["evict_at"], evict_in_s=g["evict_in_s"],
            lease_left_s=round(r["expires"] - now, 3))

    def _position(self, resource, ticket_id) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM tickets WHERE resource=? AND id<=?", (resource, ticket_id)
        ).fetchone()[0]

    # ------------------------------------------------------------------ operations
    def _max_hold(self, value: Any) -> float | None:
        """A holder's own max hold (acquire `max_hold`): seconds > 0; capped by the resource's limit."""
        if value is None:
            return None
        if isinstance(value, bool):
            raise BadRequest(f"max_hold must be a number of seconds: {value!r}")
        try:
            v = float(value)
        except (TypeError, ValueError):
            raise BadRequest(f"max_hold must be a number of seconds: {value!r}") from None
        if not (v >= 1) or v == float("inf"):
            raise BadRequest("max_hold must be at least 1 s (leave it out for the server's limit)")
        return v

    def acquire_step(self, resource, owner, *, reason=None, host=None, pid=None, lease_s=None,
                     ticket=None, max_hold_s=None, may_grant: Callable[[], bool] = lambda: True) -> dict:
        """One non-blocking acquire attempt. Creates a ticket when `ticket` is None.

        Returns {"state": "acquired", ...} or {"state": "waiting", "ticket", "position", "holder"};
        {"state": "abandoned"} when it was this ticket's turn but `may_grant()` said the client is gone.
        Raises TicketGone when `ticket` no longer exists (dropped/cancelled): rejoin with a new one.
        """
        _name(resource, "resource")
        _name(owner, "owner")
        lease = self._lease(lease_s)
        reason, host, pid = _text(reason), _text(host), _pid(pid)
        max_hold = self._max_hold(max_hold_s)
        if ticket is not None:
            try:
                ticket = int(ticket)
            except (TypeError, ValueError, OverflowError):
                raise BadRequest(f"ticket must be an integer: {ticket!r}") from None
        with self._tx() as now:
            self._sweep(now)
            r = self._resource(now, resource)
            if r["holder_owner"] == owner:
                # Idempotent re-acquire by the holder: keep holding, refresh the lease. Its `since` and
                # max hold stay as they were (a re-acquire cannot reset or raise the guard).
                self._check_max_hold(r, now)
                if ticket is not None and ticket != r["holder_ticket"]:
                    self.db.execute("DELETE FROM tickets WHERE id=? AND resource=? AND owner=?",
                                    (ticket, resource, owner))
                self.db.execute("UPDATE resources SET expires=?, lease_s=? WHERE name=?",
                                (now + lease, lease, resource))
                self._event(now, resource, "reacquire", owner, r["holder_ticket"], {"lease_s": lease})
                r = self._resource(now, resource)
                return {"state": "acquired", "resource": resource, "ticket": r["holder_ticket"],
                        "reacquired": True, "holder": self._holder(r, now)}
            if ticket is None:
                cur = self.db.execute(
                    "INSERT INTO tickets(resource, owner, reason, host, pid, created, last_seen)"
                    " VALUES (?,?,?,?,?,?,?)", (resource, owner, reason, host, pid, now, now))
                ticket = cur.lastrowid
                self._event(now, resource, "enqueue", owner, ticket, {"reason": reason, "host": host})
                t = self.db.execute("SELECT * FROM tickets WHERE id=?", (ticket,)).fetchone()
            else:
                t = self.db.execute("SELECT * FROM tickets WHERE id=? AND resource=?",
                                    (ticket, resource)).fetchone()
                if t is None or t["owner"] != owner:
                    raise TicketGone(f"ticket {ticket} is not waiting for {resource} (dropped, cancelled"
                                     " or granted to someone else); take a new one", ticket=ticket)
                self.db.execute("UPDATE tickets SET last_seen=? WHERE id=?", (now, ticket))
            first = self.db.execute(
                "SELECT MIN(id) FROM tickets WHERE resource=?", (resource,)).fetchone()[0]
            if r["holder_owner"] is None and first == ticket:
                if not may_grant():
                    # Its turn, but the client already hung up (killed, Ctrl-C): never hand the lock
                    # to a dead waiter; drop the ticket so the next one goes now, not after heartbeat_s.
                    self.db.execute("DELETE FROM tickets WHERE id=?", (ticket,))
                    self._event(now, resource, "abandon", owner, ticket)
                    return {"state": "abandoned", "resource": resource, "ticket": ticket}
                self.db.execute("DELETE FROM tickets WHERE id=?", (ticket,))
                self.db.execute(
                    "UPDATE resources SET holder_owner=?, holder_reason=?, holder_host=?, holder_pid=?,"
                    " holder_ticket=?, since=?, expires=?, lease_s=?, holder_max_hold_s=? WHERE name=?",
                    (owner, t["reason"], t["host"], t["pid"], ticket, now, now + lease, lease, max_hold,
                     resource))
                self._event(now, resource, "acquire", owner, ticket,
                            {"waited_s": round(now - t["created"], 3), "lease_s": lease,
                             "reason": t["reason"], "host": t["host"], "pid": t["pid"],
                             **({"max_hold_s": max_hold} if max_hold else {})})
                r = self._resource(now, resource)
                return {"state": "acquired", "resource": resource, "ticket": ticket,
                        "reacquired": False, "holder": self._holder(r, now)}
            return {"state": "waiting", "resource": resource, "ticket": ticket,
                    "position": self._position(resource, ticket),
                    "queue_length": self.db.execute(
                        "SELECT COUNT(*) FROM tickets WHERE resource=?", (resource,)).fetchone()[0],
                    "holder": self._holder(r, now)}

    def acquire(self, resource, owner, *, wait_s: float = 0, may_grant: Callable[[], bool] = lambda: True,
                **kw) -> dict:
        """Long-poll acquire: retries until acquired, until the queue position or holder changes
        (so the client can show progress), or until `wait_s` (monotonic) has passed.

        Each attempt refreshes the ticket's heartbeat (on every state change and at least every
        heartbeat_s/4). The client's connection is checked about once a second: when it is gone
        (killed, Ctrl-C) the ticket is dropped at once ("abandon"), so a dead waiter never blocks the
        queue until its heartbeat times out.
        """
        deadline = time.monotonic() + max(0.0, float(wait_s))
        touch_s = max(0.05, self.heartbeat_s / 4)

        def view(r):
            return r.get("position"), (r.get("holder") or {}).get("owner")

        with self.cond:
            res = self.acquire_step(resource, owner, may_grant=may_grant, **kw)
            kw["ticket"] = res.get("ticket")
            seen = view(res)
            last_step = time.monotonic()
            while res["state"] == "waiting" and view(res) == seen:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                seen_changes = self.changes
                self.cond.wait(min(left, 1.0, touch_s))
                if not may_grant():
                    return self._abandon(resource, owner, res["ticket"])
                if self.changes != seen_changes or time.monotonic() - last_step >= touch_s or time.monotonic() >= deadline:
                    res = self.acquire_step(resource, owner, may_grant=may_grant, **kw)
                    last_step = time.monotonic()
            if res["state"] == "waiting" and not may_grant():
                return self._abandon(resource, owner, res["ticket"])
            return res

    def _abandon(self, resource, owner, ticket) -> dict:
        """The waiting client hung up: withdraw its ticket now."""
        with self._tx() as now:
            if self.db.execute("DELETE FROM tickets WHERE id=? AND resource=?", (ticket, resource)).rowcount:
                self._event(now, resource, "abandon", owner, ticket)
        return {"state": "abandoned", "resource": resource, "ticket": ticket}

    def renew(self, resource, owner, *, lease_s=None) -> dict:
        _name(resource, "resource")
        _name(owner, "owner")
        lease = self._lease(lease_s)
        with self._tx() as now:
            self._sweep(now)
            r = self._resource(now, resource, create=False)
            if r is None or r["holder_owner"] != owner:
                raise NotHolder(f"{owner} does not hold {resource}",
                                holder=self._holder(r, now))
            self._check_max_hold(r, now)
            self.db.execute("UPDATE resources SET expires=?, lease_s=? WHERE name=?",
                            (now + lease, lease, resource))
            self._event(now, resource, "renew", owner, r["holder_ticket"], {"lease_s": lease})
            return {"state": "renewed", "resource": resource,
                    "holder": self._holder(self._resource(now, resource), now)}

    def _clear(self, resource):
        self.db.execute(
            "UPDATE resources SET holder_owner=NULL, holder_reason=NULL, holder_host=NULL,"
            " holder_pid=NULL, holder_ticket=NULL, since=NULL, expires=NULL, lease_s=NULL,"
                " holder_max_hold_s=NULL"
            " WHERE name=?", (resource,))

    def release(self, resource, owner) -> dict:
        _name(resource, "resource")
        _name(owner, "owner")
        with self._tx() as now:
            self._sweep(now)
            r = self._resource(now, resource, create=False)
            if r is None or r["holder_owner"] != owner:
                raise NotHolder(f"{owner} does not hold {resource}", holder=self._holder(r, now))
            self._clear(resource)
            self._event(now, resource, "release", owner, r["holder_ticket"],
                        {"held_s": round(now - r["since"], 3)})
            return {"state": "released", "resource": resource}

    def force_release(self, resource, by, reason=None) -> dict:
        _name(resource, "resource")
        _name(by, "by")
        with self._tx() as now:
            self._sweep(now)
            r = self._resource(now, resource, create=False)
            if r is None or r["holder_owner"] is None:
                raise NotHolder(f"{resource} is not held", holder=None)
            self._clear(resource)
            self._event(now, resource, "force_release", r["holder_owner"], r["holder_ticket"],
                        {"by": by, "reason": _text(reason), "held_s": round(now - r["since"], 3)})
            return {"state": "force_released", "resource": resource, "previous_holder": r["holder_owner"]}

    def cancel(self, resource, ticket, owner=None) -> dict:
        """Withdraw a waiting ticket. If that ticket was granted meanwhile, release the lock too
        (the client gave up, e.g. Ctrl-C, possibly racing with the grant)."""
        _name(resource, "resource")
        try:
            ticket = int(ticket)
        except (TypeError, ValueError, OverflowError):
            raise BadRequest(f"ticket must be an integer: {ticket!r}") from None
        with self._tx() as now:
            self._sweep(now)
            t = self.db.execute("SELECT * FROM tickets WHERE id=? AND resource=?",
                                (ticket, resource)).fetchone()
            if t is not None:
                self.db.execute("DELETE FROM tickets WHERE id=?", (ticket,))
                self._event(now, resource, "cancel", t["owner"], ticket)
                return {"state": "cancelled", "resource": resource, "ticket": ticket}
            r = self._resource(now, resource, create=False)
            if r is not None and r["holder_ticket"] == ticket and (owner is None or owner == r["holder_owner"]):
                self._clear(resource)
                self._event(now, resource, "release", r["holder_owner"], ticket,
                            {"held_s": round(now - r["since"], 3), "via": "cancel"})
                return {"state": "released", "resource": resource, "ticket": ticket}
            return {"state": "unknown", "resource": resource, "ticket": ticket}

    # ------------------------------------------------------------------ queries
    def _resource_status(self, r, now) -> dict:
        queue = []
        for i, t in enumerate(self.db.execute(
                "SELECT * FROM tickets WHERE resource=? ORDER BY id", (r["name"],)).fetchall(), 1):
            queue.append({"position": i, "ticket": t["id"], "owner": t["owner"], "reason": t["reason"],
                          "host": t["host"], "pid": t["pid"], "waiting_s": round(now - t["created"], 3),
                          "last_seen_s": round(now - t["last_seen"], 3)})
        return {"resource": r["name"], "free": r["holder_owner"] is None,
                "holder": self._holder(r, now), "queue": queue}

    def status(self, resource: str | None = None) -> dict:
        with self.lock:
            self.sweep()
            now = self.clock()
            if resource is not None:
                _name(resource, "resource")
                r = self.db.execute("SELECT * FROM resources WHERE name=?", (resource,)).fetchone()
                if r is None:
                    return {"now": now, "resource": resource, "free": True, "holder": None, "queue": []}
                return {"now": now, **self._resource_status(r, now)}
            rows = self.db.execute("SELECT * FROM resources ORDER BY name").fetchall()
            return {"now": now, "resources": [self._resource_status(r, now) for r in rows]}

    def events(self, resource: str | None = None, after: int | None = None, limit: int = 100) -> dict:
        limit = max(1, min(int(limit), 1000))
        where, args = [], []
        if resource is not None:
            where.append("resource=?")
            args.append(_name(resource, "resource"))
        if after is not None:
            where.append("id>?")
            args.append(int(after))
        sql = "SELECT * FROM events" + (" WHERE " + " AND ".join(where) if where else "")
        with self.lock:
            if after is not None:
                rows = self.db.execute(sql + " ORDER BY id LIMIT ?", (*args, limit)).fetchall()
            else:
                rows = self.db.execute(sql + " ORDER BY id DESC LIMIT ?", (*args, limit)).fetchall()[::-1]
        return {"events": [{"id": e["id"], "ts": e["ts"], "resource": e["resource"], "kind": e["kind"],
                            "owner": e["owner"], "ticket": e["ticket"],
                            "detail": json.loads(e["detail"]) if e["detail"] else None} for e in rows]}

    def close(self):
        with self.lock:
            self.db.close()

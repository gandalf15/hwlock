"""HTTP API for hwlock (stdlib only: ThreadingHTTPServer, one thread per request).

Routes (JSON in/out; all except /health need `Authorization: Bearer $HWLOCK_TOKEN` when a token is set):
  GET  /health                                 liveness (never needs a token)
  GET  /                                       auto-refreshing HTML status page (token via ?token=...)
  GET  /status[?resource=R]  or /status/R      holders, leases, queues
  GET  /events[?resource=R&after=ID&limit=N]   audit log
  POST /r/<resource>/acquire        {owner, reason?, host?, pid?, lease?, max_hold?, ticket?, new_ticket?,
                                     attach?, wait?}
         200 acquired | 202 waiting (long-polls up to `wait` s, capped by the server) | 410 ticket gone
         (without `ticket`, re-attaches to the owner's dormant or waiting ticket: "reattached": true)
  POST /r/<resource>/renew          {owner, lease?}           200 | 409 not_holder / over_max_hold
  POST /r/<resource>/release        {owner}                   200 | 409 not holder
  POST /r/<resource>/cancel         {ticket, owner?, attach?} 200 (also releases if that ticket was granted)
  POST /r/<resource>/detach         {ticket?, owner?, attach?} 200 ticket dormant (keeps its place for the
                                                              re-attach window; releases if it was granted)
  `attach`: an id each client process picks once; when given, a cancel/detach acts only if it comes from
  the ticket's (or grant's) latest client, else 200 {"state": "superseded"} (a re-run replaced it).
  POST /r/<resource>/force-release  {by, reason?}             200 | 409 not held  (admin; logged)
"""

from __future__ import annotations

import hmac
import html
import json
import os
import select
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .core import NAME_RE, BadRequest, HwlockError, Store


class Config:
    def __init__(self, **kw):
        env = os.environ.get
        self.host = kw.get("host") or env("HWLOCK_HOST", "127.0.0.1")
        self.port = int(kw.get("port") or env("HWLOCK_PORT", "8770"))
        self.db = kw.get("db") or env("HWLOCK_DB", "hwlock.db")
        self.token = kw.get("token") if "token" in kw else env("HWLOCK_TOKEN") or None
        self.admin_token = kw.get("admin_token") if "admin_token" in kw else env("HWLOCK_ADMIN_TOKEN") or None
        self.default_lease_s = float(kw.get("default_lease_s") or env("HWLOCK_DEFAULT_LEASE_S", "1800"))
        self.max_lease_s = float(kw.get("max_lease_s") or env("HWLOCK_MAX_LEASE_S", "86400"))
        self.heartbeat_s = float(kw.get("heartbeat_s") or env("HWLOCK_HEARTBEAT_S", "120"))
        self.poll_s = float(kw.get("poll_s") or env("HWLOCK_POLL_S", "25"))
        # starvation guard; per-resource values as "jetson=7200,rig=0" override the global ones
        self.max_hold_s = float(kw["max_hold_s"] if "max_hold_s" in kw else env("HWLOCK_MAX_HOLD_S") or "7200")
        self.hold_grace_s = float(kw["hold_grace_s"] if "hold_grace_s" in kw
                                  else env("HWLOCK_HOLD_GRACE_S") or "300")
        self.resource_max_hold_s = _per_resource(kw.get("resource_max_hold_s")
                                                 or env("HWLOCK_RESOURCE_MAX_HOLD_S", ""), "HWLOCK_RESOURCE_MAX_HOLD_S")
        self.resource_hold_grace_s = _per_resource(kw.get("resource_hold_grace_s")
                                                   or env("HWLOCK_RESOURCE_HOLD_GRACE_S", ""),
                                                   "HWLOCK_RESOURCE_HOLD_GRACE_S")
        if min([self.max_hold_s, self.hold_grace_s, *self.resource_max_hold_s.values(),
                *self.resource_hold_grace_s.values()]) < 0:
            raise ValueError("max hold and grace must be >= 0 (max hold 0 = no starvation guard)")
        self.sweep_s = float(kw.get("sweep_s") or env("HWLOCK_SWEEP_S", "1"))
        # how long a waiter that hung up (killed, tool timeout, SIGTERM) keeps its place; 0 = drop at once
        self.reattach_s = float(kw["reattach_s"] if "reattach_s" in kw else env("HWLOCK_REATTACH_S") or "120")
        if not self.reattach_s >= 0:
            raise ValueError("HWLOCK_REATTACH_S must be >= 0 (0 = drop a hung-up waiter's ticket at once)")
        if self.heartbeat_s < self.poll_s + 5:
            raise ValueError("HWLOCK_HEARTBEAT_S must be at least HWLOCK_POLL_S + 5 so a long-polling"
                             " waiter is never dropped")


def _per_resource(spec, what: str) -> dict:
    """"jetson=7200, rig=0" -> {"jetson": 7200.0, "rig": 0.0} (a dict passes through)."""
    if isinstance(spec, dict):
        return {k: float(v) for k, v in spec.items()}
    out = {}
    for item in filter(None, (x.strip() for x in str(spec).split(","))):
        name, sep, val = item.partition("=")
        try:
            if not sep or not NAME_RE.match(name.strip()):
                raise ValueError
            out[name.strip()] = float(val)
        except ValueError:
            raise ValueError(f"{what}: expected resource=seconds[,resource=seconds...], got {item!r}") from None
    return out


def _peer_alive(sock: socket.socket) -> bool:
    """False when the client already closed its side of the connection (process died, Ctrl-C)."""
    try:
        p = select.poll()  # not select.select: that fails for fds >= 1024 (many open connections)
        p.register(sock, select.POLLIN | select.POLLHUP | select.POLLERR)
        ev = p.poll(0)
        if not ev:
            return True
        if ev[0][1] & select.POLLIN:
            return bool(sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT))
        return False
    except BlockingIOError:
        return True
    except (OSError, ValueError):
        return False


def _wait(value, cap: float) -> float:
    """Long-poll wait in seconds from the request: a number (or numeric string), 0..cap; NaN -> 0."""
    if value is None:
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise BadRequest(f"wait must be a number of seconds: {value!r}")
    try:
        w = float(value)
    except ValueError:
        raise BadRequest(f"wait must be a number of seconds: {value!r}") from None
    return 0.0 if w != w else max(0.0, min(w, cap))


def _fmt_s(s) -> str:
    if s is None:
        return ""
    s = int(s)
    if abs(s) >= 3600:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    if abs(s) >= 60:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s}s"


def _guard_html(g) -> str:
    """Max-hold line for the holder: the limit, and the deadlines while people wait."""
    if not g:
        return ""
    if "evict_in_s" not in g:
        return f" · max hold {_fmt_s(g['max_hold_s'])} once others wait"
    refuse = ("renew refused now" if g["refuse_in_s"] <= 0 else f"renew refused in {_fmt_s(g['refuse_in_s'])}")
    cls = " class=warn" if g["refuse_in_s"] <= 0 else ""
    return (f"<br><span{cls}>max hold {_fmt_s(g['max_hold_s'])}: {refuse} · evicted in"
            f" {_fmt_s(max(0, g['evict_in_s']))}"
            f" ({time.strftime('%H:%M:%S', time.localtime(g['evict_at']))})</span>")


def render_html(status: dict, token_qs: str) -> str:
    e = html.escape
    rows = []
    for r in status["resources"]:
        h = r["holder"]
        holder = ("<span class=free>free</span>" if h is None else
                  f"<b>{e(h['owner'])}</b> {e(h['reason'] or '')}<br><small>{e(h['host'] or '')}"
                  f" pid {e(str(h['pid'] or '-'))} · held {_fmt_s(h['held_s'])} ·"
                  f" lease left {_fmt_s(h['lease_left_s'])}{_guard_html(h.get('guard'))}</small>")
        queue = "".join(
            f"<li>{q['position']}. <b>{e(q['owner'])}</b> {e(q['reason'] or '')}"
            f" <small>waiting {_fmt_s(q['waiting_s'])} · ticket {q['ticket']}</small></li>"
            for q in r["queue"]) or ("" if r.get("dormant") else "<li class=muted>empty</li>")
        queue += "".join(
            f"<li class=muted>dormant: <b>{e(d['owner'])}</b> {e(d['reason'] or '')} <small>ticket"
            f" {d['ticket']} · re-attach within {_fmt_s(max(0, d['dormant_left_s']))} to get place"
            f" {d['position_on_reattach']} back</small></li>" for d in r.get("dormant", []))
        rows.append(f"<tr><td><b>{e(r['resource'])}</b></td><td>{holder}</td><td><ol>{queue}</ol></td></tr>")
    body = "".join(rows) or "<tr><td colspan=3 class=muted>no resources yet</td></tr>"
    return f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width, initial-scale=1"><meta http-equiv=refresh content=3>
<title>hwlock</title><style>
:root{{--bg:#fff;--fg:#1d1d1f;--muted:#888;--line:#ddd;--ok:#1a7f37;--warn:#b35900}}
@media (prefers-color-scheme: dark){{:root{{--bg:#141414;--fg:#eee;--muted:#999;--line:#333;--ok:#4ac26b;--warn:#f0a050}}}}
body{{background:var(--bg);color:var(--fg);font:15px/1.4 system-ui,sans-serif;margin:0;padding:16px}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid var(--line);padding:8px;
text-align:left;vertical-align:top}}ol{{margin:0;padding:0;list-style:none}}.free{{color:var(--ok)}}.warn{{color:var(--warn)}}
.muted,small{{color:var(--muted)}}a{{color:inherit}}</style></head><body>
<h1>hwlock</h1><table><tr><th>resource</th><th>holder</th><th>queue</th></tr>{body}</table>
<p class=muted>server time {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(status['now']))} ·
refreshes every 3 s · <a href="/events{token_qs}">events</a> · <a href="/status{token_qs}">json</a></p>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    server_version = f"hwlock/{__version__}"
    protocol_version = "HTTP/1.1"

    # quieter access log; state changes are logged as events
    def log_message(self, fmt, *args):
        if os.environ.get("HWLOCK_ACCESS_LOG"):
            super().log_message(fmt, *args)

    @property
    def app(self) -> "App":
        return self.server.app  # type: ignore[attr-defined]

    def _send(self, status: int, payload, content_type="application/json"):
        body = (payload if isinstance(payload, str) else json.dumps(payload)).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    def _authorized(self, query: dict, admin=False) -> bool:
        need = self.app.cfg.admin_token if admin and self.app.cfg.admin_token else self.app.cfg.token
        if not need:
            return True
        got = self.headers.get("Authorization", "")
        got = got[7:] if got.startswith("Bearer ") else (query.get("token") or [""])[0]
        return hmac.compare_digest(got.encode(), need.encode())

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if n < 0:
            raise BadRequest("bad Content-Length")
        if n > 65536:
            raise BadRequest("body too large")
        raw = self.rfile.read(n) if n else b""
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except ValueError:
            raise BadRequest("body must be JSON") from None
        if not isinstance(data, dict):
            raise BadRequest("body must be a JSON object")
        return data

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method: str):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        parts = [unquote(p) for p in url.path.split("/") if p]
        try:
            if method == "GET" and parts == ["health"]:
                return self._send(200, {"ok": True, "version": __version__})
            if not self._authorized(query, admin=parts[-1:] == ["force-release"]):
                return self._send(401, {"error": "unauthorized", "message": "missing or wrong token"})
            store = self.app.store
            q = lambda k: (query.get(k) or [None])[0]  # noqa: E731
            if method == "GET" and not parts:
                tq = f"?token={q('token')}" if q("token") else ""
                return self._send(200, render_html(store.status(), html.escape(tq)), "text/html")
            if method == "GET" and parts == ["status"]:
                return self._send(200, store.status(q("resource")))
            if method == "GET" and len(parts) == 2 and parts[0] == "status":
                return self._send(200, store.status(parts[1]))
            if method == "GET" and parts == ["events"]:
                after = q("after")
                return self._send(200, store.events(q("resource"), int(after) if after else None,
                                                    int(q("limit") or 100)))
            if method == "POST" and len(parts) == 3 and parts[0] == "r":
                res, op, b = parts[1], parts[2], self._body()
                if op == "acquire":
                    wait = _wait(b.get("wait"), self.app.cfg.poll_s)
                    sock = self.connection
                    out = store.acquire(res, b.get("owner"), reason=b.get("reason"), host=b.get("host"),
                                        pid=b.get("pid"), lease_s=b.get("lease"), ticket=b.get("ticket"),
                                        max_hold_s=b.get("max_hold"), new_ticket=b.get("new_ticket", False),
                                        attach=b.get("attach"), wait_s=wait, may_grant=lambda: _peer_alive(sock))
                    if out["state"] == "abandoned":
                        self.close_connection = True
                        return None
                    return self._send(200 if out["state"] == "acquired" else 202, out)
                if op == "renew":
                    return self._send(200, store.renew(res, b.get("owner"), lease_s=b.get("lease")))
                if op == "release":
                    return self._send(200, store.release(res, b.get("owner")))
                if op == "cancel":
                    return self._send(200, store.cancel(res, b.get("ticket"), b.get("owner"), b.get("attach")))
                if op == "detach":
                    return self._send(200, store.detach(res, b.get("ticket"), b.get("owner"), b.get("attach")))
                if op == "force-release":
                    return self._send(200, store.force_release(res, b.get("by"), b.get("reason")))
            return self._send(404, {"error": "not_found", "message": f"no route {method} {url.path}"})
        except HwlockError as err:
            return self._send(err.status, err.payload())
        except (ValueError, TypeError, OverflowError) as err:
            return self._send(400, {"error": "bad_request", "message": str(err)})
        except Exception as err:  # pragma: no cover - last resort, keep the server up
            print(f"hwlock: internal error: {err!r}", file=sys.stderr, flush=True)
            return self._send(500, {"error": "internal", "message": str(err)})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128


class App:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        os.makedirs(os.path.dirname(os.path.abspath(cfg.db)), exist_ok=True)
        self.store = Store(cfg.db, default_lease_s=cfg.default_lease_s, max_lease_s=cfg.max_lease_s,
                           heartbeat_s=cfg.heartbeat_s, max_hold_s=cfg.max_hold_s,
                           hold_grace_s=cfg.hold_grace_s, resource_max_hold_s=cfg.resource_max_hold_s,
                           resource_hold_grace_s=cfg.resource_hold_grace_s, reattach_s=cfg.reattach_s,
                           log=lambda m: print(f"hwlock: {m}", flush=True))
        self.httpd = Server((cfg.host, cfg.port), Handler)
        self.httpd.app = self  # type: ignore[attr-defined]
        self._stop = threading.Event()

    def _sweeper(self):
        while not self._stop.wait(self.cfg.sweep_s):
            try:
                self.store.sweep()
            except Exception as err:  # pragma: no cover
                print(f"hwlock: sweep failed: {err!r}", file=sys.stderr, flush=True)

    def serve_forever(self):
        threading.Thread(target=self._sweeper, name="sweeper", daemon=True).start()
        print(f"hwlock {__version__} listening on http://{self.cfg.host}:{self.httpd.server_address[1]}"
              f" db={self.cfg.db} auth={'token' if self.cfg.token else 'none'}"
              f" max_hold={self.cfg.max_hold_s:g}s grace={self.cfg.hold_grace_s:g}s"
              f" reattach={self.cfg.reattach_s:g}s"
              + "".join(f" {k}:max_hold={v:g}s" for k, v in self.cfg.resource_max_hold_s.items())
              + "".join(f" {k}:grace={v:g}s" for k, v in self.cfg.resource_hold_grace_s.items()), flush=True)
        try:
            self.httpd.serve_forever()
        finally:
            self._stop.set()

    def shutdown(self):
        self._stop.set()
        self.httpd.shutdown()

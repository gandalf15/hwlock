"""Hardware health: probes per resource, a hysteresis state machine, and the scheduler threads.

Config (JSON; JSON is also valid YAML, so a `resources.yaml` written as JSON works too):

    {"resources": {
      "jetson": {
        "interval_s": 5, "down_after": 3, "up_after": 2, "degraded_after": 2,
        "probes": [
          {"type": "ssh_banner", "host": "192.168.0.99", "port": 22, "timeout_s": 3},
          {"type": "http_json", "url": "http://192.168.0.99:8771/", "timeout_s": 3,
           "degraded_if": {"mem_available_mb": "<800", "temp_max_c": ">85"}}
        ]}}}

Probe types: ssh_banner (TCP connect AND an "SSH-" banner within the timeout: a frozen box accepts TCP
in the kernel but sends nothing), tcp (connect only), http_json (GET, JSON object: metrics, boot_id,
thresholds), command (runs in the server's container; exit 0 = ok).

Each round runs all probes of a resource in parallel and gives one outcome:
  down      if a probe with on_fail "down" failed (default for ssh_banner, tcp, command),
  degraded  if a probe with on_fail "degraded" failed (default for http_json: the box answers ssh but
            its exporter does not) or a `degraded_if` threshold is crossed,
  up        otherwise.
The state (up / degraded / down / unknown) changes only after several rounds in a row (hysteresis):
down after `down_after` down rounds, back from down (or unknown) after `up_after` rounds that are not
down, up -> degraded after `degraded_after` degraded rounds, degraded -> up after `up_after` clean rounds.
A changed boot_id from an http_json probe is a reboot (handled at once, no hysteresis).

All I/O runs outside the Store lock; the Store only records transitions (see Store.health_report).
"""

from __future__ import annotations

import concurrent.futures
import json
import re
import socket
import subprocess
import threading
import time
from typing import Any
from urllib.parse import urlsplit

from .core import NAME_RE

STATES = ("up", "degraded", "down", "unknown")
PROBE_TYPES = ("ssh_banner", "tcp", "http_json", "command")
_THRESHOLD_RE = re.compile(r"^\s*(<=|>=|<|>)\s*(-?[0-9.]+(?:e-?\d+)?)\s*$")


class ConfigError(ValueError):
    pass


# ---------------------------------------------------------------------------- probes
class ProbeResult:
    __slots__ = ("ok", "error", "data", "ms", "breaches")

    def __init__(self, ok: bool, error: str | None = None, data: dict | None = None, ms: float = 0.0):
        self.ok, self.error, self.data, self.ms = ok, error, data, ms
        self.breaches: list[str] = []


def _deadline_left(deadline: float) -> float:
    left = deadline - time.monotonic()
    if left <= 0:
        raise socket.timeout("timed out")
    return left


def _connect(host: str, port: int, deadline: float) -> socket.socket:
    return socket.create_connection((host, port), timeout=_deadline_left(deadline))


def ssh_banner(host: str, port: int, timeout_s: float) -> ProbeResult:
    """TCP connect, then read the server's identification line ("SSH-2.0-...") within one total deadline.
    A box frozen by an out-of-memory condition accepts the TCP connection (the kernel does that) but
    sshd never answers: that is a failure here, unlike a plain TCP check."""
    deadline = time.monotonic() + timeout_s
    try:
        s = _connect(host, port, deadline)
    except OSError as err:
        return ProbeResult(False, f"connect failed: {err}")
    got = b""
    try:
        with s:
            while b"\n" not in got and len(got) < 512:
                s.settimeout(_deadline_left(deadline))
                chunk = s.recv(256)
                if not chunk:
                    break
                got += chunk
    except OSError:
        pass
    if got.startswith(b"SSH-"):
        return ProbeResult(True, data={"banner": got.split(b"\n")[0].strip().decode("ascii", "replace")[:100]})
    if not got:
        return ProbeResult(False, f"TCP accepted but no SSH banner within {timeout_s:g} s (frozen?)")
    return ProbeResult(False, f"not an SSH banner: {got[:40]!r}")


def tcp(host: str, port: int, timeout_s: float) -> ProbeResult:
    try:
        socket.create_connection((host, port), timeout=timeout_s).close()
        return ProbeResult(True)
    except OSError as err:
        return ProbeResult(False, f"connect failed: {err}")


def http_get_json(url: str, timeout_s: float) -> ProbeResult:
    """GET url (plain HTTP/1.0, no redirects) with ONE total deadline for connect + response: urllib's
    timeout is per socket operation, so a half-frozen box that trickles bytes could stall it."""
    deadline = time.monotonic() + timeout_s
    u = urlsplit(url)
    if u.scheme != "http" or not u.hostname:
        return ProbeResult(False, f"only http:// URLs are supported: {url}")
    try:
        s = _connect(u.hostname, u.port or 80, deadline)
    except OSError as err:
        return ProbeResult(False, f"connect failed: {err}")
    buf = b""
    try:
        with s:
            path = (u.path or "/") + (f"?{u.query}" if u.query else "")
            s.settimeout(_deadline_left(deadline))
            s.sendall(f"GET {path} HTTP/1.0\r\nHost: {u.netloc}\r\nAccept: application/json\r\n"
                      f"Connection: close\r\n\r\n".encode())
            while len(buf) < 1_000_000:
                s.settimeout(_deadline_left(deadline))
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
    except OSError as err:
        return ProbeResult(False, f"no complete answer within {timeout_s:g} s ({err})")
    head, sep, body = buf.partition(b"\r\n\r\n")
    if not sep:
        return ProbeResult(False, "incomplete HTTP response")
    status_line = head.split(b"\r\n", 1)[0].decode("latin-1")
    parts = status_line.split()
    if len(parts) < 2 or parts[1] != "200":
        return ProbeResult(False, f"HTTP {status_line[:60]}")
    try:
        data = json.loads(body)
    except ValueError:
        return ProbeResult(False, "answer is not JSON")
    if not isinstance(data, dict):
        return ProbeResult(False, "answer is not a JSON object")
    return ProbeResult(True, data=data)


def run_command(argv, timeout_s: float) -> ProbeResult:
    try:
        p = subprocess.run(argv, shell=isinstance(argv, str), capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return ProbeResult(False, f"command timed out after {timeout_s:g} s")
    except OSError as err:
        return ProbeResult(False, f"command failed to start: {err}")
    if p.returncode != 0:
        tail = (p.stderr or p.stdout or "").strip().splitlines()[-1:] or [""]
        return ProbeResult(False, f"exit {p.returncode}: {tail[0][:120]}")
    return ProbeResult(True)


def _lookup(data: dict, key: str):
    cur: Any = data
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur if isinstance(cur, (int, float)) and not isinstance(cur, bool) else None


class Probe:
    def __init__(self, spec: dict, where: str):
        if not isinstance(spec, dict):
            raise ConfigError(f"{where}: a probe must be an object")
        self.type = spec.get("type")
        if self.type not in PROBE_TYPES:
            raise ConfigError(f"{where}: type must be one of {', '.join(PROBE_TYPES)}, got {self.type!r}")
        self.timeout_s = float(spec.get("timeout_s", 3))
        if not 0 < self.timeout_s <= 60:
            raise ConfigError(f"{where}: timeout_s must be in (0, 60]")
        default_fail = "degraded" if self.type == "http_json" else "down"
        self.on_fail = spec.get("on_fail", default_fail)
        if self.on_fail not in ("down", "degraded"):
            raise ConfigError(f"{where}: on_fail must be down or degraded")
        self.thresholds: list[tuple[str, str, float]] = []
        if self.type in ("ssh_banner", "tcp"):
            self.host = spec.get("host")
            if not isinstance(self.host, str) or not self.host:
                raise ConfigError(f"{where}: host is required")
            self.port = int(spec.get("port", 22 if self.type == "ssh_banner" else 0))
            if not 0 < self.port < 65536:
                raise ConfigError(f"{where}: port is required (1..65535)")
            self.name = f"{self.type} {self.host}:{self.port}"
        elif self.type == "http_json":
            self.url = spec.get("url")
            if not isinstance(self.url, str) or not self.url.startswith("http://"):
                raise ConfigError(f"{where}: url must be an http:// URL")
            self.boot_id_key = spec.get("boot_id_key", "boot_id")
            self.uptime_key = spec.get("uptime_key", "uptime_s")
            limits = spec.get("degraded_if") or {}
            if not isinstance(limits, dict):
                raise ConfigError(f"{where}: degraded_if must be an object like {{\"mem_available_mb\": \"<800\"}}")
            for key, rule in limits.items():
                m = _THRESHOLD_RE.match(str(rule))
                if not m:
                    raise ConfigError(f"{where}: degraded_if.{key}: expected e.g. \"<800\" or \">85\", got {rule!r}")
                self.thresholds.append((key, m.group(1), float(m.group(2))))
            self.name = f"http_json {self.url}"
        else:
            self.argv = spec.get("argv") or spec.get("cmd")
            if not (isinstance(self.argv, str) or (isinstance(self.argv, list) and self.argv
                                                     and all(isinstance(a, str) for a in self.argv))):
                raise ConfigError(f"{where}: command needs argv (a list of strings) or cmd (a shell string)")
            self.name = spec.get("name") or f"command {self.argv if isinstance(self.argv, str) else ' '.join(self.argv)}"[:80]

    def run(self) -> ProbeResult:
        t0 = time.monotonic()
        if self.type == "ssh_banner":
            res = ssh_banner(self.host, self.port, self.timeout_s)
        elif self.type == "tcp":
            res = tcp(self.host, self.port, self.timeout_s)
        elif self.type == "http_json":
            res = http_get_json(self.url, self.timeout_s)
            if res.ok:
                for key, op, limit in self.thresholds:
                    v = _lookup(res.data, key)
                    if v is None:
                        continue
                    if ((op == "<" and v < limit) or (op == "<=" and v <= limit) or (op == ">" and v > limit)
                            or (op == ">=" and v >= limit)):
                        res.breaches.append(f"{key} {v:g} {op} {limit:g}")
        else:
            res = run_command(self.argv, self.timeout_s)
        res.ms = round((time.monotonic() - t0) * 1000, 1)
        return res


class ResourceConfig:
    def __init__(self, name: str, spec: dict):
        if not NAME_RE.match(name):
            raise ConfigError(f"resource name {name!r} must match [A-Za-z0-9._-]{{1,64}}")
        if not isinstance(spec, dict):
            raise ConfigError(f"{name}: must be an object")
        self.name = name
        self.interval_s = float(spec.get("interval_s", 5))
        self.down_after = int(spec.get("down_after", 3))
        self.up_after = int(spec.get("up_after", 2))
        self.degraded_after = int(spec.get("degraded_after", 2))
        if self.interval_s < 0.05 or min(self.down_after, self.up_after, self.degraded_after) < 1:
            raise ConfigError(f"{name}: interval_s >= 0.05 and down_after/up_after/degraded_after >= 1")
        probes = spec.get("probes")
        if not isinstance(probes, list) or not probes:
            raise ConfigError(f"{name}: probes must be a non-empty list")
        self.probes = [Probe(p, f"{name}.probes[{i}]") for i, p in enumerate(probes)]


def load_config(text: str) -> dict[str, ResourceConfig]:
    """Parse the health config (JSON). {} for an empty file."""
    if not text.strip():
        return {}
    try:
        data = json.loads(text)
    except ValueError as err:
        raise ConfigError(f"health config is not valid JSON: {err}") from None
    if not isinstance(data, dict) or not isinstance(data.get("resources", {}), dict):
        raise ConfigError('health config must be {"resources": {"<name>": {...}}}')
    return {name: ResourceConfig(name, spec) for name, spec in data.get("resources", {}).items()}


def load_config_file(path: str) -> dict[str, ResourceConfig]:
    with open(path, encoding="utf-8") as f:
        return load_config(f.read())


# ---------------------------------------------------------------------------- state machine
class Tracker:
    """Hysteresis over round outcomes ("up", "degraded", "down"). observe() returns the new state on a
    transition, else None. Starts from a persisted state so a server restart changes nothing by itself."""

    def __init__(self, state: str, down_after: int = 3, up_after: int = 2, degraded_after: int = 2):
        self.state = state if state in STATES else "unknown"
        self.down_after, self.up_after, self.degraded_after = down_after, up_after, degraded_after
        self.bad = self.good = self.deg = self.clean = 0

    def observe(self, outcome: str) -> str | None:
        if outcome == "down":
            self.bad += 1
            self.good = self.deg = self.clean = 0
        else:
            self.bad = 0
            self.good += 1
            if outcome == "degraded":
                self.deg += 1
                self.clean = 0
            else:
                self.clean += 1
                self.deg = 0
        new = None
        if self.state != "down" and self.bad >= self.down_after:
            new = "down"
        elif self.state in ("down", "unknown") and self.good >= self.up_after:
            new = outcome  # back: at the level of the latest round
        elif self.state == "up" and self.deg >= self.degraded_after:
            new = "degraded"
        elif self.state == "degraded" and self.clean >= self.up_after:
            new = "up"
        if new is not None and new != self.state:
            self.state = new
            return new
        return None


def evaluate(probes: list[Probe], results: list[ProbeResult]) -> tuple[str, str]:
    """(outcome, reason) of one round."""
    down, degraded = [], []
    for p, r in zip(probes, results):
        if not r.ok:
            (down if p.on_fail == "down" else degraded).append(f"{p.name}: {r.error}")
        elif r.breaches:
            degraded.append(", ".join(r.breaches))
    if down:
        return "down", "; ".join(down + degraded)
    if degraded:
        return "degraded", "; ".join(degraded)
    return "up", ""


# ---------------------------------------------------------------------------- scheduler
class Monitor:
    """One thread per configured resource: probe every interval_s, feed the Tracker, report to the Store."""

    def __init__(self, store, configs: dict[str, ResourceConfig], log=None):
        self.store, self.configs, self.log = store, configs, log
        self._stop = threading.Event()
        self.threads: list[threading.Thread] = []
        store.health_register(list(configs))

    def start(self):
        for cfg in self.configs.values():
            t = threading.Thread(target=self._loop, args=(cfg,), name=f"health-{cfg.name}", daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self._stop.set()

    def _round(self, cfg: ResourceConfig, pool) -> tuple[str, str, list[dict], dict | None]:
        futures = [pool.submit(p.run) for p in cfg.probes]
        results = []
        for p, f in zip(cfg.probes, futures):
            try:
                results.append(f.result(timeout=p.timeout_s + 5))
            except Exception as err:  # pragma: no cover - a probe must never kill the loop
                results.append(ProbeResult(False, f"probe error: {err!r}"))
        outcome, reason = evaluate(cfg.probes, results)
        probes = [{"probe": p.name, "ok": r.ok, "ms": r.ms, **({"error": r.error} if r.error else {}),
                   **({"breaches": r.breaches} if r.breaches else {})} for p, r in zip(cfg.probes, results)]
        metrics, boot = None, None
        for p, r in zip(cfg.probes, results):
            if p.type == "http_json" and r.ok:
                metrics = r.data
                bid = r.data.get(p.boot_id_key)
                up = _lookup(r.data, p.uptime_key)
                if isinstance(bid, str) and bid:
                    boot = {"boot_id": bid[:100], "uptime_s": up}
                break
        return outcome, reason, probes, {"metrics": metrics, "boot": boot}

    def _loop(self, cfg: ResourceConfig):
        tracker = Tracker(self.store.health_state(cfg.name), cfg.down_after, cfg.up_after, cfg.degraded_after)
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(cfg.probes),
                                                   thread_name_prefix=f"probe-{cfg.name}") as pool:
            while not self._stop.is_set():
                t0 = time.monotonic()
                try:
                    outcome, reason, probes, extra = self._round(cfg, pool)
                    new = tracker.observe(outcome)
                    self.store.health_report(cfg.name, outcome=outcome, new_state=new, reason=reason,
                                             probes=probes, metrics=extra["metrics"], boot=extra["boot"],
                                             hysteresis={"down_after": cfg.down_after, "up_after": cfg.up_after,
                                                         "degraded_after": cfg.degraded_after,
                                                         "interval_s": cfg.interval_s})
                except Exception as err:  # pragma: no cover
                    if self.log:
                        self.log(f"health round for {cfg.name} failed: {err!r}")
                self._stop.wait(max(0.05, cfg.interval_s - (time.monotonic() - t0)))

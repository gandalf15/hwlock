import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "client" / "hwlock"
# The client must also run on old Pythons (e.g. a Jetson); CI runs the CLI tests with python3.8 here.
CLIENT_PYTHON = os.environ.get("HWLOCK_CLIENT_PYTHON", sys.executable)
sys.path.insert(0, str(ROOT))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Api:
    """Minimal HTTP helper for tests: returns (status, json)."""

    def __init__(self, url, token=None):
        self.url, self.token = url, token

    def req(self, method, path, body=None, token=..., timeout=60):
        token = self.token if token is ... else token
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.url + path, data=data, method=method)
        r.add_header("Content-Type", "application/json")
        if token:
            r.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                raw = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, (json.loads(raw) if "json" in ctype else raw.decode())
        except urllib.error.HTTPError as e:
            raw = e.read()
            return e.code, (json.loads(raw) if raw[:1] == b"{" else raw.decode())

    def get(self, path, **kw):
        return self.req("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.req("POST", path, body or {}, **kw)

    def acquire(self, res, owner, token=..., **body):
        return self.post(f"/r/{res}/acquire", {"owner": owner, **body}, token=token)

    def status(self, res=None):
        return self.get(f"/status/{res}" if res else "/status")[1]

    def events(self, res=None, kinds=None):
        q = f"?limit=1000&resource={res}" if res else "?limit=1000"
        ev = self.get(f"/events{q}")[1]["events"]
        return [e for e in ev if kinds is None or e["kind"] in kinds]


class ServerProc:
    def __init__(self, tmp_path, **env):
        self.port = free_port()
        self.db = str(tmp_path / "hwlock.db")
        self.log = tmp_path / f"server-{self.port}.log"
        self.env = {**os.environ, "HWLOCK_PORT": str(self.port), "HWLOCK_HOST": "127.0.0.1",
                    "HWLOCK_SWEEP_S": "0.2", **{k: str(v) for k, v in env.items()}}
        self.env.pop("HWLOCK_TOKEN", None) if "HWLOCK_TOKEN" not in env else None
        self.url = f"http://127.0.0.1:{self.port}"
        self.api = Api(self.url, env.get("HWLOCK_TOKEN"))
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen([sys.executable, "-m", "hwlock_server", "--db", self.db], cwd=ROOT,
                                     env=self.env, stdout=open(self.log, "a"), stderr=subprocess.STDOUT)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.url + "/health", timeout=1):
                    return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("server did not start: " + self.log.read_text())

    def kill(self):
        """SIGKILL: no graceful shutdown, like a crash or `docker kill`."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.kill()


@pytest.fixture
def make_server(tmp_path):
    servers = []

    def make(**env):
        s = ServerProc(tmp_path, **env)
        servers.append(s)
        return s.start()

    yield make
    for s in servers:
        s.stop()


@pytest.fixture
def server(make_server):
    return make_server()


@pytest.fixture
def api(server):
    return server.api


def run_client(server, *args, env=None, timeout=60, **kw):
    e = {**os.environ, "HWLOCK_URL": server.url, **(env or {})}
    if server.api.token and "HWLOCK_TOKEN" not in (env or {}):
        e["HWLOCK_TOKEN"] = server.api.token
    elif "HWLOCK_TOKEN" not in (env or {}):
        e.pop("HWLOCK_TOKEN", None)
    return subprocess.run([CLIENT_PYTHON, str(CLIENT), *args], env=e, capture_output=True, text=True,
                          timeout=timeout, **kw)


def popen_client(server, *args, env=None, **kw):
    e = {**os.environ, "HWLOCK_URL": server.url, **(env or {})}
    if server.api.token:
        e.setdefault("HWLOCK_TOKEN", server.api.token)
    return subprocess.Popen([CLIENT_PYTHON, str(CLIENT), *args], env=e, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, **kw)


def wait_for(cond, timeout=10.0, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(step)
    raise AssertionError("condition not met within %.1fs" % timeout)

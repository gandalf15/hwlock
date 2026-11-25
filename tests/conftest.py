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
    e = {**os.environ, "HWLOCK_URL": server.url, "HWLOCK_CONFIG": "/nonexistent", **(env or {})}
    if server.api.token and "HWLOCK_TOKEN" not in (env or {}):
        e["HWLOCK_TOKEN"] = server.api.token
    elif "HWLOCK_TOKEN" not in (env or {}):
        e.pop("HWLOCK_TOKEN", None)
    return subprocess.run([CLIENT_PYTHON, str(CLIENT), *args], env=e, capture_output=True, text=True,
                          timeout=timeout, **kw)


def popen_client(server, *args, env=None, **kw):
    e = {**os.environ, "HWLOCK_URL": server.url, "HWLOCK_CONFIG": "/nonexistent", **(env or {})}
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


EXPORTER = ROOT / "exporter" / "hwlock-exporter"


class ExporterProc:
    """A real hwlock-exporter process. It reads its proc/sys data from a directory the test controls (as
    --proc/--sys/--boot-id-file allow), so a test can make the box "reboot" (new boot_id, small uptime)
    or run low on memory, while everything from the exporter's HTTP answer onward is the real path."""

    def __init__(self, tmp_path, name="exp"):
        self.dir = tmp_path / name
        self.proc, self.sys = self.dir / "proc", self.dir / "sys"
        (self.proc / "sys/kernel/random").mkdir(parents=True)
        for zone, typ, temp in (("thermal_zone0", "cpu-thermal", 45000), ("thermal_zone1", "gpu-thermal", 47500)):
            (self.sys / "class/thermal" / zone).mkdir(parents=True)
            (self.sys / "class/thermal" / zone / "type").write_text(typ + "\n")
            (self.sys / "class/thermal" / zone / "temp").write_text(f"{temp}\n")
        (self.sys / "devices/platform/bus@0/17000000.gpu").mkdir(parents=True)
        (self.sys / "devices/platform/bus@0/17000000.gpu/load").write_text("155\n")
        (self.proc / "loadavg").write_text("0.50 0.40 0.30 1/100 1234\n")
        self.boot_id = "11111111-1111-1111-1111-111111111111"
        self.write_boot(self.boot_id, uptime=5000)
        self.set_mem_available(4000)
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}/"
        self.log = self.dir / "exporter.log"
        self.proc_handle = None

    def write_boot(self, boot_id, uptime):
        self.boot_id = boot_id
        (self.proc / "sys/kernel/random/boot_id").write_text(boot_id + "\n")
        (self.proc / "uptime").write_text(f"{uptime:.2f} 100.00\n")

    def reboot(self, boot_id="22222222-2222-2222-2222-222222222222", uptime=0.5):
        # the box rebooted "just now" (the exporter reads a fixed uptime, so keep it small: a reboot that
        # claims to be older than the hold would rightly not void it)
        self.write_boot(boot_id, uptime)

    def set_mem_available(self, mb):
        (self.proc / "meminfo").write_text(
            f"MemTotal:        7599428 kB\nMemFree:          100000 kB\nMemAvailable:   {int(mb * 1024)} kB\n"
            "SwapTotal:       4194300 kB\nSwapFree:        4194300 kB\n")

    def start(self, *extra):
        self.proc_handle = subprocess.Popen(
            [CLIENT_PYTHON, str(EXPORTER), "--bind", "127.0.0.1", "--port", str(self.port), "--proc", str(self.proc),
             "--sys", str(self.sys), "--nvpmodel-dir", str(self.dir / "nvpmodel"), *extra],
            stdout=open(self.log, "a"), stderr=subprocess.STDOUT)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.url, timeout=1) as r:
                    if r.status == 200 or extra:
                        return self
            except urllib.error.HTTPError:
                return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("exporter did not start: " + self.log.read_text())

    def get(self):
        with urllib.request.urlopen(self.url, timeout=2) as r:
            return json.loads(r.read())

    def kill(self):
        if self.proc_handle and self.proc_handle.poll() is None:
            self.proc_handle.send_signal(signal.SIGCONT)
            self.proc_handle.kill()
            self.proc_handle.wait()

    def freeze(self):
        """SIGSTOP: the kernel still accepts TCP connections on its socket, but nothing answers."""
        self.proc_handle.send_signal(signal.SIGSTOP)

    def thaw(self):
        self.proc_handle.send_signal(signal.SIGCONT)


@pytest.fixture
def make_exporter(tmp_path):
    made = []

    def make(name="exp", start=True):
        e = ExporterProc(tmp_path, name)
        made.append(e)
        return e.start() if start else e

    yield make
    for e in made:
        e.kill()


def health_config(tmp_path, resources: dict, name="health.json") -> str:
    path = tmp_path / name
    path.write_text(json.dumps({"resources": resources}))
    return str(path)


def exporter_resource(exp, *, on_fail="down", interval=0.2, down_after=3, up_after=2, degraded_after=2,
                      timeout=0.5, degraded_if=None, extra_probes=()):
    return {"interval_s": interval, "down_after": down_after, "up_after": up_after,
            "degraded_after": degraded_after,
            "probes": [{"type": "http_json", "url": exp.url, "timeout_s": timeout, "on_fail": on_fail,
                        "degraded_if": degraded_if or {"mem_available_mb": "<800", "temp_max_c": ">85"}},
                       *extra_probes]}


def health_state(api, res="jetson"):
    h = api.status(res).get("health") or {}
    return h.get("state")

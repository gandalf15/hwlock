"""Hardware health: probes, hysteresis, notifications to waiting and holding clients, reboot handling.

Everything here runs real processes: a real hwlock server with a health config, a real hwlock-exporter
(reading a proc/sys directory the test controls, so it can "reboot" or run low on memory), real
clients. A box "dies" by killing or SIGSTOP-ing its exporter process (SIGSTOP: TCP still accepted by the
kernel, no answer: the freeze case). The sshd container version of that is in test_health_docker.py.
"""

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request

import pytest

from conftest import (CLIENT_PYTHON, EXPORTER, exporter_resource, health_config, health_state, popen_client, run_client,
                      wait_for)
from hwlock_server import health as H

FAST = {"HWLOCK_LONG_POLL_S": "2"}


def kinds(api, res="jetson"):
    return [e["kind"] for e in api.events(res) if e["kind"].startswith("health_")
            or e["kind"] in ("rebooted", "lease_voided_reboot")]


@pytest.fixture
def jetson(tmp_path, make_server, make_exporter):
    """A server with resource 'jetson' probed through a real exporter; waits until it is up."""
    def make(**kw):
        exp = make_exporter()
        cfg = health_config(tmp_path, {"jetson": exporter_resource(exp, **kw)})
        srv = make_server(HWLOCK_HEALTH_CONFIG=cfg)
        wait_for(lambda: health_state(srv.api) == "up", timeout=10)
        return srv, exp
    return make


# ---------------------------------------------------------------------------- probes (real sockets)
def _listener(handler):
    """A real TCP server on 127.0.0.1 that runs handler(conn) per connection; returns (port, close)."""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    s.listen(16)
    stop = threading.Event()
    conns = []

    def loop():
        s.settimeout(0.2)
        while not stop.is_set():
            try:
                c, _ = s.accept()
            except OSError:
                continue
            conns.append(c)

            def run(c=c):
                try:
                    handler(c)
                except OSError:
                    pass
            threading.Thread(target=run, daemon=True).start()

    threading.Thread(target=loop, daemon=True).start()

    def close():
        stop.set()
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        s.close()
        for c in conns:
            c.close()
    return s.getsockname()[1], close


def test_ssh_banner_needs_the_banner_not_just_tcp():
    port, close = _listener(lambda c: c.sendall(b"SSH-2.0-OpenSSH_9.6\r\n"))
    try:
        r = H.ssh_banner("127.0.0.1", port, 1.0)
        assert r.ok and r.data["banner"] == "SSH-2.0-OpenSSH_9.6"
        assert H.tcp("127.0.0.1", port, 1.0).ok
    finally:
        close()
    # accepts TCP but never says anything (an OOM-frozen box): tcp passes, ssh_banner fails within the timeout
    port, close = _listener(lambda c: time.sleep(30))
    try:
        assert H.tcp("127.0.0.1", port, 1.0).ok
        t0 = time.monotonic()
        r = H.ssh_banner("127.0.0.1", port, 0.5)
        assert not r.ok and "no SSH banner" in r.error and time.monotonic() - t0 < 1.5
    finally:
        close()
    port, close = _listener(lambda c: c.sendall(b"HTTP/1.1 400 Bad\r\n\r\n"))
    try:
        r = H.ssh_banner("127.0.0.1", port, 1.0)
        assert not r.ok and "not an SSH banner" in r.error
    finally:
        close()
    r = H.ssh_banner("127.0.0.1", port, 1.0)  # closed now: refused
    assert not r.ok and "connect failed" in r.error


def test_http_json_has_one_total_deadline():
    """A half-dead box that trickles bytes must not stall the probe past its timeout."""
    def trickle(c):
        try:
            c.recv(1000)
            for ch in b"HTTP/1.0 200 OK\r\n\r\n{}":
                c.sendall(bytes([ch]))
                time.sleep(0.2)
        except OSError:
            pass
    port, close = _listener(trickle)
    try:
        t0 = time.monotonic()
        r = H.http_get_json(f"http://127.0.0.1:{port}/", 1.0)
        assert not r.ok and time.monotonic() - t0 < 1.6, r.error
    finally:
        close()


def test_tracker_hysteresis():
    t = H.Tracker("unknown", down_after=3, up_after=2, degraded_after=2)
    assert [t.observe(o) for o in ("up", "up")] == [None, "up"]
    # two failed rounds are not enough, and one good round in between resets the count
    assert [t.observe(o) for o in ("down", "down", "up", "down", "down")] == [None] * 5
    assert t.observe("down") == "down"
    # back only after up_after rounds that are not down; at the latest round's level
    assert [t.observe(o) for o in ("up", "down", "up", "degraded")] == [None, None, None, "degraded"]
    assert [t.observe(o) for o in ("up", "degraded", "up", "up")] == [None, None, None, "up"]
    assert [t.observe(o) for o in ("degraded", "up", "degraded", "degraded")] == [None, None, None, "degraded"]
    # a persisted state is the starting point (server restart): no transition by itself
    t2 = H.Tracker("down", 3, 2, 2)
    assert t2.observe("down") is None and t2.state == "down"


def test_config_errors_stop_the_server(tmp_path):
    bad = [
        {"jetson": {"probes": []}},
        {"jetson": {"probes": [{"type": "ping"}]}},
        {"jetson": {"probes": [{"type": "ssh_banner"}]}},
        {"jetson": {"probes": [{"type": "http_json", "url": "http://x/", "degraded_if": {"mem": "low"}}]}},
        {"bad name": {"probes": [{"type": "tcp", "host": "x", "port": 1}]}},
    ]
    for i, res in enumerate(bad):
        with pytest.raises(H.ConfigError):
            H.load_config(json.dumps({"resources": res}))
    with pytest.raises(H.ConfigError):
        H.load_config("{not json")
    path = health_config(tmp_path, bad[1])
    from conftest import ROOT
    p = subprocess.run([sys.executable, "-m", "hwlock_server", "--db", str(tmp_path / "x.db"), "--port", "0"],
                       cwd=ROOT, env={**os.environ, "HWLOCK_HEALTH_CONFIG": path}, capture_output=True, text=True,
                       timeout=20)
    assert p.returncode != 0 and "type must be one of" in p.stderr


# ---------------------------------------------------------------------------- exporter
def test_exporter_reads_the_real_proc():
    """With its defaults the exporter reports this machine's real boot id and memory."""
    out = subprocess.run([CLIENT_PYTHON, str(EXPORTER), "--once"], capture_output=True, text=True, timeout=20)
    d = json.loads(out.stdout)
    assert d["boot_id"] == open("/proc/sys/kernel/random/boot_id").read().strip()
    assert 0 < d["mem_available_mb"] <= d["mem_total_mb"]
    assert d["uptime_s"] > 0 and d["load1"] >= 0 and d["ncpu"] >= 1


def test_exporter_jetson_style_sysfs(make_exporter):
    exp = make_exporter()
    d = exp.get()
    assert d["boot_id"] == exp.boot_id and d["uptime_s"] == 5000
    assert d["mem_available_mb"] == 4000 and d["mem_total_mb"] == pytest.approx(7421.3, abs=0.1)
    assert d["temps_c"] == {"cpu-thermal": 45.0, "gpu-thermal": 47.5} and d["temp_max_c"] == 47.5
    assert d["gpu_load_pct"] == 15.5 and d["load1"] == 0.5
    nv = exp.dir / "nvpmodel"
    nv.mkdir()
    (nv / "status").write_text("pmode:0002\n")
    (exp.dir / "nvpmodel.conf").write_text("< POWER_MODEL ID=0 NAME=15W >\n< POWER_MODEL ID=2 NAME=MAXN_SUPER >\n")
    (nv / "conf_file_path").write_text(str(exp.dir / "nvpmodel.conf"))
    assert exp.get()["nvpmodel"] == "MAXN_SUPER"


def test_exporter_allow_list(make_exporter):
    exp = make_exporter(start=False).start("--allow", "10.0.0.0/8")
    with pytest.raises(urllib.error.HTTPError) as e:
        exp.get()
    assert e.value.code == 403


# ---------------------------------------------------------------------------- server: state, status, page
def test_status_page_and_health_endpoint_show_metrics(jetson, server_url_of=None):
    srv, exp = jetson()
    api = srv.api
    st = api.status("jetson")
    h = st["health"]
    assert h["state"] == "up" and h["boot_id"] == exp.boot_id
    assert h["metrics"]["mem_available_mb"] == 4000 and h["metrics"]["gpu_load_pct"] == 15.5
    assert h["probes"][0]["ok"] and h["checked_s_ago"] < 5
    assert any(r["resource"] == "jetson" for r in api.status()["resources"])  # listed though never acquired
    code, page = api.get("/")
    assert code == 200 and "UP" in page and "mem free 4000" in page and "GPU 16 %" in page
    code, out = api.get("/r/jetson/health")
    assert code == 200 and out["health"]["state"] == "up" and out["events"] == []
    assert api.get("/r/other/health")[0] == 404
    assert api.status("other")["health"] is None
    r = run_client(srv, "status")
    assert "jetson: health UP" in r.stdout and "mem free 4000" in r.stdout
    assert kinds(api) == ["health_up"]


def test_down_needs_consecutive_failures_and_comes_back(jetson):
    srv, exp = jetson(down_after=5, interval=0.2, timeout=0.3)
    api = srv.api
    # a short stall (about 2 failed rounds) is not "down"
    exp.freeze()
    time.sleep(0.7)
    exp.thaw()
    time.sleep(1.5)
    assert health_state(api) == "up" and kinds(api) == ["health_up"]
    # a frozen exporter (TCP accepted, no answer) for long enough is
    exp.freeze()
    t0 = time.time()
    wait_for(lambda: health_state(api) == "down", timeout=10)
    assert time.time() - t0 >= 5 * 0.2
    h = api.status("jetson")["health"]
    assert "no complete answer" in h["reason"]
    exp.thaw()
    wait_for(lambda: health_state(api) == "up", timeout=10)
    assert kinds(api) == ["health_up", "health_down", "health_up"]
    ev = [e for e in api.events("jetson") if e["kind"] == "health_down"][0]
    assert ev["detail"]["from"] == "up" and ev["detail"]["to"] == "down"


def test_exporter_failure_alone_is_degraded_not_down(tmp_path, make_server, make_exporter):
    """Default rule: an http_json failure while the liveness probe (here tcp to the hwlock server's own
    port, which stays up) passes means degraded."""
    exp = make_exporter()
    port_holder = socket.socket()
    port_holder.bind(("127.0.0.1", 0))
    port_holder.listen(64)
    try:
        res = exporter_resource(exp, on_fail="degraded", extra_probes=[
            {"type": "tcp", "host": "127.0.0.1", "port": port_holder.getsockname()[1], "timeout_s": 0.5}])
        srv = make_server(HWLOCK_HEALTH_CONFIG=health_config(tmp_path, {"jetson": res}))
        wait_for(lambda: health_state(srv.api) == "up")
        exp.kill()
        wait_for(lambda: health_state(srv.api) == "degraded")
        port_holder.close()
        wait_for(lambda: health_state(srv.api) == "down")
        assert kinds(srv.api) == ["health_up", "health_degraded", "health_down"]
    finally:
        port_holder.close()


# ---------------------------------------------------------------------------- waiting clients
def test_waiters_are_told_at_once_and_fail_if_down(jetson):
    srv, exp = jetson()
    api = srv.api
    assert run_client(srv, "acquire", "jetson", "holder").returncode == 0
    # long-poll 25 s: they can only react this fast because the server ends the poll on the health event
    quitter = popen_client(srv, "acquire", "jetson", "quitter", "--fail-if-down")
    stayer = popen_client(srv, "acquire", "jetson", "stayer")
    wait_for(lambda: len(api.status("jetson")["queue"]) == 2)
    time.sleep(0.5)
    exp.kill()
    t0 = time.time()
    quitter.wait(15)
    assert quitter.returncode == 6
    assert time.time() - t0 < 5
    err = quitter.stderr.read()
    assert "jetson is DOWN" in err and "gave up (--fail-if-down)" in err
    assert [q["owner"] for q in api.status("jetson")["queue"]] == ["stayer"]
    # nobody is granted a device that is down: the holder leaves, the stayer keeps waiting
    assert run_client(srv, "release", "jetson", "holder").returncode == 0
    time.sleep(1.5)
    assert api.status("jetson")["holder"] is None and stayer.poll() is None
    assert run_client(srv, "acquire", "jetson", "late", "--fail-if-down").returncode == 6
    exp.start()
    stayer.wait(15)
    assert stayer.returncode == 0
    err = stayer.stderr.read()
    assert "WARNING: jetson is DOWN" in err and "Still waiting" in err and "jetson is UP again" in err
    assert api.status("jetson")["holder"]["owner"] == "stayer"


def test_a_dead_box_is_not_granted_before_its_first_verdict(tmp_path, make_server):
    """A newly configured resource starts "unknown" until its probes decide. An acquire in that window must
    wait, not be handed a box that is already dead; --fail-if-down then exits 6 once it is declared down."""
    port, close = _listener(lambda c: time.sleep(60))  # accepts TCP, never sends a banner: frozen
    try:
        cfg = health_config(tmp_path, {"jetson": {"interval_s": 0.5, "down_after": 3, "probes": [
            {"type": "ssh_banner", "host": "127.0.0.1", "port": port, "timeout_s": 1.5}]}})
        srv = make_server(HWLOCK_HEALTH_CONFIG=cfg)
        assert health_state(srv.api) == "unknown"
        quitter = popen_client(srv, "acquire", "jetson", "early", "--fail-if-down")
        stayer = popen_client(srv, "acquire", "jetson", "plain")
        quitter.wait(30)
        err = quitter.stderr.read()
        assert quitter.returncode == 6, err
        assert "until its health is known" in err and "gave up (--fail-if-down)" in err
        assert health_state(srv.api) == "down"
        time.sleep(0.5)
        assert stayer.poll() is None and srv.api.status("jetson")["holder"] is None
        assert "acquire" not in [e["kind"] for e in srv.api.events("jetson")]
        stayer.kill()
    finally:
        close()


def test_acquire_while_down_waits_and_says_so(jetson):
    srv, exp = jetson()
    exp.kill()
    wait_for(lambda: health_state(srv.api) == "down")
    p = popen_client(srv, "acquire", "jetson", "w")
    wait_for(lambda: srv.api.status("jetson")["queue"])
    time.sleep(1)
    assert p.poll() is None and srv.api.status("jetson")["holder"] is None
    exp.start()
    p.wait(15)
    assert p.returncode == 0 and "nobody is granted it" in p.stderr.read()


# ---------------------------------------------------------------------------- holding clients
def test_run_is_stopped_when_the_device_goes_down(jetson, tmp_path):
    srv, exp = jetson()
    marker = tmp_path / "term"
    script = f"trap 'touch {marker}; exit 0' TERM; while true; do sleep 0.1; done"
    p = popen_client(srv, "run", "jetson", "job", "bench", "--", "bash", "-c", script, env=FAST)
    wait_for(lambda: (srv.api.status("jetson")["holder"] or {}).get("owner") == "job")
    time.sleep(0.5)
    exp.kill()
    p.wait(15)
    err = p.stderr.read()
    assert p.returncode == 6, err
    assert marker.exists(), "the command must get SIGTERM"
    assert "jetson is DOWN" in err and "exit 6" in err
    assert srv.api.status("jetson")["holder"] is None  # run released it


def test_run_on_down_warn_keeps_the_command(jetson, tmp_path):
    srv, exp = jetson()
    go = tmp_path / "go"
    p = popen_client(srv, "run", "jetson", "job", "--on-down=warn", "--", "bash", "-c",
                     f"while [ ! -e {go} ]; do sleep 0.1; done; exit 3", env=FAST)
    wait_for(lambda: (srv.api.status("jetson")["holder"] or {}).get("owner") == "job")
    exp.kill()
    wait_for(lambda: health_state(srv.api) == "down")
    time.sleep(1)
    assert p.poll() is None
    go.touch()
    p.wait(15)
    err = p.stderr.read()
    assert p.returncode == 3 and "--on-down=warn" in err and "check its results" in err


def test_reboot_voids_the_holder_and_keeps_the_queue(jetson, tmp_path):
    srv, exp = jetson()
    api = srv.api
    hold = popen_client(srv, "hold", "jetson", "holder", "--until-file", str(tmp_path / "never"), env=FAST)
    wait_for(lambda: (api.status("jetson")["holder"] or {}).get("owner") == "holder")
    w1 = popen_client(srv, "acquire", "jetson", "w1")
    wait_for(lambda: len(api.status("jetson")["queue"]) == 1)
    w2 = popen_client(srv, "acquire", "jetson", "w2")
    wait_for(lambda: len(api.status("jetson")["queue"]) == 2)
    time.sleep(0.5)
    exp.reboot()  # a new boot id and a small uptime: the box came back from a reboot
    hold.wait(15)
    err = hold.stderr.read()
    assert hold.returncode == 7, err
    assert "REBOOTED" in err or "rebooted" in err
    w1.wait(15)
    assert w1.returncode == 0 and "REBOOTED" in w1.stderr.read()
    assert api.status("jetson")["holder"]["owner"] == "w1"
    assert [q["owner"] for q in api.status("jetson")["queue"]] == ["w2"]
    ev = [e for e in api.events("jetson") if e["kind"] in ("rebooted", "lease_voided_reboot")]
    assert [e["kind"] for e in ev] == ["rebooted", "lease_voided_reboot"]
    assert ev[1]["owner"] == "holder" and ev[0]["detail"]["old_boot_id"].startswith("1111")
    # the voided holder's renew/release says why (exit 7), not a plain NOT_HOLDER
    r = run_client(srv, "renew", "jetson", "holder")
    assert r.returncode == 7 and r.stdout.startswith("VOIDED_REBOOT") and "rebooted" in r.stderr
    assert run_client(srv, "release", "jetson", "holder").returncode == 7
    assert api.status("jetson")["health"]["last_reboot"] is not None
    run_client(srv, "release", "jetson", "w1")
    w2.wait(15)
    assert w2.returncode == 0


def test_reboot_does_not_void_a_lock_taken_after_the_boot(jetson):
    """The exporter may start well after sshd: a lock taken after the boot is kept when the new boot id
    is finally seen."""
    srv, exp = jetson()
    exp.kill()
    wait_for(lambda: health_state(srv.api) == "down")
    exp.reboot(uptime=600)  # booted 10 min ago; the exporter just came up
    srv.api.acquire("jetson", "early")  # cannot be granted while down: it waits
    exp.start()
    wait_for(lambda: "rebooted" in kinds(srv.api))
    assert "lease_voided_reboot" not in kinds(srv.api)


def test_hold_exits_on_down(jetson, tmp_path):
    srv, exp = jetson()
    hold = popen_client(srv, "hold", "jetson", "h", "--until-file", str(tmp_path / "never"), env=FAST)
    wait_for(lambda: (srv.api.status("jetson")["holder"] or {}).get("owner") == "h")
    exp.kill()
    hold.wait(15)
    assert hold.returncode == 6 and srv.api.status("jetson")["holder"] is None


# ---------------------------------------------------------------------------- degraded, watch
def test_degraded_threshold_watch_and_exit_on_degraded(jetson, tmp_path):
    srv, exp = jetson()
    watch = popen_client(srv, "watch", "jetson")
    run = popen_client(srv, "run", "jetson", "job", "--exit-on-degraded", "--", "sleep", "60", env=FAST)
    wait_for(lambda: (srv.api.status("jetson")["holder"] or {}).get("owner") == "job")
    time.sleep(0.5)
    assert watch.poll() is None
    exp.set_mem_available(500)
    watch.wait(15)
    out = watch.stdout.read()
    assert watch.returncode == 8 and out.startswith("DEGRADED") and "mem_available_mb 500 < 800" in out
    run.wait(15)
    assert run.returncode == 8
    # degraded is only a warning by default
    r = run_client(srv, "run", "jetson", "job2", "--", "true", env=FAST)
    assert r.returncode == 0 and "WARNING: jetson is DEGRADED" in r.stderr
    assert run_client(srv, "acquire", "jetson", "picky", "--exit-on-degraded").returncode == 8
    assert srv.api.status("jetson")["holder"] is None
    # watch --until up: blocks through other changes until the device is up again
    until = popen_client(srv, "watch", "jetson", "--until", "up")
    time.sleep(1)
    assert until.poll() is None
    exp.set_mem_available(4000)
    until.wait(15)
    assert until.returncode == 0 and until.stdout.read().startswith("UP")


def test_watch_codes(jetson):
    srv, exp = jetson()
    r = run_client(srv, "watch", "jetson", "--timeout", "1")
    assert r.returncode == 3 and "watching jetson: jetson is UP" in r.stderr
    assert run_client(srv, "watch", "nothing-here").returncode == 4
    assert run_client(srv, "watch", "jetson", "--until", "up").returncode == 0  # already up
    w = popen_client(srv, "watch", "jetson")
    time.sleep(0.5)
    exp.reboot()
    w.wait(15)
    assert w.returncode == 7 and w.stdout.read().startswith("REBOOTED")
    w = popen_client(srv, "watch", "jetson", "--json")
    time.sleep(0.5)
    exp.kill()
    w.wait(15)
    assert w.returncode == 6 and json.loads(w.stdout.read())["events"][-1]["kind"] == "health_down"


# ---------------------------------------------------------------------------- restarts
def test_health_survives_server_restarts(tmp_path, make_server, make_exporter):
    exp = make_exporter()
    cfg = health_config(tmp_path, {"jetson": exporter_resource(exp)})
    srv = make_server(HWLOCK_HEALTH_CONFIG=cfg)
    api = srv.api
    wait_for(lambda: health_state(api) == "up")
    # a plain restart changes nothing: no new health events, no false reboot
    srv.stop()
    srv.start()
    time.sleep(1.5)
    assert kinds(api) == ["health_up"] and health_state(api) == "up"
    # the device reboots while the server is down: noticed after the restart (boot id is persisted)
    assert api.acquire("jetson", "holder")[0] == 200
    srv.stop()
    exp.reboot()
    srv.start()
    wait_for(lambda: "lease_voided_reboot" in kinds(api))
    assert api.status("jetson")["holder"] is None
    # down is persisted: right after a restart the device is still down, before any probe has run
    exp.kill()
    wait_for(lambda: health_state(api) == "down")
    srv.stop()
    srv.start()
    assert health_state(api) == "down"
    time.sleep(1.5)
    assert kinds(api)[-1] == "health_down" and health_state(api) == "down"
    assert api.acquire("jetson", "w", wait=0)[0] == 202  # still no grants

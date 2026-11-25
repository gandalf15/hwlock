"""The single-file `hwlock` client, run as a subprocess against a real server."""

import signal
import sys
import time

from conftest import popen_client, run_client, wait_for

FAST = {"HWLOCK_LONG_POLL_S": "1"}


def holder(api, res="jetson"):
    h = api.status(res)["holder"]
    return h and h["owner"]


def test_acquire_renew_release_exit_codes(server):
    api = server.api
    r = run_client(server, "acquire", "jetson", "a", "bench", "--lease", "60")
    assert r.returncode == 0 and "a holds jetson" in r.stdout
    h = api.status("jetson")["holder"]
    assert (h["owner"], h["reason"], h["lease_s"]) == ("a", "bench", 60)
    assert h["pid"] is not None and h["host"]
    r = run_client(server, "acquire", "jetson", "a")
    assert r.returncode == 0 and "already held" in r.stdout
    r = run_client(server, "renew", "jetson", "a")
    assert r.returncode == 0 and r.stdout.startswith("RENEWED")
    r = run_client(server, "renew", "jetson", "b")
    assert r.returncode == 1 and "NOT_HOLDER holder=a" in r.stdout
    r = run_client(server, "release", "jetson", "b")
    assert r.returncode == 1 and "NOT_HOLDER holder=a" in r.stdout
    r = run_client(server, "release", "jetson", "a")
    assert r.returncode == 0 and r.stdout.strip() == "RELEASED"
    assert holder(api) is None


def test_usage_errors(server):
    assert run_client(server).returncode == 2
    assert run_client(server, "help").returncode == 0
    assert run_client(server, "acquire", "jetson").returncode == 2
    assert run_client(server, "acquire", "jetson", "a", "--lease").returncode == 2
    assert run_client(server, "run", "jetson", "a").returncode == 2
    assert run_client(server, "hold", "jetson", "a").returncode == 2
    assert run_client(server, "acquire", "jetson", "bad owner").returncode == 4  # server rejects


def test_status_output(server):
    run_client(server, "acquire", "jetson", "a", "deploy")
    p = popen_client(server, "acquire", "jetson", "b", "bench", env=FAST)
    wait_for(lambda: server.api.status("jetson")["queue"])
    r = run_client(server, "status")
    assert "jetson: held by a" in r.stdout and "deploy" in r.stdout
    assert "1. b" in r.stdout and "bench" in r.stdout
    assert '"owner": "a"' in run_client(server, "status", "jetson", "--json").stdout
    run_client(server, "release", "jetson", "a")
    p.wait(10)
    assert p.returncode == 0
    assert "kind" not in run_client(server, "events").stdout and "acquire" in run_client(server, "events").stdout


def test_waiting_prints_queue_position_then_acquires(server):
    run_client(server, "acquire", "jetson", "a")
    run_client(server, "acquire", "jetson", "b", "--max-wait", "0.1")  # gives up, cancels its ticket
    p1 = popen_client(server, "acquire", "jetson", "w1", env=FAST)
    wait_for(lambda: len(server.api.status("jetson")["queue"]) == 1)
    p2 = popen_client(server, "acquire", "jetson", "w2", env=FAST)
    wait_for(lambda: len(server.api.status("jetson")["queue"]) == 2)
    time.sleep(1.5)
    run_client(server, "release", "jetson", "a")
    p1.wait(10)
    assert holder(server.api) == "w1"
    time.sleep(1.5)  # let w2 see (and print) its new position before w1 releases
    run_client(server, "release", "jetson", "w1")
    p2.wait(10)
    err1, err2 = p1.stderr.read(), p2.stderr.read()
    assert "position 1 of" in err1 and "holder a" in err1
    assert "position 2 of 2" in err2 and "position 1 of 1" in err2 and "holder w1" in err2
    for err in (err1, err2):
        lines = err.splitlines()
        assert all(a != b for a, b in zip(lines, lines[1:])), "prints only when something changed"


def test_max_wait_gives_up_with_exit_3_and_cancels_ticket(server):
    run_client(server, "acquire", "jetson", "a")
    t0 = time.monotonic()
    r = run_client(server, "acquire", "jetson", "b", "--max-wait", "1.5")
    assert r.returncode == 3 and "gave up" in r.stderr
    assert 1.4 < time.monotonic() - t0 < 6
    assert server.api.status("jetson")["queue"] == []
    assert server.api.events("jetson", {"cancel"})[0]["owner"] == "b"


def test_interrupt_while_waiting_cancels_ticket(server):
    run_client(server, "acquire", "jetson", "a")
    for sig in (signal.SIGINT, signal.SIGTERM):
        p = popen_client(server, "acquire", "jetson", "b")
        wait_for(lambda: server.api.status("jetson")["queue"])
        time.sleep(0.3)
        p.send_signal(sig)
        p.wait(10)
        assert p.returncode == 130
        assert server.api.status("jetson")["queue"] == []
    assert holder(server.api) == "a"


def test_killed_waiter_is_not_granted_the_lock(server):
    """SIGKILL while long-polling: no cancel is sent, but the server sees the closed connection
    when it is that ticket's turn, and passes the lock on instead of granting a dead client."""
    run_client(server, "acquire", "jetson", "a")
    dead = popen_client(server, "acquire", "jetson", "dead")
    wait_for(lambda: server.api.status("jetson")["queue"])
    live = popen_client(server, "acquire", "jetson", "live", env=FAST)
    wait_for(lambda: len(server.api.status("jetson")["queue"]) == 2)
    dead.kill()
    dead.wait()
    time.sleep(0.3)
    run_client(server, "release", "jetson", "a")
    live.wait(10)
    assert live.returncode == 0 and holder(server.api) == "live"
    assert server.api.events("jetson", {"abandon"})[0]["owner"] == "dead"


def test_run_auto_renews_and_releases(server):
    api = server.api
    p = popen_client(server, "run", "jetson", "runner", "long job", "--lease", "2", "--",
                     sys.executable, "-c", "import time; time.sleep(5)")
    wait_for(lambda: holder(api) == "runner")
    t0 = time.monotonic()
    # lease is 2 s, the job takes 5 s: it must stay held while the job runs (checked until 4.5 s;
    # the release right at the end may be seen before the client process has exited)
    while time.monotonic() - t0 < 4.5:
        h = api.status("jetson")["holder"]
        assert h and h["owner"] == "runner", f"lost the lock after {time.monotonic() - t0:.1f}s"
        time.sleep(0.2)
    p.wait(10)
    assert p.returncode == 0
    assert holder(api) is None
    ev = api.events("jetson")
    kinds = [e["kind"] for e in ev]
    assert kinds.count("renew") >= 4 and "expire" not in kinds
    assert kinds[-1] == "release" and ev[-1]["detail"]["held_s"] >= 5


def test_run_releases_on_failure_and_returns_exit_code(server):
    r = run_client(server, "run", "jetson", "f", "--", sys.executable, "-c", "raise SystemExit(7)")
    assert r.returncode == 7
    assert holder(server.api) is None
    r = run_client(server, "run", "jetson", "f", "--", "/nonexistent/cmd")
    assert r.returncode == 127 and holder(server.api) is None
    r = run_client(server, "run", "jetson", "f", "--", "sh", "-c", "kill -9 $$")
    assert r.returncode == 128 + 9 and holder(server.api) is None


def test_run_releases_on_interrupt(server, tmp_path):
    marker = tmp_path / "child-got-signal"
    code = ("import signal,sys,time\n"
            "def h(*a):\n open(sys.argv[1],'w').write('x'); sys.exit(5)\n"
            "signal.signal(signal.SIGINT,h); signal.signal(signal.SIGTERM,h)\n"
            "time.sleep(30)\n")
    for sig in (signal.SIGINT, signal.SIGTERM):
        if marker.exists():
            marker.unlink()
        p = popen_client(server, "run", "jetson", "x", "--", sys.executable, "-c", code, str(marker))
        wait_for(lambda: holder(server.api) == "x")
        time.sleep(0.5)
        p.send_signal(sig)
        p.wait(10)
        assert marker.exists(), "the signal must reach the command"
        assert p.returncode == 5
        assert holder(server.api) is None


def test_run_waits_its_turn(server):
    run_client(server, "acquire", "jetson", "first")
    p = popen_client(server, "run", "jetson", "second", "--", sys.executable, "-c", "print('ran')", env=FAST)
    time.sleep(1.5)
    assert p.poll() is None
    run_client(server, "release", "jetson", "first")
    out, _ = p.communicate(timeout=10)
    assert p.returncode == 0 and "ran" in out and holder(server.api) is None


def test_hold_until_file(server, tmp_path):
    flag = tmp_path / "done"
    p = popen_client(server, "hold", "jetson", "holder", "manual session", "--until-file", str(flag),
                     "--lease", "2", "--poll", "0.2")
    wait_for(lambda: holder(server.api) == "holder")
    time.sleep(4)  # twice the lease: renewals keep it
    assert holder(server.api) == "holder"
    flag.write_text("")
    p.wait(10)
    assert p.returncode == 0 and holder(server.api) is None
    assert "expire" not in [e["kind"] for e in server.api.events("jetson")]


def test_unreachable_and_unauthorized_exit_4(make_server):
    srv = make_server(HWLOCK_TOKEN="tok")
    r = run_client(srv, "status", env={"HWLOCK_TOKEN": ""})
    assert r.returncode == 4 and "unauthorized" in r.stderr
    assert run_client(srv, "status").returncode == 0
    srv.stop()
    r = run_client(srv, "status")
    assert r.returncode == 4


def test_waiting_client_reattaches_after_server_restart(make_server):
    srv = make_server(HWLOCK_POLL_S="1", HWLOCK_HEARTBEAT_S="10")
    api = srv.api
    run_client(srv, "acquire", "jetson", "a")
    p = popen_client(srv, "acquire", "jetson", "b", env=FAST)
    wait_for(lambda: api.status("jetson")["queue"])
    ticket = api.status("jetson")["queue"][0]["ticket"]
    srv.kill()
    time.sleep(12)  # down for longer than the heartbeat window
    srv.start()
    assert api.status("jetson")["queue"][0]["ticket"] == ticket
    run_client(srv, "release", "jetson", "a")
    p.wait(30)
    err = p.stderr.read()
    assert p.returncode == 0, err
    assert "unreachable" in err and "rejoining" not in err
    assert api.events("jetson", {"acquire"})[-1]["ticket"] == ticket


def test_config_file_supplies_url_and_token(make_server, tmp_path):
    srv = make_server(HWLOCK_TOKEN="from-file")
    conf = tmp_path / "config"
    conf.write_text(f"# hwlock client config\nurl = {srv.url}\ntoken=from-file\n")
    env = {"HWLOCK_CONFIG": str(conf), "HWLOCK_URL": "", "HWLOCK_TOKEN": ""}
    r = run_client(srv, "acquire", "jetson", "cfg", env=env)
    assert r.returncode == 0, r.stderr
    assert srv.api.status("jetson")["holder"]["owner"] == "cfg"

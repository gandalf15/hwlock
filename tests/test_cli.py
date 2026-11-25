"""The single-file `hwlock` client, run as a subprocess against a real server."""

import os
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


def test_killed_waiter_is_dropped_even_when_its_turn_comes_after_its_poll(make_server):
    """SIGKILL while long-polling, its turn comes only after that poll's server-side wait has ended:
    the server must notice the closed connection during the poll (not wait for the heartbeat
    timeout, 60 s here), drop the ticket, and the next waiter must get the lock at once."""
    server = make_server(HWLOCK_POLL_S="2", HWLOCK_HEARTBEAT_S="60")
    poll = {"HWLOCK_LONG_POLL_S": "2"}
    run_client(server, "acquire", "jetson", "a")
    dead = popen_client(server, "acquire", "jetson", "dead", env=poll)
    wait_for(lambda: server.api.status("jetson")["queue"])
    live = popen_client(server, "acquire", "jetson", "live", env=poll)
    wait_for(lambda: len(server.api.status("jetson")["queue"]) == 2)
    dead.kill()
    dead.wait()
    time.sleep(4)  # > the dead client's last poll (2 s): its turn comes after the server answered it
    assert [q["owner"] for q in server.api.status("jetson")["queue"]] == ["live"], \
        "dead waiter's ticket must be gone before its turn"
    t0 = time.monotonic()
    run_client(server, "release", "jetson", "a")
    live.wait(10)
    assert live.returncode == 0 and holder(server.api) == "live"
    assert time.monotonic() - t0 < 3
    assert [e["owner"] for e in server.api.events("jetson", {"abandon", "drop"})] == ["dead"]
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


def test_renew_over_max_hold_prints_like_jetson_lock(make_server):
    server = make_server(HWLOCK_MAX_HOLD_S="1", HWLOCK_HOLD_GRACE_S="2")
    run_client(server, "acquire", "jetson", "hog")
    waiter = popen_client(server, "acquire", "jetson", "next", env=FAST)
    wait_for(lambda: server.api.status("jetson")["queue"])
    time.sleep(1.2)
    r = run_client(server, "renew", "jetson", "hog")
    assert r.returncode == 5 and r.stdout.startswith("OVER_MAX_HOLD waiting=1"), (r.stdout, r.stderr)
    waiter.wait(10)  # evicted after max hold + grace
    assert waiter.returncode == 0 and holder(server.api) == "next"
    assert server.api.events("jetson", {"evict_max_hold"})[0]["owner"] == "hog"


GUARD = {"HWLOCK_MAX_HOLD_S": "0", "HWLOCK_RESOURCE_MAX_HOLD_S": "jetson=2", "HWLOCK_RESOURCE_HOLD_GRACE_S": "jetson=3"}


def acquired_at(api, owner, res="jetson"):
    return [e["ts"] for e in api.events(res, {"acquire"}) if e["owner"] == owner]


def test_blind_proxy_hold_loses_to_a_waiter_within_max_hold_plus_grace(make_server, tmp_path):
    """The 2026-11-25 incident: an idle proxy hold renewing blindly while a job queues behind it."""
    server = make_server(**GUARD)
    api = server.api
    hog = popen_client(server, "hold", "jetson", "proxy", "idle proxy", "--lease", "60",
                       "--until-file", str(tmp_path / "never"))
    wait_for(lambda: holder(api) == "proxy")
    waiter = popen_client(server, "acquire", "jetson", "job", "real work", env=FAST)
    waiter.wait(15)
    assert waiter.returncode == 0 and holder(api) == "job"
    t_hog, t_job = acquired_at(api, "proxy")[0], acquired_at(api, "job")[0]
    assert 2 + 3 <= t_job - t_hog < 2 + 3 + 1.5  # max hold + grace (the server sweeps every 0.2 s)
    _, err = hog.communicate(timeout=10)
    assert hog.returncode == 5, err  # hold ends on its own, with its own exit code
    assert "OVER_MAX_HOLD" in err and "Not renewing any more" in err and "exit 5" in err
    kinds = [e["kind"] for e in api.events("jetson")]
    assert kinds.count("renew_refused") == 1  # it stopped renewing after the refusal
    assert kinds.count("evict_max_hold") == 1 and "release" not in kinds


def test_hold_without_waiters_has_no_limit(make_server, tmp_path):
    server = make_server(**GUARD)
    done = tmp_path / "done"
    hog = popen_client(server, "hold", "jetson", "solo", "--lease", "3", "--until-file", str(done))
    wait_for(lambda: holder(server.api) == "solo")
    time.sleep(7)  # > max hold + grace, and the 3 s lease is renewed meanwhile
    assert holder(server.api) == "solo"
    done.touch()
    _, err = hog.communicate(timeout=10)
    assert hog.returncode == 0, err
    kinds = [e["kind"] for e in server.api.events("jetson")]
    assert "renew_refused" not in kinds and "evict_max_hold" not in kinds and "renew" in kinds


def test_run_past_max_hold_warns_and_lets_the_command_finish_by_default(make_server):
    server = make_server(**GUARD)
    api = server.api
    run = popen_client(server, "run", "jetson", "long", "--lease", "60", "--",
                       sys.executable, "-c", "import time; time.sleep(9); print('done')")
    wait_for(lambda: holder(api) == "long")
    waiter = popen_client(server, "acquire", "jetson", "next", env=FAST)
    waiter.wait(15)
    assert waiter.returncode == 0 and run.poll() is None  # the command is still running
    out, err = run.communicate(timeout=15)
    assert run.returncode == 0 and "done" in out, err  # the command's own exit code
    assert "OVER_MAX_HOLD" in err and "keeps running" in err and "lost jetson" in err
    assert "check its results" in err
    assert holder(api) == "next"  # run did not release the next holder's lock


def test_run_kill_on_preempt_stops_the_command_before_the_eviction(make_server, tmp_path):
    server = make_server(**GUARD)
    api = server.api
    pidfile = tmp_path / "pid"
    # ignores SIGTERM, so only the SIGKILL escalation can stop it in time
    child = ("import os, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN);"
             "open(%r, 'w').write(str(os.getpid())); time.sleep(60)" % str(pidfile))
    run = popen_client(server, "run", "jetson", "long", "--lease", "60", "--kill-on-preempt", "--",
                       sys.executable, "-c", child)
    wait_for(pidfile.exists)
    pid = int(wait_for(pidfile.read_text))
    waiter = popen_client(server, "acquire", "jetson", "next", env=FAST)
    died = {}

    def alive():
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    t0 = time.time()
    while time.time() - t0 < 15 and alive():
        time.sleep(0.02)
    died["t"] = time.time()
    _, err = run.communicate(timeout=10)
    waiter.wait(15)
    assert run.returncode == 5, err
    assert "--kill-on-preempt" in err and "command stopped" in err
    assert waiter.returncode == 0 and holder(api) == "next"
    # dead before the eviction deadline (since + max hold + grace) ...
    assert died["t"] < acquired_at(api, "long")[0] + 2 + 3 - 0.3, (died["t"] - acquired_at(api, "long")[0], err)
    # ... and the lock went on by run's own release (after it reaped the command), not by eviction
    kinds = [(e["kind"], e["owner"]) for e in api.events("jetson") if e["kind"] in ("release", "evict_max_hold",
                                                                                   "acquire")]
    assert kinds == [("acquire", "long"), ("release", "long"), ("acquire", "next")], kinds


def test_status_shows_held_for_and_the_guard_deadline(make_server):
    server = make_server(HWLOCK_MAX_HOLD_S="0", HWLOCK_RESOURCE_MAX_HOLD_S="jetson=600")
    run_client(server, "acquire", "jetson", "a", "deploy")
    r = run_client(server, "status", "jetson")
    assert "held 0s" in r.stdout and "max hold" not in r.stdout  # nobody waits: no deadline
    p = popen_client(server, "acquire", "jetson", "b", env=FAST)
    wait_for(lambda: server.api.status("jetson")["queue"])
    r = run_client(server, "status", "jetson")
    assert "max hold 10m00s: renew refused in 9m5" in r.stdout and "evicted in 14m5" in r.stdout, r.stdout
    run_client(server, "release", "jetson", "a")
    p.wait(10)


def test_acquire_max_hold_flag(make_server):
    server = make_server(HWLOCK_MAX_HOLD_S="600")
    r = run_client(server, "acquire", "jetson", "a", "--max-hold", "30")
    assert r.returncode == 0 and "max hold 30s while others wait" in r.stdout, r.stdout
    assert server.api.status("jetson")["holder"]["guard"]["max_hold_s"] == 30

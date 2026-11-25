"""The bash/curl fallback client and the jetson-lock drop-in wrapper (examples/jetson-lock)."""

import os
import shutil
import subprocess
import sys
import time

import pytest

from conftest import CLIENT, ROOT, wait_for

pytestmark = pytest.mark.skipif(not (shutil.which("bash") and shutil.which("curl")), reason="needs bash+curl")
SH = ROOT / "client" / "hwlock.sh"
WRAPPER = ROOT / "examples" / "jetson-lock"


def sh(server, *args, env=None, timeout=60):
    e = {**os.environ, "HWLOCK_URL": server.url, **(env or {})}
    return subprocess.run(["bash", str(SH), *args], env=e, capture_output=True, text=True, timeout=timeout)


def test_bash_fallback(server):
    api = server.api
    r = sh(server, "acquire", "jetson", "bash-a", "via curl", "60")
    assert r.returncode == 0, r.stderr
    h = api.status("jetson")["holder"]
    assert (h["owner"], h["reason"], h["lease_s"]) == ("bash-a", "via curl", 60)
    assert sh(server, "renew", "jetson", "bash-a").stdout.strip() == "RENEWED"
    r = sh(server, "release", "jetson", "other")
    assert r.returncode == 1 and r.stdout.strip() == "NOT_HOLDER holder=bash-a"
    e = {**os.environ, "HWLOCK_URL": server.url}
    waiter = subprocess.Popen(["bash", str(SH), "acquire", "jetson", "bash-b"], env=e, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wait_for(lambda: api.status("jetson")["queue"])
    time.sleep(0.5)
    assert sh(server, "release", "jetson", "bash-a").stdout.strip() == "RELEASED"
    out, err = waiter.communicate(timeout=15)
    assert waiter.returncode == 0 and "position 1" in err
    assert api.status("jetson")["holder"]["owner"] == "bash-b"
    assert '"owner": "bash-b"' in sh(server, "status", "jetson").stdout


@pytest.fixture
def fake_ssh(tmp_path):
    """Stands in for ssh: drops the options and host, runs the 'remote' command locally."""
    p = tmp_path / "fake-ssh"
    p.write_text('#!/usr/bin/env bash\nwhile [[ $1 == -o ]]; do shift 2; done\n'
                 'echo "host=$1" >&2; shift; exec "$@"\n')
    p.chmod(0o755)
    return str(p)


def wrapper(server, *args, env=None, timeout=60):
    e = {**os.environ, "HWLOCK_URL": server.url, "HWLOCK": str(CLIENT), "HWLOCK_CONFIG": "/nonexistent",
         **(env or {})}
    return subprocess.run(["bash", str(WRAPPER), *args], env=e, capture_output=True, text=True,
                          timeout=timeout)


def test_jetson_lock_wrapper_keeps_old_verbs(server, fake_ssh):
    api = server.api
    r = wrapper(server, "acquire", "job-a", "deploy", env={"JETSON_LOCK_LEASE_S": "120"})
    assert r.returncode == 0, r.stderr
    h = api.status("jetson")["holder"]
    assert (h["owner"], h["reason"], h["lease_s"]) == ("job-a", "deploy", 120)
    assert "jetson: held by job-a" in wrapper(server, "status").stdout
    assert wrapper(server, "renew", "job-a").returncode == 0
    assert wrapper(server, "release", "job-b").returncode == 1
    r = wrapper(server, "acquire", "job-b", env={"JETSON_LOCK_MAX_WAIT_S": "1"})
    assert r.returncode == 3
    assert wrapper(server, "release", "job-a").stdout.strip() == "RELEASED"
    env = {"JETSON_SSH": f"{fake_ssh} -o BatchMode=yes", "JETSON_HOST": "me@jetson"}
    r = wrapper(server, "run", "job-c", "bench", "--", sys.executable, "-c", "print('on jetson'); raise SystemExit(4)",
                env=env)
    assert r.returncode == 4 and "on jetson" in r.stdout and "host=me@jetson" in r.stderr
    r = wrapper(server, "run", "job-c", "--", "true", env=env)
    assert r.returncode == 0
    assert api.status("jetson")["free"]
    owners = [e["owner"] for e in api.events("jetson", {"acquire"})]
    assert owners == ["job-a", "job-c", "job-c"]
    assert wrapper(server, "bogus").returncode == 2


def test_over_max_hold_exit_5_in_bash_client_and_wrapper(make_server):
    server = make_server(HWLOCK_MAX_HOLD_S="600", HWLOCK_HOLD_GRACE_S="30")
    api = server.api
    # JETSON_LOCK_MAX_HOLD_S maps to --max-hold (lowers the server's limit for this hold)
    r = wrapper(server, "acquire", "job-a", env={"JETSON_LOCK_MAX_HOLD_S": "1", "JETSON_LOCK_LEASE_S": "60"})
    assert r.returncode == 0, r.stderr
    assert api.status("jetson")["holder"]["guard"]["max_hold_s"] == 1
    ticket = api.acquire("jetson", "job-b", wait=0)[1]["ticket"]
    time.sleep(1.2)
    r = wrapper(server, "renew", "job-a")
    assert r.returncode == 5 and r.stdout.startswith("OVER_MAX_HOLD waiting=1"), (r.stdout, r.stderr)
    r = sh(server, "renew", "jetson", "job-a")
    assert r.returncode == 5 and r.stdout.startswith("OVER_MAX_HOLD waiting=1 evict_in="), (r.stdout, r.stderr)
    r = sh(server, "renew", "jetson", "nobody")
    assert r.returncode == 1 and r.stdout.strip() == "NOT_HOLDER holder=job-a"
    api.post("/r/jetson/cancel", {"ticket": ticket})

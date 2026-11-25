"""The real container: build, run under its own compose project, kill it mid-queue, restart, check state.

Uses compose project `hwlock-test` on a random port, so a deployed `hwlock` instance is never touched.
Skipped when docker is not available. Run only these: pytest -m docker
"""

import json
import os
import shutil
import subprocess
import time

import pytest

from conftest import ROOT, Api, free_port, wait_for

pytestmark = pytest.mark.docker

PROJECT = "hwlock-test"


def docker_ok():
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@pytest.fixture
def container():
    if not docker_ok():
        pytest.skip("docker not available")
    port = free_port()
    env = {k: v for k, v in os.environ.items() if not k.startswith("HWLOCK_")}
    env.update(HWLOCK_PUBLISH_PORT=str(port), HWLOCK_HEARTBEAT_S="60")

    def compose(*args, check=True):
        return subprocess.run(["docker", "compose", "-p", PROJECT, "-f", str(ROOT / "compose.yaml"), *args],
                              cwd=ROOT, env=env, capture_output=True, text=True, check=check, timeout=300)

    compose("down", "-v", check=False)
    compose("up", "-d", "--build")
    api = Api(f"http://127.0.0.1:{port}")

    def up():
        try:
            return api.get("/health", timeout=1)[0] == 200
        except OSError:
            return False

    wait_for(up, timeout=60, step=0.3)
    yield compose, api, up
    compose("down", "-v", check=False)


def test_container_survives_kill_mid_queue(container):
    compose, api, up = container
    assert compose("exec", "-T", "hwlock", "id", "-u").stdout.strip() == "10001", "must run as non-root"
    cid = compose("ps", "-q", "hwlock").stdout.strip()

    def health():
        out = subprocess.run(["docker", "inspect", cid], capture_output=True, text=True).stdout
        return json.loads(out)[0]["State"].get("Health", {}).get("Status") == "healthy"

    wait_for(health, timeout=60, step=1)

    assert api.acquire("jetson", "holder", reason="deploy", lease=600)[0] == 200
    tickets = [api.acquire("jetson", f"w{i}", wait=0)[1]["ticket"] for i in range(3)]
    before = api.status("jetson")

    compose("kill", "-s", "SIGKILL", "hwlock")
    assert not up()
    time.sleep(2)
    compose("up", "-d")
    wait_for(up, timeout=60, step=0.3)

    after = api.status("jetson")
    assert after["holder"]["owner"] == "holder" and after["holder"]["reason"] == "deploy"
    assert after["holder"]["expires"] == before["holder"]["expires"]
    assert [q["ticket"] for q in after["queue"]] == tickets
    st, out = api.acquire("jetson", "w0", ticket=tickets[0], wait=0)
    assert st == 202 and out["position"] == 1
    assert api.post("/r/jetson/release", {"owner": "holder"})[0] == 200
    assert api.acquire("jetson", "w0", ticket=tickets[0], wait=0)[0] == 200
    assert [e["kind"] for e in api.events()].count("server_start") == 2


def test_client_is_in_the_image(container):
    compose, _, _ = container
    r = compose("exec", "-T", "hwlock", "hwlock", "status")
    assert r.returncode == 0

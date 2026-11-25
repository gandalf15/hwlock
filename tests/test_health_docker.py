"""Health probes against a real sshd in a container, and the health config inside the real hwlock image.

`docker pause` freezes every process of the sshd container: the kernel still completes the TCP handshake
(docker's port forwarding accepts it), but sshd never sends its banner. That is what the Jetson did when
it ran out of memory on 2026-11-25, and what the ssh_banner probe must call down. `docker stop` is a box
that is off (connection refused). Skipped when docker is not available. Run only these: pytest -m docker
"""

import os
import subprocess
import uuid

import pytest

from conftest import ROOT, Api, free_port, health_config, health_state, popen_client, wait_for

pytestmark = pytest.mark.docker

SSHD_IMAGE = "hwlock-test-sshd:1"
SSHD_DOCKERFILE = b"""FROM alpine:3.20
RUN apk add --no-cache openssh-server && ssh-keygen -A
CMD ["/usr/sbin/sshd", "-D", "-e"]
"""


def docker(*args, check=True, **kw):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check, timeout=300, **kw)


@pytest.fixture
def sshd():
    from test_docker import docker_ok
    if not docker_ok():
        pytest.skip("docker not available")
    if docker("image", "inspect", SSHD_IMAGE, check=False).returncode != 0:
        subprocess.run(["docker", "build", "-q", "-t", SSHD_IMAGE, "-"], input=SSHD_DOCKERFILE, check=True,
                       capture_output=True, timeout=300)
    name = f"hwlock-test-sshd-{uuid.uuid4().hex[:8]}"
    docker("run", "-d", "--name", name, "-p", "127.0.0.1::22", SSHD_IMAGE)
    port = int(docker("port", name, "22/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1])
    yield name, port
    docker("unpause", name, check=False)  # a paused container cannot be removed
    docker("rm", "-f", name, check=False)


def test_ssh_banner_probe_against_real_sshd(sshd, tmp_path, make_server):
    name, port = sshd
    cfg = health_config(tmp_path, {"jetson": {"interval_s": 0.3, "down_after": 3, "up_after": 2, "probes": [
        {"type": "ssh_banner", "host": "127.0.0.1", "port": port, "timeout_s": 1}]}})
    srv = make_server(HWLOCK_HEALTH_CONFIG=cfg)
    api = srv.api
    wait_for(lambda: health_state(api) == "up", timeout=30)
    assert api.status("jetson")["health"]["probes"][0]["ok"]

    # frozen: TCP is accepted, no banner. A waiting --fail-if-down client is told and gives up.
    assert api.acquire("jetson", "holder")[0] == 200
    quitter = popen_client(srv, "acquire", "jetson", "quitter", "--fail-if-down")
    wait_for(lambda: api.status("jetson")["queue"])
    docker("pause", name)
    quitter.wait(30)
    assert quitter.returncode == 6
    h = api.status("jetson")["health"]
    assert h["state"] == "down" and "TCP accepted but no SSH banner" in h["reason"], h
    docker("unpause", name)
    wait_for(lambda: health_state(api) == "up", timeout=30)

    # off: connection refused
    docker("stop", "-t", "1", name)
    wait_for(lambda: health_state(api) == "down", timeout=30)
    assert "connect failed" in api.status("jetson")["health"]["reason"]
    docker("start", name)
    # docker may publish the port again on another host port: the probe keeps the old one, so re-check
    new_port = int(docker("port", name, "22/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1])
    if new_port == port:
        wait_for(lambda: health_state(api) == "up", timeout=30)
    kinds = [e["kind"] for e in api.events("jetson") if e["kind"].startswith("health_")]
    assert kinds[:4] == ["health_up", "health_down", "health_up", "health_down"]


def test_health_config_in_the_container(tmp_path):
    """The image reads the config from the mounted /config directory (as uid 10001) and shows health."""
    from test_docker import docker_ok
    if not docker_ok():
        pytest.skip("docker not available")
    port = free_port()
    cfgdir = tmp_path / "config"
    cfgdir.mkdir()
    # probe the server's own port inside the container: up
    health_config(cfgdir, {"self": {"interval_s": 0.5, "probes": [
        {"type": "tcp", "host": "127.0.0.1", "port": 8770, "timeout_s": 1}]}}, name="health.json")
    os.chmod(cfgdir / "health.json", 0o644)
    env = {k: v for k, v in os.environ.items() if not k.startswith("HWLOCK_")}
    env.update(HWLOCK_PUBLISH_PORT=str(port), HWLOCK_BIND="127.0.0.1", HWLOCK_TOKEN="", HWLOCK_ADMIN_TOKEN="",
               HWLOCK_IMAGE="hwlock:test", HWLOCK_CONFIG_DIR=str(cfgdir),
               HWLOCK_HEALTH_CONFIG="/config/health.json")

    def compose(*args, check=True):
        return subprocess.run(["docker", "compose", "-p", "hwlock-test-health", "-f", str(ROOT / "compose.yaml"),
                               *args], cwd=ROOT, env=env, capture_output=True, text=True, check=check, timeout=300)

    compose("down", "-v", check=False)
    try:
        compose("up", "-d", "--build")
        api = Api(f"http://127.0.0.1:{port}")

        def up():
            try:
                return api.get("/health", timeout=1)[0] == 200
            except OSError:
                return False

        wait_for(up, timeout=60, step=0.3)
        wait_for(lambda: health_state(api, "self") == "up", timeout=30)
        code, out = api.get("/r/self/health")
        assert code == 200 and out["health"]["probes"][0]["ok"]
    finally:
        compose("down", "-v", check=False)

# hwlock

hwlock makes AI subagents and humans take turns on shared hardware: a Jetson, a GPU box, a robot or a test rig.

Each named resource has one holder, a first-in-first-out (FIFO) queue of waiters, and leases that expire on their own.

- **Server:** one small process (Python standard library and SQLite in WAL mode) that runs in Docker.
- **Client:** a single file, `client/hwlock`, that runs on any Python from 3.8 up. It has no dependencies, so it also runs on a Jetson.
- **Licence:** MIT. There are no third-party runtime dependencies. `pytest` (MIT) is used only for the tests.

```text
agent A ──acquire jetson──▶ ┌──────────────┐   holder: A (lease 30 min, renewable)
agent B ──acquire jetson──▶ │ hwlock :8770 │   queue : B, C   (long-poll, FIFO)
human   ──status──────────▶ └──────────────┘   audit log, HTML page at /
```

## Quick start

```bash
git clone git@github.com:gandalf15/hwlock.git && cd hwlock
docker compose up -d                       # serves http://127.0.0.1:8770 (this machine only)
install -m755 client/hwlock ~/.local/bin/  # or copy the single file anywhere

hwlock acquire jetson my-job "TensorRT build"   # waits its turn, prints the queue position
hwlock renew   jetson my-job                     # extends the lease for long work
hwlock release jetson my-job
hwlock run     jetson my-job "bench" -- ssh jetson ./bench.sh   # acquire, auto-renew, always release
hwlock status                                    # every resource, holder, lease left, queue
```

Open <http://127.0.0.1:8770/> for a status page that refreshes every 3 s.

## Client reference

```text
hwlock acquire <resource> <owner> [reason] [--lease S] [--max-wait S]
hwlock renew   <resource> <owner> [--lease S]
hwlock release <resource> <owner>
hwlock status  [resource] [--json]
hwlock run     <resource> <owner> [reason] [--lease S] [--max-wait S] -- <cmd...>
hwlock hold    <resource> <owner> [reason] --until-file F [--lease S] [--max-wait S]
hwlock events  [resource] [--limit N] [--json]
hwlock force-release <resource> <by> [reason]      # admin, logged
```

| Exit | Meaning |
|---|---|
| 0 | Success. |
| 1 | You are not the holder (`renew`/`release`), or the resource is not held (`force-release`). The output is `NOT_HOLDER holder=<owner>`. |
| 2 | Usage error. |
| 3 | Gave up after `--max-wait`. The ticket was withdrawn. |
| 4 | The server is unreachable, or it rejected the request: wrong token or invalid input. |
| 130 | Interrupted while waiting (Ctrl-C or SIGTERM). The ticket was withdrawn. |
| `run` | The command's own exit code, or 128+N if the command was killed by signal N. 127 if it could not be started. |

### Commands

- **`acquire`** takes a ticket. The first request returns at once with a ticket id. After that, the client waits by long-poll: the server holds each request for up to 25 s and answers early when the queue position or the holder changes. Nobody polls fast.
  - It prints the position to stderr only when the position or the holder changes.
  - It exits once you hold the lock, and the lease starts counting.
  - If you already hold the resource under the same owner name, `acquire` succeeds at once and refreshes the lease.
- **`run`** acquires the lock, then renews it every lease/3 in a background thread while `<cmd>` runs. It releases the lock when the command exits, whether it succeeded, failed or was interrupted. SIGTERM and SIGHUP are forwarded to the command, and so is SIGINT if a terminal Ctrl-C has not already reached it.
- **`hold`** acquires the lock and keeps renewing it until file `F` exists (or until Ctrl-C/SIGTERM), then releases it. Use it for interactive sessions or for jobs you cannot wrap with `run`:
  `hwlock hold rig marcel "manual debugging" --until-file /tmp/rig.done & … ; touch /tmp/rig.done`
- **Owner names** match `[A-Za-z0-9._-]{1,64}`. Make each one unique per job, for example `quant-exp-3` or `ci-1234`. Renew and release are checked against the owner name.

Environment variables:

- `HWLOCK_URL`: the server address. The default is `http://127.0.0.1:8770`.
- `HWLOCK_TOKEN`: the bearer token, if the server has one set.

### bash/curl fallback

`client/hwlock.sh` covers `acquire`, `renew`, `release` and `status` with only bash and curl. Use it on machines that have no Python at all:

```bash
HWLOCK_URL=http://pc:8770 client/hwlock.sh acquire jetson job-1 "deploy" 1800
client/hwlock.sh release jetson job-1
```

The raw HTTP calls, if you want to script them yourself:

```bash
curl -s -XPOST $HWLOCK_URL/r/jetson/acquire -d '{"owner":"job-1","reason":"x","wait":0}'  # 200 acquired | 202 {"ticket":N,...}
curl -s -XPOST $HWLOCK_URL/r/jetson/acquire -d '{"owner":"job-1","ticket":N,"wait":25}'  # repeat until 200
curl -s -XPOST $HWLOCK_URL/r/jetson/renew   -d '{"owner":"job-1"}'
curl -s -XPOST $HWLOCK_URL/r/jetson/release -d '{"owner":"job-1"}'                       # 409 if not holder
```

## Semantics

- **Resources** are created the first time someone acquires them, for example `jetson`, `gpu0` or `robot-arm`. Each resource has at most one holder, which the database schema enforces: the holder is stored as columns on the resource row.
- **FIFO.** Tickets get increasing ids, and only the first waiting ticket can be granted. The grant happens only while that ticket's own client has a request in flight with its connection still open. A waiter that has died is never handed the lock.
- **Leases.** The default lease is 1800 s (`HWLOCK_DEFAULT_LEASE_S`). You can override it per call with `--lease`, up to `HWLOCK_MAX_LEASE_S`, which defaults to 24 h. When a holder's lease expires, the holder is evicted and an `expire` event is logged. `renew` sets the lease to *now + lease*.
- **Heartbeats.** Each acquire request a waiting ticket makes counts as its heartbeat, including the long-poll requests. A ticket with no heartbeat for `HWLOCK_HEARTBEAT_S` (120 s) is dropped (`drop` event). A client that crashes while waiting therefore loses its place.
  - If the client is killed during a long-poll, its ticket is dropped as soon as it is its turn (`abandon` event).
  - If the client is stopped with Ctrl-C or SIGTERM, it withdraws its ticket at once.
- **Restarts.** The holder, the queue and the audit log are stored in SQLite.
  - Leases are absolute server timestamps, so they keep counting while the server is down.
  - On startup, every waiting ticket gets a fresh heartbeat window.
  - Waiting clients retry with backoff (1 s growing to 5 s) and re-attach with their ticket id, keeping their place in the queue.
- **Clock.** The server's clock is the only one that is used. Clients send durations, never timestamps.
- **Edge cases.**
  - The owner re-acquiring while holding gets the lock back at once and the lease is refreshed (`reacquire` event).
  - `release` or `renew` from a non-holder returns 409.
  - `force-release` (admin) is logged with who did it and why.
  - Cancelling a ticket that was granted in the meantime releases the lock.

## Server

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness check. It never needs a token. |
| `GET /` | HTML status page that refreshes every 3 s. With a token set, open `/?token=…` (a browser cannot send a bearer header). |
| `GET /status`, `GET /status/<resource>` | JSON: holder (owner, reason, host, pid, since, lease left) and queue. |
| `GET /events?resource=&after=&limit=` | Audit log, with these event kinds: `create`, `enqueue`, `acquire`, `reacquire`, `renew`, `release`, `expire`, `drop`, `abandon`, `cancel`, `force_release`, `server_start`. |
| `POST /r/<res>/acquire` | Body `{owner, reason?, host?, pid?, lease?, ticket?, wait?}`. Returns 200 acquired, 202 waiting or 410 ticket gone. |
| `POST /r/<res>/renew` | Body `{owner, lease?}`. Returns 200, or 409 not holder. |
| `POST /r/<res>/release` | Body `{owner}`. Returns 200, or 409 not holder. |
| `POST /r/<res>/cancel` | Body `{ticket, owner?}`. |
| `POST /r/<res>/force-release` | Body `{by, reason?}`. Needs `HWLOCK_ADMIN_TOKEN` if that is set. |

Settings, all optional:

- `HWLOCK_TOKEN`: the bearer token for every call except `/health`.
- `HWLOCK_ADMIN_TOKEN`: the token for `force-release`.
- `HWLOCK_DEFAULT_LEASE_S`: default lease, 1800.
- `HWLOCK_MAX_LEASE_S`: maximum lease, 86400.
- `HWLOCK_HEARTBEAT_S`: heartbeat window for waiting tickets, 120. It must be at least `HWLOCK_POLL_S` + 5.
- `HWLOCK_POLL_S`: how long the server holds a long-poll, 25.
- `HWLOCK_SWEEP_S`: how often expired leases and dead tickets are checked, 1.
- `HWLOCK_DB`, `HWLOCK_HOST`, `HWLOCK_PORT`: where the database lives and where the server listens.

### Docker

`docker compose up -d` builds a `python:3.12-slim` image that runs as a non-root user (uid 10001). It keeps the database in the named volume `hwlock-data`, restarts with the `unless-stopped` policy and has a healthcheck.

By default the port is bound to **127.0.0.1:8770**. To serve it on the LAN, set a token first:

```bash
echo "HWLOCK_TOKEN=$(openssl rand -hex 16)" >> .env
echo "HWLOCK_BIND=0.0.0.0" >> .env      # all interfaces; a single LAN IP would stop 127.0.0.1 answering
docker compose up -d
```

Without Docker: `python3 -m hwlock_server --db ~/.local/share/hwlock.db` (Python 3.12 or later, no packages needed).

## Using it with AI agents

### Snippet for CLAUDE.md / AGENTS.md (paste and adapt)

```markdown
## Shared hardware: take the lock (hwlock)
- `<resource>` (e.g. `jetson`: ssh user@host) is shared by every agent, subagent, workflow and human.
  Never use it without holding its lock: no ssh commands, deploys, benchmarks, model loads, builds or live tests.
- Take it: `hwlock acquire <resource> <job-name> "<what for>"`. It waits its turn in FIFO order and prints the
  queue position; do not poll, sleep-loop or pgrep for other jobs instead. Use a unique job name per task.
- Release it as soon as you go back to local work: `hwlock release <resource> <job-name>`.
  Acquire again later if you need it again; do not sit on it while you think, edit or wait for a human.
- Long work: `hwlock renew <resource> <job-name>` at least every 10 min (lease 30 min), or wrap one command:
  `hwlock run <resource> <job-name> "<what for>" -- <command>` (auto-renew, always releases).
- `hwlock status` shows who holds what and why. Never `force-release` someone else's lock; ask the user.
- If you are told you lost the lock (lease expired), stop using the hardware and acquire again.
```

### Claude Code skill

`skills/hwlock/SKILL.md` is a ready-made skill. Copy it into a project as `.claude/skills/hwlock/SKILL.md`, or into `~/.claude/skills/hwlock/` for all projects. Claude then loads it whenever a task touches the shared hardware.

### Examples

- [`examples/ssh-jetson.sh`](examples/ssh-jetson.sh): ssh to a Jetson under the lock: one-shot with `run`, and a multi-step session with acquire, renew and release.
- [`examples/workflow-subagents.md`](examples/workflow-subagents.md): how many workflow subagents share one device. Each holds the lock only for its hardware phase.
- [`examples/jetson-lock`](examples/jetson-lock): a drop-in wrapper that keeps the verbs of an older, project-specific lock script ([docs/migrate-andrea.md](docs/migrate-andrea.md)).

## Development

```bash
make venv && make test   # unit, API, 50-client concurrency, CLI, shell clients, docker restart test
make test-fast           # no docker
```

The docker test uses the compose project `hwlock-test` on a random port, so it never touches a deployed instance. CI runs everything on Python 3.12, and runs the CLI tests a second time with the client on Python 3.8.

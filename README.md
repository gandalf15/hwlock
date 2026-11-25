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
hwlock acquire <resource> <owner> [reason] [--lease S] [--max-wait S] [--max-hold S] [--new-ticket]
hwlock renew   <resource> <owner> [--lease S]
hwlock release <resource> <owner>
hwlock status  [resource] [--json]
hwlock run     <resource> <owner> [reason] [--lease S] [--max-wait S] [--max-hold S] [--new-ticket] [--kill-on-preempt] -- <cmd...>
hwlock hold    <resource> <owner> [reason] --until-file F [--lease S] [--max-wait S] [--max-hold S] [--new-ticket]
hwlock events  [resource] [--limit N] [--json]
hwlock force-release <resource> <by> [reason]      # admin, logged
```

| Exit | Meaning |
|---|---|
| 0 | Success. |
| 1 | You are not the holder (`renew`/`release`), or the resource is not held (`force-release`). The output is `NOT_HOLDER holder=<owner>`. `hold` also exits 1 when its lease was lost for another reason than the max hold. |
| 2 | Usage error. |
| 3 | Gave up after `--max-wait`. The ticket was withdrawn. |
| 4 | The server is unreachable, or it rejected the request: wrong token or invalid input. |
| 5 | `OVER_MAX_HOLD`: the [starvation guard](#starvation-guard) refused `renew` or a re-`acquire`, because you have held the resource past its max hold while others wait. `renew` prints `OVER_MAX_HOLD waiting=N evict_in=S`. `hold` exits 5 once it lost the lock that way, and `run --kill-on-preempt` exits 5 after stopping the command. |
| 130 | Ctrl-C (SIGINT) while waiting. The ticket was withdrawn: you gave up your place. |
| 143 | SIGTERM while waiting, for example a tool timeout or `timeout`. The ticket was **kept**: it is dormant for the server's re-attach window (120 s), and re-running the same `acquire` within it gets your place back. See [Keeping your place](#keeping-your-place). |
| `run` | The command's own exit code, or 128+N if the command was killed by signal N. 127 if it could not be started. |

### Commands

- **`acquire`** takes a ticket. The first request returns at once with a ticket id. After that, the client waits by long-poll: the server holds each request for up to 25 s and answers early when the queue position or the holder changes. Nobody polls fast.
  - It prints the position to stderr only when the position or the holder changes.
  - It exits once you hold the lock, and the lease starts counting.
  - If you already hold the resource under the same owner name, `acquire` succeeds at once and refreshes the lease.
- **`run`** acquires the lock, then renews it every lease/3 in a background thread while `<cmd>` runs. It releases the lock when the command exits, whether it succeeded, failed or was interrupted. SIGTERM and SIGHUP are forwarded to the command, and so is SIGINT if a terminal Ctrl-C has not already reached it.
  - If the starvation guard refuses a renew, `run` stops renewing and prints `WARNING: OVER_MAX_HOLD: …` with the time left until the next waiter gets the lock.
  - **Default:** the command keeps running, and `run` returns its exit code. You are warned that the command may still be running after the lock has gone, and at the end that you should check its results. This is the safe choice for work that must not be cut off halfway, such as flashing.
  - **`--kill-on-preempt`:** `run` sends SIGTERM to the command at once, then SIGKILL 10 s later, or 1 s before the eviction if that comes sooner. The command is gone before the next holder gets the device, and `run` exits 5. It does the same if the lock is lost in any other way.
- **`hold`** acquires the lock and keeps renewing it until file `F` exists (or until Ctrl-C/SIGTERM), then releases it. Use it for interactive sessions or for jobs you cannot wrap with `run`:
  `hwlock hold rig marcel "manual debugging" --until-file /tmp/rig.done & … ; touch /tmp/rig.done`
  A `hold` is exactly the kind of proxy that can sit on a device while nothing uses it. When the starvation guard refuses its renew, it stops renewing, warns, and exits 5 once the next waiter has the lock.
- **Keeping your place.** <a id="keeping-your-place"></a>AI agents' shell commands get killed by tool timeouts and are simply run again. So a waiting client that is stopped with SIGTERM, or killed outright, does not give up its place:
  - Its ticket turns **dormant** on the server for `HWLOCK_REATTACH_S` (120 s). The Python client also tells the server at once, prints `ticket N kept for 2m00s`, and exits 143.
  - A new `acquire` (or `run`/`hold`) with the **same resource and owner** within that window re-attaches to that ticket (`re-attached to ticket N` on stderr) and is back at its old place among the tickets not yet served. If the lock went to people behind it meanwhile, that is fine: it simply comes next.
  - A dormant ticket is skipped while dormant: the next connected waiter is served, and a free lock is never held up by it.
  - After the window it is dropped (`ticket_dropped` event), and the next `acquire` joins at the back.
  - Ctrl-C (SIGINT) still gives the place up (the ticket is cancelled, exit 130). `--new-ticket` drops any old ticket of this owner and joins at the back.
- **`--max-hold S`** (`acquire`, `run`, `hold`) asks for a shorter max hold than the server's for this hold. The server caps it at its own limit, so you can lower it but not raise it.
- **Owner names** match `[A-Za-z0-9._-]{1,64}`. Make each one unique per job, for example `quant-exp-3` or `ci-1234`. Renew and release are checked against the owner name, and so is re-attaching: two clients that wait at the same time with the same owner on one resource share one ticket. When it is granted, both get the lock (as a re-acquire by the holder would), and a Ctrl-C in either one cancels the shared ticket.

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
- **FIFO.** Tickets get increasing ids, and only the first live (not dormant) waiting ticket can be granted. The grant happens only while that ticket's own client has a request in flight with its connection still open. A waiter that has died is never handed the lock.
- **Leases.** The default lease is 1800 s (`HWLOCK_DEFAULT_LEASE_S`). You can override it per call with `--lease`, up to `HWLOCK_MAX_LEASE_S`, which defaults to 24 h. When a holder's lease expires, the holder is evicted and an `expire` event is logged. `renew` sets the lease to *now + lease*.
- **Heartbeats.** Each acquire request a waiting ticket makes counts as its heartbeat, including the long-poll requests. A live ticket with no heartbeat for `HWLOCK_HEARTBEAT_S` (120 s) is dropped (`drop` event).
  - If the client is killed during a long-poll, the server notices the closed connection within about 1 s and makes the ticket **dormant** at once (`abandon` and `ticket_dormant` events), whatever its position. A dormant ticket keeps its place for `HWLOCK_REATTACH_S` (120 s) but is skipped, so it never blocks the queue or a free lock; see [Keeping your place](#keeping-your-place). Only a client killed in the few milliseconds between two polls waits for the heartbeat timeout instead.
  - Clients must keep the connection open while they wait: a client that half-closes its socket after sending the request counts as gone.
  - SIGTERM: the client detaches (`POST /detach`, ticket dormant, `ticket_dormant` event with `via: detach`) and exits 143. Ctrl-C: it withdraws the ticket at once (`cancel`) and exits 130.
  - Re-attaching: an acquire without a ticket id finds the owner's dormant (or live) ticket on that resource (`ticket_reattached` event); an acquire with the id of a dormant ticket revives it too. With `HWLOCK_REATTACH_S=0` a hung-up waiter's ticket is dropped at once, as before.
  - The killed process and its re-run share owner and ticket id, so each client process also sends its own random `attach` id. The newest client of a ticket (the re-run) takes it over: a late `detach` or `cancel` from the old process, or the server noticing the old connection close, then changes nothing (`"state": "superseded"`). Without that, an old client's detach still in flight could release the lock the re-run had just been granted. Requests without an `attach` id (older clients) act as before.
- **Restarts.** The holder, the queue and the audit log are stored in SQLite.
  - Leases are absolute server timestamps, so they keep counting while the server is down.
  - On startup, every live waiting ticket gets a fresh heartbeat window. Dormant tickets keep their re-attach deadline, which, like a lease, keeps counting while the server is down; one that ran out meanwhile is dropped on startup.
  - Waiting clients retry with backoff (1 s growing to 5 s) and re-attach with their ticket id, keeping their place in the queue.
- **Starvation guard.** <a id="starvation-guard"></a>Leases only protect against holders that crash. They do not protect against a holder that keeps renewing blindly while doing nothing, which is what happened on 2026-11-25: an idle proxy hold kept the lock while three jobs queued. So, **while at least one ticket waits**, a holder may keep a resource at most its *max hold* (default 7200 s), counted from when it took it (`since`).
  - After that, `renew` and re-`acquire` are refused with 409 `over_max_hold` (a `renew_refused` event).
  - The holder is evicted at `max(since + max hold, arrival of the oldest waiter) + grace`, with a default grace of 300 s, and the next waiter gets the lock (an `evict_max_hold` event). Taking the later of the two means a holder that was alone past its max hold still gets the full grace once someone starts waiting.
  - **Nobody waiting means no limit.** A waiter whose client died is dropped first (or is dormant) and never causes an eviction. A ticket that re-attaches counts as waiting from its re-attach, so the holder still gets the full grace.
  - `since` and the waiters' arrival times are stored in SQLite, so the deadlines survive a server restart. A re-`acquire` by the holder does not reset them.
  - The limits can be set globally, per resource, and lowered per acquire with `max_hold`; see the settings below. `GET /status` shows them as `holder.guard`: `{max_hold_s, grace_s}`, plus, while people wait, `waiting`, `refuse_at`, `evict_at`, `refuse_in_s` and `evict_in_s`. The HTML page and `hwlock status` show the deadlines whenever someone waits.
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
| `GET /status`, `GET /status/<resource>` | JSON: holder (owner, reason, host, pid, since, lease left), `queue` (live waiters) and `dormant` (tickets whose client is gone, with `dormant_left_s` and `position_on_reattach`). |
| `GET /events?resource=&after=&limit=` | Audit log, with these event kinds: `create`, `enqueue`, `acquire`, `reacquire`, `renew`, `release`, `expire`, `drop`, `abandon`, `ticket_dormant`, `ticket_reattached`, `ticket_dropped`, `cancel`, `force_release`, `renew_refused`, `evict_max_hold`, `server_start`. |
| `POST /r/<res>/acquire` | Body `{owner, reason?, host?, pid?, lease?, max_hold?, ticket?, new_ticket?, attach?, wait?}`. Returns 200 acquired, 202 waiting or 410 ticket gone. Without `ticket` it re-attaches to the owner's dormant or waiting ticket (`"reattached": true`); `new_ticket: true` drops those and joins at the back. Also 409 `over_max_hold` when the holder re-acquires past its max hold while others wait. |
| `POST /r/<res>/renew` | Body `{owner, lease?}`. Returns 200, 409 `not_holder`, or 409 `over_max_hold` with `{message, waiting, held_s, max_hold_s, grace_s, evict_at, evict_in_s, lease_left_s}`. |
| `POST /r/<res>/release` | Body `{owner}`. Returns 200, or 409 not holder. |
| `POST /r/<res>/cancel` | Body `{ticket, owner?, attach?}`. Gives the place up (also for a dormant ticket). With `attach`, only when it is the ticket's (or grant's) newest client; otherwise `"state": "superseded"` and nothing changes. |
| `POST /r/<res>/detach` | Body `{ticket?, owner?, attach?}`. The client is leaving but keeps its place: the ticket turns dormant for the re-attach window. If that ticket was granted meanwhile, the lock is released. `attach` as for cancel. |
| `POST /r/<res>/force-release` | Body `{by, reason?}`. Needs `HWLOCK_ADMIN_TOKEN` if that is set. |

Settings, all optional:

- `HWLOCK_TOKEN`: the bearer token for every call except `/health`.
- `HWLOCK_ADMIN_TOKEN`: the token for `force-release`.
- `HWLOCK_DEFAULT_LEASE_S`: default lease, 1800.
- `HWLOCK_MAX_LEASE_S`: maximum lease, 86400.
- `HWLOCK_HEARTBEAT_S`: heartbeat window for waiting tickets, 120. It must be at least `HWLOCK_POLL_S` + 5.
- `HWLOCK_POLL_S`: how long the server holds a long-poll, 25.
- `HWLOCK_REATTACH_S`: how long a waiter that hung up or detached keeps its place as a dormant ticket, 120. 0 drops it at once.
- `HWLOCK_MAX_HOLD_S`: the [starvation guard](#starvation-guard)'s max hold, 7200. Set it to 0 to turn the guard off.
- `HWLOCK_HOLD_GRACE_S`: the grace period before an over-max holder is evicted, 300.
- `HWLOCK_RESOURCE_MAX_HOLD_S`, `HWLOCK_RESOURCE_HOLD_GRACE_S`: per-resource values that override the two above, as `resource=seconds` pairs separated by commas, for example `jetson=7200,robot-arm=0`. The server logs the values it uses at startup.
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
  If a tool timeout kills the waiting acquire, just run the same command again within 2 min: it keeps its place.
  Ctrl-C (or `--new-ticket`) gives the place up.
- Release it as soon as you go back to local work: `hwlock release <resource> <job-name>`.
  Acquire again later if you need it again; do not sit on it while you think, edit or wait for a human.
- Long work: `hwlock renew <resource> <job-name>` at least every 10 min (lease 30 min), or wrap one command:
  `hwlock run <resource> <job-name> "<what for>" -- <command>` (auto-renew, always releases).
- `hwlock status` shows who holds what and why. Never `force-release` someone else's lock; ask the user.
- If you are told you lost the lock (lease expired), stop using the hardware and acquire again.
- Never keep the lock for work that is not using the device (no idle `hold`, no blind renew loop). While others
  wait, a hold ends after 2 h: `OVER_MAX_HOLD` (exit 5) means finish the step, release, and queue again.
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

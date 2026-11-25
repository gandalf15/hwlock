# hwlock

hwlock makes AI subagents and humans take turns on shared hardware: a Jetson, a GPU box, a robot or a test rig.

Each named resource has one holder, a first-in-first-out (FIFO) queue of waiters, and leases that expire on their own.
Optionally, the server also watches each device's [health](#hardware-health) (alive, low on memory, rebooted), stops
handing out a dead device, and tells waiting and holding clients at once.

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
hwlock acquire <resource> <owner> [reason] [--lease S] [--max-wait S] [--max-hold S] [--new-ticket] [--fail-if-down] [--exit-on-degraded]
hwlock renew   <resource> <owner> [--lease S]
hwlock release <resource> <owner>
hwlock status  [resource] [--json]
hwlock run     <resource> <owner> [reason] [--lease S] [--max-wait S] [--max-hold S] [--new-ticket] [--kill-on-preempt]
               [--on-down kill|warn] [--fail-if-down] [--exit-on-degraded] -- <cmd...>
hwlock hold    <resource> <owner> [reason] --until-file F [--lease S] [--max-wait S] [--max-hold S] [--new-ticket] [--fail-if-down] [--exit-on-degraded]
hwlock watch   <resource> [--until up|down|degraded|rebooted] [--timeout S] [--json]
hwlock check   <resource> [--rebooted-within S] [--json]   # one-shot: 0 up, 8 degraded, 6 down/unknown, 7 rebooted
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
| 6 | The device is **DOWN** (only for resources with [health probes](#hardware-health)): `acquire --fail-if-down` gave up (ticket withdrawn), `run` stopped the command, `hold` ended (lock released), `watch` saw it go down. |
| 7 | The device **REBOOTED**: your lease was voided (`run` stopped the command, `hold` ended; `renew`/`release` print `VOIDED_REBOOT`), or `watch` saw a reboot. Whatever you set up on the device is gone. |
| 8 | The device is **DEGRADED** (low memory, hot, exporter unreachable): only with `--exit-on-degraded`, or `watch` saw it. |
| 130 | Ctrl-C (SIGINT) while waiting. The ticket was withdrawn: you gave up your place. |
| 143 | SIGTERM while waiting, for example a tool timeout or `timeout`. The ticket was **kept**: it is dormant for the server's re-attach window (120 s), and re-running the same `acquire` within it gets your place back. See [Keeping your place](#keeping-your-place). |
| `run` | The command's own exit code, or 128+N if the command was killed by signal N. 127 if it could not be started. 6/7/8 if it was stopped for the device's health. |

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
- **`watch`** blocks until the next health change of a resource (or, with `--until STATE`, until it is in that state), prints it and exits with its code: 0 back up, 6 down, 7 rebooted, 8 degraded, 3 after `--timeout`. Agents start it as a background command: its exit is the notification. See [Hardware health](#hardware-health).
- **`--max-hold S`** (`acquire`, `run`, `hold`) asks for a shorter max hold than the server's for this hold. The server caps it at its own limit, so you can lower it but not raise it.
- **Owner names** match `[A-Za-z0-9._-]{1,64}`. Make each one unique per job, for example `quant-exp-3` or `ci-1234`. Renew and release are checked against the owner name, and so is re-attaching: two clients that wait at the same time with the same owner on one resource share one ticket. When it is granted, both get the lock (as a re-acquire by the holder would), and a Ctrl-C in either one cancels the shared ticket.

Environment variables:

- `HWLOCK_URL`: the server address. The default is `http://127.0.0.1:8770`.
- `HWLOCK_TOKEN`: the bearer token, if the server has one set.

### bash/curl fallback

`client/hwlock.sh` covers `acquire`, `renew`, `release` and `status` with only bash and curl. Use it on machines that have no Python at all. It does not act on health (no exit 6/7/8, no `watch`); while a device is down its `acquire` simply keeps waiting, since the server grants nothing then:

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

## Hardware health

A lock does not help when the device itself dies. On 2026-11-25 the 8 GB Jetson ran out of memory and froze for over
30 minutes: ssh accepted the TCP connection but never answered. Jobs kept waiting for a dead board, the holder's work was
lost when it was rebooted by hand, and nobody was told. So the server can probe each device and act on what it sees.

**States** per resource: `up`, `degraded`, `down`, `unknown` (not decided yet), plus a `rebooted` event when the device's
boot id changes.

**Configuration**: a JSON file (JSON is also valid YAML) named by `HWLOCK_HEALTH_CONFIG`. In Docker, put it in
`config/health.json` (gitignored; start from [`config/health.example.json`](config/health.example.json)) and set
`HWLOCK_HEALTH_CONFIG=/config/health.json` in `.env`; compose mounts `HWLOCK_CONFIG_DIR` (default `./config`) read-only at
`/config`. A broken file stops the server at startup, with the reason in its log. Resources without probes behave as before.

```json
{"resources": {"jetson": {
  "interval_s": 5, "down_after": 3, "up_after": 2, "degraded_after": 2,
  "probes": [
    {"type": "ssh_banner", "host": "192.168.0.99", "port": 22, "timeout_s": 3},
    {"type": "http_json", "url": "http://192.168.0.99:8771/", "timeout_s": 3,
     "degraded_if": {"mem_available_mb": "<800", "temp_max_c": ">85"}}]}}}
```

| Probe | Passes when | On failure (`on_fail` default) |
|---|---|---|
| `ssh_banner` `host`, `port` (22) | TCP connects **and** the `SSH-` banner arrives within `timeout_s`. A frozen box accepts TCP in its kernel but sshd never speaks: a plain TCP check would call it alive. | `down` |
| `tcp` `host`, `port` | TCP connects. | `down` |
| `http_json` `url` | `GET` answers 200 with a JSON object within `timeout_s` (one deadline for the whole exchange). Reads `boot_id` and `uptime_s` (keys: `boot_id_key`, `uptime_key`) and checks `degraded_if` thresholds (`<`, `<=`, `>`, `>=`; dotted keys for nested values). | `degraded` (the box answers ssh but its exporter does not) |
| `command` `argv` (list) or `cmd` (shell) | Exit 0 within `timeout_s`. Runs **in the server's container**. | `down` |

Every `interval_s` (5 s) all probes of a resource run in parallel. A round is `down` if a probe with `on_fail: down`
failed, `degraded` if one with `on_fail: degraded` failed or a threshold was crossed, `up` otherwise. The state changes
only after several rounds in a row (hysteresis): `down` after `down_after` (3) down rounds, back from `down` after
`up_after` (2) rounds that are not down (to the latest round's level), `up` to `degraded` after `degraded_after` (2)
degraded rounds, and back after `up_after` clean rounds. With the defaults a dead box is `down` within about 15 to 20 s.

**What happens**
- Every transition is an event (`health_up`, `health_degraded`, `health_down`), and the server ends every waiting
  client's long-poll at once, so `acquire` prints the change right away (`WARNING: jetson is DOWN (for 12s): ssh_banner
  192.168.0.99:22: TCP accepted but no SSH banner within 3 s (frozen?)`).
- **While a device is down nobody is granted it**, nor while it is still `unknown` (a newly configured resource, until
  its first probes decide: a few rounds), so a dead box is never handed out before it is known to be dead. The queue
  keeps its order and moves on when the device is back.
  `acquire` keeps waiting and says so; `--fail-if-down` gives up instead (ticket withdrawn, exit 6). A holder keeps its
  lock (and lease) while the device is down: `run` and `hold` end themselves (below).
- **Reboot**: when an `http_json` probe reports a new `boot_id`, the server logs `rebooted` and voids the holder's
  lease (`lease_voided_reboot`): its work on the device is gone. Only a lock taken before that boot is voided (one taken
  after it, for example while the exporter was still starting, is kept). Waiters keep their tickets and FIFO order and
  are told. The voided holder's `renew`/`release` answer 409 with `voided` (`VOIDED_REBOOT`, exit 7), not a plain
  `NOT_HOLDER`. A reboot is only seen through a boot id: without an exporter, a quick reboot looks like a short outage.
- **Holders** (`run`, `hold`) long-poll `GET /r/<res>/health` next to their renewals, so they learn at once:
  - `run`: on down or reboot it stops the command (SIGTERM, SIGKILL 10 s later) and exits 6 or 7. `--on-down=warn` only
    warns and returns the command's code. It releases the lock on exit (not needed after a reboot: the lease is void).
  - `hold`: ends at once, exit 6 (lock released) or 7.
- **Degraded** is a warning by default (the device works but is short of memory, hot, or its exporter is gone).
  `--exit-on-degraded` (`acquire`, `run`, `hold`) treats it like down, with exit 8.
- `renew` prints a warning when the device is down or degraded and still exits 0: the holder decides.
- **Persistence**: the state, its start and the last boot id are stored in SQLite, so a server restart neither
  forgets a down device (still no grants right after the restart) nor reports a false reboot, and a reboot that
  happened while the server was down is still noticed. The latest metrics are kept in memory only.
- `hwlock status`, `GET /status` and the page at `/` show each device's state, why, and its latest metrics (memory,
  load, temperatures, GPU load, uptime, power mode) and last reboot. `GET /r/<res>/health` returns the same with the
  probe results.

### Exporter

[`exporter/hwlock-exporter`](exporter/hwlock-exporter) is a tiny read-only HTTP endpoint for any Linux box: one Python
file (3.7 or later, standard library only), no secrets, no writes, no commands run. `GET /` returns JSON:
`boot_id` (`/proc/sys/kernel/random/boot_id`), `uptime_s`, `mem_total_mb`, `mem_available_mb`, `swap_*_mb`,
`load1/5/15`, `ncpu`, `temps_c` (every `/sys/class/thermal` zone by type), `temp_max_c`, `cpu_temp_c`, `gpu_temp_c`,
`gpu_load_pct` (Jetson: `/sys/devices/platform/**/…gpu/load`, per mille), `nvpmodel` (Jetson power mode, from
`/var/lib/nvpmodel`), and `psi_mem_*_avg10` where the kernel has pressure stall information.

```bash
exporter/install.sh --ssh marcel@192.168.0.99 --allow 192.168.0.0/24,127.0.0.0/8   # LAN + loopback only
exporter/install.sh                                       # on the box itself, answering everyone who reaches it
exporter/uninstall.sh --ssh marcel@192.168.0.99
```

The installer puts it in `~/.local/bin`, writes the systemd `--user` unit `hwlock-exporter` (port 8771, all interfaces,
`Restart=always`; binding to one LAN address could fail at boot before the network is up, so limit who may read it with
`--allow` instead: other clients, for example over a VPN or a Docker bridge, get 403; keep `127.0.0.0/8` for the
installer's own check), starts it and checks it answers. A user unit runs while the user's systemd instance does: from boot
with lingering (`loginctl enable-linger`) or a desktop autologin (the Jetson has one), otherwise only after a login.
Check it with `curl http://<box>:8771/`, logs with `journalctl --user -u hwlock-exporter`.

## Server

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness check. It never needs a token. |
| `GET /` | HTML status page that refreshes every 3 s. With a token set, open `/?token=…` (a browser cannot send a bearer header). |
| `GET /status`, `GET /status/<resource>` | JSON: holder (owner, reason, host, pid, since, lease left), `queue` (live waiters), `dormant` (tickets whose client is gone, with `dormant_left_s` and `position_on_reattach`) and `health` (null without probes). Acquire and renew answers also carry `health` (without metrics). |
| `GET /events?resource=&after=&limit=` | Audit log, with these event kinds: `create`, `enqueue`, `acquire`, `reacquire`, `renew`, `release`, `expire`, `drop`, `abandon`, `ticket_dormant`, `ticket_reattached`, `ticket_dropped`, `cancel`, `force_release`, `renew_refused`, `evict_max_hold`, `health_up`, `health_degraded`, `health_down`, `rebooted`, `lease_voided_reboot`, `server_start`. |
| `POST /r/<res>/acquire` | Body `{owner, reason?, host?, pid?, lease?, max_hold?, ticket?, new_ticket?, attach?, wait?}`. Returns 200 acquired, 202 waiting or 410 ticket gone. Without `ticket` it re-attaches to the owner's dormant or waiting ticket (`"reattached": true`); `new_ticket: true` drops those and joins at the back. Also 409 `over_max_hold` when the holder re-acquires past its max hold while others wait. |
| `POST /r/<res>/renew` | Body `{owner, lease?}`. Returns 200, 409 `not_holder`, or 409 `over_max_hold` with `{message, waiting, held_s, max_hold_s, grace_s, evict_at, evict_in_s, lease_left_s}`. |
| `POST /r/<res>/release` | Body `{owner}`. Returns 200, or 409 not holder. |
| `POST /r/<res>/cancel` | Body `{ticket, owner?, attach?}`. Gives the place up (also for a dormant ticket). With `attach`, only when it is the ticket's (or grant's) newest client; otherwise `"state": "superseded"` and nothing changes. |
| `POST /r/<res>/detach` | Body `{ticket?, owner?, attach?}`. The client is leaving but keeps its place: the ticket turns dormant for the re-attach window. If that ticket was granted meanwhile, the lock is released. `attach` as for cancel. |
| `POST /r/<res>/force-release` | Body `{by, reason?}`. Needs `HWLOCK_ADMIN_TOKEN` if that is set. |
| `GET /r/<res>/health?after=SEQ&wait=S` | Health of a resource with probes (404 without): `{health: {state, since, for_s, reason, seq, boot_id, last_reboot, metrics, probes, checked_s_ago}, events, holder}`. With `after`, long-polls up to `wait` s (capped like acquire) until a health event newer than `seq` exists and lists those events. |

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
- `HWLOCK_HEALTH_CONFIG`: the [health](#hardware-health) probe config (JSON file). Unset: no health checks.
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
- Waiting can take long: start `hwlock acquire ...` as a BACKGROUND command and keep working; you are notified when
  it exits. Never loop, sleep or grep over its output file to see whether it is done. Same for
  `hwlock watch <resource>` (exits at the next health change) if you want to hear when the device dies or comes back.
- The device can die. Exit codes: 6 DOWN = stop, report it to the user with the printed reason, do not retry blindly
  (`hwlock watch <resource> --until up` in the background tells you when it is back); 7 REBOOTED = your lease was
  voided and whatever you started on the device is gone: re-acquire, re-check your setup (deployed files, running
  services, loaded models) and redo the interrupted step; 8 DEGRADED (only with `--exit-on-degraded`) = low memory or
  hot: do not start heavy work, report. `hwlock status` shows the reason and the latest metrics.
```

### Claude Code skill

`skills/hwlock/SKILL.md` is a ready-made skill. Copy it into a project as `.claude/skills/hwlock/SKILL.md`, or into `~/.claude/skills/hwlock/` for all projects. Claude then loads it whenever a task touches the shared hardware.

### Examples

- [`examples/ssh-jetson.sh`](examples/ssh-jetson.sh): ssh to a Jetson under the lock: one-shot with `run`, and a multi-step session with acquire, renew and release.
- [`examples/workflow-subagents.md`](examples/workflow-subagents.md): how many workflow subagents share one device. Each holds the lock only for its hardware phase.
- [`examples/jetson-lock`](examples/jetson-lock): a drop-in wrapper that keeps the verbs of an older, project-specific lock script ([docs/migrate-andrea.md](docs/migrate-andrea.md)).

## Development

```bash
make venv && make test   # unit, API, 50-client concurrency, CLI, shell clients, health, docker restart + sshd tests
make test-fast           # no docker
```

The docker test uses the compose project `hwlock-test` on a random port, so it never touches a deployed instance. CI runs everything on Python 3.12, and runs the CLI tests a second time with the client on Python 3.8.

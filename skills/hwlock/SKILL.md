---
name: hwlock
description: Take turns on shared hardware (a Jetson, GPU box, robot, test rig) with other agents, subagents and humans through the hwlock service. Use before ANY use of a shared device - ssh commands, deploys, benchmarks, model loads, builds, flashing, live tests - and to see who is using it (hwlock status). Also use when a lock is lost, a lease must be renewed, or a job waits for hardware.
---

# hwlock: one holder per device, FIFO queue, leases

Shared hardware is used by many agents at once. Two jobs on one device corrupt each other's
measurements, run out of memory or break deploys. The `hwlock` service serialises access.

## Rules
1. **Hold the lock for every use of the device**, including "quick" ssh commands, and only then.
2. **Acquire** (waits in FIFO order; prints the queue position; long-polls - never write your own poll loop):
   `hwlock acquire <resource> <job-name> "<what for>"`
   `<job-name>` must be unique to your task (e.g. `trt-build-fp16`, `bench-run-3`): renew and
   release are checked against it. Add `--max-wait 1800` if you must not wait forever (exit 3 = gave up).
3. **Release as soon as you go back to local work** (editing, analysing results, waiting for a human):
   `hwlock release <resource> <job-name>`. Acquire again when you need the device again.
4. **Long work**: the lease is 30 min by default. Either renew at least every 10 min
   (`hwlock renew <resource> <job-name>`), or wrap a single command so renewal and release are automatic:
   `hwlock run <resource> <job-name> "<what for>" -- ssh user@device '<command>'`
   (`run` releases on success, failure and Ctrl-C, and exits with the command's exit code.)
   Background job you cannot wrap: `hwlock hold <resource> <job-name> "<why>" --until-file /tmp/<job>.done &`
   and `touch /tmp/<job>.done` when finished.
5. **Look, don't guess**: `hwlock status` shows holder, reason, lease left and queue for all devices.
6. **Never** `force-release` another holder's lock, never use the device while someone else holds it,
   and never kill another agent's process to get the device. If something looks stuck, report it to the user.
7. If `renew`/`release` prints `NOT_HOLDER` or `run` warns "lost", your lease expired: stop touching the
   device, re-acquire, and redo the step whose result is now suspect.

## Exit codes
0 ok - 1 not the holder - 2 usage - 3 gave up (`--max-wait`) - 4 server unreachable/rejected
(check `HWLOCK_URL`, `HWLOCK_TOKEN`) - 130 interrupted - `run`: the command's code.

## Setup (only if `hwlock` is not on PATH)
The client is one stdlib-only Python file: `client/hwlock` from the hwlock repository. Server URL
from `HWLOCK_URL` (default `http://127.0.0.1:8770`), token from `HWLOCK_TOKEN`. Do not start your own
server: there must be exactly one per site, or the lock means nothing.

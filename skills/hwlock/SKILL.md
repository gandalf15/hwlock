---
name: hwlock
description: Take turns on shared hardware (a Jetson, GPU box, robot, test rig) with other agents, subagents and humans through the hwlock service. Use before ANY use of a shared device - ssh commands, deploys, benchmarks, model loads, builds, flashing, live tests - and to see who is using it (hwlock status). Also use when a lock is lost, a lease must be renewed, a job waits for hardware, or a device is down, degraded or rebooted (exit 6/7/8, hwlock watch).
---

# hwlock: one holder per device, FIFO queue, leases

Shared hardware is used by many agents at once. Two jobs on one device corrupt each other's
measurements, run out of memory or break deploys. The `hwlock` service serialises access.

## Rules
1. **Hold the lock for every use of the device**, including "quick" ssh commands, and only then.
2. **Acquire** (waits in FIFO order; prints the queue position; the server notifies the client the moment it is your turn - never write your own wait loop):
   `hwlock acquire <resource> <job-name> "<what for>"`
   `<job-name>` must be unique to your task (e.g. `trt-build-fp16`, `bench-run-3`): renew and
   release are checked against it. Add `--max-wait 1800` if you must not wait forever (exit 3 = gave up).
   **Your acquire was killed by a tool timeout (exit 143)?** Just run the very same command again within
   2 min: the server kept your ticket (dormant) and you get your place back ("re-attached to ticket N").
   Do not switch to a new job name for the retry, or you join at the back. Ctrl-C (exit 130) or
   `--new-ticket` gives the place up. Prefer running a long wait in the background so it is not killed.
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
8. **Starvation guard**: while others wait, nobody may keep a device longer than its max hold (default 2 h
   from when you took it). If `renew` prints `OVER_MAX_HOLD` (exit 5), or `run`/`hold` warn about it, you
   will be evicted after a grace period (default 5 min; the message says when). Finish or stop the current
   step within that time, release, and `acquire` again (back of the queue) if you still need the device.
   Do not hold the lock "for" a job that is not using the device: no idle `hold`, no blind renew loop.
   `run` lets the command finish by default (check its results); add `--kill-on-preempt` for commands
   that must not touch the device once someone else has it.

9. **Wait in the background, never poll.** An acquire can wait a long time. Start it as a BACKGROUND command
   (your harness's background/`run_in_background` option) and go on with local work: you are notified when it
   exits, and its exit code tells you what happened. Never loop, `sleep`, `tail` or `grep` over its output file to
   see whether it is done. For a quick look before starting, `hwlock check <resource>` answers at once
   (exit 0 up, 8 degraded, 6 down; it uses the server's probes, so a frozen box that still answers ping is down).
   The same goes for `hwlock watch <resource>`, which blocks until the device's next
   health change: start it in the background if you want to hear when the device dies or comes back.
10. **The device can die.** The server probes it (ssh banner, exporter) and tells waiters and holders at once.
   Nobody is granted a device that is down; `acquire` keeps waiting and prints the state (`--fail-if-down` to
   give up instead). `run` stops the command when the device goes down or reboots; `hold` ends.

## Exit codes and what to do
| Code | Meaning | Do |
|---|---|---|
| 0 | ok | go on |
| 1 | not the holder (lease expired) | stop touching the device, re-acquire, redo the suspect step |
| 2 | usage | fix the command |
| 3 | gave up after `--max-wait` (or `watch --timeout`) | report "device busy", decide with the user |
| 4 | server unreachable/rejected | check `HWLOCK_URL`, `HWLOCK_TOKEN`; never use the device without the lock |
| 5 | `OVER_MAX_HOLD` | finish the step within the grace, release, re-queue |
| 6 | device **DOWN** (`--fail-if-down`, `run`/`hold` stopped, `watch`) | stop. Report it to the user with the printed reason. Do not retry blindly or try to "fix" the box over ssh. `hwlock watch <res> --until up` in the background tells you when it is back |
| 7 | device **REBOOTED**: your lease was voided (`VOIDED_REBOOT`) | everything you set up on it is gone (deployed files in tmpfs, running services, loaded models, partial builds). Re-acquire, re-check your setup on the device, redo the interrupted step |
| 8 | device **DEGRADED** (`--exit-on-degraded`, `watch`): low memory, hot, exporter gone | do not start heavy work (model loads, builds); report. Plain runs only warn |
| 130 | Ctrl-C while waiting | place given up |
| 143 | SIGTERM while waiting (tool timeout) | re-run the very same acquire within 2 min to keep your place |
| `run` | the command's code (6/7/8 if stopped for health) | |

`hwlock status` shows each device's health, why, and its latest metrics (free memory, load, temperatures, GPU).

## Setup (only if `hwlock` is not on PATH)
The client is one stdlib-only Python file: `client/hwlock` from the hwlock repository. Server URL
from `HWLOCK_URL` (default `http://127.0.0.1:8770`), token from `HWLOCK_TOKEN`. Do not start your own
server: there must be exactly one per site, or the lock means nothing.

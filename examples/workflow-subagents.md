# Many workflow subagents, one device

A workflow script fans out N subagents. Each one does mostly local work (reading code, editing,
analysing) and only some hardware work. Give each subagent the hwlock rules (paste the README
snippet into its prompt or into the project's CLAUDE.md / AGENTS.md, or install `skills/hwlock`),
and a unique job name.

## Prompt fragment for each subagent

```text
The Jetson is shared with other subagents. Before any ssh/deploy/benchmark on it run
  hwlock acquire jetson <JOB> "<what for>"
as a BACKGROUND command (it waits its turn; you are notified when it exits; never poll, sleep or
grep its output), and run `hwlock release jetson <JOB>` as soon as your hardware step is done.
Prefer `hwlock run jetson <JOB> "<what for>" -- ssh jetson '<cmd>'` for single commands.
Your job name is <JOB> = sweep-<i>. Never force-release.
If hwlock exits 6 (Jetson DOWN): stop and report it, do not retry. 7 (REBOOTED, lease voided):
re-acquire, re-check your setup on the Jetson, redo the step. 8 (DEGRADED): no heavy work, report.
```

## Pattern

```text
subagent i:
  plan + edit locally                              (no lock)
  hwlock run jetson sweep-i "build" -- ssh jetson 'make engine'
  analyse logs locally                             (no lock)
  hwlock acquire jetson sweep-i "benchmark"
    ssh jetson './bench --runs 5'                  (renew if > 10 min)
  hwlock release jetson sweep-i
  write report                                     (no lock)
```

Why it works:
- Waiting subagents each keep one open request to the server (renewed every ~25 s as a heartbeat) and are notified when
  their turn comes, so a queue of 20 costs no tokens and almost no CPU.
- FIFO order is strict, so no subagent starves; `hwlock status` / the page at `/` shows who waits for what.
- A subagent whose waiting acquire is killed (crash, tool timeout) keeps its place for 120 s: re-running
  the same acquire (same owner) re-attaches to its ticket. Meanwhile the ticket is skipped, so it never
  blocks the others; after 120 s it is dropped. Ctrl-C gives the place up at once. A holder that crashes
  loses the lock when its lease runs out (30 min), so the device never stays locked forever.
- An orchestrator that must stop everything can see holders in `hwlock status --json` and ask
  them to finish; `force-release` is for humans and is logged.
- **When the device dies** (out of memory, frozen, powered off), the server sees it within about 15 s and
  stops handing it out. Waiting subagents are told at once (and keep their place); a subagent's `run` stops
  its command and exits 6; after a reboot the holder's lease is voided (exit 7) and the queue goes on in order.
  Nobody keeps waiting on a dead board without knowing it.

## Being told when the device dies

The orchestrator (or the main session) can run, as a BACKGROUND command,

```bash
hwlock watch jetson            # exits at the next health change: 0 up, 6 down, 7 rebooted, 8 degraded
```

and is notified when it exits; no polling. Run it again after handling the change. To wait for a dead device
to come back: `hwlock watch jetson --until up`.

## Orchestrator-side throttling (optional)

If the orchestrator itself starts device jobs, wrap each one:

```bash
hwlock run jetson "orchestrator-step-$STEP" "nightly matrix" --max-wait 7200 --fail-if-down -- ./device_job.sh
case $? in
  3) echo "device busy for 2 h, skipping";;
  6) echo "Jetson is down: stopping the matrix, tell the user"; exit 6;;
  7) echo "Jetson rebooted mid-step: redeploy, then redo step $STEP";;
esac
```

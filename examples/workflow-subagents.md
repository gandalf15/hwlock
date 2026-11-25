# Many workflow subagents, one device

A workflow script fans out N subagents. Each one does mostly local work (reading code, editing,
analysing) and only some hardware work. Give each subagent the hwlock rules (paste the README
snippet into its prompt or into the project's CLAUDE.md / AGENTS.md, or install `skills/hwlock`),
and a unique job name.

## Prompt fragment for each subagent

```text
The Jetson is shared with other subagents. Before any ssh/deploy/benchmark on it run
  hwlock acquire jetson <JOB> "<what for>"
(it waits its turn; do not poll), and run `hwlock release jetson <JOB>` as soon as your
hardware step is done. Prefer `hwlock run jetson <JOB> "<what for>" -- ssh jetson '<cmd>'`
for single commands. Your job name is <JOB> = sweep-<i>. Never force-release.
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
- Waiting subagents sit in one long-poll each (one request per ~25 s), so a queue of 20 costs nothing.
- FIFO order is strict, so no subagent starves; `hwlock status` / the page at `/` shows who waits for what.
- A subagent that crashes loses its ticket after 120 s and its lease after 30 min (or at once on Ctrl-C),
  so the device never stays locked forever.
- An orchestrator that must stop everything can see holders in `hwlock status --json` and ask
  them to finish; `force-release` is for humans and is logged.

## Orchestrator-side throttling (optional)

If the orchestrator itself starts device jobs, wrap each one:

```bash
hwlock run jetson "orchestrator-step-$STEP" "nightly matrix" --max-wait 7200 -- ./device_job.sh
case $? in 3) echo "device busy for 2 h, skipping";; esac
```

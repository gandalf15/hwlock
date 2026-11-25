# Moving Andrea's `scripts/jetson-lock` to hwlock

The current `andrea_ai/scripts/jetson-lock` keeps its state on the Jetson in `~/.jetson-lock/`. It uses ssh and atomic `mkdir`, and clients poll every 5 s. This guide replaces it with a thin wrapper around the hwlock service, using resource `jetson`. The verbs, arguments and environment variables stay the same, so agents and the CLAUDE.md rule need no retraining.

**Do not do the switch while jobs are using the old lock.** Section 3 explains when and how.

## 1. What is already in place

- The hwlock service runs on the PC as the compose project `hwlock` (`/home/marcel/code/hwlock`). It listens on `http://127.0.0.1:8770`, and the HTML page is at <http://127.0.0.1:8770/>.
  Check it with `curl -s 127.0.0.1:8770/health`.
- The client is at `/home/marcel/code/hwlock/client/hwlock`. It is a single file that needs only the Python standard library. The wrapper below finds it there, or on `PATH`, or through `$HWLOCK`.
- The wrapper below is the same file as `examples/jetson-lock` in the hwlock repo. `tests/test_shell_clients.py::test_jetson_lock_wrapper_keeps_old_verbs` tests it against a real server: acquire, renew, release, status, `JETSON_LOCK_LEASE_S`, `JETSON_LOCK_MAX_WAIT_S`, and `run` through a fake ssh.

## 2. Drop-in replacement for `scripts/jetson-lock`

Replace the whole file with this text, and keep the file executable:

```bash
#!/usr/bin/env bash
# jetson-lock - drop-in replacement for the old ssh+mkdir jetson-lock: same verbs, same arguments,
# now backed by the hwlock service (resource "jetson"). Run on the PC (or any machine that reaches
# HWLOCK_URL and has ssh to the Jetson).
#
#   jetson-lock acquire <owner> [reason]   # take a ticket, wait in FIFO order, hold the lock
#   jetson-lock renew   <owner>            # extend the lease
#   jetson-lock release <owner>            # free it
#   jetson-lock status                     # holder, lease left, queue
#   jetson-lock run <owner> [reason] -- <ssh command...>   # acquire, run on the Jetson, release (also on failure)
#
# Env (same as before): JETSON_HOST (marcel@192.168.0.99), JETSON_LOCK_LEASE_S (default: server's 1800),
# JETSON_LOCK_MAX_WAIT_S (0 = forever). New: HWLOCK_URL, HWLOCK_TOKEN, HWLOCK (path of the client),
# JETSON_LOCK_RESOURCE (default jetson), JETSON_SSH (the ssh command used by `run`).
# Exit codes: as `hwlock` (0 ok, 1 not holder, 2 usage, 3 gave up after max-wait, 4 server problem,
# 130 interrupted; run: the remote command's code).
set -euo pipefail
HOST=${JETSON_HOST:-marcel@192.168.0.99}
RES=${JETSON_LOCK_RESOURCE:-jetson}
SSH=${JETSON_SSH:-ssh -o BatchMode=yes -o ConnectTimeout=10}
HWLOCK=${HWLOCK:-$(command -v hwlock || echo "$HOME/code/hwlock/client/hwlock")}
[[ -x $HWLOCK ]] || { echo "jetson-lock: hwlock client not found (set HWLOCK=/path/to/hwlock)" >&2; exit 2; }

lease=(); wait=()
[[ -n ${JETSON_LOCK_LEASE_S:-} ]] && lease=(--lease "$JETSON_LOCK_LEASE_S")
[[ ${JETSON_LOCK_MAX_WAIT_S:-0} != 0 ]] && wait=(--max-wait "$JETSON_LOCK_MAX_WAIT_S")

cmd=${1:-}; shift || true
case $cmd in
acquire)
    [[ $# -ge 1 && $# -le 2 ]] || { echo "usage: jetson-lock acquire <owner> [reason]" >&2; exit 2; }
    exec "$HWLOCK" acquire "$RES" "$@" ${lease[@]+"${lease[@]}"} ${wait[@]+"${wait[@]}"} ;;
renew)   exec "$HWLOCK" renew "$RES" "${1:?usage: jetson-lock renew <owner>}" ${lease[@]+"${lease[@]}"} ;;
release) exec "$HWLOCK" release "$RES" "${1:?usage: jetson-lock release <owner>}" ;;
status)  exec "$HWLOCK" status "$RES" ;;
run)
    owner=${1:?usage: jetson-lock run <owner> [reason] -- <command>}; shift; reason=()
    [[ ${1:-} != -- ]] && { reason=("$1"); shift; }
    [[ ${1:-} == -- ]] || { echo "usage: jetson-lock run <owner> [reason] -- <command>" >&2; exit 2; }
    shift
    # shellcheck disable=SC2086  # $SSH is a command line on purpose
    exec "$HWLOCK" run "$RES" "$owner" ${reason[@]+"${reason[@]}"} ${lease[@]+"${lease[@]}"} \
        ${wait[@]+"${wait[@]}"} -- $SSH "$HOST" "$@" ;;
*) sed -n '2,19p' "$0"; exit 2 ;;
esac
```

### What stays the same

- The verbs and arguments: `acquire <owner> [reason]`, `renew <owner>`, `release <owner>`, `status`, and `run <owner> [reason] -- <ssh command...>`.
  `run` still runs the command **on the Jetson over ssh** (`ssh -o BatchMode=yes -o ConnectTimeout=10 $JETSON_HOST ...`), and it still releases the lock when the command fails.
- The variables `JETSON_HOST`, `JETSON_LOCK_LEASE_S` and `JETSON_LOCK_MAX_WAIT_S` (0 means wait forever). `JETSON_LOCK_POLL_S` is no longer used: clients long-poll instead.
- The owner format `[A-Za-z0-9._-]{1,64}`, the FIFO order, the 30-minute lease, and the 120 s window after which a waiting ticket that stopped refreshing is dropped.
- `renew` and `release` print `NOT_HOLDER holder=<owner>` and exit 1 when you are not the holder. A successful `release` prints `RELEASED` and exits 0.

### What changes (all improvements; nobody relies on the old behaviour)

- **Exit codes.**
  - Giving up after `JETSON_LOCK_MAX_WAIT_S` now exits **3** instead of 2.
  - Ctrl-C while waiting exits 130 and withdraws the ticket.
  - When the server is unreachable or rejects a request, the exit code is 4.
  - In the andrea_ai repo, only CLAUDE.md mentions jetson-lock. No script checks its exit codes.
- **Output.**
  - A successful `renew` prints `RENEWED lease left 30m00s` instead of `RENEWED`.
  - The waiting lines look like `hwlock: waiting for jetson: position 2 of 3, ticket 17, holder X: reason`.
  - `status` prints `jetson: held by … since … lease left … host … pid … reason: …`, then one line per queued ticket.
- **`run` keeps the lock for long commands.** It renews the lease every lease/3 while the ssh command runs. The old script did not renew, so a `run` longer than 30 min could lose the lock.
- **No ssh round-trip per lock operation.** The lock no longer depends on the Jetson being reachable, and every change is in the audit log. Run `hwlock events jetson` or open the page.
- **Same owner re-acquiring.** If the owner already holds the lock, `acquire` succeeds at once and refreshes the lease. With the old script it waited behind its own lock.

### Jetson-side checks that must change in the same commit

`tools/quant/jetson_prep.sh` and `tools/quant/run_parakeet_matrix.sh` run **on the Jetson**. Their `need_lock()` reads `~/.jetson-lock/lock/{owner,expires}` directly. After the switch those files are not updated any more, so `need_lock` would abort with exit 3 every time. That is safe, but it breaks those runs.

The Jetson cannot reach `127.0.0.1:8770` on the PC. There are two ways to fix this:

**A (recommended): expose hwlock on the LAN with a token, and ask it from the Jetson.**

1. On the PC:

   ```bash
   cd ~/code/hwlock
   # 0.0.0.0, not the LAN IP: publishing on one IP stops answering on 127.0.0.1 (the PC's own clients).
   printf 'HWLOCK_TOKEN=%s\nHWLOCK_BIND=0.0.0.0\n' "$(openssl rand -hex 16)" > .env   # .env is gitignored
   docker compose up -d
   mkdir -p ~/.config/hwlock && printf 'url=http://127.0.0.1:8770\ntoken=%s\n' "$(sed -n 's/^HWLOCK_TOKEN=//p' .env)" > ~/.config/hwlock/config
   chmod 600 .env ~/.config/hwlock/config
   ```

   Once a token is set, every client needs it. The `~/.config/hwlock/config` file gives it to every PC shell and agent without any exports. The browser page becomes `http://127.0.0.1:8770/?token=…`.
2. On the Jetson, create `~/.config/hwlock/config` with `url=http://192.168.0.197:8770` and the same token, mode 600.
   The PC's address 192.168.0.197 comes from DHCP. Give it a DHCP reservation (or use a hostname that resolves on the Jetson). If the address changes, `need_lock` fails safe and aborts.
3. In both quant scripts, replace `need_lock()` with:

   ```bash
   need_lock() {   # hwlock: abort unless LOCK_OWNER (default quant-exp) holds the jetson lock with lease left
     local url token h
     url=$(sed -n 's/^url *= *//p' ~/.config/hwlock/config 2>/dev/null); token=$(sed -n 's/^token *= *//p' ~/.config/hwlock/config 2>/dev/null)
     h=$(curl -fsS -m 5 ${token:+-H "Authorization: Bearer $token"} "${url:-http://192.168.0.197:8770}/status/jetson" \
         | python3 -c 'import json,sys; h=json.load(sys.stdin)["holder"] or {}; print(h.get("owner") or "-", int(h.get("lease_left_s") or 0))') || h="unreachable 0"
     if [[ ${h% *} != "${LOCK_OWNER:-quant-exp}" ]] || (( ${h#* } <= 0 )); then
       echo "$(date +%T) ABORT: jetson lock not held by ${LOCK_OWNER:-quant-exp} (holder/lease: $h)"; exit 3
     fi
   }
   ```

   If the server cannot be reached, the check fails safe and aborts.

**B: check on the PC instead.** Drop `need_lock` from the Jetson scripts and always start them through `scripts/jetson-lock run <owner> -- …`. The lock is then guaranteed for the whole command, which `run` now keeps renewed. This needs no LAN exposure, but you lose the in-script guard for steps that are started by hand.

## 3. Switch procedure (main session, when the old queue is empty)

Running jobs execute the old script's code. bash reads a script while it runs it, so **replace the file with `mv`, never by editing it in place.** A running bash keeps the old inode and is not affected.

```bash
cd ~/code/andrea_ai
# 0. hwlock is up and the 'jetson' resource is free in it
curl -fsS 127.0.0.1:8770/health && ~/code/hwlock/client/hwlock status jetson

# 1. Wait until the OLD lock is idle: "holder: (free)" and an empty "queue:".
scripts/jetson-lock status
pgrep -af 'jetson-lock (acquire|run)' || echo "no old-style lock clients running"

# 2. Fence the old lock: take it with a 24 h lease, so a process that still runs the old code
#    (started before the swap) waits instead of using the Jetson next to hwlock users.
cp scripts/jetson-lock ~/.cache/jetson-lock.legacy      # the old script, to manage the fence later
JETSON_LOCK_LEASE_S=86400 ~/.cache/jetson-lock.legacy acquire migration-to-hwlock "fence: lock moved to hwlock"

# 3. Swap the file atomically and commit (with the quant need_lock change from section 2).
cp ~/code/hwlock/examples/jetson-lock scripts/jetson-lock.new && chmod +x scripts/jetson-lock.new
mv scripts/jetson-lock.new scripts/jetson-lock
git add scripts/jetson-lock tools/quant/jetson_prep.sh tools/quant/run_parakeet_matrix.sh
git commit -m "scripts/jetson-lock: thin wrapper over the hwlock service (resource jetson); same verbs"

# 4. The quant scripts run ON the Jetson: copy the changed ones there, under the new lock, while Andrea is
#    idle (CLAUDE.md deploy rule: no "speech start" in the journal for 60 s; never rsync --delete).
scripts/jetson-lock run migration-sync "sync quant need_lock" -- true   # proves the new lock works first
~/code/hwlock/client/hwlock acquire jetson migration-sync "sync quant need_lock"
rsync -a tools/quant/jetson_prep.sh tools/quant/run_parakeet_matrix.sh marcel@192.168.0.99:andrea/tools/quant/   # the Jetson copy lives in ~/andrea
~/code/hwlock/client/hwlock release jetson migration-sync

# 5. Smoke test through the new path.
scripts/jetson-lock acquire migration-smoke "smoke test" && scripts/jetson-lock status
scripts/jetson-lock release migration-smoke
scripts/jetson-lock run migration-smoke "smoke ssh" -- true; echo "run rc=$?"   # 0, and the lock is free again
```

Then:

- **CLAUDE.md.** The rule text can stay as it is, because the verbs are unchanged. Optionally add one line: "`scripts/jetson-lock status` / http://127.0.0.1:8770/ show holder and queue; `~/code/hwlock/client/hwlock events jetson` shows the history."
- **Old fence.** After a day, check `~/.cache/jetson-lock.legacy status`. It should show only the `migration-to-hwlock` holder and nothing in the queue. If so, release it (`~/.cache/jetson-lock.legacy release migration-to-hwlock`) and remove `~/.jetson-lock` on the Jetson. If an old-style waiter is still queued behind the fence, it is a stale process that was started before the switch. Stop it, or let its owner know.
- **Rollback.** `git revert` the swap commit and release the fence. All the old state is still in `~/.jetson-lock` on the Jetson.

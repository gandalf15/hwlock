#!/usr/bin/env bash
# Using a Jetson over ssh under hwlock. Assumes `hwlock` on PATH and HWLOCK_URL pointing at the server.
set -euo pipefail
JETSON=${JETSON:-user@jetson.local}
JOB=${JOB:-bench-$(date +%s)}

# 1) One command: acquire, keep the lease alive while it runs, always release (also on failure/Ctrl-C).
hwlock run jetson "$JOB" "tegrastats sample" -- ssh -o BatchMode=yes "$JETSON" 'timeout 10 tegrastats'

# 2) A multi-step session: hold the lock only for the device steps.
hwlock acquire jetson "$JOB" "deploy + benchmark" --max-wait 3600
trap 'hwlock release jetson "$JOB" >/dev/null || true' EXIT      # never leave it held by accident
rsync -a --exclude .git ./ "$JETSON:app/"
hwlock renew jetson "$JOB"                                        # long step ahead: refresh the lease
ssh -o BatchMode=yes "$JETSON" 'cd app && ./install.sh && ./bench.sh > /tmp/bench.json'
scp "$JETSON:/tmp/bench.json" ./results/
hwlock release jetson "$JOB"; trap - EXIT                         # done with the device
python3 analyse.py results/bench.json                             # local work: lock already free

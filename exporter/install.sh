#!/usr/bin/env bash
# Install hwlock-exporter as a systemd --user unit (hwlock-exporter.service) on this machine, or on another
# one over ssh. Read-only HTTP JSON endpoint for the hwlock server's http_json health probe.
#
#   exporter/install.sh [--port 8771] [--bind 0.0.0.0] [--allow CIDR[,CIDR]]             # on this machine
#   exporter/install.sh --ssh user@host [--port 8771] [--bind ...] [--allow ...]           # on another one
#
# Installs ~/.local/bin/hwlock-exporter and ~/.config/systemd/user/hwlock-exporter.service, enables and
# (re)starts it, then checks http://127.0.0.1:<port>/. Needs python3 (>= 3.7) and systemd on the target.
# A --user unit runs while the user's systemd instance runs: from boot with lingering
# (loginctl enable-linger) or with a desktop autologin; otherwise only after a login. Remove it with
# exporter/uninstall.sh.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
port=8771 bind=0.0.0.0 allow="" ssh_target=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --port) port=$2; shift 2 ;;
        --bind) bind=$2; shift 2 ;;
        --allow) allow=$2; shift 2 ;;
        --ssh) ssh_target=$2; shift 2 ;;
        -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
        *) echo "install.sh: unknown argument $1" >&2; exit 2 ;;
    esac
done
[[ $port =~ ^[0-9]+$ ]] || { echo "install.sh: --port must be a number" >&2; exit 2; }

if [[ -n $ssh_target ]]; then
    tmp=$(ssh -o BatchMode=yes "$ssh_target" 'mktemp -d /tmp/hwlock-exporter.XXXXXX')
    scp -q -o BatchMode=yes "$here/hwlock-exporter" "$here/install.sh" "$ssh_target:$tmp/"
    ssh -o BatchMode=yes "$ssh_target" "bash $tmp/install.sh --port $port --bind '$bind' --allow '$allow'; rc=\$?; rm -rf $tmp; exit \$rc"
    exit $?
fi

command -v python3 >/dev/null || { echo "install.sh: python3 not found" >&2; exit 1; }
py=$(command -v python3)
bin=$HOME/.local/bin/hwlock-exporter
unit=$HOME/.config/systemd/user/hwlock-exporter.service
install -D -m 755 "$here/hwlock-exporter" "$bin"
args="--port $port --bind $bind"
[[ -n $allow ]] && args="$args --allow $allow"
mkdir -p "$(dirname "$unit")"
cat > "$unit" <<UNIT
# hwlock-exporter: read-only health endpoint (boot id, memory, load, temperatures, GPU load) for the
# hwlock server's http_json probe. Installed by hwlock/exporter/install.sh; remove with uninstall.sh.
[Unit]
Description=hwlock-exporter (read-only health JSON on port $port)
After=network-online.target

[Service]
ExecStart=$py $bin $args
Restart=always
RestartSec=5
Nice=5
NoNewPrivileges=true

[Install]
WantedBy=default.target
UNIT
systemctl --user daemon-reload
systemctl --user enable hwlock-exporter.service >/dev/null
systemctl --user restart hwlock-exporter.service
for _ in $(seq 1 20); do
    if out=$(python3 - "$port" <<'PY' 2>/dev/null
import json, sys, urllib.request
d = json.load(urllib.request.urlopen("http://127.0.0.1:%s/" % sys.argv[1], timeout=2))
print("boot_id=%s mem_available_mb=%s temp_max_c=%s gpu_load_pct=%s" % (
    d.get("boot_id"), d.get("mem_available_mb"), d.get("temp_max_c"), d.get("gpu_load_pct")))
PY
    ); then
        echo "hwlock-exporter is up on $(hostname):$port: $out"
        if [[ $(loginctl show-user "$USER" -p Linger --value 2>/dev/null) != yes ]]; then
            echo "note: lingering is off: the unit starts with your user session (a desktop autologin is enough);"
            echo "      without one it starts only after a login (loginctl enable-linger $USER starts it at boot)."
        fi
        exit 0
    fi
    sleep 0.5
done
echo "install.sh: hwlock-exporter did not answer on port $port; see: journalctl --user -u hwlock-exporter" >&2
exit 1

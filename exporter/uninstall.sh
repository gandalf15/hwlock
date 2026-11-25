#!/usr/bin/env bash
# Remove hwlock-exporter (the systemd --user unit and ~/.local/bin/hwlock-exporter) from this machine,
# or from another one: exporter/uninstall.sh --ssh user@host
set -euo pipefail
if [[ ${1:-} == --ssh ]]; then
    ssh -o BatchMode=yes "${2:?usage: uninstall.sh --ssh user@host}" 'bash -s' < "$0"
    exit $?
fi
systemctl --user disable --now hwlock-exporter.service 2>/dev/null || true
rm -f "$HOME/.config/systemd/user/hwlock-exporter.service" "$HOME/.local/bin/hwlock-exporter"
systemctl --user daemon-reload
echo "hwlock-exporter removed from $(hostname)"

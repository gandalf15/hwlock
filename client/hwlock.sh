#!/usr/bin/env bash
# hwlock.sh - bash + curl fallback client for machines without Python. Same verbs as `hwlock`
# for the basics; prefer the Python client (run/hold with auto-renew, re-attach, clean Ctrl-C).
#
#   hwlock.sh acquire <resource> <owner> [reason] [lease_s]   # waits (long-poll) until held
#   hwlock.sh renew   <resource> <owner> [lease_s]
#   hwlock.sh release <resource> <owner>
#   hwlock.sh status  [resource]                              # raw JSON
#
# Env: HWLOCK_URL (default http://127.0.0.1:8770), HWLOCK_TOKEN.
# Exit: 0 ok, 1 not holder, 4 server error, 5 OVER_MAX_HOLD (renew refused: held past the server's max
# hold while others wait; release soon, you are evicted after the grace period), 130 Ctrl-C while waiting
# (ticket cancelled), 143 SIGTERM while waiting (ticket kept: re-run the same acquire within the server's
# re-attach window, default 120 s, to get the place back; bash handles the TERM once the current long-poll
# returns, at most ~25 s).
set -euo pipefail
URL=${HWLOCK_URL:-http://127.0.0.1:8770}
auth=(); [[ -n ${HWLOCK_TOKEN:-} ]] && auth=(-H "Authorization: Bearer $HWLOCK_TOKEN")
esc() { local s=${1//\\/\\\\}; s=${s//\"/\\\"}; printf '%s' "$s"; }
# post <path> <json> [max_time] -> prints "<http code> <body>"
post() { curl -sS -m "${3:-15}" "${auth[@]}" -H 'Content-Type: application/json' -d "$2" \
              -w '\n%{http_code}' "$URL$1" | { b=$(cat); printf '%s %s\n' "${b##*$'\n'}" "${b%$'\n'*}"; }; }
# first (top-level) occurrence of a scalar field; the holder object comes later in every answer
field() { grep -o "\"$1\": *\"\{0,1\}[^,\"}]*" <<<"$2" | head -1 | sed 's/.*: *"\{0,1\}//'; }

cmd=${1:-}; shift || true
case $cmd in
acquire)
    res=$1 owner=$2 reason=${3:-} lease=${4:-}
    attach="sh-$$-$RANDOM$RANDOM"   # this process: a late cancel/detach of ours never touches a re-run's ticket
    body="\"owner\": \"$(esc "$owner")\", \"reason\": \"$(esc "$reason")\", \"host\": \"$(hostname)\", \"pid\": $PPID, \"attach\": \"$attach\""
    [[ -n $lease ]] && body+=", \"lease\": $lease"
    r=$(post "/r/$res/acquire" "{$body, \"wait\": 0}"); last=""
    while :; do
        code=${r%% *} json=${r#* }
        case $code in
            200) echo "hwlock: $owner holds $res"; exit 0 ;;
            202) ticket=$(field ticket "$json"); msg="waiting for $res: position $(field position "$json"), ticket $ticket"
                 [[ $msg != "$last" ]] && echo "hwlock: $msg" >&2; last=$msg
                 trap 'post "/r/$res/cancel" "{\"ticket\": $ticket, \"attach\": \"$attach\"}" >/dev/null; exit 130' INT
                 trap 'post "/r/$res/detach" "{\"ticket\": $ticket, \"attach\": \"$attach\"}" >/dev/null; echo "hwlock: SIGTERM; ticket $ticket kept for re-attach" >&2; exit 143' TERM
                 r=$(post "/r/$res/acquire" "{$body, \"ticket\": $ticket, \"wait\": 25}" 40) ;;
            410) r=$(post "/r/$res/acquire" "{$body, \"wait\": 0}") ;;   # ticket dropped: rejoin
            409) echo "OVER_MAX_HOLD waiting=$(field waiting "$json") evict_in=$(field evict_in_s "$json" | cut -d. -f1)"; exit 5 ;;
            *)   echo "hwlock: server said $code $json" >&2; exit 4 ;;
        esac
    done ;;
renew|release)
    res=$1 owner=$2 body="\"owner\": \"$(esc "$owner")\""
    [[ $cmd == renew && -n ${3:-} ]] && body+=", \"lease\": $3"
    r=$(post "/r/$res/$cmd" "{$body}"); code=${r%% *}
    case $code in 200) echo "${cmd^^}ED" | sed 's/EED$/ED/'; exit 0 ;;
                  409) if [[ $(field error "${r#* }") == over_max_hold ]]; then
                           echo "OVER_MAX_HOLD waiting=$(field waiting "${r#* }") evict_in=$(field evict_in_s "${r#* }" | cut -d. -f1)"; exit 5
                       fi
                       echo "NOT_HOLDER holder=$(field owner "${r#* }")"; exit 1 ;;
                  *) echo "hwlock: server said ${r}" >&2; exit 4 ;; esac ;;
status) curl -sS -m 15 "${auth[@]}" "$URL/status${1:+/$1}"; echo ;;
*) sed -n '2,15p' "$0"; exit 2 ;;
esac

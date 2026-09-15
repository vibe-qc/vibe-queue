#!/usr/bin/env bash
# Walk a list of (host, program, sha) through `vq admin update`, unattended.
#
# THE POINT OF THIS FILE: it contains no `grep` of any vq message. Every
# decision comes from an exit code, and every detail from `--json`. That is
# the contract `docs/orchestration.md` describes, and this is the check that
# it is finished -- if this script cannot be written cleanly, the contract is
# not done.
#
# It is the sweep the 2026-09 fleet migration wished it had. That one grepped
# for "local checkout mutation lock" to decide whether to retry (a substring
# match on a sentence no test pinned), ran `ps -eo command | grep -c "[n]inja"`
# to decide whether a build had finished (a loop whose `grep -c` exits 1 on a
# zero count, so it spun for six hours after the build was done), and stopped
# dead on a failed marker that belonged to a different program.
#
# Usage:
#   fleet-sweep.sh TARGETS_FILE
#
# TARGETS_FILE holds one `host program sha` per line; blank lines and lines
# beginning with # are ignored. Example:
#
#   compute-a vibeqc-dev      6421ed34...
#   compute-b vibeqc-release  6421ed34...
#
# Exit: 0 if every target ended `ok` or `already-current`; 1 otherwise.

set -uo pipefail

VQ="${VQ:-vq}"
RETRY_LIMIT="${RETRY_LIMIT:-20}"
RETRY_SLEEP="${RETRY_SLEEP:-90}"

# Exit codes are the contract. See docs/orchestration.md.
readonly EX_LOCKED=75
readonly EX_MARKER=76
readonly EX_PRECONDITION=77

targets_file="${1:?usage: fleet-sweep.sh TARGETS_FILE}"

note() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# Read one field out of a --json payload without parsing prose.
json_field() {
    python3 -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1]) or "")' "$1"
}

# Run one update. Echoes the payload; returns vq's exit code unchanged.
run_update() {
    local host=$1 program=$2 sha=$3
    shift 3
    "$VQ" admin update "$program" "$host" --expected-sha "$sha" --json "$@"
}

sweep_one() {
    local host=$1 program=$2 sha=$3
    local attempt=0 acknowledged=0 payload rc outcome

    while :; do
        attempt=$((attempt + 1))
        if [ "$attempt" -gt "$RETRY_LIMIT" ]; then
            note "$host/$program: giving up after $RETRY_LIMIT attempts"
            return 1
        fi

        if [ "$acknowledged" -eq 1 ]; then
            payload=$(run_update "$host" "$program" "$sha" \
                --acknowledge-failed-marker)
        else
            payload=$(run_update "$host" "$program" "$sha")
        fi
        rc=$?
        outcome=$(printf '%s' "$payload" | json_field outcome)

        case "$rc" in
            0)
                # ok and already-current both mean continue. The outcome says
                # which, for the log; neither needs different handling.
                note "$host/$program: $outcome"
                return 0
                ;;
            "$EX_LOCKED")
                note "$host/$program: locked, retrying in ${RETRY_SLEEP}s"
                sleep "$RETRY_SLEEP"
                ;;
            "$EX_MARKER")
                if [ "$acknowledged" -eq 1 ]; then
                    note "$host/$program: still marker-present after " \
                         "acknowledging; stopping"
                    return 1
                fi
                note "$host/$program: marker-present, acknowledging"
                acknowledged=1
                ;;
            "$EX_PRECONDITION")
                # A safety gate DECIDED, and decided no. Retrying or forcing
                # past it is the dangerous thing to do. A gate that merely
                # could not gather its evidence reports EX_LOCKED instead and
                # is retried above -- that split is what stops a slow login
                # node reading as a non-converged host.
                note "$host/$program: precondition-failed, STOPPING SWEEP"
                printf '%s\n' "$payload" >&2
                return 2
                ;;
            *)
                note "$host/$program: failed (exit $rc)"
                printf '%s\n' "$payload" >&2
                return 1
                ;;
        esac
    done
}

# Is anything running on this host? No `ps`, no ssh, no grep.
wait_until_idle() {
    local host=$1 waited=0
    while [ "$("$VQ" admin status "$host" --json | json_field in_flight)" = "True" ]; do
        if [ "$waited" -ge "$((RETRY_SLEEP * RETRY_LIMIT))" ]; then
            note "$host: still busy after ${waited}s; continuing anyway"
            return 0
        fi
        note "$host: an operation is in flight, waiting"
        sleep "$RETRY_SLEEP"
        waited=$((waited + RETRY_SLEEP))
    done
}

failures=0
stop=0
while read -r host program sha _rest; do
    case "${host:-}" in ''|'#'*) continue ;; esac
    [ "$stop" -eq 1 ] && break
    wait_until_idle "$host"
    sweep_one "$host" "$program" "$sha"
    case $? in
        0) ;;
        2) stop=1; failures=$((failures + 1)) ;;
        *) failures=$((failures + 1)) ;;
    esac
done < "$targets_file"

if [ "$failures" -eq 0 ]; then
    note "sweep complete: every target is at its pin"
    exit 0
fi
note "sweep finished with $failures failure(s)"
exit 1

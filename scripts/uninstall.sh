#!/usr/bin/env bash
# Remove a vq source install.
#
# USAGE
#     ./scripts/uninstall.sh [OPTIONS]
#
# WHAT IS REMOVED
#     By default: the virtual environment, and nothing else.
#
#     Your queue and job history live in the state directory, and your host
#     definitions live in the config directory. Those are DATA, not install
#     artefacts -- reinstalling vq is meant to find them intact. Removing them
#     takes a separate, explicit flag, and is refused while work is still
#     queued unless you also pass --force.
#
# OPTIONS
#     --venv PATH           Explicit venv. Default: auto-detect, preferring
#                           vibe-queue/.venv.
#     --purge-state         Also delete the state directory: the queue, job
#                           workspaces, logs, and history. IRREVERSIBLE.
#     --purge-config        Also delete the config directory, including
#                           config.toml and your host definitions.
#     --all                 --purge-state and --purge-config together.
#     --keep-venv           Do not remove the environment. Useful with a purge
#                           flag when you want a clean slate but the same venv.
#     --adopt-legacy        One-time adoption of an unmarked pre-marker vq
#                           environment after exact PEP 610 checkout proof.
#     --yes                 Do not prompt for confirmation.
#     --force               Purge state even if jobs are still queued.
#     --dry-run             Print exactly what would be removed, and stop.
#     -h, --help            Show this help.
#
# EXAMPLES
#     ./scripts/uninstall.sh --dry-run
#     ./scripts/uninstall.sh
#     ./scripts/uninstall.sh --purge-state --yes
#     ./scripts/uninstall.sh --all --yes
#
# This does not touch systemd units, launchd plists, or anything under /opt/vq.
# Those are named at the end if present, with the command to remove them, so
# nothing outside this checkout is deleted on your behalf.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_venv_helpers.sh
. "$SCRIPT_DIR/_venv_helpers.sh"

EXPLICIT_VENV=""
VENV_OPTION_SET=0
PURGE_STATE=0
PURGE_CONFIG=0
KEEP_VENV=0
ADOPT_LEGACY=0
ASSUME_YES=0
FORCE=0
DRY_RUN=0

print_help() {
    awk '/^# USAGE/ {p=1} p && !/^#/ {exit} p {sub(/^# ?/, ""); print}' "$0"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --venv)
            vq_require_option_value "$1" "$#" "${2-}"
            EXPLICIT_VENV="$2"
            VENV_OPTION_SET=1
            shift 2
            ;;
        --purge-state)  PURGE_STATE=1; shift ;;
        --purge-config) PURGE_CONFIG=1; shift ;;
        --all)          PURGE_STATE=1; PURGE_CONFIG=1; shift ;;
        --keep-venv)    KEEP_VENV=1; shift ;;
        --adopt-legacy) ADOPT_LEGACY=1; shift ;;
        --yes|-y)       ASSUME_YES=1; shift ;;
        --force)        FORCE=1; shift ;;
        --dry-run)      DRY_RUN=1; shift ;;
        -h|--help)      print_help; exit 0 ;;
        *)
            echo "Error: unknown argument '$1'." >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

if [ "$KEEP_VENV" = "1" ] && [ "$PURGE_STATE" = "0" ] && [ "$PURGE_CONFIG" = "0" ]; then
    echo "Error: --keep-venv with no purge flag would remove nothing." >&2
    echo "Add --purge-state and/or --purge-config, or drop --keep-venv." >&2
    exit 1
fi
if [ "$FORCE" = "1" ] && [ "$PURGE_STATE" != "1" ]; then
    echo "Error: --force is only meaningful with --purge-state (or --all)." >&2
    exit 1
fi
if [ "$ADOPT_LEGACY" = "1" ] && [ "$KEEP_VENV" = "1" ]; then
    echo "Error: --adopt-legacy cannot be used with --keep-venv." >&2
    exit 1
fi

if [ "$VENV_OPTION_SET" = "1" ]; then
    VENV_INPUT="$EXPLICIT_VENV"
else
    VENV_INPUT="${VQ_VENV:-}"
fi
vq_detect_venv VENV_PATH "$VENV_INPUT"
if [ "$KEEP_VENV" != "1" ] && [ -n "$VENV_PATH" ]; then
    vq_check_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
elif [ "$ADOPT_LEGACY" = "1" ]; then
    echo "Error: --adopt-legacy requires an existing unmarked vq environment." >&2
    exit 1
fi
STATE_DIR="$(vq_state_dir)"
CONFIG_DIR="$(vq_config_dir)"
[ "$PURGE_STATE" = "1" ] && vq_assert_safe_data_target "$STATE_DIR" "state"
[ "$PURGE_CONFIG" = "1" ] && vq_assert_safe_data_target "$CONFIG_DIR" "config"
QUEUED="$(vq_count_queue_entries)"

echo "==> vq uninstall"
echo "    venv:         ${VENV_PATH:-<not found>}$([ "$KEEP_VENV" = "1" ] && echo '  (kept: --keep-venv)')"
echo "    state dir:    $STATE_DIR$([ -d "$STATE_DIR" ] && echo "  ($QUEUED queue entries)" || echo '  (absent)')"
echo "    config dir:   $CONFIG_DIR$([ -d "$CONFIG_DIR" ] || echo '  (absent)')"
echo "    daemon:       $(vq_daemon_describe "${VENV_PATH:-/nonexistent}")"
[ "$ADOPT_LEGACY" = "1" ] && echo "    adoption:     exact legacy PEP 610 proof required"
echo
REMOVE_LINES=()
KEEP_LINES=()
if [ "$KEEP_VENV" != "1" ] && [ -n "$VENV_PATH" ]; then
    REMOVE_LINES+=("the virtualenv $VENV_PATH")
elif [ -n "$VENV_PATH" ]; then
    KEEP_LINES+=("the virtualenv $VENV_PATH  (--keep-venv)")
fi
if [ "$PURGE_STATE" = "1" ]; then
    REMOVE_LINES+=("the STATE directory $STATE_DIR (queue, workspaces, logs, history)")
else
    KEEP_LINES+=("the state directory $STATE_DIR  (pass --purge-state to remove)")
fi
if [ "$PURGE_CONFIG" = "1" ]; then
    REMOVE_LINES+=("the CONFIG directory $CONFIG_DIR (config.toml, host definitions)")
else
    KEEP_LINES+=("the config directory $CONFIG_DIR  (pass --purge-config to remove)")
fi

if [ ${#REMOVE_LINES[@]} -gt 0 ]; then
    echo "    Will remove:"
    printf '      - %s\n' "${REMOVE_LINES[@]}"
fi
if [ ${#KEEP_LINES[@]} -gt 0 ]; then
    echo "    Will KEEP:"
    printf '      - %s\n' "${KEEP_LINES[@]}"
fi
echo

if [ "$KEEP_VENV" != "1" ] && [ -z "$VENV_PATH" ] && \
   [ "$PURGE_STATE" = "0" ] && [ "$PURGE_CONFIG" = "0" ]; then
    echo "Nothing to do: no vq virtualenv was found."
    echo "Name one explicitly with --venv PATH, or pass a purge flag."
    exit 0
fi

if [ "$PURGE_STATE" = "1" ] && [ "$QUEUED" != "0" ] && [ "$FORCE" != "1" ]; then
    cat >&2 <<EOF
Error: refusing to purge state while $QUEUED queue entries remain.

Inspect them first:
    ${VENV_PATH:+$VENV_PATH/bin/}vq queue
    ${VENV_PATH:+$VENV_PATH/bin/}vq status

Those entries include job workspaces and results that are not recoverable
after this. If you have already saved what you need:
    $SCRIPT_DIR/uninstall.sh --purge-state --force
EOF
    exit 1
fi

DAEMON_PID=""
if [ "$PURGE_STATE" = "1" ]; then
    DAEMON_PID="$(sed -n '1p' "$STATE_DIR/daemon.pid" 2>/dev/null | tr -dc '0-9' || true)"
    if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        if [ -z "$VENV_PATH" ] || ! vq_process_belongs_to_venv "$DAEMON_PID" "$VENV_PATH"; then
            echo "Error: refusing to purge state while daemon pid $DAEMON_PID is alive." >&2
            echo "Stop the daemon and retry; its state must not disappear underneath it." >&2
            exit 1
        fi
    fi
fi

if [ "$DRY_RUN" = "1" ]; then
    echo "==> Dry-run complete. Nothing was changed."
    exit 0
fi

if [ "$ASSUME_YES" != "1" ]; then
    if [ "$PURGE_STATE" = "1" ] || [ "$PURGE_CONFIG" = "1" ]; then
        echo "This deletes data that cannot be recovered."
        printf 'Type "yes" to continue: '
    else
        printf 'Remove the vq environment? [y/N] '
    fi
    read -r reply
    case "$reply" in
        y|Y|yes|YES) ;;
        *) echo "Aborted; nothing was changed."; exit 1 ;;
    esac
    echo
fi

if [ -n "$VENV_PATH" ]; then
    LIFECYCLE_TARGET="$VENV_PATH"
elif [ -n "$VENV_INPUT" ]; then
    LIFECYCLE_TARGET="$(vq_path_from_project "$VENV_INPUT")"
else
    LIFECYCLE_TARGET="$VQ_PROJECT_DIR/.venv"
fi
trap 'vq_lifecycle_lock_cleanup' EXIT
vq_acquire_lifecycle_lock "$LIFECYCLE_TARGET" uninstall

if [ "$KEEP_VENV" != "1" ] && [ -n "$VENV_PATH" ]; then
    vq_require_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
fi

if [ -n "$VENV_PATH" ] && vq_daemon_is_running "$VENV_PATH"; then
    vq_daemon_stop "$VENV_PATH"
    # Do not restart it: the environment it ran from is about to go away.
    VQ_DAEMON_WAS_RUNNING=0
fi

if [ "$KEEP_VENV" != "1" ] && [ -n "$VENV_PATH" ]; then
    vq_assert_safe_venv_target "$VENV_PATH"
    vq_remove_venv "$VENV_PATH" 0
fi

if [ "$PURGE_STATE" = "1" ] && [ -d "$STATE_DIR" ]; then
    DAEMON_PID="$(sed -n '1p' "$STATE_DIR/daemon.pid" 2>/dev/null | tr -dc '0-9' || true)"
    if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "Error: refusing to purge state while daemon pid $DAEMON_PID is alive." >&2
        echo "Stop the daemon and retry; its state must not disappear underneath it." >&2
        exit 1
    fi
    vq_assert_safe_data_target "$STATE_DIR" "state"
    echo "==> Removing state directory: $STATE_DIR"
    rm -rf -- "$STATE_DIR"
fi

if [ "$PURGE_CONFIG" = "1" ] && [ -d "$CONFIG_DIR" ]; then
    vq_assert_safe_data_target "$CONFIG_DIR" "config"
    echo "==> Removing config directory: $CONFIG_DIR"
    rm -rf -- "$CONFIG_DIR"
fi

vq_release_lifecycle_lock
trap - EXIT

echo
echo "==> vq uninstall complete."

# Service definitions live outside this checkout. Name them; never remove them.
LEFTOVERS=0
report_leftover() {
    [ -e "$1" ] || return 0
    if [ "$LEFTOVERS" = "0" ]; then
        echo
        echo "    Still installed outside this checkout (not removed):"
        LEFTOVERS=1
    fi
    printf '      - %s\n' "$1"
    printf '        %s\n' "$2"
}

report_leftover "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/vq-daemon.service" \
    "systemctl --user disable --now vq-daemon && rm that file"
report_leftover "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/vq-web.service" \
    "systemctl --user disable --now vq-web && rm that file"
report_leftover "$HOME/Library/LaunchAgents/com.vq.daemon.plist" \
    "launchctl bootout gui/$(id -u)/com.vq.daemon, then rm that plist"
report_leftover "$HOME/Library/LaunchAgents/com.vq.web.plist" \
    "launchctl bootout gui/$(id -u)/com.vq.web, then rm that plist"
report_leftover "/opt/vq" \
    "root-owned multi-user install; see docs/multi_user_deployment.md"

if [ "$PURGE_STATE" != "1" ] && [ -d "$STATE_DIR" ]; then
    echo
    echo "    Your queue and job history were kept at:"
    echo "        $STATE_DIR"
fi

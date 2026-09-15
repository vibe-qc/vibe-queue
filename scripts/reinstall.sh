#!/usr/bin/env bash
# Rebuild a vq environment from the current checkout, without touching Git.
#
# USAGE
#     ./scripts/reinstall.sh [OPTIONS]
#
# WHEN TO USE THIS
#     update.sh   moves the checkout, then reinstalls.
#     reinstall.sh leaves the checkout exactly where it is and rebuilds the
#                 environment from it.
#
#     Reach for it when the environment is the suspect, not the code: a broken
#     or half-installed venv, a Python upgrade that stranded the interpreter, a
#     dependency resolved wrong, an editable/non-editable switch, or an install
#     whose reported provenance disagrees with the tree it came from.
#
# OPTIONS
#     --extras GROUP        core, web, test, dev, or all. Default: preserve the
#                           installed profile (core when no metadata exists).
#     --editable            Reinstall with `pip install -e`.
#     --copied              Reinstall as a non-editable copy. By default the
#                           installed mode is preserved.
#     --python BIN          Python used to recreate the venv (default: python3).
#     --venv PATH           Explicit venv. Default: auto-detect, preferring
#                           vibe-queue/.venv.
#     --keep-venv           Reinstall the package in place instead of replacing
#                           the whole environment. Faster; does not fix a venv
#                           whose interpreter or pip is broken.
#     --adopt-legacy        One-time adoption of an unmarked pre-marker vq
#                           environment after exact PEP 610 checkout proof.
#     --restart-daemon      Retired safety flag: now refuses and points to the
#                           durable `vq self-update` / `vq admin update` path.
#     --dry-run             Preview only; does not change files.
#     -h, --help            Show this help.
#
# EXAMPLES
#     ./scripts/reinstall.sh --dry-run
#     ./scripts/reinstall.sh   # only while no daemon runs
#     ./scripts/reinstall.sh --keep-venv --extras web
#     ./scripts/reinstall.sh --python python3.13
#
# Whole-environment rebuilds are failure-atomic: the previous environment is
# kept beside the target and restored if creation, installation, or
# verification fails. --keep-venv is an explicit in-place repair and does not
# provide that replacement guarantee. Direct use requires the selected
# environment not to own a running daemon; use `vq self-update` or
# `vq admin update` instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_venv_helpers.sh
. "$SCRIPT_DIR/_venv_helpers.sh"

EXTRAS_PROFILE=""
EDITABLE=""
INSTALL_MODE=""
PYTHON_BIN="python3"
PYTHON_OPTION_SET=0
EXPLICIT_VENV=""
VENV_OPTION_SET=0
KEEP_VENV=0
ADOPT_LEGACY=0
RESTART_DAEMON=0
DRY_RUN=0

print_help() {
    awk '/^# USAGE/ {p=1} p && !/^#/ {exit} p {sub(/^# ?/, ""); print}' "$0"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --extras)
            vq_require_option_value "$1" "$#" "${2-}"
            EXTRAS_PROFILE="$2"
            shift 2
            ;;
        --python)
            vq_require_option_value "$1" "$#" "${2-}"
            PYTHON_BIN="$2"
            PYTHON_OPTION_SET=1
            shift 2
            ;;
        --venv)
            vq_require_option_value "$1" "$#" "${2-}"
            EXPLICIT_VENV="$2"
            VENV_OPTION_SET=1
            shift 2
            ;;
        --editable)
            [ -z "$INSTALL_MODE" ] || { echo "Error: choose only one of --editable / --copied." >&2; exit 1; }
            INSTALL_MODE="editable"
            EDITABLE=1
            shift
            ;;
        --copied)
            [ -z "$INSTALL_MODE" ] || { echo "Error: choose only one of --editable / --copied." >&2; exit 1; }
            INSTALL_MODE="copied"
            EDITABLE=0
            shift
            ;;
        --keep-venv)      KEEP_VENV=1; shift ;;
        --adopt-legacy)   ADOPT_LEGACY=1; shift ;;
        --restart-daemon) RESTART_DAEMON=1; shift ;;
        --dry-run)        DRY_RUN=1; shift ;;
        -h|--help)        print_help; exit 0 ;;
        *)
            echo "Error: unknown argument '$1'." >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

if [ "$KEEP_VENV" = "1" ] && [ "$PYTHON_OPTION_SET" = "1" ]; then
    echo "Error: --python cannot be used with --keep-venv." >&2
    exit 1
fi
if [ "$VENV_OPTION_SET" = "1" ]; then
    VENV_INPUT="$EXPLICIT_VENV"
else
    VENV_INPUT="${VQ_VENV:-}"
fi
vq_detect_venv VENV_PATH "$VENV_INPUT"
if [ -z "$VENV_PATH" ]; then
    if [ -n "$VENV_INPUT" ]; then
        VENV_TARGET="$VENV_INPUT"
    else
        VENV_TARGET="$VQ_PROJECT_DIR/.venv"
    fi
else
    VENV_TARGET="$VENV_PATH"
fi

if [ "$KEEP_VENV" = "1" ]; then
    if [ -z "$VENV_PATH" ]; then
        cat >&2 <<EOF
Error: --keep-venv needs an existing environment, and none was found.

Create one with:
    $SCRIPT_DIR/install.sh
EOF
        exit 1
    fi
    vq_resolve_venv_path VENV_PATH "$VENV_TARGET"
else
    vq_resolve_venv_path VENV_PATH "$VENV_TARGET"
fi
vq_assert_safe_venv_target "$VENV_PATH"
vq_assert_mutable_runtime_target "$VENV_PATH" 0
if [ "$RESTART_DAEMON" = "1" ]; then
    cat >&2 <<EOF
Error: direct lifecycle scripts no longer stop and restart a serving vq daemon.
Use vq self-update or vq admin update for a managed serving-environment
transaction. Stop an inactive local development service yourself before a
direct reinstall.
EOF
    exit 1
fi
RECOVERY_STATUS=0
RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
    VQ_LIFECYCLE_VENV="$VENV_PATH"
    trap 'vq_lifecycle_cleanup' EXIT
    vq_acquire_lifecycle_lock "$VENV_PATH" reinstall-recovery
    vq_assert_mutable_runtime_target "$VENV_PATH" 0
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
    vq_recover_pending_venv_replacement_before_use \
        "$VENV_PATH" reinstall "$DRY_RUN" || RECOVERY_STATUS=$?
    vq_release_lifecycle_lock
    trap - EXIT
    exit 1
fi
vq_check_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
if [ "$KEEP_VENV" != "1" ]; then
    # Ownership of an existing target was proved without executing it above.
    vq_resolve_creation_python PYTHON_BIN "$PYTHON_BIN" "$VENV_PATH"
fi

SAVED_PROFILE=""
SAVED_EDITABLE=""
vq_load_install_metadata "$VENV_PATH" SAVED_PROFILE SAVED_EDITABLE || true
[ -n "$EXTRAS_PROFILE" ] || EXTRAS_PROFILE="${SAVED_PROFILE:-core}"
[ -n "$EDITABLE" ] || EDITABLE="${SAVED_EDITABLE:-0}"
vq_extras_to_spec "$EXTRAS_PROFILE" EXTRAS_SPEC

HEAD_SHORT="$(git -C "$VQ_REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo '<no checkout>')"

echo "==> vq reinstall"
echo "    source:       $VQ_PROJECT_DIR"
echo "    checkout:     unchanged, at $HEAD_SHORT"
echo "    venv:         $VENV_PATH"
echo "    strategy:     $([ "$KEEP_VENV" = "1" ] && echo 'reinstall the package in place' || echo 'replace the whole environment')"
[ "$KEEP_VENV" != "1" ] && echo "    python:       $PYTHON_BIN"
echo "    capabilities: $EXTRAS_PROFILE  (pip suffix: '${EXTRAS_SPEC:-<core>}')"
echo "    mode:         $([ "$EDITABLE" = "1" ] && echo 'editable (-e)' || echo 'copied into the venv')"
echo "    daemon:       $(vq_daemon_describe "$VENV_PATH")"
[ "$ADOPT_LEGACY" = "1" ] && echo "    adoption:     exact legacy PEP 610 proof required"
[ "$DRY_RUN" = "1" ] && echo "    --dry-run:    no files will be changed"
echo

if [ "$DRY_RUN" = "1" ]; then
    if [ "$KEEP_VENV" = "1" ]; then
        echo "    [venv] would be kept: $VENV_PATH"
    elif [ -e "$VENV_PATH" ]; then
        if [ -f "$VENV_PATH/pyvenv.cfg" ]; then
            echo "    [venv] would be replaced atomically: $VENV_PATH"
        else
            echo "    [venv] exists but is not recognisably a virtualenv; reinstall would refuse."
        fi
    else
        echo "    [venv] would be created: $VENV_PATH"
    fi
    echo "    [pip] would install vq${EXTRAS_SPEC} from this checkout."
    echo "    [marker] would restamp SOURCE-SHA from $HEAD_SHORT."
    if vq_daemon_is_running "$VENV_PATH"; then
        if [ "$RESTART_DAEMON" = "1" ]; then
            echo "    [daemon] would be stopped before, and started after, the rebuild."
        else
            echo "    [daemon] is running; direct reinstall refuses and requires a managed update."
        fi
    fi
    echo
    echo "==> Dry-run complete."
    exit 0
fi

VQ_LIFECYCLE_VENV="$VENV_PATH"
trap 'vq_lifecycle_cleanup' EXIT
vq_acquire_lifecycle_lock "$VENV_PATH" reinstall
vq_assert_mutable_runtime_target "$VENV_PATH" 0
RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
    vq_recover_pending_venv_replacement_before_use \
        "$VENV_PATH" reinstall 0 || RECOVERY_STATUS=$?
    vq_release_lifecycle_lock
    trap - EXIT
    exit 1
fi
# Direct reinstall never owns a daemon restart. Prove the target inactive
# while holding the exact lifecycle lock and before legacy adoption or any
# in-place/whole-environment mutation.
vq_assert_daemon_stopped_or_managed \
    "$VENV_PATH" 0 "Rebuilding this environment"
vq_require_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"

if [ "$KEEP_VENV" = "1" ]; then
    vq_check_venv_health "$VENV_PATH" || {
        echo "Re-run without --keep-venv to replace the environment safely." >&2
        exit 1
    }
    vq_install_environment "$VENV_PATH" "$EXTRAS_SPEC" "$EDITABLE"
    vq_verify_environment "$VENV_PATH" "$EXTRAS_PROFILE"
    vq_record_venv_ownership "$VENV_PATH"
    vq_record_source_marker "$VENV_PATH" "$EDITABLE"
    vq_record_install_metadata "$VENV_PATH" "$EXTRAS_PROFILE" "$EDITABLE"
else
    vq_begin_venv_replacement "$VENV_PATH" 0 1
    vq_start_venv_replacement "$VENV_PATH"

    echo "==> Creating replacement virtualenv: $VENV_PATH"
    "$PYTHON_BIN" -m venv "$VENV_PATH"
    vq_install_environment "$VENV_PATH" "$EXTRAS_SPEC" "$EDITABLE"
    vq_verify_environment "$VENV_PATH" "$EXTRAS_PROFILE"
    vq_record_venv_ownership "$VENV_PATH"
    vq_record_source_marker "$VENV_PATH" "$EDITABLE"
    vq_record_install_metadata "$VENV_PATH" "$EXTRAS_PROFILE" "$EDITABLE"
    vq_commit_venv_replacement
fi

vq_release_lifecycle_lock
trap - EXIT

echo
echo "==> vq reinstall complete."
echo "    Version: $("$VENV_PATH/bin/vq" --version | head -n 1)"
echo "    Source:  $("$VENV_PATH/bin/vq" source-sha 2>/dev/null || echo '<no marker>')"

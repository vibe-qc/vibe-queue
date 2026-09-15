#!/usr/bin/env bash
# Install vq from this source checkout.
#
# USAGE
#     ./scripts/install.sh [OPTIONS]
#
# OPTIONS
#     --extras GROUP        Capability profile (default: core):
#                           core = the vq CLI and daemon
#                           web  = core + the read-only dashboard
#                           test = core + the test dependencies
#                           dev  = core + test + lint/type tooling
#                           all  = web + test + dev
#     --editable            Install with `pip install -e`. The daemon then
#                           derives its source SHA from the checkout, so the
#                           code it runs moves whenever the checkout moves.
#                           Convenient for development; never for a shared or
#                           privileged install.
#     --copied              Install a non-editable copy (the default). Useful
#                           when an invocation is assembled programmatically.
#     --python BIN          Python used to create the venv (default: python3).
#     --venv PATH           Venv path (default: vibe-queue/.venv). Relative
#                           paths are resolved from the vibe-queue directory.
#     --force               Replace an existing, recognisable virtualenv.
#     --adopt-legacy        One-time adoption of an unmarked pre-marker vq
#                           environment, only after exact PEP 610 proof that it
#                           was installed from this checkout. Requires --force.
#     --restart-daemon      Retired safety flag: now refuses and points to the
#                           durable `vq self-update` / `vq admin update` path.
#     --dry-run             Print the resolved install without changing files.
#     -h, --help            Show this help.
#
# EXAMPLES
#     ./scripts/install.sh
#     ./scripts/install.sh --extras web
#     ./scripts/install.sh --editable --extras dev
#     ./scripts/install.sh --python python3.13 --venv .venv-py313
#     ./scripts/install.sh --force   # only while no daemon runs
#
# The install is self-contained under vibe-queue/.venv and does not build or
# install vibe-qc. Root-owned multi-user installs under /opt/vq are managed
# separately -- see docs/multi_user_deployment.md. On the shared compute hosts
# the checkouts are owned by `vq admin update`; do not run this there (CLAUDE.md
# section 15).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_venv_helpers.sh
. "$SCRIPT_DIR/_venv_helpers.sh"

PYTHON_BIN="python3"
VENV_INPUT="${VQ_VENV:-.venv}"
EXTRAS_PROFILE="core"
EDITABLE=0
INSTALL_MODE=""
FORCE=0
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
            shift 2
            ;;
        --venv)
            vq_require_option_value "$1" "$#" "${2-}"
            VENV_INPUT="$2"
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
        --force)          FORCE=1; shift ;;
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

if [ "$ADOPT_LEGACY" = "1" ] && [ "$FORCE" != "1" ]; then
    echo "Error: --adopt-legacy requires --force for install." >&2
    exit 1
fi

vq_extras_to_spec "$EXTRAS_PROFILE" EXTRAS_SPEC
vq_resolve_venv_path VENV_PATH "$VENV_INPUT"
vq_assert_safe_venv_target "$VENV_PATH"
vq_assert_mutable_runtime_target "$VENV_PATH" 0
if [ "$RESTART_DAEMON" = "1" ]; then
    cat >&2 <<EOF
Error: direct lifecycle scripts no longer stop and restart a serving vq daemon.
Use vq self-update or vq admin update for a managed serving-environment
transaction. Stop an inactive local development service yourself before a
direct install.
EOF
    exit 1
fi
RECOVERY_STATUS=0
RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
    VQ_LIFECYCLE_VENV="$VENV_PATH"
    trap 'vq_lifecycle_cleanup' EXIT
    vq_acquire_lifecycle_lock "$VENV_PATH" install-recovery
    vq_assert_mutable_runtime_target "$VENV_PATH" 0
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
    vq_recover_pending_venv_replacement_before_use \
        "$VENV_PATH" install "$DRY_RUN" || RECOVERY_STATUS=$?
    vq_release_lifecycle_lock
    trap - EXIT
    exit 1
fi
if [ -e "$VENV_PATH" ]; then
    if [ "$FORCE" = "1" ]; then
        vq_check_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
    fi
else
    [ "$ADOPT_LEGACY" != "1" ] || {
        echo "Error: --adopt-legacy requires an existing unmarked vq environment." >&2
        exit 1
    }
fi
if [ ! -e "$VENV_PATH" ] || [ "$FORCE" = "1" ]; then
    # Existing targets have proved checkout ownership before this may inspect
    # pyvenv.cfg or execute the resolved external base interpreter.
    vq_resolve_creation_python PYTHON_BIN "$PYTHON_BIN" "$VENV_PATH"
fi

echo "==> vq install"
echo "    source:       $VQ_PROJECT_DIR"
echo "    venv:         $VENV_PATH"
echo "    python:       $PYTHON_BIN"
echo "    capabilities: $EXTRAS_PROFILE  (pip suffix: '${EXTRAS_SPEC:-<core>}')"
echo "    mode:         $([ "$EDITABLE" = "1" ] && echo 'editable (-e)' || echo 'copied into the venv')"
echo "    daemon:       $(vq_daemon_describe "$VENV_PATH")"
[ "$FORCE" = "1" ] && echo "    --force:      replace the existing virtualenv"
[ "$ADOPT_LEGACY" = "1" ] && echo "    adoption:     exact legacy PEP 610 proof required"
[ "$DRY_RUN" = "1" ] && echo "    --dry-run:    no files will be changed"
echo

if [ "$DRY_RUN" = "1" ]; then
    if [ -e "$VENV_PATH" ]; then
        if [ "$FORCE" = "1" ]; then
            if [ -f "$VENV_PATH/pyvenv.cfg" ]; then
                echo "    [venv] exists and would be replaced atomically."
            else
                echo "    [venv] exists but is not recognisably a virtualenv; install would refuse."
            fi
        else
            echo "    [venv] exists; install would refuse without --force."
        fi
    else
        echo "    [venv] would be created."
    fi
    echo "    [pip] would install vq${EXTRAS_SPEC} from this checkout."
    echo "    [marker] would stamp checkout ownership and SOURCE-SHA after verification."
    if vq_daemon_is_running "$VENV_PATH"; then
        if [ "$RESTART_DAEMON" = "1" ]; then
            echo "    [daemon] would be stopped before, and started after, the replacement."
        else
                echo "    [daemon] is running; direct install refuses and requires a managed update."
        fi
    fi
    echo
    echo "==> Dry-run complete."
    exit 0
fi

VQ_LIFECYCLE_VENV="$VENV_PATH"
trap 'vq_lifecycle_cleanup' EXIT
vq_acquire_lifecycle_lock "$VENV_PATH" install
vq_assert_mutable_runtime_target "$VENV_PATH" 0
RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
    vq_recover_pending_venv_replacement_before_use \
        "$VENV_PATH" install 0 || RECOVERY_STATUS=$?
    vq_release_lifecycle_lock
    trap - EXIT
    exit 1
fi
# Direct install never owns a daemon restart. Prove the target inactive while
# holding the exact lifecycle lock and before legacy adoption or any target
# mutation.
vq_assert_daemon_stopped_or_managed \
    "$VENV_PATH" 0 "Replacing this environment"

if [ -e "$VENV_PATH" ]; then
    if [ "$FORCE" != "1" ]; then
        cat >&2 <<EOF
Error: '$VENV_PATH' already exists.

Update it in place:
    $SCRIPT_DIR/update.sh --skip-git --venv "$VENV_PATH"

Rebuild it from the current tree:
    $SCRIPT_DIR/reinstall.sh --venv "$VENV_PATH"

Or replace it after checking the path:
    $SCRIPT_DIR/install.sh --force --venv "$VENV_PATH"
EOF
        exit 1
    fi
    vq_require_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
fi

vq_begin_venv_replacement "$VENV_PATH" 0 1
vq_start_venv_replacement "$VENV_PATH"

echo "==> Creating virtualenv: $VENV_PATH"
"$PYTHON_BIN" -m venv "$VENV_PATH"
vq_install_environment "$VENV_PATH" "$EXTRAS_SPEC" "$EDITABLE"
vq_verify_environment "$VENV_PATH" "$EXTRAS_PROFILE"
vq_record_venv_ownership "$VENV_PATH"
vq_record_source_marker "$VENV_PATH" "$EDITABLE"
vq_record_install_metadata "$VENV_PATH" "$EXTRAS_PROFILE" "$EDITABLE"
vq_commit_venv_replacement

vq_release_lifecycle_lock
trap - EXIT

echo
echo "==> vq install complete."
echo
echo "    Activate it:"
echo "        source \"$VENV_PATH/bin/activate\""
echo
echo "    Check it:          $VENV_PATH/bin/vq daemon ping"
echo "    Submit a job:      $VENV_PATH/bin/vq submit --help"
echo "    Daemon setup:      $VQ_PROJECT_DIR/docs/lifecycle.md"
if vq_profile_has_web "$EXTRAS_PROFILE"; then
    echo "    Dashboard:         $VENV_PATH/bin/vq web run"
fi

#!/usr/bin/env bash
# Update a vq source install.
#
# USAGE
#     ./scripts/update.sh [OPTIONS]
#
# OPTIONS
#     --dev                 Switch to and fast-forward `main`.
#     --release             Switch to and fast-forward `release`.
#     --branch NAME         Switch to a branch, tag, or commit before install.
#     --ref NAME            Back-compatible spelling of --branch.
#                           With no selector, update the current branch.
#     --extras GROUP        core, web, test, dev, or all. Default: preserve the
#                           installed profile (core when no metadata exists).
#     --editable            Reinstall with `pip install -e`.
#     --copied              Reinstall as a non-editable copy. By default the
#                           installed mode is preserved: what pip recorded in
#                           PEP 610 `direct_url.json` first, then vq's own
#                           `.vq-install-metadata` note, then copied. A host
#                           that drives fleet rollouts must stay editable --
#                           vq resolves its controller checkout from where it
#                           is imported, and a copied install resolves into
#                           site-packages, which `vq admin rollout-latest`
#                           refuses.
#     --python BIN          Python for --recreate-venv (default: python3).
#     --venv PATH           Explicit venv. Default: auto-detect, preferring
#                           vibe-queue/.venv.
#     --recreate-venv       Safely replace the detected virtualenv.
#     --adopt-legacy        One-time adoption of an unmarked pre-marker vq
#                           environment after exact PEP 610 checkout proof.
#     --restart-daemon      Retired safety flag: now refuses and points to the
#                           durable `vq self-update` / `vq admin update` path.
#     --skip-git            Reinstall from the current tree without fetching.
#     --dry-run             Preview only; does not fetch or change files.
#     -h, --help            Show this help.
#
# EXAMPLES
#     ./scripts/update.sh
#     ./.venv/bin/vq self-update --expected-sha FULL_SHA
#     ./scripts/update.sh --release
#     ./scripts/update.sh --skip-git
#     ./scripts/update.sh --skip-git --recreate-venv
#
# Git operations apply to the whole vibe-qc checkout because vibe-queue is a
# peer subproject in that repository. The Python reinstall touches only the vq
# environment.
#
# NOT the same thing as `vq admin update`. That command updates a managed host's
# checkout and environment through the admin lifecycle, with drain handling
# and provenance verification. This script updates the local source install you
# are standing in. Direct use requires the selected environment not to own a
# running daemon; use `vq self-update` or `vq admin update` for a serving
# environment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_venv_helpers.sh
. "$SCRIPT_DIR/_venv_helpers.sh"

BRANCH=""
BRANCH_SOURCE=""
USE_CONFIGURED_UPSTREAM=0
UPSTREAM_REMOTE=""
UPSTREAM_BRANCH=""
UPSTREAM_DISPLAY=""
EXTRAS_PROFILE=""
EDITABLE=""
INSTALL_MODE=""
PYTHON_BIN="python3"
PYTHON_OPTION_SET=0
EXPLICIT_VENV=""
VENV_OPTION_SET=0
RECREATE_VENV=0
ADOPT_LEGACY=0
RESTART_DAEMON=0
ADMIN_MANAGED_DAEMON_RESTART_PID="${VQ_ADMIN_MANAGED_DAEMON_RESTART_PID:-}"
ADMIN_MANAGED_DAEMON_RESTART=0
SKIP_GIT=0
DRY_RUN=0

print_help() {
    awk '/^# USAGE/ {p=1} p && !/^#/ {exit} p {sub(/^# ?/, ""); print}' "$0"
}

set_branch() {
    local value="$1"
    local source="$2"
    if [ -n "$BRANCH" ]; then
        echo "Error: '$source' conflicts with $BRANCH_SOURCE ($BRANCH)." >&2
        echo "Choose only one of --dev / --release / --branch / --ref." >&2
        exit 1
    fi
    BRANCH="$value"
    BRANCH_SOURCE="$source"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dev)     set_branch main --dev; shift ;;
        --release) set_branch release --release; shift ;;
        --branch)
            vq_require_option_value "$1" "$#" "${2-}"
            set_branch "$2" "--branch $2"
            shift 2
            ;;
        --ref)
            vq_require_option_value "$1" "$#" "${2-}"
            set_branch "$2" "--ref $2"
            shift 2
            ;;
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
        --recreate-venv)  RECREATE_VENV=1; shift ;;
        --adopt-legacy)   ADOPT_LEGACY=1; shift ;;
        --restart-daemon) RESTART_DAEMON=1; shift ;;
        --skip-git)       SKIP_GIT=1; shift ;;
        --dry-run)        DRY_RUN=1; shift ;;
        -h|--help)        print_help; exit 0 ;;
        *)
            echo "Error: unknown argument '$1'." >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

if [ "$SKIP_GIT" = "1" ] && [ -n "$BRANCH" ]; then
    echo "Error: --skip-git cannot be combined with a branch or ref selector." >&2
    exit 1
fi
if [ "$PYTHON_OPTION_SET" = "1" ] && [ "$RECREATE_VENV" != "1" ]; then
    echo "Error: --python is only used with --recreate-venv." >&2
    exit 1
fi
if [ -n "$ADMIN_MANAGED_DAEMON_RESTART_PID" ]; then
    case "$ADMIN_MANAGED_DAEMON_RESTART_PID" in
        *[!0-9]*)
            echo "Error: VQ_ADMIN_MANAGED_DAEMON_RESTART_PID must be a numeric parent PID." >&2
            exit 1
            ;;
    esac
    if [ "$ADMIN_MANAGED_DAEMON_RESTART_PID" != "$PPID" ]; then
        echo "Error: the outer-admin restart handshake does not match this script's parent." >&2
        exit 1
    fi
fi

# A direct whole-venv replacement must never move the checkout.  Enforce this
# source-stability contract before inspecting the current Git branch: release
# candidates and other supported checkouts may be detached, but the safer
# --skip-git refusal is still the actionable result.  A managed outer admin
# transaction is authenticated against the exact target after path resolution
# below, so only its parent-PID presence defers this early direct-path check.
if [ "$RECREATE_VENV" = "1" ] && [ "$SKIP_GIT" != "1" ] && \
   [ -z "$ADMIN_MANAGED_DAEMON_RESTART_PID" ]; then
    cat >&2 <<EOF
Error: direct --recreate-venv requires --skip-git so crash recovery never
restores an old environment against a moved checkout. Use vq self-update or
vq admin update for an atomic source-and-environment update.
EOF
    exit 1
fi

if [ "$SKIP_GIT" != "1" ]; then
    if ! git -C "$VQ_REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        echo "Error: update.sh needs a Git checkout (or use --skip-git)." >&2
        exit 1
    fi
    if [ -z "$BRANCH" ]; then
        BRANCH="$(git -C "$VQ_REPO_ROOT" symbolic-ref --quiet --short HEAD || true)"
        if [ -z "$BRANCH" ]; then
            echo "Error: HEAD is detached; choose --dev, --release, or --branch NAME." >&2
            exit 1
        fi
        BRANCH_SOURCE="current branch"
        USE_CONFIGURED_UPSTREAM=1
        UPSTREAM_REMOTE="$(git -C "$VQ_REPO_ROOT" config --get "branch.$BRANCH.remote" || true)"
        UPSTREAM_BRANCH="$(git -C "$VQ_REPO_ROOT" config --get "branch.$BRANCH.merge" || true)"
        if [ -z "$UPSTREAM_REMOTE" ] || [ -z "$UPSTREAM_BRANCH" ]; then
            cat >&2 <<EOF
Error: current branch '$BRANCH' has no configured remote upstream.

Set one explicitly, for example:
    git -C "$VQ_REPO_ROOT" branch --set-upstream-to REMOTE/BRANCH "$BRANCH"

Or reinstall this local checkout without fetching:
    $SCRIPT_DIR/update.sh --skip-git

Or choose a remote branch explicitly:
    $SCRIPT_DIR/update.sh --branch NAME
EOF
            exit 1
        fi
        case "$UPSTREAM_BRANCH" in
            refs/heads/*) UPSTREAM_BRANCH="${UPSTREAM_BRANCH#refs/heads/}" ;;
            *)
                echo "Error: branch '$BRANCH' has unsupported upstream '$UPSTREAM_BRANCH'." >&2
                echo "Configure a single remote branch, or use --skip-git." >&2
                exit 1
                ;;
        esac
        if [ "$UPSTREAM_REMOTE" != "." ] && \
           ! git -C "$VQ_REPO_ROOT" remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1; then
            echo "Error: configured upstream remote '$UPSTREAM_REMOTE' does not exist." >&2
            echo "Repair the branch upstream, or use --skip-git." >&2
            exit 1
        fi
        UPSTREAM_DISPLAY="$(
            git -C "$VQ_REPO_ROOT" for-each-ref --format='%(upstream:short)' "refs/heads/$BRANCH"
        )"
        [ -n "$UPSTREAM_DISPLAY" ] || UPSTREAM_DISPLAY="$UPSTREAM_REMOTE/$UPSTREAM_BRANCH"
    fi
fi

if [ "$VENV_OPTION_SET" = "1" ]; then
    VENV_INPUT="$EXPLICIT_VENV"
else
    VENV_INPUT="${VQ_VENV:-}"
fi
vq_detect_venv VENV_PATH "$VENV_INPUT"
if [ "$RECREATE_VENV" = "1" ]; then
    if [ -n "$VENV_PATH" ]; then
        VENV_TARGET="$VENV_PATH"
    elif [ -n "$VENV_INPUT" ]; then
        VENV_TARGET="$VENV_INPUT"
    else
        VENV_TARGET="$VQ_PROJECT_DIR/.venv"
    fi
    vq_resolve_venv_path VENV_PATH "$VENV_TARGET"
elif [ -n "$VENV_PATH" ]; then
    VENV_TARGET="$VENV_PATH"
    vq_resolve_venv_path VENV_PATH "$VENV_TARGET"
fi
if [ -n "$ADMIN_MANAGED_DAEMON_RESTART_PID" ]; then
    [ -n "$VENV_PATH" ] || {
        echo "Error: the outer-admin restart handshake requires an exact virtualenv target." >&2
        exit 1
    }
    vibe_toolset_require_admin_restart_capability \
        "$VQ_REPO_ROOT" "$VENV_PATH" "$ADMIN_MANAGED_DAEMON_RESTART_PID"
    ADMIN_MANAGED_DAEMON_RESTART=1
fi
if [ -n "$VENV_PATH" ]; then
    vq_assert_mutable_runtime_target "$VENV_PATH" 1
fi
if [ "$ADMIN_MANAGED_DAEMON_RESTART" != "1" ]; then
    if [ "$RESTART_DAEMON" = "1" ]; then
        cat >&2 <<EOF
Error: direct lifecycle scripts no longer stop and restart a serving vq daemon.
Use the first-class managed transaction instead:
    $VENV_PATH/bin/vq self-update --expected-sha FULL_SHA
or:
    $VENV_PATH/bin/vq admin update ENV
EOF
        exit 1
    fi
    if [ "$RECREATE_VENV" = "1" ] && [ "$SKIP_GIT" != "1" ]; then
        cat >&2 <<EOF
Error: direct --recreate-venv requires --skip-git so crash recovery never
restores an old environment against a moved checkout. Use vq self-update or
vq admin update for an atomic source-and-environment update.
EOF
        exit 1
    fi
fi
RECOVERY_STATUS=0
if [ -n "$VENV_PATH" ]; then
    RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
    if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
        VQ_LIFECYCLE_VENV="$VENV_PATH"
        trap 'vq_lifecycle_cleanup' EXIT
        vq_acquire_lifecycle_lock "$VENV_PATH" update-recovery
        vq_assert_mutable_runtime_target "$VENV_PATH" 0
        vq_assert_daemon_stopped_or_managed \
            "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
        vq_recover_pending_venv_replacement_before_use \
            "$VENV_PATH" update "$DRY_RUN" || RECOVERY_STATUS=$?
        vq_release_lifecycle_lock
        trap - EXIT
        exit 1
    fi
fi
if [ -n "$VENV_PATH" ]; then
    vq_assert_safe_venv_target "$VENV_PATH"
    vq_check_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
elif [ "$ADOPT_LEGACY" = "1" ]; then
    echo "Error: --adopt-legacy requires an existing unmarked vq environment." >&2
    exit 1
fi
if [ "$RECREATE_VENV" = "1" ]; then
    # Ownership of an existing target was proved without executing it above.
    vq_resolve_creation_python PYTHON_BIN "$PYTHON_BIN" "$VENV_PATH"
fi

SAVED_PROFILE=""
SAVED_EDITABLE=""
INSTALLED_EDITABLE=""
if [ -n "$VENV_PATH" ]; then
    vq_load_install_metadata "$VENV_PATH" SAVED_PROFILE SAVED_EDITABLE || true
    vq_detect_installed_editable "$VENV_PATH" INSTALLED_EDITABLE || true
fi
[ -n "$EXTRAS_PROFILE" ] || EXTRAS_PROFILE="${SAVED_PROFILE:-core}"
# What pip recorded outranks vq's own sidecar note, and the sidecar outranks
# the copied default. The note is absent from every venv install.sh did not
# create -- including the plain `pip install -e` one CONTRIBUTING.md
# documents -- so defaulting past it converted editable installs to copied
# while reporting that the mode had been preserved.
if [ -z "$EDITABLE" ]; then
    EDITABLE="${INSTALLED_EDITABLE:-${SAVED_EDITABLE:-0}}"
fi
if [ -n "$INSTALLED_EDITABLE" ] && [ -n "$SAVED_EDITABLE" ] && \
   [ "$INSTALLED_EDITABLE" != "$SAVED_EDITABLE" ]; then
    echo "Warning: this venv records extras/mode '$SAVED_EDITABLE' but pip" >&2
    echo "         installed vq $([ "$INSTALLED_EDITABLE" = "1" ] && echo editable || echo copied)." \
        >&2
    echo "         Trusting pip; the note will be rewritten to match." >&2
fi
if [ "$INSTALLED_EDITABLE" = "1" ] && [ "$EDITABLE" = "0" ]; then
    cat >&2 <<'EOF'
Warning: replacing an editable install with a copied one.
         vq resolves its controller checkout from where vq is imported, so a
         copied install puts it inside site-packages and `vq admin
         rollout-latest` refuses on this host:
           vq runtime source .../lib/pythonX.Y is not a git checkout
         Re-run with --editable if this environment drives fleet rollouts.
EOF
fi
vq_extras_to_spec "$EXTRAS_PROFILE" EXTRAS_SPEC

echo "==> vq update"
echo "    checkout:     $VQ_REPO_ROOT"
if [ "$SKIP_GIT" = "1" ]; then
    echo "    Git:          unchanged (--skip-git)"
else
    echo "    Git target:   $BRANCH ($BRANCH_SOURCE)"
    if [ "$USE_CONFIGURED_UPSTREAM" = "1" ]; then
        echo "    Git upstream: $UPSTREAM_DISPLAY"
    fi
fi
echo "    venv:         ${VENV_PATH:-<not found>}"
echo "    capabilities: $EXTRAS_PROFILE  (pip suffix: '${EXTRAS_SPEC:-<core>}')"
echo "    mode:         $([ "$EDITABLE" = "1" ] && echo 'editable (-e)' || echo 'copied into the venv')"
echo "    daemon:       $(vq_daemon_describe "${VENV_PATH:-/nonexistent}")"
[ "$RECREATE_VENV" = "1" ] && echo "    --recreate-venv: replace the virtualenv"
[ "$ADOPT_LEGACY" = "1" ] && echo "    adoption:     exact legacy PEP 610 proof required"
[ "$DRY_RUN" = "1" ] && echo "    --dry-run: no files will be changed"
echo

if [ "$DRY_RUN" = "1" ]; then
    if [ "$SKIP_GIT" != "1" ]; then
        if [ "$USE_CONFIGURED_UPSTREAM" = "1" ]; then
            if [ "$UPSTREAM_REMOTE" = "." ]; then
                echo "    [git] would fast-forward '$BRANCH' from its local configured"
                echo "          upstream '$UPSTREAM_DISPLAY' without fetching."
            else
                echo "    [git] would fetch '$UPSTREAM_REMOTE' and fast-forward '$BRANCH'"
                echo "          from its configured upstream '$UPSTREAM_DISPLAY'."
            fi
        else
            echo "    [git] would fetch origin and fast-forward/check out '$BRANCH'."
        fi
        echo "          (dry-run deliberately does not fetch.)"
    fi
    if [ "$RECREATE_VENV" = "1" ] && [ -e "${VENV_PATH:-}" ]; then
        echo "    [venv] would replace atomically: $VENV_PATH"
    elif [ "$RECREATE_VENV" = "1" ]; then
        echo "    [venv] would create: $VENV_PATH"
    elif [ -n "$VENV_PATH" ]; then
        echo "    [venv] would update: $VENV_PATH"
    else
        echo "    [venv] none found; update would refuse and point to install.sh."
    fi
    echo "    [pip] would install vq${EXTRAS_SPEC} from this checkout."
    echo "    [marker] would restamp SOURCE-SHA after the install."
    if [ -n "$VENV_PATH" ] && vq_daemon_is_running "$VENV_PATH"; then
        if [ "$RESTART_DAEMON" = "1" ]; then
            echo "    [daemon] would be stopped before, and started after, the reinstall."
        elif [ "$ADMIN_MANAGED_DAEMON_RESTART" = "1" ]; then
            echo "    [daemon] restart is owned by the outer vq admin update."
        else
            echo "    [daemon] is running; direct update refuses and requires a managed update."
        fi
    fi
    echo
    echo "==> Dry-run complete."
    exit 0
fi

if [ -z "$VENV_PATH" ]; then
    cat >&2 <<EOF
Error: no vq virtualenv was found.

Create it once with:
    $SCRIPT_DIR/install.sh

Or name an existing one explicitly:
    $SCRIPT_DIR/update.sh --skip-git --venv PATH
EOF
    exit 1
fi

VQ_LIFECYCLE_VENV="$VENV_PATH"
trap 'vq_lifecycle_cleanup' EXIT
vq_acquire_lifecycle_lock "$VENV_PATH" update
vq_assert_mutable_runtime_target "$VENV_PATH" 1
RECEIPT_PATH="$(vq_venv_replacement_receipt_path "$VENV_PATH")"
if [ -e "$RECEIPT_PATH" ] || [ -L "$RECEIPT_PATH" ]; then
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Recovering an interrupted environment replacement"
    vq_recover_pending_venv_replacement_before_use \
        "$VENV_PATH" update 0 || RECOVERY_STATUS=$?
    vq_release_lifecycle_lock
    trap - EXIT
    exit 1
fi
if [ "$ADMIN_MANAGED_DAEMON_RESTART" = "1" ]; then
    # The inherited lock descriptors serialize the transaction; they are not
    # evidence that the outer process actually stopped the service. Re-probe
    # the exact venv here, before any build/checkout/venv work, and fail closed.
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Updating this environment under outer-admin control"
else
    # Direct updates never own a daemon restart. Prove the target inactive
    # immediately after taking the exact lifecycle lock and before ownership
    # adoption, build-lock acquisition, or durable replacement admission.
    vq_assert_daemon_stopped_or_managed \
        "$VENV_PATH" 0 "Updating this environment"
fi
if [ "$SKIP_GIT" != "1" ]; then
    # Interoperate with direct native build entry points that hold only the
    # checkout build lock. Global acquisition order is lifecycle, then build.
    # The build lock is a CROSS-COMPONENT lock, and vibe-queue no longer
    # shares a checkout with anything that builds native code. In the
    # monorepo, `scripts/_build_lock.sh` sat in the shared top-level
    # scripts/ directory (VQ_REPO_ROOT was one level up) and serialized a vq
    # update against a vibe-qc native build in the same tree. Since the
    # 2026-09-08 split VQ_REPO_ROOT is this repository, which carries no
    # native build entry points, so there is nothing to interoperate with --
    # see the same reasoning in scripts/_venv_helpers.sh.
    #
    # Acquire it when it IS present (a monorepo-shaped checkout still shares
    # a tree with vibe-qc), and skip it otherwise. Hard-requiring it made
    # `vq admin update vibeqc-queue <host>` fail on every host the moment
    # that host was repointed at the split repository.
    if [ -f "$VQ_REPO_ROOT/scripts/_build_lock.sh" ]; then
        # shellcheck source=./_build_lock.sh
        . "$VQ_REPO_ROOT/scripts/_build_lock.sh"
        vibeqc_acquire_build_lock
    fi
fi

VENV_PREFLIGHT_COMPLETE=0
prepare_venv_update() {
    [ "$VENV_PREFLIGHT_COMPLETE" = "0" ] || return 0
    if [ "$RECREATE_VENV" = "1" ]; then
        # Arm cleanup before checkout mutation. The old venv stays in place
        # until replacement work begins and is restored after any later error.
        if [ "$ADMIN_MANAGED_DAEMON_RESTART" = "1" ]; then
            vq_begin_venv_replacement "$VENV_PATH" "$ADOPT_LEGACY" 0
        else
            vq_begin_venv_replacement "$VENV_PATH" "$ADOPT_LEGACY" 1
        fi
    else
        vq_require_venv_ownership "$VENV_PATH" "$ADOPT_LEGACY"
        if ! vq_check_venv_health "$VENV_PATH"; then
            echo "Re-run with --recreate-venv to replace it safely." >&2
            exit 1
        fi
    fi
    VENV_PREFLIGHT_COMPLETE=1
}
prepare_venv_update

if [ "$ADMIN_MANAGED_DAEMON_RESTART" = "1" ]; then
    echo "==> The outer vq admin transaction stopped the daemon and owns its verified restart."
fi

if [ "$SKIP_GIT" != "1" ]; then
    if ! git -C "$VQ_REPO_ROOT" diff --quiet || ! git -C "$VQ_REPO_ROOT" diff --cached --quiet; then
        echo "Error: the checkout has uncommitted tracked changes." >&2
        echo "Commit, stash, or revert them before updating." >&2
        exit 1
    fi

    if [ "$USE_CONFIGURED_UPSTREAM" = "1" ]; then
        if [ "$UPSTREAM_REMOTE" = "." ]; then
            echo "==> Using local configured upstream '$UPSTREAM_DISPLAY' (no fetch)."
        else
            echo "==> Fetching configured upstream remote '$UPSTREAM_REMOTE'..."
            git -C "$VQ_REPO_ROOT" fetch --quiet "$UPSTREAM_REMOTE" --tags
        fi
        if ! git -C "$VQ_REPO_ROOT" rev-parse --verify \
            "${BRANCH}@{upstream}^{commit}" >/dev/null 2>&1; then
            echo "Error: configured upstream '$UPSTREAM_DISPLAY' was not available after fetch." >&2
            echo "Repair the branch tracking configuration, or use --skip-git." >&2
            exit 1
        fi
        echo "==> Updating '$BRANCH' by fast-forwarding from '$UPSTREAM_DISPLAY'..."
        git -C "$VQ_REPO_ROOT" merge --ff-only --quiet "${BRANCH}@{upstream}"
    else
        echo "==> Fetching origin..."
        git -C "$VQ_REPO_ROOT" fetch --quiet origin --tags
        if git -C "$VQ_REPO_ROOT" show-ref --verify --quiet "refs/heads/$BRANCH"; then
            echo "==> Switching to '$BRANCH' and fast-forwarding..."
            git -C "$VQ_REPO_ROOT" checkout --quiet "$BRANCH"
            git -C "$VQ_REPO_ROOT" pull --ff-only --quiet origin "$BRANCH"
        elif git -C "$VQ_REPO_ROOT" show-ref --verify --quiet "refs/tags/$BRANCH"; then
            echo "==> Checking out tag '$BRANCH' (detached HEAD)..."
            git -C "$VQ_REPO_ROOT" checkout --quiet "$BRANCH"
        elif git -C "$VQ_REPO_ROOT" show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
            echo "==> Creating local branch '$BRANCH' tracking origin/$BRANCH..."
            git -C "$VQ_REPO_ROOT" checkout --quiet -b "$BRANCH" "origin/$BRANCH"
        elif git -C "$VQ_REPO_ROOT" cat-file -e "$BRANCH^{commit}" 2>/dev/null; then
            echo "==> Checking out commit '$BRANCH' (detached HEAD)..."
            git -C "$VQ_REPO_ROOT" checkout --quiet --detach "$BRANCH"
        else
            echo "Error: '$BRANCH' is not a known branch, tag, or commit." >&2
            exit 1
        fi
    fi
    echo "    now at $(git -C "$VQ_REPO_ROOT" rev-parse --short HEAD): $(git -C "$VQ_REPO_ROOT" log -1 --format='%s')"
fi

if [ "$RECREATE_VENV" = "1" ]; then
    vq_start_venv_replacement "$VENV_PATH"
    echo "==> Creating replacement virtualenv: $VENV_PATH"
    "$PYTHON_BIN" -m venv "$VENV_PATH"
fi
vq_install_environment "$VENV_PATH" "$EXTRAS_SPEC" "$EDITABLE"
vq_verify_environment "$VENV_PATH" "$EXTRAS_PROFILE"
vq_record_venv_ownership "$VENV_PATH"
vq_record_source_marker "$VENV_PATH" "$EDITABLE"
vq_record_install_metadata "$VENV_PATH" "$EXTRAS_PROFILE" "$EDITABLE"
if [ "$RECREATE_VENV" = "1" ]; then
    vq_commit_venv_replacement
fi

vq_release_lifecycle_lock
trap - EXIT

echo
echo "==> vq update complete."
echo "    Version: $("$VENV_PATH/bin/vq" --version | head -n 1)"

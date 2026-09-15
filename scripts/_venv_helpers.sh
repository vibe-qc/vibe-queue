#!/usr/bin/env bash
# Shared virtual-environment helpers for vq's shell entry points.
#
# Sourced by install.sh, update.sh, reinstall.sh, and uninstall.sh so every
# command agrees on the canonical source environment:
#
#     <checkout>/vibe-queue/.venv
#
# vq differs from its sibling subprojects in three ways that shape everything
# here:
#
#   1. A vq install usually has a DAEMON running out of it. Replacing a venv
#      under a live daemon swaps code beneath a running process; the daemon is
#      stopped first and restarted after, or the script refuses.
#   2. The installed package carries a SOURCE-SHA provenance marker that daemon
#      RPC reports and scheduler-compat gates believe. It is written after pip
#      install and is not tracked by the wheel, so every install path here
#      restamps it. Skipping that is what left compute-host reporting a commit it
#      was not running (2026-08-02).
#   3. ~/.local/share/vq holds the live queue and job history. It is user data.
#      Nothing here deletes it without an explicit, separate opt-in.
#
# Root-owned multi-user installs (/opt/vq) are deliberately NOT handled by
# these scripts. See docs/multi_user_deployment.md and the fleet runbook.

VQ_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
VQ_PROJECT_DIR="$(cd "$VQ_SCRIPT_DIR/.." && pwd -P)"
# vibe-queue is its own repository: the project directory IS the repo root.
# In the former monorepo this was one level up, and the lifecycle lock lived
# in the shared top-level scripts/ directory. Each component now carries its
# own copy -- the cross-component lock only ever mattered when several
# components shared one checkout and one venv target.
VQ_REPO_ROOT="$VQ_PROJECT_DIR"
# shellcheck source=./_lifecycle_lock.sh
. "$VQ_SCRIPT_DIR/_lifecycle_lock.sh"

VQ_MIN_PYTHON_MAJOR=3
VQ_MIN_PYTHON_MINOR=12

# Value-taking flags must never consume the next option as their value. A
# relative path that genuinely begins with '-' remains expressible as './-x'.
vq_require_option_value() {
    local option="$1"
    local remaining="$2"
    local value="${3-}"

    if [ "$remaining" -lt 2 ] || [ -z "$value" ]; then
        echo "Error: $option requires a non-empty argument." >&2
        return 1
    fi
    case "$value" in
        -*)
            echo "Error: $option requires a value, not option-like argument '$value'." >&2
            return 1
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Paths and discovery
# ---------------------------------------------------------------------------

vq_strip_final_path_syntax() {
    local target="$1"
    local previous=""

    while [ "$target" != "$previous" ]; do
        previous="$target"
        while [ "$target" != "/" ] && [ "${target%/}" != "$target" ]; do
            target="${target%/}"
        done
        case "$target" in
            */.) target="${target%/.}" ;;
        esac
    done
    [ -n "$target" ] || target="/"
    printf '%s\n' "$target"
}

vq_assert_no_parent_path_components() {
    local target="$1"
    local kind="$2"

    case "/$target/" in
        */../*)
            echo "Error: refusing $kind target with a '..' path component: '$target'." >&2
            echo "Use the resolved path explicitly." >&2
            return 1
            ;;
    esac
}

vq_assert_not_symlinked_venv_target() {
    local target="$1"
    local lexical=""

    lexical="$(vq_strip_final_path_syntax "$target")"
    if [ -L "$lexical" ]; then
        echo "Error: refusing symlinked virtualenv target '$target'." >&2
        echo "Use the real directory path after verifying its ownership." >&2
        return 1
    fi
}

vq_path_from_project() {
    local raw="$1"
    case "$raw" in
        /*) printf '%s\n' "$raw" ;;
        *)  printf '%s/%s\n' "$VQ_PROJECT_DIR" "$raw" ;;
    esac
}

vq_resolve_venv_path() {
    local out_var="$1"
    local raw="$2"
    local resolved=""
    local absolute

    case "$raw" in
        /*) absolute="$raw" ;;
        *)  absolute="$VQ_PROJECT_DIR/$raw" ;;
    esac
    vq_assert_not_symlinked_venv_target "$absolute"
    # Canonicalise without executing anything from the selected target. A
    # foreign venv may contain an arbitrary bin/python or sitecustomize.py;
    # path resolution is a shell/filesystem operation, not an import hook.
    resolved="$(vq_canonical_path "$absolute")" || resolved="$absolute"
    printf -v "$out_var" '%s' "$resolved"
}

vq_detect_venv() {
    local out_var="$1"
    local explicit="${2:-}"
    local requested="${explicit:-${VQ_VENV:-}}"
    local found=""
    local candidate
    local candidates=()

    if [ -n "$requested" ]; then
        candidates+=("$(vq_path_from_project "$requested")")
    else
        # Only unambiguously vq-owned environments are auto-selected. An
        # activated VIRTUAL_ENV is deliberately NOT consulted: on this repo it
        # is usually vibe-qc's much heavier compiled environment, and these
        # scripts mutate what they find.
        candidates+=(
            "$VQ_PROJECT_DIR/.venv"
            "$VQ_PROJECT_DIR/venv"
            "$VQ_PROJECT_DIR/.venv-vq"
            "$VQ_REPO_ROOT/.venv-vq"
        )
    fi

    for candidate in "${candidates[@]}"; do
        if [ -x "$candidate/bin/python" ] || \
           { [ -f "$candidate/pyvenv.cfg" ] && [ ! -L "$candidate/pyvenv.cfg" ]; }; then
            vq_assert_safe_venv_target "$candidate"
            found="$candidate"
            break
        fi
    done

    printf -v "$out_var" '%s' "$found"
}

vq_command_path_noexec() {
    local out_var="$1"
    local requested="$2"
    local candidate=""
    local link=""
    local parent=""
    local hops=0

    case "$requested" in
        */*) candidate="$requested" ;;
        *) candidate="$(command -v "$requested" 2>/dev/null || true)" ;;
    esac
    [ -n "$candidate" ] || return 1
    case "$candidate" in
        /*) ;;
        */*) candidate="$PWD/$candidate" ;;
        *) return 1 ;;
    esac

    while :; do
        parent="$(dirname "$candidate")"
        [ -d "$parent" ] || return 1
        candidate="$(cd "$parent" 2>/dev/null && pwd -P)/$(basename "$candidate")"
        [ -L "$candidate" ] || break
        link="$(readlink "$candidate" 2>/dev/null || true)"
        [ -n "$link" ] || return 1
        case "$link" in
            /*) candidate="$link" ;;
            *) candidate="$(dirname "$candidate")/$link" ;;
        esac
        hops=$((hops + 1))
        [ "$hops" -le 40 ] || return 1
    done
    [ -x "$candidate" ] || return 1
    printf -v "$out_var" '%s' "$candidate"
}

vq_path_is_within() {
    local path="$1"
    local root="$2"

    case "$path" in
        "$root"|"$root"/*) return 0 ;;
        *) return 1 ;;
    esac
}

# Fedora Atomic/Silverblue exposes ordinary user homes through /home while the
# physical path is /var/home/<user>. Permit that one system-root exception only
# when HOME resolves to the same boundary and the home directory belongs to the
# current uid. Git metadata is checked before this exception, and the target's
# own ownership is checked later when it already exists.
vq_is_current_atomic_home_descendant() {
    local canonical="$1"
    local home_real=""
    local home_owner_uid=""
    local current_uid=""

    [ -n "${HOME:-}" ] && [ -d "$HOME" ] || return 1
    home_real="$(cd "$HOME" 2>/dev/null && pwd -P)" || return 1
    case "$home_real" in
        /var/home/?*|/private/var/home/?*) ;;
        *) return 1 ;;
    esac
    home_owner_uid="$(stat -c '%u' "$home_real" 2>/dev/null || stat -f '%u' "$home_real" 2>/dev/null || true)"
    current_uid="$(id -u)"
    [ -n "$home_owner_uid" ] && [ "$home_owner_uid" = "$current_uid" ] || return 1
    case "$canonical" in
        "$home_real"/*) return 0 ;;
        *) return 1 ;;
    esac
}

vq_assert_python() {
    local python_bin="$1"

    if [ ! -x "$python_bin" ]; then
        echo "Error: Python interpreter '$python_bin' was not found." >&2
        return 1
    fi
    if ! PYTHONNOUSERSITE=1 "$python_bin" -I -S -c \
        "import sys; raise SystemExit(sys.version_info < ($VQ_MIN_PYTHON_MAJOR, $VQ_MIN_PYTHON_MINOR))"; then
        echo "Error: vq requires Python $VQ_MIN_PYTHON_MAJOR.$VQ_MIN_PYTHON_MINOR or newer." >&2
        return 1
    fi
}

# A lifecycle command is often launched from an activated copy of the very
# venv it is about to replace. Resolve that transient interpreter to CPython's
# base executable before the target is moved aside; otherwise `python -m venv`
# disappears halfway through the transaction. A standalone interpreter that
# still lives under the protected target is unsafe and is rejected.
vq_resolve_creation_python() {
    local out_var="$1"
    local requested="$2"
    local target="$3"
    local selected_python=""
    local base_python=""
    local recorded_python=""
    local config_file=""
    local target_real=""

    target_real="$(vq_canonical_path "$(vq_strip_final_path_syntax "$target")")" || return
    if ! vq_command_path_noexec selected_python "$requested"; then
        echo "Error: Python interpreter '$requested' was not found." >&2
        return 1
    fi
    if vq_path_is_within "$selected_python" "$target_real"; then
        # A copied venv interpreter does not resolve through a symlink. Read the
        # standard library's recorded base executable without running it.
        config_file="$target_real/pyvenv.cfg"
        if [ ! -f "$config_file" ] || [ -L "$config_file" ]; then
            echo "Error: refusing to execute Python from the unproven target '$target'." >&2
            return 1
        fi
        recorded_python="$(sed -n 's/^executable = //p' "$config_file")"
        if [ -z "$recorded_python" ] || \
           [ "$(printf '%s\n' "$recorded_python" | wc -l | tr -d ' ')" != "1" ] || \
           ! vq_command_path_noexec selected_python "$recorded_python" || \
           vq_path_is_within "$selected_python" "$target_real"; then
            echo "Error: pyvenv.cfg does not name a safe base Python outside '$target'." >&2
            return 1
        fi
    fi

    vq_assert_python "$selected_python" || return
    base_python="$(PYTHONNOUSERSITE=1 "$selected_python" -I -S -c '
import os
import sys

base = getattr(sys, "_base_executable", None) or sys.executable
print(os.path.realpath(base))
' 2>/dev/null || true)"
    base_python="${base_python%%$'\n'*}"
    if [ -z "$base_python" ] || \
       ! vq_command_path_noexec base_python "$base_python"; then
        echo "Error: could not resolve a usable base interpreter from '$requested'." >&2
        return 1
    fi
    vq_assert_python "$base_python" || return
    if vq_path_is_within "$base_python" "$target_real"; then
        echo "Error: Python interpreter '$requested' resolves inside the virtualenv target." >&2
        echo "Choose a base Python outside '$target'." >&2
        return 1
    fi
    printf -v "$out_var" '%s' "$base_python"
}

vq_assert_not_protected_system_target() {
    local canonical="$1"
    local kind="$2"

    case "$canonical" in
        */.git|*/.git/*)
            echo "Error: refusing $kind target inside Git metadata: '$canonical'." >&2
            return 1
            ;;
    esac
    if vq_is_current_atomic_home_descendant "$canonical"; then
        return 0
    fi
    case "$canonical" in
        /tmp/*|/private/tmp/*|/private/var/folders/*)
            return 0
            ;;
        /var/lib/vq|/private/var/lib/vq)
            # Dedicated checks below provide the multi-user remediation.
            return 0
            ;;
        /Applications|/Applications/*|/bin|/bin/*|/boot|/boot/*|/dev|/dev/*|\
        /etc|/etc/*|/home|/Library|/Library/*|/media|/mnt|/nix|/nix/*|\
        /opt|/opt/*|/private|/private/etc|/private/etc/*|\
        /private/var|/private/var/*|/proc|/proc/*|/run|/run/*|/sbin|/sbin/*|\
        /snap|/snap/*|/srv|/srv/*|/System|/System/*|/sys|/sys/*|/tmp|/usr|/usr/*|\
        /Users|/var|/var/*|/Volumes)
            echo "Error: refusing $kind target under protected system path '$canonical'." >&2
            return 1
            ;;
    esac
}

vq_assert_safe_venv_target() {
    local target="$1"
    local lexical=""
    local canonical=""
    local home_real=""
    local owner_uid=""
    local current_uid=""

    [ -n "$target" ] || {
        echo "Error: refusing an empty virtualenv target." >&2
        return 1
    }
    lexical="$(vq_strip_final_path_syntax "$target")"
    vq_assert_no_parent_path_components "$lexical" "virtualenv" || return
    vq_assert_not_symlinked_venv_target "$lexical" || return
    canonical="$(vq_canonical_path "$lexical")" || {
        echo "Error: cannot resolve virtualenv target '$target'." >&2
        return 1
    }
    vq_assert_not_protected_system_target "$canonical" "virtualenv" || return

    if [ -n "${HOME:-}" ]; then
        home_real="$(vq_canonical_path "$HOME" 2>/dev/null || true)"
        case "$home_real" in
            /*) ;;
            *) home_real="" ;;
        esac
    fi

    case "$canonical" in
        ""|/|"$VQ_SCRIPT_DIR"|"$VQ_PROJECT_DIR"|"$VQ_REPO_ROOT")
            echo "Error: refusing unsafe virtualenv target '$target'." >&2
            return 1
            ;;
        /opt/vq|/opt/vq/*)
            cat >&2 <<'EOF'
Error: refusing to operate on a root-owned multi-user install under /opt/vq.

That install is the privilege boundary and is managed separately:
    vibe-queue/docs/multi_user_deployment.md
    vibe-queue/docs/fleet_update_runbook.md  (multi-user refresh)
EOF
            return 1
            ;;
        /var/lib/vq|/private/var/lib/vq)
            echo "Error: refusing root-owned multi-user virtualenv target '$target'." >&2
            echo "Use the audited multi-user deployment or retirement procedure instead." >&2
            return 1
            ;;
    esac
    case "$VQ_REPO_ROOT/" in
        "$canonical/"*)
            echo "Error: refusing virtualenv target '$target': it contains the vibe-qc checkout." >&2
            return 1
            ;;
    esac
    if [ -n "$home_real" ]; then
        case "$home_real/" in
            "$canonical/"*)
                echo "Error: refusing virtualenv target '$target': it contains the home directory." >&2
                return 1
                ;;
        esac
    fi
    if [ -e "$lexical" ] && [ ! -d "$lexical" ]; then
        echo "Error: virtualenv target exists but is not a directory: $target" >&2
        return 1
    fi
    if [ -e "$lexical" ]; then
        owner_uid="$(stat -c '%u' "$lexical" 2>/dev/null || stat -f '%u' "$lexical" 2>/dev/null || true)"
        current_uid="$(id -u)"
        if [ -z "$owner_uid" ] || [ "$owner_uid" != "$current_uid" ]; then
            echo "Error: refusing unowned virtualenv target '$target'." >&2
            echo "Target owner uid: ${owner_uid:-<unknown>}; current uid: $current_uid." >&2
            return 1
        fi
    fi
}

vq_assert_mutable_runtime_target() {
    local target="$1"
    local allow_admin_slot_build="${2:-0}"
    local immutable_marker="$target/.vq-immutable-runtime"
    local generation=""
    local state_marker=""
    local authorized=0

    if [ -e "$immutable_marker" ] || [ -L "$immutable_marker" ]; then
        if [ ! -f "$immutable_marker" ] || [ -L "$immutable_marker" ]; then
            echo "Error: immutable-runtime marker is not a regular file: $immutable_marker" >&2
        else
            echo "Error: refusing to mutate an immutable runtime generation: $target" >&2
        fi
        echo "Publish a new generation and atomically switch its stable pointer instead." >&2
        return 1
    fi
    if [[ "$target" =~ ^(.+)/releases/([0-9a-f]{40})/source/\.venv$ ]]; then
        generation="${BASH_REMATCH[1]}/releases/${BASH_REMATCH[2]}"
        state_marker="$generation/.vq-runtime-slot-state"
        if [ "$allow_admin_slot_build" = "1" ] && \
           [ "${_VIBE_TOOLSET_ADMIN_LOCK_PRESENT:-0}" = "1" ] && \
           [ "${VIBE_TOOLSET_LIFECYCLE_LOCK_TARGET:-}" = "$target" ] && \
           [ -f "$state_marker" ] && [ ! -L "$state_marker" ] && \
           PYTHONNOUSERSITE=1 "$_VIBE_TOOLSET_ADMIN_LOCK_PYTHON" -I -S -c '
import json, pathlib, re, stat, sys
path = pathlib.Path(sys.argv[1])
sha = sys.argv[2]
info = path.lstat()
value = json.loads(path.read_text(encoding="utf-8"))
valid = (
    stat.S_ISREG(info.st_mode)
    and info.st_nlink == 1
    and set(value) == {"schema", "kind", "id", "transaction", "state"}
    and type(value.get("schema")) is int
    and value.get("schema") == 1
    and value.get("kind") == "vq-runtime-slot-build"
    and value.get("id") == sha
    and value.get("state") == "building"
    and isinstance(value.get("transaction"), str)
    and re.fullmatch(r"[0-9a-f]{32}", value["transaction"])
)
raise SystemExit(0 if valid else 1)
' "$state_marker" "${BASH_REMATCH[2]}"; then
            authorized=1
        fi
        if [ "$authorized" != "1" ]; then
            echo "Error: refusing direct in-place mutation of runtime slot $generation" >&2
            echo "Only the exact vq admin build transaction may populate a new slot." >&2
            return 1
        fi
    fi
}

vq_canonical_path() {
    local target="$1"
    local parent=""
    local suffix=""
    local component=""
    local ancestor=""
    local parent_real=""

    if [ -d "$target" ]; then
        (cd "$target" 2>/dev/null && pwd -P)
        return
    fi
    if [ -e "$target" ] || [ -L "$target" ]; then
        return 1
    fi
    parent="$(dirname "$target")"
    suffix="$(basename "$target")"
    while [ ! -d "$parent" ]; do
        if [ -e "$parent" ] || [ -L "$parent" ]; then
            return 1
        fi
        component="$(basename "$parent")"
        case "$component" in
            ""|.|..|/) return 1 ;;
        esac
        suffix="$component/$suffix"
        ancestor="$(dirname "$parent")"
        [ "$ancestor" != "$parent" ] || return 1
        parent="$ancestor"
    done
    parent_real="$(cd "$parent" 2>/dev/null && pwd -P)" || return
    if [ "$parent_real" = "/" ]; then
        printf '/%s\n' "$suffix"
    else
        printf '%s/%s\n' "$parent_real" "$suffix"
    fi
}

# State and config roots are operator-selected through environment variables,
# so an explicit purge flag is not enough by itself: a typo such as
# VQ_STATE_DIR=<checkout> must never turn uninstall into `rm -rf <checkout>`.
# Custom per-user paths remain supported, but broad/system paths, symlinks,
# home, and anything containing this checkout are rejected.
vq_assert_safe_data_target() {
    local target="$1"
    local kind="$2"
    local lexical=""
    local canonical=""
    local home_real=""
    local multi_user_real=""
    local owner_uid=""
    local current_uid=""

    [ -n "$target" ] || {
        echo "Error: refusing an empty $kind directory target." >&2
        return 1
    }
    lexical="$(vq_strip_final_path_syntax "$target")"
    vq_assert_no_parent_path_components "$lexical" "$kind directory" || return
    if [ -L "$lexical" ]; then
        echo "Error: refusing symlinked $kind directory '$target'." >&2
        echo "Remove the symlink manually after checking its destination." >&2
        return 1
    fi
    canonical="$(vq_canonical_path "$lexical")"
    case "$canonical" in
        /*) ;;
        *)
            echo "Error: refusing relative $kind directory '$target'." >&2
            return 1
            ;;
    esac
    vq_assert_not_protected_system_target "$canonical" "$kind directory" || return
    if [ -n "${HOME:-}" ] && [ -d "$HOME" ]; then
        home_real="$(cd "$HOME" 2>/dev/null && pwd -P || true)"
    fi
    if [ -n "${VQ_MULTI_USER_ROOT:-}" ]; then
        multi_user_real="$(vq_canonical_path "$VQ_MULTI_USER_ROOT" 2>/dev/null || true)"
    fi

    case "$canonical" in
        /|/bin|/boot|/dev|/etc|/home|/Library|/opt|/private|/private/var|/private/var/tmp|/root|/run|/sbin|/srv|/System|/tmp|/usr|/Users|/var)
            echo "Error: refusing unsafe $kind directory '$target'." >&2
            return 1
            ;;
    esac
    if [ -n "$home_real" ] && [ "$canonical" = "$home_real" ]; then
        echo "Error: refusing to purge the home directory as vq $kind." >&2
        return 1
    fi
    case "$canonical" in
        /var/lib/vq|/private/var/lib/vq)
            echo "Error: refusing multi-user $kind root '$target'." >&2
            echo "Use the audited multi-user retirement procedure instead." >&2
            return 1
            ;;
    esac
    if [ -n "$multi_user_real" ] && [ "$canonical" = "$multi_user_real" ]; then
        echo "Error: refusing configured multi-user $kind root '$target'." >&2
        echo "Use the audited multi-user retirement procedure instead." >&2
        return 1
    fi
    case "$VQ_REPO_ROOT/" in
        "$canonical/"*)
            echo "Error: refusing $kind directory '$target': it contains the vibe-qc checkout." >&2
            return 1
            ;;
    esac
    case "$canonical/" in
        "$VQ_REPO_ROOT/"*)
            echo "Error: refusing $kind directory '$target': it is inside the vibe-qc checkout." >&2
            return 1
            ;;
    esac
    if [ "$(dirname "$(dirname "$canonical")")" = "/" ]; then
        echo "Error: refusing broad $kind directory '$target'." >&2
        echo "Use a dedicated child directory (for example /var/lib/vq)." >&2
        return 1
    fi
    if [ -e "$lexical" ] && [ ! -d "$lexical" ]; then
        echo "Error: $kind target exists but is not a directory: $target" >&2
        return 1
    fi
    if [ -e "$lexical" ]; then
        owner_uid="$(stat -c '%u' "$lexical" 2>/dev/null || stat -f '%u' "$lexical" 2>/dev/null || true)"
        current_uid="$(id -u)"
        if [ -z "$owner_uid" ] || [ "$owner_uid" != "$current_uid" ]; then
            echo "Error: refusing unowned $kind directory '$target'." >&2
            echo "Target owner uid: ${owner_uid:-<unknown>}; current uid: $current_uid." >&2
            return 1
        fi
    fi
}

# ---------------------------------------------------------------------------
# Checkout ownership
# ---------------------------------------------------------------------------

VQ_OWNERSHIP_MARKER_NAME=".vq-checkout-owner"

vq_find_external_inspector() {
    local out_var="$1"
    local target="$2"
    local target_real=""
    local candidate=""
    local resolved_candidate=""
    local -a candidates=(
        /usr/bin/python3
        /usr/local/bin/python3
        /opt/homebrew/bin/python3
        /opt/local/bin/python3
        python3
    )

    target_real="$(vq_canonical_path "$(vq_strip_final_path_syntax "$target")")" || return
    for candidate in "${candidates[@]}"; do
        resolved_candidate=""
        vq_command_path_noexec resolved_candidate "$candidate" || continue
        vq_path_is_within "$resolved_candidate" "$target_real" && continue
        if PYTHONNOUSERSITE=1 "$resolved_candidate" -I -S -c \
            'import sys; raise SystemExit(sys.version_info < (3, 8))' \
            >/dev/null 2>&1; then
            printf -v "$out_var" '%s' "$resolved_candidate"
            return 0
        fi
    done
    echo "Error: no isolated external Python is available to inspect legacy PEP 610 metadata." >&2
    return 1
}

vq_legacy_pep610_matches_checkout() {
    local target="$1"
    local inspector=""

    vq_find_external_inspector inspector "$target" || return
    PYTHONNOUSERSITE=1 "$inspector" -I -S - "$target" "$VQ_PROJECT_DIR" <<'PY'
import glob
import json
import os
import pathlib
import sys
import urllib.parse

root = pathlib.Path(sys.argv[1])
expected = os.path.realpath(sys.argv[2])
patterns = (
    root / "lib" / "python*" / "site-packages" / "vq-*.dist-info" / "direct_url.json",
    root / "lib64" / "python*" / "site-packages" / "vq-*.dist-info" / "direct_url.json",
)
matches = sorted({pathlib.Path(path) for pattern in patterns for path in glob.glob(str(pattern))})
safe_matches = []
for candidate in matches:
    if not candidate.is_file():
        raise SystemExit(1)
    cursor = candidate
    safe = True
    while cursor != root:
        if cursor.is_symlink():
            safe = False
            break
        parent = cursor.parent
        if parent == cursor or root not in parent.parents and parent != root:
            raise SystemExit(1)
        cursor = parent
    if safe:
        safe_matches.append(candidate)

# Linux virtualenvs commonly expose one real ``lib`` tree through a
# ``lib64 -> lib`` compatibility alias.  Trust exactly one symlink-free
# metadata path, then require every lexical glob hit to name that same inode.
# A symlink to a different payload, two independent metadata files, or a
# metadata tree reachable only through a symlink still fails closed.
if len(safe_matches) != 1:
    raise SystemExit(1)
metadata = safe_matches[0]
try:
    if any(not os.path.samefile(candidate, metadata) for candidate in matches):
        raise SystemExit(1)
except OSError:
    raise SystemExit(1)

try:
    payload = json.loads(metadata.read_text(encoding="utf-8"))
    url = payload["url"]
except (OSError, KeyError, TypeError, ValueError):
    raise SystemExit(1)
if not isinstance(url, str):
    raise SystemExit(1)
parsed = urllib.parse.urlsplit(url)
if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
    raise SystemExit(1)
recorded = os.path.realpath(urllib.parse.unquote(parsed.path))
raise SystemExit(recorded != expected)
PY
}

# Report the mode pip actually recorded for this venv's vq: 1 editable,
# 0 copied, non-zero exit when it cannot be determined.
#
# `.vq-install-metadata` is vq's own note and only exists in a venv that
# install.sh built. A venv made the way CONTRIBUTING.md documents --
# `python -m venv .venv && .venv/bin/pip install -e '.[test,web]'` -- has no
# such note, so "preserve the installed mode" fell back to *copied* and
# silently converted an editable install on the next update. PEP 610
# `direct_url.json` is what pip itself wrote and is present either way.
#
# That silent conversion is not cosmetic: `fleet_release.runtime_repo()`
# resolves the controller checkout from `vq.__file__`, which lands inside
# site-packages for a copied install, and `vq admin rollout-latest` then
# refuses on the host outright.
vq_detect_installed_editable() {
    local target="$1"
    local out_var="$2"
    local inspector=""
    local detected=""

    [ -n "$target" ] || return 1
    vq_find_external_inspector inspector "$target" 2>/dev/null || return 1
    detected="$(PYTHONNOUSERSITE=1 "$inspector" -I -S - "$target" <<'PY'
import glob
import json
import os
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
patterns = (
    root / "lib" / "python*" / "site-packages" / "vq-*.dist-info" / "direct_url.json",
    root / "lib64" / "python*" / "site-packages" / "vq-*.dist-info" / "direct_url.json",
)
matches = sorted({pathlib.Path(path) for pattern in patterns for path in glob.glob(str(pattern))})
if not matches:
    raise SystemExit(1)
# A lib64 -> lib alias is normal and names one inode; anything else is two
# installs or a redirected payload, and this must not guess between them.
try:
    if any(not os.path.samefile(candidate, matches[0]) for candidate in matches):
        raise SystemExit(1)
    if matches[0].is_symlink():
        raise SystemExit(1)
    payload = json.loads(matches[0].read_text(encoding="utf-8"))
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
info = payload.get("dir_info")
if not isinstance(info, dict):
    # A wheel, an archive or a VCS install: not a directory install at all,
    # and not something this question has an answer for.
    raise SystemExit(1)
sys.stdout.write("1" if info.get("editable") is True else "0")
PY
)" || return 1
    case "$detected" in 0|1) ;; *) return 1 ;; esac
    printf -v "$out_var" '%s' "$detected"
}

vq_ownership_marker_status() {
    local target="$1"
    local marker="$target/$VQ_OWNERSHIP_MARKER_NAME"
    local line_count=""
    local version=""
    local project=""
    local extra=""

    if [ -L "$marker" ]; then
        return 3
    fi
    if [ ! -e "$marker" ]; then
        return 2
    fi
    [ -f "$marker" ] || return 3
    line_count="$(wc -l < "$marker" 2>/dev/null | tr -d ' ' || true)"
    version="$(sed -n '1p' "$marker" 2>/dev/null || true)"
    project="$(sed -n '2p' "$marker" 2>/dev/null || true)"
    extra="$(sed -n '3p' "$marker" 2>/dev/null || true)"
    if [ "$line_count" = "2" ] && [ "$version" = "version=1" ] && \
       [ "$project" = "project=$VQ_PROJECT_DIR" ] && [ -z "$extra" ]; then
        return 0
    fi
    if [ "$line_count" = "2" ] && [ "$version" = "version=1" ] && \
       [ "${project#project=}" != "$project" ] && [ -z "$extra" ]; then
        return 4
    fi
    return 3
}

vq_record_venv_ownership() {
    local target="$1"
    local marker="$target/$VQ_OWNERSHIP_MARKER_NAME"
    local temporary=""

    temporary="$(mktemp "$target/.vq-checkout-owner.XXXXXX")" || {
        echo "Error: could not create the vq checkout-ownership marker." >&2
        return 1
    }
    chmod 600 "$temporary"
    if ! printf 'version=1\nproject=%s\n' "$VQ_PROJECT_DIR" > "$temporary" || \
       ! mv -- "$temporary" "$marker"; then
        rm -f -- "$temporary"
        echo "Error: could not record vq checkout ownership in '$target'." >&2
        return 1
    fi
}

vq_check_venv_ownership() {
    local target="$1"
    local adopt_legacy="${2:-0}"
    local marker_status=0

    if [ ! -e "$target" ]; then
        if [ "$adopt_legacy" = "1" ]; then
            echo "Error: --adopt-legacy requires an existing unmarked vq environment." >&2
            return 1
        fi
        return 0
    fi
    if [ ! -f "$target/pyvenv.cfg" ] || [ -L "$target/pyvenv.cfg" ]; then
        echo "Error: refusing '$target': it is not a regular, recognisable virtualenv." >&2
        return 1
    fi

    vq_ownership_marker_status "$target" || marker_status=$?
    case "$marker_status" in
        0)
            if [ "$adopt_legacy" = "1" ]; then
                echo "Error: --adopt-legacy is only for an unmarked legacy vq environment." >&2
                return 1
            fi
            return 0
            ;;
        2)
            if [ "$adopt_legacy" != "1" ]; then
                echo "Error: refusing unowned virtualenv '$target'." >&2
                echo "For a pre-marker vq install from this checkout, inspect it and retry with --adopt-legacy." >&2
                return 1
            fi
            if ! vq_legacy_pep610_matches_checkout "$target"; then
                echo "Error: legacy PEP 610 metadata does not prove this venv came from:" >&2
                echo "       $VQ_PROJECT_DIR" >&2
                return 1
            fi
            return 0
            ;;
        4)
            echo "Error: virtualenv ownership marker belongs to another checkout: '$target'." >&2
            return 1
            ;;
        *)
            echo "Error: malformed or symlinked virtualenv ownership marker: '$target/$VQ_OWNERSHIP_MARKER_NAME'." >&2
            return 1
            ;;
    esac
}

vq_require_venv_ownership() {
    local target="$1"
    local adopt_legacy="${2:-0}"
    local marker_status=0

    vq_check_venv_ownership "$target" "$adopt_legacy" || return
    [ -e "$target" ] || return 0
    vq_ownership_marker_status "$target" || marker_status=$?
    if [ "$marker_status" = "2" ]; then
        echo "==> Adopting legacy vq environment after exact PEP 610 proof..."
        vq_record_venv_ownership "$target" || return
        vq_ownership_marker_status "$target" || {
            echo "Error: the vq checkout-ownership marker failed verification." >&2
            return 1
        }
    fi
}

vq_acquire_lifecycle_lock() {
    local target="$1"
    local action="$2"
    local lexical=""
    local canonical=""
    local lock_python=""

    vq_assert_safe_venv_target "$target" || return
    lexical="$(vq_strip_final_path_syntax "$target")"
    canonical="$(vq_canonical_path "$lexical")" || return
    vq_find_external_inspector lock_python "$canonical" || return
    vibe_toolset_acquire_lifecycle_lock \
        "$lock_python" "$VQ_REPO_ROOT" "$canonical" "vq $action"
}

vq_release_lifecycle_lock() {
    vibe_toolset_release_lifecycle_lock
}

# EXIT cleanup for lifecycle paths that do not replace a venv (currently
# uninstall). Preserve the operation's original failure, but surface a lock
# release failure when the operation itself succeeded.
vq_lifecycle_lock_cleanup() {
    local status="$?"
    local lock_status=0

    set +e
    vq_release_lifecycle_lock || lock_status=$?
    if [ "$status" = "0" ] && [ "$lock_status" != "0" ]; then
        status="$lock_status"
    fi
    return "$status"
}

vq_remove_venv() {
    local target="$1"
    local adopt_legacy="${2:-0}"

    vq_assert_safe_venv_target "$target" || return
    if [ ! -e "$target" ]; then
        return 0
    fi
    if [ ! -f "$target/pyvenv.cfg" ]; then
        echo "Error: refusing to remove '$target': it is not recognisably a virtualenv." >&2
        echo "Move or remove it manually after checking its contents." >&2
        return 1
    fi
    # This is deliberately adjacent to rm: preflight evidence is not authority
    # to delete a path that changed while the user was confirming the action.
    vq_require_venv_ownership "$target" "$adopt_legacy" || return
    echo "==> Removing virtualenv: $target"
    rm -rf -- "$target"
}

# ---------------------------------------------------------------------------
# Daemon lifecycle
#
# A vq venv usually has a daemon running out of it. These helpers detect it,
# and stop/start it around a replacement. systemd user units are preferred when
# one is active, because `vq daemon stop` against a supervised unit just gets
# the process restarted underneath the script.
# ---------------------------------------------------------------------------

VQ_DAEMON_UNIT="vq-daemon"
VQ_DAEMON_LAUNCHD_LABEL="com.vq.daemon"
VQ_DAEMON_WAS_RUNNING=0
VQ_DAEMON_MANAGER=""
VQ_DAEMON_LAUNCHD_PLIST="${HOME:-}/Library/LaunchAgents/com.vq.daemon.plist"

vq_daemon_systemd_active() {
    command -v systemctl >/dev/null 2>&1 || return 1
    systemctl --user is-active --quiet "$VQ_DAEMON_UNIT" 2>/dev/null
}

vq_process_belongs_to_venv() {
    local pid="$1"
    local venv="$2"
    local command_line=""

    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    if [ -r "/proc/$pid/cmdline" ]; then
        command_line="$(tr '\000' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
    else
        # `comm` reports the base framework Python on macOS even for a venv.
        # The full argv retains the venv's `bin/vq` script or Python path.
        command_line="$(ps -ww -p "$pid" -o command= 2>/dev/null | sed 's/^ *//' || true)"
    fi
    [ -n "$command_line" ] || return 1
    case "$command_line" in
        *"$venv/bin/vq"*|*"$venv/bin/python"*) return 0 ;;
        *) return 1 ;;
    esac
}

vq_systemd_daemon_belongs_to_venv() {
    local venv="$1"
    local pid=""
    local exec_start=""

    vq_daemon_systemd_active || return 1
    pid="$(systemctl --user show "$VQ_DAEMON_UNIT" -p MainPID --value 2>/dev/null || true)"
    case "$pid" in
        ''|*[!0-9]*|0) ;;
        *) vq_process_belongs_to_venv "$pid" "$venv" && return 0 ;;
    esac
    exec_start="$(systemctl --user show "$VQ_DAEMON_UNIT" -p ExecStart --value 2>/dev/null || true)"
    case "$exec_start" in
        *"$venv/bin/vq"*|*"$venv/bin/python"*) return 0 ;;
        *) return 1 ;;
    esac
}

vq_launchd_daemon_record() {
    command -v launchctl >/dev/null 2>&1 || return 1
    launchctl print "gui/$(id -u)/$VQ_DAEMON_LAUNCHD_LABEL" 2>/dev/null
}

vq_launchd_daemon_belongs_to_venv() {
    local venv="$1"
    local record=""
    local pid=""

    record="$(vq_launchd_daemon_record)" || return 1
    pid="$(printf '%s\n' "$record" | sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -n 1)"
    if [ -n "$pid" ] && vq_process_belongs_to_venv "$pid" "$venv"; then
        return 0
    fi
    case "$record" in
        *"$venv/bin/vq"*|*"$venv/bin/python"*) return 0 ;;
        *) return 1 ;;
    esac
}

vq_daemon_manager_for_venv() {
    local venv="$1"

    if vq_systemd_daemon_belongs_to_venv "$venv"; then
        printf 'systemd-user\n'
        return 0
    fi
    if vq_launchd_daemon_belongs_to_venv "$venv"; then
        printf 'launchd-user\n'
        return 0
    fi
    return 1
}

# A daemon socket is per state directory, NOT per venv: every vq install for
# this user reaches the same daemon. So "a daemon answers" does not mean "this
# environment's daemon". Resolve the pidfile's process executable and compare
# it against the venv actually being operated on -- otherwise rebuilding a
# scratch environment would stop the user's real daemon.
vq_daemon_belongs_to_venv() {
    local venv="$1"
    local pidfile="$(vq_state_dir)/daemon.pid"
    local pid=""

    [ -f "$pidfile" ] || return 1
    pid="$(sed -n '1p' "$pidfile" 2>/dev/null | tr -dc '0-9')"
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1

    vq_process_belongs_to_venv "$pid" "$venv"
}

vq_daemon_is_running() {
    local venv="$1"

    vq_daemon_manager_for_venv "$venv" >/dev/null 2>&1 && return 0
    vq_daemon_belongs_to_venv "$venv"
}

vq_daemon_describe() {
    local venv="$1"

    if vq_systemd_daemon_belongs_to_venv "$venv"; then
        echo "running (systemd --user unit '$VQ_DAEMON_UNIT')"
    elif vq_launchd_daemon_belongs_to_venv "$venv"; then
        echo "running (launchd user agent '$VQ_DAEMON_LAUNCHD_LABEL')"
    elif vq_daemon_belongs_to_venv "$venv"; then
        echo "running from this environment without a supported supervisor"
    elif vq_daemon_systemd_active || vq_launchd_daemon_record >/dev/null 2>&1; then
        echo "another vq daemon is supervised for this user (not from this venv; left alone)"
    else
        # Never execute a candidate venv's bin/vq just to describe it. Before
        # checkout ownership is proven that path is arbitrary foreign code.
        echo "not detected from pid/supervisor state"
    fi
}

vq_daemon_stop() {
    local venv="$1"

    VQ_DAEMON_WAS_RUNNING=0
    VQ_DAEMON_MANAGER="$(vq_daemon_manager_for_venv "$venv" 2>/dev/null || true)"
    if [ "$VQ_DAEMON_MANAGER" = "systemd-user" ]; then
        echo "==> Stopping the vq daemon (systemd --user unit '$VQ_DAEMON_UNIT')..."
        systemctl --user stop "$VQ_DAEMON_UNIT"
        VQ_DAEMON_WAS_RUNNING=1
        return 0
    fi
    if [ "$VQ_DAEMON_MANAGER" = "launchd-user" ]; then
        if [ ! -f "$VQ_DAEMON_LAUNCHD_PLIST" ]; then
            echo "Error: launchd owns the daemon, but its plist is not at:" >&2
            echo "       $VQ_DAEMON_LAUNCHD_PLIST" >&2
            echo "Cannot promise an automatic restart, so nothing was stopped." >&2
            return 1
        fi
        echo "==> Stopping the vq daemon (launchd user agent '$VQ_DAEMON_LAUNCHD_LABEL')..."
        launchctl bootout "gui/$(id -u)/$VQ_DAEMON_LAUNCHD_LABEL"
        VQ_DAEMON_WAS_RUNNING=1
        return 0
    fi
    # Scoped deliberately: a daemon answering on this state dir that is NOT
    # running from this venv belongs to some other install and is not ours to
    # stop. Rebuilding a scratch environment must never take down the user's
    # real daemon.
    if vq_daemon_belongs_to_venv "$venv"; then
        echo "==> Stopping the vq daemon..."
        "$venv/bin/vq" daemon stop || {
            echo "Error: could not stop the running vq daemon." >&2
            return 1
        }
        VQ_DAEMON_WAS_RUNNING=1
        VQ_DAEMON_MANAGER="unsupervised"
    fi
}

vq_daemon_start() {
    local venv="$1"

    [ "$VQ_DAEMON_WAS_RUNNING" = "1" ] || return 0
    if [ "$VQ_DAEMON_MANAGER" = "systemd-user" ]; then
        echo "==> Restarting the vq daemon (systemd --user unit '$VQ_DAEMON_UNIT')..."
        systemctl --user start "$VQ_DAEMON_UNIT"
        VQ_DAEMON_WAS_RUNNING=0
        return 0
    fi
    if [ "$VQ_DAEMON_MANAGER" = "launchd-user" ]; then
        echo "==> Restarting the vq daemon (launchd user agent '$VQ_DAEMON_LAUNCHD_LABEL')..."
        launchctl bootstrap "gui/$(id -u)" "$VQ_DAEMON_LAUNCHD_PLIST"
        launchctl enable "gui/$(id -u)/$VQ_DAEMON_LAUNCHD_LABEL"
        launchctl kickstart -k "gui/$(id -u)/$VQ_DAEMON_LAUNCHD_LABEL"
        VQ_DAEMON_WAS_RUNNING=0
        return 0
    fi
    cat >&2 <<EOF
Error: the daemon was stopped, but it was not managed by systemd or launchd.
The removed 'vq daemon start' command cannot restart it safely. Run it under a
supported supervisor; see:
    $VQ_PROJECT_DIR/docs/lifecycle.md
EOF
    return 1
}

vq_assert_daemon_stopped_or_managed() {
    local venv="$1"
    local restart_daemon="$2"
    local action="$3"

    vq_daemon_is_running "$venv" || return 0
    if [ "$restart_daemon" = "1" ]; then
        if vq_daemon_manager_for_venv "$venv" >/dev/null 2>&1; then
            return 0
        fi
        cat >&2 <<EOF
Error: this daemon is not managed by systemd-user or launchd-user, so the
script cannot restart it after $action. Stop it yourself, install a supported
service definition, then retry. See:
    $VQ_PROJECT_DIR/docs/lifecycle.md
EOF
        return 1
    fi
    cat >&2 <<EOF
Error: a vq daemon is $(vq_daemon_describe "$venv").

$action would swap code underneath it. Use `vq self-update` or
`vq admin update` for a serving environment, or stop it yourself first:
    systemctl --user stop $VQ_DAEMON_UNIT     # if systemd-managed
    $venv/bin/vq daemon stop                  # otherwise
EOF
    return 1
}

# EXIT cleanup used by every mutating lifecycle script. Restore a transactional
# venv first, then bring back a daemon that was stopped before a later failure.
vq_lifecycle_cleanup() {
    local status="$?"
    local restart_status=0
    local lock_status=0

    set +e
    vq_abort_venv_replacement
    if [ "$VQ_DAEMON_WAS_RUNNING" = "1" ]; then
        vq_daemon_start "${VQ_LIFECYCLE_VENV:-}" || restart_status=$?
    fi
    vq_release_lifecycle_lock || lock_status=$?
    if [ "$status" = "0" ] && [ "$restart_status" != "0" ]; then
        status="$restart_status"
    fi
    if [ "$status" = "0" ] && [ "$lock_status" != "0" ]; then
        status="$lock_status"
    fi
    return "$status"
}

# ---------------------------------------------------------------------------
# Failure-atomic environment replacement
#
# A replacement is built at its final path because Python virtualenvs embed
# absolute paths and are not safely relocatable. The old environment waits in a
# same-parent backup until creation, installation, and verification all pass.
# ---------------------------------------------------------------------------

VQ_VENV_TX_ACTIVE=0
VQ_VENV_TX_TARGET=""
VQ_VENV_TX_BACKUP=""
VQ_VENV_TX_TOKEN=""
VQ_VENV_TX_HAD_ORIGINAL=0
VQ_VENV_TX_MUTATION_STARTED=0
VQ_VENV_TX_OWNERSHIP_REQUIRED=0
VQ_VENV_TX_DURABLE=0
VQ_VENV_TX_INSPECTOR=""
VQ_VENV_TX_HELPER="$VQ_SCRIPT_DIR/_venv_transaction.py"

vq_venv_replacement_receipt_path() {
    local target="$1"
    printf '%s/.%s.vq-venv-replacement.json\n' \
        "$(dirname "$target")" "$(basename "$target")"
}

vq_recover_pending_venv_replacement_before_use() {
    local target="$1"
    local action="$2"
    local dry_run="${3:-0}"
    local receipt=""
    local inspector=""
    local result=""
    local status=0

    receipt="$(vq_venv_replacement_receipt_path "$target")"
    if [ ! -e "$receipt" ] && [ ! -L "$receipt" ]; then
        return 0
    fi
    if [ "$dry_run" = "1" ]; then
        echo "Error: a durable virtualenv replacement receipt needs recovery:" >&2
        echo "       $receipt" >&2
        echo "Re-run without --dry-run; recovery will restore or finish the exact transaction and exit." >&2
        return 1
    fi

    echo "==> Recovering an interrupted virtualenv replacement..." >&2
    vq_find_external_inspector inspector "$target" || {
        return 1
    }
    set +e
    result="$(PYTHONNOUSERSITE=1 "$inspector" -I -S "$VQ_VENV_TX_HELPER" \
        recover "$VQ_REPO_ROOT" "$target" 2>&1)"
    status=$?
    set -e
    if [ "$status" != "0" ]; then
        printf '%s\n' "$result" >&2
        echo "Error: interrupted replacement remains fenced; no service or candidate was executed." >&2
        return 1
    fi
    echo "    $result" >&2
    echo "Recovery completed. Re-run the requested lifecycle command explicitly." >&2
    return 10
}

vq_begin_venv_replacement() {
    local target="$1"
    local adopt_legacy="${2:-0}"
    local durable="${3:-1}"
    local backup=""
    local transaction=""
    local inspector=""

    if [ "$VQ_VENV_TX_ACTIVE" = "1" ]; then
        echo "Error: a virtualenv replacement transaction is already active." >&2
        return 1
    fi
    vq_assert_safe_venv_target "$target" || return
    if [ -e "$target" ] && [ ! -f "$target/pyvenv.cfg" ]; then
        echo "Error: refusing to replace '$target': it is not recognisably a virtualenv." >&2
        echo "Move or remove it manually after checking its contents." >&2
        return 1
    fi
    if [ -e "$target" ]; then
        vq_require_venv_ownership "$target" "$adopt_legacy" || return
    elif [ "$adopt_legacy" = "1" ]; then
        echo "Error: --adopt-legacy requires an existing unmarked vq environment." >&2
        return 1
    fi

    mkdir -p "$(dirname "$target")"
    if [ "$durable" = "1" ]; then
        [ -f "$VQ_VENV_TX_HELPER" ] && [ ! -L "$VQ_VENV_TX_HELPER" ] || {
            echo "Error: durable virtualenv transaction helper is missing or unsafe." >&2
            return 1
        }
        vq_find_external_inspector inspector "$target" || return
        transaction="$(PYTHONNOUSERSITE=1 "$inspector" -I -S \
            "$VQ_VENV_TX_HELPER" begin "$VQ_REPO_ROOT" "$target")" || return
        case "$transaction" in
            [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
            *)
                echo "Error: durable virtualenv transaction returned an invalid identity." >&2
                return 1
                ;;
        esac
        if [ -e "$target" ]; then
            backup="$(dirname "$target")/.$(basename "$target").vq-venv-backup-$transaction"
        fi
    elif [ -e "$target" ]; then
        if ! backup="$(mktemp -d "${target}.previous.XXXXXX")"; then
            echo "Error: could not reserve a rollback path beside '$target'." >&2
            return 1
        fi
        rmdir "$backup"
    fi

    VQ_VENV_TX_ACTIVE=1
    VQ_VENV_TX_TARGET="$target"
    VQ_VENV_TX_BACKUP="$backup"
    VQ_VENV_TX_TOKEN="${transaction:-${BASHPID:-$$}.${RANDOM}.${RANDOM}}"
    VQ_VENV_TX_HAD_ORIGINAL=0
    [ -e "$target" ] && VQ_VENV_TX_HAD_ORIGINAL=1
    VQ_VENV_TX_MUTATION_STARTED=0
    VQ_VENV_TX_OWNERSHIP_REQUIRED="$VQ_VENV_TX_HAD_ORIGINAL"
    VQ_VENV_TX_DURABLE="$durable"
    VQ_VENV_TX_INSPECTOR="$inspector"
}

vq_start_venv_replacement() {
    local target="$1"
    local marker

    if [ "$VQ_VENV_TX_ACTIVE" != "1" ] || [ "$target" != "$VQ_VENV_TX_TARGET" ]; then
        echo "Error: virtualenv replacement does not match the active transaction." >&2
        return 1
    fi
    # Revalidate immediately before the first move. Preflight can be separated
    # from mutation by a Git fetch, and an ownership/symlink change in that
    # interval must turn into a refusal rather than a recursive replacement.
    vq_assert_safe_venv_target "$target" || return
    if [ "$VQ_VENV_TX_OWNERSHIP_REQUIRED" = "1" ]; then
        vq_check_venv_ownership "$target" 0 || return
    fi

    VQ_VENV_TX_MUTATION_STARTED=1
    if [ "$VQ_VENV_TX_DURABLE" = "1" ]; then
        PYTHONNOUSERSITE=1 "$VQ_VENV_TX_INSPECTOR" -I -S \
            "$VQ_VENV_TX_HELPER" start "$VQ_REPO_ROOT" "$target"
    else
        if [ "$VQ_VENV_TX_HAD_ORIGINAL" = "1" ]; then
            mv -- "$target" "$VQ_VENV_TX_BACKUP"
        fi
        mkdir "$target"
        marker="$target/.vq-venv-transaction"
        printf '%s\n' "$VQ_VENV_TX_TOKEN" > "$marker"
    fi
}

vq_abort_venv_replacement() {
    local marker
    local marker_token=""
    local original_still_in_place=0
    local restore_failed=0

    [ "$VQ_VENV_TX_ACTIVE" = "1" ] || return 0
    if [ "$VQ_VENV_TX_DURABLE" = "1" ]; then
        echo "==> Virtualenv replacement did not commit; restoring the previous state..." >&2
        if ! PYTHONNOUSERSITE=1 "$VQ_VENV_TX_INSPECTOR" -I -S \
            "$VQ_VENV_TX_HELPER" recover "$VQ_REPO_ROOT" \
            "$VQ_VENV_TX_TARGET"; then
            restore_failed=1
        fi
        VQ_VENV_TX_ACTIVE=0
        VQ_VENV_TX_OWNERSHIP_REQUIRED=0
        VQ_VENV_TX_DURABLE=0
        VQ_VENV_TX_INSPECTOR=""
        if [ "$restore_failed" = "1" ]; then
            echo "Error: durable virtualenv rollback remains fenced by its receipt." >&2
        fi
        return 0
    fi
    if [ "$VQ_VENV_TX_MUTATION_STARTED" = "1" ]; then
        echo "==> Virtualenv replacement failed; restoring the previous state..." >&2
        marker="$VQ_VENV_TX_TARGET/.vq-venv-transaction"
        if [ -f "$marker" ]; then
            marker_token="$(sed -n '1p' "$marker" 2>/dev/null || true)"
        fi
        if [ "$VQ_VENV_TX_HAD_ORIGINAL" = "1" ] && \
           [ -e "$VQ_VENV_TX_TARGET" ] && \
           [ ! -e "$marker" ] && \
           [ ! -e "$VQ_VENV_TX_BACKUP" ]; then
            # The EXIT trap can run after the transaction is armed but before
            # the same-filesystem mv begins. In that state the unmarked target
            # is still the intact original, so rollback is already complete.
            original_still_in_place=1
        fi
        if [ -e "$VQ_VENV_TX_TARGET" ]; then
            if [ "$original_still_in_place" = "1" ]; then
                :
            elif [ "$marker_token" = "$VQ_VENV_TX_TOKEN" ]; then
                if vq_assert_safe_venv_target "$VQ_VENV_TX_TARGET"; then
                    rm -rf -- "$VQ_VENV_TX_TARGET"
                else
                    restore_failed=1
                fi
            elif ! rmdir "$VQ_VENV_TX_TARGET" 2>/dev/null; then
                echo "Error: refusing to remove an unowned replacement target:" >&2
                echo "       $VQ_VENV_TX_TARGET" >&2
                restore_failed=1
            fi
        fi
        if [ -n "$VQ_VENV_TX_BACKUP" ] && [ -e "$VQ_VENV_TX_BACKUP" ]; then
            if [ "$restore_failed" = "0" ] && \
               ! mv -- "$VQ_VENV_TX_BACKUP" "$VQ_VENV_TX_TARGET"; then
                restore_failed=1
            fi
        fi
    fi

    VQ_VENV_TX_ACTIVE=0
    VQ_VENV_TX_OWNERSHIP_REQUIRED=0
    VQ_VENV_TX_DURABLE=0
    VQ_VENV_TX_INSPECTOR=""
    if [ "$restore_failed" = "1" ]; then
        echo "Error: automatic virtualenv rollback was incomplete." >&2
        if [ -n "$VQ_VENV_TX_BACKUP" ]; then
            echo "The previous environment remains at: $VQ_VENV_TX_BACKUP" >&2
        fi
    fi
    return 0
}

vq_commit_venv_replacement() {
    local backup="$VQ_VENV_TX_BACKUP"
    local marker="$VQ_VENV_TX_TARGET/.vq-venv-transaction"

    if [ "$VQ_VENV_TX_ACTIVE" != "1" ] || [ "$VQ_VENV_TX_MUTATION_STARTED" != "1" ]; then
        echo "Error: cannot commit an incomplete virtualenv replacement." >&2
        return 1
    fi
    vq_check_venv_health "$VQ_VENV_TX_TARGET"

    if [ "$VQ_VENV_TX_DURABLE" = "1" ]; then
        PYTHONNOUSERSITE=1 "$VQ_VENV_TX_INSPECTOR" -I -S \
            "$VQ_VENV_TX_HELPER" commit "$VQ_REPO_ROOT" \
            "$VQ_VENV_TX_TARGET"
        VQ_VENV_TX_ACTIVE=0
        VQ_VENV_TX_BACKUP=""
        VQ_VENV_TX_OWNERSHIP_REQUIRED=0
        VQ_VENV_TX_DURABLE=0
        VQ_VENV_TX_INSPECTOR=""
        return 0
    fi

    # Disable rollback only after the final-path environment is healthy. Any
    # later backup cleanup failure leaves the verified new venv in service.
    VQ_VENV_TX_ACTIVE=0
    if ! rm -f -- "$marker"; then
        echo "Warning: could not remove transaction marker '$marker'." >&2
    fi
    if [ -n "$backup" ] && ! vq_remove_venv "$backup"; then
        echo "Warning: the old virtualenv remains at '$backup'." >&2
        echo "Remove it manually after confirming the update." >&2
    fi
    VQ_VENV_TX_BACKUP=""
    VQ_VENV_TX_OWNERSHIP_REQUIRED=0
    VQ_VENV_TX_DURABLE=0
    VQ_VENV_TX_INSPECTOR=""
}

vq_check_venv_health() {
    local venv="$1"

    if [ ! -f "$venv/pyvenv.cfg" ] || [ ! -x "$venv/bin/python" ]; then
        echo "Error: '$venv' is not a usable virtualenv." >&2
        return 1
    fi
    if ! "$venv/bin/python" -c \
        "import sys; raise SystemExit(not (sys.prefix != sys.base_prefix and sys.version_info >= ($VQ_MIN_PYTHON_MAJOR, $VQ_MIN_PYTHON_MINOR)))" \
        >/dev/null 2>&1; then
        echo "Error: '$venv/bin/python' is broken or older than Python $VQ_MIN_PYTHON_MAJOR.$VQ_MIN_PYTHON_MINOR." >&2
        return 1
    fi
    if ! "$venv/bin/python" -m pip --version >/dev/null 2>&1; then
        echo "Error: pip is not usable in '$venv'." >&2
        return 1
    fi
    if [ -x "$venv/bin/pip" ] && ! "$venv/bin/pip" --version >/dev/null 2>&1; then
        echo "Error: '$venv/bin/pip' has a stale interpreter path." >&2
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Extras profiles
# ---------------------------------------------------------------------------

vq_extras_to_spec() {
    local profile="$1"
    local out_var="$2"
    local spec

    case "$profile" in
        core|none)  spec="" ;;
        web)        spec="[web]" ;;
        test)       spec="[test]" ;;
        dev)        spec="[dev,test]" ;;
        all)        spec="[web,test,dev]" ;;
        *)
            echo "Error: --extras must be one of: core / web / test / dev / all (got '$profile')." >&2
            return 1
            ;;
    esac
    printf -v "$out_var" '%s' "$spec"
}

vq_profile_has_web() {
    case "$1" in
        web|all) return 0 ;;
        *) return 1 ;;
    esac
}

VQ_INSTALL_METADATA_NAME=".vq-install-metadata"

vq_load_install_metadata() {
    local venv="$1"
    local profile_var="$2"
    local editable_var="$3"
    local metadata="$venv/$VQ_INSTALL_METADATA_NAME"
    local loaded_profile=""
    local loaded_editable=""

    [ -f "$metadata" ] || return 1
    loaded_profile="$(sed -n 's/^extras=//p' "$metadata" | head -n 1)"
    loaded_editable="$(sed -n 's/^editable=//p' "$metadata" | head -n 1)"
    case "$loaded_profile" in core|web|test|dev|all) ;; *) return 1 ;; esac
    case "$loaded_editable" in 0|1) ;; *) return 1 ;; esac
    printf -v "$profile_var" '%s' "$loaded_profile"
    printf -v "$editable_var" '%s' "$loaded_editable"
}

vq_record_install_metadata() {
    local venv="$1"
    local profile="$2"
    local editable="$3"
    local metadata="$venv/$VQ_INSTALL_METADATA_NAME"
    local temporary="$metadata.tmp.$$"

    (
        umask 077
        {
            printf 'version=1\n'
            printf 'extras=%s\n' "$profile"
            printf 'editable=%s\n' "$editable"
        } > "$temporary"
        mv -- "$temporary" "$metadata"
    )
}

# ---------------------------------------------------------------------------
# Install / verify
# ---------------------------------------------------------------------------

vq_install_environment() {
    local venv="$1"
    local extras_spec="$2"
    local editable="${3:-0}"
    local -a pip_args=(--upgrade)

    echo "==> Updating pip and wheel-build tools..."
    "$venv/bin/python" -m pip install --quiet --upgrade pip setuptools wheel

    if [ "$editable" = "1" ]; then
        pip_args+=(--editable)
        echo "==> Installing vq${extras_spec} from this checkout (editable)..."
    else
        echo "==> Installing vq${extras_spec} from this checkout..."
    fi
    "$venv/bin/python" -m pip install "${pip_args[@]}" "${VQ_PROJECT_DIR}${extras_spec}"
}

# Stamp the package-local SOURCE-SHA provenance marker.
#
# Daemon RPC reports this, and scheduler-compat gates believe it. pip does not
# track the marker (it is written after install), so an upgrade would otherwise
# leave the previous one orphaned in the new package directory and the daemon
# would report a commit it is not running. Every install path here restamps it.
#
# An editable install derives its SHA from the checkout directly and must not
# stamp the package path: that path is the tracked source tree, so a marker
# would dirty an otherwise immutable exact update.
vq_record_source_marker() {
    local venv="$1"
    local editable="${2:-0}"
    local sha=""

    if [ "$editable" = "1" ]; then
        echo "==> Editable install: SOURCE-SHA comes from Git; no marker written."
        return 0
    fi

    if ! git -C "$VQ_REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; then
        echo "Error: cannot record SOURCE-SHA without a Git checkout." >&2
        return 1
    fi
    sha="$(git -C "$VQ_REPO_ROOT" rev-parse HEAD 2>/dev/null || true)"
    if [ -z "$sha" ]; then
        echo "Error: could not read HEAD; SOURCE-SHA marker not written." >&2
        return 1
    fi
    echo "==> Recording SOURCE-SHA provenance marker..."
    if ! "$venv/bin/vq" source-sha --write-marker "$sha" >/dev/null; then
        echo "Error: could not write the SOURCE-SHA marker." >&2
        echo "The lifecycle operation is incomplete and will not report success." >&2
        return 1
    fi
    echo "    $sha"
}

vq_verify_environment() {
    local venv="$1"
    local profile="$2"
    local imports="import vq, vq.cli, vq.daemon, vq.admin"

    if vq_profile_has_web "$profile"; then
        imports="$imports, fastapi, starlette, uvicorn, jinja2"
    fi

    echo "==> Verifying the installed environment..."
    "$venv/bin/python" -c "$imports"
    "$venv/bin/vq" --version
    "$venv/bin/vq" daemon --help >/dev/null
    if vq_profile_has_web "$profile"; then
        "$venv/bin/vq" web --help >/dev/null
    fi
}

# ---------------------------------------------------------------------------
# State directories — user data, never removed implicitly
# ---------------------------------------------------------------------------

vq_state_dir() {
    printf '%s\n' "${VQ_STATE_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/vq}"
}

vq_config_dir() {
    printf '%s\n' "${VQ_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}/vq}"
}

vq_count_queue_entries() {
    local queue
    queue="$(vq_state_dir)/queue"

    if [ ! -d "$queue" ]; then
        printf '0\n'
        return 0
    fi
    find "$queue" -maxdepth 1 -type f -name '*.json' 2>/dev/null | wc -l | tr -d ' '
}

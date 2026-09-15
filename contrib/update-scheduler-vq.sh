#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: update-scheduler-vq.sh [OPTIONS]

Refresh the vq install used by a daemonless scheduler login host.

When invoked by ``vq admin update HOST``, the script consumes the exact source
archive published in ``VQ_SCHEDULER_STAGE``. Standalone use reinstalls the
existing scheduler-side source tree unless --source is supplied.

Options:
  --source DIR            Source vq tree to copy before reinstalling.
  --target DIR            Scheduler-side vq tree. Default: $HOME/vibe-queue.
  --venv DIR              Virtualenv to install into. Default: TARGET/.venv.
  --backup-dir DIR        Backup directory before copying source.
                          Default: $HOME/vibe-queue-backups.
  --require-version VER   Refuse if the source/target pyproject version is not VER.
  -h, --help              Show this help.

Environment defaults:
  VQ_SCHEDULER_VQ_SOURCE
  VQ_SCHEDULER_VQ_TARGET
  VQ_SCHEDULER_VQ_VENV
  VQ_SCHEDULER_VQ_BACKUP_DIR
  VQ_SCHEDULER_VQ_REQUIRE_VERSION
  VQ_SCHEDULER_STAGE
  VQ_SCHEDULER_EXPECTED_SOURCE_SHA
  VQ_SCHEDULER_EXPECTED_TREE_SHA256
EOF
}

die() {
    echo "update-scheduler-vq: $*" >&2
    exit 1
}

target="${VQ_SCHEDULER_VQ_TARGET:-$HOME/vibe-queue}"
source_dir="${VQ_SCHEDULER_VQ_SOURCE:-}"
venv="${VQ_SCHEDULER_VQ_VENV:-}"
backup_dir="${VQ_SCHEDULER_VQ_BACKUP_DIR:-$HOME/vibe-queue-backups}"
require_version="${VQ_SCHEDULER_VQ_REQUIRE_VERSION:-}"
stage="${VQ_SCHEDULER_STAGE:-}"
expected_source_sha="${VQ_SCHEDULER_EXPECTED_SOURCE_SHA:-}"
expected_tree_sha256="${VQ_SCHEDULER_EXPECTED_TREE_SHA256:-}"
extract_root=""

cleanup() {
    if [ -n "$extract_root" ] && [ -d "$extract_root" ]; then
        rm -rf "$extract_root"
    fi
}
trap cleanup EXIT

while [ "$#" -gt 0 ]; do
    case "$1" in
        --source)
            [ "$#" -ge 2 ] || die "--source needs a directory"
            source_dir="$2"
            shift 2
            ;;
        --target)
            [ "$#" -ge 2 ] || die "--target needs a directory"
            target="$2"
            shift 2
            ;;
        --venv)
            [ "$#" -ge 2 ] || die "--venv needs a directory"
            venv="$2"
            shift 2
            ;;
        --backup-dir)
            [ "$#" -ge 2 ] || die "--backup-dir needs a directory"
            backup_dir="$2"
            shift 2
            ;;
        --require-version)
            [ "$#" -ge 2 ] || die "--require-version needs a value"
            require_version="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

if [ -z "$venv" ]; then
    venv="$target/.venv"
fi

[ -x "$venv/bin/python" ] || die "missing venv python: $venv/bin/python"

if [ -n "$stage" ]; then
    [ -z "$source_dir" ] || die "--source cannot be combined with VQ_SCHEDULER_STAGE"
    [ -d "$stage" ] || die "scheduler stage is not a directory: $stage"
    [ -f "$stage/vibe-queue-src.tar.gz" ] || die "stage archive is missing"
    [ -f "$stage/ARCHIVE-SHA256" ] || die "stage archive checksum is missing"
    [ -f "$stage/SOURCE-SHA" ] || die "stage SOURCE-SHA is missing"
    [ -f "$stage/SOURCE-TREE-SHA256" ] || die "stage tree digest is missing"
    [ -n "$expected_source_sha" ] || die "expected source SHA was not provided"
    [ -n "$expected_tree_sha256" ] || die "expected source-tree digest was not provided"

    stage_source_sha="$(head -n 1 "$stage/SOURCE-SHA" | tr -d '[:space:]')"
    stage_tree_sha256="$(head -n 1 "$stage/SOURCE-TREE-SHA256" | tr -d '[:space:]')"
    [ "$stage_source_sha" = "$expected_source_sha" ] || \
        die "stage SOURCE-SHA $stage_source_sha does not match expected $expected_source_sha"
    [ "$stage_tree_sha256" = "$expected_tree_sha256" ] || \
        die "stage tree digest $stage_tree_sha256 does not match expected $expected_tree_sha256"

    expected_archive_sha256="$(awk 'NR == 1 {print $1}' "$stage/ARCHIVE-SHA256")"
    case "$expected_archive_sha256" in
        *[!0-9a-fA-F]*|'') die "stage archive checksum is invalid" ;;
    esac
    [ "${#expected_archive_sha256}" -eq 64 ] || die "stage archive checksum is invalid"
    if command -v sha256sum >/dev/null 2>&1; then
        actual_archive_sha256="$(sha256sum "$stage/vibe-queue-src.tar.gz" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
        actual_archive_sha256="$(shasum -a 256 "$stage/vibe-queue-src.tar.gz" | awk '{print $1}')"
    else
        die "sha256sum or shasum is required to verify the scheduler stage"
    fi
    [ "$actual_archive_sha256" = "$expected_archive_sha256" ] || \
        die "stage archive checksum mismatch"

    extract_root="$(mktemp -d "${TMPDIR:-/tmp}/vq-scheduler-source.XXXXXX")"
    tar -xzf "$stage/vibe-queue-src.tar.gz" -C "$extract_root"
    source_dir="$extract_root/vibe-queue"
    [ -d "$source_dir/src/vq" ] || die "stage archive is not a vq source tree"
    echo "stage: $stage (source $expected_source_sha)"
fi

pyproject_version() {
    "$venv/bin/python" - "$1/pyproject.toml" <<'PY'
from __future__ import annotations

import sys
import tomllib
from pathlib import Path

pyproject = Path(sys.argv[1])
if not pyproject.is_file():
    raise SystemExit(f"missing pyproject.toml: {pyproject}")
data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
print(data["project"]["version"])
PY
}

verify_version() {
    local root="$1"
    local version
    version="$(pyproject_version "$root")"
    if [ -n "$require_version" ] && [ "$version" != "$require_version" ]; then
        die "$root has vq version $version, expected $require_version"
    fi
    echo "$version"
}

if [ -n "$source_dir" ]; then
    [ -d "$source_dir/src/vq" ] || die "source is not a vq tree: $source_dir"
    source_version="$(verify_version "$source_dir")"
    echo "source: $source_dir (vq $source_version)"

    if [ -n "$expected_tree_sha256" ]; then
        staged_tree_sha256="$(
            PYTHONPATH="$source_dir/src" "$venv/bin/python" - "$source_dir/src/vq" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

from vq.admin import source_tree_sha256

print(source_tree_sha256(Path(sys.argv[1])))
PY
        )"
        [ "$staged_tree_sha256" = "$expected_tree_sha256" ] || \
            die "staged source-tree digest $staged_tree_sha256 does not match expected $expected_tree_sha256"
        echo "staged source-tree: $staged_tree_sha256"
    fi

    source_real="$(cd "$source_dir" && pwd -P)"
    if [ -d "$target" ]; then
        target_real="$(cd "$target" && pwd -P)"
    else
        target_real=""
    fi

    if [ "$source_real" != "$target_real" ]; then
        command -v rsync >/dev/null 2>&1 || die "rsync is required for --source"
        mkdir -p "$backup_dir"
        if [ -d "$target" ]; then
            stamp="$(date -u +%Y%m%dT%H%M%SZ)"
            backup="$backup_dir/vibe-queue-before-update-$stamp.tar.gz"
            tar \
                --exclude=.venv \
                --exclude=.git \
                --exclude=__pycache__ \
                --exclude=.pytest_cache \
                -czf "$backup" \
                -C "$(dirname "$target")" "$(basename "$target")"
            echo "backup: $backup"
        fi
        mkdir -p "$target"
        rsync -a --checksum --delete \
            --exclude=.git/ \
            --exclude=.venv/ \
            --exclude=__pycache__/ \
            --exclude=.pytest_cache/ \
            --exclude='*.pyc' \
            "$source_real/" "$target/"
    else
        echo "source and target are identical; refreshing venv in place"
    fi
else
    [ -d "$target/src/vq" ] || die "target is not a vq tree: $target"
    target_version="$(verify_version "$target")"
    echo "source: existing target tree $target (vq $target_version)"
fi

"$venv/bin/python" -m pip install -e "$target"
"$venv/bin/vq" --version
if [ -n "$expected_tree_sha256" ]; then
    installed_tree_sha256="$("$venv/bin/vq" source-tree-sha256)"
    [ "$installed_tree_sha256" = "$expected_tree_sha256" ] || \
        die "installed source-tree digest $installed_tree_sha256 does not match expected $expected_tree_sha256"
    "$venv/bin/vq" source-sha --write-marker "$expected_source_sha" >/dev/null
    installed_source_sha="$("$venv/bin/vq" source-sha)"
    [ "$installed_source_sha" = "$expected_source_sha" ] || \
        die "installed SOURCE-SHA $installed_source_sha does not match expected $expected_source_sha"
    echo "provenance: source-tree $installed_tree_sha256; source $installed_source_sha"
fi
"$venv/bin/python" - <<'PY'
from __future__ import annotations

from vq.scheduler_dispatch import SchedulerDispatcher

if not hasattr(SchedulerDispatcher, "exit_marker_code"):
    raise SystemExit("scheduler delayed-exit-marker support is missing")
print("scheduler delayed-exit-marker support: present")
PY

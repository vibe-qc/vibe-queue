"""Regression tests for the staged scheduler-helper updater."""
from __future__ import annotations

import hashlib
import os
import subprocess
import tarfile
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "contrib" / "update-scheduler-vq.sh"
SOURCE_SHA = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
TREE_SHA256 = "12" * 32


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def _make_stage(tmp_path: Path) -> Path:
    source = tmp_path / "source" / "vibe-queue"
    package = source / "src" / "vq"
    package.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        '[project]\nname = "vq"\nversion = "0.12.0"\n', encoding="utf-8"
    )
    (package / "__init__.py").write_text("FRESH = True\n", encoding="utf-8")

    stage = tmp_path / "stage"
    stage.mkdir()
    archive = stage / "vibe-queue-src.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(source, arcname="vibe-queue")
    archive_sha256 = hashlib.sha256(archive.read_bytes()).hexdigest()
    (stage / "ARCHIVE-SHA256").write_text(
        f"{archive_sha256}  {archive.name}\n", encoding="utf-8"
    )
    (stage / "SOURCE-SHA").write_text(SOURCE_SHA + "\n", encoding="utf-8")
    (stage / "SOURCE-TREE-SHA256").write_text(
        TREE_SHA256 + "\n", encoding="utf-8"
    )
    return stage


def _make_target(tmp_path: Path) -> tuple[Path, Path, Path]:
    target = tmp_path / "target"
    package = target / "src" / "vq"
    package.mkdir(parents=True)
    (target / "pyproject.toml").write_text(
        '[project]\nname = "vq"\nversion = "0.12.0"\n', encoding="utf-8"
    )
    (package / "__init__.py").write_text("STALE = True\n", encoding="utf-8")

    venv_bin = target / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    marker = tmp_path / "installed-source-sha"
    log = tmp_path / "vq-calls.log"
    _write_executable(
        venv_bin / "python",
        """#!/bin/sh
if [ "${1:-}" = "-m" ]; then
    exit 0
fi
if [ -n "${2:-}" ] && [ "$(basename "$2")" = "pyproject.toml" ]; then
    printf '0.12.0\\n'
    exit 0
fi
if [ -n "${2:-}" ] && [ "$(basename "$2")" = "vq" ]; then
    printf '%s\\n' "$VQ_FAKE_STAGED_TREE_SHA256"
    exit 0
fi
printf 'scheduler delayed-exit-marker support: present\\n'
""",
    )
    _write_executable(
        venv_bin / "vq",
        """#!/bin/sh
printf '%s\\n' "$*" >> "$VQ_TEST_LOG"
case "${1:-}" in
    --version) printf 'vq, version 0.12.0\\n' ;;
    source-tree-sha256) printf '%s\\n' "$VQ_FAKE_TREE_SHA256" ;;
    source-sha)
        if [ "${2:-}" = "--write-marker" ]; then
            printf '%s\\n' "$3" > "$VQ_TEST_SOURCE_MARKER"
        else
            cat "$VQ_TEST_SOURCE_MARKER"
        fi
        ;;
    *) exit 2 ;;
esac
""",
    )
    return target, marker, log


def _run_updater(
    tmp_path: Path,
    stage: Path,
    target: Path,
    marker: Path,
    log: Path,
    *,
    staged_tree: str = TREE_SHA256,
    installed_tree: str = TREE_SHA256,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "VQ_SCHEDULER_STAGE": str(stage),
            "VQ_SCHEDULER_EXPECTED_SOURCE_SHA": SOURCE_SHA,
            "VQ_SCHEDULER_EXPECTED_TREE_SHA256": TREE_SHA256,
            "VQ_SCHEDULER_VQ_TARGET": str(target),
            "VQ_SCHEDULER_VQ_BACKUP_DIR": str(tmp_path / "backups"),
            "VQ_TEST_LOG": str(log),
            "VQ_TEST_SOURCE_MARKER": str(marker),
            "VQ_FAKE_STAGED_TREE_SHA256": staged_tree,
            "VQ_FAKE_TREE_SHA256": installed_tree,
        }
    )
    return subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def test_staged_archive_replaces_stale_source_and_records_provenance(
    tmp_path: Path,
) -> None:
    stage = _make_stage(tmp_path)
    target, marker, log = _make_target(tmp_path)

    proc = _run_updater(tmp_path, stage, target, marker, log)

    assert proc.returncode == 0, proc.stderr
    installed = (target / "src" / "vq" / "__init__.py").read_text(
        encoding="utf-8"
    )
    assert installed == "FRESH = True\n"
    assert marker.read_text(encoding="utf-8").strip() == SOURCE_SHA
    assert log.read_text(encoding="utf-8").splitlines() == [
        "--version",
        "source-tree-sha256",
        f"source-sha --write-marker {SOURCE_SHA}",
        "source-sha",
    ]


def test_stage_identity_mismatch_fails_before_touching_live_target(
    tmp_path: Path,
) -> None:
    stage = _make_stage(tmp_path)
    (stage / "SOURCE-SHA").write_text("b" * 40 + "\n", encoding="utf-8")
    target, marker, log = _make_target(tmp_path)

    proc = _run_updater(tmp_path, stage, target, marker, log)

    assert proc.returncode != 0
    assert "does not match expected" in proc.stderr
    assert (target / "src" / "vq" / "__init__.py").read_text(
        encoding="utf-8"
    ) == "STALE = True\n"
    assert target.is_dir()
    assert not marker.exists()


def test_post_install_digest_failure_never_moves_live_target(tmp_path: Path) -> None:
    stage = _make_stage(tmp_path)
    target, marker, log = _make_target(tmp_path)

    proc = _run_updater(tmp_path, stage, target, marker, log, installed_tree="34" * 32)

    assert proc.returncode != 0
    assert "installed source-tree digest" in proc.stderr
    assert target.is_dir()
    assert (target / ".venv" / "bin" / "vq").is_file()
    assert not marker.exists()


def test_staged_tree_digest_failure_precedes_live_target_backup(
    tmp_path: Path,
) -> None:
    stage = _make_stage(tmp_path)
    target, marker, log = _make_target(tmp_path)

    proc = _run_updater(tmp_path, stage, target, marker, log, staged_tree="56" * 32)

    assert proc.returncode != 0
    assert "staged source-tree digest" in proc.stderr
    assert (target / "src" / "vq" / "__init__.py").read_text(
        encoding="utf-8"
    ) == "STALE = True\n"
    assert not (tmp_path / "backups").exists()
    assert not marker.exists()

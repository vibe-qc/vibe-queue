from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def test_update_helper_accepts_fetched_commit_ref(tmp_path: Path) -> None:
    # scripts/_setup_helpers.sh belongs to vibe-qc, which is a separate
    # repository since the 2026-09 split. Clone it alongside to exercise
    # this contract; otherwise there is nothing here to test.
    repo_root = Path(__file__).resolve().parents[2] / "vibe-qc"
    if not (repo_root / "scripts" / "_setup_helpers.sh").is_file():
        pytest.skip("vibe-qc is not checked out alongside this repository")
    helper = repo_root / "scripts" / "_setup_helpers.sh"

    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", str(origin)], check=True)

    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-b", "main")
    _git(source, "config", "user.email", "test@example.invalid")
    _git(source, "config", "user.name", "Test User")
    (source / "README.md").write_text("first\n")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "first")
    _git(source, "remote", "add", "origin", str(origin))
    _git(source, "push", "-u", "origin", "main")

    (source / "README.md").write_text("blessed\n")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "blessed")
    blessed_sha = _git(source, "rev-parse", "HEAD")

    (source / "README.md").write_text("moving tip\n")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "moving tip")
    _git(source, "push", "origin", "main")

    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", str(origin), str(clone)],
        check=True,
        capture_output=True,
        text=True,
    )

    subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -euo pipefail; "
                f". {helper}; "
                "vibeqc_checkout_ref \"$1\""
            ),
            "_",
            blessed_sha,
        ],
        cwd=clone,
        check=True,
        capture_output=True,
        text=True,
    )

    assert _git(clone, "rev-parse", "HEAD") == blessed_sha
    assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
    assert (clone / "README.md").read_text() == "blessed\n"


def test_update_helper_fails_when_branch_checkout_fails(tmp_path: Path) -> None:
    """A held branch must not fall through and build the current worktree."""

    # scripts/_setup_helpers.sh belongs to vibe-qc, which is a separate
    # repository since the 2026-09 split. Clone it alongside to exercise
    # this contract; otherwise there is nothing here to test.
    repo_root = Path(__file__).resolve().parents[2] / "vibe-qc"
    if not (repo_root / "scripts" / "_setup_helpers.sh").is_file():
        pytest.skip("vibe-qc is not checked out alongside this repository")
    helper = repo_root / "scripts" / "_setup_helpers.sh"

    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", str(origin)], check=True)

    clone = tmp_path / "clone"
    clone.mkdir()
    _git(clone, "init", "-b", "main")
    _git(clone, "config", "user.email", "test@example.invalid")
    _git(clone, "config", "user.name", "Test User")
    (clone / "README.md").write_text("main\n")
    _git(clone, "add", ".")
    _git(clone, "commit", "-m", "initial")
    _git(clone, "remote", "add", "origin", str(origin))
    _git(clone, "push", "-u", "origin", "main")
    _git(clone, "branch", "worker")

    worker = tmp_path / "worker"
    _git(clone, "worktree", "add", str(worker), "worker")

    proc = subprocess.run(
        [
            "bash",
            "-c",
            (
                f". {helper}; "
                'if vibeqc_checkout_ref main; then exit 99; else exit 0; fi'
            ),
        ],
        cwd=worker,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "already" in proc.stderr
    assert "worktree" in proc.stderr or "checked out" in proc.stderr
    assert _git(worker, "branch", "--show-current") == "worker"

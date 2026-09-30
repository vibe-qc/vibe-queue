"""A slot build with a configured origin, end to end through `vq admin update`.

Fresh generations keep the build input's configured upstream (#72). Earlier
end-to-end slot builds used inputs without an origin, so the build step's own
origin fetch was never exercised against an upstream. Materialization promises
that a commit already available locally needs no network; the build must keep
that promise when the upstream is unreachable.
"""
from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from vq import admin, config, paths, runtime_slots


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "-c", "user.name=Runtime Test",
         "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
         *args],
        text=True, stderr=subprocess.PIPE,
    ).strip()


@pytest.fixture
def build_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-q", "-b", "main")
    (upstream / ".gitignore").write_text(".venv/\n")
    (upstream / "build.sh").write_text(
        "#!/bin/sh\nset -eu\n"
        + shlex.quote(sys.executable) + " -m venv --without-pip .venv\n"
    )
    _git(upstream, "add", ".gitignore", "build.sh")
    _git(upstream, "commit", "-qm", "builder")
    repo = tmp_path / "legacy"
    subprocess.run(["git", "clone", "-q", str(upstream), str(repo)], check=True)
    sha = _git(repo, "rev-parse", "HEAD")
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    root = tmp_path / "runtime"
    program = config.VenvProgram(
        kind="venv", python=str(legacy_python), git_dir=str(repo),
        branch="main", update_script="build.sh", runtime_slot_root=str(root),
    )
    return upstream, repo, root, sha, program


def test_build_with_reachable_origin_keeps_the_upstream(build_input) -> None:
    upstream, _repo, root, sha, program = build_input

    result = admin._do_slot_update_work("demo", program, expected_sha=sha)

    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(root) == sha
    source = runtime_slots.slot_source(root, sha)
    assert _git(source, "remote", "get-url", "origin") == str(upstream)


def test_build_of_a_local_commit_needs_no_reachable_origin(build_input, tmp_path) -> None:
    upstream, _repo, root, sha, program = build_input
    upstream.rename(tmp_path / "upstream-unreachable")

    result = admin._do_slot_update_work("demo", program, expected_sha=sha)

    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(root) == sha
    python = runtime_slots.slot_python(root, sha)
    assert python.is_file()


def test_absent_commit_still_fails_closed_without_origin(build_input, tmp_path) -> None:
    upstream, _repo, root, _sha, program = build_input
    (upstream / "later.txt").write_text("later\n")
    _git(upstream, "add", "later.txt")
    _git(upstream, "commit", "-qm", "later")
    later = _git(upstream, "rev-parse", "HEAD")
    upstream.rename(tmp_path / "upstream-unreachable")

    result = admin._do_slot_update_work("demo", program, expected_sha=later)

    assert not result.success
    assert runtime_slots.resolve_current(root) is None

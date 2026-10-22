"""Unattended updates of a slot-rooted program read the store, and only read it.

Slot updates never move the registered build checkout, so drift measured
against its HEAD never clears. The maintainer's rule for these paths
(2026-09-24): "current" is the active generation, the target comes from a
read-only ``ls-remote``, and the no-downgrade ancestry proof runs inside the new
unpublished generation. A published generation's ``.git`` is never written
(#72).
"""
from __future__ import annotations

import hashlib
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from vq import admin, auto_update, config, paths, runtime_slots

pytestmark = [
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.no_autopatch_branch_check,
]


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "-c", "user.name=Runtime Test",
         "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
         *args],
        text=True, stderr=subprocess.PIPE,
    ).strip()


def _commit(repo: Path, name: str, *, tag: str | None = None) -> str:
    (repo / f"{name}.txt").write_text(name + "\n")
    _git(repo, "add", f"{name}.txt")
    _git(repo, "commit", "-qm", name)
    if tag is not None:
        _git(repo, "tag", "-a", tag, "-m", tag)
    return _git(repo, "rev-parse", "HEAD")


def _tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        digest.update(str(item.relative_to(path)).encode())
        if item.is_file() and not item.is_symlink():
            digest.update(item.read_bytes())
    return digest.hexdigest()


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Generation v1.1.0 active; the legacy checkout still sits at v1.0.0."""
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
    _git(upstream, "tag", "-a", "v1.0.0", "-m", "v1.0.0")
    repo = tmp_path / "legacy"
    subprocess.run(["git", "clone", "-q", str(upstream), str(repo)], check=True)
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    second = _commit(upstream, "second", tag="v1.1.0")
    root = tmp_path / "runtime"
    fields = dict(
        kind="venv", python=str(legacy_python), git_dir=str(repo),
        branch="main", update_script="build.sh", runtime_slot_root=str(root),
        runtime_slot_execution=True,
    )
    built = admin._do_slot_update_work(
        "demo", config.VenvProgram(**fields), expected_sha=second,
        expected_tag="v1.1.0",
    )
    assert built.success, built.work_errors
    assert runtime_slots.resolve_current(root) == second

    def program(*, policy: str = "tag", build_input: str = "legacy") -> config.VenvProgram:
        return config.VenvProgram(
            **fields, auto_update_policy=policy, runtime_slot_build_input=build_input,
        )

    def cfg(prog: config.VenvProgram) -> config.Config:
        return config.Config(programs={"demo": prog})

    return upstream, repo, root, second, program, cfg


def test_tag_drift_reads_the_active_generation(store) -> None:
    _upstream, _repo, _root, second, program, cfg = store

    decision = auto_update.check_env_drift("demo", cfg(program()))

    assert decision.action == "skip", decision.reason
    assert decision.current_tag == "v1.1.0"
    assert decision.current_sha == second


def test_branch_drift_reads_the_active_generation(store) -> None:
    _upstream, _repo, _root, second, program, cfg = store

    decision = auto_update.check_env_drift("demo", cfg(program(policy="branch")))

    assert decision.action == "skip", decision.reason
    assert decision.current_sha == second


def test_branch_drift_without_checkout_never_writes_a_published_generation(
    store, tmp_path,
) -> None:
    upstream, repo, root, second, program, cfg = store
    third = _commit(upstream, "third")
    repo.rename(tmp_path / "retired-legacy")
    git_meta = runtime_slots.slot_source(root, second) / ".git"
    before = _tree_digest(git_meta)

    decision = auto_update.check_env_drift(
        "demo", cfg(program(policy="branch", build_input="current")),
    )

    assert decision.action == "update", decision.reason
    assert (decision.current_sha, decision.target_sha) == (second, third)
    assert _tree_digest(git_meta) == before


def test_slot_build_refuses_a_target_that_does_not_descend_from_its_baseline(
    store,
) -> None:
    upstream, _repo, root, second, program, _cfg = store
    _git(upstream, "checkout", "-q", "-b", "side", "v1.0.0")
    sideways = _commit(upstream, "sideways")

    with admin.slot_build_requires_ancestor(second):
        result = admin._do_slot_update_work(
            "demo", program(build_input="current"), expected_sha=sideways,
        )

    assert not result.success
    assert any("descendant" in error for error in result.work_errors)
    assert runtime_slots.resolve_current(root) == second


def test_slot_build_accepts_a_descendant_of_its_baseline(store) -> None:
    upstream, _repo, root, second, program, _cfg = store
    third = _commit(upstream, "third")

    with admin.slot_build_requires_ancestor(second):
        result = admin._do_slot_update_work(
            "demo", program(), expected_sha=third,
        )

    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(root) == third


def test_build_env_job_updates_a_retired_checkout_registration(
    store, tmp_path, monkeypatch,
) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    upstream, repo, root, second, program, cfg = store
    third = _commit(upstream, "third")
    repo.rename(tmp_path / "retired-legacy")
    active_git = runtime_slots.slot_source(root, second) / ".git"
    before = _tree_digest(active_git)
    configured = cfg(program(policy="branch", build_input="current"))
    monkeypatch.setattr(config, "load_config", lambda: configured)

    stale = CliRunner().invoke(
        main, ["build-env", "demo", "--baseline-sha", third, "--expected-sha", third],
    )
    assert stale.exit_code == 2, stale.output
    assert "baseline is stale" in stale.output

    result = CliRunner().invoke(
        main, ["build-env", "demo", "--baseline-sha", second, "--expected-sha", third],
    )

    assert result.exit_code == 0, result.output
    assert runtime_slots.resolve_current(root) == third
    assert _tree_digest(active_git) == before

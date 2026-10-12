"""A slot-rooted registration is judged by its generations, not its build input.

Slot updates never move the registered legacy checkout: they build and
activate ``<root>/releases/<sha>``. Idempotence, ``vq admin status`` and the
recorded outcome must therefore read the store's generation. Reading the
legacy HEAD instead skips a real rollback as "nothing to do" and reports a SHA
the host is not running, which fleet rollout verification compares (#72).
"""
from __future__ import annotations

import subprocess
import sys
import venv
from pathlib import Path

import pytest

from vq import admin, config, paths, runtime_slots


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE,
    ).strip()


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Two verified generations; the legacy checkout is clean at the second."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    repo = tmp_path / "legacy"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / ".gitignore").write_text(".venv/\n")
    root = tmp_path / "runtime"
    shas = []
    for index in (1, 2):
        (repo / "version.txt").write_text(str(index))
        _git(repo, "add", ".gitignore", "version.txt")
        _git(repo, "-c", "user.name=Runtime Test",
             "-c", "user.email=test@example.invalid",
             "-c", "commit.gpgsign=false", "commit", "-qm", f"generation {index}")
        sha = _git(repo, "rev-parse", "HEAD")
        shas.append(sha)
        transaction = str(index) * 32
        runtime_slots.begin_slot_build(root, sha, transaction_id=transaction,
                                       in_use=lambda: set())
        runtime_slots.materialize_source(repo, root, sha)
        python = runtime_slots.slot_python(root, sha)
        venv.EnvBuilder(with_pip=False, symlinks=True).create(python.parent.parent)
        runtime_slots.seal_slot_build(root, sha, transaction_id=transaction)
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    assert _git(repo, "status", "--porcelain") == ""

    def program(*, execution: bool) -> config.VenvProgram:
        return config.VenvProgram(
            kind="venv", python=str(legacy_python), git_dir=str(repo),
            runtime_slot_root=str(root), runtime_slot_execution=execution,
        )

    return root, shas, program


@pytest.mark.parametrize("execution", [False, True])
def test_legacy_head_at_target_does_not_satisfy_a_slot_rollback(store, execution) -> None:
    root, (first, second), program = store
    runtime_slots.activate(root, first)

    current, why = admin.already_current(
        program(execution=execution), expected_sha=second, expected_tag=None,
    )

    assert current is False, why
    assert first[:12] in why


@pytest.mark.parametrize("execution", [False, True])
def test_active_verified_generation_is_already_current(store, execution) -> None:
    root, (first, second), program = store
    runtime_slots.activate(root, first)
    _git(Path(program(execution=execution).git_dir), "checkout", "-q", "--detach", second)

    current, why = admin.already_current(
        program(execution=execution), expected_sha=first, expected_tag=None,
    )

    assert current is True, why


def test_slot_store_without_a_current_generation_is_not_current(store) -> None:
    _root, (_first, second), program = store

    current, why = admin.already_current(
        program(execution=False), expected_sha=second, expected_tag=None,
    )

    assert current is False
    assert "no active runtime generation" in why


def test_admin_status_reports_the_executing_generation(store) -> None:
    root, (first, _second), program = store
    runtime_slots.activate(root, first)

    status = admin.query_env_status("demo", program(execution=True))

    assert status.error is None
    assert status.git_dir == str(runtime_slots.slot_source(root, first))
    assert status.current_sha is not None and first.startswith(status.current_sha)


def test_admin_status_reports_an_unresolvable_binding(store) -> None:
    _root, _shas, program = store

    status = admin.query_env_status("demo", program(execution=True))

    assert status.current_sha is None
    assert status.error is not None
    assert "runtime-slot execution" in status.error


def test_verified_slot_reuse_records_the_activated_generation(store) -> None:
    root, (first, second), program = store
    runtime_slots.activate(root, second)
    prog = program(execution=True)

    result = admin._do_slot_update_work("demo", prog, expected_sha=first)
    assert result.success, result.work_errors
    admin.record_update_outcome("demo", result, multi_user=False)

    record = admin.read_admin_status()["demo"]
    assert runtime_slots.resolve_current(root) == first
    assert record.last_sha is not None and first.startswith(record.last_sha)
    assert record.last_installed_sha is not None
    assert first.startswith(record.last_installed_sha)

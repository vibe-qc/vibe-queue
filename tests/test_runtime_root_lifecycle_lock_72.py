"""Lifecycle locks for a slot-rooted program include its runtime store (#72).

The build checkout is the lock identity every direct script and admin path has
used. Once build input moves to the store's own generations, that checkout may
be retired, and the only stable identity left is the canonical runtime root.
It is held as a ``checkout``-scope resource so that rollout controller
handoffs from older vq versions, which validate exactly the ``checkout`` and
``target`` scopes, still parse.
"""
from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

import pytest

from vq import admin, config, runtime_slots

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]


def _program(tmp_path: Path, name: str, root: Path | None) -> config.VenvProgram:
    repo = tmp_path / name
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q", "-b", "main"], check=True)
    python = repo / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    return config.VenvProgram(
        kind="venv", python=str(python), git_dir=str(repo),
        runtime_slot_root=None if root is None else str(root),
    )


def _root_key(root: Path) -> tuple[str, str]:
    canonical = admin._canonical_future_lifecycle_path(runtime_slots.layout(root).root)
    return ("checkout", str(canonical))


def test_slot_rooted_program_holds_its_runtime_root(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    prog = _program(tmp_path, "legacy", root)

    with admin.toolset_lifecycle_lock([prog], action="test"):
        held = admin._active_toolset_lifecycle_resources()

    assert _root_key(root) in held
    scopes = {scope for scope, _resource in held}
    assert scopes <= {"checkout", "target"}


def test_program_without_a_store_holds_no_root_key(tmp_path: Path) -> None:
    prog = _program(tmp_path, "legacy", None)

    with admin.toolset_lifecycle_lock([prog], action="test"):
        held = admin._active_toolset_lifecycle_resources()

    assert len(held) == 2


def test_registrations_sharing_a_store_contend_on_it(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    first = _program(tmp_path, "first", root)
    second = _program(tmp_path, "second", root)
    holding = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def hold() -> None:
        try:
            with admin.toolset_lifecycle_lock([first], action="holder"):
                holding.set()
                release.wait(timeout=30)
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)
            holding.set()

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert holding.wait(timeout=30)
        assert not errors
        with (
            pytest.raises(admin.AdminUpdateInProgress),
            admin.toolset_lifecycle_lock([second], action="contender"),
        ):
            pass
    finally:
        release.set()
        holder.join(timeout=30)
    assert not errors

    with admin.toolset_lifecycle_lock([second], action="after-release"):
        pass

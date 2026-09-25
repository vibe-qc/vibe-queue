"""`runtime_slot_build_input = "current"` builds from the store, not the checkout.

With the default ``"legacy"``, every generation is cloned from the registered
build checkout, so that checkout can never be retired. The explicit
``"current"`` opt-in clones the next generation from the active verified
generation instead, whose ``origin`` is the configured upstream. Update gates
and lifecycle locks then no longer need the registered checkout on disk (#72).
"""
from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

import pydantic
import pytest

from vq import admin, config, paths, runtime_slots

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "-c", "user.name=Runtime Test",
         "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
         *args],
        text=True, stderr=subprocess.PIPE,
    ).strip()


def _commit(upstream: Path, name: str) -> str:
    (upstream / f"{name}.txt").write_text(name + "\n")
    _git(upstream, "add", f"{name}.txt")
    _git(upstream, "commit", "-qm", name)
    return _git(upstream, "rev-parse", "HEAD")


@pytest.fixture
def migrated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A first generation built from the checkout, then the checkout retired."""
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
    first = _git(repo, "rev-parse", "HEAD")
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    root = tmp_path / "runtime"
    fields = dict(
        kind="venv", python=str(legacy_python), git_dir=str(repo),
        branch="main", update_script="build.sh", runtime_slot_root=str(root),
        runtime_slot_execution=True,
    )
    built = admin._do_slot_update_work(
        "demo", config.VenvProgram(**fields), expected_sha=first,
    )
    assert built.success, built.work_errors
    assert runtime_slots.resolve_current(root) == first

    def program(build_input: str) -> config.VenvProgram:
        return config.VenvProgram(**fields, runtime_slot_build_input=build_input)

    return upstream, repo, root, first, program


def test_default_is_legacy_and_current_requires_execution_binding(tmp_path) -> None:
    base = dict(kind="venv", python=str(tmp_path / "s/.venv/bin/python"),
                git_dir=str(tmp_path / "s"))
    assert config.VenvProgram(**base).runtime_slot_build_input == "legacy"
    with pytest.raises(pydantic.ValidationError, match="runtime_slot_execution"):
        config.VenvProgram(**base, runtime_slot_root=str(tmp_path / "rt"),
                           runtime_slot_build_input="current")
    with pytest.raises(pydantic.ValidationError):
        config.VenvProgram(**base, runtime_slot_build_input="elsewhere")


def test_current_input_builds_the_next_generation_without_the_checkout(
    migrated, tmp_path,
) -> None:
    upstream, repo, root, first, program = migrated
    second = _commit(upstream, "second")
    repo.rename(tmp_path / "retired-legacy")
    prog = program("current")
    cfg = config.Config(programs={"demo": prog})

    assert admin._resolve_venv_program("demo", cfg) is prog
    with admin.toolset_lifecycle_lock([prog], action="test"):
        held = admin._active_toolset_lifecycle_resources()
    assert all(not resource.startswith(str(tmp_path / "legacy")) for _s, resource in held)

    result = admin._do_slot_update_work("demo", prog, expected_sha=second)

    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(root) == second
    source = runtime_slots.slot_source(root, second)
    assert _git(source, "remote", "get-url", "origin") == str(upstream)
    assert _git(runtime_slots.slot_source(root, first), "rev-parse", "HEAD") == first


def test_legacy_input_still_needs_the_checkout(migrated, tmp_path) -> None:
    upstream, repo, _root, _first, program = migrated
    _commit(upstream, "second")
    repo.rename(tmp_path / "retired-legacy")
    cfg = config.Config(programs={"demo": program("legacy")})

    with pytest.raises(admin.AdminError, match="not a directory"):
        admin._resolve_venv_program("demo", cfg)


def test_current_input_without_an_active_generation_refuses(tmp_path) -> None:
    repo = tmp_path / "legacy"
    prog = config.VenvProgram(
        kind="venv", python=str(repo / ".venv/bin/python"), git_dir=str(repo),
        runtime_slot_root=str(tmp_path / "runtime"), runtime_slot_execution=True,
        runtime_slot_build_input="current",
    )
    cfg = config.Config(programs={"demo": prog})

    with pytest.raises(admin.AdminError, match="no active runtime generation"):
        admin._resolve_venv_program("demo", cfg)


def test_current_input_keeps_the_checkout_locks_while_it_exists(migrated) -> None:
    _upstream, repo, _root, _first, program = migrated

    with admin.toolset_lifecycle_lock([program("current")], action="test"):
        held = admin._active_toolset_lifecycle_resources()

    checkout = str(admin._canonical_lifecycle_checkout(repo))
    assert ("checkout", checkout) in held


def test_extras_route_reads_the_generation_once_the_checkout_is_gone(
    migrated, tmp_path,
) -> None:
    _upstream, repo, root, first, program = migrated
    repo.rename(tmp_path / "retired-legacy")

    routes = admin.declared_extras_routes(program("current"))

    assert routes.update != admin.ExtrasRoute.UNKNOWN, routes.update_detail
    assert str(runtime_slots.slot_source(root, first)) in routes.update_detail


def test_programs_json_reports_the_effective_build_input(migrated, tmp_path) -> None:
    from vq import cli

    _upstream, repo, root, first, program = migrated
    repo.rename(tmp_path / "retired-legacy")

    record = cli._program_record("demo", program("current"))

    assert record["runtime_slot_build_input"] == "current"
    assert record["build_git_dir"] == str(runtime_slots.slot_source(root, first))
    assert record["build_python"] == str(runtime_slots.slot_python(root, first))
    legacy_record = cli._program_record("demo", program("legacy"))
    assert legacy_record["runtime_slot_build_input"] == "legacy"
    assert legacy_record["build_git_dir"] == str(repo)


def test_serving_registration_refuses_current_input(migrated, monkeypatch) -> None:
    upstream, _repo, _root, _first, program = migrated
    second = _commit(upstream, "second")
    prog = program("current")
    cfg = config.Config(programs={"queue": prog})
    probe = admin._SelfUpdateProbe(
        is_self_update=True, daemon_running=True, service_manager="systemd",
        manager_available=True, diagnostic="sandbox serving registration",
    )
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _prog: probe)

    with pytest.raises(admin.AdminError, match="serves the vq daemon"):
        admin.update_env("queue", cfg, host="localhost", expected_sha=second)

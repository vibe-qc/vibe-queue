"""rollout-latest admits a slot-served driver (#146).

Once the driver's vibeqc-queue registration is slot-executed, the controller
runs from its verified slot generation, and the registered git_dir/python are
compatibility paths that ``retire-checkout`` may remove. The controller check
compared the running checkout and interpreter against those registered paths,
so every rollout refused. It also canonicalized the retired checkout and
failed outright. It now binds ``execution_program()``. A real mismatch still
refuses, and the re-entry handoff is checked against the exact resources the
controller locks.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_fleet_rollout_cli import _action, _patch_discovery, _plan, _report
from vq import admin, config, fleet_rollout
from vq.cli import main

LEGACY = "/legacy/vibe-queue"
SLOT = "/srv/vq-runtime/releases/abc/source"


def _slot_served(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    running_checkout: str = SLOT,
    running_python: str = f"{SLOT}/.venv/bin/python",
) -> list[str]:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    cfg = config.load_config()
    cfg.programs["vibeqc-queue"] = cfg.programs["vibeqc-queue"].model_copy(update={
        "git_dir": LEGACY,
        "python": f"{LEGACY}/.venv/bin/python",
        "runtime_slot_root": str(tmp_path / "runtime"),
        "runtime_slot_execution": True,
        "runtime_slot_build_input": "current",
    })
    resolved: list[str] = []

    def execution_program(self: config.VenvProgram) -> config.VenvProgram:
        resolved.append(self.git_dir)
        return self.model_copy(update={
            "git_dir": SLOT,
            "python": f"{SLOT}/.venv/bin/python",
            "runtime_slot_execution": False,
        })

    def canonical_checkout(path: Path) -> Path:
        if str(path).startswith(LEGACY):
            raise admin.AdminError(f"{path} is not a git checkout (retired)")
        return Path(path)

    monkeypatch.setattr(config.VenvProgram, "execution_program", execution_program)
    monkeypatch.setattr("vq.cli.admin_module._canonical_lifecycle_checkout",
                        canonical_checkout)
    monkeypatch.setattr("vq.cli.admin_module._canonical_lifecycle_target",
                        lambda path: Path(path))
    monkeypatch.setattr("vq.cli.fleet_release.runtime_repo",
                        lambda: Path(running_checkout))
    monkeypatch.setattr("vq.cli.sys.executable", running_python)
    return resolved


def test_a_slot_served_driver_with_a_retired_checkout_can_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    resolved = _slot_served(monkeypatch, tmp_path)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "rollout-latest dry run" in result.output
    assert resolved == [LEGACY]


def test_a_controller_outside_the_selected_slot_still_refuses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _slot_served(monkeypatch, tmp_path, running_checkout="/srv/vq-runtime/releases/old/source")

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run"])

    assert result.exit_code == 1
    assert "does not match the running controller checkout" in result.output


def test_an_interpreter_outside_the_selected_slot_still_refuses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _slot_served(monkeypatch, tmp_path, running_python=f"{LEGACY}/.venv/bin/python")

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run"])

    assert result.exit_code == 1
    assert "running controller interpreter" in result.output


def test_the_handoff_is_checked_against_the_locked_resources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _slot_served(monkeypatch, tmp_path)
    seen: dict[str, object] = {}
    real = fleet_rollout.adopt_rollout_reentry_handoff

    def adopt(**kwargs):
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr("vq.cli.fleet_rollout.adopt_rollout_reentry_handoff", adopt)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run"])

    assert result.exit_code == 0, result.output
    prog = config.load_config().programs["vibeqc-queue"]
    assert seen["expected_lifecycle_resources"] == admin.toolset_lifecycle_resources(
        [prog], extra_resources=(("checkout", "/repo"),))
    assert ("target", f"{LEGACY}/.venv") not in seen["expected_lifecycle_resources"]


def test_reentry_resolves_the_slot_each_time(monkeypatch: pytest.MonkeyPatch) -> None:
    from vq.cli import _rollout_controller_program

    generations = iter(["one", "two"])
    prog = config.VenvProgram(kind="venv", python=f"{LEGACY}/.venv/bin/python",
                              git_dir=LEGACY)
    monkeypatch.setattr(
        config.VenvProgram, "execution_program",
        lambda self: self.model_copy(update={"python": f"/slots/{next(generations)}/py"}))

    assert _rollout_controller_program(prog).python == "/slots/one/py"
    assert _rollout_controller_program(prog).python == "/slots/two/py"

    def broken(self):
        raise config.ConfigError("no verified generation")

    monkeypatch.setattr(config.VenvProgram, "execution_program", broken)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="runtime slot"):
        _rollout_controller_program(prog)


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.parametrize("legacy_present", [True, False])
def test_resources_match_what_the_lock_holds(tmp_path: Path, legacy_present: bool) -> None:
    repo = tmp_path / "legacy"
    if legacy_present:
        repo.mkdir()
        subprocess.run(["git", "-C", str(repo), "init", "-q", "-b", "main"], check=True)
        python = repo / ".venv/bin/python"
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)
    prog = config.VenvProgram(
        kind="venv", python=str(repo / ".venv/bin/python"), git_dir=str(repo),
        runtime_slot_root=str(tmp_path / "runtime"), runtime_slot_execution=True,
        runtime_slot_build_input="current",
    )

    with admin.toolset_lifecycle_lock([prog], action="test"):
        held = admin._active_toolset_lifecycle_resources()

    assert tuple(sorted(held)) == admin.toolset_lifecycle_resources([prog])
    assert len(held) == (3 if legacy_present else 1)

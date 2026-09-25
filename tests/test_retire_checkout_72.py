"""Reviewed deletion of a legacy build checkout (#72): plan, then delete."""
from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, checkout_retirement, config, paths, runtime_retention, runtime_slots
from vq.spec import JobSpec, JobState

pytestmark = [
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.no_autopatch_branch_check,
]


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "-c", "user.name=T", "-c", "user.email=t@example.invalid",
         "-c", "commit.gpgsign=false", *args], text=True, stderr=subprocess.PIPE,
    ).strip()


@pytest.fixture
def migrated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-q", "-b", "main")
    (upstream / ".gitignore").write_text(".venv/\n")
    (upstream / "build.sh").write_text(
        "#!/bin/sh\nset -eu\n" + shlex.quote(sys.executable) + " -m venv --without-pip .venv\n"
    )
    _git(upstream, "add", ".gitignore", "build.sh")
    _git(upstream, "commit", "-qm", "builder")
    repo = tmp_path / "legacy"
    subprocess.run(["git", "clone", "-q", str(upstream), str(repo)], check=True)
    sha = _git(repo, "rev-parse", "HEAD")
    python = repo / ".venv/bin/python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    fields = dict(kind="venv", python=str(python), git_dir=str(repo), branch="main",
                  update_script="build.sh", runtime_slot_root=str(tmp_path / "runtime"),
                  runtime_slot_execution=True)
    assert admin._do_slot_update_work("demo", config.VenvProgram(**fields),
                                      expected_sha=sha).success
    current = config.VenvProgram(**fields, runtime_slot_build_input="current")
    specs: list[JobSpec] = []
    monkeypatch.setattr(admin, "_runtime_slot_spec_snapshot", lambda: list(specs))
    monkeypatch.setattr(runtime_retention, "_service_files", lambda: ([], []))
    return repo, fields, config.Config(programs={"demo": current}), specs


def test_a_clean_pushed_checkout_is_deleted_by_its_reviewed_plan(migrated) -> None:
    repo, _fields, cfg, _specs = migrated
    value = checkout_retirement.plan("demo", cfg)
    assert value["blockers"] == []
    assert value["head"] == _git(repo, "rev-parse", "HEAD")

    receipt = checkout_retirement.apply("demo", cfg, value["plan_id"])

    assert receipt["removed"] is True
    assert not repo.exists()
    assert json.loads(Path(receipt["receipt"]).read_text())["head"] == value["head"]
    assert admin._resolve_venv_program("demo", cfg) is cfg.programs["demo"]


@pytest.mark.parametrize("dirt", ["modified", "stash", "unpushed", "untracked"])
def test_local_work_blocks_retirement(migrated, dirt) -> None:
    repo, _fields, cfg, _specs = migrated
    if dirt == "modified":
        (repo / "build.sh").write_text("changed\n")
    elif dirt == "untracked":
        (repo / "notes.txt").write_text("mine\n")
    elif dirt == "stash":
        (repo / "build.sh").write_text("changed\n")
        _git(repo, "stash")
    else:
        (repo / "local.txt").write_text("x\n")
        _git(repo, "add", "local.txt")
        _git(repo, "commit", "-qm", "local only")

    value = checkout_retirement.plan("demo", cfg)

    assert value["blockers"]
    with pytest.raises(checkout_retirement.RetirementError, match="blockers"):
        checkout_retirement.apply("demo", cfg, value["plan_id"])
    assert repo.is_dir()


def test_an_alias_still_building_from_the_checkout_blocks(migrated) -> None:
    _repo, fields, cfg, _specs = migrated
    cfg = config.Config(programs={**cfg.programs, "demo-dev": config.VenvProgram(**fields)})

    value = checkout_retirement.plan("demo", cfg)

    assert value["blockers"] == ["registration 'demo-dev' still builds from this checkout"]


def test_live_jobs_using_the_checkout_block(migrated) -> None:
    repo, _fields, cfg, specs = migrated
    running = JobSpec(id="job1", cwd="/tmp", cpus=1,
                      command=[str(repo / ".venv/bin/python"), "run.py"])
    running.state = JobState.RUNNING
    started = JobSpec(id="job2", cwd="/tmp", cpus=1, command=["sh", "x"], program="demo")
    started.state = JobState.RUNNING
    specs.extend([running, started])

    blockers = checkout_retirement.plan("demo", cfg)["blockers"]

    assert "live job job1 names this checkout" in blockers
    assert "started job job2 has no recorded runtime selection" in blockers


def test_a_legacy_build_input_is_refused(migrated) -> None:
    _repo, fields, _cfg, _specs = migrated
    cfg = config.Config(programs={"demo": config.VenvProgram(**fields)})

    with pytest.raises(checkout_retirement.RetirementError, match="still builds"):
        checkout_retirement.plan("demo", cfg)


def test_a_changed_checkout_refuses_the_stale_plan(migrated) -> None:
    repo, _fields, cfg, _specs = migrated
    value = checkout_retirement.plan("demo", cfg)
    (repo / "late.txt").write_text("x\n")

    with pytest.raises(checkout_retirement.RetirementError, match="plan again"):
        checkout_retirement.apply("demo", cfg, value["plan_id"])
    assert repo.is_dir()


def test_cli_plans_by_default(migrated, monkeypatch) -> None:
    from vq.cli import main

    repo, _fields, cfg, _specs = migrated
    monkeypatch.setattr(config, "load_config", lambda: cfg)

    planned = CliRunner().invoke(main, ["admin", "retire-checkout", "demo"])
    assert planned.exit_code == 0, planned.output
    value = json.loads(planned.output)
    assert repo.is_dir()
    applied = CliRunner().invoke(main, ["admin", "retire-checkout", "demo",
                                        "--apply-plan", value["plan_id"]])
    assert applied.exit_code == 0, applied.output
    assert not repo.exists()
    assert runtime_slots.resolve_current(Path(cfg.programs["demo"].runtime_slot_root))

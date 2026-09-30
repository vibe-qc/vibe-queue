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


def _index(tmp_path: Path, program: str, state: str | None, bindings=()) -> Path:
    """Register a migrate-runtime receipt in a given state (None: no journal)."""
    from vq import runtime_migration

    receipt = tmp_path / f"receipt-{program}-{state}"
    receipt.mkdir()
    if state is not None:
        (receipt / "transaction.json").write_text(json.dumps({
            "schema": runtime_migration.SCHEMA, "state": state,
            "plan": {"launchers": [{"before": {"path": str(p)}} for p in bindings],
                     "registries": []},
        }))
    runtime_migration.register_receipt(receipt, program)
    return receipt


@pytest.mark.parametrize(
    ("state", "blocked"),
    [("applying", True), ("prepared", True), ("rolling_back", True),
     ("applied", False), ("rolled_back", False), (None, False)],
)
def test_an_unfinished_migration_of_the_program_blocks(
    migrated, tmp_path: Path, state, blocked,
) -> None:
    """#123: a transaction that may still roll back onto the checkout blocks."""
    _repo, _fields, cfg, _specs = migrated
    _index(tmp_path, "demo", state)
    _index(tmp_path, "unrelated", "applying")
    blockers = checkout_retirement.plan("demo", cfg)["blockers"]
    assert bool([b for b in blockers if "migrate-runtime transaction" in b]) is blocked
    assert not any("unrelated" in b for b in blockers)


def test_an_unreadable_migration_receipt_blocks(migrated, tmp_path: Path) -> None:
    _repo, _fields, cfg, _specs = migrated
    receipt = _index(tmp_path, "demo", "applied")
    (receipt / "transaction.json").unlink()
    receipt.rmdir()
    blockers = checkout_retirement.plan("demo", cfg)["blockers"]
    assert any("unreadable transaction" in b for b in blockers)


def test_a_migrated_launcher_naming_the_checkout_blocks(migrated, tmp_path: Path) -> None:
    repo, _fields, cfg, _specs = migrated
    launcher = tmp_path / "bin-python"
    launcher.write_text(f"#!/bin/sh\nexec {repo}/.venv/bin/python \"$@\"\n")
    _index(tmp_path, "demo", "applied", [launcher])
    assert any("migrated binding" in b and str(launcher) in b
               for b in checkout_retirement.plan("demo", cfg)["blockers"])
    launcher.write_text("#!/bin/sh\nexec /opt/elsewhere/python \"$@\"\n")
    assert checkout_retirement.plan("demo", cfg)["blockers"] == []


def test_a_declared_launcher_is_checked_and_part_of_the_plan(migrated, tmp_path: Path) -> None:
    repo, _fields, cfg, _specs = migrated
    launcher = tmp_path / "wrapper"
    launcher.write_text(f"#!/bin/sh\nexec {repo}/.venv/bin/python \"$@\"\n")
    blocked = checkout_retirement.plan("demo", cfg, launchers=(str(launcher),))
    assert any("declared launcher" in b for b in blocked["blockers"])
    launcher.write_text("#!/bin/sh\nexec /opt/elsewhere/python \"$@\"\n")
    clean = checkout_retirement.plan("demo", cfg, launchers=(str(launcher),))
    assert clean["blockers"] == [] and clean["launchers"] == [str(launcher)]
    missing = tmp_path / "gone"
    assert any("does not exist" in b for b in checkout_retirement.plan(
        "demo", cfg, launchers=(str(missing),))["blockers"])
    with pytest.raises(checkout_retirement.RetirementError, match="changed"):
        checkout_retirement.apply("demo", cfg, clean["plan_id"])
    assert checkout_retirement.apply(
        "demo", cfg, clean["plan_id"], launchers=(str(launcher),))["removed"] is True


def test_cli_exits_1_for_a_plan_with_blockers(migrated, monkeypatch, tmp_path: Path) -> None:
    """#123: a blocked plan exits 1, like migrate-runtime and repair-state-paths."""
    from vq.cli import main

    _repo, _fields, cfg, _specs = migrated
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    _index(tmp_path, "demo", "applying")
    result = CliRunner().invoke(main, ["admin", "retire-checkout", "demo"])
    assert result.exit_code == 1
    assert json.loads(result.output)["blockers"]


def test_an_unreadable_receipt_index_blocks(migrated, tmp_path: Path) -> None:
    """#123 follow-up: Path.glob swallowed a PermissionError and reported nothing."""
    import os

    from vq import runtime_migration

    _repo, _fields, cfg, _specs = migrated
    _index(tmp_path, "demo", "applied")
    index = runtime_migration.index_dir()
    index.chmod(0)
    try:
        if os.access(index, os.R_OK):
            pytest.skip("running with privileges that ignore directory modes")
        blockers = checkout_retirement.plan("demo", cfg)["blockers"]
    finally:
        index.chmod(0o700)
    assert any("receipt index" in b for b in blockers)

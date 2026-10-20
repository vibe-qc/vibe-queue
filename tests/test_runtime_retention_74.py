"""Reviewed runtime-generation retention (#74): plan, then apply exactly that."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import venv
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths, runtime_retention, runtime_slots
from vq.spec import JobSpec, JobState, ProgramRuntimePin

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Four verified generations, activated in order: current D, previous C."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    repo = tmp_path / "legacy"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(repo), "-c", "user.name=T", "-c", "user.email=t@example.invalid",
             "-c", "commit.gpgsign=false", *args], text=True, stderr=subprocess.PIPE,
        ).strip()

    git("init", "-q", "-b", "main")
    root = tmp_path / "runtime"
    shas = []
    for index in range(4):
        (repo / "v.txt").write_text(str(index))
        git("add", "v.txt")
        git("commit", "-qm", f"g{index}")
        sha = git("rev-parse", "HEAD")
        shas.append(sha)
        transaction = f"{index + 1}" * 32
        runtime_slots.begin_slot_build(root, sha, transaction_id=transaction, in_use=set)
        runtime_slots.materialize_source(repo, root, sha)
        python = runtime_slots.slot_python(root, sha)
        venv.EnvBuilder(with_pip=False, symlinks=True).create(python.parent.parent)
        runtime_slots.seal_slot_build(root, sha, transaction_id=transaction)
        os.utime(runtime_slots.slot_path(root, sha), (1_000_000 + index, 1_000_000 + index))
        runtime_slots.activate(root, sha)
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    program = config.VenvProgram(
        kind="venv", python=str(legacy_python), git_dir=str(repo),
        runtime_slot_root=str(root), runtime_slot_execution=True,
    )
    cfg = config.Config(programs={"demo": program})
    services: dict[str, object] = {"files": [], "blockers": []}
    monkeypatch.setattr(
        runtime_retention, "_service_files",
        lambda: (list(services["files"]), list(services["blockers"])),
    )
    specs: list[JobSpec] = []
    monkeypatch.setattr(admin, "_runtime_slot_spec_snapshot", lambda: list(specs))
    return root, shas, cfg, specs, services


def _spec(job_id: str, state: JobState, **fields) -> JobSpec:
    spec = JobSpec(id=job_id, cwd="/tmp", cpus=1, command=["sh", "payload.sh"], **fields)
    spec.state = state
    return spec


def _kept(value: dict) -> dict[str, list[str]]:
    return {g["sha"]: g["reasons"] for g in value["generations"] if g["retain"]}


def test_plan_keeps_pointers_and_keep_n_and_proposes_the_rest(store) -> None:
    root, (a, b, c, d), cfg, _specs, _services = store

    none_extra = runtime_retention.plan("demo", cfg, keep=0)
    one_extra = runtime_retention.plan("demo", cfg, keep=1)

    assert none_extra["candidates"] == [b, a]
    assert _kept(none_extra) == {d: ["current"], c: ["previous"]}
    assert one_extra["candidates"] == [a]
    assert _kept(one_extra)[b] == ["within --keep 1"]
    assert none_extra["reclaimable_bytes"] > 0
    assert sorted(p.name for p in (root / "releases").iterdir()) == sorted([a, b, c, d])


def test_a_pinned_running_job_keeps_its_generation(store) -> None:
    root, (a, _b, _c, _d), cfg, specs, _services = store
    pin = ProgramRuntimePin(
        resolved_runtime_root=str(root),
        resolved_runtime_source=str(runtime_slots.slot_source(root, a)),
        resolved_runtime_python=str(runtime_slots.slot_python(root, a)),
    )
    specs.append(_spec("job1", JobState.RUNNING, program="demo", program_runtime_pin=pin))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert a not in value["candidates"]
    assert _kept(value)[a] == ["live job job1"]


def test_a_started_job_without_a_provable_generation_keeps_everything(store) -> None:
    _root, _shas, cfg, specs, _services = store
    specs.append(_spec("job2", JobState.RUNNING))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["candidates"] == []
    assert value["census"]["unresolved_live_jobs"] == ["job2"]


def test_unstarted_and_remote_jobs_hold_no_local_generation(store) -> None:
    _root, (a, b, _c, _d), cfg, specs, _services = store
    specs.append(_spec("queued", JobState.PENDING))
    specs.append(_spec("remote", JobState.RUNNING, scheduler_target="cluster"))
    specs.append(_spec("done", JobState.COMPLETED))

    value = runtime_retention.plan("demo", cfg, keep=0)

    assert value["candidates"] == [b, a]


def test_service_definitions_and_declarations_are_kept(store, tmp_path) -> None:
    root, (a, b, _c, _d), cfg, _specs, services = store
    unit = tmp_path / "vq-daemon.service"
    unit.write_text(f"ExecStart={runtime_slots.slot_python(root, a)} -m vq.cli daemon\n")
    services["files"] = [unit]

    value = runtime_retention.plan("demo", cfg, keep=0, retain=(b,))

    assert value["candidates"] == []
    assert _kept(value)[a] == [f"service {unit}"]
    assert _kept(value)[b] == ["declared by operator"]


def test_unreadable_service_authority_blocks_apply(store) -> None:
    _root, _shas, cfg, _specs, services = store
    services["blockers"] = ["systemd user unit authority is unavailable: test"]
    value = runtime_retention.plan("demo", cfg, keep=0)

    with pytest.raises(runtime_retention.RetentionError, match="blockers"):
        runtime_retention.apply("demo", cfg, value["plan_id"], keep=0)


def test_apply_removes_exactly_the_reviewed_candidates(store) -> None:
    root, (a, b, c, d), cfg, _specs, _services = store
    value = runtime_retention.plan("demo", cfg, keep=1)

    receipt = runtime_retention.apply("demo", cfg, value["plan_id"], keep=1)

    assert receipt["removed"] == [a]
    assert receipt["failed"] == {}
    assert sorted(p.name for p in (root / "releases").iterdir()) == sorted([b, c, d])
    assert json.loads(Path(receipt["receipt"]).read_text())["removed"] == [a]
    assert runtime_slots.resolve_current(root) == d
    assert runtime_slots.rollback(root) == c


def test_cli_can_retain_only_current_and_clear_rollback(store, monkeypatch) -> None:
    from vq.cli import main

    root, (a, b, c, d), cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    args = ["admin", "runtime-retention", "demo", "--keep", "0", "--drop-previous"]
    planned = CliRunner().invoke(main, args)
    assert planned.exit_code == 0, planned.output
    value = json.loads(planned.output)
    assert value["candidates"] == [c, b, a]
    assert runtime_slots._previous_sha(root) == c

    applied = CliRunner().invoke(main, [*args, "--apply-plan", value["plan_id"]])
    assert applied.exit_code == 0, applied.output
    receipt = json.loads(applied.output)
    assert receipt["removed"] == [c, b, a]
    assert receipt["previous_cleared"] == c
    assert receipt["failed"] == {}
    assert runtime_slots.list_slots(root) == [d]
    assert runtime_slots.resolve_current(root) == d
    assert runtime_slots.rollback(root) is None
    assert not (root / "previous").is_symlink()


@pytest.mark.parametrize("reference", ["job", "queued-pointer", "retain", "service-pointer"])
def test_drop_previous_keeps_referenced_rollback(store, tmp_path, reference) -> None:
    root, (a, b, c, d), cfg, specs, services = store
    retain = ()
    if reference == "job":
        specs.append(_spec("live", JobState.RUNNING,
                           program_runtime_pin=ProgramRuntimePin(
                               resolved_runtime_root=str(root),
                               resolved_runtime_source=str(runtime_slots.slot_source(root, c)),
                               resolved_runtime_python=str(runtime_slots.slot_python(root, c)),
                           )))
    elif reference == "queued-pointer":
        spec = _spec("queued", JobState.PENDING)
        spec.command = [str(root / "previous/venv/bin/python"), "payload.py"]
        specs.append(spec)
    elif reference == "retain":
        retain = (c,)
    else:
        unit = tmp_path / "vq-daemon.service"
        unit.write_text(f"ExecStart={root}/previous/venv/bin/python -m vq.cli daemon\n")
        services["files"] = [unit]
    value = runtime_retention.plan("demo", cfg, keep=0, drop_previous=True, retain=retain)
    assert value["candidates"] == [b, a]
    receipt = runtime_retention.apply(
        "demo", cfg, value["plan_id"], keep=0, drop_previous=True, retain=retain,
    )
    assert receipt["removed"] == [b, a]
    assert runtime_slots.list_slots(root) == [d, c]
    assert runtime_slots._previous_sha(root) == c


@pytest.mark.parametrize("change", ["pointer", "reference"])
def test_drop_previous_rejects_stale_plan(store, change) -> None:
    root, (a, b, c, d), cfg, specs, _services = store
    value = runtime_retention.plan("demo", cfg, keep=0, drop_previous=True)
    if change == "pointer":
        runtime_slots._atomic_symlink(Path("releases") / b, root / "previous")
    else:
        specs.append(_spec("late", JobState.RUNNING))
    with pytest.raises(runtime_retention.RetentionError, match="plan again"):
        runtime_retention.apply("demo", cfg, value["plan_id"], keep=0, drop_previous=True)
    assert runtime_slots.list_slots(root) == [d, c, b, a]


@pytest.mark.parametrize("damage", ["missing", "modified"])
def test_drop_previous_requires_verified_current(store, damage) -> None:
    root, (_a, _b, _c, d), cfg, _specs, _services = store
    if damage == "missing":
        (root / "current").unlink()
    else:
        (runtime_slots.slot_source(root, d) / "v.txt").write_text("modified")
    value = runtime_retention.plan("demo", cfg, keep=0, drop_previous=True)
    assert any("verified current" in blocker for blocker in value["blockers"])
    with pytest.raises(runtime_retention.RetentionError, match="blockers"):
        runtime_retention.apply("demo", cfg, value["plan_id"], keep=0, drop_previous=True)


def test_drop_previous_cannot_be_scheduled(store, monkeypatch) -> None:
    from vq.cli import main

    _root, _shas, cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    result = CliRunner().invoke(main, [
        "admin", "runtime-retention", "demo", "--keep", "0", "--drop-previous", "--schedule",
    ])
    assert result.exit_code == 2
    assert "one-off reviewed plan" in result.output
    assert runtime_retention.read_schedule() == {}


def test_drop_previous_unlinks_before_partial_removal(store, monkeypatch) -> None:
    root, (_a, _b, c, d), cfg, _specs, _services = store
    value = runtime_retention.plan("demo", cfg, keep=0, drop_previous=True)
    remove = runtime_retention.shutil.rmtree

    def fail_previous(path):
        if path.name == c:
            assert not (root / "previous").is_symlink()
            raise OSError("partial removal")
        remove(path)

    monkeypatch.setattr(runtime_retention.shutil, "rmtree", fail_previous)
    receipt = runtime_retention.apply(
        "demo", cfg, value["plan_id"], keep=0, drop_previous=True,
    )
    assert receipt["failed"] == {c: "partial removal"}
    assert receipt["previous_cleared"] == c
    assert runtime_slots.resolve_current(root) == d
    assert runtime_slots.rollback(root) is None


def test_a_changed_store_refuses_the_stale_plan(store) -> None:
    root, (a, _b, _c, _d), cfg, specs, _services = store
    value = runtime_retention.plan("demo", cfg, keep=0)
    specs.append(_spec("late", JobState.RUNNING))

    with pytest.raises(runtime_retention.RetentionError, match="plan again"):
        runtime_retention.apply("demo", cfg, value["plan_id"], keep=0)
    assert (root / "releases" / a).is_dir()


def test_cli_defaults_to_the_read_only_plan(store, monkeypatch) -> None:
    from vq.cli import main

    root, (a, _b, _c, _d), cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)

    planned = CliRunner().invoke(main, ["admin", "runtime-retention", "demo", "--keep", "1"])
    assert planned.exit_code == 0, planned.output
    value = json.loads(planned.output)
    assert value["candidates"] == [a]
    assert (root / "releases" / a).is_dir()

    applied = CliRunner().invoke(main, [
        "admin", "runtime-retention", "demo", "--keep", "1",
        "--apply-plan", value["plan_id"],
    ])
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["removed"] == [a]
    assert not (root / "releases" / a).exists()


def test_schedule_round_trip_through_the_cli(store, monkeypatch) -> None:
    from vq.cli import main

    _root, _shas, cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)

    added = CliRunner().invoke(main, ["admin", "runtime-retention", "demo", "--schedule",
                                      "--keep", "1"])
    assert added.exit_code == 0, added.output
    assert runtime_retention.read_schedule() == {"demo": 1}
    removed = CliRunner().invoke(main, ["admin", "runtime-retention", "demo", "--unschedule"])
    assert removed.exit_code == 0, removed.output
    assert runtime_retention.read_schedule() == {}


def test_auto_cleanup_sweep_applies_scheduled_retention(store, monkeypatch) -> None:
    from vq import cleanup

    root, (a, b, c, d), cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    runtime_retention.write_schedule({"demo": 1})
    paths.queue_dir().mkdir(parents=True, exist_ok=True)

    counts = cleanup.run_auto_cleanup_pass(
        cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 30)
    )

    assert counts["runtime_generations_removed"] == 1
    assert sorted(p.name for p in (root / "releases").iterdir()) == sorted([b, c, d])


def test_scheduled_retention_skips_while_an_update_owns_the_host(store, monkeypatch) -> None:
    import contextlib

    from vq import cleanup

    root, (a, _b, _c, _d), cfg, _specs, _services = store
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    runtime_retention.write_schedule({"demo": 0})
    paths.queue_dir().mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def held():
        raise admin.AdminUpdateInProgress("an update owns this host")
        yield  # pragma: no cover

    monkeypatch.setattr(admin, "admin_update_ownership", held)

    counts = cleanup.run_auto_cleanup_pass(
        cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 30)
    )

    assert counts["runtime_generations_removed"] == 0
    assert counts["runtime_retention_skipped"] == 1
    assert (root / "releases" / a).is_dir()

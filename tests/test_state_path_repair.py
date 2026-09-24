"""Terminal record repair preserves the current fetch referent, not guessed history."""

from __future__ import annotations

import hashlib
import json
import os
import tarfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import fetch, paths
from vq import state_path_repair as repair
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def migrated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    new = tmp_path / "current"
    old = tmp_path / "retired"
    queue = new / "queue"
    queue.mkdir(parents=True)
    workspace = new / "jobs" / "job123"
    workspace.mkdir(parents=True)
    (workspace / "result.txt").write_bytes(b"verified current output\n")
    spec = JobSpec(
        id="job123",
        command=["true"],
        cpus=1,
        cwd=str(old / "jobs" / "job123"),
        workdir=str(old / "workdirs" / "job123"),
        state=JobState.COMPLETED,
    )
    record = queue / "job123.json"
    spec.write(record)
    raw = json.loads(record.read_bytes())
    raw["future_extension"] = {"nested": [1, "preserve me"]}
    record.write_text(json.dumps(raw, indent=4) + "\n")
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(new))
    return old, new, record


def alias_plan(migrated):
    old, new, _ = migrated
    old.symlink_to(new, target_is_directory=True)
    return repair.plan_repair(new, old, new)


def test_fetch_red_to_green_after_current_referent_preserving_repair(migrated, tmp_path):
    old, new, record = migrated
    original = record.read_bytes()
    with pytest.raises(FileNotFoundError, match="workspace"):
        fetch.fetch_local("job123", tmp_path / "before")
    plan = alias_plan(migrated)
    assert plan["blockers"] == []
    assert plan["records"][0]["fields"]["workdir"]["artifact"]["kind"] == "absent"
    result = repair.apply_repair(
        plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
    )
    assert result["records_changed"] == 1
    updated = json.loads(record.read_bytes())
    before = json.loads(original)
    for field in repair.FIELDS:
        if isinstance(before.get(field), str):
            before[field] = before[field].replace(str(old), str(new), 1)
    assert updated == before
    backups = list((tmp_path / "receipt").glob("*.original.json"))
    assert len(backups) == 1 and backups[0].read_bytes() == original
    old.unlink()  # The repaired record no longer depends on the fixture alias.
    output = fetch.fetch_local("job123", tmp_path / "after")
    assert (output / "result.txt").read_bytes() == b"verified current output\n"
    assert (new / "jobs" / "job123" / "result.txt").read_bytes() == b"verified current output\n"


def test_read_only_plan_is_repeatable_and_does_not_create_lock(migrated):
    _, new, record = migrated
    plan = alias_plan(migrated)
    before = {str(p): p.read_bytes() for p in new.rglob("*") if p.is_file()}
    assert repair.plan_repair(new, Path(plan["old_root"]), new) == plan
    assert before == {str(p): p.read_bytes() for p in new.rglob("*") if p.is_file()}
    assert not paths.spec_lock_path(record).exists()


@pytest.mark.parametrize(
    "state",
    [
        s
        for s in JobState
        if s
        not in {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.KILLED,
            JobState.INTERRUPTED,
            JobState.OOM_KILLED,
            JobState.STARVED,
            JobState.TIME_EXCEEDED,
            JobState.ABORTED_BY_QUEUE,
        }
    ],
)
def test_active_records_block_without_writes(migrated, tmp_path, state):
    old, new, record = migrated
    raw = json.loads(record.read_bytes())
    raw["state"] = state.value
    record.write_text(json.dumps(raw))
    original = record.read_bytes()
    plan = alias_plan(migrated)
    assert any("active" in x for x in plan["blockers"])
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert record.read_bytes() == original
    assert not (tmp_path / "receipt").exists()


def test_missing_old_root_is_unproven_not_inferred_from_matching_job_id(migrated, tmp_path):
    old, new, record = migrated
    plan = repair.plan_repair(new, old, new)
    assert any("unproven" in x for x in plan["blockers"])
    assert plan["records"][0]["fields"]["cwd"]["artifact"]["kind"] == "directory"
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("cwd", "jobs/otherjob"),
        ("workdir", "workdirs/../outside"),
        ("archive_path", "archive/otherjob.tar.bz2"),
        ("cwd", "jobs/job123/nested"),
    ],
)
def test_mapping_refuses_wrong_job_and_noncanonical_paths(migrated, field, value):
    old, _, record = migrated
    raw = json.loads(record.read_bytes())
    raw[field] = str(old) + "/" + value
    record.write_text(json.dumps(raw))
    assert alias_plan(migrated)["blockers"]


@pytest.mark.parametrize(
    "change", ["record", "artifact", "alias", "queue-symlink", "artifact-symlink"]
)
def test_stale_or_redirected_plan_cannot_apply(migrated, tmp_path, change):
    old, new, record = migrated
    plan = alias_plan(migrated)
    if change == "record":
        record.write_bytes(record.read_bytes() + b" ")
    elif change == "artifact":
        (new / "jobs/job123/result.txt").write_bytes(b"different bytes")
    elif change == "alias":
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        old.unlink()
        old.symlink_to(elsewhere)
    elif change == "queue-symlink":
        record.parent.rename(tmp_path / "other-queue")
        (new / "queue").symlink_to(tmp_path / "other-queue", target_is_directory=True)
    else:
        (new / "jobs/job123").rename(tmp_path / "other-artifact")
        (new / "jobs/job123").symlink_to(tmp_path / "other-artifact", target_is_directory=True)
    with pytest.raises((repair.RepairError, OSError)):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert not (tmp_path / "receipt").exists()


def test_archived_record_with_missing_workspace_repairs_archive_and_raw_fields(migrated, tmp_path):
    old, new, record = migrated
    (new / "archive").mkdir()
    archive = new / "archive/job123.tar.bz2"
    with tarfile.open(archive, "w:bz2") as stream:
        stream.add(new / "jobs/job123", arcname="job123")
    (new / "jobs/job123/result.txt").unlink()
    (new / "jobs/job123").rmdir()
    raw = json.loads(record.read_bytes())
    raw.update(archived_at="2026-01-01T00:00:00Z", archive_path=str(old / "archive/job123.tar.bz2"))
    record.write_text(json.dumps(raw))
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    plan = alias_plan(migrated)
    assert not plan["blockers"]
    repair.apply_repair(plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt")
    old.unlink()
    out = fetch.fetch_local("job123", tmp_path / "fetched")
    assert (out / "result.txt").read_bytes() == b"verified current output\n"
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == digest


def test_multi_user_duplicate_records_are_both_preserved(migrated, tmp_path):
    old, new, record = migrated
    user = new / "users" / str(os.geteuid())
    (user / "queue").mkdir(parents=True)
    duplicate = user / "queue/job123.json"
    duplicate.write_bytes(record.read_bytes())
    plan = alias_plan(migrated)
    assert len(plan["records"]) == 2 and not plan["blockers"]
    result = repair.apply_repair(
        plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
    )
    assert result["records_changed"] == 2
    assert json.loads(record.read_bytes()) == json.loads(duplicate.read_bytes())
    assert len(list((tmp_path / "receipt").glob("*.original.json"))) == 2


def test_cli_defaults_to_json_dry_run_and_requires_matching_digest(migrated, tmp_path):
    old, new, record = migrated
    old.symlink_to(new)
    args = [
        "admin",
        "repair-state-paths",
        "--state-root",
        str(new),
        "--from-root",
        str(old),
        "--to-root",
        str(new),
    ]
    result = CliRunner().invoke(main, args)
    assert result.exit_code == 0, result.output
    plan = json.loads(result.output)
    original = record.read_bytes()
    denied = CliRunner().invoke(
        main, args + ["--apply-plan", "0" * 64, "--receipt-dir", str(tmp_path / "denied")]
    )
    assert denied.exit_code != 0 and record.read_bytes() == original
    applied = CliRunner().invoke(
        main, args + ["--apply-plan", plan["plan_id"], "--receipt-dir", str(tmp_path / "receipt")]
    )
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["records_changed"] == 1


@pytest.mark.parametrize("kind", ["duplicate", "nonfinite", "id-mismatch", "symlink", "hardlink"])
def test_unsafe_record_is_never_followed_or_rewritten(migrated, tmp_path, kind):
    _, _, record = migrated
    if kind == "duplicate":
        record.write_bytes(record.read_bytes().rstrip()[:-1] + b', "id": "job123"}')
    elif kind == "nonfinite":
        record.write_bytes(record.read_bytes().rstrip()[:-1] + b', "future": NaN}')
    elif kind == "id-mismatch":
        record.write_text(record.read_text().replace('"id": "job123"', '"id": "other"'))
    elif kind == "symlink":
        record.rename(tmp_path / "outside.json")
        record.symlink_to(tmp_path / "outside.json")
    else:
        os.link(record, tmp_path / "outside.json")
    plan = alias_plan(migrated)
    assert plan["blockers"]
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )


def test_unknown_selection_and_lookalike_prefix_do_not_select_another_record(migrated):
    old, new, record = migrated
    alias_plan(migrated)
    plan = repair.plan_repair(new, old, new, jobs=("absent123",))
    assert plan["records"] == [] and "not found" in plan["blockers"][0]
    raw = json.loads(record.read_bytes())
    raw["cwd"] = str(old) + "-other/jobs/job123"
    raw["workdir"] = None
    record.write_text(json.dumps(raw))
    plan = repair.plan_repair(new, old, new)
    assert plan["records"] == [] and plan["blockers"] == []


def test_lock_symlink_does_not_grant_write_authority(migrated, tmp_path):
    _, _, record = migrated
    plan = alias_plan(migrated)
    original = record.read_bytes()
    victim = tmp_path / "victim"
    victim.write_bytes(b"untouched")
    paths.spec_lock_path(record).symlink_to(victim)
    with pytest.raises(OSError):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert record.read_bytes() == original and victim.read_bytes() == b"untouched"
    assert not (tmp_path / "receipt/result.json").exists()


def test_fault_before_replace_preserves_original_and_durable_intent(
    migrated, tmp_path, monkeypatch
):
    _, _, record = migrated
    plan = alias_plan(migrated)
    original = record.read_bytes()

    def fail_replace(*args, **kwargs):
        raise OSError("injected replacement failure")

    monkeypatch.setattr(repair.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert record.read_bytes() == original
    assert next((tmp_path / "receipt").glob("*.original.json")).read_bytes() == original
    assert (tmp_path / "receipt/000001.intent.json").is_file()
    assert not (tmp_path / "receipt/000001.applied.json").exists()
    assert not (tmp_path / "receipt/result.json").exists()


def test_queue_redirect_after_backup_cannot_rewrite_external_records(
    migrated, tmp_path, monkeypatch
):
    _, new, record = migrated
    plan = alias_plan(migrated)
    original = record.read_bytes()
    write = repair._write_new
    outside = tmp_path / "outside-queue"

    def redirect_after_backup(fd, name, data, **kwargs):
        write(fd, name, data, **kwargs)
        if name.endswith(".original.json"):
            (new / "queue").rename(outside)
            (new / "queue").symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(repair, "_write_new", redirect_after_backup)
    with pytest.raises(OSError):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert (outside / "job123.json").read_bytes() == original
    assert not (tmp_path / "receipt/result.json").exists()


def test_uncoordinated_record_replacement_before_commit_is_detected(
    migrated, tmp_path, monkeypatch
):
    _, _, record = migrated
    plan = alias_plan(migrated)
    original = record.read_bytes()
    write = repair._write_new
    changed = json.loads(original)
    changed["state"] = "pending"
    raced = json.dumps(changed).encode()

    def replace_after_intent(fd, name, data, **kwargs):
        write(fd, name, data, **kwargs)
        if name.endswith(".intent.json"):
            record.write_bytes(raced)

    monkeypatch.setattr(repair, "_write_new", replace_after_intent)
    with pytest.raises(repair.RepairError, match="before atomic replacement"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert record.read_bytes() == raced
    assert next((tmp_path / "receipt").glob("*.original.json")).read_bytes() == original
    assert not (tmp_path / "receipt/result.json").exists()


def test_symlinked_receipt_parent_and_embedded_receipt_are_refused(migrated, tmp_path):
    _, new, record = migrated
    plan = alias_plan(migrated)
    original = record.read_bytes()
    (tmp_path / "receipt-parent").symlink_to(tmp_path)
    for receipt in (new / "receipt", tmp_path / "receipt-parent/receipt"):
        with pytest.raises((repair.RepairError, OSError)):
            repair.apply_repair(plan, expected_plan_id=plan["plan_id"], receipt_dir=receipt)
    assert record.read_bytes() == original


def test_internal_artifact_symlink_is_hashed_without_following(migrated, tmp_path):
    _, new, _ = migrated
    outside = tmp_path / "not-part-of-job"
    outside.write_bytes(b"unreadable target")
    link = new / "jobs/job123/external"
    link.symlink_to(outside)
    plan = alias_plan(migrated)
    outside.write_bytes(b"changed outside the artifact")
    assert plan == repair.plan_repair(new, Path(plan["old_root"]), new)
    assert not plan["blockers"]


def test_duplicate_job_with_different_artifact_binding_is_ambiguous(migrated):
    old, new, record = migrated
    user = new / "users" / str(os.geteuid())
    (user / "queue").mkdir(parents=True)
    (user / "jobs/job123").mkdir(parents=True)
    raw = json.loads(record.read_bytes())
    raw["cwd"] = str(old / "users" / str(os.geteuid()) / "jobs/job123")
    (user / "queue/job123.json").write_text(json.dumps(raw))
    plan = alias_plan(migrated)
    assert any("ambiguous" in blocker for blocker in plan["blockers"])


def test_partial_apply_preserves_all_originals_and_can_replan_remaining(
    migrated, tmp_path, monkeypatch
):
    old, new, record = migrated
    original = record.read_bytes()
    second = json.loads(original)
    second["id"] = "job456"
    second["cwd"] = str(old / "jobs/job456")
    second["workdir"] = None
    other = record.parent / "job456.json"
    other.write_text(json.dumps(second))
    (new / "jobs/job456").mkdir()
    (new / "jobs/job456/result").write_text("second")
    second_original = other.read_bytes()
    plan = alias_plan(migrated)
    replace = repair.os.replace

    def fail_second(src, dst, **kwargs):
        if dst == "job456.json":
            raise OSError("injected second-record failure")
        return replace(src, dst, **kwargs)

    monkeypatch.setattr(repair.os, "replace", fail_second)
    with pytest.raises(OSError, match="second-record"):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert json.loads(record.read_bytes())["cwd"] == str(new / "jobs/job123")
    assert other.read_bytes() == second_original
    preserved = [p.read_bytes() for p in (tmp_path / "receipt").glob("*.original.json")]
    assert original in preserved and second_original in preserved
    assert (tmp_path / "receipt/000001.applied.json").exists()
    assert (tmp_path / "receipt/000002.intent.json").exists()
    assert not (tmp_path / "receipt/result.json").exists()
    monkeypatch.setattr(repair.os, "replace", replace)
    remaining = repair.plan_repair(new, old, new)
    assert [r["jobid"] for r in remaining["records"]] == ["job456"]
    repair.apply_repair(
        remaining, expected_plan_id=remaining["plan_id"], receipt_dir=tmp_path / "second-receipt"
    )
    assert json.loads(other.read_bytes())["cwd"] == str(new / "jobs/job456")


def test_repair_preserves_record_mode_even_with_tighter_umask(migrated, tmp_path):
    _, _, record = migrated
    record.chmod(0o640)
    before = record.stat()
    plan = alias_plan(migrated)
    original_mask = os.umask(0o077)
    try:
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    finally:
        os.umask(original_mask)
    after = record.stat()
    assert (after.st_mode, after.st_uid, after.st_gid) == (
        before.st_mode,
        before.st_uid,
        before.st_gid,
    )


@pytest.mark.parametrize("field", ["cwd", "archive_path"])
def test_fetch_source_must_have_expected_artifact_type(migrated, field):
    old, new, record = migrated
    if field == "cwd":
        (new / "jobs/job123/result.txt").unlink()
        (new / "jobs/job123").rmdir()
        (new / "jobs/job123").write_bytes(b"not a directory")
    else:
        (new / "archive/job123.tar.bz2").mkdir(parents=True)
        raw = json.loads(record.read_bytes())
        raw.update(
            archived_at="2026-01-01T00:00:00Z", archive_path=str(old / "archive/job123.tar.bz2")
        )
        record.write_text(json.dumps(raw))
    plan = alias_plan(migrated)
    assert any("wrong artifact type" in x for x in plan["blockers"])


def test_busy_spec_lock_refuses_write_and_preserves_backup(migrated, tmp_path):
    _, _, record = migrated
    plan = alias_plan(migrated)
    before = record.read_bytes()
    with paths.spec_lock(record), pytest.raises(BlockingIOError):
        repair.apply_repair(
            plan, expected_plan_id=plan["plan_id"], receipt_dir=tmp_path / "receipt"
        )
    assert record.read_bytes() == before
    assert next((tmp_path / "receipt").glob("*.original.json")).read_bytes() == before


def test_directory_swap_between_stat_and_open_is_rejected(migrated, tmp_path, monkeypatch):
    _, new, _ = migrated
    (new / "jobs/job123/nested").mkdir()
    (new / "jobs/job123/nested/data").write_bytes(b"original")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"external")
    open_fd = repair.os.open
    swapped = False

    def swap_before_open(name, flags, *args, **kwargs):
        nonlocal swapped
        if name == "nested" and not swapped and kwargs.get("dir_fd") is not None:
            swapped = True
            (new / "jobs/job123/nested").rename(tmp_path / "preserved-nested")
            (new / "jobs/job123/nested").symlink_to(outside, target_is_directory=True)
        return open_fd(name, flags, *args, **kwargs)

    monkeypatch.setattr(repair.os, "open", swap_before_open)
    plan = alias_plan(migrated)
    assert swapped and plan["blockers"]
    assert (outside / "secret").read_bytes() == b"external"


def test_plan_rejects_active_job_before_artifact_inventory(migrated, monkeypatch):
    _, _, record = migrated
    raw = json.loads(record.read_bytes())
    raw["state"] = "running"
    record.write_text(json.dumps(raw))

    def forbidden(*args):
        raise AssertionError("must not inventory an active job")

    monkeypatch.setattr(repair, "_artifact", forbidden)
    assert any("active" in x for x in alias_plan(migrated)["blockers"])

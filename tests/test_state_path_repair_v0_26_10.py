"""Reconcile migration inventory with the strict current-referent apply contract.

The absent-old-root proposal's fixture is retained as a refusal control. A
job ID and existence census are useful diagnostics, not content proof.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import state_path_repair as repair
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def roots(tmp_path: Path):
    new = tmp_path / "current"
    old = tmp_path / "retired"
    (new / "queue").mkdir(parents=True)
    return old, new


def record(roots, jobid="job123", *, alias=True, state=JobState.COMPLETED, **changes):
    old, new = roots
    workspace = new / "jobs" / jobid
    workspace.mkdir(parents=True)
    (workspace / "result.txt").write_text("retained bytes\n")
    spec = JobSpec(id=jobid, command=["true"], cpus=1,
                   cwd=str(old / "jobs" / jobid), state=state)
    raw = json.loads(spec.to_json())
    raw.update(changes)
    path = new / "queue" / f"{jobid}.json"
    path.write_text(json.dumps(raw))
    if alias and not old.is_symlink():
        old.symlink_to(new, target_is_directory=True)
    return path


def plan(roots):
    old, new = roots
    return repair.plan_repair(new, old, new)


def test_absent_old_keeps_inventory_but_refuses_apply(roots, tmp_path):
    path = record(roots, alias=False)
    before = path.read_bytes()
    result = plan(roots)
    assert result["historical_content_verified"] is False
    assert result["summary"] == {
        "scanned_records": 1, "affected_records": 1, "candidate_records": 1,
        "applicable_records": 0, "blockers": 1, "residue_records": 0,
        "alias_dependent_links": 0,
    }
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(result, expected_plan_id=result["plan_id"],
                            receipt_dir=tmp_path / "receipt")
    assert path.read_bytes() == before
    assert not (tmp_path / "receipt").exists()
    assert not list(path.parent.glob("*.lock"))


def test_residue_reports_nested_json_pointers_and_preserves_bytes(roots, tmp_path):
    old, new = roots
    path = record(roots, command=["cat", str(old / "input.dat")],
                  future_extension={"a/b~c": [str(old / "external")]})
    before = json.loads(path.read_bytes())
    result = plan(roots)
    assert result["residue"] == [{"path": "queue/job123.json", "jobid": "job123",
                                   "pointers": ["/command/1", "/future_extension/a~1b~0c/0"]}]
    assert result["summary"]["residue_records"] == 1
    assert result["summary"]["applicable_records"] == 1
    repair.apply_repair(result, expected_plan_id=result["plan_id"],
                        receipt_dir=tmp_path / "receipt")
    after = json.loads(path.read_bytes())
    assert after["command"] == before["command"]
    assert after["future_extension"] == before["future_extension"]
    assert after["cwd"] == str(new / "jobs" / "job123")


def test_residue_on_an_already_repaired_record_is_still_visible(roots):
    old, new = roots
    record(roots, cwd=str(new / "jobs" / "job123"), command=[str(old / "tool")])
    result = plan(roots)
    assert result["records"] == []
    assert result["summary"]["affected_records"] == 0
    assert result["summary"]["residue_records"] == 1


@pytest.mark.parametrize("cwd_tail", ["jobs/prefix-job123", "jobs/job123-extra",
                                      "arbitrary/job123", "jobs/job123/nested"])
def test_substring_or_wrong_layout_is_not_job_identity(roots, cwd_tail):
    old, new = roots
    path = record(roots, cwd=str(old / cwd_tail))
    (new / cwd_tail).mkdir(parents=True, exist_ok=True)
    before = path.read_bytes()
    result = plan(roots)
    assert result["records"] == []
    assert result["blockers"]
    assert result["inventory"][0]["blocker"]
    assert result["summary"]["applicable_records"] == 0
    assert path.read_bytes() == before


def test_one_blocked_field_prevents_partial_record_rewrite(roots, tmp_path):
    old, _ = roots
    path = record(roots, workdir=str(old / "jobs" / "job123"))
    before = path.read_bytes()
    result = plan(roots)
    assert result["inventory"][0]["fields"] == ["cwd", "workdir"]
    assert result["records"] == []
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(result, expected_plan_id=result["plan_id"],
                            receipt_dir=tmp_path / "receipt")
    assert path.read_bytes() == before


def test_blocked_record_prevents_other_candidates_from_applying(roots, tmp_path):
    good = record(roots)
    bad = record(roots, jobid="job456", state=JobState.RUNNING)
    before = [p.read_bytes() for p in (good, bad)]
    result = plan(roots)
    assert result["summary"]["affected_records"] == 2
    assert result["summary"]["candidate_records"] == 1
    assert result["summary"]["applicable_records"] == 0
    with pytest.raises(repair.RepairError, match="blockers"):
        repair.apply_repair(result, expected_plan_id=result["plan_id"],
                            receipt_dir=tmp_path / "receipt")
    assert [p.read_bytes() for p in (good, bad)] == before


def test_artifact_change_invalidates_plan_with_unchanged_record(roots, tmp_path):
    _, new = roots
    path = record(roots)
    before = path.read_bytes()
    result = plan(roots)
    output = new / "jobs" / "job123" / "result.txt"
    metadata = output.stat()
    # Same length and restored mtime do not hide changed content.
    output.write_text("different data\n")
    os.utime(output, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    assert path.read_bytes() == before
    with pytest.raises(repair.RepairError, match="plan changed"):
        repair.apply_repair(result, expected_plan_id=result["plan_id"],
                            receipt_dir=tmp_path / "receipt")
    assert path.read_bytes() == before


def test_artifact_old_root_links_are_reported_without_rewriting(roots, tmp_path):
    old, new = roots
    record(roots)
    link = new / "jobs" / "job123" / "linked.txt"
    target = old / "jobs" / "job123" / "result.txt"
    link.symlink_to(target)
    result = plan(roots)
    assert result["alias_dependencies"] == [
        {"jobid": "job123", "path": "jobs/job123/linked.txt", "target": str(target)}
    ]
    assert result["summary"]["alias_dependent_links"] == 1
    repair.apply_repair(result, expected_plan_id=result["plan_id"],
                        receipt_dir=tmp_path / "receipt")
    assert link.read_bytes() == b"retained bytes\n"
    assert os.readlink(link) == str(target)
    old.unlink()  # Record repair alone is not evidence that alias retirement is safe.
    assert link.is_symlink() and not link.exists()


def test_cli_explicit_json_has_counts_and_keeps_missing_root_blocked(roots):
    old, new = roots
    path = record(roots, alias=False)
    before = path.read_bytes()
    result = CliRunner().invoke(main, ["admin", "repair-state-paths", "--state-root", str(new),
                                     "--from-root", str(old), "--to-root", str(new), "--json"])
    assert result.exit_code == 1
    assert json.loads(result.output)["summary"]["applicable_records"] == 0
    assert path.read_bytes() == before

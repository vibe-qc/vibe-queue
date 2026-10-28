"""A verified current generation can refresh admission without a fabricated marker."""

import json
import subprocess
from dataclasses import asdict
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_detached_operation_reconcile_115 import PROGRAM
from tests.test_scheduler_detached_build import SHA
from tests.test_scheduler_detached_build import state_dir as _state_dir_fixture
from tests.test_scheduler_runtime_observation_212 import setup as _setup_fixture
from vq import admin, submit, transport
from vq.cli import main

setup = _setup_fixture

state_dir = _state_dir_fixture


@pytest.fixture
def current(setup):
    admin.clear_admin_update_marker(setup[1])
    assert not admin.admin_update_marker_exists()
    return setup


def observe(current, **kwargs):
    return admin.reconcile_scheduler_runtime_observation(
        "host_f",
        PROGRAM,
        current[0],
        marker_id=None,
        expected_sha=SHA,
        expected_tag=None,
        current_without_marker=True,
        **kwargs,
    )


def test_current_observation_archives_old_receipt_and_repairs_resolver(
    current, state_dir, monkeypatch
):
    old = admin.SchedulerRuntimeUpdateResult(
        host="host_f",
        program=PROGRAM,
        mode="update",
        command="old-build",
        command_ssh="cluster-build",
        verify_command="",
        verify_ssh="host_f-login",
        expected_sha="b" * 40,
        command_rc=0,
        verify_rc=0,
    )
    # A genuine prior successful generation, distinct from the observed current.
    receipt = {
        **current[2],
        "source_sha": "b" * 40,
        "active_path": f"/opt/{PROGRAM}/releases/" + ("b" * 40) + "/source/.venv/bin/python",
    }
    admin._apply_scheduler_runtime_receipt(old, json.dumps(receipt))
    admin.record_scheduler_runtime_outcome(old)
    before = asdict(admin.load_scheduler_runtime_status()[f"host_f:{PROGRAM}"])
    current[2]["active_path"] = f"/opt/{PROGRAM}/releases/{SHA}/source/.venv/bin/python"

    def registry(*args, **kwargs):
        entries = [
            {
                "name": PROGRAM,
                "python": "/opt/current-python",
                "kind": "venv",
                "current_git_sha_full": SHA,
            }
        ]
        return subprocess.CompletedProcess([], 0, json.dumps(entries), "")

    monkeypatch.setattr(transport, "run_remote_vq", registry)
    with pytest.raises(ValueError, match="got " + ("b" * 40)):
        submit._scheduler_target_identity(current[0], "host_f", PROGRAM, SHA)
    job = state_dir / "state" / "retained-job.json"
    job.write_bytes(b'{"pin":"original"}')
    result = observe(current)
    evidence = json.loads(Path(result["evidence_path"]).read_text())
    assert evidence["prior_runtime_record"] == before
    assert evidence["prior_marker"] is None
    assert evidence["admission_mode"] == "current-without-marker"
    assert evidence["historical_build_outcome"] == "unchanged-unproven"
    assert not result["marker_cleared"] and not admin.admin_update_marker_exists()
    assert job.read_bytes() == b'{"pin":"original"}'
    pin = submit._scheduler_target_identity(current[0], "host_f", PROGRAM, SHA)
    assert pin.expected_git_sha == SHA
    assert pin.resolved_executable == current[2]["active_path"]
    with pytest.raises(ValueError, match="registered runtime"):
        submit._scheduler_target_identity(current[0], "host_f", PROGRAM, "b" * 40)


def test_dry_run_does_not_write_observation(current):
    assert observe(current, dry_run=True)["recorded"] is False
    assert not admin.load_scheduler_runtime_status()
    assert not (admin.paths.state_root() / "scheduler-runtime-observations").exists()


def test_no_marker_mode_requires_explicit_choice(current):
    with pytest.raises(admin.AdminError, match="marker ID or"):
        admin.reconcile_scheduler_runtime_observation(
            "host_f", PROGRAM, current[0], marker_id=None, expected_sha=SHA, expected_tag=None
        )
    assert not current[3]


def test_modes_are_exclusive(current):
    with pytest.raises(admin.AdminError, match="exclusive"):
        admin.reconcile_scheduler_runtime_observation(
            "host_f",
            PROGRAM,
            current[0],
            marker_id="f" * 32,
            expected_sha=SHA,
            expected_tag=None,
            current_without_marker=True,
        )
    assert not current[3]


def test_existing_marker_refused_even_if_dead(setup):
    with pytest.raises(admin.AdminError, match="update marker conflicts"):
        observe(setup)
    assert not setup[3] and admin.admin_update_marker_exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_sha", "f" * 40),
        ("healthy", False),
        ("quiescent", False),
        ("updater_pid", 123),
        ("active_path", "/opt/runtime/current"),
        ("health_detail", None),
        ("activation", "in-place"),
    ],
)
def test_bad_current_proof_cannot_change_admission(current, field, value):
    current[2][field] = value
    with pytest.raises(admin.AdminError, match="proof failed"):
        observe(current)
    assert not admin.load_scheduler_runtime_status()
    assert not admin.admin_update_marker_exists()


def test_marker_appearing_during_verifier_refuses(current, monkeypatch):
    original = admin.transport.run_remote_shell

    def verify(*args, **kwargs):
        admin._write_admin_update_marker_atomic(current[1])
        return original(*args, **kwargs)

    monkeypatch.setattr(admin.transport, "run_remote_shell", verify)
    with pytest.raises(admin.AdminError, match="appeared"):
        observe(current)
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def test_record_changed_during_verifier_is_not_overwritten(current, monkeypatch):
    original = admin.transport.run_remote_shell

    def verify(*args, **kwargs):
        other = admin.SchedulerRuntimeUpdateResult(
            host="host_f",
            program=PROGRAM,
            mode="update",
            command="other",
            command_ssh="cluster-build",
            verify_command="",
            verify_ssh="login",
            expected_sha="f" * 40,
            command_rc=7,
        )
        admin.record_scheduler_runtime_outcome(other)
        return original(*args, **kwargs)

    monkeypatch.setattr(admin.transport, "run_remote_shell", verify)
    with pytest.raises(admin.AdminError, match="record changed"):
        observe(current)
    assert admin.load_scheduler_runtime_status()[f"host_f:{PROGRAM}"].command_rc == 7


def test_cli_current_without_marker(current):
    result = CliRunner().invoke(
        main,
        [
            "admin",
            "reconcile-runtime",
            "host_f",
            PROGRAM,
            "--current-without-marker",
            "--expected-sha",
            SHA,
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["verified"] is True
    assert not admin.load_scheduler_runtime_status()

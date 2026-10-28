"""A new observation repairs admission without inventing build completion."""

import json
import subprocess
from dataclasses import asdict
from pathlib import Path

import pytest

from tests.test_detached_operation_reconcile_115 import PROGRAM, RECEIPT, _config, _stale_marker
from tests.test_scheduler_detached_build import SHA
from tests.test_scheduler_detached_build import state_dir as _state_dir_fixture
from vq import admin

state_dir = _state_dir_fixture


@pytest.fixture
def setup(state_dir, monkeypatch):
    cfg = _config(state_dir)
    marker = _stale_marker(monkeypatch)
    receipt = {
        **RECEIPT,
        "active_path": f"/opt/{PROGRAM}/releases/{SHA}/python",
        "health_detail": "source/native/dependencies verified",
    }
    calls = []

    def verify(host, *argv, **kwargs):
        assert argv[0] == "/site/bin/verify-runtime", "must never build or launch"
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps(receipt), "")

    monkeypatch.setattr(admin.transport, "run_remote_shell", verify)
    return cfg, marker, receipt, calls


def invoke(setup, **kwargs):
    cfg, marker, _, _ = setup
    return admin.reconcile_scheduler_runtime_observation(
        "host_f",
        PROGRAM,
        cfg,
        marker_id=marker.marker_id,
        expected_sha=SHA,
        expected_tag=None,
        **kwargs,
    )


def test_new_observation_preserves_history_and_job_bytes(setup, state_dir):
    cfg, marker, _, calls = setup
    old = admin.SchedulerRuntimeUpdateResult(
        host="host_f",
        program=PROGRAM,
        mode="update",
        command="old-build",
        command_ssh="cluster-build",
        verify_command="",
        verify_ssh="host_f-login",
        expected_sha="b" * 40,
        command_rc=9,
    )
    admin.record_scheduler_runtime_outcome(old)
    before = asdict(admin.load_scheduler_runtime_status()[f"host_f:{PROGRAM}"])
    job = state_dir / "state" / "retained-job.json"
    job.parent.mkdir(exist_ok=True)
    job.write_text('{"pin":"old-generation"}')
    outcome = invoke(setup)
    assert outcome["recorded"] and outcome["marker_cleared"]
    evidence = json.loads(Path(outcome["evidence_path"]).read_text())
    assert evidence["prior_marker"] == asdict(marker)
    assert evidence["prior_runtime_record"] == before
    assert evidence["historical_build_outcome"] == "unchanged-unproven"
    record = admin.load_scheduler_runtime_status()[f"host_f:{PROGRAM}"]
    assert record.last_ok_sha == SHA and record.observation["kind"] == "current-state"
    assert record.command_rc == 0  # actual new verification command, not lost build
    assert job.read_text() == '{"pin":"old-generation"}'
    assert len(calls) == 1 and not admin.admin_update_marker_exists()


def test_dry_run_writes_nothing(setup, state_dir):
    assert invoke(setup, dry_run=True)["recorded"] is False
    assert admin.admin_update_marker_exists()
    assert not admin.load_scheduler_runtime_status()
    assert not (admin.paths.state_root() / "scheduler-runtime-observations").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("program", "other"),
        ("source_sha", "b" * 40),
        ("tag", "v9"),
        ("healthy", False),
        ("activation", "in-place"),
        ("quiescent", False),
        ("updater_pid", 123),
        ("active_path", "/opt/runtime/current"),
        ("active_path", f"/opt/other/releases/{SHA}/python"),
        ("active_path", f"/opt/{PROGRAM}/releases/{SHA}/../python"),
        ("health_detail", None),
    ],
)
def test_wrong_or_unhealthy_identity_preserves_state(setup, field, value):
    setup[2][field] = value
    with pytest.raises(admin.AdminError, match="proof failed"):
        invoke(setup)
    assert admin.admin_update_marker_exists()
    assert not admin.load_scheduler_runtime_status()


def test_transport_uncertainty_is_not_terminal(setup, monkeypatch):
    def fail(*a, **k):
        raise admin.transport.RemoteError("unreachable")

    monkeypatch.setattr(admin.transport, "run_remote_shell", fail)
    with pytest.raises(admin.AdminError, match="unproven"):
        invoke(setup)
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def test_live_writer_refused_without_remote_call(setup, monkeypatch):
    monkeypatch.setattr(admin, "_pid_liveness", lambda pid: True)
    with pytest.raises(admin.AdminError, match="still live"):
        invoke(setup)
    assert not setup[3]


@pytest.mark.parametrize("change", ["paused", "transaction", "broader", "wrong-id"])
def test_wrong_or_unsafe_scope_refused(setup, change):
    marker = setup[1]
    original_id = marker.marker_id
    if change == "paused":
        marker.paused_jobids = ["retained"]
    if change == "transaction":
        marker.managed_transaction = {"id": "old"}
    if change == "broader":
        marker.envs.append("scheduler-runtime:host_f:other")
    if change == "wrong-id":
        marker.marker_id = "a" * 32
    admin._write_admin_update_marker_atomic(marker)
    marker.marker_id = original_id
    with pytest.raises(admin.AdminError):
        invoke(setup)
    assert not setup[3] and admin.admin_update_marker_exists()


def test_marker_changed_during_verification_is_preserved(setup, monkeypatch):
    original = admin.transport.run_remote_shell

    def verify(*a, **kw):
        marker = setup[1]
        replacement = admin.AdminUpdateMarker(**asdict(marker))
        replacement.failure_reason = "new evidence"
        admin._write_admin_update_marker_atomic(replacement)
        return original(*a, **kw)

    monkeypatch.setattr(admin.transport, "run_remote_shell", verify)
    with pytest.raises(admin.AdminError, match="marker changed"):
        invoke(setup)
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def test_archive_failure_cannot_clear_marker_or_change_admission(setup, monkeypatch):
    def fail(*a, **kw):
        raise OSError("disk full")

    monkeypatch.setattr(admin.paths, "atomic_write_text", fail)
    with pytest.raises(OSError, match="disk full"):
        invoke(setup)
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def test_cli_dry_run(setup):
    from click.testing import CliRunner

    from vq.cli import main

    r = CliRunner().invoke(
        main,
        [
            "admin",
            "reconcile-runtime",
            "host_f",
            PROGRAM,
            "--marker-id",
            setup[1].marker_id,
            "--expected-sha",
            SHA,
            "--dry-run",
            "--json",
        ],
    )
    assert r.exit_code == 0, r.output
    assert json.loads(r.output)["verified"] is True
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def artifact_receipt(setup):
    receipt = setup[2]
    receipt["active_path"] = "/opt/libexec/immutable-launcher-python"
    receipt["artifact_sha256"] = "c" * 64
    receipt["immutable_publication"] = {
        "schema": "vq-immutable-publication-v1",
        "authenticated": True,
        "immutable": True,
        "program": PROGRAM,
        "source_sha": SHA,
        "tag": None,
        "active_path": receipt["active_path"],
        "manifest_path": "/opt/artifacts/identity.json",
        "manifest_sha256": "d" * 64,
        "launcher_sha256": "e" * 64,
        "artifact_path": "/opt/artifacts/package.tar.gz",
        "artifact_sha256": "c" * 64,
    }
    return receipt


def test_authenticated_artifact_layout_observation_preserves_history(setup):
    receipt = artifact_receipt(setup)
    assert invoke(setup, dry_run=True)["verified"]
    assert admin.admin_update_marker_exists()
    outcome = invoke(setup)
    assert outcome["recorded"] and outcome["active_path"] == receipt["active_path"]
    evidence = json.loads(Path(outcome["evidence_path"]).read_text())
    assert evidence["verification"]["immutable_publication"] == receipt["immutable_publication"]
    assert evidence["prior_marker"]["marker_id"] == setup[1].marker_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "unknown"),
        ("authenticated", False),
        ("immutable", False),
        ("program", "other"),
        ("source_sha", "f" * 40),
        ("tag", "v9"),
        ("active_path", "/opt/other"),
        ("manifest_path", "/opt/current/identity.json"),
        ("artifact_path", "/opt/../package"),
        ("manifest_sha256", None),
        ("launcher_sha256", "not-a-digest"),
        ("artifact_sha256", "f" * 64),
    ],
)
def test_unbound_artifact_identity_refused(setup, field, value):
    artifact_receipt(setup)["immutable_publication"][field] = value
    with pytest.raises(admin.AdminError, match="proof failed"):
        invoke(setup)
    assert admin.admin_update_marker_exists() and not admin.load_scheduler_runtime_status()


def test_digest_and_sha_named_path_alone_do_not_authenticate(setup):
    receipt = artifact_receipt(setup)
    receipt.pop("immutable_publication")
    receipt["active_path"] = f"/opt/libexec/{PROGRAM}-{SHA}-python"
    with pytest.raises(admin.AdminError, match="proof failed"):
        invoke(setup)
    assert admin.admin_update_marker_exists()


def test_store_path_does_not_hide_invalid_explicit_attestation(setup):
    setup[2]["immutable_publication"] = {"schema": "unknown"}
    with pytest.raises(admin.AdminError, match="invalid authenticated"):
        invoke(setup)
    assert admin.admin_update_marker_exists()

"""Reconcile a rollout operation whose driver died during a detached build (#115).

The crashed state is built from the real pieces: an activated outer
operation, its create-once detached binding, and the embedded helper really
launching and recording the build in a local scratch directory. The driver
then "dies": the lease is released with no result, so the operation is
outcome-unknown, exactly what blocked every later rollout.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_scheduler_detached_build import (
    SHA,
    _activated_outer_operation,
    _canonical,
    state_dir,  # noqa: F401  (fixture)
)
from vq import admin, config, fleet_operation
from vq.cli import main

PROGRAM = "vibeqc-release"
MARKER_ENV = f"scheduler-runtime:host_f:{PROGRAM}"
BUILD = [sys.executable, "-c", "print('built')", "--program", PROGRAM, "--expected-sha", SHA]
RECEIPT = {
    "program": PROGRAM, "source_sha": SHA, "tag": None, "healthy": True,
    "activation": "atomic", "active_path": "/opt/runtime/current", "quiescent": True,
    "updater_pid": None,
}


def _config(state_dir: Path) -> config.Config:  # noqa: F811
    (state_dir / "cfg" / "config.toml").write_text("\n".join([
        'default_host = "localhost"',
        "[hosts.localhost]",
        'ssh = "localhost"',
        "[hosts.host_f]",
        'ssh = "host_f-login"',
        'scheduler = "pbs"',
        'scheduler_dialect = "torque"',
        'scratch_root = "/home/USER"',
        'scheduler_driver = "localhost"',
        'fleet_role = "managed"',
        f"[hosts.host_f.scheduler_runtime_deployments.{PROGRAM}]",
        'update_command = "/site/bin/deploy-runtime"',
        'verify_command = "/site/bin/verify-runtime"',
        'update_host = "cluster-build"',
        "detached_build = true",
        f"[programs.{PROGRAM}]",
        'kind = "binary"',
        'binary = "/opt/vibeqc/bin/vibeqc"',
        "",
    ]))
    return config.load_config()


def _helper(action: str, payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, action],
        input=_canonical(payload).decode() + "\n",
        capture_output=True, text=True, timeout=30,
    )


def _crashed_operation(state_dir: Path, monkeypatch, command=BUILD, identity=None):  # noqa: F811
    """An outcome-unknown operation whose bound build really ran to the end."""
    scratch = state_dir / "remote-scratch"
    scratch.mkdir(mode=0o700)
    identity = identity or _identity()
    with _activated_outer_operation(state_dir, monkeypatch, identity=identity) as (
        handle, context,
    ):
        binding, created, request = admin._build_detached_binding_request(
            context,
            host_cfg=config.HostConfig(ssh="cluster-build"),
            argv=list(command),
            scratch_root=str(scratch),
            program=PROGRAM,
            timeout=30.0,
            mode=f"runtime {PROGRAM} update",
        )
        assert created
        launch = _helper("launch", request)
        assert launch.returncode == 0, launch.stderr
        deadline = time.monotonic() + 30
        while True:
            observed = json.loads(_helper("observe", {
                "schema": admin._DETACHED_QUERY_SCHEMA, "request": request,
                "offset": 0, "max_bytes": admin._DETACHED_OBSERVATION_CHUNK,
            }).stdout)
            if observed["result"] is not None and not observed["lease_busy"]:
                break
            assert time.monotonic() < deadline, "the recorder never finished"
            time.sleep(0.02)
    for name in (fleet_operation.ENV_OPERATION_ID,
                 fleet_operation.ENV_OPERATION_REQUEST_SHA256,
                 fleet_operation.ENV_OPERATION_NONCE):
        monkeypatch.delenv(name)
    assert fleet_operation.observe_operation(handle.operation_id).state == "outcome-unknown"
    return handle, binding


def _identity() -> fleet_operation.OperationIdentity:
    from tests.test_scheduler_detached_build import _outer_identity

    return _outer_identity(host="host_f", program=PROGRAM)


class BuildHost:
    """The build host runs the real helper; the login host answers verify."""

    def __init__(self, receipt=None, verify_rc=0):
        self.receipt = RECEIPT if receipt is None else receipt
        self.verify_rc = verify_rc
        self.calls: list[str] = []

    def __call__(self, host_cfg, *argv, **kwargs):
        if argv[0] == "python3":
            assert host_cfg.ssh == "cluster-build"
            self.calls.append(argv[3])
            assert argv[3] in {"retained", "observe"}, "a reconcile must never launch"
            return _helper(argv[3], json.loads(kwargs["stdin_data"]))
        assert host_cfg.ssh == "host_f-login"
        assert list(argv) == ["/site/bin/verify-runtime", "--program", PROGRAM,
                              "--expected-sha", SHA]
        self.calls.append("verify")
        return subprocess.CompletedProcess(list(argv), self.verify_rc,
                                           stdout=json.dumps(self.receipt), stderr="")


@pytest.fixture
def build_host(monkeypatch):
    host = BuildHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", host)
    return host


def _stale_marker(monkeypatch, *, alive=False):
    marker = admin.acquire_admin_update_marker([MARKER_ENV], "host_f")
    admin._admin_update_marker_owner.__dict__.clear()
    real = admin._pid_liveness
    monkeypatch.setattr(admin, "_pid_liveness",
                        lambda pid: alive if pid == marker.pid else real(pid))
    return marker


def test_a_proven_success_is_reconciled_without_replay(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    handle, binding = _crashed_operation(state_dir, monkeypatch)
    _stale_marker(monkeypatch)

    outcome = admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)

    assert outcome.recorded and outcome.marker_cleared and outcome.update.success
    assert build_host.calls == ["retained", "observe", "verify"]
    assert outcome.update.command_output == "built\n"
    record = admin.load_scheduler_runtime_status()[f"host_f:{PROGRAM}"]
    assert (record.last_success, record.last_ok_sha) == (True, SHA)
    assert not admin.admin_update_marker_exists()

    observed = fleet_operation.observe_operation(handle.operation_id)
    assert observed.state == "completed"
    result = observed.result
    assert (result["status"], result["executed"], result["returncode"], result["child"]) == (
        "success", True, 0, None)
    evidence = json.loads((handle.directory / "reconciliation.json").read_text())["evidence"]
    assert evidence["remote_run_id"] == binding.run_id
    assert evidence["remote_result"]["returncode"] == 0
    assert evidence["verification"]["source_sha"] == SHA
    # What the rollout's harvest reads back.
    fleet_operation.read_output_tail(handle.operation_id)

    again = admin.reconcile_detached_scheduler_operation
    with pytest.raises(admin.AdminError, match="is completed"):
        again(handle.operation_id, cfg)


def test_dry_run_checks_everything_and_writes_nothing(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    _stale_marker(monkeypatch)

    outcome = admin.reconcile_detached_scheduler_operation(
        handle.operation_id, cfg, dry_run=True)

    assert outcome.update.success and not outcome.recorded and not outcome.marker_cleared
    assert build_host.calls == ["retained", "observe", "verify"]
    assert fleet_operation.observe_operation(handle.operation_id).state == "outcome-unknown"
    assert admin.admin_update_marker_exists()
    assert f"host_f:{PROGRAM}" not in admin.load_scheduler_runtime_status()


def test_a_failed_remote_build_stays_blocked(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    failing = [sys.executable, "-c", "import sys; sys.exit(3)", "--program", PROGRAM,
               "--expected-sha", SHA]
    handle, _ = _crashed_operation(state_dir, monkeypatch, command=failing)

    with pytest.raises(admin.AdminError, match="rc=3"):
        admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    assert "verify" not in build_host.calls
    assert fleet_operation.observe_operation(handle.operation_id).state == "outcome-unknown"


def test_a_failed_verification_records_nothing(state_dir, monkeypatch):  # noqa: F811
    cfg = _config(state_dir)
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    host = BuildHost(receipt={**RECEIPT, "healthy": False})
    monkeypatch.setattr(admin.transport, "run_remote_shell", host)

    outcome = admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)

    assert not outcome.update.success and not outcome.recorded
    assert "receipt did not report healthy=true" in outcome.update.work_errors
    assert fleet_operation.observe_operation(handle.operation_id).state == "outcome-unknown"
    assert f"host_f:{PROGRAM}" not in admin.load_scheduler_runtime_status()


def test_a_live_update_for_the_runtime_is_not_reconciled(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    _stale_marker(monkeypatch, alive=True)

    with pytest.raises(admin.AdminError, match="still running"):
        admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    assert build_host.calls == []


def test_an_operation_without_a_binding_stays_blocked(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    with _activated_outer_operation(state_dir, monkeypatch, identity=_identity()) as (
        handle, _,
    ):
        pass

    with pytest.raises(admin.AdminError, match="no detached build binding"):
        admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    assert build_host.calls == []


def test_a_retained_request_that_differs_from_the_binding_is_refused(
    state_dir, monkeypatch,  # noqa: F811
):
    cfg = _config(state_dir)
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    host = BuildHost()

    def tampered(host_cfg, *argv, **kwargs):
        proc = host(host_cfg, *argv, **kwargs)
        if argv[0] == "python3" and argv[3] == "retained":
            payload = json.loads(proc.stdout)
            payload["request"]["request_identity"]["argv"][2] = "print('other')"
            proc = subprocess.CompletedProcess(proc.args, 0, stdout=json.dumps(payload))
        return proc

    monkeypatch.setattr(admin.transport, "run_remote_shell", tampered)
    with pytest.raises(admin.AdminError, match="does not match this invocation"):
        admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    assert host.calls == ["retained"]


def test_the_helper_refuses_a_binding_it_does_not_hold(state_dir, monkeypatch):  # noqa: F811
    handle, binding = _crashed_operation(state_dir, monkeypatch)
    ok = _helper("retained", {"schema": admin._DETACHED_RETAINED_QUERY_SCHEMA,
                              "binding": binding.as_dict()})
    assert ok.returncode == 0, ok.stderr
    other = {**binding.as_dict(), "command_sha256": "0" * 64}
    refused = _helper("retained", {"schema": admin._DETACHED_RETAINED_QUERY_SCHEMA,
                                   "binding": other})
    assert refused.returncode == 2 and "another binding" in refused.stderr


def test_a_child_less_result_needs_the_reconciliation_receipt(state_dir, monkeypatch):  # noqa: F811
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    nonce = "d" * 64
    fleet_operation._write_operation_receipt(handle, "result.json", fleet_operation._result_payload(
        handle, nonce=nonce, status="success", executed=True, returncode=0,
        retry_safe=False, child=None,
        output_sha256=fleet_operation.hashlib.sha256(b"").hexdigest(),
        output_stored_bytes=0, output_observed_bytes=0, output_truncated=False,
        duration_seconds=0.0,
    ))
    with pytest.raises(fleet_operation.OperationStateError, match="process identity"):
        fleet_operation.observe_operation(handle.operation_id)


def test_recording_twice_returns_the_same_result(state_dir, monkeypatch, build_host):  # noqa: F811
    cfg = _config(state_dir)
    handle, binding = _crashed_operation(state_dir, monkeypatch)
    admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    first = fleet_operation.observe_operation(handle.operation_id).result
    evidence = json.loads((handle.directory / "reconciliation.json").read_text())["evidence"]

    again = fleet_operation.record_reconciled_result(
        handle.operation_id, expected_request_sha256=handle.request_sha256,
        binding=binding, evidence=evidence,
    )
    assert again == first


def test_the_cli_reports_the_evidence(state_dir, monkeypatch, build_host):  # noqa: F811
    _config(state_dir)
    handle, _ = _crashed_operation(state_dir, monkeypatch)
    result = CliRunner().invoke(main, ["admin", "reconcile-operation", handle.operation_id,
                                       "--dry-run", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["verified"] is True and payload["recorded"] is False
    assert (payload["actual_sha"], payload["remote_returncode"]) == (SHA, 0)

    blocked = CliRunner().invoke(main, ["admin", "reconcile-operation", "f" * 64])
    assert blocked.exit_code == 1 and "cannot read operation" in blocked.output


def test_the_rollout_harvests_a_reconciled_operation(state_dir, monkeypatch, build_host):  # noqa: F811
    """The refusal in the issue, then the ordinary harvest after reconciling."""
    from vq import fleet_rollout

    cfg = _config(state_dir)
    digest = "a" * 64
    rollout = "v0.17.10-" + "b" * 12
    action = fleet_rollout.RolloutAction(
        id=f"scheduler-runtime:host_f:{PROGRAM}",
        phase="scheduler-runtime",
        host="host_f",
        program=PROGRAM,
        pin_name="vibeqc",
        target_sha=SHA,
        target_version="0.17.10",
        target_tag=None,
        argv=["admin", "update", PROGRAM, "host_f", "--expected-sha", SHA],
        decision="update",
        reason="test",
        before={},
    )
    identity = fleet_rollout._operation_identity(rollout, digest, action, attempt=1)
    handle, _ = _crashed_operation(state_dir, monkeypatch, identity=identity)
    observed = fleet_operation.observe_operation(handle.operation_id)
    fleet_rollout.save_run(fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={action.id: {
            "decision": "update", "reason": action.reason, "status": "running",
            "argv": action.argv, "target_sha": SHA, "target_version": "0.17.10",
            "target_tag": None,
            "operation_attempts": [fleet_rollout._attempt_ref(
                observed, identity, request_sha256=handle.request_sha256)],
        }},
    ))
    monkeypatch.setattr(fleet_rollout.fleet_operation, "launch_supervisor",
                        lambda *a, **k: pytest.fail("a reconciled operation is never replayed"))

    with pytest.raises(fleet_rollout.FleetRolloutError, match="outcome unknown"):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest, inspect_only=False)

    admin.reconcile_detached_scheduler_operation(handle.operation_id, cfg)
    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest, inspect_only=False)

    record = fleet_rollout.load_run(rollout).actions[action.id]
    attempt = record["operation_attempts"][0]
    assert (attempt["state"], attempt["harvested"]) == ("completed", True)
    assert (record["status"], record["rc"]) == ("success", 0)

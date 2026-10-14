"""Operator reconciliation must recover one allocation without replay or repinning."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime

import pytest
from click.testing import CliRunner

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _unconfirmed_scheduler_spec,
    _write_multi_user_unconfirmed_scheduler_spec,
)
from tests.test_daemon_scheduler import (
    daemon as scheduler_daemon,  # noqa: F401 — shared daemon fixture
)
from vq import audit, config, driver_identity, rpc
from vq import daemon as daemon_mod
from vq import scheduler_recovery as recovery
from vq.cli import main as cli
from vq.scheduler_dialect import SchedulerPhase
from vq.scheduler_dispatch import SchedulerSubmitReceipt
from vq.spec import JobSpec, JobState, ProgramRuntimePin


@pytest.fixture
def ready(scheduler_daemon, monkeypatch):  # noqa: F811
    daemon = scheduler_daemon
    cfg = config.Config(hosts={"host_f": config.HostConfig(
        ssh="scheduler.example", scheduler="pbs", scheduler_dialect="torque",
        scratch_root="/remote", scheduler_driver="driver",
    ), "driver": config.HostConfig(ssh="driver.example")})
    monkeypatch.setattr(daemon_mod, "load_config", lambda: cfg)
    driver_identity.driver_id()
    spec = _unconfirmed_scheduler_spec(daemon, "j1", state=JobState.SUBMIT_OUTCOME_UNKNOWN)
    spec.scheduler_remote_workspace = "/remote/j1"
    spec.program_runtime_pin = ProgramRuntimePin(
        expected_git_sha="a" * 40, resolved_git_sha="a" * 40,
        resolved_runtime_root="/runtimes/immutable-a", scheduler_host="host_f",
    )
    spec.write(daemon._spec_path(spec.id))
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    original = daemon.scheduler_submit_recovery(spec.id)
    return daemon, spec, mock, original


def evidence(original, *, scheduler_id="123.server"):
    intent = original["binding"]["intent"]
    qtime = datetime.fromisoformat(intent["started_at"]).astimezone(UTC)
    record = (
        f"Job Id: {scheduler_id}\n"
        "    Job_Name = j1\n"
        "    Job_Owner = scientist@login\n"
        "    job_state = R\n"
        "    Output_Path = login:/remote/j1/.pbs-spool.out\n"
        "    Error_Path = login:/remote/j1/.pbs-spool.err\n"
        f"    qtime = {qtime.strftime('%a %b %d %H:%M:%S %Y')}\n"
    )
    return {
        "schema": "vq.scheduler-recovery-evidence/1", "decision": "accepted",
        "binding_digest": original["binding_digest"], "format": "torque-qstat-f",
        "record": record, "record_sha256": hashlib.sha256(record.encode()).hexdigest(),
        "source": "scheduler operator supplied qstat -f record (synthetic test)",
        "captured_at": datetime.now(UTC).isoformat(), "account": "scientist",
        "server": "server", "scheduler_utc_offset": "+00:00",
        "attestation": recovery.ATTESTATION,
    }


def apply(daemon, data, *, owner_uid=None):
    preview = daemon.scheduler_submit_recovery("j1", data, owner_uid=owner_uid)
    return daemon.scheduler_submit_recovery(
        "j1", data, owner_uid=owner_uid, apply=True,
        expected_plan_digest=preview["plan_digest"], operator_uid=os.geteuid(),
    )


def test_apply_is_durable_before_transition_and_restart_recovers_same_runtime(ready):
    daemon, spec, mock, original = ready
    untouched = _unconfirmed_scheduler_spec(daemon, "other", state=JobState.SUBMIT_OUTCOME_UNKNOWN)
    before_other = daemon._spec_path(untouched.id).read_bytes()
    data = evidence(original)
    preview = daemon.scheduler_submit_recovery("j1", data)
    assert not preview["applied"]
    assert recovery.read_record("j1", owner_uid=None, multi_user=False) is None
    first = apply(daemon, data)
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert apply(daemon, data) == {**first, "replayed": True}
    assert daemon._spec_path(untouched.id).read_bytes() == before_other
    # A crash here leaves only the durable intent: normal restart reattachment
    # consumes it, without an RPC thread mutating live scheduler tracking.
    daemon._reattach_or_interrupt_at_startup()
    recovered = JobSpec.read(daemon._spec_path("j1"))
    assert recovered.state == JobState.RUNNING
    assert recovered.scheduler_job_id == "123.server"
    assert recovered.program_runtime_pin == spec.program_runtime_pin
    assert recovered.scheduler_remote_workspace == spec.scheduler_remote_workspace
    assert mock.submitted == mock.cancelled == []
    assert JobSpec.read(daemon._spec_path(untouched.id)).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert apply(daemon, data)["replayed"]
    record = recovery.read_record("j1", owner_uid=None, multi_user=False)
    assert record["operator_auth"] == "owner-socket"
    assert record["evidence"] == data
    path = recovery._record_path("j1", owner_uid=None, multi_user=False)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("change,match", [
    ({"decision": "rejected"}, "absence is not rejection"),
    ({"binding_digest": "0" * 64}, "another original"),
    ({"record_sha256": "0" * 64}, "digest mismatch"),
    ({"account": "other"}, "account conflicts"),
    ({"server": "other"}, "server conflicts"),
    ({"attestation": "yes"}, "attestation"),
    ({"scheduler_utc_offset": "local"}, "UTC offset"),
    ({"captured_at": "2020-01-01T00:00:00+00:00"}, "window"),
])
def test_insufficient_and_conflicting_envelopes_do_not_write(ready, change, match):
    daemon, spec, mock, original = ready
    before = daemon._spec_path(spec.id).read_bytes()
    data = {**evidence(original), **change}
    with pytest.raises(ValueError, match=match):
        apply(daemon, data)
    assert daemon._spec_path(spec.id).read_bytes() == before
    assert recovery.read_record("j1", owner_uid=None, multi_user=False) is None
    assert mock.submitted == mock.cancelled == []


@pytest.mark.parametrize("old,new", [
    ("Job_Name = j1", "Job_Name = other"),
    ("/remote/j1/.pbs-spool.out", "/remote/other/.pbs-spool.out"),
    ("job_state = R", "job_state = unknown"),
    ("Job_Name = j1", "Job_Name = j1\n    Job_Name = j1"),
    ("Job Id: 123.server", "Job Id: 123.server\nJob Id: 124.server"),
    ("qtime = ", "unsupported_time = "),
])
def test_raw_records_are_checked_even_with_matching_digest(ready, old, new):
    daemon, _, _, original = ready
    data = evidence(original)
    data["record"] = data["record"].replace(old, new)
    data["record_sha256"] = hashlib.sha256(data["record"].encode()).hexdigest()
    with pytest.raises(ValueError):
        apply(daemon, data)


def test_plan_revalidation_and_one_durable_decision(ready):
    daemon, spec, _, original = ready
    data = evidence(original)
    preview = daemon.scheduler_submit_recovery("j1", data)
    with pytest.raises(ValueError, match="exact current"):
        daemon.scheduler_submit_recovery("j1", data, apply=True, expected_plan_digest="stale")
    spec.program_runtime_pin.resolved_runtime_root = "/runtimes/different"
    spec.write(daemon._spec_path("j1"))
    with pytest.raises(ValueError, match="another original"):
        daemon.scheduler_submit_recovery("j1", data, apply=True,
                                         expected_plan_digest=preview["plan_digest"])
    spec.program_runtime_pin.resolved_runtime_root = "/runtimes/immutable-a"
    spec.write(daemon._spec_path("j1"))
    apply(daemon, data)
    with pytest.raises(ValueError, match="conflicting operator"):
        apply(daemon, evidence(original, scheduler_id="124.server"))


@pytest.mark.parametrize("late", [False, True])
def test_other_acceptance_keeps_capacity_reserved_and_never_cancels(ready, late):
    daemon, _, mock, original = ready
    apply(daemon, evidence(original))
    if late:
        daemon._reattach_or_interrupt_at_startup()
        assert "j1" in daemon._scheduler_running
    mock.receipt = SchedulerSubmitReceipt("accepted", "999.server", 0)
    if late:
        handle = daemon._scheduler_running["j1"]
        assert daemon._imported_scheduler_evidence_conflicts(
            JobSpec.read(daemon._spec_path("j1")), handle,
        )
    else:
        daemon._reattach_or_interrupt_at_startup()
    current = JobSpec.read(daemon._spec_path("j1"))
    assert current.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert current.scheduler_state == "submit_evidence_conflict"
    assert "j1" not in daemon._scheduler_running
    assert mock.submitted == mock.cancelled == []
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.SUBMIT_OUTCOME_UNKNOWN


def test_multiuser_import_rejects_mutated_intent(ready, tmp_path, monkeypatch):
    daemon, _, _, _ = ready
    spec, spec_path, _ = _write_multi_user_unconfirmed_scheduler_spec(
        daemon, tmp_path, monkeypatch,
    )
    spec.state = JobState.SUBMIT_OUTCOME_UNKNOWN
    spec.scheduler_remote_workspace = "/remote/j1"
    spec.write(spec_path)
    daemon_mod._replace_scheduler_submit_binding_spec("1000", spec)
    original = daemon.scheduler_submit_recovery("j1", owner_uid="1000")
    spec.command = ["different-command"]
    spec.write(spec_path)
    with pytest.raises(ValueError, match="daemon-owned submit authority"):
        apply(daemon, evidence(original), owner_uid="1000")
    spec.command = ["true"]
    spec.write(spec_path)
    applied = apply(daemon, evidence(original), owner_uid="1000")
    assert applied["operator_auth"] == "admin-token"
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(spec_path).scheduler_job_id == "123.server"
    assert daemon_mod._read_scheduler_submit_binding("1000", "j1").scheduler_job_id == "123.server"


def test_rpc_requires_admin_auth_and_explicit_attestation(monkeypatch):
    calls = []
    server = rpc.RPCServer(multi_user=True)
    rpc.register_scheduler_submit_recovery_methods(server, lambda *a, **k: calls.append((a, k)))
    from vq import auth
    monkeypatch.setattr(auth, "verify_admin_token", lambda token: token == "test-token")
    handler = server._methods["set_scheduler_submit_recovery"]
    args = dict(jobid="j1", evidence={}, expected_plan_digest="digest")
    with pytest.raises(PermissionError, match="admin token"):
        handler(**args, attest_authoritative=True)
    with pytest.raises(PermissionError, match="attestation"):
        handler(**args, token="test-token")
    handler(**args, attest_authoritative=True, token="test-token", _peer_uid=42)
    assert calls[0][1]["operator_uid"] == 42
    summary = audit.summarise_args("set_scheduler_submit_recovery", {
        **args, "evidence": {"record": "private"}, "token": "secret",
    })
    assert "private" not in summary and "secret" not in summary


def test_cli_daemon_failure_has_no_file_fallback(ready, monkeypatch, tmp_path):
    _, _, _, original = ready
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps(evidence(original)))
    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    def unavailable(*args, **kwargs):
        raise ConnectionError("offline")
    monkeypatch.setattr(rpc, "call", unavailable)
    result = CliRunner().invoke(cli, ["admin", "reconcile-submit", "j1", "--evidence", str(path)])
    assert result.exit_code != 0 and "owning daemon reconciliation unavailable" in result.output
    assert recovery.read_record("j1", owner_uid=None, multi_user=False) is None


def test_transport_peer_identity_overrides_client_claim(ready, monkeypatch):
    import socket

    daemon, _, _, original = ready
    data = evidence(original)
    preview = daemon.scheduler_submit_recovery("j1", data)
    server = rpc.RPCServer(multi_user=False)
    rpc.register_scheduler_submit_recovery_methods(server, daemon.scheduler_submit_recovery)
    audit_rows = []
    monkeypatch.setattr(audit, "append_audit_line", lambda **row: audit_rows.append(row))
    method = "set_scheduler_submit_recovery"
    args = dict(jobid="j1", evidence=data, expected_plan_digest=preview["plan_digest"],
                attest_authoritative=True, _peer_uid=99999)
    left, right = socket.socketpair()
    try:
        server._respond(left, method, server._methods[method], args, peer_uid=42)
        result = json.loads(right.recv(65536))
    finally:
        left.close()
        right.close()
    assert result["ok"]
    assert recovery.read_record("j1", owner_uid=None, multi_user=False)["operator_uid"] == 42
    assert audit_rows[0]["uid"] == 42
    assert data["record"] not in audit_rows[0]["args_summary"]


def test_crash_after_journal_rename_is_recovered_idempotently(ready, monkeypatch):
    daemon, _, mock, original = ready
    data = evidence(original)
    original_fsync = os.fsync
    calls = 0
    def fail_directory_sync(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated crash after atomic rename")
        original_fsync(fd)
    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", fail_directory_sync)
        with pytest.raises(OSError, match="simulated crash"):
            apply(daemon, data)
    assert apply(daemon, data)["replayed"]
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_job_id == "123.server"
    assert mock.submitted == mock.cancelled == []
    directory = recovery._record_path("j1", owner_uid=None, multi_user=False).parent
    assert len(list(directory.iterdir())) == 1


def test_retained_evidence_does_not_recover_a_different_retry_attempt(ready):
    daemon, spec, _, original = ready
    apply(daemon, evidence(original))
    spec.retry_count = 1
    spec.write(daemon._spec_path("j1"))
    assert recovery.accepted_id(spec, daemon_mod.load_config(), owner_uid=None,
                                multi_user=False) is None
    next_original = daemon.scheduler_submit_recovery("j1")
    assert next_original["existing_plan_digest"] is None
    apply(daemon, evidence(next_original, scheduler_id="124.server"))
    first = recovery.read_record("j1", owner_uid=None, multi_user=False)
    assert first["scheduler_job_id"] == "123.server"
    assert recovery.read_record("j1", owner_uid=None, multi_user=False,
                                retry_count=1)["scheduler_job_id"] == "124.server"


def test_replaced_private_journal_refuses_recovery(ready):
    daemon, _, mock, original = ready
    apply(daemon, evidence(original))
    path = recovery._record_path("j1", owner_uid=None, multi_user=False)
    path.chmod(0o644)
    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == "submit_evidence_conflict"
    assert mock.submitted == mock.cancelled == []


def test_cli_preview_and_apply_use_same_owning_daemon_plan(ready, monkeypatch, tmp_path):
    daemon, _, mock, original = ready
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps(evidence(original)))
    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    server = rpc.RPCServer(multi_user=False)
    rpc.register_scheduler_submit_recovery_methods(server, daemon.scheduler_submit_recovery)
    calls = []
    def local_rpc(method, args, **kwargs):
        calls.append(method)
        return server._methods[method](**args)
    monkeypatch.setattr(rpc, "call", local_rpc)
    runner = CliRunner()
    args = ["admin", "reconcile-submit", "j1", "--evidence", str(path)]
    preview = runner.invoke(cli, args)
    assert preview.exit_code == 0, preview.output
    digest = json.loads(preview.output)["plan_digest"]
    applied = runner.invoke(cli, [*args, "--apply", "--plan-digest", digest,
                                  "--attest-authoritative"])
    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.output)["applied"]
    assert calls == ["get_scheduler_submit_recovery", "set_scheduler_submit_recovery"]
    assert mock.submitted == mock.cancelled == []

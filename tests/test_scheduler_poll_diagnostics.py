"""Long poll commands must not hide the reason the observation failed (#51)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from tests.test_scheduler_dispatch import (
    _QSTAT_TABLE,
    FakeRunner,
    make_dispatcher,
    make_slurm_dispatcher,
)
from vq import transport
from vq.config import HostConfig
from vq.daemon import _safe_scheduler_poll_diagnostic
from vq.scheduler_dialect import SchedulerPhase
from vq.scheduler_dispatch import (
    RemoteResult,
    SchedulerError,
    SchedulerHandle,
    SchedulerRemoteOutcomeUnknown,
    SshRemoteRunner,
)
from vq.spec import JobSpec, JobState


def _long_argv(command: str) -> list[str]:
    ids = [f"{number}.cluster.example" for number in range(300)]
    return [command, *ids] if command == "qstat" else [command, "--jobs", ",".join(ids)]


@pytest.mark.parametrize("command", ["qstat", "squeue", "sacct"])
def test_long_poll_keeps_ssh_failure_reason_and_transport_contract(monkeypatch, command):
    argv = _long_argv(command)
    seen = []

    def remote(host, *args, **kwargs):
        seen.append((host, args, kwargs))
        return subprocess.CompletedProcess(
            args, 255, "", "Connection timed out during banner exchange"
        )

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    host = HostConfig(ssh="cluster")
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(host).run(argv, check=False)
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "ssh transport failed" in diagnostic
    assert "exit 255" in diagnostic
    assert command in diagnostic
    assert "Connection timed out during banner exchange" in diagnostic
    assert len(diagnostic) <= 240
    assert seen[0][0] is host and seen[0][1] == tuple(argv)
    assert seen[0][2]["retry_transient"] == (2 if command == "qstat" else 0)
    assert seen[0][2]["check"] is False


@pytest.mark.parametrize("command", ["qstat", "squeue"])
def test_long_poll_keeps_timeout_category(monkeypatch, command):
    def remote(host, *args, **kwargs):
        try:
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        except subprocess.TimeoutExpired as exc:
            raise transport.RemoteOutcomeUnknown(
                "remote shell timed out: " + " ".join(args)
            ) from exc

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    with pytest.raises(SchedulerRemoteOutcomeUnknown) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv(command))
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "timeout" in diagnostic
    assert "outcome is unknown" in diagnostic
    assert command in diagnostic
    assert len(diagnostic) <= 240


def test_poll_diagnostic_redacts_secrets_and_paths_before_bounding(monkeypatch, tmp_path: Path):
    private_path = str(tmp_path / "private identity")
    stderr = (
        f'Permission denied opening "{private_path}"; token="two private words" '
        "password=HIDDEN_PASSWORD Bearer HIDDEN_BEARER\x00\n"
    )
    monkeypatch.setattr(
        transport,
        "run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(a, 255, "", stderr),
    )
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv("qstat"))
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "Permission denied" in diagnostic
    assert private_path not in diagnostic and str(tmp_path) not in diagnostic
    assert "private identity" not in diagnostic
    assert "two private words" not in diagnostic
    assert "HIDDEN_PASSWORD" not in diagnostic and "HIDDEN_BEARER" not in diagnostic
    assert "\n" not in diagnostic and "\x00" not in diagnostic
    assert len(diagnostic) <= 240


@pytest.mark.parametrize("detail", [False, True])
def test_scheduler_rejection_keeps_stderr_for_live_and_detail(detail):
    runner = FakeRunner(
        responder=lambda argv, stdin: RemoteResult(
            1,
            "",
            "Permission denied reading scheduler accounting"
            if detail
            else "Scheduler temporarily unavailable",
        )
    )
    dispatcher = make_slurm_dispatcher(runner)
    handles = [SchedulerHandle(str(n), "/ws/job") for n in range(300)]
    with pytest.raises(SchedulerError) as caught:
        (dispatcher.poll_detail if detail else dispatcher.poll)(handles)
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "exit 1" in diagnostic
    assert ("Permission denied" if detail else "temporarily unavailable") in diagnostic
    assert len(diagnostic) <= 240


def test_torque_missing_ids_still_preserve_live_rows_and_terminal_reconciliation():
    runner = FakeRunner(
        responder=lambda argv, stdin: RemoteResult(
            153, _QSTAT_TABLE, "qstat: Unknown Job Id 3.cluster"
        )
    )
    dispatcher = make_dispatcher(runner)
    phases = dispatcher.poll([SchedulerHandle(f"{n}.cluster", "/ws/job") for n in (1, 2, 3)])
    assert phases == {
        "1.cluster": SchedulerPhase.RUNNING,
        "2.cluster": SchedulerPhase.PENDING,
        "3.cluster": SchedulerPhase.FINISHED,
    }
    assert len(runner.calls) == 1


def test_empty_ssh_stderr_remains_explicit(monkeypatch):
    monkeypatch.setattr(
        transport, "run_remote_shell", lambda *a, **k: subprocess.CompletedProcess(a, 255, "", "")
    )
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv("qstat"))
    assert "(empty)" in _safe_scheduler_poll_diagnostic(caught.value)


@pytest.mark.parametrize("detail", [False, True])
def test_real_poll_error_survives_daemon_persistence_without_terminalizing(
    monkeypatch,
    request,
    detail,
):
    daemon_instance = request.getfixturevalue("daemon")
    monkeypatch.setattr(
        transport,
        "run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(a, 255, "", "Connection refused by gateway"),
    )
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv("sacct" if detail else "qstat"))
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        accounting_required_for_absent=detail,
        detail_error=caught.value if detail else None,
        poll_error=None if detail else caught.value,
    )
    _inject(daemon_instance, mock)
    spec = _submit_scheduler(daemon_instance, "j1")
    daemon_instance._start_scheduler_job(spec)
    daemon_instance._reconcile_scheduler()
    observed = JobSpec.read(daemon_instance._spec_path("j1"))
    assert observed.state is JobState.RUNNING
    assert observed.scheduler_state == "poll_failed"
    assert observed.exit_code is None and observed.finished_at is None
    assert "exit 255" in observed.scheduler_poll_last_error
    assert "Connection refused by gateway" in observed.scheduler_poll_last_error
    assert len(observed.scheduler_poll_last_error) <= 240
    assert mock.marker_queries == [] and mock.fetched == [] and mock.cancelled == []


@pytest.mark.parametrize("persist", [False, True])
def test_bounded_accounting_output_never_replays_command_arguments(monkeypatch, request, persist):
    marker = "REVIEW_PRIVATE_JOB_ID"
    argv = ["sacct", "--jobs", marker + "," + ",".join(str(n) for n in range(300))]
    captured = []

    def bounded_child(command, **kwargs):
        captured.append((command, kwargs))
        raise transport.SubprocessOutputLimitExceeded("stdout cap exceeded")

    # Keep actual run_remote_shell and its exception message. Replace the sole
    # subprocess boundary before any process or SSH connection can start.
    monkeypatch.setattr(transport, "run_owned_subprocess", bounded_child)
    with pytest.raises(SchedulerRemoteOutcomeUnknown) as caught:
        SshRemoteRunner(HostConfig(ssh="review.example")).run(argv, check=False)
    assert len(captured) == 1
    assert captured[0][1]["max_stderr_bytes"] == 4096
    assert captured[0][1]["timeout"] > 0
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    if persist:
        instance = request.getfixturevalue("daemon")
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            accounting_required_for_absent=True,
            detail_error=caught.value,
        )
        _inject(instance, mock)
        spec = _submit_scheduler(instance, "j1")
        instance._start_scheduler_job(spec)
        instance._reconcile_scheduler()
        observed = JobSpec.read(instance._spec_path("j1"))
        assert observed.state is JobState.RUNNING
        assert observed.scheduler_state == "poll_failed"
        assert observed.exit_code is None and observed.finished_at is None
        assert mock.marker_queries == [] and mock.fetched == [] and mock.cancelled == []
        diagnostic = observed.scheduler_poll_last_error
    assert "outcome is unknown" in diagnostic
    assert len(diagnostic) <= 240
    assert marker not in diagnostic, diagnostic
    assert "--jobs" not in diagnostic, diagnostic
    assert "output capture limit exceeded" in diagnostic


@pytest.mark.parametrize("error_class", [transport.RemoteError, transport.RemoteOutcomeUnknown])
def test_poll_does_not_treat_wrapper_stderr_delimiter_as_a_stream(monkeypatch, error_class):
    def remote(*args, **kwargs):
        raise error_class("wrapper sacct --jobs stderr: PRIVATE_ARGUMENT_VALUE")

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv("sacct"))
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "PRIVATE_ARGUMENT_VALUE" not in diagnostic
    assert "--jobs" not in diagnostic
    assert "stderr: (empty)" in diagnostic
    assert len(diagnostic) <= 240


def test_poll_preserves_structured_stderr_instead_of_wrapper_text(monkeypatch):
    def remote(*args, **kwargs):
        raise transport.RemoteCommandError(
            "wrapper --jobs PRIVATE_ARGUMENT_VALUE stderr: PRIVATE_ARGUMENT_VALUE",
            returncode=1,
            stderr="Permission denied; Bearer PRIVATE_CREDENTIAL_VALUE",
        )

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="cluster")).run(_long_argv("squeue"))
    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "Permission denied" in diagnostic and "exit 1" in diagnostic
    assert "PRIVATE_ARGUMENT_VALUE" not in diagnostic
    assert "PRIVATE_CREDENTIAL_VALUE" not in diagnostic
    assert "--jobs" not in diagnostic

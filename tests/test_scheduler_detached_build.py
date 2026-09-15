"""Detached builds on a fixed scheduler build host.

The foreground build lane ties a remote compile's life to one SSH
connection: the 2026-07-24 host_f deploy lost a 90-minute build to a dropped
VPN (ssh rc=255, heartbeats silent for 88 minutes, then a bare failure).
With ``detached_build = true`` the command runs under ``setsid`` with its
output, rc, and pid parked in a per-run directory; the driver observes via
fresh per-poll connections. These tests pin the protocol: a dropped poll is
tolerated, output streams into the run log exactly once, a wall timeout
kills the remote process group, and the runtime lane wires it end to end.
"""
from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import pytest
from pydantic import ValidationError

from vq import admin, config, fleet_operation, output, paths, transport
from vq.scheduler_dialect import SchedulerPhase

SHA = "c" * 40


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        lambda host_cfg, specs: {
            str(s.scheduler_job_id): SchedulerPhase.RUNNING for s in specs
        },
    )
    monkeypatch.setattr(admin, "DETACHED_BUILD_POLL_INTERVAL_SECONDS", 0.01)
    return tmp_path


# Budget for a loop or a join that is only waiting for work to finish. A
# liveness guard: it exists so a genuine hang fails the test instead of
# blocking the suite, and its value means nothing beyond "longer than this is
# certainly broken". Must be generous -- CI shares a loaded runner, and a
# 2-4 second ceiling on total wall time is a coin flip there (issue #5).
_LIVENESS_SECONDS = 30.0

# How long the early-EOF probe action sleeps after closing its output streams.
# The floor assertion below is derived from it: finishing sooner than this
# would mean the action was cut short when its stdout closed, which is the
# property that test exists to check.
_EOF_ACTION_SLEEP = 0.3


def _host_cfg() -> config.HostConfig:
    return config.HostConfig(ssh="cluster-build")


def _outer_identity(
    *,
    host: str = "host_f",
    program: str = "vibeqc-release",
    expected_sha: str = SHA,
) -> fleet_operation.OperationIdentity:
    return fleet_operation.OperationIdentity(
        rollout_id="v0.24.0-" + "a" * 12,
        report_digest_sha256="a" * 64,
        attempt=1,
        action_id=f"scheduler-runtime:{host}:{program}",
        phase="scheduler-runtime",
        host=host,
        program=program,
        pin_name="vibeqc",
        target_sha=expected_sha,
        target_version="0.24.0",
        target_tag=None,
        argv=("admin", "update", program, host, "--expected-sha", expected_sha),
    )


@contextmanager
def _activated_outer_operation(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    identity: fleet_operation.OperationIdentity | None = None,
) -> Iterator[tuple[
    fleet_operation.OperationHandle,
    fleet_operation.OperationExecutionContext,
]]:
    handle = fleet_operation.prepare_operation(
        identity or _outer_identity(),
        state_root=state_dir / "state",
    )
    (handle.directory / "output.log").write_bytes(b"")
    (handle.directory / "output.log").chmod(0o600)
    nonce = "d" * 64
    fleet_operation._write_operation_receipt(
        handle,
        "ready.json",
        fleet_operation._ready_payload(handle, nonce=nonce),
    )
    fleet_operation._write_operation_receipt(
        handle,
        "authorization.json",
        fleet_operation._authorization_payload(handle, nonce=nonce),
    )
    fleet_operation._write_operation_receipt(
        handle,
        "activation.json",
        fleet_operation._activation_payload(handle, nonce=nonce),
    )
    context = fleet_operation.OperationExecutionContext(
        operation_id=handle.operation_id,
        request_sha256=handle.request_sha256,
        nonce=nonce,
    )
    monkeypatch.setenv(fleet_operation.ENV_OPERATION_ID, context.operation_id)
    monkeypatch.setenv(
        fleet_operation.ENV_OPERATION_REQUEST_SHA256,
        context.request_sha256,
    )
    monkeypatch.setenv(fleet_operation.ENV_OPERATION_NONCE, context.nonce)
    lease_fd = os.open(handle.directory / "lease.lock", os.O_RDWR)
    try:
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield handle, context
    finally:
        fcntl.flock(lease_fd, fcntl.LOCK_UN)
        os.close(lease_fd)


def _canonical(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _bound_observation(
    request: dict[str, object],
    *,
    output_text: str = "compiled\n",
    returncode: int = 0,
    timed_out: bool = False,
    lease_busy: bool = False,
    binding_sha256: str | None = None,
    offset: int = 0,
) -> dict[str, object]:
    binding = request["binding"]
    assert isinstance(binding, dict)
    exact_binding_sha = hashlib.sha256(_canonical(binding)).hexdigest()
    receipt_sha = binding_sha256 or exact_binding_sha
    output_bytes = output_text.encode()
    output_chunk = output_bytes[
        offset : offset + admin._DETACHED_OBSERVATION_CHUNK
    ]
    pid = 4242
    return {
        "schema": admin._DETACHED_OBSERVATION_SCHEMA,
        "request": request,
        "lease": {
            "schema": admin._DETACHED_LEASE_SCHEMA,
            "binding_sha256": receipt_sha,
            "recorder_pid": pid,
            "acquired_at": "2026-08-10T12:00:00+00:00",
        },
        "activation": {
            "schema": admin._DETACHED_ACTIVATION_SCHEMA,
            "binding_sha256": receipt_sha,
            "recorder_pid": pid,
            "launch_intent": True,
            "activated_at": "2026-08-10T12:00:01+00:00",
        },
        "result": {
            "schema": admin._DETACHED_RESULT_SCHEMA,
            "binding_sha256": receipt_sha,
            "status": (
                "timed-out"
                if timed_out
                else ("success" if returncode == 0 else "failed")
            ),
            "returncode": returncode,
            "timed_out": timed_out,
            "completed_at": "2026-08-10T12:00:02+00:00",
            "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
            "output_stored_sha256": hashlib.sha256(output_bytes).hexdigest(),
            "output_observed_bytes": len(output_bytes),
            "output_stored_bytes": len(output_bytes),
            "output_truncated": False,
        },
        "lease_busy": lease_busy,
        "output_offset": offset,
        "output_next_offset": offset + len(output_chunk),
        "output_stored_bytes": len(output_bytes),
        "output_stored_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "output_base64": base64.b64encode(output_chunk).decode("ascii"),
    }


class _BoundBuildHost:
    def __init__(
        self,
        observations: list[object] | None = None,
        *,
        launch_returncode: int = 0,
    ) -> None:
        self.observations = list(observations or [])
        self.launch_returncode = launch_returncode
        self.launches = 0
        self.observes = 0
        self.requests: list[dict[str, object]] = []

    def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        assert host_cfg.ssh == "cluster-build"
        assert argv[:3] == (
            "python3",
            "-c",
            admin._DETACHED_REMOTE_HELPER_SOURCE,
        )
        action = argv[3]
        payload = json.loads(kwargs.get("stdin_data") or "")
        assert isinstance(payload, dict)
        request = payload if action == "launch" else payload.get("request")
        assert isinstance(request, dict)
        self.requests.append(request)
        if action == "launch":
            self.launches += 1
            binding = request["binding"]
            assert isinstance(binding, dict)
            persisted = fleet_operation.read_detached_scheduler_binding(
                str(binding["operation_id"])
            )
            assert persisted is not None
            return subprocess.CompletedProcess(
                args=list(argv),
                returncode=self.launch_returncode,
                stdout=(
                    json.dumps(
                        {
                            "schema": admin._DETACHED_LAUNCH_SCHEMA,
                            "status": "activated",
                            "remote_request_sha256": binding[
                                "remote_request_sha256"
                            ],
                        }
                    )
                    if self.launch_returncode == 0
                    else ""
                ),
                stderr="lost response" if self.launch_returncode else "",
            )
        if action == "observe":
            self.observes += 1
            step = self.observations.pop(0) if self.observations else None
            if isinstance(step, BaseException):
                raise step
            if isinstance(step, subprocess.CompletedProcess):
                return step
            payload = step or _bound_observation(request)
            return subprocess.CompletedProcess(
                args=list(argv),
                returncode=0,
                stdout=json.dumps(payload),
                stderr="",
            )
        raise AssertionError(f"unexpected helper action {action!r}")


def test_detached_build_requires_a_fixed_build_host() -> None:
    """An allocation build already survives drops; detaching it is a config
    error, not a silent no-op."""
    with pytest.raises(ValidationError, match="requires update_host"):
        config.SchedulerRuntimeDeployment(
            update_command="/site/bin/deploy-runtime",
            verify_command="/site/bin/verify-runtime",
            detached_build=True,
        )


@pytest.mark.parametrize(
    "spoofed",
    [
        {fleet_operation.ENV_OPERATION_ID: "a" * 64},
        {"VQ_FLEET_OPERATION_UNKNOWN": "reject-me"},
        {
            fleet_operation.ENV_OPERATION_ID: "A" * 64,
            fleet_operation.ENV_OPERATION_REQUEST_SHA256: "b" * 64,
            fleet_operation.ENV_OPERATION_NONCE: "c" * 64,
        },
    ],
)
def test_runtime_rejects_partial_or_malformed_outer_context_before_marker_or_ssh(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    spoofed: dict[str, str],
) -> None:
    for name, value in spoofed.items():
        monkeypatch.setenv(name, value)

    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("context failure reached marker or transport mutation")

    monkeypatch.setattr(admin, "_guard_admin_update_marker", forbidden)
    monkeypatch.setattr(admin.transport, "run_remote_shell", forbidden)

    with pytest.raises(admin.AdminError, match="execution context"):
        admin.update_scheduler_runtime(
            "host_f",
            "vibeqc-release",
            config.Config(),
            expected_sha=SHA,
        )


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"phase": "helper"}, "phase"),
        ({"action_id": "scheduler-runtime:host_f:vibeqc-dev"}, "action"),
        ({"host": "host_a"}, "host"),
        ({"program": "vibeqc-dev"}, "program"),
        ({"target_sha": "e" * 40}, "target SHA"),
        ({"target_tag": "v0.24.0"}, "target tag"),
    ],
)
def test_runtime_rejects_wrong_live_outer_identity_before_marker_or_ssh(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, object],
    match: str,
) -> None:
    fields = _outer_identity().as_dict()
    fields.update(changes)
    identity = fleet_operation.OperationIdentity.from_dict(fields)

    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("identity failure reached marker or transport mutation")

    with _activated_outer_operation(state_dir, monkeypatch, identity=identity):
        monkeypatch.setattr(admin, "_guard_admin_update_marker", forbidden)
        monkeypatch.setattr(admin.transport, "run_remote_shell", forbidden)
        with pytest.raises(admin.AdminError, match=match):
            admin.update_scheduler_runtime(
                "host_f",
                "vibeqc-release",
                config.Config(),
                expected_sha=SHA,
            )


def test_bound_detached_build_persists_before_fixed_argv_launch_and_retains_evidence(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _BoundBuildHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)

    with _activated_outer_operation(state_dir, monkeypatch) as (handle, _):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            [
                "/site/bin/deploy-runtime",
                "--literal",
                "; touch /tmp/must-not-run",
            ],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

        binding = fleet_operation.read_detached_scheduler_binding(
            handle.operation_id,
            state_root=handle.state_root,
        )
        assert binding is not None
        assert binding.target == "cluster-build"
        assert binding.program == "vibeqc-release"
        assert binding.mode == "runtime vibeqc-release update"
        assert re.fullmatch(
            r"/home/USER/\.vq-admin-r4b1-([0-9a-f]{64})/"
            r"detached/run-\1",
            binding.remote_run_dir,
        )
        assert (handle.directory / "scheduler-command.json").exists()
        assert not (handle.directory / "result.json").exists()

    assert proc.returncode == 0
    assert proc.stdout == "compiled\n"
    assert fake.launches == 1
    assert fake.observes == 1
    assert all(request["request_identity"]["argv"][2] == "; touch /tmp/must-not-run"
               for request in fake.requests)  # type: ignore[index]


def test_oversized_bound_request_fails_before_binding_or_ssh(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("oversized request reached SSH")

    monkeypatch.setattr(admin.transport, "run_remote_shell", forbidden)
    with _activated_outer_operation(state_dir, monkeypatch) as (handle, _):
        with pytest.raises(transport.RemoteError, match="bounded helper limit"):
            admin._run_detached_scheduler_command(
                _host_cfg(),
                ["/site/bin/deploy-runtime", "x" * admin._DETACHED_REQUEST_LIMIT],
                scratch_root="/home/USER",
                program="vibeqc-release",
                timeout=60.0,
                mode="runtime vibeqc-release update",
            )
        assert fleet_operation.read_detached_scheduler_binding(
            handle.operation_id,
            state_root=handle.state_root,
        ) is None


def test_double_slash_scratch_fails_before_binding_or_ssh(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("noncanonical scratch root reached SSH")

    monkeypatch.setattr(admin.transport, "run_remote_shell", forbidden)
    with _activated_outer_operation(state_dir, monkeypatch) as (handle, _):
        with pytest.raises(transport.RemoteError, match="normalized absolute"):
            admin._run_detached_scheduler_command(
                _host_cfg(),
                ["/site/bin/deploy-runtime"],
                scratch_root="//tmp",
                program="vibeqc-release",
                timeout=60.0,
                mode="runtime vibeqc-release update",
            )
        assert fleet_operation.read_detached_scheduler_binding(
            handle.operation_id,
            state_root=handle.state_root,
        ) is None


@pytest.mark.parametrize("launch_returncode", [255, 2])
def test_bound_detached_ambiguous_launch_is_adopted_without_replay(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    launch_returncode: int,
) -> None:
    fake = _BoundBuildHost(launch_returncode=launch_returncode)
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    command = ["/site/bin/deploy-runtime"]

    with _activated_outer_operation(state_dir, monkeypatch) as (handle, _):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            command,
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )
        assert proc.returncode == 0
        assert fleet_operation.read_detached_scheduler_binding(
            handle.operation_id,
            state_root=handle.state_root,
        ) is not None

        fake.launch_returncode = 0
        fake.observations = [
            subprocess.CompletedProcess(
                args=[], returncode=2, stdout="", stderr="missing run"
            )
        ]
        with pytest.raises(transport.RemoteError, match="outcome unknown"):
            admin._run_detached_scheduler_command(
                _host_cfg(),
                command,
                scratch_root="/home/USER",
                program="vibeqc-release",
                timeout=60.0,
                mode="runtime vibeqc-release update",
            )

    assert fake.launches == 1
    assert fake.observes == 2


@pytest.mark.parametrize("launch_failure", ["rc2", "malformed-receipt"])
def test_completed_ambiguous_launcher_gets_one_exact_observation_not_grace(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    launch_failure: str,
) -> None:
    class CompletedAmbiguousHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "launch" and launch_failure == "malformed-receipt":
                self.launches += 1
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout="not-json", stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    fake = CompletedAmbiguousHost(
        observations=[
            subprocess.CompletedProcess(
                args=[], returncode=2, stdout="", stderr="missing exact run"
            )
        ],
        launch_returncode=2 if launch_failure == "rc2" else 0,
    )
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with (
        _activated_outer_operation(state_dir, monkeypatch),
        pytest.raises(transport.RemoteError, match="outcome unknown"),
    ):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert fake.launches == 1
    assert fake.observes == 1


def test_transport_ambiguous_launch_missing_run_expires_without_replay(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _BoundBuildHost(
        observations=[
            subprocess.CompletedProcess(
                args=[], returncode=2, stdout="", stderr="missing exact run"
            )
        ],
        launch_returncode=255,
    )
    monkeypatch.setattr(admin, "DETACHED_BUILD_OBSERVATION_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with (
        _activated_outer_operation(state_dir, monkeypatch),
        pytest.raises(transport.RemoteError, match="outcome unknown"),
    ):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert fake.launches == 1
    assert fake.observes == 1


def test_bound_detached_poll_rc255_is_unobservable_not_a_receipt(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _BoundBuildHost(
        observations=[
            subprocess.CompletedProcess(
                args=[], returncode=255, stdout="", stderr="connection reset"
            ),
            None,
        ]
    )
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)

    with _activated_outer_operation(state_dir, monkeypatch):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert proc.returncode == 0
    assert fake.launches == 1
    assert fake.observes == 2


def test_bound_strict_terminal_result_is_accepted_before_lease_release(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FinishingHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "observe":
                query = json.loads(kwargs["stdin_data"])
                self.observes += 1
                payload = _bound_observation(query["request"], lease_busy=True)
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout=json.dumps(payload), stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    fake = FinishingHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with _activated_outer_operation(state_dir, monkeypatch):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert proc.returncode == 0
    assert fake.observes == 1


def test_bound_detached_mismatched_receipt_is_outcome_unknown(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MismatchHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "observe":
                query = json.loads(kwargs["stdin_data"])
                request = query["request"]
                self.observes += 1
                payload = _bound_observation(
                    request,
                    binding_sha256="e" * 64,
                )
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout=json.dumps(payload), stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    fake = MismatchHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with (
        _activated_outer_operation(state_dir, monkeypatch),
        pytest.raises(transport.RemoteError, match="outcome unknown"),
    ):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert fake.launches == 1
    assert fake.observes == 1


def test_bound_detached_rejects_stream_not_matching_terminal_output_digest(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OutputMismatchHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "observe":
                query = json.loads(kwargs["stdin_data"])
                request = query["request"]
                self.observes += 1
                payload = _bound_observation(request)
                wrong = hashlib.sha256(b"different retained bytes").hexdigest()
                payload["output_stored_sha256"] = wrong
                result = payload["result"]
                assert isinstance(result, dict)
                result["output_stored_sha256"] = wrong
                result["output_sha256"] = wrong
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout=json.dumps(payload), stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    fake = OutputMismatchHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with (
        _activated_outer_operation(state_dir, monkeypatch),
        pytest.raises(transport.RemoteError, match="terminal retained-output digest"),
    ):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )


def test_bound_detached_rejects_untruncated_full_output_digest_mismatch(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        _, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )
    payload = _bound_observation(request)
    result = payload["result"]
    assert isinstance(result, dict)
    result["output_sha256"] = hashlib.sha256(b"contradictory full output").hexdigest()

    with pytest.raises(transport.RemoteError, match="invalid output accounting"):
        admin._parse_detached_observation(payload, request=request, offset=0)


def test_bound_detached_rejects_output_truncated_before_storage_limit(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        _, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )
    payload = _bound_observation(request, output_text="0123456789")
    result = payload["result"]
    assert isinstance(result, dict)
    result["output_observed_bytes"] = 20
    result["output_sha256"] = hashlib.sha256(b"0123456789abcdefghij").hexdigest()
    result["output_truncated"] = True

    with pytest.raises(transport.RemoteError, match="invalid output accounting"):
        admin._parse_detached_observation(payload, request=request, offset=0)


def test_bound_terminal_output_backlog_drains_without_poll_interval_sleeps(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained = (
        "x" * (admin._DETACHED_OBSERVATION_CHUNK - 1)
        + "€"
        + "y" * (2 * admin._DETACHED_OBSERVATION_CHUNK + 17)
    )

    class BacklogHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "observe":
                query = json.loads(kwargs["stdin_data"])
                self.observes += 1
                payload = _bound_observation(
                    query["request"],
                    output_text=retained,
                    offset=query["offset"],
                )
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout=json.dumps(payload), stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    sleeps: list[float] = []
    monkeypatch.setattr(admin, "DETACHED_BUILD_POLL_INTERVAL_SECONDS", 30.0)
    monkeypatch.setattr(admin.time, "sleep", lambda seconds: sleeps.append(seconds))
    fake = BacklogHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with _activated_outer_operation(state_dir, monkeypatch):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )

    assert proc.stdout == retained
    assert fake.observes == 4
    assert sleeps == [30.0]


def test_bound_detached_remote_timeout_cannot_become_deploy_success(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TimedOutHost(_BoundBuildHost):
        def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
            if argv[-1] == "observe":
                query = json.loads(kwargs["stdin_data"])
                self.observes += 1
                payload = _bound_observation(
                    query["request"],
                    returncode=0,
                    timed_out=True,
                )
                return subprocess.CompletedProcess(
                    args=list(argv), returncode=0, stdout=json.dumps(payload), stderr=""
                )
            return super().__call__(host_cfg, *argv, **kwargs)

    fake = TimedOutHost()
    monkeypatch.setattr(admin.transport, "run_remote_shell", fake)
    with (
        _activated_outer_operation(state_dir, monkeypatch),
        pytest.raises(transport.RemoteError, match="remote self-timeout"),
    ):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )


@pytest.mark.parametrize(
    ("receipt_name", "timestamp_field", "timestamp", "message"),
    [
        (
            "activation",
            "activated_at",
            "2026-08-10T11:59:59+00:00",
            "activation predates lease acquisition",
        ),
        (
            "result",
            "completed_at",
            "2026-08-10T12:00:00+00:00",
            "result predates activation",
        ),
    ],
)
def test_bound_detached_receipt_timestamps_must_follow_protocol_order(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_name: str,
    timestamp_field: str,
    timestamp: str,
    message: str,
) -> None:
    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        _, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )
    payload = _bound_observation(request)
    receipt = payload[receipt_name]
    assert isinstance(receipt, dict)
    receipt[timestamp_field] = timestamp

    with pytest.raises(transport.RemoteError, match=message):
        admin._parse_detached_observation(payload, request=request, offset=0)


def test_embedded_observer_reads_receipts_in_reverse_publish_order() -> None:
    source = admin._DETACHED_REMOTE_HELPER_SOURCE
    observe_body = source[
        source.index("def observe(query):") : source.index("\ndef main():")
    ]

    result_index = observe_body.index(
        'result_receipt = optional_json(run_fd, "result.json")'
    )
    activation_index = observe_body.index(
        'activation_receipt = optional_json(run_fd, "activation.json")'
    )
    lease_index = observe_body.index(
        'lease_receipt = optional_json(run_fd, "lease.json")'
    )
    output_index = observe_body.index(
        "data, chunk = output_snapshot(run_fd, offset, max_bytes)"
    )

    assert result_index < activation_index < lease_index < output_index


def test_embedded_helper_launches_records_and_retains_owner_only_receipts(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch = state_dir / "remote-scratch"
    scratch.mkdir(mode=0o700)
    legacy_detached = scratch / ".vq-admin" / "detached"
    legacy_detached.mkdir(parents=True, mode=0o755)
    (scratch / ".vq-admin").chmod(0o755)
    legacy_detached.chmod(0o755)
    other_user_namespace = scratch / f"{admin._DETACHED_PROTOCOL_ROOT}{'e' * 64}"
    other_user_namespace.mkdir(mode=0o700)
    (other_user_namespace / "sentinel").write_text("unrelated owner namespace\n")
    sentinel = state_dir / "must-not-exist"
    literal = f"literal; touch {sentinel}"
    command = [
        sys.executable,
        "-c",
        (
            "import os,sys; inherited=os.umask(0); os.umask(inherited); "
            "print(f'{inherited:03o}'); print(sys.argv[1])"
        ),
        literal,
    ]

    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, created, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=command,
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=10.0,
            mode="runtime vibeqc-release update",
        )
        assert created is True
        launch = subprocess.run(
            [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
            umask=0o027,
        )
        assert launch.returncode == 0, launch.stderr

        offset = 0
        output_bytes = bytearray()
        deadline = time.monotonic() + _LIVENESS_SECONDS
        while True:
            query = {
                "schema": admin._DETACHED_QUERY_SCHEMA,
                "request": request,
                "offset": offset,
                "max_bytes": admin._DETACHED_OBSERVATION_CHUNK,
            }
            observed_proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    admin._DETACHED_REMOTE_HELPER_SOURCE,
                    "observe",
                ],
                input=_canonical(query).decode() + "\n",
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert observed_proc.returncode == 0, observed_proc.stderr
            payload = json.loads(observed_proc.stdout)
            observed = admin._parse_detached_observation(
                payload,
                request=request,
                offset=offset,
            )
            output_bytes.extend(observed.output)
            offset = observed.next_offset
            if observed.state == "completed" and offset == observed.stored_bytes:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("embedded recorder did not become terminal")
            time.sleep(0.01)

    assert observed.returncode == 0
    assert output_bytes.decode() == "027\n" + literal + "\n"
    assert not sentinel.exists(), "action argv must never be interpreted by a shell"
    run_dir = Path(binding.remote_run_dir)
    assert run_dir.exists()
    assert stat.S_IMODE(run_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((scratch / ".vq-admin").stat().st_mode) == 0o755
    assert stat.S_IMODE(
        (scratch / f"{admin._DETACHED_PROTOCOL_ROOT}{binding.run_id}").stat().st_mode
    ) == 0o700
    assert (other_user_namespace / "sentinel").read_text() == (
        "unrelated owner namespace\n"
    )
    for name in (
        "request.json",
        "lease.lock",
        "lease.json",
        "activation.json",
        "result.json",
        "output.log",
    ):
        info = (run_dir / name).stat()
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert info.st_nlink == 1


def test_embedded_launcher_exception_after_inherit_does_not_unlock_recorder(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch = state_dir / "lease-transfer-scratch"
    scratch.mkdir(mode=0o700)
    command = [sys.executable, "-c", "import time; time.sleep(1)"]
    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=command,
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=10.0,
            mode="runtime vibeqc-release update",
        )
        needle = "        os.close(lease_fd)\n        lease_fd = -1\n"
        replacement = (
            "        raise RuntimeError('injected after recorder inherited lease')\n"
        )
        assert admin._DETACHED_REMOTE_HELPER_SOURCE.count(needle) == 1
        injected_source = admin._DETACHED_REMOTE_HELPER_SOURCE.replace(
            needle,
            replacement,
        )
        launch = subprocess.run(
            [sys.executable, "-c", injected_source, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert launch.returncode != 0
        lease_fd = os.open(Path(binding.remote_run_dir, "lease.lock"), os.O_RDWR)
        try:
            with pytest.raises(OSError):
                fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(lease_fd)
        result_path = Path(binding.remote_run_dir, "result.json")
        deadline = time.monotonic() + 5
        while not result_path.exists():
            if time.monotonic() >= deadline:
                raise AssertionError("inherited recorder did not finish")
            time.sleep(0.01)


def test_embedded_helper_timeout_kills_owned_group_even_after_parent_exits(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch = state_dir / "timeout-scratch"
    scratch.mkdir(mode=0o700)
    fork_and_hold_stdout = (
        "import os,signal,time; "
        "pid=os.fork(); "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN) if pid == 0 else None; "
        "time.sleep(30)"
    )
    command = [sys.executable, "-c", fork_and_hold_stdout]

    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=command,
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=0.5,
            mode="runtime vibeqc-release update",
        )
        started_at = time.monotonic()
        launch = subprocess.run(
            [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert launch.returncode == 0, launch.stderr
        deadline = time.monotonic() + 5
        offset = 0
        while True:
            query = {
                "schema": admin._DETACHED_QUERY_SCHEMA,
                "request": request,
                "offset": offset,
                "max_bytes": admin._DETACHED_OBSERVATION_CHUNK,
            }
            poll = subprocess.run(
                [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "observe"],
                input=_canonical(query).decode() + "\n",
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert poll.returncode == 0, poll.stderr
            observed = admin._parse_detached_observation(
                json.loads(poll.stdout), request=request, offset=offset
            )
            offset = observed.next_offset
            if observed.state == "completed":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("timed-out process group was not reaped")
            time.sleep(0.02)

    assert observed.timed_out is True
    assert observed.returncode != 0
    # `timed_out` and the non-zero return code above are what prove the group
    # was reaped by the timeout. This is only a ceiling on the whole exchange
    # -- subprocess spawn, helper startup and the poll loop included -- so it
    # is a liveness bound, not a measurement.
    assert time.monotonic() - started_at < _LIVENESS_SECONDS
    assert Path(binding.remote_run_dir, "result.json").exists()


def test_embedded_helper_handles_early_output_eof_while_action_keeps_running(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch = state_dir / "early-eof-scratch"
    scratch.mkdir(mode=0o700)
    command = [
        sys.executable,
        "-c",
        "import os,time; os.close(1); os.close(2); time.sleep(0.3)",
    ]
    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=command,
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=2.0,
            mode="runtime vibeqc-release update",
        )
        started_at = time.monotonic()
        launch = subprocess.run(
            [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert launch.returncode == 0, launch.stderr
        result_path = Path(binding.remote_run_dir, "result.json")
        deadline = time.monotonic() + _LIVENESS_SECONDS
        while not result_path.exists():
            if time.monotonic() >= deadline:
                raise AssertionError("early-EOF action did not become terminal")
            time.sleep(0.01)

    elapsed = time.monotonic() - started_at
    # The floor is the property: closing stdout and stderr must not cut the
    # action short, so the run has to last about as long as the action sleeps.
    # Load can only push this up, never below, so it is safe on a busy runner.
    assert elapsed >= _EOF_ACTION_SLEEP - 0.1
    # The ceiling used to be `< 2`, compared against the 2.0s timeout the
    # helper was given -- zero margin by construction, and on a loaded runner
    # the spawn overhead alone took it to 2.4s. What it was really claiming is
    # that the timeout never fired, and the status assertion below proves that
    # exactly. This is now just a liveness bound.
    assert elapsed < _LIVENESS_SECONDS
    assert json.loads(result_path.read_text())["status"] == "success"


def test_embedded_helper_caps_retained_output_while_draining_action(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch = state_dir / "bounded-output-scratch"
    scratch.mkdir(mode=0o700)
    emitted = admin._DETACHED_OUTPUT_LIMIT + 12345
    command = [
        sys.executable,
        "-c",
        f"import sys; sys.stdout.buffer.write(b'x' * {emitted})",
    ]

    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=command,
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=10.0,
            mode="runtime vibeqc-release update",
        )
        launch = subprocess.run(
            [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert launch.returncode == 0, launch.stderr
        result_path = Path(binding.remote_run_dir, "result.json")
        deadline = time.monotonic() + _LIVENESS_SECONDS
        while not result_path.exists():
            if time.monotonic() >= deadline:
                raise AssertionError("bounded-output recorder did not finish")
            time.sleep(0.01)
        result = json.loads(result_path.read_text())

    output_path = Path(binding.remote_run_dir, "output.log")
    assert output_path.stat().st_size == admin._DETACHED_OUTPUT_LIMIT
    assert result["output_stored_bytes"] == admin._DETACHED_OUTPUT_LIMIT
    assert result["output_observed_bytes"] == emitted
    assert result["output_truncated"] is True


@pytest.mark.parametrize("attack", ["preposition", "symlink-scratch"])
def test_embedded_helper_rejects_prepositioned_or_symlinked_run_paths(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    real_scratch = state_dir / "real-scratch"
    real_scratch.mkdir(mode=0o700)
    scratch = real_scratch
    if attack == "symlink-scratch":
        scratch = state_dir / "scratch-link"
        scratch.symlink_to(real_scratch, target_is_directory=True)

    with _activated_outer_operation(state_dir, monkeypatch) as (_, context):
        binding, _, request = admin._build_detached_binding_request(
            context,
            host_cfg=_host_cfg(),
            argv=[sys.executable, "-c", "raise SystemExit(0)"],
            scratch_root=str(scratch),
            program="vibeqc-release",
            timeout=10.0,
            mode="runtime vibeqc-release update",
        )
        sentinel: Path | None = None
        if attack == "preposition":
            run_dir = Path(binding.remote_run_dir)
            run_dir.mkdir(parents=True, mode=0o700)
            managed_root = real_scratch / (
                f"{admin._DETACHED_PROTOCOL_ROOT}{binding.run_id}"
            )
            managed_root.chmod(0o700)
            (managed_root / "detached").chmod(0o700)
            sentinel = run_dir / "sentinel"
            sentinel.write_text("unchanged\n")
        launch = subprocess.run(
            [sys.executable, "-c", admin._DETACHED_REMOTE_HELPER_SOURCE, "launch"],
            input=_canonical(request).decode() + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )

    assert launch.returncode != 0
    if sentinel is not None:
        assert sentinel.read_text() == "unchanged\n"


class _FakeBuildHost:
    """Scripted remote side of the detach/poll protocol."""

    def __init__(self, polls: list[object]) -> None:
        self.polls = polls
        self.launcher_stdin: str | None = None
        self.kill_scripts: list[str] = []
        self.cleanup_seen = False

    def __call__(self, host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        stdin = kwargs.get("stdin_data") or ""
        if "cmd.sh" in stdin and "VQCMD" in stdin:
            self.launcher_stdin = stdin
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0,
                stdout="detached pid 4242\n", stderr="",
            )
        if "kill --" in stdin:
            self.kill_scripts.append(stdin)
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="", stderr=""
            )
        if "rm -rf" in stdin:
            self.cleanup_seen = True
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="", stderr=""
            )
        if 'echo "RC=' in stdin:
            step = self.polls.pop(0) if self.polls else "RC=none\nSIZE=0"
            if isinstance(step, Exception):
                raise step
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout=str(step), stderr=""
            )
        raise AssertionError(f"unexpected remote invocation: {stdin[:80]!r}")


def _poll(rc: str, chunk: str, offset: int) -> str:
    size = offset + len(chunk.encode())
    return (
        f"RC={rc}\nSIZE={size}\n{admin._DETACHED_OUTPUT_MARKER}\n{chunk}"
    )


def test_detached_build_survives_a_dropped_poll_connection(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """THE POINT. A connection loss costs one poll, never the build."""
    fake = _FakeBuildHost(
        polls=[
            transport.RemoteError("kex_exchange_identification: reset"),
            _poll("none", "compiling libint\n", 0),
            _poll("0", "activated\n", len(b"compiling libint\n")),
        ]
    )
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake)
    run_log_path = tmp_path / "transcript.log"
    run_log = output.RunLog(run_log_path)

    with output.channel(run_log=run_log):
        proc = admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime", "--program", "vibeqc-release"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=60.0,
            mode="runtime vibeqc-release update",
        )
    run_log.close()

    assert proc.returncode == 0
    assert proc.stdout == "compiling libint\nactivated\n"
    assert "/site/bin/deploy-runtime" in (fake.launcher_stdin or "")
    transcript = run_log_path.read_text()
    assert transcript.count("compiling libint") == 1
    assert "activated" in transcript
    assert fake.cleanup_seen, "the run dir must be cleaned up after success"


def test_detached_timeout_kills_the_remote_process_group(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeBuildHost(polls=[_poll("none", "", 0)] * 50)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake)

    with pytest.raises(transport.RemoteError, match="exceeded"):
        admin._run_detached_scheduler_command(
            _host_cfg(),
            ["/site/bin/deploy-runtime"],
            scratch_root="/home/USER",
            program="vibeqc-release",
            timeout=0.05,
            mode="runtime vibeqc-release update",
        )

    assert fake.kill_scripts, "the wall timeout must kill the process group"


def test_runtime_lane_streams_a_detached_build_end_to_end(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`vq admin update PROGRAM HOST` with detached_build: one streamed copy
    of the build output in the transcript, receipt verified, LAST OK true."""
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                'update_host = "cluster-build"',
                "detached_build = true",
                "",
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/opt/vibeqc/bin/vibeqc"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    receipt = {
        "program": "vibeqc-release",
        "source_sha": SHA,
        "tag": None,
        "healthy": True,
        "activation": "atomic",
        "active_path": "/site/runtimes/vibeqc-release/current",
        "health_detail": "import ok",
        "quiescent": True,
        "updater_pid": None,
    }
    fake = _FakeBuildHost(polls=[_poll("0", "building on cluster-build\n", 0)])
    original_call = fake.__call__

    def dispatch(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if not kwargs.get("stdin_data"):
            # The independent login-host verify command.
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0,
                stdout=json.dumps(receipt), stderr="",
            )
        return original_call(host_cfg, *argv, **kwargs)

    monkeypatch.setattr("vq.admin.transport.run_remote_shell", dispatch)

    result = admin.update_scheduler_runtime(
        "host_f", "vibeqc-release", cfg, expected_sha=SHA
    )

    assert result.success is True, result.work_errors
    assert "building on cluster-build" in result.command_output
    transcript = Path(result.run_log_path or "").read_text()
    assert transcript.count("building on cluster-build") == 1
    assert "streamed above" in transcript


def test_bound_runtime_rc0_still_requires_independent_verify_for_last_ok(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                'update_host = "cluster-build"',
                "detached_build = true",
                "",
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/opt/vibeqc/bin/vibeqc"',
                "",
            ]
        )
    )
    cfg = config.load_config()
    fake = _BoundBuildHost()

    def dispatch(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if argv and argv[0] == "python3":
            return fake(host_cfg, *argv, **kwargs)
        assert argv[0] == "/site/bin/verify-runtime"
        return subprocess.CompletedProcess(
            args=list(argv), returncode=1, stdout="", stderr="verify failed"
        )

    monkeypatch.setattr(admin.transport, "run_remote_shell", dispatch)
    with _activated_outer_operation(state_dir, monkeypatch):
        result = admin.update_scheduler_runtime(
            "host_f",
            "vibeqc-release",
            cfg,
            expected_sha=SHA,
        )

    assert result.command_rc == 0
    assert result.verify_rc == 1
    assert result.success is False
    assert result.marker_cleared is False
    record = admin.load_scheduler_runtime_status()["host_f:vibeqc-release"]
    assert record.last_success is False
    assert record.last_ok_sha is None

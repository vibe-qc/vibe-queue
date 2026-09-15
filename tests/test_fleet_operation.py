"""Durable local supervisor contract for one mutating fleet action."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import math
import os
import signal
import stat
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from vq import fleet_operation

REPORT_DIGEST = "a" * 64
TARGET_SHA = "b" * 40


def _identity(
    *,
    attempt: int = 1,
    argv: tuple[str, ...] = ("--help",),
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> fleet_operation.OperationIdentity:
    return fleet_operation.OperationIdentity(
        rollout_id="v0.24.0-aaaaaaaaaaaa",
        report_digest_sha256=REPORT_DIGEST,
        attempt=attempt,
        action_id="local-runtime:driver:vibeqc-queue",
        phase="local-runtime",
        host="driver",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=TARGET_SHA,
        target_version="0.24.0",
        target_tag="v0.24.0",
        argv=argv,
        lifecycle_resources=lifecycle_resources,
    )


def _prepare(tmp_path: Path, **kwargs: object) -> fleet_operation.OperationHandle:
    identity = _identity(**kwargs)
    return fleet_operation.prepare_operation(identity, state_root=tmp_path / "state")


# A bound on "this eventually happens", never a timing claim. A healthy run
# finishes these waits in milliseconds; a machine starved of CPU for seconds at
# a time (load 88-131 on 2026-09-12, #26) must not turn a 3 s deadline into a
# failure of an ordering property.
LIVENESS_SECONDS = 60.0


def _wait_ready(
    handle: fleet_operation.OperationHandle,
    *,
    timeout: float = 3.0,
) -> fleet_operation.OperationObservation:
    return fleet_operation.wait_for_ready(
        handle.operation_id,
        state_root=handle.state_root,
        timeout=timeout,
        poll_interval=0.005,
    )


def _write_running_wait_spec(handle: fleet_operation.OperationHandle, jobid: str) -> Path:
    """Create one harmless local wait target beneath the operation state root."""
    from vq.spec import JobSpec, JobState

    queue = handle.state_root / "queue"
    workspace = handle.state_root / "jobs" / jobid
    queue.mkdir(parents=True, exist_ok=True)
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
    )
    path = queue / f"{jobid}.json"
    spec.write(path)
    return path


def _child_pids(parent_pid: int) -> list[int]:
    """Return direct children through the portable macOS/Linux ps surface."""
    proc = subprocess.run(
        ["ps", "-axo", "pid=,ppid="],
        check=True,
        capture_output=True,
        text=True,
    )
    children: list[int] = []
    for line in proc.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and int(fields[1]) == parent_pid:
            children.append(int(fields[0]))
    return children


def _wait_for_activation(
    handle: fleet_operation.OperationHandle,
    *,
    timeout: float = 5.0,
) -> fleet_operation.OperationObservation:
    deadline = time.monotonic() + timeout
    while True:
        observed = fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )
        if observed.activation is not None:
            return observed
        if time.monotonic() >= deadline:
            raise AssertionError("operation never published activation")
        time.sleep(0.01)


def _activate_operation_for_scheduler_binding(
    handle: fleet_operation.OperationHandle,
    *,
    nonce: str = "d" * 64,
) -> fleet_operation.OperationExecutionContext:
    output_path = handle.directory / "output.log"
    output_path.write_bytes(b"")
    output_path.chmod(0o600)
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
    return fleet_operation.OperationExecutionContext(
        operation_id=handle.operation_id,
        request_sha256=handle.request_sha256,
        nonce=nonce,
    )


@contextlib.contextmanager
def _hold_operation_lease(handle: fleet_operation.OperationHandle):  # type: ignore[no-untyped-def]
    lease_fd = os.open(handle.directory / "lease.lock", os.O_RDWR)
    try:
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        fcntl.flock(lease_fd, fcntl.LOCK_UN)
        os.close(lease_fd)


def test_operation_id_is_canonical_and_binds_every_action_field() -> None:
    identity = _identity()
    expected = hashlib.sha256(
        json.dumps(
            identity.as_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()

    assert fleet_operation.operation_id(identity) == expected
    assert identity.as_dict()["schema"] == ("vq.fleet.rollout_operation_identity/1")

    original = fleet_operation.operation_id(identity)
    changed_values = (
        _identity(attempt=2),
        fleet_operation.OperationIdentity(**{**identity.constructor_fields(), "host": "host_a"}),
        fleet_operation.OperationIdentity(
            **{**identity.constructor_fields(), "argv": ("admin", "update")}
        ),
    )
    assert all(fleet_operation.operation_id(value) != original for value in changed_values)


def test_identity_lifecycle_resources_round_trip_through_request_json(
    tmp_path: Path,
) -> None:
    resources = (
        ("target", str(tmp_path / "venv")),
        ("checkout", str(tmp_path / "repo")),
    )
    identity = _identity(lifecycle_resources=resources)

    assert identity.lifecycle_resources == tuple(sorted(resources))
    assert fleet_operation.OperationIdentity.from_dict(identity.as_dict()) == identity

    handle = fleet_operation.prepare_operation(
        identity,
        state_root=tmp_path / "state",
    )
    persisted = json.loads((handle.directory / "request.json").read_text())
    assert persisted["identity"]["lifecycle_resources"] == [
        list(item) for item in tuple(sorted(resources))
    ]
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.identity == identity


@pytest.mark.parametrize(
    "argv",
    [
        ("admin", "update", "vq", "--token", "secret"),
        ("admin", "update", "vq", "--token=secret"),
        ("admin", "update", "vq", "--token-file", "/tmp/token"),
        ("admin", "update", "vq", "--token-stdin"),
        ("admin", "update", "vq", "--password=hunter2"),
        ("admin", "update", "bad\x00value"),
    ],
)
def test_identity_rejects_credentials_and_nul(argv: tuple[str, ...]) -> None:
    with pytest.raises(fleet_operation.OperationSecurityError):
        _identity(argv=argv)


def test_identity_rejects_invalid_hash_attempt_and_control_text() -> None:
    fields = _identity().constructor_fields()
    for changes in (
        {"report_digest_sha256": "A" * 64},
        {"target_sha": "not-a-sha"},
        {"attempt": 0},
        {"rollout_id": "bad\nrollout"},
    ):
        with pytest.raises(fleet_operation.OperationSecurityError):
            fleet_operation.OperationIdentity(**{**fields, **changes})


def test_execution_context_environment_is_all_or_none_and_strict() -> None:
    valid = {
        fleet_operation.ENV_OPERATION_ID: "a" * 64,
        fleet_operation.ENV_OPERATION_REQUEST_SHA256: "b" * 64,
        fleet_operation.ENV_OPERATION_NONCE: "c" * 64,
    }
    assert fleet_operation.execution_context_from_environ({}) is None
    assert fleet_operation.execution_context_from_environ(valid) == (
        fleet_operation.OperationExecutionContext(
            operation_id="a" * 64,
            request_sha256="b" * 64,
            nonce="c" * 64,
        )
    )

    for invalid in (
        {fleet_operation.ENV_OPERATION_ID: "a" * 64},
        {**valid, fleet_operation.ENV_OPERATION_ID: "A" * 64},
        {**valid, fleet_operation.ENV_OPERATION_REQUEST_SHA256: "short"},
        {**valid, fleet_operation.ENV_OPERATION_NONCE: "c" * 63},
        {**valid, "VQ_FLEET_OPERATION_UNRECOGNIZED": "reject-me"},
    ):
        with pytest.raises(fleet_operation.OperationSecurityError):
            fleet_operation.execution_context_from_environ(invalid)


def test_prepare_creates_private_fixed_layout_and_is_idempotent(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    repeated = fleet_operation.prepare_operation(
        handle.identity,
        state_root=handle.state_root,
    )

    assert repeated == handle
    assert handle.directory == (handle.state_root / "rollouts" / "operations" / handle.operation_id)
    assert stat.S_IMODE(handle.directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(handle.directory.parent.stat().st_mode) == 0o700
    assert sorted(path.name for path in handle.directory.iterdir()) == [
        "decision.lock",
        "lease.lock",
        "request.json",
    ]
    for name in ("request.json", "lease.lock", "decision.lock"):
        path = handle.directory / name
        assert stat.S_ISREG(path.lstat().st_mode)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert path.stat().st_uid == os.geteuid()

    payload = json.loads((handle.directory / "request.json").read_text())
    assert payload == {
        "schema": "vq.fleet.rollout_operation_request/1",
        "operation_id": handle.operation_id,
        "identity": handle.identity.as_dict(),
    }
    assert (
        handle.request_sha256
        == hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
    )
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.identity == handle.identity
    assert observed.request_sha256 == handle.request_sha256


def test_temp_only_receipt_crash_is_recovered_under_reconciliation(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    temporary = handle.directory / f".ready.json.{'1' * 32}.tmp"
    temporary.write_bytes(b'{"schema":')
    temporary.chmod(0o600)
    before = temporary.stat()
    before_bytes = temporary.read_bytes()
    directory_mtime = handle.directory.stat().st_mtime_ns

    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.list_operation_ids(state_root=handle.state_root)
    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )
    after = temporary.stat()
    assert temporary.read_bytes() == before_bytes
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    assert handle.directory.stat().st_mtime_ns == directory_mtime

    assert fleet_operation.list_operation_ids(
        state_root=handle.state_root,
        recover=True,
    ) == [handle.operation_id]
    assert temporary.exists() is False
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "prepared"


def test_linked_receipt_crash_recovers_same_inode_temp_and_target(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    output = handle.directory / "output.log"
    output.write_bytes(b"")
    output.chmod(0o600)
    payload = fleet_operation._ready_payload(handle, nonce="2" * 64)
    encoded = (json.dumps(payload, allow_nan=False, indent=2, sort_keys=True) + "\n").encode()
    temporary = handle.directory / f".ready.json.{'3' * 32}.tmp"
    temporary.write_bytes(encoded)
    temporary.chmod(0o600)
    target = handle.directory / "ready.json"
    os.link(temporary, target)
    assert target.stat().st_ino == temporary.stat().st_ino
    assert target.stat().st_nlink == 2
    before = temporary.stat()
    directory_mtime = handle.directory.stat().st_mtime_ns

    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.list_operation_ids(state_root=handle.state_root)
    after = temporary.stat()
    assert (after.st_dev, after.st_ino, after.st_nlink, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
        before.st_mtime_ns,
    )
    assert target.stat().st_nlink == 2
    assert handle.directory.stat().st_mtime_ns == directory_mtime

    assert fleet_operation.list_operation_ids(
        state_root=handle.state_root,
        recover=True,
    ) == [handle.operation_id]
    assert temporary.exists() is False
    assert target.stat().st_nlink == 1
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "abandoned-pre-authorization"


def test_unpublished_request_temp_recovery_does_not_wedge_global_listing(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    request = handle.directory / "request.json"
    encoded = request.read_bytes()
    request.unlink()
    temporary = handle.directory / f".request.json.{'4' * 32}.tmp"
    temporary.write_bytes(encoded)
    temporary.chmod(0o600)

    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.list_operation_ids(state_root=handle.state_root)
    assert temporary.read_bytes() == encoded

    assert (
        fleet_operation.list_operation_ids(
            state_root=handle.state_root,
            recover=True,
        )
        == []
    )
    assert temporary.exists() is False
    assert fleet_operation.list_operation_ids(state_root=handle.state_root) == []

    repeated = fleet_operation.prepare_operation(
        handle.identity,
        state_root=handle.state_root,
    )
    assert repeated == handle
    assert fleet_operation.list_operation_ids(state_root=handle.state_root) == [handle.operation_id]


def test_empty_pre_request_directory_recovery_does_not_wedge_listing_or_prepare(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    for name in ("request.json", "lease.lock", "decision.lock"):
        (handle.directory / name).unlink()
    directory_mtime = handle.directory.stat().st_mtime_ns

    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.list_operation_ids(state_root=handle.state_root)
    assert list(handle.directory.iterdir()) == []
    assert handle.directory.stat().st_mtime_ns == directory_mtime

    assert (
        fleet_operation.list_operation_ids(
            state_root=handle.state_root,
            recover=True,
        )
        == []
    )
    assert sorted(path.name for path in handle.directory.iterdir()) == [
        "decision.lock",
        "lease.lock",
    ]
    for name in ("decision.lock", "lease.lock"):
        info = (handle.directory / name).stat()
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert info.st_uid == os.geteuid()
        assert info.st_nlink == 1
        assert info.st_size == 0

    repeated = fleet_operation.prepare_operation(
        handle.identity,
        state_root=handle.state_root,
    )
    assert repeated == handle
    assert fleet_operation.list_operation_ids(state_root=handle.state_root) == [handle.operation_id]


def test_decision_only_pre_request_recovery_preserves_lock_and_allows_prepare(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    (handle.directory / "request.json").unlink()
    (handle.directory / "lease.lock").unlink()
    decision = handle.directory / "decision.lock"
    before = decision.stat()
    directory_mtime = handle.directory.stat().st_mtime_ns

    with pytest.raises(fleet_operation.OperationStateError, match="explicit"):
        fleet_operation.list_operation_ids(state_root=handle.state_root)
    after = decision.stat()
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    assert handle.directory.stat().st_mtime_ns == directory_mtime

    assert (
        fleet_operation.list_operation_ids(
            state_root=handle.state_root,
            recover=True,
        )
        == []
    )
    assert decision.stat().st_ino == before.st_ino
    lease = handle.directory / "lease.lock"
    lease_info = lease.stat()
    assert stat.S_ISREG(lease_info.st_mode)
    assert stat.S_IMODE(lease_info.st_mode) == 0o600
    assert lease_info.st_uid == os.geteuid()
    assert lease_info.st_nlink == 1
    assert lease_info.st_size == 0

    repeated = fleet_operation.prepare_operation(
        handle.identity,
        state_root=handle.state_root,
    )
    assert repeated == handle
    assert fleet_operation.list_operation_ids(state_root=handle.state_root) == [handle.operation_id]


def test_polling_waits_for_hardlink_publish_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)
    linked = threading.Event()
    allow_cleanup = threading.Event()
    original_unlink = fleet_operation.os.unlink
    paused = False

    def paused_unlink(path: object, *args: object, **kwargs: object) -> None:
        nonlocal paused
        if (
            not paused
            and isinstance(path, str)
            and path.startswith(".ready.json.")
            and path.endswith(".tmp")
        ):
            paused = True
            linked.set()
            assert allow_cleanup.wait(LIVENESS_SECONDS)
        original_unlink(path, *args, **kwargs)

    # The waiter must be seen observing while the publish is paused before
    # "it has not returned" can mean anything; the old fixed sleep could pass
    # before the waiter thread had been scheduled at all (#26).
    original_observe = fleet_operation.observe_operation
    waiter_observing = threading.Event()

    def watched_observe(*args: object, **kwargs: object) -> object:
        if linked.is_set() and not allow_cleanup.is_set():
            waiter_observing.set()
        return original_observe(*args, **kwargs)

    monkeypatch.setattr(fleet_operation.os, "unlink", paused_unlink)
    monkeypatch.setattr(fleet_operation, "observe_operation", watched_observe)
    with ThreadPoolExecutor(max_workers=2) as pool:
        supervisor = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=LIVENESS_SECONDS,
            poll_interval=0.005,
        )
        assert linked.wait(LIVENESS_SECONDS)
        waiter = pool.submit(
            fleet_operation.wait_for_ready,
            handle.operation_id,
            state_root=handle.state_root,
            timeout=LIVENESS_SECONDS,
            poll_interval=0.005,
        )
        assert waiter_observing.wait(LIVENESS_SECONDS)
        # A negative probe may stay short: load can only make it less strict,
        # never make it fail.
        time.sleep(0.03)
        assert waiter.done() is False
        allow_cleanup.set()
        ready = waiter.result(timeout=LIVENESS_SECONDS)
        assert ready.ready is not None
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert supervisor.result(timeout=LIVENESS_SECONDS) == 0


def test_list_operation_ids_is_sorted_and_fails_closed_on_unknown_entries(
    tmp_path: Path,
) -> None:
    first = _prepare(tmp_path, attempt=2)
    second = _prepare(tmp_path, attempt=1)
    assert fleet_operation.list_operation_ids(state_root=first.state_root) == sorted(
        [first.operation_id, second.operation_id]
    )

    unexpected = first.directory.parent / "README"
    unexpected.write_text("not an operation")
    with pytest.raises(fleet_operation.OperationSecurityError):
        fleet_operation.list_operation_ids(state_root=first.state_root)


def test_list_operation_ids_rejects_symlink_entry(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    (handle.directory.parent / ("e" * 64)).symlink_to(handle.directory)
    with pytest.raises(fleet_operation.OperationSecurityError):
        fleet_operation.list_operation_ids(state_root=handle.state_root)


def test_prepare_rejects_prepositioned_operations_symlink(tmp_path: Path) -> None:
    state = tmp_path / "state"
    (state / "rollouts").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (state / "rollouts" / "operations").symlink_to(outside)

    with pytest.raises(fleet_operation.OperationSecurityError):
        fleet_operation.prepare_operation(_identity(), state_root=state)
    assert list(outside.iterdir()) == []


def test_operation_path_rejects_traversal_and_noncanonical_id(tmp_path: Path) -> None:
    state = tmp_path / "state"
    for unsafe in ("../escape", "a" * 63, "A" * 64, "a" * 64 + "/child"):
        with pytest.raises(fleet_operation.OperationSecurityError):
            fleet_operation.observe_operation(unsafe, state_root=state)


def test_observation_rejects_symlink_and_oversized_receipt(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    ready = handle.directory / "ready.json"
    ready.symlink_to(handle.directory / "request.json")
    with pytest.raises(fleet_operation.OperationSecurityError):
        fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )

    ready.unlink()
    ready.write_bytes(b"{" + b" " * (fleet_operation.MAX_RECEIPT_BYTES + 1))
    ready.chmod(0o600)
    with pytest.raises(fleet_operation.OperationSecurityError):
        fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )


def test_supervisor_never_launches_without_authorization(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    launched = False

    def forbidden_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
        nonlocal launched
        launched = True
        raise AssertionError("child must not launch before authorization")

    rc = fleet_operation.run_supervisor(
        handle.operation_id,
        state_root=handle.state_root,
        authorization_timeout=0.04,
        poll_interval=0.005,
        child_popen=forbidden_popen,
    )

    assert rc == fleet_operation.SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE
    assert launched is False
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.retry_safe is True
    assert observed.lease_busy is False
    assert observed.authorization is not None
    assert observed.authorization["decision"] == "abort"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"
    assert observed.result["executed"] is False


def test_two_phase_success_writes_bound_receipts_and_output(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
        )
        ready = _wait_ready(handle)
        assert ready.state == "running-pre-authorization"
        authorization = fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=5) == 0

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.retry_safe is False
    assert observed.result is not None
    assert observed.result["status"] == "success"
    assert observed.result["returncode"] == 0
    assert observed.result["executed"] is True
    assert observed.result["retry_safe"] is False
    assert observed.ready is not None
    assert observed.activation is not None
    nonce = observed.ready["nonce"]
    assert authorization["nonce"] == nonce
    assert observed.activation["nonce"] == nonce
    assert observed.activation["launch_intent"] is True
    assert "child" not in observed.activation
    assert observed.result["nonce"] == nonce
    assert observed.result["child"]["pid"] > 0

    output = (handle.directory / "output.log").read_bytes()
    assert b"Usage:" in output
    assert observed.result["output_stored_bytes"] == len(output)
    assert observed.result["output_observed_bytes"] == len(output)
    assert observed.result["output_truncated"] is False
    assert observed.result["output_sha256"] == hashlib.sha256(output).hexdigest()
    for name in (
        "ready.json",
        "authorization.json",
        "activation.json",
        "result.json",
        "output.log",
    ):
        assert stat.S_IMODE((handle.directory / name).stat().st_mode) == 0o600

    tail = fleet_operation.read_output_tail(
        handle.operation_id,
        state_root=handle.state_root,
        max_bytes=16,
    )
    assert tail.truncated is True
    assert tail.bytes_read <= 16
    assert tail.stored_bytes == len(output)
    assert tail.total_bytes == len(output)
    assert tail.output_sha256 == hashlib.sha256(output).hexdigest()
    assert tail.text == output[-16:].decode("utf-8", "replace")


def test_output_tail_requires_terminal_verified_result(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.read_output_tail(
            handle.operation_id,
            state_root=handle.state_root,
            max_bytes=80,
        )


def test_live_output_reader_advances_a_bounded_append_only_offset(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)

    def narrated_child(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        del argv
        script = "import os,time;os.write(1,b'first\\n');time.sleep(0.15);os.write(1,b'second\\n')"
        return subprocess.Popen([sys.executable, "-c", script], **kwargs)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=narrated_child,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        deadline = time.monotonic() + 3
        first = None
        while first is None or not first.data:
            first = fleet_operation.read_output_since(
                handle.operation_id,
                first.next_offset if first is not None else 0,
                state_root=handle.state_root,
                max_bytes=5,
            )
            if time.monotonic() >= deadline:
                raise AssertionError("live narration never reached the spool")
            if not first.data:
                time.sleep(0.005)
        assert first.data == b"first"
        assert first.offset == 0
        assert first.next_offset == 5
        assert first.stored_bytes >= first.next_offset
        assert future.result(timeout=5) == 0

    second = fleet_operation.read_output_since(
        handle.operation_id,
        first.next_offset,
        state_root=handle.state_root,
        max_bytes=80,
    )
    assert first.data + second.data == b"first\nsecond\n"
    assert second.offset == first.next_offset
    assert second.next_offset == len(b"first\nsecond\n")
    assert second.at_end is True


def test_live_output_reader_rejects_invalid_or_future_offsets(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    for offset in (-1, True, fleet_operation.MAX_OUTPUT_BYTES + 1):
        with pytest.raises(ValueError):
            fleet_operation.read_output_since(
                handle.operation_id,
                offset,
                state_root=handle.state_root,
                max_bytes=1,
            )
    with pytest.raises(ValueError):
        fleet_operation.read_output_since(
            handle.operation_id,
            0,
            state_root=handle.state_root,
            max_bytes=fleet_operation.MAX_OUTPUT_TAIL_BYTES + 1,
        )
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.read_output_since(
            handle.operation_id,
            1,
            state_root=handle.state_root,
            max_bytes=1,
        )
    with pytest.raises(ValueError):
        fleet_operation.read_output_tail(
            handle.operation_id,
            state_root=handle.state_root,
            max_bytes=fleet_operation.MAX_OUTPUT_TAIL_BYTES + 1,
        )


def test_executed_nonzero_is_terminal_and_never_retry_safe(tmp_path: Path) -> None:
    handle = _prepare(tmp_path, argv=("definitely-not-a-vq-command",))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=LIVENESS_SECONDS,
            poll_interval=0.005,
        )
        _wait_ready(handle, timeout=LIVENESS_SECONDS)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=LIVENESS_SECONDS) != 0

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "failed"
    assert observed.result["returncode"] != 0
    assert observed.result["executed"] is True
    assert observed.result["retry_safe"] is False
    assert observed.retry_safe is False


def test_child_output_is_continuously_drained_into_a_bounded_spool(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    emitted = fleet_operation.MAX_OUTPUT_BYTES + 131_071

    def noisy_child(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        del argv
        script = f"import os; os.write(1, b'x' * {emitted})"
        return subprocess.Popen([sys.executable, "-c", script], **kwargs)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=noisy_child,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=10) == 0

    output = (handle.directory / "output.log").read_bytes()
    assert len(output) == fleet_operation.MAX_OUTPUT_BYTES
    assert output == b"x" * fleet_operation.MAX_OUTPUT_BYTES
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.result is not None
    assert observed.result["output_stored_bytes"] == fleet_operation.MAX_OUTPUT_BYTES
    assert observed.result["output_observed_bytes"] == emitted
    assert observed.result["output_truncated"] is True
    assert "admin logs" in observed.result["output_truncation_guidance"]
    tail = fleet_operation.read_output_tail(
        handle.operation_id,
        state_root=handle.state_root,
        max_bytes=64,
    )
    assert tail.bytes_read == 64
    assert tail.stored_bytes == fleet_operation.MAX_OUTPUT_BYTES
    assert tail.total_bytes == emitted
    assert tail.truncated is True


def test_only_one_supervisor_can_hold_the_lifetime_lease(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    release = threading.Event()
    entered = threading.Event()

    def blocking_child(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        entered.set()
        assert release.wait(3)
        return subprocess.Popen(argv, **kwargs)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=blocking_child,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert entered.wait(3)
        second = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=0.1,
        )
        with pytest.raises(fleet_operation.OperationBusyError):
            second.result(timeout=3)
        release.set()
        assert first.result(timeout=5) == 0


def test_launch_intent_is_durable_before_child_popen(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)

    def asserting_child(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        activation = json.loads((handle.directory / "activation.json").read_text())
        assert activation["launch_intent"] is True
        assert activation["request_sha256"] == handle.request_sha256
        assert "child" not in activation
        assert "start_new_session" not in kwargs
        return subprocess.Popen(argv, **kwargs)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=asserting_child,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=5) == 0


def test_activated_child_receives_only_validated_outer_operation_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)
    for name in (
        fleet_operation.ENV_OPERATION_ID,
        fleet_operation.ENV_OPERATION_REQUEST_SHA256,
        fleet_operation.ENV_OPERATION_NONCE,
    ):
        monkeypatch.setenv(name, "f" * 64)
    monkeypatch.setenv("VQ_FLEET_OPERATION_UNRECOGNIZED", "drop-me")
    monkeypatch.setenv("VQ_UNRELATED", "preserve-me")
    captured: dict[str, str] = {}

    def capture_child(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        captured.update(environment)
        return subprocess.Popen(argv, **kwargs)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=capture_child,
        )
        ready = _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=5) == 0

    assert captured[fleet_operation.ENV_OPERATION_ID] == handle.operation_id
    assert captured[fleet_operation.ENV_OPERATION_REQUEST_SHA256] == (
        handle.request_sha256
    )
    assert captured[fleet_operation.ENV_OPERATION_NONCE] == ready.ready["nonce"]
    assert "VQ_FLEET_OPERATION_UNRECOGNIZED" not in captured
    assert captured["VQ_UNRELATED"] == "preserve-me"


def test_detached_scheduler_binding_is_private_create_once_and_exact(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    context = _activate_operation_for_scheduler_binding(handle)
    expected = fleet_operation.DetachedSchedulerCommandBinding(
        operation_id=handle.operation_id,
        request_sha256=handle.request_sha256,
        nonce=context.nonce,
        target="cluster-build",
        program="vibeqc-release",
        mode="runtime vibeqc-release update",
        protocol=fleet_operation.DETACHED_SCHEDULER_PROTOCOL,
        run_id="e" * 64,
        bound_at="2026-08-10T12:00:00+00:00",
        remote_run_dir=(
            f"/scratch/.vq-admin/detached/run-{'e' * 64}"
        ),
        command_sha256="e" * 64,
        remote_request_sha256="f" * 64,
    )

    with _hold_operation_lease(handle):
        binding, created = fleet_operation.bind_detached_scheduler_command(
            context,
            target=expected.target,
            program=expected.program,
            mode=expected.mode,
            protocol=expected.protocol,
            run_id=expected.run_id,
            bound_at=expected.bound_at,
            remote_run_dir=expected.remote_run_dir,
            command_sha256=expected.command_sha256,
            remote_request_sha256=expected.remote_request_sha256,
            state_root=handle.state_root,
        )

    assert created is True
    assert binding == expected
    receipt_path = handle.directory / "scheduler-command.json"
    assert stat.S_IMODE(receipt_path.stat().st_mode) == 0o600
    before = receipt_path.read_bytes()
    assert json.loads(before) == expected.as_dict()
    assert fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    ).activation is not None
    assert fleet_operation.read_detached_scheduler_binding(
        handle.operation_id,
        state_root=handle.state_root,
    ) == expected

    repeated, created = fleet_operation.bind_detached_scheduler_command(
        context,
        target=expected.target,
        program=expected.program,
        mode=expected.mode,
        protocol=expected.protocol,
        run_id=expected.run_id,
        bound_at=expected.bound_at,
        remote_run_dir=expected.remote_run_dir,
        command_sha256=expected.command_sha256,
        remote_request_sha256=expected.remote_request_sha256,
        state_root=handle.state_root,
    )
    assert repeated == expected
    assert created is False
    assert receipt_path.read_bytes() == before

    fleet_operation._write_operation_receipt(handle, "result.json", {})
    assert fleet_operation.read_detached_scheduler_binding(
        handle.operation_id,
        state_root=handle.state_root,
    ) == expected

    with pytest.raises(fleet_operation.OperationStateError, match="binding"):
        fleet_operation.bind_detached_scheduler_command(
            context,
            target="different-build-host",
            program=expected.program,
            mode=expected.mode,
            protocol=expected.protocol,
            run_id=expected.run_id,
            bound_at=expected.bound_at,
            remote_run_dir=expected.remote_run_dir,
            command_sha256=expected.command_sha256,
            remote_request_sha256=expected.remote_request_sha256,
            state_root=handle.state_root,
        )
    assert receipt_path.read_bytes() == before


def test_detached_scheduler_binding_requires_live_unfinished_outer_operation(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    context = _activate_operation_for_scheduler_binding(handle)
    kwargs = {
        "target": "cluster-build",
        "program": "vibeqc-release",
        "mode": "runtime vibeqc-release update",
        "protocol": fleet_operation.DETACHED_SCHEDULER_PROTOCOL,
        "run_id": "f" * 64,
        "bound_at": "2026-08-10T12:00:00+00:00",
        "remote_run_dir": f"/scratch/.vq-admin/detached/run-{'f' * 64}",
        "command_sha256": "f" * 64,
        "remote_request_sha256": "e" * 64,
        "state_root": handle.state_root,
    }

    with pytest.raises(fleet_operation.OperationStateError, match="lifetime lease"):
        fleet_operation.bind_detached_scheduler_command(context, **kwargs)

    with _hold_operation_lease(handle):
        assert fleet_operation.validate_live_execution_context(
            context,
            state_root=handle.state_root,
        ) == handle.identity

    fleet_operation._write_operation_receipt(handle, "result.json", {})
    with _hold_operation_lease(handle):
        with pytest.raises(fleet_operation.OperationStateError, match="terminal"):
            fleet_operation.validate_live_execution_context(
                context,
                state_root=handle.state_root,
            )
        with pytest.raises(fleet_operation.OperationStateError, match="terminal"):
            fleet_operation.bind_detached_scheduler_command(context, **kwargs)


@pytest.mark.parametrize("field", ["operation_id", "request_sha256", "nonce"])
def test_detached_scheduler_binding_rejects_outer_identity_mismatch(
    tmp_path: Path,
    field: str,
) -> None:
    handle = _prepare(tmp_path)
    context = _activate_operation_for_scheduler_binding(handle)
    bad = fleet_operation.OperationExecutionContext(
        operation_id=("e" * 64 if field == "operation_id" else context.operation_id),
        request_sha256=(
            "e" * 64 if field == "request_sha256" else context.request_sha256
        ),
        nonce=("e" * 64 if field == "nonce" else context.nonce),
    )

    with _hold_operation_lease(handle), pytest.raises(
        fleet_operation.OperationStateError
    ):
        fleet_operation.bind_detached_scheduler_command(
            bad,
            target="cluster-build",
            program="vibeqc-release",
            mode="runtime vibeqc-release update",
            protocol=fleet_operation.DETACHED_SCHEDULER_PROTOCOL,
            run_id="f" * 64,
            bound_at="2026-08-10T12:00:00+00:00",
            remote_run_dir=(
                f"/scratch/.vq-admin/detached/run-{'f' * 64}"
            ),
            command_sha256="f" * 64,
            remote_request_sha256="e" * 64,
            state_root=handle.state_root,
        )
    assert not (handle.directory / "scheduler-command.json").exists()


def test_recorder_survives_supervisor_death_after_activation(
    tmp_path: Path,
) -> None:
    """The terminal writer, output drain, and lease outlive the launcher."""
    jobid = "recordwait001"
    handle = _prepare(
        tmp_path,
        argv=(
            "wait",
            "localhost",
            jobid,
            "--poll-interval",
            "0.1",
            "--timeout",
            "5",
        ),
    )
    spec_path = _write_running_wait_spec(handle, jobid)
    supervisor = fleet_operation.launch_supervisor(
        handle.operation_id,
        state_root=handle.state_root,
    )
    _wait_ready(handle, timeout=5)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=REPORT_DIGEST,
        state_root=handle.state_root,
    )
    _wait_for_activation(handle)

    supervisor.kill()
    supervisor.wait(timeout=5)
    after_death = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert after_death.state == "running-authorized"
    assert after_death.lease_busy is True

    from vq.spec import JobSpec, JobState

    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.exit_code = 0
    spec.write(spec_path)
    deadline = time.monotonic() + 5
    while True:
        observed = fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )
        if observed.state == "completed":
            break
        if time.monotonic() >= deadline:
            raise AssertionError("surviving recorder did not publish a result")
        time.sleep(0.01)
    assert observed.result is not None
    assert observed.result["status"] == "success"
    assert observed.result["returncode"] == 0
    assert observed.result["executed"] is True


def test_supervisor_death_before_recorder_activation_resumes_same_operation(
    tmp_path: Path,
) -> None:
    """Authorization without launch intent is a resumable handoff, not replay."""
    handle = _prepare(tmp_path)
    recorder_popen_entered = tmp_path / "recorder-popen-entered"

    def blocked_recorder_popen(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        del argv, kwargs
        recorder_popen_entered.write_text("entered\n")
        time.sleep(30)
        raise AssertionError("killed supervisor unexpectedly resumed")

    supervisor_pid = os.fork()
    if supervisor_pid == 0:  # pragma: no cover - assertions run in the parent
        try:
            fleet_operation.run_supervisor(
                handle.operation_id,
                state_root=handle.state_root,
                authorization_timeout=5,
                poll_interval=0.005,
                recorder_popen=blocked_recorder_popen,
            )
        except BaseException:
            os._exit(91)
        os._exit(0)
    try:
        _wait_ready(handle, timeout=5)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        deadline = time.monotonic() + 5
        while not recorder_popen_entered.exists():
            if time.monotonic() >= deadline:
                raise AssertionError("supervisor never reached recorder Popen")
            time.sleep(0.01)
        os.kill(supervisor_pid, signal.SIGKILL)
        waited, status = os.waitpid(supervisor_pid, 0)
        assert waited == supervisor_pid
        assert os.WIFSIGNALED(status)
    finally:
        with contextlib.suppress(ProcessLookupError, ChildProcessError):
            os.kill(supervisor_pid, signal.SIGKILL)
        with contextlib.suppress(ChildProcessError):
            os.waitpid(supervisor_pid, 0)

    before = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert before.state == "authorized-unactivated"
    assert before.retry_safe is False
    assert before.lease_busy is False
    assert before.activation is None
    ready_bytes = (handle.directory / "ready.json").read_bytes()
    authorization_bytes = (handle.directory / "authorization.json").read_bytes()

    assert fleet_operation.run_supervisor(
        handle.operation_id,
        state_root=handle.state_root,
        authorization_timeout=0,
        poll_interval=0.005,
    ) == 0
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["returncode"] == 0
    assert (handle.directory / "ready.json").read_bytes() == ready_bytes
    assert (handle.directory / "authorization.json").read_bytes() == authorization_bytes


def test_recorder_launch_exception_after_fd_inherit_preserves_lease(
    tmp_path: Path,
) -> None:
    """Launch-entry cleanup must not unlock an already-inherited lease fd."""
    handle = _prepare(tmp_path)
    inherited_children: list[subprocess.Popen[bytes]] = []

    def raise_after_inherit(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        del argv
        # Popen returns only after the child has exec'd successfully, so this
        # sleeper deterministically owns the pass_fds copy before we inject
        # the asynchronous launch failure seen by the supervisor.
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            **kwargs,
        )
        inherited_children.append(child)
        raise RuntimeError("injected after recorder inherited lease")

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                fleet_operation.run_supervisor,
                handle.operation_id,
                state_root=handle.state_root,
                authorization_timeout=3.0,
                poll_interval=0.005,
                recorder_popen=raise_after_inherit,
            )
            _wait_ready(handle)
            fleet_operation.authorize_operation(
                handle.operation_id,
                expected_report_digest_sha256=REPORT_DIGEST,
                state_root=handle.state_root,
            )
            with pytest.raises(RuntimeError, match="injected after recorder inherited"):
                future.result(timeout=5)

        assert len(inherited_children) == 1
        assert inherited_children[0].poll() is None
        observed = fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )
        assert observed.state == "running-authorized"
        assert observed.lease_busy is True
        assert observed.activation is None
        assert observed.result is None
        with pytest.raises(fleet_operation.OperationBusyError):
            fleet_operation.run_supervisor(
                handle.operation_id,
                state_root=handle.state_root,
                authorization_timeout=0,
                poll_interval=0.005,
            )
    finally:
        for child in inherited_children:
            with contextlib.suppress(ProcessLookupError):
                child.kill()
            child.wait(timeout=5)


def test_recorder_death_after_activation_remains_outcome_unknown(
    tmp_path: Path,
) -> None:
    """The new handoff closes parent death, never executor death or replay."""
    jobid = "recordwait002"
    handle = _prepare(
        tmp_path,
        argv=(
            "wait",
            "localhost",
            jobid,
            "--poll-interval",
            "0.1",
            "--timeout",
            "5",
        ),
    )
    _write_running_wait_spec(handle, jobid)
    supervisor = fleet_operation.launch_supervisor(
        handle.operation_id,
        state_root=handle.state_root,
    )
    _wait_ready(handle, timeout=5)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=REPORT_DIGEST,
        state_root=handle.state_root,
    )
    _wait_for_activation(handle)
    deadline = time.monotonic() + 5
    recorder_pids: list[int] = []
    while not recorder_pids:
        recorder_pids = _child_pids(supervisor.pid)
        if time.monotonic() >= deadline:
            raise AssertionError("supervisor never launched its recorder")
        if not recorder_pids:
            time.sleep(0.01)
    assert len(recorder_pids) == 1
    recorder_pid = recorder_pids[0]
    assert os.getpgid(recorder_pid) == recorder_pid

    os.killpg(recorder_pid, signal.SIGKILL)
    supervisor.wait(timeout=5)
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "outcome-unknown"
    assert observed.retry_safe is False
    assert observed.lease_busy is False
    assert observed.result is None
    with pytest.raises(
        fleet_operation.OperationStateError,
        match="committed decision",
    ):
        fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=0,
            poll_interval=0.005,
        )


@pytest.mark.parametrize("failure", [OSError("popen failed"), RuntimeError("factory failed")])
def test_any_child_popen_exception_after_intent_is_outcome_unknown(
    tmp_path: Path,
    failure: Exception,
) -> None:
    handle = _prepare(tmp_path)

    def failing_child(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
        del args, kwargs
        raise failure

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=failing_child,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        with pytest.raises(type(failure), match="failed"):
            future.result(timeout=5)

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "outcome-unknown"
    assert observed.retry_safe is False
    assert observed.activation is not None
    assert observed.result is None


def test_timeout_and_authorization_cannot_both_commit(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=0.04,
            poll_interval=0.005,
        )
        _wait_ready(handle)
        assert future.result(timeout=3) == (fleet_operation.SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE)
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
    decision = json.loads((handle.directory / "authorization.json").read_text())
    assert decision["decision"] == "abort"
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"


def test_abort_prepared_operation_is_terminal_and_nonlaunchable(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    decision = fleet_operation.abort_operation(
        handle.operation_id,
        state_root=handle.state_root,
        expected_request_sha256=handle.request_sha256,
    )
    assert decision["decision"] == "abort"

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.retry_safe is True
    assert observed.result is not None
    assert observed.result["status"] == "aborted"
    assert observed.result["executed"] is False
    assert observed.result["retry_safe"] is True
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=0.1,
        )
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )


def test_live_supervisor_obeys_abort_without_launching_child(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    launched = False

    def forbidden_child(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
        nonlocal launched
        launched = True
        raise AssertionError("aborted operation must not launch")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
            child_popen=forbidden_child,
        )
        _wait_ready(handle)
        fleet_operation.abort_operation(
            handle.operation_id,
            state_root=handle.state_root,
            expected_request_sha256=handle.request_sha256,
        )
        assert future.result(timeout=5) == (fleet_operation.SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE)
    assert launched is False
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"


def test_abort_racing_pre_ready_publication_binds_the_ready_nonce(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)
    ready_write_entered = threading.Event()
    allow_ready_write = threading.Event()
    original = fleet_operation._write_receipt_at

    def paused_write(
        directory_fd: int,
        name: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        if name == "ready.json":
            ready_write_entered.set()
            assert allow_ready_write.wait(3)
        return original(directory_fd, name, payload)

    monkeypatch.setattr(fleet_operation, "_write_receipt_at", paused_write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        supervisor = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
        )
        assert ready_write_entered.wait(3)
        abort = pool.submit(
            fleet_operation.abort_operation,
            handle.operation_id,
            state_root=handle.state_root,
            expected_request_sha256=handle.request_sha256,
        )
        time.sleep(0.03)
        assert abort.done() is False
        allow_ready_write.set()
        decision = abort.result(timeout=5)
        assert supervisor.result(timeout=5) == (fleet_operation.SUPERVISOR_NOT_AUTHORIZED_EXIT_CODE)

    ready = json.loads((handle.directory / "ready.json").read_text())
    assert decision["decision"] == "abort"
    assert decision["nonce"] == ready["nonce"]
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"


def test_run_authorization_and_abort_race_has_exactly_one_winner(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    barrier = threading.Barrier(2)

    def authorize() -> tuple[str, object]:
        barrier.wait()
        try:
            return "run", fleet_operation.authorize_operation(
                handle.operation_id,
                expected_report_digest_sha256=REPORT_DIGEST,
                state_root=handle.state_root,
            )
        except fleet_operation.OperationStateError as exc:
            return "run-error", exc

    def abort() -> tuple[str, object]:
        barrier.wait()
        try:
            return "abort", fleet_operation.abort_operation(
                handle.operation_id,
                state_root=handle.state_root,
                expected_request_sha256=handle.request_sha256,
            )
        except fleet_operation.OperationStateError as exc:
            return "abort-error", exc

    with ThreadPoolExecutor(max_workers=3) as pool:
        supervisor = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
        )
        _wait_ready(handle)
        contenders = [pool.submit(authorize), pool.submit(abort)]
        outcomes = [future.result(timeout=5)[0] for future in contenders]
        supervisor.result(timeout=5)

    assert sorted(outcomes) in (["abort", "run-error"], ["abort-error", "run"])
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.authorization is not None
    assert observed.authorization["decision"] in {"run", "abort"}


def test_authorization_requires_live_lease_nonce_and_report(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
        )
        _wait_ready(handle)
        with pytest.raises(fleet_operation.OperationStateError):
            fleet_operation.authorize_operation(
                handle.operation_id,
                expected_report_digest_sha256="c" * 64,
                state_root=handle.state_root,
            )
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=5) == 0


@pytest.mark.parametrize(
    "invalid",
    [None, True, "1", math.nan, math.inf, -math.inf],
)
def test_public_wait_and_supervisor_timing_requires_finite_exact_numeric(
    tmp_path: Path,
    invalid: object,
) -> None:
    handle = _prepare(tmp_path)
    with pytest.raises(ValueError):
        fleet_operation.wait_for_ready(
            handle.operation_id,
            state_root=handle.state_root,
            timeout=invalid,  # type: ignore[arg-type]
            poll_interval=0.01,
        )
    with pytest.raises(ValueError):
        fleet_operation.wait_for_ready(
            handle.operation_id,
            state_root=handle.state_root,
            timeout=0.01,
            poll_interval=invalid,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError):
        fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=invalid,  # type: ignore[arg-type]
            poll_interval=0.01,
        )
    with pytest.raises(ValueError):
        fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=0.01,
            poll_interval=invalid,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("invalid", [None, 1, True, b"a" * 64])
def test_public_digest_parameters_reject_non_strings(
    tmp_path: Path,
    invalid: object,
) -> None:
    handle = _prepare(tmp_path)
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=invalid,  # type: ignore[arg-type]
            state_root=handle.state_root,
        )
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.abort_operation(
            handle.operation_id,
            expected_request_sha256=invalid,  # type: ignore[arg-type]
            state_root=handle.state_root,
        )


def test_authorized_operation_without_activation_is_same_operation_resumable(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    output = handle.directory / "output.log"
    output.write_bytes(b"")
    output.chmod(0o600)
    ready_payload = fleet_operation._ready_payload(handle, nonce="d" * 64)
    fleet_operation._write_operation_receipt(handle, "ready.json", ready_payload)
    authorization = fleet_operation._authorization_payload(
        handle,
        nonce="d" * 64,
    )
    fleet_operation._write_operation_receipt(
        handle,
        "authorization.json",
        authorization,
    )

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "authorized-unactivated"
    assert observed.retry_safe is False


def test_authorized_unactivated_resume_carries_bound_lifecycle_descriptor(
    tmp_path: Path,
) -> None:
    resource = ("checkout", str(tmp_path / "repo"))
    handle = _prepare(tmp_path, lifecycle_resources=(resource,))
    output = handle.directory / "output.log"
    output.write_bytes(b"")
    output.chmod(0o600)
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

    lock_path = tmp_path / "controller-lifecycle.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        handoff = json.dumps(
            {
                "schema": "vq.toolset.lifecycle_handoff/1",
                "locks": [
                    {
                        "scope": resource[0],
                        "resource": resource[1],
                        "fd": lock_fd,
                        "path": str(lock_path),
                    }
                ],
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        rc = fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
            lifecycle_handoff=handoff,
        )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

    assert rc == 0
    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "completed"
    assert observed.activation is not None
    assert observed.result is not None
    assert observed.result["status"] == "success"


def test_missing_lifecycle_handoff_fails_before_launch_intent(
    tmp_path: Path,
) -> None:
    resource = ("checkout", str(tmp_path / "repo"))
    handle = _prepare(tmp_path, lifecycle_resources=(resource,))
    output = handle.directory / "output.log"
    output.write_bytes(b"")
    output.chmod(0o600)
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

    with pytest.raises(
        fleet_operation.OperationSecurityError,
        match="requires a lifecycle handoff",
    ):
        fleet_operation.run_supervisor(
            handle.operation_id,
            state_root=handle.state_root,
        )

    observed = fleet_operation.observe_operation(
        handle.operation_id,
        state_root=handle.state_root,
    )
    assert observed.state == "authorized-unactivated"
    assert observed.activation is None
    assert observed.result is None
    assert observed.lease_busy is False


def test_result_output_digest_corruption_fails_closed(tmp_path: Path) -> None:
    handle = _prepare(tmp_path)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            fleet_operation.run_supervisor,
            handle.operation_id,
            state_root=handle.state_root,
            authorization_timeout=3.0,
            poll_interval=0.005,
        )
        _wait_ready(handle)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=REPORT_DIGEST,
            state_root=handle.state_root,
        )
        assert future.result(timeout=5) == 0

    with (handle.directory / "output.log").open("ab") as stream:
        stream.write(b"tampered\n")
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )


@pytest.mark.parametrize(
    "invalid_json",
    [
        '{"schema":"x","schema":"y"}\n',
        '{"schema":"x","value":NaN}\n',
    ],
)
def test_receipt_parser_rejects_duplicate_keys_and_nonfinite_numbers(
    tmp_path: Path,
    invalid_json: str,
) -> None:
    handle = _prepare(tmp_path)
    ready = handle.directory / "ready.json"
    ready.write_text(invalid_json)
    ready.chmod(0o600)
    with pytest.raises(fleet_operation.OperationStateError):
        fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )


def test_receipt_fsync_failure_is_not_suppressed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)

    def fail_fsync(fd: int) -> None:
        del fd
        raise OSError("injected fsync failure")

    monkeypatch.setattr(fleet_operation.os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        fleet_operation._write_operation_receipt(
            handle,
            "ready.json",
            fleet_operation._ready_payload(handle, nonce="f" * 64),
        )
    assert not (handle.directory / "ready.json").exists()


def test_observe_busy_lease_is_authoritative_even_with_unavailable_pid(
    tmp_path: Path,
) -> None:
    handle = _prepare(tmp_path)
    lease_fd = os.open(handle.directory / "lease.lock", os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed = fleet_operation.observe_operation(
            handle.operation_id,
            state_root=handle.state_root,
        )
        assert observed.state == "starting"
        assert observed.lease_busy is True
        assert observed.retry_safe is False
    finally:
        fcntl.flock(lease_fd, fcntl.LOCK_UN)
        os.close(lease_fd)


def test_launch_supervisor_detaches_and_does_not_inherit_fds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)
    monkeypatch.setenv(fleet_operation.ENV_OPERATION_ID, "f" * 64)
    monkeypatch.setenv("VQ_FLEET_OPERATION_SPOOF", "must-be-removed")
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_popen(argv: list[str], **kwargs: object) -> object:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(fleet_operation.subprocess, "Popen", fake_popen)
    result = fleet_operation.launch_supervisor(
        handle.operation_id,
        state_root=handle.state_root,
    )

    assert result is sentinel
    assert captured["argv"][-3:] == [
        "-m",
        "vq.fleet_operation",
        handle.operation_id,
    ]
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["start_new_session"] is True
    assert kwargs["close_fds"] is True
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["stdout"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.DEVNULL
    env = kwargs["env"]
    assert isinstance(env, dict)
    assert env["VQ_STATE_DIR"] == str(handle.state_root)
    assert not any(name.startswith("VQ_FLEET_OPERATION_") for name in env)


def test_recorder_launcher_scrubs_reserved_operation_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _prepare(tmp_path)
    monkeypatch.setenv(fleet_operation.ENV_OPERATION_NONCE, "f" * 64)
    monkeypatch.setenv("VQ_FLEET_OPERATION_SPOOF", "must-be-removed")
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_popen(argv: list[str], **kwargs: object) -> object:
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return sentinel

    lease_fd = os.open(handle.directory / "lease.lock", os.O_RDWR)
    try:
        result = fleet_operation._launch_recorder(
            handle,
            lease_fd,
            recorder_popen=fake_popen,
        )
    finally:
        os.close(lease_fd)

    assert result is sentinel
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    env = kwargs["env"]
    assert isinstance(env, dict)
    assert env["VQ_STATE_DIR"] == str(handle.state_root)
    assert not any(name.startswith("VQ_FLEET_OPERATION_") for name in env)


def test_main_rejects_arbitrary_paths_without_touching_them(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.write_text("unchanged")
    assert fleet_operation.main([str(outside)]) == (
        fleet_operation.SUPERVISOR_INVALID_REQUEST_EXIT_CODE
    )
    assert outside.read_text() == "unchanged"

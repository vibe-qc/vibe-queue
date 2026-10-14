"""Bound exact proof lock admission without losing recovery authority (#70)."""
from __future__ import annotations

import fcntl
import gc
import os
import sys
import threading
import time
from pathlib import Path

import pytest

from vq import admin, config, paths, pause_resume
from vq.spec import JobSpec, JobState

TOKEN = "admin-update-0123456789ab"


@pytest.fixture
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    (tmp_path / "config").mkdir()
    paths.queue_dir().mkdir(parents=True)
    # Parent code ignores this limit, so the baseline fails on behavior.
    monkeypatch.setattr(pause_resume, "PROOF_LOCK_TIMEOUT_SECONDS", 0.1, raising=False)
    return paths.queue_dir()


def _job(queue: Path, jobid: str, **kwargs: object) -> Path:
    work = queue.parent / "jobs" / jobid
    work.mkdir(parents=True)
    spec = JobSpec(
        id=jobid, command=["true"], cwd=str(work), cpus=1,
        state=kwargs.pop("state", JobState.KILLED), **kwargs,
    )
    target = queue / f"{jobid}.json"
    spec.write(target)
    return target


def _hold(path: Path) -> int:
    fd = os.open(paths.spec_lock_path(path), os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def _release(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


@pytest.mark.parametrize("operation", ["pause", "resume", "absence", "reconcile"])
def test_contended_records_share_one_proof_deadline(
    queue: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    records = [_job(queue, f"terminal-{index:02d}") for index in range(64)]
    originals = [path.read_bytes() for path in records]
    holders = [_hold(path) for path in records]
    clock = [0.0]
    waits = []
    real_flock = fcntl.flock

    def wait(seconds: float) -> None:
        waits.append(seconds)
        clock[0] += seconds

    def guard(fd: int, flag: int) -> None:
        if flag == fcntl.LOCK_EX:
            try:
                real_flock(fd, flag | fcntl.LOCK_NB)
            except BlockingIOError:
                raise _UnboundedCleanup("proof attempted an unbounded held lock") from None
        else:
            real_flock(fd, flag)

    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", wait)
    monkeypatch.setattr(fcntl, "flock", guard)
    functions = {
        "pause": pause_resume.pause_token_scope_with_proof,
        "resume": pause_resume.resume_token_scope_with_proof,
        "absence": pause_resume.prove_pause_token_absent,
        "reconcile": pause_resume.reconcile_pause_intents,
    }
    args = ("localhost",) if operation == "reconcile" else ("localhost", TOKEN)
    try:
        proof = functions[operation](*args, queue_dir=queue)
    finally:
        for fd in holders:
            _release(fd)
    # Advance a synthetic monotonic clock only at real failed kernel lock
    # attempts. A per-row/per-pass budget would multiply this exact total.
    assert waits
    assert sum(waits) <= 0.1 + 1e-9
    unresolved = proof.errors if operation == "reconcile" else proof.unresolved
    assert {row[0] for row in unresolved} == {path.stem for path in records}
    assert all("lock" in row[1] for row in unresolved)
    assert [path.read_bytes() for path in records] == originals
    pause_resume.prove_pause_token_absent("localhost", TOKEN, queue_dir=queue).require_clear()


def test_real_contended_cleanup_returns_before_holder_release(queue: Path) -> None:
    target = _job(queue, "held-terminal")
    holder = _hold(target)
    started = threading.Event()
    finished = threading.Event()
    values = []
    errors = []

    def run() -> None:
        started.set()
        try:
            values.append(pause_resume.resume_token_scope_with_proof(
                "localhost", TOKEN, queue_dir=queue,
            ))
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert started.wait(10), "worker did not start"
        # Liveness allowance, not an elapsed-time performance assertion.
        completed_while_held = finished.wait(10)
    finally:
        _release(holder)
        worker.join(10)
    assert not worker.is_alive()
    assert not errors
    assert completed_while_held, "cleanup required the holder to release first"
    assert values[0].unresolved[0][0] == target.stem


class _UnboundedCleanup(RuntimeError):
    """Diagnostic guard: fail the old blocking acquire instead of hanging pytest."""


def _run_update(prog: config.VenvProgram, *, batch: bool = False) -> None:
    if batch:
        admin._update_all_owned(
            config.Config(programs={"fixture-env": prog}), host="localhost",
            restart_daemon=False, resolved_progs=[("fixture-env", prog)],
        )
        return
    admin._update_env_logged(
        "fixture-env", config.Config(programs={"fixture-env": prog}),
        prog=prog, multi_user=False, host="localhost", admin_token=None,
        expected_tag=None, expected_sha=None, force=False, restart_daemon=False,
        require_self_update=False, managed_daemon_restart=False,
        initial_self_update_probe=None, update_script_args=None,
    )


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("boundary", ["spec_lock", "_locked_authorized_spec"])
def test_interrupted_entry_cleanup_retains_marker_and_allows_recovery(
    queue: Path, monkeypatch: pytest.MonkeyPatch, boundary: str, batch: bool,
) -> None:
    target = _job(queue, "terminal", scheduler_target="fixture-scheduler")
    original = target.read_bytes()
    prog = config.VenvProgram(kind="venv", python="/unused/python", git_dir=str(queue.parent))
    monkeypatch.setattr(admin, "_new_pause_token", lambda: TOKEN)
    monkeypatch.setattr(
        admin, "_detect_vq_self_update",
        lambda prog: admin._SelfUpdateProbe(False, False, None, True, "fixture"),
    )
    fired = False
    unbounded = False
    real_flock = fcntl.flock

    def guard(fd: int, operation: int) -> None:
        nonlocal unbounded
        if fired and operation == fcntl.LOCK_EX:
            try:
                real_flock(fd, operation | fcntl.LOCK_NB)
            except BlockingIOError:
                unbounded = True
                raise _UnboundedCleanup("cleanup attempted an unbounded held lock") from None
        else:
            real_flock(fd, operation)

    def trace(frame: object, event: str, arg: object):
        nonlocal fired
        if event == "return" and frame.f_code.co_name == "__enter__":
            manager = frame.f_locals.get("self")
            generator = getattr(manager, "gen", None)
            if generator is not None and generator.gi_code.co_name == boundary and not fired:
                fired = True
                raise KeyboardInterrupt("fixture entry interruption")
        return trace

    monkeypatch.setattr(fcntl, "flock", guard)
    error_type = None
    cause_type = None
    try:
        sys.settrace(trace)
        _run_update(prog, batch=batch)
    except BaseException as exc:
        error_type = type(exc)
        cause_type = type(exc.__cause__)
    finally:
        sys.settrace(None)
    assert fired
    assert not unbounded, "cleanup reacquired its own retained lock without a deadline"
    assert error_type is pause_resume.PauseError
    assert cause_type is KeyboardInterrupt
    marker = admin.read_admin_update_marker()
    assert marker is not None and marker.pause_token == TOKEN
    assert marker.state == admin.ADMIN_UPDATE_STATE_FAILED
    assert "KeyboardInterrupt" in marker.failure_reason
    assert "not durably proven" in marker.failure_reason
    assert target.read_bytes() == original
    # The original exception can now unwind and release the retained generator.
    gc.collect()
    _, summary = admin.recover_pause_scope_and_clear_marker(marker)
    assert "scope is clear" in summary
    assert not admin.admin_update_marker_exists()


def _pause_marker() -> admin.AdminUpdateMarker:
    admin.acquire_admin_update_marker(envs=["fixture-env"], host="localhost")
    admin._record_admin_update_pause_scope(
        pause_token=TOKEN, paused_jobids=[], surgical=False, multi_user=False,
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    return marker


def test_partial_token_recovery_never_resumes_other_operator(
    queue: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _job(queue, "a-own", state=JobState.SUSPENDED, paused_by=TOKEN, pgid=900001)
    other = _job(queue, "b-other", state=JobState.SUSPENDED, paused_by="operator", pgid=900002)
    held = _job(queue, "z-own", state=JobState.SUSPENDED, paused_by=TOKEN, pgid=900003)
    original_other = other.read_bytes()
    signals = []
    monkeypatch.setattr(
        pause_resume.process_group, "signal_process_group",
        lambda *args: signals.append(args),
    )
    marker = _pause_marker()
    fd = _hold(held)
    try:
        with pytest.raises(pause_resume.PauseError, match="not durably proven"):
            admin.recover_pause_scope_and_clear_marker(marker)
        retained = admin.read_admin_update_marker()
        assert retained is not None and retained.pause_token == TOKEN
        assert JobSpec.read(first).paused_by is None
        assert JobSpec.read(held).paused_by == TOKEN
        assert other.read_bytes() == original_other
        assert [pgid for pgid, _ in signals] == [900001]
    finally:
        _release(fd)
    admin.recover_pause_scope_and_clear_marker(marker)
    assert not admin.admin_update_marker_exists()
    assert JobSpec.read(held).paused_by is None
    assert other.read_bytes() == original_other
    assert [pgid for pgid, _ in signals] == [900001, 900003]


@pytest.mark.parametrize("failure", ["corrupt", "denied", "terminal-token", "intent"])
def test_unproven_rows_keep_exact_recovery_authority(
    queue: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    target = _job(queue, "unresolved")
    if failure == "corrupt":
        target.write_text("{")
    elif failure == "denied":
        def deny(*args: object, **kwargs: object) -> None:
            raise pause_resume.ownership.OwnershipError("fixture ownership denied")
        monkeypatch.setattr(pause_resume.ownership, "check_owner", deny)
    else:
        spec = JobSpec.read(target)
        if failure == "terminal-token":
            spec.paused_by = TOKEN
        else:
            spec.pause_intent_by = TOKEN
            spec.pause_intent_at = "2026-01-01T00:00:00+00:00"
            spec.pause_intent_pgid = 900010
            # Missing/mismatching current pgid makes reconciliation unsafe.
        spec.write(target)
    original = target.read_bytes()
    marker = _pause_marker()
    with pytest.raises(pause_resume.PauseError, match="not durably proven"):
        admin.recover_pause_scope_and_clear_marker(marker)
    retained = admin.read_admin_update_marker()
    assert retained is not None and retained.pause_token == TOKEN
    assert target.read_bytes() == original


def test_normal_pause_and_resume_preserve_exact_scope(
    queue: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _job(queue, "running", state=JobState.RUNNING, pgid=900020)
    other = _job(queue, "other", state=JobState.SUSPENDED, paused_by="operator", pgid=900021)
    original_other = other.read_bytes()
    signals = []
    monkeypatch.setattr(
        pause_resume.process_group, "signal_process_group",
        lambda *args: signals.append(args),
    )
    pause_resume.pause_token_scope_with_proof(
        "localhost", TOKEN, queue_dir=queue,
    ).require_quiescent()
    assert JobSpec.read(first).paused_by == TOKEN
    pause_resume.resume_token_scope_with_proof("localhost", TOKEN, queue_dir=queue).require_clear()
    assert JobSpec.read(first).state == JobState.RUNNING
    assert JobSpec.read(first).paused_by is None
    assert other.read_bytes() == original_other
    assert [pgid for pgid, _ in signals] == [900020, 900020]


def test_expired_deadline_still_admits_uncontended_history(
    queue: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(4):
        _job(queue, f"history-{index}")
    offset = [0.0]
    real_monotonic = time.monotonic
    real_read = JobSpec.read

    def read(path: Path) -> JobSpec:
        spec = real_read(path)
        offset[0] = 100.0
        return spec

    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + offset[0])
    monkeypatch.setattr(JobSpec, "read", read)
    pause_resume.resume_token_scope_with_proof("localhost", TOKEN, queue_dir=queue).require_clear()


@pytest.mark.parametrize("boundary", ["read", "before-acquire", "after-acquire"])
def test_ordinary_interrupt_releases_lock_and_resets_budget(
    queue: Path, monkeypatch: pytest.MonkeyPatch, boundary: str,
) -> None:
    _job(queue, "ordinary-interrupt")
    real_flock = fcntl.flock
    real_read = JobSpec.read
    fired = False

    def read(path: Path) -> JobSpec:
        nonlocal fired
        if not fired:
            fired = True
            raise KeyboardInterrupt("fixture read interruption")
        return real_read(path)

    def flock(fd: int, operation: int) -> None:
        nonlocal fired
        if operation & fcntl.LOCK_EX and not fired:
            fired = True
            if boundary == "after-acquire":
                real_flock(fd, operation)
            raise KeyboardInterrupt("fixture acquisition interruption")
        real_flock(fd, operation)

    with monkeypatch.context() as interrupt:
        if boundary == "read":
            interrupt.setattr(JobSpec, "read", read)
        else:
            interrupt.setattr(fcntl, "flock", flock)
        with pytest.raises(KeyboardInterrupt):
            pause_resume.pause_token_scope_with_proof("localhost", TOKEN, queue_dir=queue)
    assert fired
    pause_resume.resume_token_scope_with_proof("localhost", TOKEN, queue_dir=queue).require_clear()


@pytest.mark.parametrize("stage", ["pause-initial", "pause-final", "resume-final"])
def test_contention_arriving_at_a_later_scan_is_explicitly_unresolved(
    queue: Path, monkeypatch: pytest.MonkeyPatch, stage: str,
) -> None:
    target = _job(queue, "late-contention", state=JobState.RUNNING, pgid=900030)
    monkeypatch.setattr(pause_resume.process_group, "signal_process_group", lambda *args: None)
    real_scope = pause_resume._pause_scope_spec_paths
    scans = 0
    holder = None

    def scope(**kwargs: object) -> list[Path]:
        nonlocal scans, holder
        scans += 1
        records = real_scope(**kwargs)
        # Reconciliation enumerates independently. These are the admission
        # capture/final scans, or the final token-absence scan after resume.
        selected = 2 if stage == "pause-final" else 1
        if scans == selected:
            holder = _hold(target)
        return records

    monkeypatch.setattr(pause_resume, "_pause_scope_spec_paths", scope)
    try:
        if stage == "resume-final":
            proof = pause_resume.resume_token_scope_with_proof("localhost", TOKEN, queue_dir=queue)
        else:
            proof = pause_resume.pause_token_scope_with_proof("localhost", TOKEN, queue_dir=queue)
    finally:
        if holder is not None:
            _release(holder)
    assert holder is not None
    assert any(jobid == target.stem and "lock" in reason for jobid, reason in proof.unresolved)
    if stage == "pause-initial":
        assert any("initial spec" in reason for _, reason in proof.unresolved)
    elif stage == "pause-final":
        assert any("final spec" in reason for _, reason in proof.unresolved)


@pytest.mark.parametrize("operation", ["pause", "resume"])
def test_single_job_recheck_timeout_cannot_escape_the_final_proof(
    queue: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    state = JobState.RUNNING if operation == "pause" else JobState.SUSPENDED
    target = _job(queue, "recheck", state=state, pgid=900040, paused_by=TOKEN)
    monkeypatch.setattr(pause_resume.process_group, "signal_process_group", lambda *args: None)
    single_job = getattr(pause_resume, f"{operation}_job")
    holder = None

    def recheck(*args: object, **kwargs: object) -> str:
        nonlocal holder
        holder = _hold(target)
        return single_job(*args, **kwargs)

    monkeypatch.setattr(pause_resume, f"{operation}_job", recheck)
    try:
        function = (
            pause_resume.pause_token_scope_with_proof
            if operation == "pause" else pause_resume.resume_token_scope_with_proof
        )
        proof = function("localhost", TOKEN, queue_dir=queue)
    finally:
        if holder is not None:
            _release(holder)
    assert holder is not None
    assert any(jobid == target.stem and "lock" in reason for jobid, reason in proof.unresolved)

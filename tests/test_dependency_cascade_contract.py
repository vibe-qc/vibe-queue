"""Old-code durable contracts for the dependency cascade extraction.

These tests call the real dispatch scan with process launch replaced by a
recorder.  They characterize dependency state, scan order, persistence,
events, notifications, retry, and the locked fresh-read race before the
Milestone 9 helper moves out of ``Daemon._dispatch_pending``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import config, paths
from vq import daemon as daemon_mod
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState


@pytest.fixture
def cascade_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Daemon]:
    """A queue-locked daemon object with no process or remote activity."""
    state_dir = tmp_path / "state"
    config_dir = tmp_path / "config"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_dir))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(
        config,
        "SYSTEM_CONFIG_PATH",
        tmp_path / "missing-system-config.toml",
    )
    monkeypatch.setattr(daemon_mod.cgroup, "reset_availability_cache", lambda: None)
    monkeypatch.setattr(daemon_mod.cgroup, "available", lambda: False)

    daemon = Daemon(
        max_cpus=4,
        max_jobs=8,
        max_mem_mb=4096,
        poll_interval=0.05,
        queue_dir=state_dir / "queue",
        jobs_dir=state_dir / "jobs",
    )
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    daemon.jobs_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield daemon
    finally:
        daemon._queue_lock_fd.close()


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.PENDING,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
    retry_max: int = 0,
    retry_count: int = 0,
    not_before: str | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["python", "-c", "pass"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        depends_on=depends_on or [],
        depends_on_any=depends_on_any or [],
        retry_max=retry_max,
        retry_count=retry_count,
        not_before=not_before,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _dispatch_with_recorded_starts(daemon: Daemon) -> list[str]:
    started: list[str] = []

    def record_start(spec: JobSpec) -> bool:
        started.append(spec.id)
        return True

    with patch.object(daemon, "_start_job", side_effect=record_start):
        daemon._dispatch_pending()
    return started


@pytest.mark.parametrize(
    ("dependency_field", "predecessor_state", "should_start"),
    [
        ("depends_on", JobState.COMPLETED, True),
        ("depends_on", JobState.RUNNING, False),
        ("depends_on", JobState.SUSPENDED, False),
        ("depends_on_any", JobState.COMPLETED, True),
        ("depends_on_any", JobState.FAILED, True),
        ("depends_on_any", JobState.KILLED, True),
        ("depends_on_any", JobState.INTERRUPTED, True),
        ("depends_on_any", JobState.OOM_KILLED, True),
        ("depends_on_any", JobState.STARVED, True),
        ("depends_on_any", JobState.TIME_EXCEEDED, True),
        ("depends_on_any", JobState.ABORTED_BY_QUEUE, True),
        ("depends_on_any", JobState.PENDING, False),
        ("depends_on_any", JobState.RUNNING, False),
        ("depends_on_any", JobState.SUSPENDED, False),
    ],
)
def test_real_dispatch_dependency_readiness_contract(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    dependency_field: str,
    predecessor_state: JobState,
    should_start: bool,
) -> None:
    _write_spec(cascade_daemon, "a-predecessor", state=predecessor_state)
    dependency_kwargs = {dependency_field: ["a-predecessor"]}
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        **dependency_kwargs,
    )
    notified: list[str] = []
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    started = _dispatch_with_recorded_starts(cascade_daemon)

    assert (dependent.id in started) is should_start
    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert persisted.state == JobState.PENDING
    assert persisted.finished_at is None
    assert persisted.failure_reason is None
    assert daemon_mod.events.read_events(Path(dependent.cwd)) == []
    assert notified == []


@pytest.mark.parametrize("dependency_field", ["depends_on", "depends_on_any"])
def test_missing_predecessor_waits_without_durable_side_effects(
    cascade_daemon: Daemon,
    dependency_field: str,
) -> None:
    dependent = _write_spec(
        cascade_daemon,
        "dependent",
        **{dependency_field: ["missing-predecessor"]},
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert persisted.state == JobState.PENDING
    assert persisted.finished_at is None
    assert persisted.failure_reason is None
    assert daemon_mod.events.read_events(Path(dependent.cwd)) == []


@pytest.mark.parametrize(
    ("required_state", "after_any_state", "outcome"),
    [
        (JobState.COMPLETED, JobState.FAILED, "start"),
        (JobState.COMPLETED, JobState.RUNNING, "wait"),
        (JobState.RUNNING, JobState.FAILED, "wait"),
        (JobState.FAILED, JobState.RUNNING, "cascade"),
    ],
)
def test_combined_dependency_modes_are_additive(
    cascade_daemon: Daemon,
    required_state: JobState,
    after_any_state: JobState,
    outcome: str,
) -> None:
    _write_spec(cascade_daemon, "a-required", state=required_state)
    _write_spec(cascade_daemon, "b-after-any", state=after_any_state)
    dependent = _write_spec(
        cascade_daemon,
        "c-dependent",
        depends_on=["a-required"],
        depends_on_any=["b-after-any"],
    )

    started = _dispatch_with_recorded_starts(cascade_daemon)
    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))

    assert (dependent.id in started) is (outcome == "start")
    if outcome == "cascade":
        assert persisted.state == JobState.FAILED
        assert persisted.failure_reason == (
            "predecessor a-required failed (state=failed)"
        )
        assert persisted.finished_at is not None
    else:
        assert persisted.state == JobState.PENDING
        assert persisted.failure_reason is None
        assert persisted.finished_at is None


def test_multiple_after_any_predecessors_must_all_be_terminal(
    cascade_daemon: Daemon,
) -> None:
    _write_spec(cascade_daemon, "a-failed", state=JobState.FAILED)
    _write_spec(cascade_daemon, "b-running", state=JobState.RUNNING)
    dependent = _write_spec(
        cascade_daemon,
        "c-dependent",
        depends_on_any=["a-failed", "b-running"],
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    running = JobSpec.read(cascade_daemon._spec_path("b-running"))
    running.state = JobState.COMPLETED
    running.write(cascade_daemon._spec_path(running.id))

    assert _dispatch_with_recorded_starts(cascade_daemon) == [dependent.id]
    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert persisted.state == JobState.PENDING
    assert daemon_mod.events.read_events(Path(dependent.cwd)) == []


def test_predecessor_retry_waits_then_cascade_bypasses_dependent_backoff(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future = "2999-01-01T00:00:00+00:00"
    predecessor = _write_spec(
        cascade_daemon,
        "a-predecessor",
        retry_max=2,
        retry_count=1,
        not_before=future,
    )
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        depends_on=[predecessor.id],
        retry_max=4,
        retry_count=2,
    )
    notified: list[str] = []
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    waiting = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert waiting.state == JobState.PENDING
    assert waiting.not_before is None
    assert notified == []

    waiting.not_before = future
    waiting.write(cascade_daemon._spec_path(waiting.id))

    exhausted = JobSpec.read(cascade_daemon._spec_path(predecessor.id))
    exhausted.state = JobState.FAILED
    exhausted.finished_at = "2026-08-02T12:00:00+00:00"
    exhausted.failure_reason = "retry budget exhausted"
    exhausted.write(cascade_daemon._spec_path(exhausted.id))

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    cascaded = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert cascaded.state == JobState.FAILED
    assert cascaded.retry_max == 4
    assert cascaded.retry_count == 2
    assert cascaded.not_before == future
    assert cascaded.failure_reason == (
        "predecessor a-predecessor failed (state=failed)"
    )
    assert notified == [dependent.id]


def test_first_declared_required_failure_controls_reason(
    cascade_daemon: Daemon,
) -> None:
    _write_spec(cascade_daemon, "a-killed", state=JobState.KILLED)
    _write_spec(cascade_daemon, "z-failed", state=JobState.FAILED)
    dependent = _write_spec(
        cascade_daemon,
        "dependent",
        depends_on=["missing", "z-failed", "a-killed"],
    )

    _dispatch_with_recorded_starts(cascade_daemon)

    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert persisted.failure_reason == (
        "predecessor z-failed failed (state=failed)"
    )


def test_cascade_persists_then_events_under_lock_then_notifies_after_unlock(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec(cascade_daemon, "a-predecessor", state=JobState.FAILED)
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        depends_on=["a-predecessor"],
    )
    cascade_daemon.notify_webhook_url = "https://notify.invalid/hook"
    cascade_daemon.notify_on_states = ["failed"]
    fixed_finished_at = "2026-08-02T12:34:56+00:00"
    fixed_event_at = "2026-08-02T12:34:57+00:00"
    monkeypatch.setattr(daemon_mod, "utcnow_iso", lambda: fixed_finished_at)
    monkeypatch.setattr(
        daemon_mod.events,
        "utcnow_iso",
        lambda: fixed_event_at,
    )

    real_lock = daemon_mod.paths.spec_lock
    real_event = daemon_mod.events.state_transition
    lock_held = False
    timeline: list[tuple[object, ...]] = []

    @contextmanager
    def tracked_lock(path: Path, **kwargs: object) -> Iterator[None]:
        nonlocal lock_held
        timeline.append(("lock-enter", path.stem))
        with real_lock(path, **kwargs):
            lock_held = True
            try:
                yield
            finally:
                lock_held = False
        timeline.append(("lock-exit", path.stem))

    def tracked_event(
        workspace: Path,
        jobid: str,
        **payload: object,
    ) -> None:
        assert lock_held is True
        persisted = JobSpec.read(cascade_daemon._spec_path(jobid))
        assert persisted.state == JobState.FAILED
        timeline.append(("event", jobid, payload["reason"]))
        real_event(workspace, jobid, **payload)

    def tracked_notification(
        spec: JobSpec,
        url: str | None,
        **kwargs: object,
    ) -> None:
        assert lock_held is False
        records = daemon_mod.events.read_events(Path(spec.cwd))
        timeline.append(
            (
                "notify",
                spec.id,
                spec.state.value,
                spec.failure_reason,
                spec.finished_at,
                url,
                tuple(kwargs["notify_on_states"]),
                len(records),
            )
        )

    monkeypatch.setattr(daemon_mod.paths, "spec_lock", tracked_lock)
    monkeypatch.setattr(daemon_mod.events, "state_transition", tracked_event)
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        tracked_notification,
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    reason = "predecessor a-predecessor failed (state=failed)"
    assert timeline == [
        ("lock-enter", "b-dependent"),
        ("event", "b-dependent", reason),
        ("lock-exit", "b-dependent"),
        (
            "notify",
            "b-dependent",
            "failed",
            reason,
            fixed_finished_at,
            "https://notify.invalid/hook",
            ("failed",),
            1,
        ),
    ]
    persisted = JobSpec.read(cascade_daemon._spec_path(dependent.id))
    assert persisted.state == JobState.FAILED
    assert persisted.finished_at == fixed_finished_at
    assert persisted.failure_reason == reason
    [event] = daemon_mod.events.read_events(Path(dependent.cwd))
    assert event == {
        "ts": fixed_event_at,
        "kind": "state_transition",
        "jobid": "b-dependent",
        "from": "pending",
        "to": "failed",
        "reason": reason,
    }


def test_sorted_chain_cascades_multiple_levels_in_one_tick(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec(cascade_daemon, "a-root", state=JobState.FAILED)
    middle = _write_spec(
        cascade_daemon,
        "b-middle",
        depends_on=["a-root"],
    )
    leaf = _write_spec(
        cascade_daemon,
        "c-leaf",
        depends_on=["b-middle"],
    )
    timeline: list[tuple[str, str]] = []
    real_event = daemon_mod.events.state_transition

    def tracked_event(
        workspace: Path,
        jobid: str,
        **payload: object,
    ) -> None:
        timeline.append(("event", jobid))
        real_event(workspace, jobid, **payload)

    monkeypatch.setattr(
        daemon_mod.events,
        "state_transition",
        tracked_event,
    )
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: timeline.append(("notify", spec.id)),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    assert JobSpec.read(cascade_daemon._spec_path(middle.id)).state == JobState.FAILED
    assert JobSpec.read(cascade_daemon._spec_path(leaf.id)).state == JobState.FAILED
    assert timeline == [
        ("event", "b-middle"),
        ("notify", "b-middle"),
        ("event", "c-leaf"),
        ("notify", "c-leaf"),
    ]
    assert (
        JobSpec.read(cascade_daemon._spec_path(leaf.id)).failure_reason
        == "predecessor b-middle failed (state=failed)"
    )


def test_reverse_sorted_chain_retries_leaf_on_next_tick(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leaf = _write_spec(
        cascade_daemon,
        "a-leaf",
        depends_on=["b-middle"],
    )
    middle = _write_spec(
        cascade_daemon,
        "b-middle",
        depends_on=["c-root"],
    )
    _write_spec(cascade_daemon, "c-root", state=JobState.FAILED)
    notified: list[str] = []
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    assert JobSpec.read(cascade_daemon._spec_path(middle.id)).state == JobState.FAILED
    assert JobSpec.read(cascade_daemon._spec_path(leaf.id)).state == JobState.PENDING
    assert notified == ["b-middle"]

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    assert JobSpec.read(cascade_daemon._spec_path(leaf.id)).state == JobState.FAILED
    assert notified == ["b-middle", "a-leaf"]


def test_failed_cascade_write_is_retried_without_early_side_effects(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec(cascade_daemon, "a-root", state=JobState.FAILED)
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        depends_on=["a-root"],
    )
    real_write = JobSpec.write
    failed_attempts = 0
    notified: list[str] = []

    def fail_first_cascade_write(spec: JobSpec, path: Path) -> None:
        nonlocal failed_attempts
        if spec.id == dependent.id and spec.state == JobState.FAILED:
            failed_attempts += 1
            if failed_attempts == 1:
                raise OSError("characterized write failure")
        real_write(spec, path)

    monkeypatch.setattr(JobSpec, "write", fail_first_cascade_write)
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    assert JobSpec.read(cascade_daemon._spec_path(dependent.id)).state == JobState.PENDING
    assert daemon_mod.events.read_events(Path(dependent.cwd)) == []
    assert notified == []

    assert _dispatch_with_recorded_starts(cascade_daemon) == []
    assert JobSpec.read(cascade_daemon._spec_path(dependent.id)).state == JobState.FAILED
    assert failed_attempts == 2
    assert len(daemon_mod.events.read_events(Path(dependent.cwd))) == 1
    assert notified == [dependent.id]


def test_locked_fresh_read_preserves_racing_terminal_mutation(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_spec(cascade_daemon, "a-root", state=JobState.FAILED)
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        depends_on=["a-root"],
    )
    target_path = cascade_daemon._spec_path(dependent.id)
    real_lock = daemon_mod.paths.spec_lock
    injected = False
    notified: list[str] = []

    @contextmanager
    def inject_terminal_before_lock(
        path: Path,
        **kwargs: object,
    ) -> Iterator[None]:
        nonlocal injected
        if path == target_path and not injected:
            injected = True
            racing = JobSpec.read(path)
            racing.state = JobState.KILLED
            racing.finished_at = "2026-08-02T13:00:00+00:00"
            racing.failure_reason = "operator kill won the race"
            racing.write(path)
        with real_lock(path, **kwargs):
            yield

    monkeypatch.setattr(
        daemon_mod.paths,
        "spec_lock",
        inject_terminal_before_lock,
    )
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    persisted = JobSpec.read(target_path)
    assert injected is True
    assert persisted.state == JobState.KILLED
    assert persisted.finished_at == "2026-08-02T13:00:00+00:00"
    assert persisted.failure_reason == "operator kill won the race"
    assert daemon_mod.events.read_events(Path(dependent.cwd)) == []
    assert notified == []


def test_locked_fresh_pending_fields_survive_stale_graph_cascade(
    cascade_daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the scan-snapshot decision while preserving fresh persisted fields."""
    _write_spec(cascade_daemon, "a-root", state=JobState.FAILED)
    dependent = _write_spec(
        cascade_daemon,
        "b-dependent",
        depends_on=["a-root"],
    )
    target_path = cascade_daemon._spec_path(dependent.id)
    real_lock = daemon_mod.paths.spec_lock
    injected = False
    notified: list[str] = []

    @contextmanager
    def replace_pending_before_lock(
        path: Path,
        **kwargs: object,
    ) -> Iterator[None]:
        nonlocal injected
        if path == target_path and not injected:
            injected = True
            racing = JobSpec.read(path)
            racing.command = ["python", "new-command.py"]
            racing.tags = ["concurrent-edit"]
            racing.depends_on = []
            racing.write(path)
        with real_lock(path, **kwargs):
            yield

    monkeypatch.setattr(
        daemon_mod.paths,
        "spec_lock",
        replace_pending_before_lock,
    )
    monkeypatch.setattr(
        daemon_mod.notify,
        "send_terminal_notification",
        lambda spec, *_args, **_kwargs: notified.append(spec.id),
    )

    assert _dispatch_with_recorded_starts(cascade_daemon) == []

    persisted = JobSpec.read(target_path)
    assert injected is True
    assert persisted.state == JobState.FAILED
    assert persisted.command == ["python", "new-command.py"]
    assert persisted.tags == ["concurrent-edit"]
    assert persisted.depends_on == []
    assert persisted.failure_reason == "predecessor a-root failed (state=failed)"
    assert len(daemon_mod.events.read_events(Path(dependent.cwd))) == 1
    assert notified == [dependent.id]

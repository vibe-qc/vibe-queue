"""Old-code parity contracts for persisted admission classifiers.

The daemon and status each implement ``not_before`` and dependency readiness.
These tests compare their public observable behavior before genuinely shared
rules move to a neutral module.  The daemon scan is real, but process launch is
replaced by a recorder; status is read through its public JSON surface.

Capacity counters, build/admin holds, refresh mutation, host pressure, and
dependency cascade are intentionally absent.  Those policies are not
equivalent between the effectful daemon and the best-effort status view.
The ordinary ``not_before`` domain is characterized here too, but one explicit
noncanonical case below proves its two parsers are not universally equivalent.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import config, paths
from vq import daemon as daemon_mod
from vq import status as status_mod
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState


@pytest.fixture
def admission_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Daemon]:
    """A queue-locked daemon object isolated from process and remote work."""
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
        max_cpus=8,
        max_jobs=16,
        max_mem_mb=8192,
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
    retry_count: int = 0,
    not_before: str | None = None,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["python", "-c", "pass"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        retry_count=retry_count,
        not_before=not_before,
        depends_on=depends_on or [],
        depends_on_any=depends_on_any or [],
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _status_payload(daemon: Daemon, jobid: str) -> dict[str, object]:
    return json.loads(
        status_mod.show_status_json(
            "localhost",
            jobid,
            tail=0,
            queue_dir=daemon.queue_dir,
        )
    )


def _dispatch_with_recorded_starts(daemon: Daemon) -> list[str]:
    started: list[str] = []

    def record_start(spec: JobSpec) -> bool:
        started.append(spec.id)
        return True

    with patch.object(daemon, "_start_job", side_effect=record_start):
        daemon._dispatch_pending()
    return started


@pytest.mark.parametrize(
    ("not_before", "retry_count", "expected_started", "expected_blockers"),
    [
        (None, 0, True, []),
        ("", 0, True, []),
        ("not-a-timestamp", 0, True, []),
        ("2999-01-01T00:00:00", 0, True, []),
        ("2000-01-01T00:00:00+00:00", 0, True, []),
        ("2000-01-01T00:00:00Z", 0, True, []),
        (
            "2999-01-01T00:00:00+00:00",
            0,
            False,
            ["held until 2999-01-01T00:00:00+00:00 (scheduled submit)"],
        ),
        (
            "2999-01-01T00:00:00Z",
            1,
            False,
            ["held until 2999-01-01T00:00:00Z (retry backoff)"],
        ),
        (
            "2999-01-01T00:00:00-05:00",
            0,
            False,
            ["held until 2999-01-01T00:00:00-05:00 (scheduled submit)"],
        ),
    ],
)
def test_not_before_status_and_dispatch_parity(
    admission_daemon: Daemon,
    not_before: str | None,
    retry_count: int,
    expected_started: bool,
    expected_blockers: list[str],
) -> None:
    spec = _write_spec(
        admission_daemon,
        "job",
        retry_count=retry_count,
        not_before=not_before,
    )

    payload = _status_payload(admission_daemon, spec.id)
    started = _dispatch_with_recorded_starts(admission_daemon)
    persisted = JobSpec.read(admission_daemon._spec_path(spec.id))

    assert (spec.id in started) is expected_started
    assert persisted.state == JobState.PENDING
    assert payload["pending_blockers"] == expected_blockers
    if expected_blockers:
        assert payload["pending_admission_reason"] == "; ".join(expected_blockers)
    else:
        assert "pending_admission_reason" not in payload


def test_noncanonical_z_separator_keeps_owner_specific_behavior(
    admission_daemon: Daemon,
) -> None:
    """Do not centralize the two subtly different timestamp parsers.

    Python accepts ``Z`` here as the ISO date/time separator, so the daemon
    sees a future timestamp.  Status first replaces every ``Z`` with an offset,
    making this noncanonical timestamp gate fail open.  Both parser policies
    are old behavior even though their observable answers differ.
    """
    spec = _write_spec(
        admission_daemon,
        "job",
        not_before="2999-01-01Z00:00:00+00:00",
    )

    payload = _status_payload(admission_daemon, spec.id)
    started = _dispatch_with_recorded_starts(admission_daemon)

    assert spec.id not in started
    assert JobSpec.read(admission_daemon._spec_path(spec.id)).state == (
        JobState.PENDING
    )
    assert payload["pending_blockers"] == []
    assert "pending_admission_reason" not in payload


@pytest.mark.parametrize(
    (
        "depends_on",
        "depends_on_any",
        "predecessors",
        "expected_started",
        "expected_blockers",
    ),
    [
        ([], [], [], True, []),
        (["required"], [], [("required", JobState.COMPLETED)], True, []),
        (
            ["required"],
            [],
            [("required", JobState.RUNNING)],
            False,
            ["waiting on 1 dependenc(y/ies): required(running)"],
        ),
        (
            ["required"],
            [],
            [("required", JobState.SUSPENDED)],
            False,
            ["waiting on 1 dependenc(y/ies): required(suspended)"],
        ),
        (
            ["missing"],
            [],
            [],
            False,
            ["waiting on 1 dependenc(y/ies): missing(unknown)"],
        ),
        ([], ["after"], [("after", JobState.COMPLETED)], True, []),
        ([], ["after"], [("after", JobState.FAILED)], True, []),
        ([], ["after"], [("after", JobState.KILLED)], True, []),
        (
            [],
            ["after"],
            [("after", JobState.RUNNING)],
            False,
            ["waiting on 1 dependenc(y/ies): after(running)"],
        ),
        (
            [],
            ["after"],
            [("after", JobState.SUSPENDED)],
            False,
            ["waiting on 1 dependenc(y/ies): after(suspended)"],
        ),
        (
            [],
            ["missing"],
            [],
            False,
            ["waiting on 1 dependenc(y/ies): missing(unknown)"],
        ),
        (
            ["required"],
            ["after"],
            [
                ("required", JobState.COMPLETED),
                ("after", JobState.FAILED),
            ],
            True,
            [],
        ),
        (
            ["required"],
            ["after"],
            [
                ("required", JobState.RUNNING),
                ("after", JobState.FAILED),
            ],
            False,
            ["waiting on 1 dependenc(y/ies): required(running)"],
        ),
        (
            ["required"],
            ["after"],
            [
                ("required", JobState.COMPLETED),
                ("after", JobState.RUNNING),
            ],
            False,
            ["waiting on 1 dependenc(y/ies): after(running)"],
        ),
        (
            ["missing-required", "done", "running"],
            ["missing-after", "failed", "suspended"],
            [
                ("done", JobState.COMPLETED),
                ("running", JobState.RUNNING),
                ("failed", JobState.FAILED),
                ("suspended", JobState.SUSPENDED),
            ],
            False,
            [
                "waiting on 4 dependenc(y/ies): "
                "missing-required(unknown), running(running), "
                "missing-after(unknown), suspended(suspended)"
            ],
        ),
    ],
)
def test_dependency_status_and_dispatch_parity(
    admission_daemon: Daemon,
    depends_on: list[str],
    depends_on_any: list[str],
    predecessors: list[tuple[str, JobState]],
    expected_started: bool,
    expected_blockers: list[str],
) -> None:
    for predecessor_id, state in predecessors:
        _write_spec(admission_daemon, predecessor_id, state=state)
    dependent = _write_spec(
        admission_daemon,
        "z-dependent",
        depends_on=depends_on,
        depends_on_any=depends_on_any,
    )

    payload = _status_payload(admission_daemon, dependent.id)
    started = _dispatch_with_recorded_starts(admission_daemon)
    persisted = JobSpec.read(admission_daemon._spec_path(dependent.id))

    assert (dependent.id in started) is expected_started
    assert persisted.state == JobState.PENDING
    assert payload["pending_blockers"] == expected_blockers
    if expected_blockers:
        assert payload["pending_admission_reason"] == "; ".join(expected_blockers)
    else:
        assert "pending_admission_reason" not in payload


def test_status_blocker_order_composes_without_driving_daemon_order(
    admission_daemon: Daemon,
) -> None:
    _write_spec(admission_daemon, "required", state=JobState.RUNNING)
    dependent = _write_spec(
        admission_daemon,
        "z-dependent",
        retry_count=1,
        not_before="2999-01-01T00:00:00+00:00",
        depends_on=["required"],
        depends_on_any=["missing-after"],
    )
    expected = [
        "held until 2999-01-01T00:00:00+00:00 (retry backoff)",
        "waiting on 2 dependenc(y/ies): required(running), "
        "missing-after(unknown)",
    ]

    payload = _status_payload(admission_daemon, dependent.id)
    started = _dispatch_with_recorded_starts(admission_daemon)

    assert dependent.id not in started
    assert JobSpec.read(admission_daemon._spec_path(dependent.id)).state == (
        JobState.PENDING
    )
    assert payload["pending_blockers"] == expected
    assert payload["pending_admission_reason"] == "; ".join(expected)


def test_non_pending_status_shape_is_not_ready_pending_shape(
    admission_daemon: Daemon,
) -> None:
    spec = _write_spec(admission_daemon, "job", state=JobState.RUNNING)

    payload = _status_payload(admission_daemon, spec.id)
    started = _dispatch_with_recorded_starts(admission_daemon)

    assert spec.id not in started
    assert JobSpec.read(admission_daemon._spec_path(spec.id)).state == JobState.RUNNING
    assert "pending_blockers" not in payload
    assert "pending_admission_reason" not in payload

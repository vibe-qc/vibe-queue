"""`vq status` on a PENDING job must explain WHY it is not running.

A pending-reason helper existed but covered only 5 of the daemon's 16 dispatch
gates and returned the first blocker only. An agent polling a stuck job got a
blank where the answer should be. These pin the added gates and the
stack-all-reasons behaviour, using only persisted state (no daemon).
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from vq import capacity, paths
from vq.spec import JobSpec, JobState
from vq.status import pending_blockers


def _spec(**fields: object) -> JobSpec:
    base: dict[str, object] = {
        "id": "j",
        "command": ["true"],
        "cwd": "/tmp",
        "cpus": 1,
        "state": JobState.PENDING,
    }
    base.update(fields)
    return JobSpec(**base)


def _cap(monkeypatch: pytest.MonkeyPatch, **fields: object) -> None:
    fields.setdefault("max_cpus", 8)
    cap = capacity.DaemonCapacity(written_at="2026-07-23T00:00:00+00:00", **fields)
    monkeypatch.setattr(
        "vq.status.capacity.read_daemon_capacity",
        lambda **_kwargs: cap,
    )


def _no_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("vq.status.drain.read_drain_state", lambda **k: None)
    monkeypatch.setattr(
        "vq.status._admin_update_hold_reason", lambda _scheduler_target: None
    )


def test_unmet_depends_on_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec(depends_on=["dep1"])
    dep = _spec(id="dep1", state=JobState.RUNNING)

    blockers = pending_blockers(spec, [spec, dep])

    assert any("waiting on 1 dependenc" in b and "dep1(running)" in b for b in blockers)


def test_depends_on_any_only_needs_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec(depends_on_any=["dep1"])
    dep = _spec(id="dep1", state=JobState.FAILED)  # terminal ⇒ satisfied

    assert pending_blockers(spec, [spec, dep]) == []


def test_refresh_before_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec(refresh_before="vibeqc-dev")

    blockers = pending_blockers(spec, [spec])

    assert any("--refresh" in b and "vibeqc-dev" in b for b in blockers)


def test_a_running_build_job_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec()
    build = _spec(id="b", state=JobState.RUNNING, build_env="vibeqc-dev")

    blockers = pending_blockers(spec, [spec, build])

    assert any("build job is running" in b and "vibeqc-dev" in b for b in blockers)


def test_scheduled_submit_not_before_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec(not_before="2099-01-01T00:00:00+00:00")

    blockers = pending_blockers(spec, [spec])

    assert any("held until 2099" in b and "scheduled submit" in b for b in blockers)


def test_a_naive_not_before_is_treated_as_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """The daemon dispatches a naive timestamp now; do not report a phantom block."""
    _no_drain(monkeypatch)
    _cap(monkeypatch)
    spec = _spec(not_before="2099-01-01T00:00:00")  # no tz

    assert pending_blockers(spec, [spec]) == []


def test_undeclared_job_is_charged_the_daemon_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The memory gate previously skipped mem_mb=None specs — the bug.

    The daemon charges an undeclared job its --default-job-mem-mb, so a client
    that reads raw mem_mb under-counts and drops the reason for exactly the
    jobs it applies to.
    """
    _no_drain(monkeypatch)
    _cap(monkeypatch, max_mem_mb=1000, default_job_mem_mb=800)
    spec = _spec()  # mem_mb=None
    running = _spec(id="r", state=JobState.RUNNING)  # also undeclared ⇒ 800 MB

    blockers = pending_blockers(spec, [spec, running])

    assert any("memory admission blocked" in b for b in blockers)
    assert any("daemon default for an undeclared job" in b for b in blockers)


def test_blockers_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    """A job can be behind several gates at once; report all of them."""
    _no_drain(monkeypatch)
    _cap(monkeypatch, max_cpus=1)
    spec = _spec(cpus=4, depends_on=["dep1"])
    dep = _spec(id="dep1", state=JobState.RUNNING)
    running = _spec(id="r", state=JobState.RUNNING, cpus=1)

    blockers = pending_blockers(spec, [spec, dep, running])

    assert any("dependenc" in b for b in blockers)
    assert any("CPU request exceeds configured cap" in b for b in blockers)


def test_admin_update_marker_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The most confusing PENDING of all: the queue is mid-rebuild."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    admin.acquire_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
    monkeypatch.setattr("vq.status.drain.read_drain_state", lambda **k: None)
    _cap(monkeypatch)
    spec = _spec()

    blockers = pending_blockers(spec, [spec])

    assert any("admin update is in progress" in b and "vibeqc-dev" in b for b in blockers)


def test_foreign_scheduler_admin_update_marker_is_not_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A host_c rebuild cannot truthfully explain a pending host_f job."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_f")

    assert pending_blockers(spec, [spec]) == []


def test_matching_scheduler_admin_update_marker_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The host filter must retain the marker that really gates the job."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_c")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "admin update is in progress" in blockers[0]
    assert "scheduler-runtime:host_c:vibeqc-dev" in blockers[0]


def test_unreadable_marker_keeps_mixed_registry_global(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One readable foreign lease must not hide a corrupt global hold."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    marker_dir = admin.admin_update_marker_dir()
    marker_dir.mkdir(parents=True)
    (marker_dir / "corrupt.json").write_text("{", encoding="utf-8")
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_f")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "unreadable or unrecognised" in blockers[0]
    assert "holds all dispatch" in blockers[0]


@pytest.mark.parametrize(
    "bad_envs",
    [
        pytest.param(
            ["scheduler-runtime:host_c:vibeqc-dev", 7],
            id="non-string-entry",
        ),
        pytest.param(7, id="scalar-container"),
    ],
)
def test_unrecognised_marker_shape_fails_closed_without_crashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bad_envs: object,
) -> None:
    """Malformed env entries are global holds, not a silent join failure."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    marker = admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    payload = asdict(marker)
    payload["envs"] = bad_envs
    admin.admin_update_marker_path().write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_f")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "unreadable or unrecognised" in blockers[0]
    assert "holds all dispatch" in blockers[0]


def test_nonstring_marker_state_does_not_hide_an_applicable_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A malformed display-only state cannot suppress a proven blocker."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    marker = admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    payload = asdict(marker)
    payload["state"] = 7
    admin.admin_update_marker_path().write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_c")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "admin update is in progress" in blockers[0]
    assert "states=7" in blockers[0]


def test_stale_durable_marker_still_reports_its_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Daemon recovery receipts remain effective after their writer exits."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    marker = admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    marker.pause_token = "admin-update-0123456789ab"
    admin._write_admin_update_marker_atomic(
        marker,
        path=admin.admin_update_marker_path(),
    )
    monkeypatch.setattr(
        admin,
        "admin_update_marker_stale_reason",
        lambda _marker: "the updater process is gone",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_c")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "stale durable admin-update recovery receipt" in blockers[0]
    assert "scheduler-runtime:host_c:vibeqc-dev" in blockers[0]
    assert "explicit recovery is required" in blockers[0]
    assert "admin update is in progress" not in blockers[0]


def test_live_and_stale_durable_markers_are_both_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Independent same-host leases remain distinct proven gates."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    stale = admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    stale.pause_token = "admin-update-0123456789ab"
    admin._write_admin_update_marker_atomic(
        stale,
        path=admin.admin_update_marker_path(),
    )
    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibe-view"],
        host="host_c",
    )
    monkeypatch.setattr(
        admin,
        "admin_update_marker_stale_reason",
        lambda marker: (
            "the updater process is gone" if marker.pause_token else None
        ),
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_c")

    blockers = pending_blockers(spec, [spec])

    assert len(blockers) == 1
    assert "stale durable admin-update recovery receipt" in blockers[0]
    assert "scheduler-runtime:host_c:vibeqc-dev" in blockers[0]
    assert "admin update is also in progress" in blockers[0]
    assert "scheduler-runtime:host_c:vibe-view" in blockers[0]


def test_stale_ordinary_marker_does_not_report_a_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Status mirrors the daemon's auto-reap of a receipt-free corpse."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path))
    from vq import admin

    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"],
        host="host_c",
    )
    monkeypatch.setattr(
        admin,
        "admin_update_marker_stale_reason",
        lambda _marker: "the updater process is gone",
    )
    _cap(monkeypatch)
    spec = _spec(scheduler_target="host_c")

    assert pending_blockers(spec, [spec]) == []


def test_no_provable_blocker_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty means 'nothing provable', not 'ready' — callers must not conflate."""
    _no_drain(monkeypatch)
    _cap(monkeypatch)

    assert pending_blockers(_spec(), [_spec()]) == []


def test_a_non_pending_spec_has_no_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    assert pending_blockers(_spec(state=JobState.RUNNING), []) == []

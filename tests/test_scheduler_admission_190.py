"""Target-specific backlog limits: real daemon admission, no scheduler RPCs."""
# ruff: noqa: F811 -- tests shadow the imported shared daemon fixture
from __future__ import annotations

import pytest
from pydantic import ValidationError

import vq.daemon as daemon_mod
from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from vq.config import Config, HostConfig
from vq.daemon import Daemon
from vq.scheduler_admission import SchedulerTargetAdmission
from vq.scheduler_dispatch import SchedulerError, SchedulerSubmitOutcomeUnknown
from vq.spec import JobSpec, JobState


def host(**options: object) -> HostConfig:
    return HostConfig.model_validate({
        "ssh": "cluster.example.com", "scheduler": "slurm",
        "scheduler_dialect": "slurm", "scratch_root": "/scratch/user",
        "scheduler_driver": "driver",
        **options,
    })


def configure(monkeypatch: pytest.MonkeyPatch, **hosts: HostConfig) -> Config:
    cfg = Config(hosts=hosts)
    monkeypatch.setattr(daemon_mod, "load_config", lambda: cfg)
    return cfg


def test_target_cap_preserves_other_cluster_dispatch(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(monkeypatch, host_f=host(scheduler_max_jobs=1), other=host())
    first, other = MockDispatcher(), MockDispatcher()
    _inject(daemon, first)
    _inject(daemon, other, "other")
    _submit_scheduler(daemon, "first")
    _submit_scheduler(daemon, "second")
    _submit_scheduler(daemon, "elsewhere", target="other")

    daemon._dispatch_pending()

    assert first.submitted == ["first"]
    assert other.submitted == ["elsewhere"]
    assert JobSpec.read(daemon._spec_path("second")).state == JobState.PENDING
    assert JobSpec.read(daemon._spec_path("first")).state == JobState.RUNNING


def test_aliases_share_strictest_cap(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(monkeypatch, host_f=host(scheduler_max_jobs=2), alias=host(
        fleet_role="alias", fleet_canonical_host="host_f", scheduler_max_jobs=1,
    ))
    first, alias = MockDispatcher(), MockDispatcher()
    _inject(daemon, first)
    _inject(daemon, alias, "alias")
    _submit_scheduler(daemon, "first", target="alias")
    _submit_scheduler(daemon, "second")
    daemon._dispatch_pending()
    assert alias.submitted == ["first"]
    assert first.submitted == []


@pytest.mark.parametrize("existing", [True, False], ids=["restart", "same-pass"])
def test_unknown_submission_reserves_target_capacity(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, existing: bool,
) -> None:
    configure(monkeypatch, host_f=host(scheduler_max_jobs=1))
    first = _submit_scheduler(daemon, "first")
    if existing:
        first.state = JobState.SUBMIT_OUTCOME_UNKNOWN
        first.scheduler_state = "submit_outcome_unknown"
        first.write(daemon._spec_path(first.id))
    _submit_scheduler(daemon, "second")
    mock = MockDispatcher(submit_error=SchedulerSubmitOutcomeUnknown("unknown outcome"))
    _inject(daemon, mock)
    daemon._dispatch_pending()
    assert mock.submitted == ([] if existing else ["first"])
    assert JobSpec.read(daemon._spec_path("second")).state == JobState.PENDING


def test_finished_allocation_releases_only_target_capacity(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(monkeypatch, host_f=host(scheduler_max_jobs=1))
    mock = MockDispatcher()
    _inject(daemon, mock)
    first = _submit_scheduler(daemon, "first")
    assert daemon._start_scheduler_job(first)
    daemon._scheduler_running[first.id].allocation_finished = True
    _submit_scheduler(daemon, "second")
    daemon._dispatch_pending()
    assert mock.submitted == ["first", "second"]
    assert first.id in daemon._scheduler_running  # collection still protected


@pytest.mark.parametrize("failed", [False, True], ids=["success", "rejected"])
def test_pacing_survives_passes_and_failed_attempts(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, failed: bool,
) -> None:
    configure(monkeypatch, host_f=host(scheduler_submit_interval_seconds=30))
    clock = [100.0]
    daemon._scheduler_submit_clock = lambda: clock[0]
    mock = MockDispatcher(submit_error=SchedulerError("rejected") if failed else None)
    _inject(daemon, mock)
    _submit_scheduler(daemon, "first")
    _submit_scheduler(daemon, "second")
    daemon._dispatch_pending()
    assert mock.submitted == ["first"]
    clock[0] = 129.999
    daemon._dispatch_pending()
    assert mock.submitted == ["first"]
    clock[0] = 130.0
    daemon._dispatch_pending()
    assert mock.submitted == ["first", "second"]


def test_alias_pacing_and_reload_keep_attempt_history() -> None:
    cfg = Config(hosts={"primary": host(), "alias": host(
        fleet_role="alias", fleet_canonical_host="primary",
        scheduler_submit_interval_seconds=10,
    )})
    attempts: dict[str, float] = {}
    budget = SchedulerTargetAdmission(cfg, [], attempts)
    budget.attempted("alias", 5)
    reloaded = SchedulerTargetAdmission(cfg, [], attempts)
    assert not reloaded.can_attempt("primary", 14)
    assert reloaded.can_attempt("primary", 15)
    assert reloaded.can_attempt("unrelated", 5)


def test_unbound_reservation_is_not_lost_and_array_weight_counts() -> None:
    cfg = Config(hosts={"first": host(scheduler_max_jobs=3), "second": host(
        scheduler_max_jobs=1,
    )})
    budget = SchedulerTargetAdmission(cfg, [("first", 2), (None, 1)], {})
    assert not budget.can_attempt("first", 0)
    assert not budget.can_attempt("second", 0)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_pacing_is_rejected(value: float) -> None:
    with pytest.raises(ValidationError):
        host(scheduler_submit_interval_seconds=value)


def test_local_host_cannot_silently_ignore_scheduler_limits() -> None:
    with pytest.raises(ValidationError, match="non-local scheduler"):
        HostConfig(ssh="localhost", scheduler_max_jobs=1)

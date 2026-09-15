"""Daemon-side tests for the scheduler (cluster) dispatch path (design doc §17).

The daemon routes a spec carrying ``scheduler_target`` to the SSH+qsub path
(``_start_scheduler_job`` / ``_reconcile_scheduler``) instead of a local Popen.
These tests inject a mock ``SchedulerDispatcher`` via the daemon's
``_scheduler_dispatchers`` cache (which also bypasses config loading), so the
full dispatch → reconcile → terminal cycle runs with no SSH, no cluster, and no
Linux-only cgroup code. The local Popen path is exercised by the existing
``test_daemon.py`` suite and is untouched by this feature.
"""

from __future__ import annotations

import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import vq.daemon as daemon_mod
import vq.scheduler_dispatch as scheduler_dispatch
from vq import config, drain, events, paths
from vq.daemon import (
    SCHEDULER_FINISHED_MARKER_GRACE_SECONDS,
    Daemon,
    _OrphanJob,
    _RunningJob,
)
from vq.scheduler_dialect import QstatDetail, SchedulerPhase, SlurmDialect
from vq.scheduler_dispatch import (
    SchedulerError,
    SchedulerHandle,
    SchedulerSubmitReceipt,
)
from vq.spec import JobSpec, JobState, ProgramRuntimePin


class MockDispatcher:
    """A SchedulerDispatcher stand-in: records calls, returns canned phases.

    ``fetch_results`` writes the exit-marker into the local workspace, simulating
    a staged-back cluster workspace, so the daemon's rc read resolves.
    """

    def __init__(
        self,
        *,
        submit_id: str = "555.cluster",
        phase: SchedulerPhase = SchedulerPhase.FINISHED,
        rc: int | None = 0,
        marker_rcs: list[int | None] | None = None,
        detail: QstatDetail | None = None,
        phase_by_id: dict[str, SchedulerPhase] | None = None,
        detail_by_id: dict[str, QstatDetail] | None = None,
        accounting_required_for_absent: bool = False,
        explicitly_absent_job_ids: set[str] | None = None,
        poll_error: Exception | None = None,
        detail_error: Exception | None = None,
        submit_error: Exception | None = None,
        marker_error: Exception | None = None,
        fetch_error: Exception | None = None,
        telemetry_results: list[bool] | None = None,
        max_wall_time_seconds: int | None = None,
    ) -> None:
        self.submit_id = submit_id
        self.phase = phase
        self.rc = rc  # written to the marker on fetch; None ⇒ no marker
        self.marker_rcs = list(marker_rcs) if marker_rcs is not None else None
        self.last_marker_rc: int | None = None
        self.detail = detail  # returned by poll_detail (None ⇒ no detail for the job)
        self.phase_by_id = phase_by_id
        self.detail_by_id = detail_by_id
        self.accounting_required_for_absent = accounting_required_for_absent
        self.explicitly_absent_job_ids = frozenset(
            explicitly_absent_job_ids or set()
        )
        self.poll_error = poll_error
        self.detail_error = detail_error
        self.submit_error = submit_error
        self.marker_error = marker_error
        self.fetch_error = fetch_error
        self.telemetry_results = list(telemetry_results or [True])
        self.telemetry_calls = 0
        self.max_wall_time_seconds = max_wall_time_seconds
        self.submitted: list[str] = []
        self.cancelled: list[str] = []
        self.cleaned: list[tuple[str, str, int | None]] = []
        self.fetched: list[str] = []
        self.marker_queries: list[tuple[str, int | None]] = []
        self.diagnostics: list[tuple[str, int | None]] = []
        self.polled = 0
        self.detail_polled = 0
        self.submit_kwargs: list[dict[str, object]] = []
        # The scheduler id the job recorded for itself, as the real dispatcher
        # would read it back off the shared workspace. None ⇒ nothing recorded.
        self.recorded: str | None = None
        self.recorded_reads = 0
        self.receipt: SchedulerSubmitReceipt | None = None
        self.receipt_reads = 0
        self.retry_preparations: list[str] = []

    def remote_workspace(self, job_id: str) -> str:
        return f"/remote/{job_id}"

    def recorded_job_id(self, job_id: str) -> str | None:
        self.recorded_reads += 1
        return self.recorded

    def submit_receipt(self, job_id: str) -> SchedulerSubmitReceipt | None:
        self.receipt_reads += 1
        return self.receipt

    def prepare_retry_attempt(self, job_id: str) -> None:
        """Model exact removal of evidence from a proven-terminal attempt."""
        self.retry_preparations.append(job_id)
        self.recorded = None
        self.receipt = None

    def submit(
        self, *, job_id: str, command: list[str], cpus: int, **kw: object
    ) -> SchedulerHandle:
        self.submitted.append(job_id)
        self.submit_kwargs.append(dict(kw))
        if self.submit_error is not None:
            raise self.submit_error
        return SchedulerHandle(job_id=self.submit_id, remote_workspace=f"/remote/{job_id}")

    def poll(self, handles: list[SchedulerHandle]) -> dict[str, SchedulerPhase]:
        self.polled += 1
        if self.poll_error is not None:
            raise self.poll_error
        if self.phase_by_id is not None:
            return {
                handle.job_id: self.phase_by_id[handle.job_id]
                for handle in handles
            }
        return {h.job_id: self.phase for h in handles}

    def poll_with_evidence(self, handles: list[SchedulerHandle]) -> object:
        return SimpleNamespace(
            phases=self.poll(handles),
            explicitly_absent_job_ids=self.explicitly_absent_job_ids,
        )

    def poll_detail(self, handles: list[SchedulerHandle]) -> dict[str, QstatDetail]:
        self.detail_polled += 1
        if self.detail_error is not None:
            raise self.detail_error
        if self.detail_by_id is not None:
            return {
                handle.job_id: self.detail_by_id[handle.job_id]
                for handle in handles
                if handle.job_id in self.detail_by_id
            }
        if self.detail is None:
            return {}
        return {h.job_id: self.detail for h in handles}

    def phase_from_detail(self, detail: QstatDetail) -> SchedulerPhase | None:
        if not detail.raw_state:
            return None
        # sacct can annotate states ("CANCELLED by 1234"); classify by the
        # first token, like the real dialects.
        state = detail.raw_state.split(maxsplit=1)[0]
        if state in {"Q", "W", "H", "T", "PENDING", "CONFIGURING"}:
            return SchedulerPhase.PENDING
        if state in {"R", "E", "S", "RUNNING", "COMPLETING"}:
            return SchedulerPhase.RUNNING
        if state in {
            "C",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "OUT_OF_MEMORY",
        }:
            return SchedulerPhase.FINISHED
        raise SchedulerError(f"unknown mock scheduler state {detail.raw_state!r}")

    def abnormal_termination_from_detail(self, detail: QstatDetail) -> str | None:
        # Delegate to the real Slurm classifier so daemon-level tests exercise
        # the production classification (Torque-flavored mock states like "C"
        # never classify, exactly as with the real TorqueDialect).
        if not detail.raw_state:
            return None
        return SlurmDialect().abnormal_termination(detail.raw_state)

    def cancel(self, handle: SchedulerHandle) -> None:
        self.cancelled.append(handle.job_id)

    def cleanup_remote_workspace(self, handle: SchedulerHandle) -> None:
        self.cleaned.append((handle.job_id, handle.remote_workspace, handle.array_size))

    def exit_marker_code(
        self, handle: SchedulerHandle, *, array_index: int | None = None
    ) -> int | None:
        self.marker_queries.append((handle.job_id, array_index))
        if self.marker_error is not None:
            raise self.marker_error
        if handle.array_size is None and array_index is not None:
            self.last_marker_rc = None
            return None
        if self.marker_rcs is not None:
            if self.marker_rcs:
                self.last_marker_rc = self.marker_rcs.pop(0)
            return self.last_marker_rc
        self.last_marker_rc = self.rc
        return self.rc

    def fetch_results(self, handle: SchedulerHandle, local_dir: Path) -> None:
        self.fetched.append(handle.job_id)
        if self.fetch_error is not None:
            raise self.fetch_error
        marker_rc = self.last_marker_rc if self.last_marker_rc is not None else self.rc
        if marker_rc is not None:
            marker = Path(local_dir) / "_vq" / "exit-code"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(str(marker_rc))

    def record_terminal_resource_sample(
        self,
        _handle: SchedulerHandle,
        _local_dir: Path,
    ) -> bool:
        result = self.telemetry_results[
            min(self.telemetry_calls, len(self.telemetry_results) - 1)
        ]
        self.telemetry_calls += 1
        return result

    def missing_marker_diagnostics(
        self, handle: SchedulerHandle, *, array_index: int | None = None
    ) -> dict[str, object]:
        self.diagnostics.append((handle.job_id, array_index))
        marker = f"{handle.remote_workspace}/_vq/exit-code"
        if array_index is not None:
            marker = f"{marker}.{array_index}"
        return {
            "scheduler_job_id": handle.job_id,
            "remote_workspace": handle.remote_workspace,
            "remote_exit_marker": marker,
            "remote_workspace_listing": "remote listing",
            "remote_stdout_tail": "remote stdout",
            "remote_stderr_tail": "remote stderr",
        }


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs"
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d


def _submit_scheduler(
    daemon: Daemon, jobid: str, *, target: str = "host_f", cpus: int = 1
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid, command=["true"], cwd=str(workspace), cpus=cpus, scheduler_target=target
    )
    spec.write(daemon._spec_path(jobid))
    return spec


# How long a deliberately-blocked operation stays blocked. The tests that
# assert "a slow X does not serialize Y" set this on the blocking mock, so an
# operation that HAD been serialized behind it would take about this long.
_BLOCKING_SECONDS = 2.0

# Budget for a spin loop or a join that is only waiting for work to finish.
# These are liveness guards: they exist so a genuine hang fails the test
# instead of blocking the suite, and their value carries no meaning beyond
# "longer than this is certainly broken". They must be generous. At 1.0s they
# were not, and CI failed twice in a row on a loaded shared runner while the
# same tests passed locally in milliseconds (issue #5).
#
# Do not use this for a timing assertion that is actually under test. Those
# time one specific call and compare against _BLOCKING_SECONDS.
_LIVENESS_SECONDS = 30.0


def _inject(daemon: Daemon, mock: MockDispatcher, target: str = "host_f") -> None:
    daemon._scheduler_dispatchers[target] = mock  # type: ignore[assignment]


class ControlledSchedulerTransfers:
    """Real reconciliation with poll/transfer completion controlled by the test.

    No sleeps or SSH: starting a transfer parks its actual worker callback until
    ``complete`` runs it. Polls still use the daemon's observation builder, but
    are delivered already complete so group order is the only admission race.
    """

    def __init__(self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch) -> None:
        self.daemon = daemon
        self.workers: dict[str, Callable[[], None]] = {}
        self.admissions: list[str] = []
        controller = self

        class Thread:
            def __init__(
                self, *, target: Callable[[], None], name: str, daemon: bool,
            ) -> None:
                assert name == "vq-scheduler-fetch"
                assert daemon is True
                self.target = target

            def start(self) -> None:
                new = set(controller.daemon._scheduler_fetch_flights) - set(
                    controller.admissions
                )
                assert len(new) == 1
                jobid = new.pop()
                controller.workers[jobid] = self.target
                controller.admissions.append(jobid)
                assert len(controller.workers) <= 2

        monkeypatch.setattr(daemon_mod.threading, "Thread", Thread)
        daemon._background_scheduler_polling = True

    def add(self, dispatcher: MockDispatcher, jobid: str, target: str) -> JobSpec:
        _inject(self.daemon, dispatcher, target)
        dispatcher.submit_id = f"{jobid}.cluster"
        if dispatcher.phase_by_id is not None:
            dispatcher.phase_by_id.setdefault(dispatcher.submit_id, SchedulerPhase.FINISHED)
        spec = _submit_scheduler(self.daemon, jobid, target=target)
        self.daemon._start_scheduler_job(spec)
        return spec

    def poll(self) -> None:
        groups: dict[object, list[tuple[str, object]]] = {}
        for jobid, job in self.daemon._scheduler_running.items():
            groups.setdefault(job.dispatcher, []).append((jobid, job))
        for dispatcher, items in groups.items():
            done = threading.Event()
            done.set()
            self.daemon._scheduler_poll_flights[dispatcher] = (
                daemon_mod._SchedulerPollFlight(
                    tuple(items), 0, done,
                    self.daemon._observe_scheduler_dispatcher(
                        dispatcher, tuple(items), refresh_sequence=0,
                    ),
                )
            )
        self.daemon._reconcile_scheduler()

    def complete(self, jobid: str) -> None:
        self.workers.pop(jobid)()
        assert self.daemon._scheduler_fetch_flights[jobid].done.is_set()


def test_later_dispatcher_gets_fetch_turn_despite_continuing_early_arrivals(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two completed transfers per round cannot starve a ready later host."""
    control = ControlledSchedulerTransfers(daemon, monkeypatch)
    early = MockDispatcher(phase_by_id={"anchor.cluster": SchedulerPhase.RUNNING})
    later = MockDispatcher()
    control.add(early, "anchor", "early")
    for index in range(4):
        control.add(early, f"early{index}", "early")
    late_spec = control.add(later, "later0", "later")
    control.poll()
    assert control.admissions == ["early0", "early1"]
    assert set(control.workers) == {"early0", "early1"}
    assert not later.marker_queries  # Full capacity does not probe another marker.
    waiting = JobSpec.read(daemon._spec_path(late_spec.id))
    assert waiting.state is JobState.RUNNING
    assert waiting.scheduler_state == "finishing"
    assert daemon._scheduler_running[late_spec.id].fetch_failure_misses == 0
    trace = []
    for turn in range(4):
        for jobid in tuple(control.workers):
            control.complete(jobid)
        for offset in range(2):
            control.add(early, f"arrival{turn}_{offset}", "early")
        control.poll()
        trace.append(tuple(control.workers))
        if late_spec.id in control.workers:
            break
    assert late_spec.id in control.workers, trace
    # With two eligible groups it must receive the next capacity opportunity.
    assert len(trace) == 1
    control.complete(late_spec.id)
    control.poll()
    result = JobSpec.read(daemon._spec_path(late_spec.id))
    assert result.state is JobState.COMPLETED
    assert result.exit_code == 0
    assert late_spec.id not in daemon._scheduler_running
    assert later.fetched == ["later0.cluster"]


def test_mid_pass_completion_cannot_give_recently_served_group_another_turn(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capacity that opens after a waiting group was visited belongs next pass."""
    control = ControlledSchedulerTransfers(daemon, monkeypatch)
    early, later = MockDispatcher(), MockDispatcher()
    for index in range(4):
        control.add(early, f"early{index}", "early")
    late_spec = control.add(later, "later0", "later")
    control.poll()
    original_read = daemon._read_active_spec

    def read_and_complete(jobid: str, job: object, **kwargs: object) -> object:
        result = original_read(jobid, job, **kwargs)
        # The later group sees both slots busy; they finish while the already
        # served earlier group is being processed. The following pass must
        # still start with the later group, not spend newly freed slots here.
        if jobid == "early0" and "early0" in control.workers:
            control.complete("early0")
            control.complete("early1")
        return result

    monkeypatch.setattr(daemon, "_read_active_spec", read_and_complete)
    control.poll()
    assert control.admissions == ["early0", "early1"]
    assert late_spec.id in daemon._scheduler_running
    control.poll()
    assert control.admissions[2] == late_spec.id


@pytest.mark.parametrize("blocked", ["poll_unknown", "marker_missing"])
def test_unready_dispatcher_does_not_block_other_fetch_admission(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, blocked: str,
) -> None:
    control = ControlledSchedulerTransfers(daemon, monkeypatch)
    early = MockDispatcher(
        poll_error=SchedulerError("unknown") if blocked == "poll_unknown" else None,
        rc=None if blocked == "marker_missing" else 0,
    )
    later = MockDispatcher()
    waiting = control.add(early, "unknown0", "early")
    control.add(later, "later0", "later")
    control.poll()
    assert control.admissions == ["later0"]
    assert waiting.id in daemon._scheduler_running
    assert JobSpec.read(daemon._spec_path(waiting.id)).state is JobState.RUNNING
    assert daemon._scheduler_running[waiting.id].fetch_failure_misses == 0
    control.complete("later0")
    control.poll()
    assert JobSpec.read(daemon._spec_path("later0")).state is JobState.COMPLETED
    assert waiting.id in daemon._scheduler_running


def test_new_dispatcher_arrivals_join_behind_an_existing_fetch_waiter(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = ControlledSchedulerTransfers(daemon, monkeypatch)
    early, later = MockDispatcher(), MockDispatcher()
    for index in range(4):
        control.add(early, f"early{index}", "early")
    control.add(later, "later0", "later")
    control.poll()
    for jobid in tuple(control.workers):
        control.complete(jobid)
    for index in range(3):
        control.add(MockDispatcher(), f"new{index}", f"newgroup{index}")
    control.poll()
    assert control.admissions[2] == "later0"
    assert len(control.workers) == 2


def test_failed_reconciliation_does_not_leave_fetch_admission_budget_behind(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = ControlledSchedulerTransfers(daemon, monkeypatch)
    early, later = MockDispatcher(), MockDispatcher()
    control.add(early, "early0", "early")
    control.add(early, "early1", "early")
    later_spec = control.add(later, "later0", "later")
    original_read = daemon._read_active_spec

    def read_or_fail(jobid: str, job: object, **kwargs: object) -> object:
        if jobid == "later0":
            raise RuntimeError("injected after capacity was consumed")
        return original_read(jobid, job, **kwargs)

    monkeypatch.setattr(daemon, "_read_active_spec", read_or_fail)
    with pytest.raises(RuntimeError, match="injected after capacity"):
        control.poll()
    monkeypatch.setattr(daemon, "_read_active_spec", original_read)
    for jobid in tuple(control.workers):
        control.complete(jobid)
    # Existing direct callers must still use current capacity outside a pass.
    daemon._fetch_scheduler_results(
        later_spec.id, daemon._scheduler_running[later_spec.id], later_spec,
        log_failure=lambda exc: pytest.fail(str(exc)),
    )
    assert control.admissions == ["early0", "early1", "later0"]
    assert daemon._scheduler_running[later_spec.id].fetch_failure_misses == 0


def test_blocked_terminal_fetch_does_not_block_new_dispatch(daemon: Daemon) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingFetch(MockDispatcher):
        def fetch_results(self, handle: SchedulerHandle, local_dir: Path) -> None:
            entered.set()
            release.wait(timeout=_BLOCKING_SECONDS)
            super().fetch_results(handle, local_dir)

    slow = BlockingFetch(submit_id="old.cluster")
    fast = MockDispatcher(submit_id="new.cluster", phase=SchedulerPhase.RUNNING)
    _inject(daemon, slow, "host_c")
    _inject(daemon, fast, "host_f")
    old = _submit_scheduler(daemon, "old", target="host_c")
    daemon._start_scheduler_job(old)
    _submit_scheduler(daemon, "new", target="host_f")
    daemon._background_scheduler_polling = True
    try:
        started = time.monotonic()
        deadline = started + _LIVENESS_SECONDS
        while not entered.is_set():
            daemon._reconcile_scheduler()
            assert time.monotonic() < deadline
            time.sleep(0.001)
        dispatch_started = time.monotonic()
        daemon._dispatch_pending()
        # The property under test: dispatch did not serialize behind the
        # blocked fetch. Timed over the dispatch call alone -- measuring from
        # `started` also charged it for the spin loop above, which is a
        # liveness wait and not part of the claim.
        assert time.monotonic() - dispatch_started < _BLOCKING_SECONDS / 2
        assert JobSpec.read(daemon._spec_path("new")).scheduler_job_id == "new.cluster"
        assert JobSpec.read(daemon._spec_path("old")).state == JobState.RUNNING
        assert slow.fetched == []
        for _ in range(3):
            daemon._reconcile_scheduler()
        assert len(daemon._scheduler_fetch_flights) == 1
    finally:
        release.set()
        for flight in getattr(daemon, "_scheduler_fetch_flights", {}).values():
            assert flight.done.wait(timeout=_LIVENESS_SECONDS)
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while JobSpec.read(daemon._spec_path("old")).state != JobState.COMPLETED:
        daemon._reconcile_scheduler()
        assert time.monotonic() < deadline
        time.sleep(0.001)
    assert slow.fetched == ["old.cluster"]


def test_background_fetch_is_bounded_and_does_not_count_waits_as_failures(
    daemon: Daemon,
) -> None:
    release = threading.Event()

    class BlockingFetch(MockDispatcher):
        def fetch_results(self, handle: SchedulerHandle, local_dir: Path) -> None:
            release.wait(timeout=_BLOCKING_SECONDS)
            super().fetch_results(handle, local_dir)

    specs = []
    for i in range(3):
        mock = BlockingFetch(submit_id=f"{i}.cluster")
        _inject(daemon, mock, f"target{i}")
        spec = _submit_scheduler(daemon, f"job{i}", target=f"target{i}")
        daemon._start_scheduler_job(spec)
        specs.append(spec)
    daemon._background_scheduler_polling = True
    try:
        for _ in range(3):
            for spec in specs:
                job = daemon._scheduler_running[spec.id]
                outcome = daemon._fetch_scheduler_results(
                    spec.id, job, spec, log_failure=lambda exc: pytest.fail(str(exc)),
                )
                assert outcome is daemon_mod._SchedulerFetchOutcome.RETRY
                assert job.fetch_failure_misses == 0
        assert len(daemon._scheduler_fetch_flights) == 2
    finally:
        release.set()
        for flight in daemon._scheduler_fetch_flights.values():
            assert flight.done.wait(timeout=_LIVENESS_SECONDS)


@pytest.mark.parametrize('prior_phase', [None, 'running', 'fetch_failed', 'marker_probe_failed'])
def test_terminal_observation_survives_fetch_capacity_wait_and_retry(daemon, prior_phase):
    from vq.listing import effective_queue_state, scheduler_running_confirmed

    release = threading.Event()
    retry_release = threading.Event()
    retry_entered = threading.Event()
    dispatchers = []
    specs = []

    class BlockingFetch(MockDispatcher):
        attempts = 0

        def fetch_results(self, handle, local_dir):
            self.attempts += 1
            if self.submit_id == '2.cluster':
                if self.attempts == 1:
                    raise SchedulerError('injected final fetch failure')
                retry_entered.set()
                assert retry_release.wait(_LIVENESS_SECONDS)
            else:
                assert release.wait(_LIVENESS_SECONDS)
            super().fetch_results(handle, local_dir)

    for i in range(3):
        dispatcher = BlockingFetch(submit_id=f'{i}.cluster')
        _inject(daemon, dispatcher, f'target{i}')
        spec = _submit_scheduler(daemon, f'phase-job{i}', target=f'target{i}')
        daemon._start_scheduler_job(spec)
        specs.append(spec)
        dispatchers.append(dispatcher)
    third_path = daemon._spec_path(specs[2].id)
    third = JobSpec.read(third_path)
    third.scheduler_state = prior_phase
    third.write(third_path)
    daemon._background_scheduler_polling = True
    for spec in specs[:2]:
        daemon._fetch_scheduler_results(
            spec.id, daemon._scheduler_running[spec.id], spec,
            log_failure=lambda exc: pytest.fail(str(exc)),
        )

    def advance_until(condition):
        deadline = time.monotonic() + _LIVENESS_SECONDS
        while not condition():
            daemon._reconcile_scheduler()
            assert time.monotonic() < deadline
            time.sleep(0.001)

    try:
        advance_until(lambda: len(daemon._scheduler_fetch_flights) == 2
                      and JobSpec.read(third_path).scheduler_poll_last_success_at is not None)
        waiting = JobSpec.read(third_path)
        expected = (
            prior_phase if prior_phase in {'fetch_failed', 'marker_probe_failed'} else 'finishing'
        )
        assert waiting.scheduler_state == expected
        assert effective_queue_state(waiting) == expected
        assert scheduler_running_confirmed(waiting) is False
        assert waiting.state == JobState.RUNNING
        assert waiting.finished_at is None and waiting.exit_code is None
        assert len(daemon._scheduler_running) == 3
        assert dispatchers[2].marker_queries == []
        assert daemon._scheduler_running[waiting.id].fetch_failure_misses == 0

        release.set()
        advance_until(retry_entered.is_set)
        failed = JobSpec.read(third_path)
        assert failed.scheduler_state == 'fetch_failed'
        assert failed.state == JobState.RUNNING and failed.finished_at is None
        assert daemon._scheduler_running[failed.id].fetch_failure_misses == 1
        # Successful scheduler polls during a blocked retry must not erase
        # the diagnostic from the completed failed fetch.
        previous_poll = failed.scheduler_poll_last_success_at
        advance_until(
            lambda: JobSpec.read(third_path).scheduler_poll_last_success_at != previous_poll
        )
        assert JobSpec.read(third_path).scheduler_state == 'fetch_failed'
        retry_release.set()
        advance_until(
            lambda: all(JobSpec.read(daemon._spec_path(s.id)).is_terminal for s in specs)
        )
        assert all(
            JobSpec.read(daemon._spec_path(s.id)).state == JobState.COMPLETED for s in specs
        )
        assert dispatchers[2].attempts == 2
        assert not daemon._scheduler_running
    finally:
        release.set()
        retry_release.set()
        for flight in daemon._scheduler_fetch_flights.values():
            assert flight.done.wait(_LIVENESS_SECONDS)


@pytest.mark.parametrize("error", [SchedulerError("transfer failed"), ValueError("unexpected")])
def test_background_fetch_errors_are_consumed_on_main_loop(
    daemon: Daemon, error: Exception,
) -> None:
    mock = MockDispatcher(fetch_error=error)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "fetch-error")
    daemon._start_scheduler_job(spec)
    job = daemon._scheduler_running[spec.id]
    daemon._background_scheduler_polling = True
    errors = []
    outcome = daemon._fetch_scheduler_results(spec.id, job, spec, log_failure=errors.append)
    assert outcome is daemon_mod._SchedulerFetchOutcome.RETRY
    assert daemon._scheduler_fetch_flights[spec.id].done.wait(timeout=_LIVENESS_SECONDS)
    assert errors == [] and job.fetch_failure_misses == 0
    if isinstance(error, SchedulerError):
        outcome = daemon._fetch_scheduler_results(spec.id, job, spec, log_failure=errors.append)
        assert outcome is daemon_mod._SchedulerFetchOutcome.RETRY
        assert errors == [error] and job.fetch_failure_misses == 1
    else:
        with pytest.raises(ValueError, match="unexpected"):
            daemon._fetch_scheduler_results(spec.id, job, spec, log_failure=errors.append)
    assert spec.id not in daemon._scheduler_fetch_flights


def test_background_fetch_does_not_revive_an_external_kill(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "killed-fetch")
    daemon._start_scheduler_job(spec)
    daemon._background_scheduler_polling = True
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while spec.id not in daemon._scheduler_fetch_flights:
        daemon._reconcile_scheduler()
        assert time.monotonic() < deadline
        time.sleep(0.001)
    flight = daemon._scheduler_fetch_flights[spec.id]
    assert flight.done.wait(timeout=_LIVENESS_SECONDS)
    killed = JobSpec.read(daemon._spec_path(spec.id))
    killed.state = JobState.KILLED
    killed.write(daemon._spec_path(spec.id))
    while spec.id in daemon._scheduler_running:
        daemon._reconcile_scheduler()
        assert time.monotonic() < deadline
        time.sleep(0.001)
    assert JobSpec.read(daemon._spec_path(spec.id)).state == JobState.KILLED
    daemon._reconcile_scheduler()
    assert not daemon._scheduler_fetch_flights


@pytest.mark.parametrize("change", ["workspace", "handle", "dispatcher"])
def test_background_fetch_result_requires_the_same_binding(
    daemon: Daemon, change: str,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "changed-binding")
    daemon._start_scheduler_job(spec)
    job = daemon._scheduler_running[spec.id]
    daemon._background_scheduler_polling = True
    kwargs = {"log_failure": lambda exc: pytest.fail(str(exc))}
    daemon._fetch_scheduler_results(spec.id, job, spec, **kwargs)
    assert daemon._scheduler_fetch_flights[spec.id].done.wait(timeout=_LIVENESS_SECONDS)
    if change == "workspace":
        spec.cwd = str(daemon.jobs_dir / "replacement")
    elif change == "handle":
        job.handle = SchedulerHandle("replacement.cluster", "/replacement")
    else:
        job.dispatcher = MockDispatcher()  # type: ignore[assignment]

    outcome = daemon._fetch_scheduler_results(spec.id, job, spec, **kwargs)

    assert outcome is daemon_mod._SchedulerFetchOutcome.RETRY
    assert spec.id not in daemon._scheduler_fetch_flights
    assert JobSpec.read(daemon._spec_path(spec.id)).state == JobState.RUNNING


def test_background_fetch_retry_limit_counts_completed_failures_only(daemon: Daemon) -> None:
    mock = MockDispatcher(fetch_error=SchedulerError("offline"))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "retry-limit")
    daemon._start_scheduler_job(spec)
    job = daemon._scheduler_running[spec.id]
    daemon._background_scheduler_polling = True
    errors = []
    for attempt in range(daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT):
        assert daemon._fetch_scheduler_results(
            spec.id, job, spec, log_failure=errors.append,
        ) is daemon_mod._SchedulerFetchOutcome.RETRY
        assert daemon._scheduler_fetch_flights[spec.id].done.wait(timeout=_LIVENESS_SECONDS)
        outcome = daemon._fetch_scheduler_results(
            spec.id, job, spec, log_failure=errors.append,
        )
        assert job.fetch_failure_misses == attempt + 1
        assert outcome is (
            daemon_mod._SchedulerFetchOutcome.UNAVAILABLE
            if attempt + 1 == daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
            else daemon_mod._SchedulerFetchOutcome.RETRY
        )
    assert len(errors) == daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT


def test_scheduler_status_refresh_waits_for_a_post_request_poll(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "j1"))
    results: list[dict[str, object]] = []

    def request() -> None:
        results.append(daemon.request_scheduler_status_refresh("j1", 1.0))

    thread = threading.Thread(target=request)
    thread.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    assert thread.is_alive()
    daemon._reconcile_scheduler()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert results == [
        {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": True,
            "observed_at": results[0]["observed_at"],
        }
    ]
    assert isinstance(results[0]["observed_at"], str)


def test_background_scheduler_poll_isolates_a_stalled_host(
    daemon: Daemon,
) -> None:
    poll_started = threading.Event()
    release_poll = threading.Event()

    class BlockingDispatcher(MockDispatcher):
        def poll(
            self, handles: list[SchedulerHandle]
        ) -> dict[str, SchedulerPhase]:
            self.polled += 1
            poll_started.set()
            release_poll.wait(timeout=_BLOCKING_SECONDS)
            return {handle.job_id: self.phase for handle in handles}

    slow = BlockingDispatcher(
        submit_id="slow.cluster",
        phase=SchedulerPhase.RUNNING,
    )
    fast = MockDispatcher(
        submit_id="fast.cluster",
        phase=SchedulerPhase.RUNNING,
    )
    _inject(daemon, slow, target="host_c")
    _inject(daemon, fast, target="host_f")
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "slow-vq", target="host_c")
    )
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "fast-vq", target="host_f")
    )
    daemon._background_scheduler_polling = True

    try:
        started_at = time.monotonic()
        daemon._reconcile_scheduler()
        assert time.monotonic() - started_at < _BLOCKING_SECONDS / 2
        assert poll_started.wait(timeout=_LIVENESS_SECONDS)

        deadline = time.monotonic() + _LIVENESS_SECONDS
        while fast.detail_polled == 0:
            assert time.monotonic() < deadline
            time.sleep(0.001)

        daemon._reconcile_scheduler()

        assert JobSpec.read(daemon._spec_path("fast-vq")).scheduler_state == "running"
        slow_on_disk = JobSpec.read(daemon._spec_path("slow-vq"))
        assert slow_on_disk.scheduler_state is None
        assert slow_on_disk.scheduler_poll_last_attempted_at is None
        daemon._reconcile_scheduler()
        assert slow.polled == 1
    finally:
        release_poll.set()


@pytest.mark.parametrize(
    "runner_error",
    [
        OSError("ssh executable disappeared"),
        subprocess.SubprocessError("scheduler child process failed"),
    ],
    ids=["os-error", "subprocess-error"],
)
def test_background_scheduler_poll_runner_errors_are_host_local(
    daemon: Daemon,
    runner_error: Exception,
) -> None:
    broken = MockDispatcher(
        submit_id="broken.cluster",
        phase=SchedulerPhase.RUNNING,
        poll_error=runner_error,
    )
    healthy = MockDispatcher(
        submit_id="healthy.cluster",
        phase=SchedulerPhase.RUNNING,
    )
    _inject(daemon, broken, target="broken")
    _inject(daemon, healthy, target="healthy")
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "broken-vq", target="broken")
    )
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "healthy-vq", target="healthy")
    )
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while not all(
        flight.done.is_set()
        for flight in daemon._scheduler_poll_flights.values()
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    daemon._reconcile_scheduler()

    broken_on_disk = JobSpec.read(daemon._spec_path("broken-vq"))
    healthy_on_disk = JobSpec.read(daemon._spec_path("healthy-vq"))
    assert broken_on_disk.state == JobState.RUNNING
    assert broken_on_disk.scheduler_state == "poll_failed"
    assert broken_on_disk.scheduler_poll_last_error is not None
    assert "broken-vq" in daemon._scheduler_running
    assert healthy_on_disk.scheduler_state == "running"


@pytest.mark.parametrize(
    "runner_error",
    [
        OSError("ssh executable disappeared"),
        subprocess.SubprocessError("scheduler child process failed"),
    ],
    ids=["os-error", "subprocess-error"],
)
def test_background_scheduler_detail_runner_errors_are_host_local(
    daemon: Daemon,
    runner_error: Exception,
) -> None:
    broken = MockDispatcher(
        submit_id="broken.cluster",
        phase=SchedulerPhase.RUNNING,
        detail_error=runner_error,
    )
    healthy = MockDispatcher(
        submit_id="healthy.cluster",
        phase=SchedulerPhase.RUNNING,
    )
    _inject(daemon, broken, target="broken")
    _inject(daemon, healthy, target="healthy")
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "broken-vq", target="broken")
    )
    daemon._start_scheduler_job(
        _submit_scheduler(daemon, "healthy-vq", target="healthy")
    )
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while not all(
        flight.done.is_set()
        for flight in daemon._scheduler_poll_flights.values()
    ):
        assert time.monotonic() < deadline
        time.sleep(0.001)
    daemon._reconcile_scheduler()

    broken_on_disk = JobSpec.read(daemon._spec_path("broken-vq"))
    healthy_on_disk = JobSpec.read(daemon._spec_path("healthy-vq"))
    assert broken_on_disk.state == JobState.RUNNING
    assert broken_on_disk.scheduler_state == "running"
    assert "broken-vq" in daemon._scheduler_running
    assert healthy_on_disk.scheduler_state == "running"


def test_background_status_refresh_rejects_a_pre_request_flight(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        submit_id="refresh.cluster",
        phase=SchedulerPhase.RUNNING,
    )
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "refresh-vq"))
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while mock.detail_polled < 1:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh("refresh-vq", 1.0)
        )
    )
    request.start()
    while daemon._scheduler_refresh_requested < 1:
        assert time.monotonic() < deadline
        time.sleep(0.001)

    daemon._reconcile_scheduler()  # consume the pre-request flight
    assert request.is_alive()
    daemon._reconcile_scheduler()  # start the post-request flight
    while mock.detail_polled < 2:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    daemon._reconcile_scheduler()
    request.join(timeout=1.0)

    assert not request.is_alive()
    assert results[0]["completed"] is True
    assert isinstance(results[0]["observed_at"], str)


def test_scheduler_status_refresh_times_out_without_a_new_poll(
    daemon: Daemon,
) -> None:
    result = daemon.request_scheduler_status_refresh("j1", 0.01)

    assert result == {
        "schema": "vq.scheduler.status_refresh/1",
        "completed": False,
        "observed_at": None,
        "reason": "timeout",
    }


def test_scheduler_status_refresh_stop_wakes_waiter(daemon: Daemon) -> None:
    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh("j1", 5.0)
        )
    )
    request.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    daemon.stop()
    request.join(timeout=1.0)

    assert not request.is_alive()
    assert results == [
        {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": False,
            "observed_at": None,
            "reason": "daemon_stopping",
        }
    ]


@pytest.mark.parametrize("timeout", [True, 0, -1, float("nan"), float("inf"), 31])
def test_scheduler_status_refresh_rejects_unbounded_timeouts(
    daemon: Daemon, timeout: object
) -> None:
    with pytest.raises(ValueError, match="at most 30"):
        daemon.request_scheduler_status_refresh(  # type: ignore[arg-type]
            "j1", timeout
        )


def test_scheduler_status_refresh_does_not_adopt_an_inflight_old_poll(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "j1"))
    poll_started = threading.Event()
    release_poll = threading.Event()
    original_pass = daemon._reconcile_scheduler_pass  # noqa: SLF001

    def held_pass() -> dict[str, str]:
        poll_started.set()
        assert release_poll.wait(1.0)
        return {"j1": "2026-08-11T12:00:00+00:00"}

    monkeypatch.setattr(daemon, "_reconcile_scheduler_pass", held_pass)
    old_poll = threading.Thread(target=daemon._reconcile_scheduler)
    old_poll.start()
    assert poll_started.wait(1.0)

    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh("j1", 1.0)
        )
    )
    request.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    release_poll.set()
    old_poll.join(timeout=1.0)
    assert not old_poll.is_alive()
    assert request.is_alive()

    monkeypatch.setattr(daemon, "_reconcile_scheduler_pass", original_pass)
    daemon._reconcile_scheduler()
    request.join(timeout=1.0)

    assert not request.is_alive()
    assert results[0]["completed"] is True


def test_scheduler_status_refresh_rejects_untracked_job_as_fresh(
    daemon: Daemon,
) -> None:
    spec = _submit_scheduler(daemon, "untracked")
    spec.state = JobState.RUNNING
    spec.scheduler_job_id = "123"
    spec.scheduler_state = "reattach_failed"
    spec.write(daemon._spec_path(spec.id))
    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh(spec.id, 1.0)
        )
    )
    request.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    daemon._reconcile_scheduler()
    request.join(timeout=1.0)

    assert results == [
        {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": False,
            "observed_at": None,
            "reason": "job_not_observed",
        }
    ]
    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.scheduler_poll_last_attempted_at is None
    assert on_disk.scheduler_poll_last_success_at is None


def test_scheduler_status_refresh_does_not_acknowledge_a_different_job(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh("requested", 1.0)
        )
    )
    request.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    monkeypatch.setattr(
        daemon,
        "_reconcile_scheduler_pass",
        lambda: {"different": "2026-08-11T12:00:00+00:00"},
    )
    daemon._reconcile_scheduler()
    request.join(timeout=1.0)

    assert not request.is_alive()
    assert results == [
        {
            "schema": "vq.scheduler.status_refresh/1",
            "completed": False,
            "observed_at": None,
            "reason": "job_not_observed",
        }
    ]


def test_scheduler_status_refresh_rejects_failed_poll_as_fresh(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(poll_error=SchedulerError("squeue unavailable"))
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "j1"))
    results: list[dict[str, object]] = []
    request = threading.Thread(
        target=lambda: results.append(
            daemon.request_scheduler_status_refresh("j1", 1.0)
        )
    )
    request.start()
    deadline = time.monotonic() + _LIVENESS_SECONDS
    while daemon._scheduler_refresh_requested == 0:  # noqa: SLF001
        assert time.monotonic() < deadline
        time.sleep(0.001)

    daemon._reconcile_scheduler()
    request.join(timeout=1.0)

    assert results[0]["completed"] is False
    assert results[0]["reason"] == "job_not_observed"
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.scheduler_state == "poll_failed"
    assert on_disk.scheduler_poll_last_success_at is None


def _tag_vq_array_element(
    daemon: Daemon, spec: JobSpec, *, index: int = 2, total: int = 5
) -> None:
    spec.array_index = index
    spec.array_total = total
    spec.array_group_id = "arraygrp"
    spec.write(daemon._spec_path(spec.id))


# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #


def test_start_scheduler_job_qsubs_and_tracks(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    assert daemon._start_scheduler_job(spec) is True
    # qsub happened, job is tracked, spec is RUNNING with the scheduler id.
    assert mock.submitted == ["j1"]
    assert mock.submit_kwargs[0].get("array_size") is None
    assert "j1" in daemon._scheduler_running
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "555.cluster"


def test_scheduler_claim_is_durable_before_qsub(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    observed_claims: list[JobSpec] = []
    orig_submit = mock.submit

    def observing_submit(**kw: object) -> SchedulerHandle:
        observed_claims.append(JobSpec.read(daemon._spec_path("j1")))
        return orig_submit(**kw)  # type: ignore[arg-type]

    mock.submit = observing_submit  # type: ignore[assignment]

    assert daemon._start_scheduler_job(spec) is True

    assert len(observed_claims) == 1
    claim = observed_claims[0]
    assert claim.state == JobState.SUBMITTING
    assert claim.scheduler_state == "submitting"
    assert claim.started_at is not None
    assert claim.scheduler_job_id is None
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "555.cluster"


def test_scheduler_phase_two_preserves_concurrent_nonterminal_update(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    orig_submit = mock.submit
    heartbeat = "2026-08-02T12:02:00+00:00"

    def updating_submit(**kw: object) -> SchedulerHandle:
        handle = orig_submit(**kw)  # type: ignore[arg-type]
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            updated = JobSpec.read(daemon._spec_path("j1"))
            assert updated.state == JobState.SUBMITTING
            assert updated.scheduler_job_id is None
            updated.scheduler_state = "queued"
            updated.last_heartbeat_at = heartbeat
            updated.write(daemon._spec_path("j1"))
        return handle

    mock.submit = updating_submit  # type: ignore[assignment]

    assert daemon._start_scheduler_job(spec) is True

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "555.cluster"
    assert on_disk.scheduler_state == "queued"
    assert on_disk.last_heartbeat_at == heartbeat


def test_scheduler_stale_pending_argument_does_not_submit_after_external_kill(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    stale = _submit_scheduler(daemon, "j1").model_copy(deep=True)
    finished_at = "2026-08-02T12:00:00+00:00"
    with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
        killed = JobSpec.read(daemon._spec_path("j1"))
        killed.state = JobState.KILLED
        killed.finished_at = finished_at
        killed.failure_reason = "operator killed before scheduler claim"
        killed.write(daemon._spec_path("j1"))

    assert daemon._start_scheduler_job(stale) is False

    assert mock.submitted == []
    assert mock.cancelled == []
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.finished_at == finished_at
    assert on_disk.failure_reason == "operator killed before scheduler claim"


def test_scheduler_submit_failure_preserves_scheduler_diagnostics(
    daemon: Daemon,
) -> None:
    error = SchedulerError(
        "sbatch failed (exit 1) for rp211:\n"
        "  stderr: sbatch: error: Batch job submission failed: "
        "Requested time limit is invalid (missing or exceeds some limit)"
    )
    mock = MockDispatcher(submit_error=error)
    _inject(daemon, mock, target="host_c")
    spec = _submit_scheduler(daemon, "rp211", target="host_c")
    spec.wall_time_seconds = 7200
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code is None
    assert on_disk.scheduler_job_id is None
    assert on_disk.failure_reason is not None
    assert "scheduler submit to host_c failed" in on_disk.failure_reason
    assert "Requested time limit is invalid" in on_disk.failure_reason


def test_multi_user_scheduler_rejection_uses_captured_safe_workspace(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, safe_workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    outside = tmp_path / "outside-owner-workspace"
    mock = MockDispatcher()

    def reject_after_cwd_race(**_kwargs: object) -> SchedulerHandle:
        raced = JobSpec.read(spec_path)
        raced.cwd = str(outside)
        raced.write(spec_path)
        raise SchedulerError("qsub committed rejection")

    mock.submit = reject_after_cwd_race  # type: ignore[method-assign]
    _inject(daemon, mock)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.FAILED
    assert on_disk.cwd == str(safe_workspace)
    assert on_disk.failure_reason is not None
    assert "qsub committed rejection" in on_disk.failure_reason
    assert not (outside / "_vq" / "events.jsonl").exists()
    assert (safe_workspace / "_vq" / "events.jsonl").is_file()


def test_multi_user_dispatcher_construction_failure_uses_captured_workspace(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, safe_workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    outside = tmp_path / "outside-owner-workspace"

    def fail_after_cwd_race(_target: str) -> MockDispatcher:
        raced = JobSpec.read(spec_path)
        raced.cwd = str(outside)
        raced.write(spec_path)
        raise config.ConfigError("scheduler host vanished")

    monkeypatch.setattr(daemon, "_scheduler_dispatcher_for", fail_after_cwd_race)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.FAILED
    assert on_disk.cwd == str(safe_workspace)
    assert not (outside / "_vq" / "events.jsonl").exists()
    assert (safe_workspace / "_vq" / "events.jsonl").is_file()


def test_scheduler_submit_ambiguity_is_nonterminal_and_not_replayed(
    daemon: Daemon,
) -> None:
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    mock = MockDispatcher(submit_error=unknown_type("qsub outcome is unknown"))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")

    assert daemon._start_scheduler_job(spec) is False
    assert daemon._start_scheduler_job(JobSpec.read(daemon._spec_path("j1"))) is False

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_state == "submit_outcome_unknown"
    assert not on_disk.is_terminal
    assert mock.submitted == ["j1"]


def test_scheduler_submit_unknown_reserves_capacity_indefinitely(
    daemon: Daemon,
) -> None:
    daemon.max_scheduler_jobs = 1
    unknown = _submit_scheduler(daemon, "unknown")
    unknown.state = JobState.SUBMIT_OUTCOME_UNKNOWN
    unknown.scheduler_state = "submit_outcome_unknown"
    unknown.started_at = (
        datetime.now(UTC)
        - timedelta(seconds=daemon_mod.SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS * 10)
    ).isoformat()
    unknown.write(daemon._spec_path("unknown"))
    _submit_scheduler(daemon, "next")
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._dispatch_pending()

    assert mock.submitted == []
    assert JobSpec.read(daemon._spec_path("next")).state == JobState.PENDING


def test_scheduler_submit_unknown_reserves_capacity_in_the_same_dispatch_scan(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(daemon_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        daemon_mod,
        "SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS",
        0.5,
    )
    daemon.max_scheduler_jobs = 1
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    mock = MockDispatcher(submit_error=unknown_type("qsub outcome is unknown"))
    _inject(daemon, mock)
    original_submit = mock.submit

    def slow_unknown_submit(**kwargs: object) -> SchedulerHandle:
        clock[0] += 0.6
        return original_submit(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mock, "submit", slow_unknown_submit)
    reconciled: list[float] = []
    monkeypatch.setattr(
        daemon,
        "_reconcile_scheduler",
        lambda: reconciled.append(clock[0]),
    )
    _submit_scheduler(daemon, "first")
    _submit_scheduler(daemon, "second")

    daemon._dispatch_pending()

    assert mock.submitted == ["first"]
    assert JobSpec.read(daemon._spec_path("first")).state == (
        JobState.SUBMIT_OUTCOME_UNKNOWN
    )
    assert JobSpec.read(daemon._spec_path("second")).state == JobState.PENDING
    assert reconciled == pytest.approx([0.6])


def test_immediately_recovered_ambiguous_submit_reserves_same_tick_capacity(
    daemon: Daemon,
) -> None:
    daemon.max_scheduler_jobs = 1
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    mock = MockDispatcher(submit_error=unknown_type("local observer lost"))
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)
    _submit_scheduler(daemon, "first")
    _submit_scheduler(daemon, "second")

    daemon._dispatch_pending()

    assert mock.submitted == ["first"]
    assert JobSpec.read(daemon._spec_path("first")).state == JobState.RUNNING
    assert JobSpec.read(daemon._spec_path("second")).state == JobState.PENDING


def _unconfirmed_scheduler_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.SUBMITTING,
) -> JobSpec:
    spec = _submit_scheduler(daemon, jobid)
    spec.state = state
    spec.scheduler_state = state.value
    spec.started_at = utcnow = datetime.now(UTC).isoformat()
    spec.last_heartbeat_at = utcnow
    spec.write(daemon._spec_path(jobid))
    return spec


def _write_multi_user_unconfirmed_scheduler_spec(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cwd: Path | None = None,
    cpus: int = 1,
    jobid: str = "j1",
    create_binding: bool = True,
) -> tuple[JobSpec, Path, Path]:
    owner = "1000"
    multi_root = tmp_path / "multi-user"
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_root))
    queue_dir = paths.user_queue_dir(owner)
    jobs_dir = paths.user_jobs_dir(owner)
    queue_dir.mkdir(parents=True)
    jobs_dir.mkdir(parents=True)
    workspace = cwd or (jobs_dir / jobid)
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=cpus,
        scheduler_target="host_f",
        state=JobState.SUBMITTING,
        scheduler_state="submitting",
        submitter=owner,
        started_at=datetime.now(UTC).isoformat(),
    )
    spec_path = paths.user_spec_path(owner, spec.id)
    spec.write(spec_path)
    daemon._multi_user = True
    list(daemon._iter_specs())
    if create_binding:
        daemon_mod._ensure_scheduler_submit_binding(owner, spec, "host_f")
    return spec, spec_path, workspace


def _scheduler_binding_path(owner: str, jobid: str) -> Path:
    return (
        daemon_mod._scheduler_submit_binding_directory()
        / daemon_mod._scheduler_submit_binding_name(owner, jobid)
    )


def _configure_multi_user_dispatch_test(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Quotas:
        @staticmethod
        def effective_max_pending_jobs(_uid: str) -> None:
            return None

        @staticmethod
        def effective_max_concurrent_cpus(_uid: str) -> None:
            return None

    class Config:
        quotas = Quotas()

    monkeypatch.setattr(daemon_mod, "load_config", lambda: Config())
    monkeypatch.setattr(drain, "read_effective_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(daemon, "_config_unusable", lambda: False)
    monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)


def test_startup_rejects_unconfirmed_multi_user_spec_before_remote_evidence_reads(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forged_workspace = tmp_path / "outside-owner-jobs"
    _spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        cwd=forged_workspace,
    )
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert not on_disk.is_terminal
    assert on_disk.failure_reason is not None
    assert "multi-user spec gate" in on_disk.failure_reason
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0
    assert not (forged_workspace / "_vq" / "events.jsonl").exists()


def test_dispatch_keeps_invalid_unknown_scheduler_submit_nonterminal_and_reserved(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outside = tmp_path / "outside-owner-jobs"
    _spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        cwd=outside,
    )
    unknown = JobSpec.read(spec_path)
    unknown.state = JobState.SUBMIT_OUTCOME_UNKNOWN
    unknown.scheduler_state = "submit_outcome_unknown"
    unknown.write(spec_path)
    owner = "1000"
    next_workspace = paths.user_jobs_dir(owner) / "j2"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="j2",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter=owner,
    )
    next_path = paths.user_spec_path(owner, "j2")
    next_spec.write(next_path)
    daemon.max_scheduler_jobs = 1
    mock = MockDispatcher()
    _inject(daemon, mock)
    _configure_multi_user_dispatch_test(daemon, monkeypatch)

    daemon._dispatch_pending()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert not on_disk.is_terminal
    assert JobSpec.read(next_path).state == JobState.PENDING
    assert mock.submitted == []
    assert not (outside / "_vq" / "events.jsonl").exists()


def test_startup_binds_unconfirmed_submit_to_captured_owner_path_and_target(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    mock = MockDispatcher()

    def race_target(_job_id: str) -> SchedulerSubmitReceipt:
        raced = JobSpec.read(spec_path)
        raced.scheduler_target = "host_c"
        raced.write(spec_path)
        return SchedulerSubmitReceipt("accepted", "18109.host_f", 0)

    mock.submit_receipt = race_target  # type: ignore[method-assign]
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert not on_disk.is_terminal
    assert on_disk.scheduler_target == "host_f"
    assert on_disk.scheduler_job_id is None
    assert on_disk.failure_reason is not None
    assert "remote submit evidence is untrusted" in (
        on_disk.failure_reason
    )
    assert mock.receipt_reads == 0
    assert "j1" not in daemon._scheduler_running


def test_multi_user_retry_refuses_unbound_remote_submit_receipt(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(spec_path).state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert "j1" not in daemon._scheduler_running

    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    daemon._scheduler_reattach_retry_last.clear()
    daemon._retry_deferred_scheduler_reattach()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_job_id is None
    assert "j1" not in daemon._scheduler_running
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0


def test_multi_user_matching_remote_receipt_and_marker_cannot_forge_authority(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "99999.host_f", 0)
    mock.recorded = "99999.host_f"
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    parked = JobSpec.read(spec_path)
    assert parked.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert parked.scheduler_job_id is None
    assert spec.id not in daemon._scheduler_running
    assert mock.cancelled == []
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0


def test_multi_user_phase_one_binding_captures_the_submitting_snapshot(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = "1000"
    multi_root = tmp_path / "multi-user"
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_root))
    queue_dir = paths.user_queue_dir(owner)
    jobs_dir = paths.user_jobs_dir(owner)
    queue_dir.mkdir(parents=True)
    workspace = jobs_dir / "j1"
    workspace.mkdir(parents=True)
    spec = JobSpec(
        id="j1",
        command=["true"],
        cwd=str(workspace),
        cpus=3,
        scheduler_target="host_f",
        submitter=owner,
    )
    spec_path = paths.user_spec_path(owner, spec.id)
    spec.write(spec_path)
    daemon._multi_user = True
    list(daemon._iter_specs())
    mock = MockDispatcher()
    _inject(daemon, mock)
    observed_bindings: list[daemon_mod._SchedulerSubmitBinding] = []
    original_submit = mock.submit

    def observe_binding(**kwargs: object) -> SchedulerHandle:
        binding = daemon_mod._read_scheduler_submit_binding(owner, spec.id)
        assert binding is not None
        observed_bindings.append(binding)
        return original_submit(**kwargs)  # type: ignore[arg-type]

    mock.submit = observe_binding  # type: ignore[method-assign]

    assert daemon._start_scheduler_job(spec) is True

    assert len(observed_bindings) == 1
    binding = observed_bindings[0]
    assert binding.admitted_spec.state == JobState.SUBMITTING
    assert binding.admitted_spec.scheduler_state == "submitting"
    assert binding.admitted_spec.started_at is not None
    assert binding.cpus == 3


def test_ambiguous_submit_deleted_spec_is_restored_but_not_remotely_trusted(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    first = MockDispatcher()

    def delete_then_lose_observer(**_kwargs: object) -> SchedulerHandle:
        spec_path.unlink()
        raise unknown_type("observer lost after possible acceptance")

    first.submit = delete_then_lose_observer  # type: ignore[method-assign]
    _inject(daemon, first)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(spec) is False
    assert not spec_path.exists()

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    second = MockDispatcher()
    second.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(restarted, second)

    restarted._reattach_or_interrupt_at_startup()

    restored = JobSpec.read(spec_path)
    assert restored.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert restored.scheduler_job_id is None
    assert spec.id not in restarted._scheduler_running
    assert second.receipt_reads == 0
    assert second.recorded_reads == 0
    restarted._queue_lock_fd.close()


def test_open_binding_restores_valid_wrong_inner_id_and_reserves_capacity(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    wrong = spec.model_copy(deep=True)
    wrong.id = "j2"
    wrong.write(spec_path)
    next_workspace = paths.user_jobs_dir("1000") / "next"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="next",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter="1000",
    )
    next_path = paths.user_spec_path("1000", next_spec.id)
    next_spec.write(next_path)

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    mock = MockDispatcher()
    _inject(restarted, mock)

    restarted._dispatch_pending()

    restored = JobSpec.read(spec_path)
    assert restored.id == spec.id
    assert restored.state in {
        JobState.SUBMITTING,
        JobState.SUBMIT_OUTCOME_UNKNOWN,
    }
    assert JobSpec.read(next_path).state == JobState.PENDING
    assert mock.submitted == []
    restarted._queue_lock_fd.close()


def test_open_bound_acceptance_survives_erased_terminal_scheduler_fields(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
    spec.state = JobState.KILLED
    spec.finished_at = datetime.now(UTC).isoformat()
    spec.scheduler_state = None
    spec.scheduler_job_id = None
    spec.write(spec_path)
    daemon._queue_lock_fd.close()

    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    mock = MockDispatcher()
    _inject(restarted, mock)

    restarted._reattach_or_interrupt_at_startup()

    final = JobSpec.read(spec_path)
    assert final.state == JobState.KILLED
    assert final.scheduler_state == "submit_cancelled_after_terminal"
    assert mock.cancelled == ["18109.host_f"]
    assert spec.id not in restarted._scheduler_running
    restarted._queue_lock_fd.close()


def test_accepted_submit_deleted_spec_is_restored_and_tracked(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, safe_workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    mock = MockDispatcher(submit_id="18109.host_f")
    original_submit = mock.submit

    def delete_after_acceptance(**kwargs: object) -> SchedulerHandle:
        handle = original_submit(**kwargs)  # type: ignore[arg-type]
        spec_path.unlink()
        return handle

    mock.submit = delete_after_acceptance  # type: ignore[method-assign]
    _inject(daemon, mock)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(spec) is True

    restored = JobSpec.read(spec_path)
    assert restored.state == JobState.RUNNING
    assert restored.cwd == str(safe_workspace)
    assert restored.scheduler_job_id == "18109.host_f"
    assert daemon._scheduler_running[spec.id].handle.job_id == "18109.host_f"


def test_startup_restores_captured_target_when_user_removes_it_mid_reconcile(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    mock = MockDispatcher()

    def remove_target(_job_id: str) -> SchedulerSubmitReceipt:
        raced = JobSpec.read(spec_path)
        raced.scheduler_target = None
        raced.write(spec_path)
        return SchedulerSubmitReceipt("accepted", "18109.host_f", 0)

    mock.submit_receipt = remove_target  # type: ignore[method-assign]
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert not on_disk.is_terminal
    assert on_disk.scheduler_target == "host_f"
    assert on_disk.scheduler_job_id is None


def test_bound_target_survives_user_erasure_between_ambiguous_submit_ticks(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    owner = "1000"
    next_workspace = paths.user_jobs_dir(owner) / "j2"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="j2",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter=owner,
    )
    next_path = paths.user_spec_path(owner, "j2")
    next_spec.write(next_path)
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    mock = MockDispatcher(submit_error=unknown_type("qsub outcome unknown"))
    _inject(daemon, mock)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(spec) is False
    raced = JobSpec.read(spec_path)
    raced.scheduler_target = None
    raced.write(spec_path)
    daemon.max_scheduler_jobs = 1
    _configure_multi_user_dispatch_test(daemon, monkeypatch)

    daemon._dispatch_pending()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_target == "host_f"
    assert JobSpec.read(next_path).state == JobState.PENDING
    assert mock.submitted == ["j1"]


def test_bound_target_survives_user_erasure_and_daemon_restart(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    first = MockDispatcher(submit_error=unknown_type("qsub outcome unknown"))
    _inject(daemon, first)
    list(daemon._iter_specs())
    assert daemon._start_scheduler_job(spec) is False
    raced = JobSpec.read(spec_path)
    raced.scheduler_target = None
    raced.write(spec_path)

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    second = MockDispatcher()
    _inject(restarted, second)

    restarted._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_target == "host_f"
    assert second.receipt_reads == 0
    assert second.recorded_reads == 0
    restarted._queue_lock_fd.close()


def test_changed_bound_workspace_blocks_remote_reconciliation(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _spec, spec_path, original_workspace = (
        _write_multi_user_unconfirmed_scheduler_spec(
            daemon,
            tmp_path,
            monkeypatch,
        )
    )
    changed_workspace = paths.user_jobs_dir("1000") / "changed"
    changed_workspace.mkdir()
    raced = JobSpec.read(spec_path)
    raced.cwd = str(changed_workspace)
    raced.write(spec_path)
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_state == "submit_reconciliation_quarantined"
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0
    assert not (changed_workspace / "_vq" / "events.jsonl").exists()
    assert not (original_workspace / "_vq" / "events.jsonl").exists()


@pytest.mark.parametrize("binding_damage", ["missing", "corrupt"])
def test_running_multi_user_scheduler_row_with_bad_binding_never_reattaches(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    binding_damage: str,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    binding_path = _scheduler_binding_path("1000", spec.id)
    if binding_damage == "missing":
        binding_path.unlink()
    else:
        binding_path.write_text("{not-json\n")
    spec.state = JobState.RUNNING
    spec.scheduler_state = None
    spec.scheduler_job_id = "forged.host_f"
    spec.write(spec_path)
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    mock.receipt = SchedulerSubmitReceipt("accepted", "forged.host_f", 0)
    mock.recorded = "forged.host_f"
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "scheduler_reconciliation_quarantined"
    assert spec.id not in daemon._scheduler_running
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0
    assert mock.polled == 0


def test_corrupt_binding_and_erased_target_still_reserve_scheduler_capacity(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    _scheduler_binding_path("1000", spec.id).write_text("{not-json\n")
    spec.state = JobState.SUBMIT_OUTCOME_UNKNOWN
    spec.scheduler_state = "submit_outcome_unknown"
    spec.scheduler_target = None
    spec.write(spec_path)
    next_workspace = paths.user_jobs_dir("1000") / "next"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="next",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter="1000",
    )
    next_path = paths.user_spec_path("1000", next_spec.id)
    next_spec.write(next_path)
    daemon.max_scheduler_jobs = 1
    _configure_multi_user_dispatch_test(daemon, monkeypatch)
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._dispatch_pending()

    parked = JobSpec.read(spec_path)
    assert parked.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert parked.scheduler_state == "submit_reconciliation_quarantined"
    assert JobSpec.read(next_path).state == JobState.PENDING
    assert mock.submitted == []
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0


def test_bound_scheduler_id_rejects_a_forged_mutable_id_on_restart(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
    spec.state = JobState.RUNNING
    spec.scheduler_state = None
    spec.scheduler_job_id = "99999.host_f"
    spec.write(spec_path)
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "scheduler_reconciliation_quarantined"
    assert spec.id not in daemon._scheduler_running
    assert mock.receipt_reads == 0
    assert mock.recorded_reads == 0
    assert mock.polled == 0


def test_multi_user_local_running_restart_does_not_require_scheduler_binding(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = "1000"
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    queue_dir = paths.user_queue_dir(owner)
    jobs_dir = paths.user_jobs_dir(owner)
    queue_dir.mkdir(parents=True)
    workspace = jobs_dir / "local"
    workspace.mkdir(parents=True)
    spec = JobSpec(
        id="local",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        submitter=owner,
        state=JobState.RUNNING,
        started_at=datetime.now(UTC).isoformat(),
    )
    spec_path = paths.user_spec_path(owner, spec.id)
    spec.write(spec_path)
    daemon._multi_user = True

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.ABORTED_BY_QUEUE
    assert on_disk.scheduler_state is None
    assert "scheduler" not in (on_disk.failure_reason or "").lower()


def test_kill_racing_ambiguous_submit_cancels_later_exact_acceptance(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown

    def accept_then_lose_observer(**_kwargs: object) -> SchedulerHandle:
        mock.submitted.append("j1")
        mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.finished_at = datetime.now(UTC).isoformat()
            killed.failure_reason = "operator killed during scheduler submit"
            killed.write(daemon._spec_path("j1"))
        raise unknown_type("local observer lost after qsub")

    mock.submit = accept_then_lose_observer  # type: ignore[method-assign]

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert mock.cancelled == ["18109.host_f"]
    assert "j1" not in daemon._scheduler_running


def test_kill_racing_ambiguous_submit_retries_delayed_acceptance_and_cancels(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown

    def lose_observer_before_receipt_visible(**_kwargs: object) -> SchedulerHandle:
        mock.submitted.append("j1")
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.finished_at = datetime.now(UTC).isoformat()
            killed.failure_reason = "operator killed during scheduler submit"
            killed.write(daemon._spec_path("j1"))
        raise unknown_type("receipt publication still in flight")

    mock.submit = lose_observer_before_receipt_visible  # type: ignore[method-assign]
    assert daemon._start_scheduler_job(spec) is False
    parked = JobSpec.read(daemon._spec_path("j1"))
    assert parked.state == JobState.KILLED
    assert parked.scheduler_state == "submit_outcome_unknown_after_terminal"
    assert mock.cancelled == []

    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    daemon._scheduler_reattach_retry_last.clear()
    daemon._retry_deferred_scheduler_reattach()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert on_disk.scheduler_state == "submit_cancelled_after_terminal"
    assert mock.cancelled == ["18109.host_f"]


def test_terminal_phase_one_submit_is_recovered_and_cancelled_after_restart(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _unconfirmed_scheduler_spec(daemon, "j1")
    spec.state = JobState.KILLED
    spec.finished_at = datetime.now(UTC).isoformat()
    spec.scheduler_state = "submitting"
    spec.write(daemon._spec_path(spec.id))

    daemon._reattach_or_interrupt_at_startup()
    assert JobSpec.read(daemon._spec_path(spec.id)).scheduler_state == (
        "submit_outcome_unknown_after_terminal"
    )

    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    daemon._scheduler_reattach_retry_last.clear()
    daemon._retry_deferred_scheduler_reattach()

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_state == "submit_cancelled_after_terminal"
    assert mock.cancelled == ["18109.host_f"]


def test_cancel_pending_after_terminal_is_retried_after_daemon_restart(
    daemon: Daemon,
) -> None:
    spec = _unconfirmed_scheduler_spec(daemon, "j1")
    spec.state = JobState.KILLED
    spec.finished_at = datetime.now(UTC).isoformat()
    spec.scheduler_state = "submit_cancel_pending_after_terminal"
    spec.scheduler_job_id = "18109.host_f"
    spec.write(daemon._spec_path(spec.id))

    first = MockDispatcher()

    def unavailable_cancel(_handle: SchedulerHandle) -> None:
        raise SchedulerError("qdel transport unavailable")

    first.cancel = unavailable_cancel  # type: ignore[method-assign]
    _inject(daemon, first)
    daemon._reattach_or_interrupt_at_startup()
    pending = JobSpec.read(daemon._spec_path(spec.id))
    assert pending.scheduler_state == "submit_cancel_pending_after_terminal"

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    mock = MockDispatcher()
    _inject(restarted, mock)

    restarted._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(restarted._spec_path(spec.id))
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_state == "submit_cancelled_after_terminal"
    assert mock.cancelled == ["18109.host_f"]
    restarted._queue_lock_fd.close()


def test_multi_user_cancel_pending_ignores_forged_mutable_scheduler_id(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
    spec.state = JobState.KILLED
    spec.finished_at = datetime.now(UTC).isoformat()
    spec.scheduler_state = "submit_cancel_pending_after_terminal"
    spec.scheduler_job_id = "99999.host_f"
    spec.write(spec_path)
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_state == "submit_reconciliation_quarantined_after_terminal"
    assert mock.cancelled == []
    assert "j1" not in daemon._scheduler_running


@pytest.mark.parametrize("terminal", [False, True])
def test_bound_direct_acceptance_recovers_without_remote_evidence(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: bool,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
    if terminal:
        spec.state = JobState.KILLED
        spec.finished_at = datetime.now(UTC).isoformat()
        spec.scheduler_state = "submitting"
    spec.write(spec_path)
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(spec_path)
    if terminal:
        assert on_disk.state == JobState.KILLED
        assert on_disk.scheduler_state == "submit_cancelled_after_terminal"
        assert mock.cancelled == ["18109.host_f"]
        assert spec.id not in daemon._scheduler_running
    else:
        assert on_disk.state == JobState.RUNNING
        assert on_disk.scheduler_job_id == "18109.host_f"
        assert daemon._scheduler_running[spec.id].handle.job_id == "18109.host_f"
        assert mock.cancelled == []
    assert mock.receipt_reads == 1
    assert mock.recorded_reads == 1


def test_closed_binding_cannot_resurrect_a_deleted_terminal_spec(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    mock = MockDispatcher(submit_id="18109.host_f")
    _inject(daemon, mock)
    list(daemon._iter_specs())
    assert daemon._start_scheduler_job(spec) is True

    monkeypatch.setattr(
        daemon_mod,
        "_remove_scheduler_submit_binding",
        lambda _owner, _jobid: (_ for _ in ()).throw(OSError("crash gap")),
    )
    daemon._record_finish(spec.id, 0)

    binding = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert binding is not None
    assert binding.transaction_state == "closed"
    assert binding.admitted_spec.state == JobState.COMPLETED
    spec_path.unlink()

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True

    assert list(restarted._iter_specs()) == []
    assert not spec_path.exists()
    restarted._queue_lock_fd.close()


@pytest.mark.parametrize("closure", ["rejected", "cancelled"])
def test_closed_submit_proof_cannot_resurrect_after_binding_unlink_failure(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    closure: str,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    mock = MockDispatcher()
    if closure == "cancelled":
        daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
        spec.state = JobState.KILLED
        spec.finished_at = datetime.now(UTC).isoformat()
        spec.scheduler_state = "submit_cancel_pending_after_terminal"
        spec.scheduler_job_id = "18109.host_f"
        spec.write(spec_path)
    _inject(daemon, mock)

    def fail_unlink(_owner: str, _jobid: str) -> None:
        raise OSError("simulated crash before binding unlink")

    monkeypatch.setattr(
        daemon_mod,
        "_remove_scheduler_submit_binding",
        fail_unlink,
    )
    if closure == "rejected":
        daemon._fail_scheduler_dispatch_transaction(
            spec,
            "scheduler command rejected",
            owner_uid="1000",
            spec_path=spec_path,
            expected_target="host_f",
            exit_code=2,
        )
    else:
        daemon._reattach_or_interrupt_at_startup()

    terminal = JobSpec.read(spec_path)
    assert terminal.is_terminal
    binding = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert binding is not None
    assert binding.transaction_state == "closed"
    assert binding.admitted_spec.is_terminal
    spec_path.unlink()

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    assert list(restarted._iter_specs()) == []
    assert not spec_path.exists()
    restarted._queue_lock_fd.close()


@pytest.mark.parametrize("closure", ["finish", "rejection", "cancellation"])
def test_private_close_precedes_mutable_terminal_write(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    closure: str,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=closure != "finish",
    )
    mock = MockDispatcher(
        submit_id="18109.host_f",
        phase=(
            SchedulerPhase.FINISHED
            if closure == "cancellation"
            else SchedulerPhase.RUNNING
        ),
    )
    _inject(daemon, mock)
    if closure == "finish":
        spec.state = JobState.PENDING
        spec.scheduler_state = None
        spec.started_at = None
        spec.write(spec_path)
        assert daemon._start_scheduler_job(spec) is True
    elif closure == "cancellation":
        daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
        spec.state = JobState.KILLED
        spec.scheduler_state = "submit_cancel_pending_after_terminal"
        spec.scheduler_job_id = "18109.host_f"
        spec.finished_at = datetime.now(UTC).isoformat()
        spec.write(spec_path)

    original_write = JobSpec.write

    def crash_before_mutable_terminal_write(candidate: JobSpec, path: Path) -> None:
        if path == spec_path and (
            (closure in {"finish", "rejection"} and candidate.is_terminal)
            or (
                closure == "cancellation"
                and candidate.scheduler_state == "submit_cancelled_after_terminal"
            )
        ):
            raise OSError("simulated daemon crash before mutable terminal write")
        original_write(candidate, path)

    with monkeypatch.context() as crash:
        crash.setattr(JobSpec, "write", crash_before_mutable_terminal_write)
        with pytest.raises(OSError, match="simulated daemon crash"):
            if closure == "finish":
                daemon._record_finish(spec.id, 0)
            elif closure == "rejection":
                daemon._fail_scheduler_dispatch_transaction(
                    spec,
                    "scheduler command rejected",
                    owner_uid="1000",
                    spec_path=spec_path,
                    expected_target="host_f",
                    exit_code=2,
                )
            else:
                daemon._reattach_or_interrupt_at_startup()

    binding = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert binding is not None
    assert binding.transaction_state == "closed"
    assert binding.admitted_spec.is_terminal
    spec_path.unlink()

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    restarted._multi_user = True
    assert list(restarted._iter_specs()) == []
    assert not spec_path.exists()
    restarted._queue_lock_fd.close()


def test_colliding_terminal_cancel_pending_keeps_owner_qualified_capacity(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    duplicate_id = "same"
    for owner in ("1000", "1001"):
        paths.user_queue_dir(owner).mkdir(parents=True)
        workspace = paths.user_jobs_dir(owner) / duplicate_id
        workspace.mkdir(parents=True)
        spec = JobSpec(
            id=duplicate_id,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            scheduler_target="host_f",
            submitter=owner,
            state=JobState.SUBMITTING,
            scheduler_state="submitting",
            started_at=datetime.now(UTC).isoformat(),
        )
        spec_path = paths.user_spec_path(owner, duplicate_id)
        spec.write(spec_path)
        if owner == "1000":
            daemon_mod._ensure_scheduler_submit_binding(owner, spec, "host_f")
            daemon_mod._bind_scheduler_job_id(owner, duplicate_id, "18109.host_f")
            spec.state = JobState.KILLED
            spec.finished_at = datetime.now(UTC).isoformat()
            spec.scheduler_state = "submit_cancel_pending_after_terminal"
            spec.scheduler_job_id = "18109.host_f"
            spec.write(spec_path)
    next_workspace = paths.user_jobs_dir("1001") / "next"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="next",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter="1001",
    )
    next_path = paths.user_spec_path("1001", next_spec.id)
    next_spec.write(next_path)
    daemon._multi_user = True
    daemon.max_scheduler_jobs = 1
    _configure_multi_user_dispatch_test(daemon, monkeypatch)
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._dispatch_pending()

    assert mock.submitted == []
    assert JobSpec.read(next_path).state == JobState.PENDING
    assert any(
        uid == "1000" and reserved.id == duplicate_id
        for uid, _path, reserved in daemon._colliding_scheduler_reservations
    )


@pytest.mark.parametrize("kill_after_persist", [False, True])
def test_recovered_acceptance_cancelled_when_kill_wins_a_persist_boundary(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    kill_after_persist: bool,
) -> None:
    spec = _unconfirmed_scheduler_spec(daemon, "j1")
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)
    original = daemon._persist_recovered_scheduler_job_id

    def persist_then_kill(*args: object, **kwargs: object) -> bool:
        persisted = original(*args, **kwargs) if kill_after_persist else False
        with paths.spec_lock(daemon._spec_path(spec.id), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path(spec.id))
            killed.state = JobState.KILLED
            killed.finished_at = datetime.now(UTC).isoformat()
            killed.failure_reason = "operator kill won reconciliation race"
            killed.write(daemon._spec_path(spec.id))
        return persisted

    monkeypatch.setattr(
        daemon,
        "_persist_recovered_scheduler_job_id",
        persist_then_kill,
    )

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.state == JobState.KILLED
    assert on_disk.scheduler_state == "submit_cancelled_after_terminal"
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert mock.cancelled == ["18109.host_f"]


def test_startup_recovers_scheduler_acceptance_from_submit_receipt(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert daemon._scheduler_running["j1"].handle.job_id == "18109.host_f"


def test_startup_recovers_scheduler_acceptance_with_conflicting_nonzero_status(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 2)
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "18109.host_f"


def test_startup_recovers_scheduler_rejection_from_submit_receipt(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("rejected", None, 2)
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.FAILED
    assert on_disk.scheduler_state == "submit_rejected"
    assert on_disk.finished_at is not None


def test_job_start_marker_outweighs_a_conflicting_rejection_receipt(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("rejected", None, 2)
    mock.recorded = "18109.host_f"
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert daemon._scheduler_running["j1"].handle.job_id == "18109.host_f"


def test_conflicting_accepted_receipt_and_job_start_marker_stay_fenced_unknown(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    mock.recorded = "18110.host_f"
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_job_id is None
    assert "j1" not in daemon._scheduler_running
    assert mock.receipt_reads == 1
    assert mock.recorded_reads == 1


def test_conflicting_scheduler_evidence_remains_durably_fenced_if_one_disappears(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    mock.recorded = "18110.host_f"
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()
    first = JobSpec.read(daemon._spec_path("j1"))
    assert first.scheduler_state == "submit_evidence_conflict"

    mock.recorded = None
    daemon._scheduler_reattach_retry_last.clear()
    daemon._retry_deferred_scheduler_reattach()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_state == "submit_evidence_conflict"
    assert on_disk.scheduler_job_id is None
    assert "j1" not in daemon._scheduler_running
    assert mock.receipt_reads == 1
    assert mock.recorded_reads == 1


def test_startup_keeps_missing_scheduler_submit_evidence_unknown(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1")

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert on_disk.scheduler_job_id is None
    assert "j1" not in daemon._scheduler_running


def test_aged_scheduler_reconciliation_quarantine_never_releases_capacity(
    daemon: Daemon,
) -> None:
    daemon.max_scheduler_jobs = 1
    quarantined = _submit_scheduler(daemon, "quarantined")
    quarantined.state = JobState.RUNNING
    quarantined.scheduler_state = "scheduler_reconciliation_quarantined"
    quarantined.started_at = (
        datetime.now(UTC)
        - timedelta(seconds=daemon_mod.SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS * 10)
    ).isoformat()
    quarantined.write(daemon._spec_path(quarantined.id))
    _submit_scheduler(daemon, "next")
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._dispatch_pending()

    assert mock.submitted == []
    assert JobSpec.read(daemon._spec_path("next")).state == JobState.PENDING


def test_startup_accepts_exact_job_start_marker_without_receipt(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.recorded = "18109.host_f"
    _inject(daemon, mock)
    _unconfirmed_scheduler_spec(daemon, "j1", state=JobState.SUBMIT_OUTCOME_UNKNOWN)

    daemon._reattach_or_interrupt_at_startup()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_job_id == "18109.host_f"
    assert mock.recorded_reads == 1


def test_scheduler_wall_time_defense_rejects_before_running_claim_or_submit(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(max_wall_time_seconds=28_800)
    _inject(daemon, mock, target="host_c")
    spec = _submit_scheduler(daemon, "too-long", target="host_c")
    spec.wall_time_seconds = 43_200
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.state == JobState.FAILED
    assert on_disk.started_at is None
    assert on_disk.failure_reason is not None
    assert "allows at most 28800 s" in on_disk.failure_reason
    assert mock.submitted == []
    transitions = [
        event
        for event in events.read_events(Path(on_disk.cwd))
        if event.get("kind") == "state_transition"
    ]
    assert transitions[-1]["from"] == JobState.PENDING.value
    assert transitions[-1]["to"] == JobState.FAILED.value
    assert "allows at most 28800 s" in transitions[-1]["reason"]


def test_scheduler_wall_time_defense_validates_latest_locked_spec(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(max_wall_time_seconds=28_800)
    _inject(daemon, mock, target="host_c")
    stale = _submit_scheduler(daemon, "raced-limit", target="host_c")
    latest = JobSpec.read(daemon._spec_path(stale.id))
    latest.wall_time_seconds = 43_200
    latest.write(daemon._spec_path(latest.id))

    assert daemon._start_scheduler_job(stale) is False

    on_disk = JobSpec.read(daemon._spec_path(stale.id))
    assert on_disk.state == JobState.FAILED
    assert on_disk.started_at is None
    assert on_disk.failure_reason is not None
    assert "allows at most 28800 s" in on_disk.failure_reason
    assert mock.submitted == []


def test_start_scheduler_job_exports_vq_metadata(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1", cpus=4)
    spec.scheduler_tasks = 2
    spec.mem_mb = 16_000
    spec.wall_time_seconds = 7200
    spec.program = "orca"
    spec.array_index = 2
    spec.array_total = 5
    spec.array_group_id = "arrgrp"
    spec.chain_index = 1
    spec.chain_total = 3
    spec.chain_group_id = "chaingrp"
    spec.rerun_until_file_exists = "$VQ_WORKDIR/DONE"
    spec.rerun_count = 3
    spec.rerun_max = 9
    spec.write(daemon._spec_path("j1"))

    assert daemon._start_scheduler_job(spec) is True

    env = mock.submit_kwargs[0]["env"]
    assert isinstance(env, dict)
    assert env["VQ_WORKDIR"] == "/remote/j1"
    assert env["VQ_JOB_ID"] == "j1"
    assert env["VQ_CPUS"] == "4"
    assert env["VQ_SCHEDULER_TASKS"] == "2"
    assert env["VQ_MEM_MB"] == "16000"
    assert env["VQ_WALL_TIME_SECONDS"] == "7200"
    assert env["VQ_PROGRAM"] == "orca"
    assert env["OMP_NUM_THREADS"] == "4"
    assert env["OPENBLAS_NUM_THREADS"] == "1"
    assert env["MKL_NUM_THREADS"] == "4"
    assert env["VQ_ARRAY_INDEX"] == "2"
    assert env["VQ_ARRAY_TOTAL"] == "5"
    assert env["VQ_ARRAY_GROUP_ID"] == "arrgrp"
    assert env["VQ_CHAIN_INDEX"] == "1"
    assert env["VQ_CHAIN_TOTAL"] == "3"
    assert env["VQ_CHAIN_GROUP_ID"] == "chaingrp"
    assert env["VQ_RERUN_COUNT"] == "3"
    assert env["VQ_RERUN_MAX"] == "9"
    # vq arrays are independent JobSpecs. Their array metadata is exported
    # to the payload, not translated into one native scheduler array submit.
    assert mock.submit_kwargs[0].get("array_size") is None
    assert mock.submit_kwargs[0]["program"] == "orca"


def test_start_scheduler_job_omits_driver_local_venv_program_paths(
    daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scheduler payloads never receive paths from the driver filesystem."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    git_dir = tmp_path / "repo"
    python = git_dir / ".venv-vibeview" / "bin" / "python"
    (cfg_dir / "config.toml").write_text(
        "[programs.vibeview-dev]\n"
        'kind = "venv"\n'
        f'python = "{python}"\n'
        f'git_dir = "{git_dir}"\n'
        'branch = "main"\n'
    )
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "vibeview")
    spec.program = "vibeview-dev"
    spec.write(daemon._spec_path("vibeview"))

    assert daemon._start_scheduler_job(spec) is True

    env = mock.submit_kwargs[0]["env"]
    assert isinstance(env, dict)
    assert env["VQ_PROGRAM"] == "vibeview-dev"
    assert env["VQ_PROGRAM_BRANCH"] == "main"
    assert "VQ_PROGRAM_BIN" not in env
    assert "VQ_PROGRAM_PYTHON" not in env
    assert "VQ_PROGRAM_GIT_DIR" not in env


def test_start_scheduler_job_omits_driver_local_binary_program_path(
    daemon: Daemon, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scheduler payloads never receive the driver's binary path either
    (GitLab #126): only portable program identity crosses the SSH hop."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    orca = tmp_path / "bin" / "orca"
    orca.parent.mkdir(parents=True, exist_ok=True)
    orca.write_text("#!/bin/sh\nexit 0\n")
    orca.chmod(0o755)
    (cfg_dir / "config.toml").write_text(
        "[programs.orca]\n"
        'kind = "binary"\n'
        f'binary = "{orca}"\n'
    )
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "orca-sched")
    spec.program = "orca"
    spec.write(daemon._spec_path("orca-sched"))

    assert daemon._start_scheduler_job(spec) is True

    env = mock.submit_kwargs[0]["env"]
    assert isinstance(env, dict)
    assert env["VQ_PROGRAM"] == "orca"
    assert "VQ_PROGRAM_EXE" not in env


def test_scheduler_job_runtime_pin_mismatch_fails_before_qsub(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    prog = config.VenvProgram(
        kind="venv",
        python="/bin/python",
        git_dir=str(tmp_path / "runtime"),
    )
    monkeypatch.setattr(
        config.VenvProgram,
        "runtime_pin_mismatches",
        lambda self, *, include_import=True: ["expected deadbeef, got cafebabe"],
    )
    monkeypatch.setattr(
        daemon_mod,
        "load_config",
        lambda: config.Config(programs={"vibeqc-dev": prog}),
    )
    spec = _submit_scheduler(daemon, "pinjob")
    spec.program = "vibeqc-dev"
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path("pinjob"))
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == -1
    assert on_disk.failure_reason is not None
    assert "runtime pin mismatch before dispatch" in on_disk.failure_reason
    assert "expected deadbeef" in on_disk.failure_reason
    assert mock.submitted == []
    assert "pinjob" not in daemon._scheduler_running


def test_scheduler_job_runtime_pin_snapshot_fails_before_qsub(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    prog = config.VenvProgram(
        kind="venv",
        python="/bin/python",
        git_dir=str(tmp_path / "runtime"),
        expected_git_sha="cafebabe0000",
    )
    monkeypatch.setattr(
        config.VenvProgram,
        "current_git_sha",
        lambda self, *, full=False: "cafebabe0000",
    )
    monkeypatch.setattr(
        daemon_mod,
        "load_config",
        lambda: config.Config(programs={"vibeqc-dev": prog}),
    )
    spec = _submit_scheduler(daemon, "pinsnap")
    spec.program = "vibeqc-dev"
    spec.program_runtime_pin = ProgramRuntimePin(expected_git_sha="deadbeef0000")
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path("pinsnap"))
    assert on_disk.state == JobState.FAILED
    assert on_disk.failure_reason is not None
    assert "driver-local runtime observation" in on_disk.failure_reason
    assert "scheduler target 'host_f'" in on_disk.failure_reason
    assert mock.submitted == []
    assert "pinsnap" not in daemon._scheduler_running


def test_scheduler_observational_pin_never_becomes_target_provenance(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    prog = config.VenvProgram(
        kind="venv",
        python="/bin/python",
        git_dir=str(tmp_path / "driver-runtime"),
    )
    monkeypatch.setattr(
        config.VenvProgram,
        "current_git_sha",
        lambda self, *, full=False: "a" * 40,
    )
    monkeypatch.setattr(
        daemon_mod,
        "load_config",
        lambda: config.Config(programs={"vibeqc-release": prog}),
    )
    spec = _submit_scheduler(daemon, "observational-pin")
    spec.program = "vibeqc-release"
    spec.program_runtime_pin = ProgramRuntimePin(
        expected_git_sha="a" * 40,
        enforce_git_sha=False,
    )
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.state == JobState.FAILED
    assert on_disk.failure_reason is not None
    assert "unenforceable provenance" in on_disk.failure_reason
    assert mock.submitted == []


def test_scheduler_target_runtime_pin_does_not_validate_driver_checkout(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    driver_prog = config.VenvProgram(
        kind="venv",
        python="/bin/python",
        git_dir=str(tmp_path / "driver-runtime"),
        expected_git_sha="cafebabe0000",
    )
    monkeypatch.setattr(
        config.VenvProgram,
        "current_git_sha",
        lambda self: "cafebabe0000",
    )
    monkeypatch.setattr(
        daemon_mod,
        "load_config",
        lambda: config.Config(programs={"vibeqc-release": driver_prog}),
    )
    spec = _submit_scheduler(daemon, "target-pin")
    spec.program = "vibeqc-release"
    spec.program_runtime_pin = ProgramRuntimePin(
        expected_git_sha="deadbeef0000",
        scheduler_host="host_f",
        resolved_executable=(
            "/home/USER/vibeqc-runtimes/vibeqc-release-deadbeef0000"
        ),
        program_kind="scheduler-runtime",
        artifact_identity=(
            "/home/USER/vibeqc-runtimes/vibeqc-release-deadbeef0000"
        ),
    )
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_job(spec) is True

    on_disk = JobSpec.read(daemon._spec_path("target-pin"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.failure_reason is None
    assert on_disk.program_runtime_pin is not None
    assert on_disk.program_runtime_pin.resolved_git_sha == "deadbeef0000"
    assert mock.submitted == ["target-pin"]
    assert "target-pin" in daemon._scheduler_running
    env = mock.submit_kwargs[0]["env"]
    assert isinstance(env, dict)
    assert env["VQ_PROGRAM_GIT_SHA"] == "deadbeef0000"


def test_scheduler_target_runtime_pin_rejects_wrong_target(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "wrong-target")
    spec.program = "vibeqc-release"
    spec.program_runtime_pin = ProgramRuntimePin(
        expected_git_sha="deadbeef0000",
        scheduler_host="host_c",
        resolved_executable=(
            "/home/USER/vibeqc-runtimes/vibeqc-release-deadbeef0000"
        ),
        program_kind="scheduler-runtime",
        artifact_identity=(
            "/home/USER/vibeqc-runtimes/vibeqc-release-deadbeef0000"
        ),
    )
    spec.write(daemon._spec_path(spec.id))

    assert daemon._start_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path("wrong-target"))
    assert on_disk.state == JobState.FAILED
    assert on_disk.failure_reason is not None
    assert "belongs to scheduler host 'host_c'" in on_disk.failure_reason
    assert "job targets 'host_f'" in on_disk.failure_reason
    assert mock.submitted == []


def test_scheduler_dispatch_reread_preserves_program_metadata(daemon: Daemon) -> None:
    """Scheduler qsub/env generation must use the locked fresh spec."""
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "program-reread")
    spec.program = "vibeqc-dev"
    spec.write(daemon._spec_path(spec.id))
    stale = spec.model_copy(deep=True)
    stale.program = None

    assert daemon._start_job(stale) is True

    on_disk = JobSpec.read(daemon._spec_path(spec.id))
    assert on_disk.program == "vibeqc-dev"
    assert mock.submit_kwargs
    assert mock.submit_kwargs[0]["program"] == "vibeqc-dev"
    env = mock.submit_kwargs[0]["env"]
    assert isinstance(env, dict)
    assert env["VQ_PROGRAM"] == "vibeqc-dev"


def test_start_scheduler_job_kill_during_qsub_qdels(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    # Simulate `vq kill` landing right after the RUNNING claim: the phase-2
    # re-read sees a terminal label, so the just-submitted job is qdel'd.
    orig_submit = mock.submit
    orig_cancel = mock.cancel
    cancel_observations: list[JobSpec] = []
    finished_at = "2026-08-02T12:01:00+00:00"

    def killing_submit(**kw: object) -> SchedulerHandle:
        h = orig_submit(**kw)  # type: ignore[arg-type]
        # qsub must not hold the spec lock: an external writer can commit the
        # terminal label before phase two records the returned scheduler id.
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.finished_at = finished_at
            killed.failure_reason = "operator killed during qsub"
            killed.write(daemon._spec_path("j1"))
        return h

    def observing_cancel(handle: SchedulerHandle) -> None:
        # qdel crosses SSH and must not run while other job writers are blocked.
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            cancel_observations.append(JobSpec.read(daemon._spec_path("j1")))
            orig_cancel(handle)

    mock.submit = killing_submit  # type: ignore[assignment]
    mock.cancel = observing_cancel  # type: ignore[assignment]
    assert daemon._start_scheduler_job(spec) is False
    assert mock.cancelled == ["555.cluster"]
    assert len(cancel_observations) == 1
    assert cancel_observations[0].state == JobState.KILLED
    assert cancel_observations[0].finished_at == finished_at
    assert cancel_observations[0].failure_reason == "operator killed during qsub"
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.finished_at == finished_at
    assert on_disk.failure_reason == "operator killed during qsub"
    assert on_disk.scheduler_job_id == "555.cluster"
    assert "j1" not in daemon._scheduler_running


def test_scheduler_cancel_failure_after_qsub_keeps_retryable_ownership(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    orig_submit = mock.submit
    orig_cancel = mock.cancel
    attempts: list[str] = []

    def killing_submit(**kw: object) -> SchedulerHandle:
        handle = orig_submit(**kw)  # type: ignore[arg-type]
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.failure_reason = "operator killed during qsub"
            killed.write(daemon._spec_path("j1"))
        return handle

    def fail_once(handle: SchedulerHandle) -> None:
        attempts.append(handle.job_id)
        if len(attempts) == 1:
            raise SchedulerError("simulated qdel transport failure")
        orig_cancel(handle)

    mock.submit = killing_submit  # type: ignore[assignment]
    mock.cancel = fail_once  # type: ignore[assignment]

    assert daemon._start_scheduler_job(spec) is False

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.failure_reason == "operator killed during qsub"
    assert on_disk.scheduler_job_id == "555.cluster"
    tracked = daemon._scheduler_running["j1"]
    assert tracked.handle.job_id == "555.cluster"
    assert tracked.term_qdeled is False

    daemon._reconcile_scheduler()

    assert attempts == ["555.cluster", "555.cluster"]
    assert mock.cancelled == ["555.cluster"]
    assert tracked.term_qdeled is True
    assert "j1" in daemon._scheduler_running


def test_single_user_external_kill_cancel_failure_survives_restart(
    daemon: Daemon,
) -> None:
    first = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, first)
    spec = _submit_scheduler(daemon, "j1")
    assert daemon._start_scheduler_job(spec) is True

    killed = JobSpec.read(daemon._spec_path(spec.id))
    killed.state = JobState.KILLED
    killed.finished_at = datetime.now(UTC).isoformat()
    killed.write(daemon._spec_path(spec.id))

    def unavailable_cancel(_handle: SchedulerHandle) -> None:
        raise SchedulerError("qdel transport unavailable")

    first.cancel = unavailable_cancel  # type: ignore[method-assign]
    daemon._reconcile_scheduler()

    pending = JobSpec.read(daemon._spec_path(spec.id))
    assert pending.state == JobState.KILLED
    assert pending.scheduler_job_id == "555.cluster"
    assert pending.scheduler_state == "submit_cancel_pending_after_terminal"
    assert spec.id in daemon._scheduler_running

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    second = MockDispatcher(phase=SchedulerPhase.FINISHED)
    _inject(restarted, second)

    restarted._reattach_or_interrupt_at_startup()

    final = JobSpec.read(restarted._spec_path(spec.id))
    assert final.state == JobState.KILLED
    assert final.scheduler_job_id == "555.cluster"
    assert final.scheduler_state == "submit_cancelled_after_terminal"
    assert second.cancelled == ["555.cluster"]
    assert spec.id not in restarted._scheduler_running
    restarted._queue_lock_fd.close()


# --------------------------------------------------------------------------- #
# reconcile / terminal
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("rc", "expected"),
    [(0, JobState.COMPLETED), (1, JobState.FAILED), (3, JobState.FAILED)],
)
def test_reconcile_terminal_by_rc(daemon: Daemon, rc: int, expected: JobState) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=rc)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    assert mock.fetched == ["555.cluster"]  # whole workspace staged home
    assert JobSpec.read(daemon._spec_path("j1")).state == expected
    assert "j1" not in daemon._scheduler_running  # reaped


def test_reconcile_vq_array_element_reads_single_job_marker(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=0)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    _tag_vq_array_element(daemon, spec)
    daemon._start_scheduler_job(spec)

    assert daemon._scheduler_running["j1"].handle.array_size is None

    daemon._reconcile_scheduler()

    assert mock.marker_queries == [("555.cluster", None)]
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert "j1" not in daemon._scheduler_running


def test_reconcile_fetch_failure_stays_live_and_recovers(daemon: Daemon) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0,
        fetch_error=SchedulerError("rsync failed"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster"]
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "fetch_failed"
    assert on_disk.last_heartbeat_at is not None
    assert sj.fetch_failure_misses == 1
    assert "j1" in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("reason") == "scheduler workspace fetch failed; daemon will retry"
    ][-1]
    assert transition["from"] == JobState.RUNNING.value
    assert transition["to"] == JobState.RUNNING.value
    assert transition["evidence"]["scheduler_job_id"] == "555.cluster"
    assert transition["evidence"]["fetch_error"] == "rsync failed"

    mock.fetch_error = None
    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster", "555.cluster"]
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert on_disk.scheduler_state == "finishing"
    assert sj.fetch_failure_misses == 0
    assert "j1" not in daemon._scheduler_running


def test_terminal_scheduler_accounting_never_delays_completion(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0,
        telemetry_results=[False, False, True],
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.COMPLETED
    assert mock.fetched == ["555.cluster"]
    assert mock.telemetry_calls == 1
    assert "j1" not in daemon._scheduler_running


@pytest.mark.parametrize("array_index", [None, 2], ids=["single", "vq-array"])
def test_reconcile_marker_probe_failure_stays_live_and_recovers(
    daemon: Daemon, array_index: int | None
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0,
        marker_error=SchedulerError("ssh cat failed"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    if array_index is not None:
        _tag_vq_array_element(daemon, spec, index=array_index)
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == []
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "marker_probe_failed"
    assert on_disk.last_heartbeat_at is not None
    assert "j1" in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("reason")
        == "scheduler exit-marker probe failed; daemon will retry"
    ][-1]
    assert transition["from"] == JobState.RUNNING.value
    assert transition["to"] == JobState.RUNNING.value
    evidence = transition["evidence"]
    assert evidence["scheduler_job_id"] == "555.cluster"
    assert evidence["remote_exit_marker"] == "/remote/j1/_vq/exit-code"
    assert evidence["marker_probe_error"] == "ssh cat failed"

    mock.marker_error = None
    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster"]
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert on_disk.scheduler_state == "finishing"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_still_running_keeps_tracking(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    assert mock.fetched == []  # not finished -> no fetch
    assert "j1" in daemon._scheduler_running
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.RUNNING


def test_reconcile_finished_waits_for_delayed_exit_marker(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, marker_rcs=[None, 0])
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "finishing"
    assert mock.fetched == []
    assert "j1" in daemon._scheduler_running

    daemon._reconcile_scheduler()
    assert mock.fetched == ["555.cluster"]
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.COMPLETED
    assert "j1" not in daemon._scheduler_running


@pytest.mark.parametrize("array_index", [None, 2], ids=["single", "vq-array"])
def test_reconcile_finished_without_marker_aborts_after_grace(
    daemon: Daemon, array_index: int | None
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, None],
        rc=None,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    if array_index is not None:
        _tag_vq_array_element(daemon, spec, index=array_index)
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.RUNNING
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()
    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == [("555.cluster", None)]
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.ABORTED_BY_QUEUE
    assert on_disk.failure_reason is not None
    assert "finished without an exit-marker" in on_disk.failure_reason
    assert "j1" not in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("to") == JobState.ABORTED_BY_QUEUE.value
    ][-1]
    evidence = transition["evidence"]
    assert evidence["scheduler_job_id"] == "555.cluster"
    assert evidence["remote_exit_marker"] == "/remote/j1/_vq/exit-code"
    assert evidence["remote_stdout_tail"] == "remote stdout"
    assert evidence["local_exit_marker"].endswith("_vq/exit-code")
    assert "events.jsonl" in evidence["local_vq_listing"]


def test_reconcile_missing_marker_walltime_becomes_time_exceeded(
    daemon: Daemon,
) -> None:
    detail = QstatDetail(
        raw_state="C",
        walltime_used="08:00:40",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, None],
        rc=None,
        detail=detail,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.TIME_EXCEEDED
    assert on_disk.exit_code is None
    assert on_disk.scheduler_walltime_used == "08:00:40"
    assert on_disk.scheduler_walltime_limit == "08:00:00"
    assert on_disk.failure_reason is not None
    assert "scheduler walltime limit reached" in on_disk.failure_reason
    assert "exit-marker missing" in on_disk.failure_reason
    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == [("555.cluster", None)]
    assert "j1" not in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("to") == JobState.TIME_EXCEEDED.value
    ][-1]
    evidence = transition["evidence"]
    assert evidence["scheduler_walltime_exceeded"] is True
    assert evidence["scheduler_walltime_used"] == "08:00:40"
    assert evidence["scheduler_walltime_limit"] == "08:00:00"
    assert evidence["remote_exit_marker"] == "/remote/j1/_vq/exit-code"
    assert evidence["local_exit_marker"].endswith("_vq/exit-code")


def test_reconcile_missing_remote_marker_recovers_after_final_fetch(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, None],
        rc=0,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()

    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == []
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert "j1" not in daemon._scheduler_running


def test_reconcile_visible_marker_rc_wins_over_fetched_local_marker(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=0)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    def fetch_conflicting_marker(handle: SchedulerHandle, local_dir: Path) -> None:
        mock.fetched.append(handle.job_id)
        marker = local_dir / "_vq" / "exit-code"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("17")

    mock.fetch_results = fetch_conflicting_marker  # type: ignore[assignment]

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster"]
    assert (Path(spec.cwd) / "_vq" / "exit-code").read_text() == "17"
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert "j1" not in daemon._scheduler_running


def test_reconcile_missing_remote_marker_uses_fetched_local_marker_before_walltime(
    daemon: Daemon,
) -> None:
    detail = QstatDetail(
        raw_state="C",
        walltime_used="08:00:40",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None],
        rc=17,
        detail=detail,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == []
    assert sj.fetch_failure_misses == 0
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == 17
    assert on_disk.scheduler_walltime_used is None
    assert on_disk.scheduler_walltime_limit is None
    assert "j1" not in daemon._scheduler_running


def test_reconcile_partial_fetch_marker_survives_failure_at_limit(
    daemon: Daemon,
) -> None:
    detail = QstatDetail(
        raw_state="C",
        walltime_used="08:00:40",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None],
        rc=None,
        detail=detail,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    def fetch_marker_then_fail(handle: SchedulerHandle, local_dir: Path) -> None:
        mock.fetched.append(handle.job_id)
        marker = local_dir / "_vq" / "exit-code"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("23")
        raise SchedulerError("later workspace merge conflict")

    mock.fetch_results = fetch_marker_then_fail  # type: ignore[assignment]

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == []
    assert sj.fetch_failure_misses == daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == 23
    assert on_disk.scheduler_state == "artifacts_unavailable"
    assert on_disk.scheduler_walltime_used is None
    assert on_disk.scheduler_walltime_limit is None
    assert "j1" not in daemon._scheduler_running


def test_reconcile_missing_marker_fetch_failure_state_trace(daemon: Daemon) -> None:
    assert daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT == 3
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, None, None],
        rc=None,
        fetch_error=SchedulerError("remote workspace vanished"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    states: list[str | None] = []
    for _ in range(daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT):
        daemon._reconcile_scheduler()
        states.append(JobSpec.read(daemon._spec_path("j1")).scheduler_state)

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert states == ["fetch_failed", "finishing", "artifacts_unavailable"]
    assert sj.fetch_failure_misses == daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
    assert mock.fetched == ["555.cluster"] * daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
    assert mock.diagnostics == [("555.cluster", None)]
    assert on_disk.state == JobState.ABORTED_BY_QUEUE
    assert on_disk.exit_code is None
    assert "j1" not in daemon._scheduler_running
    fetch_failed_events = [
        event
        for event in events.read_events(Path(spec.cwd))
        if event.get("kind") == "state_transition"
        and event.get("reason")
        == "scheduler workspace fetch failed; daemon will retry"
    ]
    assert len(fetch_failed_events) == 1
    assert fetch_failed_events[0]["evidence"]["fetch_error"] == (
        "remote workspace vanished"
    )


def test_reconcile_visible_marker_fetch_failure_at_limit_reaps_on_remote_rc(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=17,
        fetch_error=SchedulerError("remote workspace vanished"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert sj.fetch_failure_misses == daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == 17
    assert on_disk.scheduler_state == "artifacts_unavailable"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_marker_appears_after_missing_marker_fetch_failure(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, 7],
        rc=7,
        fetch_error=SchedulerError("temporary fetch failure"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()
    parked = JobSpec.read(daemon._spec_path("j1"))
    assert parked.state == JobState.RUNNING
    assert parked.scheduler_state == "fetch_failed"
    assert sj.fetch_failure_misses == 1
    assert "j1" in daemon._scheduler_running

    mock.fetch_error = None
    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert mock.marker_queries == [("555.cluster", None), ("555.cluster", None)]
    assert mock.fetched == ["555.cluster", "555.cluster"]
    assert mock.diagnostics == []
    assert sj.fetch_failure_misses == 0
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == 7
    assert on_disk.scheduler_state == "finishing"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_restart_resets_in_memory_fetch_failure_count(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0,
        fetch_error=SchedulerError("temporary fetch failure"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    assert daemon._scheduler_running["j1"].fetch_failure_misses == 1
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == "fetch_failed"

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    try:
        _inject(restarted, mock)
        restarted._reattach_or_interrupt_at_startup()
        sj = restarted._scheduler_running["j1"]
        assert sj.fetch_failure_misses == 0
        assert sj.finished_without_marker_since is None
        assert sj.finished_without_marker_misses == 0

        restarted._reconcile_scheduler()
        assert sj.fetch_failure_misses == 1
        assert JobSpec.read(restarted._spec_path("j1")).state == JobState.RUNNING

        mock.fetch_error = None
        restarted._reconcile_scheduler()
        assert sj.fetch_failure_misses == 0
        assert JobSpec.read(restarted._spec_path("j1")).state == JobState.COMPLETED
        assert "j1" not in restarted._scheduler_running
    finally:
        restarted._queue_lock_fd.close()


@pytest.mark.parametrize(
    ("existing_exit_code", "expected_exit_code"),
    [(None, 9), (41, 41)],
    ids=["fills-missing", "preserves-existing"],
)
def test_reconcile_visible_marker_fetch_race_preserves_terminal_spec(
    daemon: Daemon,
    existing_exit_code: int | None,
    expected_exit_code: int,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=9)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    def kill_during_fetch(handle: SchedulerHandle, local_dir: Path) -> None:
        del local_dir
        mock.fetched.append(handle.job_id)
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.exit_code = existing_exit_code
            killed.failure_reason = "operator killed during workspace fetch"
            killed.write(daemon._spec_path("j1"))
        raise SchedulerError("fetch lost after concurrent kill")

    mock.fetch_results = kill_during_fetch  # type: ignore[assignment]

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.exit_code == expected_exit_code
    assert on_disk.failure_reason == "operator killed during workspace fetch"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_missing_marker_fetch_race_preserves_terminal_spec(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None],
        rc=None,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    def kill_during_fetch(handle: SchedulerHandle, local_dir: Path) -> None:
        del local_dir
        mock.fetched.append(handle.job_id)
        with paths.spec_lock(daemon._spec_path("j1"), timeout=0.2):
            killed = JobSpec.read(daemon._spec_path("j1"))
            killed.state = JobState.KILLED
            killed.failure_reason = "operator killed during final workspace fetch"
            killed.write(daemon._spec_path("j1"))
        raise SchedulerError("fetch lost after concurrent kill")

    mock.fetch_results = kill_during_fetch  # type: ignore[assignment]

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.KILLED
    assert on_disk.exit_code is None
    assert on_disk.failure_reason == "operator killed during final workspace fetch"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_fetch_limit_keeps_command_retry_semantics(daemon: Daemon) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=17,
        fetch_error=SchedulerError("remote workspace vanished"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    spec.retry_max = 1
    spec.write(daemon._spec_path("j1"))
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    sj.fetch_failure_misses = daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT - 1

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.PENDING
    assert on_disk.exit_code is None
    assert on_disk.retry_count == 1
    assert on_disk.retry_max == 1
    assert on_disk.not_before is not None
    assert on_disk.scheduler_state == "artifacts_unavailable"
    assert mock.submitted == ["j1"]
    assert "j1" not in daemon._scheduler_running


def test_scheduler_command_retry_marks_second_submit_as_new_attempt(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=17)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    spec.retry_max = 1
    spec.write(daemon._spec_path("j1"))
    assert daemon._start_scheduler_job(spec) is True

    daemon._record_finish("j1", 17)
    retried = JobSpec.read(daemon._spec_path("j1"))
    retried.not_before = None
    retried.write(daemon._spec_path("j1"))
    daemon._scheduler_running.pop("j1", None)
    daemon._dispatch_pending()

    assert mock.submitted == ["j1", "j1"]
    assert mock.submit_kwargs[-1]["retry_attempt"] is True


def test_scheduler_retry_rotates_old_evidence_before_submitting_claim(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=17)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    spec.retry_max = 1
    spec.retry_count = 1
    spec.write(daemon._spec_path(spec.id))
    mock.recorded = "18109.host_f"
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)

    def crash_after_phase_one(**_kwargs: object) -> SchedulerHandle:
        assert mock.retry_preparations == [spec.id]
        assert mock.recorded is None
        assert mock.receipt is None
        raise KeyboardInterrupt("daemon died before retry scheduler mutation")

    mock.submit = crash_after_phase_one  # type: ignore[method-assign]
    with pytest.raises(KeyboardInterrupt, match="before retry scheduler mutation"):
        daemon._start_scheduler_job(spec)

    phase_one = JobSpec.read(daemon._spec_path(spec.id))
    assert phase_one.state == JobState.SUBMITTING
    assert phase_one.scheduler_job_id is None
    assert phase_one.scheduler_state == "submitting"

    daemon._queue_lock_fd.close()
    restarted = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=daemon.queue_dir,
        jobs_dir=daemon.jobs_dir,
    )
    _inject(restarted, mock)

    restarted._reattach_or_interrupt_at_startup()

    parked = JobSpec.read(restarted._spec_path(spec.id))
    assert parked.state == JobState.SUBMIT_OUTCOME_UNKNOWN
    assert parked.scheduler_job_id is None
    assert parked.scheduler_state == "submit_outcome_unknown"
    assert spec.id not in restarted._scheduler_running
    restarted._queue_lock_fd.close()


def test_multi_user_scheduler_retry_rotates_closed_attempt_authority(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.retry_max = 1
    spec.write(spec_path)
    mock = MockDispatcher(submit_id="18109.host_f", phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)

    assert daemon._start_scheduler_job(spec) is True
    daemon._record_finish(spec.id, 17)
    daemon._scheduler_running.pop(spec.id, None)

    retried = JobSpec.read(spec_path)
    assert retried.state == JobState.PENDING
    assert retried.retry_count == 1
    first_attempt = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert first_attempt is not None
    assert first_attempt.transaction_state == "closed"
    assert first_attempt.scheduler_job_id is None

    mock.submit_id = "18110.host_f"
    assert daemon._start_scheduler_job(retried) is True

    second_attempt = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert second_attempt is not None
    assert second_attempt.transaction_state == "open"
    assert second_attempt.scheduler_job_id == "18110.host_f"
    assert second_attempt.admitted_spec.retry_count == 1
    assert mock.submitted == [spec.id, spec.id]
    assert mock.submit_kwargs[-1]["retry_attempt"] is True


def test_closed_binding_rejects_ordinary_same_id_reuse(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
    )
    spec.state = JobState.COMPLETED
    spec.scheduler_state = None
    spec.scheduler_job_id = "18109.host_f"
    spec.finished_at = datetime.now(UTC).isoformat()
    spec.write(spec_path)
    daemon_mod._bind_scheduler_job_id("1000", spec.id, "18109.host_f")
    daemon_mod._close_scheduler_submit_binding("1000", spec)

    replacement = JobSpec(
        id=spec.id,
        command=["true"],
        cwd=spec.cwd,
        cpus=spec.cpus,
        scheduler_target=spec.scheduler_target,
        submitter=spec.submitter,
    )
    replacement.write(spec_path)
    mock = MockDispatcher(submit_id="18110.host_f", phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    list(daemon._iter_specs())

    assert daemon._start_scheduler_job(replacement) is False
    assert mock.submitted == []
    assert JobSpec.read(spec_path).state == JobState.COMPLETED
    tombstone = daemon_mod._read_scheduler_submit_binding("1000", spec.id)
    assert tombstone is not None
    assert tombstone.transaction_state == "closed"
    assert tombstone.scheduler_job_id == "18109.host_f"


@pytest.mark.parametrize("failure_point", ["spec", "binding"])
def test_direct_acceptance_is_tracked_before_phase_two_persistence(
    daemon: Daemon,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    spec, spec_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        create_binding=False,
    )
    spec.state = JobState.PENDING
    spec.scheduler_state = None
    spec.started_at = None
    spec.write(spec_path)
    mock = MockDispatcher(submit_id="18109.host_f", phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)

    if failure_point == "spec":
        original_write = JobSpec.write

        def fail_running_write(candidate: JobSpec, path: Path) -> None:
            if candidate.id == spec.id and candidate.state == JobState.RUNNING:
                raise OSError("simulated phase-two spec write failure")
            original_write(candidate, path)

        monkeypatch.setattr(JobSpec, "write", fail_running_write)
    else:
        original_replace = daemon_mod._replace_scheduler_submit_binding_spec

        def fail_running_binding(
            owner_uid: str,
            candidate: JobSpec,
            **kwargs: object,
        ) -> daemon_mod._SchedulerSubmitBinding:
            if candidate.id == spec.id and candidate.state == JobState.RUNNING:
                raise OSError("simulated phase-two binding write failure")
            return original_replace(owner_uid, candidate, **kwargs)

        monkeypatch.setattr(
            daemon_mod,
            "_replace_scheduler_submit_binding_spec",
            fail_running_binding,
        )

    assert daemon._start_scheduler_job(spec) is True

    assert mock.submitted == [spec.id]
    runtime = daemon._scheduler_running[spec.id]
    assert runtime.handle.job_id == "18109.host_f"
    assert runtime.owner_uid == "1000"
    assert runtime.spec_path == spec_path


@pytest.mark.parametrize("marker_visible", [True, False], ids=["visible", "missing"])
def test_reconcile_fetch_unexpected_exception_escapes(
    daemon: Daemon,
    marker_visible: bool,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0 if marker_visible else None,
        marker_rcs=None if marker_visible else [None],
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    sj = daemon._scheduler_running["j1"]
    if not marker_visible:
        sj.finished_without_marker_since = (
            time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
        )

    def crash_during_fetch(handle: SchedulerHandle, local_dir: Path) -> None:
        del local_dir
        mock.fetched.append(handle.job_id)
        raise RuntimeError("unexpected dispatcher bug")

    mock.fetch_results = crash_during_fetch  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="unexpected dispatcher bug"):
        daemon._reconcile_scheduler()

    assert mock.fetched == ["555.cluster"]
    assert sj.fetch_failure_misses == 0
    assert "j1" in daemon._scheduler_running


def test_reconcile_vq_array_recovers_bare_marker_after_final_fetch(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        marker_rcs=[None, None],
        rc=0,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    _tag_vq_array_element(daemon, spec)
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()

    assert mock.marker_queries == [
        ("555.cluster", None),
        ("555.cluster", None),
    ]
    assert mock.fetched == ["555.cluster"]
    assert mock.diagnostics == []
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert "j1" not in daemon._scheduler_running


def test_reconcile_marker_with_walltime_preserves_time_exceeded(
    daemon: Daemon,
) -> None:
    detail = QstatDetail(
        raw_state="C",
        walltime_used="08:00:40",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=143, detail=detail)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.TIME_EXCEEDED
    assert on_disk.exit_code == 143
    assert on_disk.scheduler_walltime_used == "08:00:40"
    assert on_disk.scheduler_walltime_limit == "08:00:00"
    transitions = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
    ]
    assert any(e.get("to") == JobState.TIME_EXCEEDED.value for e in transitions)
    assert transitions[-1]["exit_code"] == 143
    assert transitions[-1]["reason"] == "reaped (terminal state already set)"
    assert "j1" not in daemon._scheduler_running


def test_reconcile_external_kill_qdels(daemon: Daemon) -> None:
    # Job still listed (RUNNING) but the spec was set terminal by `vq kill`.
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    killed = JobSpec.read(daemon._spec_path("j1"))
    killed.state = JobState.KILLED
    killed.write(daemon._spec_path("j1"))
    daemon._reconcile_scheduler()
    assert mock.cancelled == ["555.cluster"]  # qdel'd
    # Still listed, so still tracked until the next poll shows it gone.
    assert "j1" in daemon._scheduler_running


# --------------------------------------------------------------------------- #
# dispatch gate (scheduler jobs skip the LOCAL cpu budget)
# --------------------------------------------------------------------------- #


def test_gate_scheduler_job_skips_local_cpu_budget(daemon: Daemon) -> None:
    # max_cpus=4, but a 20-core cluster job must still dispatch -- it runs on the
    # cluster's cores, not the driver's. The full _dispatch_pending runs.
    mock = MockDispatcher()
    _inject(daemon, mock)
    _submit_scheduler(daemon, "big", cpus=20)
    daemon._dispatch_pending()
    assert JobSpec.read(daemon._spec_path("big")).state == JobState.RUNNING
    assert mock.submitted == ["big"]


def test_gate_scheduler_job_skips_local_max_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=4,
        max_jobs=1,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    mock = MockDispatcher()
    _inject(d, mock)
    d._orphans["local-orphan"] = _OrphanJob(pgid=999999, cpus=1, mem_mb=None)
    _submit_scheduler(d, "pbsjob", cpus=20)

    d._dispatch_pending()

    assert JobSpec.read(d._spec_path("pbsjob")).state == JobState.RUNNING
    assert mock.submitted == ["pbsjob"]


def test_gate_max_scheduler_jobs_limits_scheduler_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    mock = MockDispatcher()
    _inject(d, mock)
    _submit_scheduler(d, "j1")
    _submit_scheduler(d, "j2")

    d._dispatch_pending()

    assert mock.submitted == ["j1"]
    assert JobSpec.read(d._spec_path("j1")).state == JobState.RUNNING
    assert JobSpec.read(d._spec_path("j2")).state == JobState.PENDING


def test_scheduler_submission_does_not_starve_local_child_reaping(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch
) -> None:
    action_order: list[str] = []

    class LocalHandle:
        def __init__(self) -> None:
            self.exited = False
            self.poll_count = 0

        def poll(self) -> int | None:
            self.poll_count += 1
            action_order.append("local-poll")
            return 0 if self.exited else None

    workspace = daemon.jobs_dir / "local"
    workspace.mkdir()
    local_spec = JobSpec(
        id="local",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=99_999,
        pgid=99_999,
        started_at="2026-07-25T22:26:40+00:00",
    )
    local_spec.write(daemon._spec_path(local_spec.id))
    local_handle = LocalHandle()
    daemon._running[local_spec.id] = _RunningJob(
        popen=local_handle,  # type: ignore[arg-type]
        cpus=1,
        mem_mb=None,
        stdout_fh=(workspace / "stdout.log").open("ab"),
        stderr_fh=(workspace / "stderr.log").open("ab"),
    )

    mock = MockDispatcher()
    original_submit = mock.submit

    def submit_and_complete_local(**kwargs: object) -> SchedulerHandle:
        handle = original_submit(**kwargs)  # type: ignore[arg-type]
        action_order.append(f"scheduler-submit:{kwargs['job_id']}")
        local_handle.exited = True
        return handle

    monkeypatch.setattr(mock, "submit", submit_and_complete_local)
    _inject(daemon, mock)
    _submit_scheduler(daemon, "scheduler1")
    _submit_scheduler(daemon, "scheduler2")

    daemon._dispatch_pending()

    assert mock.submitted == ["scheduler1", "scheduler2"]
    assert local_handle.poll_count >= 1
    assert action_order.index("local-poll") < action_order.index(
        "scheduler-submit:scheduler2"
    )
    assert local_spec.id not in daemon._running
    assert JobSpec.read(daemon._spec_path(local_spec.id)).state == JobState.COMPLETED


def test_scheduler_submit_burst_interleaves_reconcile_on_bounded_quantum(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(daemon_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        daemon_mod,
        "SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS",
        1.0,
        raising=False,
    )
    mock = MockDispatcher()
    original_submit = mock.submit

    def slow_submit(**kwargs: object) -> SchedulerHandle:
        clock[0] += 0.6
        return original_submit(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(mock, "submit", slow_submit)
    _inject(daemon, mock)
    for index in range(4):
        _submit_scheduler(daemon, f"burst{index}")
    reconcile_at: list[float] = []
    monkeypatch.setattr(
        daemon,
        "_reconcile_scheduler",
        lambda: reconcile_at.append(clock[0]),
    )

    daemon._dispatch_pending()

    assert mock.submitted == ["burst0", "burst1"]
    assert reconcile_at == pytest.approx([1.2])
    daemon._dispatch_pending()

    assert mock.submitted == [f"burst{index}" for index in range(4)]
    assert reconcile_at == pytest.approx([1.2, 2.4])


@pytest.mark.parametrize("new_hold", [False, True], ids=["higher-priority-arrival", "new-drain"])
def test_scheduler_quantum_rechecks_new_admission_state(
    daemon: Daemon, monkeypatch: pytest.MonkeyPatch, new_hold: bool,
) -> None:
    clock = [0.0]
    hold = [False]
    monkeypatch.setattr(daemon_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(daemon_mod, "SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS", 0.5)
    monkeypatch.setattr(
        drain, "read_effective_drain_state",
        lambda **kw: SimpleNamespace(is_full_drain=True) if hold[0] else None,
    )
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    original_submit = mock.submit

    def submit(**kwargs: object) -> SchedulerHandle:
        result = original_submit(**kwargs)  # type: ignore[arg-type]
        clock[0] += 0.6
        if len(mock.submitted) == 1:
            _submit_scheduler(daemon, "new-priority-zero")
            hold[0] = new_hold
        return result

    monkeypatch.setattr(mock, "submit", submit)
    _inject(daemon, mock)
    for i in range(3):
        old = _submit_scheduler(daemon, f"old{i}")
        old.priority = -1
        old.write(daemon._spec_path(old.id))

    daemon._dispatch_pending()
    assert mock.submitted == ["old0"]
    daemon._dispatch_pending()

    assert mock.submitted == (
        ["old0"] if new_hold else ["old0", "new-priority-zero"]
    )
    assert JobSpec.read(daemon._spec_path("old1")).state == JobState.PENDING


def test_scheduler_burst_reconcile_refreshes_per_user_quota_counters(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    owner = "1000"
    clock = [0.0]
    monkeypatch.setattr(daemon_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        daemon_mod,
        "SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS",
        0.5,
    )

    class Quotas:
        @staticmethod
        def effective_max_pending_jobs(_uid: str) -> int:
            return 2

        @staticmethod
        def effective_max_concurrent_cpus(_uid: str) -> None:
            return None

    class Config:
        quotas = Quotas()

    class Runtime:
        def __init__(self) -> None:
            self.cpus = 1
            self.owner_uid = owner

    old = _submit_scheduler(daemon, "old")
    old.state = JobState.RUNNING
    old.write(daemon._spec_path(old.id))
    first = _submit_scheduler(daemon, "first")
    second = _submit_scheduler(daemon, "second")
    specs = {spec.id: spec for spec in (old, first, second)}
    daemon._scheduler_running[old.id] = Runtime()  # type: ignore[assignment]
    daemon._job_uid = {jobid: owner for jobid in specs}
    daemon._job_spec_paths = {
        jobid: daemon._spec_path(jobid) for jobid in specs
    }
    daemon._multi_user = True
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    monkeypatch.setattr(daemon_mod, "load_config", lambda: Config())
    monkeypatch.setattr(drain, "read_effective_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(daemon, "_config_unusable", lambda: False)
    monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)
    monkeypatch.setattr(daemon, "_validate_multi_user_spec", lambda _spec: None)
    monkeypatch.setattr(daemon, "_iter_specs", lambda: iter(specs.values()))
    monkeypatch.setattr(
        daemon,
        "_read_active_spec",
        lambda jobid, *_args, **_kwargs: (specs[jobid], daemon._spec_path(jobid)),
    )
    submitted: list[str] = []

    def start(spec: JobSpec) -> bool:
        clock[0] += 0.6
        submitted.append(spec.id)
        spec.state = JobState.RUNNING
        daemon._scheduler_running[spec.id] = Runtime()  # type: ignore[assignment]
        return True

    def reconcile() -> None:
        daemon._scheduler_running.pop(old.id, None)

    monkeypatch.setattr(daemon, "_start_job", start)
    monkeypatch.setattr(daemon, "_reconcile_scheduler", reconcile)

    daemon._dispatch_pending()

    assert submitted == [first.id]
    daemon._dispatch_pending()

    assert submitted == [first.id, second.id]


def test_scheduler_submit_unknown_holds_owner_quota_in_the_same_scan(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    owner = "1000"
    clock = [0.0]
    monkeypatch.setattr(daemon_mod.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        daemon_mod,
        "SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS",
        0.5,
    )

    class Quotas:
        @staticmethod
        def effective_max_pending_jobs(_uid: str) -> int:
            return 1

        @staticmethod
        def effective_max_concurrent_cpus(_uid: str) -> None:
            return None

    class Config:
        quotas = Quotas()

    first = _submit_scheduler(daemon, "first")
    second = _submit_scheduler(daemon, "second")
    specs = {spec.id: spec for spec in (first, second)}
    daemon._job_uid = {jobid: owner for jobid in specs}
    daemon._job_spec_paths = {
        jobid: daemon._spec_path(jobid) for jobid in specs
    }
    daemon._multi_user = True
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "multi-user"))
    monkeypatch.setattr(daemon_mod, "load_config", lambda: Config())
    monkeypatch.setattr(drain, "read_effective_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(daemon, "_config_unusable", lambda: False)
    monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)
    monkeypatch.setattr(daemon, "_validate_multi_user_spec", lambda _spec: None)
    monkeypatch.setattr(daemon, "_iter_specs", lambda: iter(specs.values()))
    monkeypatch.setattr(
        daemon,
        "_read_active_spec",
        lambda jobid, *_args, **_kwargs: (specs[jobid], daemon._spec_path(jobid)),
    )
    submitted: list[str] = []

    def start(spec: JobSpec) -> bool:
        clock[0] += 0.6
        submitted.append(spec.id)
        spec.state = JobState.SUBMIT_OUTCOME_UNKNOWN
        spec.scheduler_state = "submit_outcome_unknown"
        return False

    monkeypatch.setattr(daemon, "_start_job", start)
    monkeypatch.setattr(daemon, "_reconcile_scheduler", lambda: None)

    daemon._dispatch_pending()

    assert submitted == [first.id]


def test_multi_user_unknown_reserves_the_admitted_cpu_weight(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    owner = "1000"
    unknown, unknown_path, _workspace = (
        _write_multi_user_unconfirmed_scheduler_spec(
            daemon,
            tmp_path,
            monkeypatch,
            cpus=4,
            jobid="unknown",
        )
    )
    unknown.state = JobState.SUBMIT_OUTCOME_UNKNOWN
    unknown.scheduler_state = "submit_outcome_unknown"
    unknown.cpus = 1
    unknown.write(unknown_path)
    next_workspace = paths.user_jobs_dir(owner) / "next"
    next_workspace.mkdir()
    next_spec = JobSpec(
        id="next",
        command=["true"],
        cwd=str(next_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter=owner,
    )
    next_path = paths.user_spec_path(owner, next_spec.id)
    next_spec.write(next_path)

    class Quotas:
        @staticmethod
        def effective_max_pending_jobs(_uid: str) -> None:
            return None

        @staticmethod
        def effective_max_concurrent_cpus(_uid: str) -> int:
            return 4

    class Config:
        quotas = Quotas()

    monkeypatch.setattr(daemon_mod, "load_config", lambda: Config())
    monkeypatch.setattr(drain, "read_effective_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(daemon, "_config_unusable", lambda: False)
    monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)
    mock = MockDispatcher()
    _inject(daemon, mock)

    daemon._dispatch_pending()

    assert JobSpec.read(next_path).state == JobState.PENDING
    assert mock.submitted == []


def test_immediate_ambiguity_recovery_holds_same_owner_quota_in_same_scan(
    daemon: Daemon,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    owner = "1000"
    first, _first_path, _workspace = _write_multi_user_unconfirmed_scheduler_spec(
        daemon,
        tmp_path,
        monkeypatch,
        jobid="first",
        create_binding=False,
    )
    first.state = JobState.PENDING
    first.scheduler_state = None
    first.started_at = None
    first.write(paths.user_spec_path(owner, first.id))
    second_workspace = paths.user_jobs_dir(owner) / "second"
    second_workspace.mkdir()
    second = JobSpec(
        id="second",
        command=["true"],
        cwd=str(second_workspace),
        cpus=1,
        scheduler_target="host_f",
        submitter=owner,
    )
    second_path = paths.user_spec_path(owner, second.id)
    second.write(second_path)

    class Quotas:
        @staticmethod
        def effective_max_pending_jobs(_uid: str) -> int:
            return 1

        @staticmethod
        def effective_max_concurrent_cpus(_uid: str) -> None:
            return None

    class Config:
        quotas = Quotas()

    monkeypatch.setattr(daemon_mod, "load_config", lambda: Config())
    monkeypatch.setattr(drain, "read_effective_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(daemon, "_config_unusable", lambda: False)
    monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    mock = MockDispatcher(submit_error=unknown_type("local observer lost"))
    mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
    _inject(daemon, mock)

    daemon._dispatch_pending()

    assert mock.submitted == ["first"]
    assert (
        JobSpec.read(paths.user_spec_path(owner, "first")).state
        == JobState.SUBMIT_OUTCOME_UNKNOWN
    )
    assert JobSpec.read(second_path).state == JobState.PENDING


def test_scheduler_target_drain_holds_host_f_but_allows_host_c(
    daemon: Daemon,
) -> None:
    host_f = MockDispatcher(submit_id="111.host_f")
    host_c = MockDispatcher(submit_id="222.host_c")
    _inject(daemon, host_f, target="host_f")
    _inject(daemon, host_c, target="host_c")
    _submit_scheduler(daemon, "twinpending1", target="host_f")
    _submit_scheduler(daemon, "marvinrun001", target="host_c")
    before = JobSpec.read(daemon._spec_path("twinpending1")).model_dump(mode="json")
    drain.write_drain_state(
        drain.DrainState(
            scheduler_hosts=["host_f"],
            reason="pbs_sched idle",
        ),
        via_rpc=False,
    )

    daemon._dispatch_pending()

    assert host_f.submitted == []
    assert host_c.submitted == ["marvinrun001"]
    assert JobSpec.read(daemon._spec_path("marvinrun001")).state == JobState.RUNNING
    after = JobSpec.read(daemon._spec_path("twinpending1")).model_dump(mode="json")
    assert after == before


def test_full_plus_scheduler_target_drain_holds_all_until_full_release(
    daemon: Daemon,
) -> None:
    host_f = MockDispatcher(submit_id="111.host_f")
    host_c = MockDispatcher(submit_id="222.host_c")
    _inject(daemon, host_f, target="host_f")
    _inject(daemon, host_c, target="host_c")
    _submit_scheduler(daemon, "twinpending1", target="host_f")
    _submit_scheduler(daemon, "marvinpending1", target="host_c")
    before_host_f = JobSpec.read(daemon._spec_path("twinpending1")).model_dump(
        mode="json"
    )
    before_host_c = JobSpec.read(daemon._spec_path("marvinpending1")).model_dump(
        mode="json"
    )
    drain.write_drain_state(
        drain.DrainState(
            full_dispatch=True,
            scheduler_hosts=["host_f"],
            reason="global stop plus pbs_sched idle",
        ),
        via_rpc=False,
    )

    daemon._dispatch_pending()

    assert host_f.submitted == []
    assert host_c.submitted == []
    assert (
        JobSpec.read(daemon._spec_path("twinpending1")).model_dump(mode="json")
        == before_host_f
    )
    assert (
        JobSpec.read(daemon._spec_path("marvinpending1")).model_dump(mode="json")
        == before_host_c
    )

    assert drain.release_full_drain(via_rpc=False) is True
    daemon._dispatch_pending()

    assert host_f.submitted == []
    assert host_c.submitted == ["marvinpending1"]
    assert JobSpec.read(daemon._spec_path("marvinpending1")).state == JobState.RUNNING
    assert (
        JobSpec.read(daemon._spec_path("twinpending1")).model_dump(mode="json")
        == before_host_f
    )


def test_gate_local_job_still_bounded_by_cpu_budget(daemon: Daemon) -> None:
    # The local path is unchanged: a 20-core LOCAL job stays PENDING on max_cpus=4.
    workspace = daemon.jobs_dir / "biglocal"
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(id="biglocal", command=["true"], cwd=str(workspace), cpus=20)
    spec.write(daemon._spec_path("biglocal"))
    daemon._dispatch_pending()
    assert JobSpec.read(daemon._spec_path("biglocal")).state == JobState.PENDING


# --------------------------------------------------------------------------- #
# remote workspace cleanup
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("scheduler_job_id", "expected_job_id"),
    [
        ("555.cluster", "555.cluster"),
        (None, "j1"),
        ("", "j1"),
    ],
    ids=["recorded-id", "missing-id", "empty-id"],
)
def test_scheduler_cleanup_rebuilds_full_handle_with_id_fallback(
    daemon: Daemon,
    scheduler_job_id: str | None,
    expected_job_id: str,
) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    spec.scheduler_job_id = scheduler_job_id
    spec.state = JobState.COMPLETED
    spec.array_index = 2
    spec.array_total = 5
    spec.array_group_id = "cleanup-array"

    daemon._cleanup_scheduler_remote_workspace(spec)

    assert mock.cleaned == [(expected_job_id, "/remote/j1", None)]


# --------------------------------------------------------------------------- #
# reattach across daemon restart
# --------------------------------------------------------------------------- #


def _running_scheduler_spec(
    daemon: Daemon,
    jobid: str,
    *,
    target: str = "host_f",
    scheduler_job_id: str | None = "555.cluster",
    scheduler_state: str | None = None,
) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        scheduler_target=target,
        scheduler_job_id=scheduler_job_id,
        scheduler_state=scheduler_state,
        state=JobState.RUNNING,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def test_reattach_scheduler_job_rebuilds_handle(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _running_scheduler_spec(daemon, "j1")
    spec.array_index = 2
    spec.array_total = 5
    spec.array_group_id = "reattach-array"
    assert daemon._reattach_scheduler_job(spec) is True
    sj = daemon._scheduler_running["j1"]
    assert (
        sj.handle.job_id,
        sj.handle.remote_workspace,
        sj.handle.array_size,
    ) == ("555.cluster", "/remote/j1", None)


def test_reattach_empty_scheduler_job_id_is_used_without_recovery(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher()
    mock.recorded = "18109.host_f"
    _inject(daemon, mock)
    spec = _running_scheduler_spec(daemon, "j1", scheduler_job_id="")

    assert daemon._reattach_scheduler_job(spec) is True

    assert mock.recorded_reads == 0
    handle = daemon._scheduler_running["j1"].handle
    assert (
        handle.job_id,
        handle.remote_workspace,
        handle.array_size,
    ) == ("", "/remote/j1", None)


def test_reattach_without_scheduler_job_id_fails(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _running_scheduler_spec(daemon, "j1", scheduler_job_id=None)
    assert daemon._reattach_scheduler_job(spec) is False
    assert mock.recorded_reads == 1
    assert "j1" not in daemon._scheduler_running


def test_startup_reattaches_running_scheduler_job(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    _running_scheduler_spec(daemon, "j1")
    daemon._reattach_or_interrupt_at_startup()
    # Reattached, not aborted: still RUNNING and tracked.
    assert "j1" in daemon._scheduler_running
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.RUNNING


def test_startup_defers_scheduler_job_with_no_id(daemon: Daemon) -> None:
    mock = MockDispatcher()
    _inject(daemon, mock)
    spec = _running_scheduler_spec(daemon, "j1", scheduler_job_id=None)
    daemon._reattach_or_interrupt_at_startup()
    assert "j1" not in daemon._scheduler_running
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "reattach_failed"
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("reason") == "scheduler job could not be reattached at startup"
    ][-1]
    assert transition["from"] == JobState.RUNNING.value
    assert transition["to"] == JobState.RUNNING.value


def test_deferred_scheduler_reattach_holds_scheduler_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=4,
        max_scheduler_jobs=1,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    mock = MockDispatcher()
    _inject(d, mock)
    _running_scheduler_spec(
        d, "lost", scheduler_job_id=None, scheduler_state="reattach_failed"
    )
    _submit_scheduler(d, "next")

    d._dispatch_pending()

    assert mock.submitted == []
    assert JobSpec.read(d._spec_path("lost")).state == JobState.RUNNING
    assert JobSpec.read(d._spec_path("next")).state == JobState.PENDING


def test_deferred_scheduler_reattach_retries_and_clears_status(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _running_scheduler_spec(
        daemon, "j1", scheduler_job_id="555.cluster", scheduler_state="reattach_failed"
    )

    daemon._retry_deferred_scheduler_reattach()

    assert "j1" in daemon._scheduler_running
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state is None
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("reason")
        == "scheduler job reattached after deferred startup recovery"
    ][-1]
    assert transition["evidence"]["scheduler_job_id"] == "555.cluster"


# --------------------------------------------------------------------------- #
# status telemetry (cluster-side queued/running, heartbeat)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("phase", "expected"),
    [(SchedulerPhase.RUNNING, "running"), (SchedulerPhase.PENDING, "queued")],
)
def test_reconcile_stamps_cluster_status(
    daemon: Daemon, phase: SchedulerPhase, expected: str
) -> None:
    mock = MockDispatcher(phase=phase)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.scheduler_state == expected
    assert on_disk.last_heartbeat_at is not None
    assert "j1" in daemon._scheduler_running  # still live, not reaped


def test_reconcile_status_transition_queued_to_running(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.PENDING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == "queued"
    # The cluster job starts running -> next poll updates the status.
    mock.phase = SchedulerPhase.RUNNING
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == "running"


def test_reconcile_no_status_write_when_unchanged(daemon: Daemon) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    hb1 = JobSpec.read(daemon._spec_path("j1")).last_heartbeat_at
    # Second poll, same phase, within the refresh window: NOT rewritten.
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).last_heartbeat_at == hb1


def test_reconcile_stamps_qstat_detail(daemon: Daemon) -> None:
    detail = QstatDetail(
        raw_state="R",
        exec_host="node07/0-19",
        walltime_used="02:00:00",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING, detail=detail)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.scheduler_state == "running"
    assert on_disk.scheduler_exec_host == "node07/0-19"
    assert on_disk.scheduler_walltime_used == "02:00:00"
    assert on_disk.scheduler_walltime_limit == "08:00:00"


def test_reconcile_detail_keeps_job_live_when_coarse_poll_misses(
    daemon: Daemon,
) -> None:
    detail = QstatDetail(
        raw_state="R",
        exec_host="node07/0-19",
        walltime_used="00:03:00",
        walltime_limit="08:00:00",
    )
    mock = MockDispatcher(phase=SchedulerPhase.FINISHED, rc=0, detail=detail)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "running"
    assert on_disk.scheduler_exec_host == "node07/0-19"
    assert mock.fetched == []
    assert "j1" in daemon._scheduler_running


def test_reconcile_preserves_scheduler_hold_state(daemon: Daemon) -> None:
    detail = QstatDetail(raw_state="H")
    mock = MockDispatcher(phase=SchedulerPhase.PENDING, detail=detail)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).scheduler_state == "held"


def test_reconcile_poll_failure_keeps_scheduler_job_running(daemon: Daemon) -> None:
    mock = MockDispatcher(poll_error=SchedulerError("qstat timed out"))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    before = JobSpec.read(daemon._spec_path("j1"))
    before.last_heartbeat_at = "2026-08-11T00:00:00+00:00"
    before.write(daemon._spec_path("j1"))

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert on_disk.last_heartbeat_at == "2026-08-11T00:00:00+00:00"
    assert on_disk.scheduler_poll_last_attempted_at is not None
    assert on_disk.scheduler_poll_last_success_at is None
    assert on_disk.scheduler_poll_last_error_at is not None
    assert on_disk.scheduler_poll_last_error == "qstat timed out"
    assert mock.fetched == []
    assert "j1" in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("reason") == "scheduler poll failed; daemon will retry"
    ][-1]
    assert transition["from"] == JobState.RUNNING.value
    assert transition["to"] == JobState.RUNNING.value
    assert transition["evidence"]["scheduler_job_id"] == "555.cluster"
    assert transition["evidence"]["poll_error"] == "qstat timed out"

    mock.poll_error = None
    mock.phase = SchedulerPhase.RUNNING
    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "running"
    assert on_disk.scheduler_poll_last_success_at is not None
    assert on_disk.scheduler_poll_last_error_at is not None
    assert on_disk.scheduler_poll_last_error == "qstat timed out"
    assert mock.fetched == []
    assert "j1" in daemon._scheduler_running


def test_poll_failure_warning_names_vq_job_and_scheduler_handle(
    daemon: Daemon,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock = MockDispatcher(poll_error=SchedulerError("qstat timed out"))
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "named-vq-job"))

    with caplog.at_level("WARNING", logger="vq.daemon"):
        daemon._reconcile_scheduler()

    assert any(
        "named-vq-job" in record.message and "555.cluster" in record.message
        for record in caplog.records
    )


def test_explicitly_unknown_scheduler_handle_enters_terminal_reconciliation(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        accounting_required_for_absent=True,
        explicitly_absent_job_ids={"555.cluster"},
    )
    _inject(daemon, mock)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "dead-vq-job"))

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("dead-vq-job"))
    assert on_disk.state == JobState.COMPLETED
    assert "dead-vq-job" not in daemon._scheduler_running
    assert mock.fetched == ["555.cluster"]


def test_reconcile_poll_failure_past_requested_wall_stays_reserved_unknown(
    daemon: Daemon,
) -> None:
    """Handoff time cannot prove scheduler completion after telemetry loss."""
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()

    observed = JobSpec.read(daemon._spec_path("j1"))
    assert observed.scheduler_state == "running"
    assert observed.scheduler_poll_last_success_at is not None
    last_success = observed.scheduler_poll_last_success_at
    observed.started_at = "2020-01-01T00:00:00+00:00"
    observed.wall_time_seconds = 60
    observed.write(daemon._spec_path("j1"))

    mock.poll_error = SchedulerError("scheduler telemetry unavailable")
    daemon._reconcile_scheduler()
    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert on_disk.scheduler_poll_last_success_at == last_success
    assert on_disk.finished_at is None
    assert on_disk.exit_code is None
    assert mock.marker_queries == []
    assert mock.fetched == []
    assert mock.cancelled == []
    assert "j1" in daemon._scheduler_running


def test_reconcile_poll_failure_persists_only_bounded_sanitized_diagnostic(
    daemon: Daemon,
) -> None:
    unsafe = "qstat timed out\nBearer SECRET_TOKEN\x00" + ("x" * 1_000)
    mock = MockDispatcher(poll_error=SchedulerError(unsafe))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    diagnostic = on_disk.scheduler_poll_last_error or ""
    assert len(diagnostic) <= 240
    assert "SECRET_TOKEN" not in diagnostic
    assert "\n" not in diagnostic
    assert "\x00" not in diagnostic
    transition = [
        event
        for event in events.read_events(Path(spec.cwd))
        if event.get("kind") == "state_transition"
        and event.get("reason") == "scheduler poll failed; daemon will retry"
    ][-1]
    event_diagnostic = transition["evidence"]["poll_error"]
    assert event_diagnostic == diagnostic


def test_reconcile_absent_with_accounting_failure_stays_unknown(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        accounting_required_for_absent=True,
        detail_error=SchedulerError("sacct unavailable"),
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert on_disk.scheduler_poll_last_success_at is None
    assert on_disk.scheduler_poll_last_error_at is not None
    assert "sacct unavailable" in (on_disk.scheduler_poll_last_error or "")
    assert mock.fetched == []
    assert "j1" in daemon._scheduler_running


def test_reconcile_failed_live_poll_never_terminalizes_from_accounting(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        poll_error=SchedulerError("squeue timed out"),
        detail=QstatDetail(raw_state="FAILED", exit_code=55),
        accounting_required_for_absent=True,
        rc=0,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert mock.detail_polled == 1
    assert mock.marker_queries == []
    assert mock.fetched == []


def test_reconcile_absent_without_accounting_row_stays_unknown(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert "accounting record unavailable" in (
        on_disk.scheduler_poll_last_error or ""
    )
    assert mock.marker_queries == []
    assert mock.fetched == []


def test_reconcile_absent_with_unknown_accounting_state_stays_unknown(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        detail=QstatDetail(raw_state="MYSTERY"),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"
    assert "MYSTERY" in (on_disk.scheduler_poll_last_error or "")
    assert mock.marker_queries == []
    assert mock.fetched == []


def test_reconcile_live_squeue_wins_over_terminal_accounting(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.RUNNING,
        detail=QstatDetail(raw_state="FAILED", exit_code=55),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "running"
    assert on_disk.scheduler_poll_last_success_at is not None
    assert mock.marker_queries == []
    assert mock.fetched == []


def test_reconcile_absent_with_live_accounting_stays_live(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        detail=QstatDetail(raw_state="RUNNING"),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "running"
    assert on_disk.scheduler_poll_last_success_at is not None
    assert mock.marker_queries == []
    assert mock.fetched == []


def test_terminal_accounting_rc_waits_for_marker_fence_then_falls_back(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=None,
        detail=QstatDetail(raw_state="FAILED", exit_code=55),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    parked = JobSpec.read(daemon._spec_path("j1"))
    assert parked.state == JobState.RUNNING
    assert parked.scheduler_state == "finishing"
    assert mock.fetched == []
    runtime = daemon._scheduler_running["j1"]
    runtime.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1
    )

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.FAILED
    assert on_disk.exit_code == 55
    assert mock.fetched == ["555.cluster"]
    assert "j1" not in daemon._scheduler_running


@pytest.mark.parametrize(
    ("state", "native_exit", "expected_rc", "expected_state"),
    [
        # COMPLETED/FAILED rows classify by rc alone; scheduler-attributed
        # terminations additionally carry their accounting verdict as the
        # terminal state since #414 (previously a lossy generic FAILED).
        ("COMPLETED", "0:9", 137, JobState.FAILED),
        ("FAILED", "0:0", 1, JobState.FAILED),
        ("CANCELLED", "0:9", 137, JobState.ABORTED_BY_QUEUE),
        ("TIMEOUT", "0:0", 1, JobState.TIME_EXCEEDED),
    ],
)
def test_terminal_accounting_failure_never_falls_back_to_completed(
    daemon: Daemon,
    state: str,
    native_exit: str,
    expected_rc: int,
    expected_state: JobState,
) -> None:
    detail = SlurmDialect().parse_qstat_detail(
        f"555|{state}|{native_exit}|00:11|01:00|node001\n"
    )["555"]
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=None,
        detail=detail,
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)
    daemon._reconcile_scheduler()
    daemon._scheduler_running["j1"].finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1
    )

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state != JobState.COMPLETED
    assert on_disk.state == expected_state
    assert on_disk.exit_code == expected_rc
    if expected_state not in (JobState.FAILED, JobState.COMPLETED):
        assert on_disk.failure_reason is not None
        assert f"scheduler accounting reports {state}" in on_disk.failure_reason


def test_exit_marker_remains_authoritative_over_accounting_rc(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        rc=0,
        detail=QstatDetail(raw_state="FAILED", exit_code=55),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert mock.fetched == ["555.cluster"]


def test_reconcile_batches_one_live_and_accounting_poll_per_host(
    daemon: Daemon,
) -> None:
    mock = MockDispatcher(
        phase=SchedulerPhase.RUNNING,
        detail=QstatDetail(raw_state="RUNNING"),
        accounting_required_for_absent=True,
    )
    _inject(daemon, mock)
    for jobid in ("j1", "j2"):
        daemon._start_scheduler_job(_submit_scheduler(daemon, jobid))

    daemon._reconcile_scheduler()

    assert mock.polled == 1
    assert mock.detail_polled == 1


# --------------------------------------------------------------------------- #
# A durable exit marker outlives an unfetchable workspace (host_f 2026-07-26)
# --------------------------------------------------------------------------- #


def _reconcile_until_terminal(daemon: Daemon, jobid: str, *, ticks: int) -> JobSpec:
    for _ in range(ticks):
        daemon._reconcile_scheduler()
    return JobSpec.read(daemon._spec_path(jobid))


class TestExitMarkerSurvivesUnfetchableWorkspace:
    """Terminal state must not be gated on artifact retrieval.

    host_f 2026-07-26: jobs 04b5d4b0b46c (rc 127) and 9e2f3dc15a78 (rc 0) had both
    left PBS -- `qstat` returned Unknown Job Id -- with durable `_vq/exit-code`
    markers already read and authoritative. `fetch_results` then failed, because
    `tar -cf {ws}.result.tar -C {ws} .` cannot archive a workspace that no longer
    exists, and the daemon parked both rows as `running`/`fetch_failed`
    indefinitely. The retry it was waiting for could never succeed.
    """

    def test_zero_marker_becomes_completed_without_its_workspace(
        self, daemon: Daemon
    ) -> None:
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            rc=0,
            fetch_error=SchedulerError("tar: /remote/j1.result.tar: No such file"),
        )
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)

        on_disk = _reconcile_until_terminal(
            daemon, "j1", ticks=daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
        )

        assert on_disk.state == JobState.COMPLETED
        assert on_disk.exit_code == 0
        assert "j1" not in daemon._scheduler_running

    def test_nonzero_marker_becomes_failed_without_its_workspace(
        self, daemon: Daemon
    ) -> None:
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            rc=127,
            fetch_error=SchedulerError("tar: /remote/j1.result.tar: No such file"),
        )
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)

        on_disk = _reconcile_until_terminal(
            daemon, "j1", ticks=daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
        )

        assert on_disk.state == JobState.FAILED
        assert on_disk.exit_code == 127
        assert "j1" not in daemon._scheduler_running

    def test_the_lost_workspace_is_recorded_not_silently_dropped(
        self, daemon: Daemon
    ) -> None:
        """A `completed` row with no outputs must be distinguishable from one
        whose outputs simply are not fetched yet, or an operator chases a
        phantom fetch bug."""
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            rc=0,
            fetch_error=SchedulerError("tar: no such file"),
        )
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)

        on_disk = _reconcile_until_terminal(
            daemon, "j1", ticks=daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT
        )

        assert on_disk.state == JobState.COMPLETED
        # The workspace never came home, and the row says so rather than
        # looking like an ordinary completed job with missing outputs.
        assert on_disk.scheduler_state == "artifacts_unavailable"
        assert not (Path(on_disk.cwd) / "_vq" / "exit-code").exists(), (
            "the mock never staged a workspace, so nothing should have landed"
        )
        # A bound of 1 would reap on a single transient hiccup and throw away a
        # workspace that was still retrievable.
        assert daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT >= 2

    def test_a_transient_failure_still_parks_and_recovers(
        self, daemon: Daemon
    ) -> None:
        """The bound must not turn a recoverable hiccup into a lost workspace:
        below the limit the job stays live, and a later success fetches it."""
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            rc=0,
            fetch_error=SchedulerError("ssh: connection reset"),
        )
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)

        daemon._reconcile_scheduler()
        parked = JobSpec.read(daemon._spec_path("j1"))
        assert parked.state == JobState.RUNNING
        assert parked.scheduler_state == "fetch_failed"
        assert "j1" in daemon._scheduler_running

        # Transport recovers before the bound is reached.
        mock.fetch_error = None
        daemon._reconcile_scheduler()

        on_disk = JobSpec.read(daemon._spec_path("j1"))
        assert on_disk.state == JobState.COMPLETED
        assert (Path(on_disk.cwd) / "_vq" / "exit-code").exists()

    def test_reaping_on_the_marker_does_not_resubmit_or_retry(
        self, daemon: Daemon
    ) -> None:
        mock = MockDispatcher(
            phase=SchedulerPhase.FINISHED,
            rc=127,
            fetch_error=SchedulerError("tar: no such file"),
        )
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        daemon._start_scheduler_job(spec)

        _reconcile_until_terminal(
            daemon, "j1", ticks=daemon_mod.SCHEDULER_FETCH_FAILURE_LIMIT + 2
        )

        assert mock.submitted == ["j1"], "the job must not be resubmitted"
        assert mock.cancelled == [], "a job the scheduler already forgot needs no qdel"
        assert JobSpec.read(daemon._spec_path("j1")).state == JobState.FAILED


# --------------------------------------------------------------------------- #
# reattach recovery for a handle the driver never persisted
# --------------------------------------------------------------------------- #


class TestReattachRecoversAnUnrecordedId:
    """`_start_scheduler_job` claims RUNNING with `scheduler_job_id=None`,
    submits outside the spec lock, then records the id in a second write. A
    driver death in that window strands a spec that may well have a live
    cluster job behind it.

    Startup already refuses to reap such a spec, explicitly because "a qsub may
    have succeeded just before the driver daemon died" -- but the retry it
    defers to returned False immediately on a missing id, so the one case the
    deferral exists for was the one case it could never recover.
    """

    def _stranded(self, daemon: Daemon, jobid: str = "ghost1") -> JobSpec:
        spec = _submit_scheduler(daemon, jobid)
        spec.state = JobState.RUNNING
        spec.scheduler_job_id = None
        spec.scheduler_state = "reattach_failed"
        spec.write(daemon._spec_path(jobid))
        return spec

    def test_a_stranded_spec_recovers_the_id_the_job_recorded(
        self, daemon: Daemon
    ) -> None:
        mock = MockDispatcher()
        mock.recorded = "18109.host_f"
        _inject(daemon, mock)
        spec = self._stranded(daemon)

        assert daemon._reattach_scheduler_job(spec) is True

        assert spec.id in daemon._scheduler_running
        assert mock.recorded_reads == 1
        handle = daemon._scheduler_running[spec.id].handle
        assert (
            handle.job_id,
            handle.remote_workspace,
            handle.array_size,
        ) == ("18109.host_f", "/remote/ghost1", None)

    def test_a_stranded_spec_fences_conflicting_acceptance_proofs(
        self,
        daemon: Daemon,
    ) -> None:
        mock = MockDispatcher()
        mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
        mock.recorded = "different.host_f"
        _inject(daemon, mock)
        spec = self._stranded(daemon)

        assert daemon._reattach_scheduler_job(spec) is False

        assert spec.id not in daemon._scheduler_running
        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
        assert on_disk.scheduler_state == "submit_evidence_conflict"
        assert mock.receipt_reads == 1
        assert mock.recorded_reads == 1

    def test_startup_keeps_conflicting_acceptance_proofs_indefinitely_unknown(
        self,
        daemon: Daemon,
    ) -> None:
        mock = MockDispatcher()
        mock.receipt = SchedulerSubmitReceipt("accepted", "18109.host_f", 0)
        mock.recorded = "18110.host_f"
        _inject(daemon, mock)
        spec = self._stranded(daemon)

        daemon._reattach_or_interrupt_at_startup()

        on_disk = JobSpec.read(daemon._spec_path(spec.id))
        assert on_disk.state == JobState.SUBMIT_OUTCOME_UNKNOWN
        assert on_disk.scheduler_state == "submit_evidence_conflict"
        assert spec.id not in daemon._scheduler_running

    def test_the_recovered_id_is_persisted_to_the_spec(self, daemon: Daemon) -> None:
        """Otherwise the next restart re-runs the recovery, and every consumer
        that reads the spec still sees an unnamed job."""
        mock = MockDispatcher()
        mock.recorded = "18109.host_f"
        _inject(daemon, mock)
        spec = self._stranded(daemon)

        daemon._reattach_scheduler_job(spec)

        assert JobSpec.read(daemon._spec_path(spec.id)).scheduler_job_id == "18109.host_f"

    def test_no_recorded_id_stays_deferred_rather_than_reaped(
        self, daemon: Daemon
    ) -> None:
        """A queued job has not run its script yet; absence is not evidence."""
        mock = MockDispatcher()
        mock.recorded = None
        _inject(daemon, mock)
        spec = self._stranded(daemon)

        assert daemon._reattach_scheduler_job(spec) is False
        assert spec.id not in daemon._scheduler_running

    def test_a_spec_killed_while_unnamed_is_not_resurrected(
        self, daemon: Daemon
    ) -> None:
        """An operator who gave up on an unnamed job must not have it silently
        re-tracked by a later recovery."""
        mock = MockDispatcher()
        mock.recorded = "18109.host_f"
        _inject(daemon, mock)
        spec = self._stranded(daemon)
        killed = JobSpec.read(daemon._spec_path(spec.id))
        killed.state = JobState.KILLED
        killed.write(daemon._spec_path(spec.id))

        assert daemon._reattach_scheduler_job(spec) is False
        assert spec.id not in daemon._scheduler_running

    def test_a_spec_that_already_has_an_id_never_consults_the_cluster(
        self, daemon: Daemon
    ) -> None:
        """The ordinary reattach path must not pay an SSH round trip."""
        mock = MockDispatcher()
        mock.recorded = "should-not-be-read"
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "normal1")
        spec.state = JobState.RUNNING
        spec.scheduler_job_id = "555.cluster"
        spec.write(daemon._spec_path("normal1"))

        assert daemon._reattach_scheduler_job(spec) is True

        assert mock.recorded_reads == 0
        assert daemon._scheduler_running[spec.id].handle.job_id == "555.cluster"


class TestAnUntrackableSpecStopsReservingCapacity:
    """host_f, 2026-08-01: `qstat` showed ONE of the user's jobs while vq counted
    121 active. ~120 `reattach_failed` specs with no PBS id each reserved a
    slot against `max_scheduler_jobs`, forever, so an almost-empty cluster
    dispatched nothing.

    A spec that kept its id reserves indefinitely and correctly -- vq holds a
    handle. A spec with no id can only be recovered by `recorded_job_id`, and
    once that has had its window there is nothing left to wait for.
    """

    def _untracked(
        self, daemon: Daemon, jobid: str, *, age_seconds: float, job_id: str | None
    ) -> JobSpec:
        spec = _submit_scheduler(daemon, jobid)
        spec.state = JobState.RUNNING
        spec.scheduler_state = "reattach_failed"
        spec.scheduler_job_id = job_id
        spec.last_heartbeat_at = (
            datetime.now(UTC) - timedelta(seconds=age_seconds)
        ).isoformat()
        spec.write(daemon._spec_path(jobid))
        return spec

    def test_a_stale_untrackable_spec_releases_its_slot(self, daemon: Daemon) -> None:
        spec = self._untracked(
            daemon,
            "ghost",
            age_seconds=daemon_mod.SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS + 60,
            job_id=None,
        )

        assert daemon._untracked_spec_still_reserves(spec) is False

    def test_a_recent_untrackable_spec_still_reserves(self, daemon: Daemon) -> None:
        """A job that just started has not written its id record yet."""
        spec = self._untracked(daemon, "young", age_seconds=5, job_id=None)

        assert daemon._untracked_spec_still_reserves(spec) is True

    def test_a_spec_that_kept_its_id_reserves_indefinitely(
        self, daemon: Daemon
    ) -> None:
        """vq holds a handle, so _reconcile_scheduler will reap it; the job may
        genuinely still be running on the cluster."""
        spec = self._untracked(
            daemon,
            "tracked",
            age_seconds=daemon_mod.SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS * 10,
            job_id="18647.host_f",
        )

        assert daemon._untracked_spec_still_reserves(spec) is True

    def test_a_spec_with_no_timestamp_reserves(self, daemon: Daemon) -> None:
        """Nothing to age against is not evidence of staleness."""
        spec = _submit_scheduler(daemon, "notime")
        spec.state = JobState.RUNNING
        spec.scheduler_state = "reattach_failed"
        spec.scheduler_job_id = None
        spec.last_heartbeat_at = None
        spec.started_at = None
        spec.write(daemon._spec_path("notime"))

        assert daemon._untracked_spec_still_reserves(spec) is True

    def test_the_released_slot_is_actually_usable(self, daemon: Daemon) -> None:
        """The point of the fix: a pending job dispatches behind the ghost."""
        daemon.max_scheduler_jobs = 1
        self._untracked(
            daemon,
            "ghost",
            age_seconds=daemon_mod.SCHEDULER_UNTRACKABLE_RESERVATION_SECONDS + 60,
            job_id=None,
        )
        mock = MockDispatcher()
        _inject(daemon, mock)
        _submit_scheduler(daemon, "waiting")

        daemon._dispatch_pending()

        assert mock.submitted == ["waiting"]

    def test_a_fresh_ghost_still_blocks_the_last_slot(self, daemon: Daemon) -> None:
        """The reservation is bounded, not removed."""
        daemon.max_scheduler_jobs = 1
        self._untracked(daemon, "ghost", age_seconds=5, job_id=None)
        mock = MockDispatcher()
        _inject(daemon, mock)
        _submit_scheduler(daemon, "waiting")

        daemon._dispatch_pending()

        assert mock.submitted == []

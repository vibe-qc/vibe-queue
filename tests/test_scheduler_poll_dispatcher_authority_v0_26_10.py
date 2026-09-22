"""#51 follow-up: one host, one observation, and one dispatcher per job.

The per-host keying is right and stays: a host's whole batch rides on one
connection, so its cooldown and its "at most one observation in flight" bound
belong to the host and not to a dispatcher object. What came with it was a
helper that handed **every** operation in the pass to ``items[0][1].dispatcher``
-- the first job's generation, by dict order -- on the claim that "every
generation of a target addresses the same configured host".

That claim is false. ``_config_fingerprint()`` rebuilds a target's dispatcher
on any config edit, so two live generations of one target can differ in
``ssh``, ``scheduler_dialect`` and ``submit_extra``. The batch dispatcher does
not only poll: it also carried ``cancel()`` (which runs ``qdel``),
``exit_marker_code()`` and ``phase_from_detail()``. Cancelling a job through a
generation whose alias or dialect is not the one it was submitted under is the
sharp end, and grouping by dispatcher object used to keep those apart by
accident.

So the split this file pins:

* the OBSERVER issues the batch and owns everything that reads the rows it
  came back with;
* the JOB'S OWN generation owns everything addressed to that one job.

It also pins the observer's identity (the generation the daemon serves for the
target now, carried on the flight rather than re-derived at reap time) and one
smaller thing in the same area: a local ssh that never started is undetermined,
not an answer from the host.

Everything runs against fakes: no SSH, no subprocess, no cluster. Site
identifiers are the placeholders the sibling modules use (``host_f``).
"""
from __future__ import annotations

import subprocess

import pytest

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from vq.daemon import Daemon
from vq.scheduler_dialect import QstatDetail, SchedulerPhase
from vq.spec import JobSpec, JobState


class _NamedGeneration(MockDispatcher):
    """A ``MockDispatcher`` that records which instance answered what.

    The point of the whole file: two generations must be *distinguishable*.
    ``test_two_dispatcher_generations_share_one_observation_in_flight``
    could not observe the thing it named because both of its mocks were
    identical, so any routing at all satisfied it.
    """

    def __init__(self, name: str, **kw: object) -> None:
        super().__init__(**kw)  # type: ignore[arg-type]
        self.name = name
        self.phase_from_detail_calls: list[str] = []
        self.abnormal_calls: list[str] = []
        self.polled_handles: list[list[str]] = []

    def poll(self, handles: list) -> dict[str, SchedulerPhase]:  # type: ignore[override]
        self.polled_handles.append([h.job_id for h in handles])
        return super().poll(handles)

    def phase_from_detail(self, detail: QstatDetail) -> SchedulerPhase | None:
        self.phase_from_detail_calls.append(detail.raw_state)
        return super().phase_from_detail(detail)

    def abnormal_termination_from_detail(self, detail: QstatDetail) -> str | None:
        self.abnormal_calls.append(detail.raw_state)
        return super().abnormal_termination_from_detail(detail)


def _two_generations(
    vqd: Daemon,
    *,
    first: _NamedGeneration,
    second: _NamedGeneration,
) -> tuple[JobSpec, JobSpec]:
    """One host, two live dispatcher generations, one job on each.

    The second arrives on a rebuilt dispatcher, which is what an edit to
    config.toml produces: the daemon drops the target's cached dispatcher
    while every in-flight job keeps the generation it was submitted with.
    """
    first.submit_id = "j1.cluster"
    second.submit_id = "j2.cluster"
    _inject(vqd, first)
    job_a = _submit_scheduler(vqd, "j1")
    vqd._start_scheduler_job(job_a)
    _inject(vqd, second)
    job_b = _submit_scheduler(vqd, "j2")
    vqd._start_scheduler_job(job_b)
    assert vqd._scheduler_running["j1"].dispatcher is first
    assert vqd._scheduler_running["j2"].dispatcher is second
    return job_a, job_b


# --------------------------------------------------------------------------
# The observer
# --------------------------------------------------------------------------


def test_one_host_spanning_two_generations_still_costs_one_observation(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The per-host bound, re-stated with generations that can be told apart.

    The predecessor of this test used two identical mocks, so it could not see
    which generation polled or whether both jobs' handles were in the batch.
    Both are asserted here.
    """
    first = _NamedGeneration("old", phase=SchedulerPhase.RUNNING)
    second = _NamedGeneration("new", phase=SchedulerPhase.RUNNING)
    _two_generations(daemon, first=first, second=second)

    daemon._reconcile_scheduler()

    assert (first.polled, second.polled) == (0, 1), (
        "one host, one connection -- and the generation the daemon serves for "
        "the target now is the one that makes it"
    )
    assert second.polled_handles == [["j1.cluster", "j2.cluster"]], (
        "the single observation must carry BOTH generations' handles; a "
        "generation that polls only its own jobs leaves the others unobserved"
    )


def test_the_observer_is_the_generation_the_daemon_serves_now(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """Not whichever job happens to be first in the running dict.

    An edit to ``ssh`` or ``scheduler_dialect`` is the operator saying how the
    host is reached *now*; an in-flight job's older generation records how it
    was reached *then*.
    """
    first = _NamedGeneration("old", phase=SchedulerPhase.RUNNING)
    second = _NamedGeneration("new", phase=SchedulerPhase.RUNNING)
    _two_generations(daemon, first=first, second=second)
    items = list(daemon._scheduler_running.items())

    assert daemon._scheduler_observation_dispatcher("host_f", items) is second

    # A reload empties the cache; every generation in the batch is then
    # equally stale, and the host stays observable rather than being stranded
    # on a config entry that is no longer there.
    daemon._scheduler_dispatchers.clear()
    assert daemon._scheduler_observation_dispatcher("host_f", items) is first


def test_the_flight_carries_its_observer_instead_of_re_deriving_it(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The cache can be rebuilt while an observation is in the air.

    Re-deriving the observer at reap time would then hand one generation's
    rows to another generation's dialect.
    """
    first = _NamedGeneration("old", phase=SchedulerPhase.RUNNING)
    second = _NamedGeneration("new", phase=SchedulerPhase.RUNNING)
    _two_generations(daemon, first=first, second=second)
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()
    flight = daemon._scheduler_poll_flights["host_f"]
    assert flight.done.wait(30.0), "the observation must not hang"
    assert flight.observer is second
    # The rebuild lands between the flight starting and the pass reaping it.
    third = _NamedGeneration("newer", phase=SchedulerPhase.RUNNING)
    _inject(daemon, third)

    daemon._reconcile_scheduler()

    assert third.phase_from_detail_calls == []
    assert third.polled == 0


# --------------------------------------------------------------------------
# Per-job operations
# --------------------------------------------------------------------------


def test_a_qdel_goes_through_the_generation_that_submitted_the_job(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The sharp end. An external ``vq kill`` leaves the spec terminal while
    the allocation is still listed, and the daemon escalates to ``qdel``. Sent
    over another generation's ssh alias or dialect, that addresses a host, or
    a scheduler, the allocation is not on.

    Both jobs are killed, so neither assertion can pass by the batch carrier
    happening to be the right generation for the job under test.
    """
    first = _NamedGeneration("old", phase=SchedulerPhase.RUNNING)
    second = _NamedGeneration("new", phase=SchedulerPhase.RUNNING)
    _two_generations(daemon, first=first, second=second)
    for jobid in ("j1", "j2"):
        killed = JobSpec.read(daemon._spec_path(jobid))
        killed.state = JobState.KILLED
        killed.write(daemon._spec_path(jobid))

    daemon._reconcile_scheduler()

    assert first.cancelled == ["j1.cluster"], (
        "each job must be cancelled through the generation it was submitted "
        "under, not through whichever one carried the batch"
    )
    assert second.cancelled == ["j2.cluster"]


def test_a_marker_read_and_its_diagnostics_use_the_job_s_generation(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """Both address that job's own workspace over that job's own transport,
    the way ``fetch_results`` already did."""
    first = _NamedGeneration("old", rc=None)
    second = _NamedGeneration("new", rc=None)
    _two_generations(daemon, first=first, second=second)

    daemon._reconcile_scheduler()

    assert [job for job, _ in first.marker_queries] == ["j1.cluster"]
    assert [job for job, _ in second.marker_queries] == ["j2.cluster"]


# --------------------------------------------------------------------------
# Row interpretation
# --------------------------------------------------------------------------


def test_the_rows_are_read_by_the_dialect_that_produced_them(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """``phase_from_detail`` and ``abnormal_termination_from_detail`` are
    questions about *this observation*, so they belong to the generation that
    made it -- for every job in the batch, including the ones submitted
    through another generation."""
    detail = QstatDetail(raw_state="RUNNING")
    first = _NamedGeneration("old", detail=detail)
    second = _NamedGeneration(
        "new", detail=detail, accounting_required_for_absent=True
    )
    _two_generations(daemon, first=first, second=second)

    daemon._reconcile_scheduler()

    assert second.phase_from_detail_calls == ["RUNNING", "RUNNING"], (
        "both jobs' rows came back from the observer's poll, so both are "
        "mapped by the observer's dialect"
    )
    assert first.phase_from_detail_calls == []


def test_accounting_authority_follows_the_observer(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """Whether absence from the live table needs an accounting record is a
    property of the dialect that read the live table."""
    first = _NamedGeneration("old", accounting_required_for_absent=True)
    second = _NamedGeneration("new", accounting_required_for_absent=False)
    _two_generations(daemon, first=first, second=second)

    daemon._reconcile_scheduler()

    # The observer says absence alone is terminal proof, so both jobs walk on
    # to the exit-marker fence rather than parking on a missing sacct row.
    assert [job for job, _ in first.marker_queries] == ["j1.cluster"]
    assert [job for job, _ in second.marker_queries] == ["j2.cluster"]


# --------------------------------------------------------------------------
# A local ssh that never started
# --------------------------------------------------------------------------


class TestALocalLaunchFailureIsNotAnAnswer:
    """``_scheduler_poll_answer``'s own docstring says a locally raised
    ``OSError`` is undetermined, and the runner wrapped one into a plain
    ``SchedulerError``, which reads as "the host answered" and clears the
    cooldown. Reachability is narrow -- ``FileNotFoundError`` or
    ``PermissionError`` on the ssh binary -- but a fast-failing local fault is
    exactly the shape pacing exists for.
    """

    def _runner(self, monkeypatch: pytest.MonkeyPatch, exc: BaseException):  # noqa: ANN202
        from vq import transport
        from vq.config import HostConfig
        from vq.scheduler_dispatch import SshRemoteRunner

        def _raise(*_a: object, **_kw: object) -> object:
            raise transport.RemoteLaunchError(
                "local ssh process did not start for host-f.invalid: "
                f"{exc}"
            ) from exc

        monkeypatch.setattr(transport, "run_remote_shell", _raise)
        return SshRemoteRunner(HostConfig(ssh="host-f.invalid"))

    @pytest.mark.parametrize(
        "cause",
        [FileNotFoundError("ssh"), PermissionError("ssh")],
    )
    def test_a_poll_that_never_launched_is_undetermined(
        self, monkeypatch: pytest.MonkeyPatch, cause: BaseException
    ) -> None:
        from vq.scheduler_dispatch import SchedulerRemoteOutcomeUnknown

        runner = self._runner(monkeypatch, cause)

        with pytest.raises(SchedulerRemoteOutcomeUnknown):
            runner.run(["qstat", "-f", "1.cluster"])

    def test_and_the_cooldown_treats_it_as_no_answer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import daemon as daemon_module
        from vq.scheduler_dispatch import SchedulerRemoteOutcomeUnknown

        runner = self._runner(monkeypatch, FileNotFoundError("ssh"))
        try:
            runner.run(["qstat", "-f", "1.cluster"])
        except SchedulerRemoteOutcomeUnknown as exc:
            answer = daemon_module._scheduler_poll_answer(exc)
        else:  # pragma: no cover - the call above must raise
            pytest.fail("the launch failure must surface")

        assert answer is daemon_module._SchedulerPollAnswer.UNDETERMINED

    def test_a_submit_that_never_launched_is_still_a_plain_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Narrowed to the poll commands on purpose: the submit path turns
        ``SchedulerRemoteOutcomeUnknown`` into a quarantined transaction, and
        a launch that provably never happened must not produce one."""
        from vq.scheduler_dispatch import (
            SchedulerError,
            SchedulerRemoteOutcomeUnknown,
        )

        runner = self._runner(monkeypatch, FileNotFoundError("ssh"))

        with pytest.raises(SchedulerError) as caught:
            runner.run(["qsub", "job.sh"])

        assert not isinstance(caught.value, SchedulerRemoteOutcomeUnknown)


def test_a_subprocess_failure_is_still_undetermined() -> None:
    """The neighbouring classifications must not have moved."""
    from vq import daemon as daemon_module

    assert daemon_module._scheduler_poll_answer(
        subprocess.SubprocessError("lost")
    ) is daemon_module._SchedulerPollAnswer.UNDETERMINED

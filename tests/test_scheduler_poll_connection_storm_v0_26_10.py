"""A host that refuses vq's connections must not be handed more of them (#51).

#51 reports 1,760 consecutive scheduler-poll failures against one host, every
one of them ssh saying the connection never completed, while the cluster itself
was healthy. Its hypothesis was a *burst*: that poll retries go out over
``_multiplex_bypass_options()`` fresh connections and trip a login node's
``MaxStartups`` throttle. Nobody had measured it.

The first three tests here are that measurement, and they **disprove the
hypothesis as stated**. Driving the real transport's ssh argv construction over
a faked subprocess boundary, a campaign-scale pass against a host that refuses
every connection produces exactly three ssh invocations, strictly sequential,
with the two retries using fresh connections after #169. Attempts remain
sequential and bounded; there is no burst to throttle.

What the same measurement does show is a defect of a different shape: vq's rate
against a refusing host was set by nothing but how long the failures take. Each
pass costs three attempts, the last of which is followed immediately by the next
pass's first attempt, for as long as the condition lasts — 300 attempts an hour,
forever, which is the shape of the reported 1,760. So a host that answers with a
connection-level refusal now gets a cooldown, host-wide, cleared by the first
poll that succeeds.

Everything here runs against fakes: no SSH, no subprocess, no cluster, and no
sleeping through a five-minute ceiling. Site identifiers are the placeholders
the sibling modules use (``host_a``, ``cluster``).
"""

from __future__ import annotations

import logging
import subprocess

import pytest

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from vq import daemon as daemon_module
from vq import transport
from vq.config import HostConfig
from vq.daemon import Daemon
from vq.scheduler_dialect import SchedulerPhase, SlurmDialect, TorqueDialect
from vq.scheduler_dispatch import (
    SCHEDULER_POLL_TIMEOUT_SECONDS,
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    SchedulerRemoteOutcomeUnknown,
    SshRemoteRunner,
)
from vq.spec import JobSpec, JobState

# OpenSSH's own words for the incident's failure, and for the shape a login
# node's MaxStartups throttle produces. No real host is named.
BANNER_TIMEOUT_STDERR = (
    "ssh: connect to host host_a port 22: "
    "Connection timed out during banner exchange"
)
MAX_STARTUPS_STDERR = (
    "kex_exchange_identification: Connection closed by remote host\n"
    "Connection closed by 192.0.2.10 port 22"
)
# Also exit 255, also "no job was observed" -- and not load-dependent at all.
AUTH_FAILURE_STDERR = "host_a: Permission denied (publickey).\n"


# The new names are reached through their modules rather than imported, so this
# file still collects against a build that does not have them and each test then
# fails on its own claim instead of on collection -- the same arrangement as
# tests/test_scheduler_poll_batching_v0_26_10.py.


def _host_unreachable() -> type[SchedulerError]:
    """The class the runner raises for a connection the host refused."""
    from vq import scheduler_dispatch  # noqa: PLC0415

    unreachable = getattr(scheduler_dispatch, "SchedulerHostUnreachable", None)
    if unreachable is None:
        raise AssertionError(
            "vq.scheduler_dispatch has no SchedulerHostUnreachable: a refused "
            "connection cannot be told from a standing auth fault"
        )
    return unreachable


def _backoff_seconds() -> float:
    value = getattr(daemon_module, "SCHEDULER_POLL_BACKOFF_SECONDS", None)
    if value is None:
        raise AssertionError(
            "vq.daemon has no SCHEDULER_POLL_BACKOFF_SECONDS: a host that "
            "refused the poll connection is given no cooldown at all"
        )
    return float(value)


def _backoff_max_seconds() -> float:
    value = getattr(daemon_module, "SCHEDULER_POLL_BACKOFF_MAX_SECONDS", None)
    if value is None:
        raise AssertionError(
            "vq.daemon has no SCHEDULER_POLL_BACKOFF_MAX_SECONDS: the poll "
            "cooldown has no ceiling"
        )
    return float(value)


def _backoff_record(vqd: Daemon, target: str = "host_f") -> object:
    """The one cooldown record for a configured scheduler target.

    Looked up by the target's *name*, which is what makes it a record about a
    host. The fallback through the cached dispatcher reads a per-object record
    too, deliberately: the tests that are about escalation rather than about
    keying should fail on their own claim, and the tests that are about keying
    assert the key set directly.
    """
    records = getattr(vqd, "_scheduler_poll_backoff", None)
    if records is None:
        raise AssertionError("the daemon keeps no per-host poll cooldown")
    if target in records:
        return records[target]
    cached = getattr(vqd, "_scheduler_dispatchers", {}).get(target)
    if cached is not None and cached in records:
        return records[cached]
    raise AssertionError(
        f"no poll cooldown is recorded for {target!r}: the host's last attempt "
        "was taken as proof that it answered"
    )


def _backoff_is_empty(vqd: Daemon) -> bool:
    return not getattr(vqd, "_scheduler_poll_backoff", {})


def _install_clock(vqd: Daemon, clock: object) -> None:
    """Give the cooldown a clock the test advances instead of sleeping."""
    if not hasattr(vqd, "_scheduler_poll_clock"):
        raise AssertionError(
            "the daemon has no poll-cooldown clock, so there is no cooldown"
        )
    vqd._scheduler_poll_clock = clock  # type: ignore[assignment]


# -- Part 1: the measurement ------------------------------------------------


class _SshBoundary:
    """Records every ssh invocation the transport would have launched.

    Installed over ``subprocess.run``/``run_owned_subprocess`` inside
    :mod:`vq.transport`, below the argv construction and the retry loop, so
    the options actually sent are what is counted. Retry sleeps advance a
    virtual clock instead of the test.
    """

    def __init__(self, stderr: str, returncode: int = 255) -> None:
        self.stderr = stderr
        self.returncode = returncode
        self.now = 0.0
        self.invocations: list[list[str]] = []
        self.at: list[float] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            self.invocations.append(list(cmd))
            self.at.append(self.now)
            # What a banner-exchange timeout costs: ssh's own ConnectTimeout.
            self.now += 10.0
            return subprocess.CompletedProcess(
                args=list(cmd), returncode=self.returncode, stdout="",
                stderr=self.stderr,
            )

        monkeypatch.setattr(transport.subprocess, "run", run)
        monkeypatch.setattr(transport, "run_owned_subprocess", run)
        monkeypatch.setattr(
            transport.time, "sleep", lambda seconds: setattr(
                self, "now", self.now + seconds
            )
        )

    @property
    def fresh_connections(self) -> list[list[str]]:
        return [
            argv for argv in self.invocations
            if "ControlPath=none" in argv or "ControlMaster=no" in argv
        ]


def _campaign_dispatcher(dialect: object, alias: str = "host_a") -> SchedulerDispatcher:
    return SchedulerDispatcher(
        dialect,  # type: ignore[arg-type]
        SshRemoteRunner(HostConfig(ssh=alias)),
        scratch_root="/home/USER",
    )


def _campaign_handles(dialect_name: str, count: int = 2000) -> list[SchedulerHandle]:
    """A campaign-scale batch: #51 calls ~2000 decks in flight ordinary use."""
    if dialect_name == "torque":
        ids = [f"{20000 + n}.host_a" for n in range(count)]
    else:
        ids = [str(900000 + n) for n in range(count)]
    return [SchedulerHandle(job_id, f"/ws/{job_id}") for job_id in ids]


@pytest.mark.parametrize(
    ("dialect", "name"),
    [(TorqueDialect(), "torque"), (SlurmDialect(), "slurm")],
)
def test_one_failing_pass_costs_exactly_three_sequential_ssh_attempts(
    monkeypatch: pytest.MonkeyPatch, dialect: object, name: str
) -> None:
    """The measured burst, pinned: 1 attempt + 2 replay-safe retries, in order.

    This is the number #51 asked for and nobody had. It is independent of how
    many handles are in flight, because the first chunk that fails aborts the
    rest of the observation.
    """
    boundary = _SshBoundary(BANNER_TIMEOUT_STDERR)
    boundary.install(monkeypatch)
    dispatcher = _campaign_dispatcher(dialect)

    with pytest.raises(SchedulerError):
        dispatcher.poll_with_evidence(_campaign_handles(name))

    assert len(boundary.invocations) == 3
    # Strictly sequential: each attempt starts after the previous one returned.
    assert boundary.at == sorted(boundary.at)
    assert all(
        later >= earlier + 10.0
        for earlier, later in zip(boundary.at, boundary.at[1:], strict=False)
    )


@pytest.mark.parametrize(
    ("dialect", "name"),
    [(TorqueDialect(), "torque"), (SlurmDialect(), "slurm")],
)
def test_read_only_poll_retries_use_bounded_fresh_connections(
    monkeypatch: pytest.MonkeyPatch, dialect: object, name: str
) -> None:
    """Read-only retries recover stale masters without adding attempts (#169)."""
    boundary = _SshBoundary(MAX_STARTUPS_STDERR)
    boundary.install(monkeypatch)
    dispatcher = _campaign_dispatcher(dialect)

    with pytest.raises(SchedulerError):
        dispatcher.poll_with_evidence(_campaign_handles(name))

    assert boundary.invocations, "the poll must have been attempted"
    assert len(boundary.invocations) == 3
    assert len(boundary.fresh_connections) == 2
    assert "ControlPath=none" not in boundary.invocations[0]
    assert boundary.at[1] > boundary.at[0]
    assert boundary.at[2] > boundary.at[1]


def test_a_refusing_host_is_re_contacted_immediately_without_a_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Why a cooldown is needed at all, stated as a measurement.

    Nothing in the dispatcher paces the next pass: five passes against a host
    that refuses everything cost five identical rounds of three attempts, the
    next beginning as soon as the last returned. Over an hour of a real main
    loop that is ~300 attempts against a host that is turning vq away.
    """
    boundary = _SshBoundary(BANNER_TIMEOUT_STDERR)
    boundary.install(monkeypatch)
    dispatcher = _campaign_dispatcher(TorqueDialect())
    handles = _campaign_handles("torque")

    for _ in range(5):
        with pytest.raises(SchedulerError):
            dispatcher.poll_with_evidence(handles)

    assert len(boundary.invocations) == 15
    # The gap from a pass's last refusal to the next pass's first attempt is
    # just the failure's own duration: the dispatcher adds nothing.
    assert boundary.at[3] - boundary.at[2] == 10.0


# -- the classification the cooldown keys off -------------------------------


def _run_poll_over(
    monkeypatch: pytest.MonkeyPatch, *, stderr: str, returncode: int = 255
) -> BaseException:
    boundary = _SshBoundary(stderr, returncode=returncode)
    boundary.install(monkeypatch)
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="host_a")).run(
            ["qstat", "1.host_a"], check=False
        )
    return caught.value


@pytest.mark.parametrize(
    "stderr",
    [BANNER_TIMEOUT_STDERR, MAX_STARTUPS_STDERR, "ssh: connect: Connection refused"],
)
def test_a_refused_connection_is_classified_as_host_unreachable(
    monkeypatch: pytest.MonkeyPatch, stderr: str
) -> None:
    error = _run_poll_over(monkeypatch, stderr=stderr)

    assert isinstance(error, _host_unreachable())


def test_an_auth_failure_is_not_a_throttle_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit 255 is not enough. A rejected key is a standing configuration
    fault: waiting changes nothing and would only make it slower to see."""
    error = _run_poll_over(monkeypatch, stderr=AUTH_FAILURE_STDERR)

    assert isinstance(error, SchedulerError)
    assert not isinstance(error, _host_unreachable())


def test_a_missing_scheduler_command_is_not_a_throttle_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The host answered; it simply has no ``qstat`` on the path."""
    boundary = _SshBoundary("bash: qstat: command not found", returncode=127)
    boundary.install(monkeypatch)

    result = SshRemoteRunner(HostConfig(ssh="host_a")).run(
        ["qstat", "1.host_a"], check=False
    )

    assert result.returncode == 127
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="host_a")).run(["qstat", "1.host_a"], check=True)
    assert not isinstance(caught.value, _host_unreachable())


def test_the_refusal_diagnostic_stays_bounded_and_carries_no_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """3f5ed04's reviewed contract is unchanged by the new class: the message
    names the command category and the sanitised stderr, never argument
    values."""
    from vq.daemon import _safe_scheduler_poll_diagnostic  # noqa: PLC0415

    boundary = _SshBoundary(BANNER_TIMEOUT_STDERR)
    boundary.install(monkeypatch)
    secret_id = "SECRET_JOB_ID_9999.host_a"
    with pytest.raises(SchedulerError) as caught:
        SshRemoteRunner(HostConfig(ssh="host_a")).run(
            ["qstat", secret_id], check=False
        )

    diagnostic = _safe_scheduler_poll_diagnostic(caught.value)
    assert "qstat" in diagnostic
    assert "exit 255" in diagnostic
    assert "Connection timed out during banner exchange" in diagnostic
    assert secret_id not in diagnostic
    assert len(diagnostic) <= 240


def test_the_live_poll_transport_contract_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The argv/timeout/retry contract 3f5ed04's review pinned, re-pinned here
    because this change is next door to it."""
    seen: dict[str, object] = {}

    def fake_run_remote_shell(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        seen["args"] = args
        seen.update(kwargs)
        return subprocess.CompletedProcess(
            args=["ssh"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell", fake_run_remote_shell
    )

    SshRemoteRunner(HostConfig(ssh="host_a")).run(["qstat", "1.host_a"], check=False)

    assert seen["args"][1:] == ("qstat", "1.host_a")
    assert seen["timeout"] == SCHEDULER_POLL_TIMEOUT_SECONDS
    assert seen["retry_transient"] == 2
    assert seen["check"] is False


# -- Part 2: the cooldown, at the daemon ------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _refusing_mock() -> MockDispatcher:
    return MockDispatcher(
        poll_error=_host_unreachable()(
            "qstat ssh transport failed (exit 255); stderr: "
            "Connection timed out during banner exchange"
        )
    )


def _arm(vqd: Daemon, mock: MockDispatcher, clock: _Clock) -> JobSpec:
    _inject(vqd, mock)
    _install_clock(vqd, clock)
    spec = _submit_scheduler(vqd, "j1")
    vqd._start_scheduler_job(spec)
    return spec


def test_a_host_that_refused_the_connection_is_not_re_contacted_within_the_backoff(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    assert mock.polled == 1

    # Every pass inside the cooldown, including the one an instant before it
    # ends, leaves the host alone.
    for elapsed in (0.0, 1.0, _backoff_seconds() - 0.1):
        clock.now = 1000.0 + elapsed
        daemon._reconcile_scheduler()
    assert mock.polled == 1, "the refusing host must not be polled again yet"

    clock.advance(_backoff_seconds())
    daemon._reconcile_scheduler()
    assert mock.polled == 2, "the cooldown must end, not become giving up"


def test_the_cooldown_is_per_host_so_a_new_job_cannot_reopen_it(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The connection a host refused is the one connection its whole batch
    rides on, so the cooldown belongs to the host. A per-job cooldown would let
    the next arrival in a campaign re-open a host that is turning vq away --
    and #51's incident ran during a campaign that kept submitting.

    The newcomer here arrives on a *rebuilt* dispatcher, which is what an edit
    to config.toml produces: the daemon drops the target's cached dispatcher
    while every in-flight job keeps the generation it was submitted with. One
    host, two live generations. Keying the cooldown on the dispatcher object
    made this test pass while the host got two of everything, so the newcomer
    is given a new generation on purpose."""
    clock = _Clock()
    old_generation = _refusing_mock()
    _arm(daemon, old_generation, clock)

    daemon._reconcile_scheduler()
    assert old_generation.polled == 1

    new_generation = _refusing_mock()
    _inject(daemon, new_generation)
    newcomer = _submit_scheduler(daemon, "j2")
    daemon._start_scheduler_job(newcomer)
    assert daemon._scheduler_running["j2"].dispatcher is new_generation
    assert daemon._scheduler_running["j1"].dispatcher is old_generation

    daemon._reconcile_scheduler()

    assert (old_generation.polled, new_generation.polled) == (1, 0), (
        "a newly submitted job must not reopen the host, whichever dispatcher "
        "generation it was dispatched through"
    )
    assert set(daemon._scheduler_poll_backoff) == {"host_f"}, (
        "one host must have one cooldown record, named for the host"
    )
    assert JobSpec.read(daemon._spec_path("j2")).state == JobState.RUNNING

    # One cooldown, one attempt: not one per generation.
    clock.advance(_backoff_seconds())
    daemon._reconcile_scheduler()
    assert old_generation.polled + new_generation.polled == 2


def test_two_dispatcher_generations_share_one_observation_in_flight(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """Production's bound is one read-only observation in flight per scheduler
    *host* (SPEC, design doc S17). Keyed on the dispatcher object it was one
    per generation, so a config edit mid-campaign doubled the connections a
    busy login node saw from vq.

    The two generations are told apart on purpose. This test used to build two
    identical mocks and assert only that one flight key existed, so it could
    not see which generation made the connection, whether the other one made
    one too, or whether the single observation carried both jobs' handles --
    the last of which is what makes one observation per host correct rather
    than merely cheap. All three are asserted now.
    """
    clock = _Clock()
    old_generation = _refusing_mock()
    old_generation.submit_id = "j1.cluster"
    _arm(daemon, old_generation, clock)
    new_generation = _refusing_mock()
    new_generation.submit_id = "j2.cluster"
    _inject(daemon, new_generation)
    daemon._start_scheduler_job(_submit_scheduler(daemon, "j2"))
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()

    assert list(daemon._scheduler_poll_flights) == ["host_f"]
    (flight,) = daemon._scheduler_poll_flights.values()
    assert flight.done.wait(30.0), "the observation must not hang"
    assert sorted(job_id for job_id, _ in flight.items) == ["j1", "j2"], (
        "one observation has to cover the host, not one generation's share "
        "of it: a job left out of the batch is a job left unobserved"
    )
    assert (old_generation.polled, new_generation.polled) == (0, 1), (
        "exactly one generation connects, and it is the one the daemon serves "
        "for the target now"
    )


def test_the_deferral_warning_names_the_host_after_a_dispatcher_rebuild(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The log line an operator reads while diagnosing exactly this incident
    has to say which host. Reverse-looking the dispatcher up in a cache the
    reload had just emptied named none."""
    clock = _Clock()
    old_generation = _refusing_mock()
    _arm(daemon, old_generation, clock)
    daemon._reconcile_scheduler()

    _inject(daemon, _refusing_mock())  # the rebuild: j1's generation is uncached
    clock.advance(_backoff_seconds())
    with caplog.at_level(logging.WARNING, logger="vq.daemon"):
        daemon._reconcile_scheduler()

    deferrals = [
        record.getMessage()
        for record in caplog.records
        if "deferring its next observation" in record.getMessage()
    ]
    assert deferrals, "the deferral must be announced"
    assert all("host_f" in message for message in deferrals), deferrals
    assert not any("unnamed" in message for message in deferrals), deferrals


def test_the_cooldown_lengthens_while_the_host_keeps_refusing(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    intervals: list[float] = []
    for _ in range(8):
        before = clock.now
        daemon._reconcile_scheduler()
        record = _backoff_record(daemon)
        intervals.append(record.until - before)
        clock.now = record.until

    assert intervals[0] == _backoff_seconds()
    assert intervals[1] == _backoff_seconds() * 2
    assert intervals == sorted(intervals)
    assert max(intervals) == _backoff_max_seconds()
    assert mock.polled == 8, "each elapsed cooldown must be followed by a real try"


def test_the_first_successful_poll_clears_the_backoff(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    daemon._reconcile_scheduler()
    assert mock.polled == 1

    clock.advance(_backoff_seconds())
    mock.poll_error = None
    mock.phase = SchedulerPhase.RUNNING
    daemon._reconcile_scheduler()
    assert mock.polled == 2
    assert _backoff_is_empty(daemon)

    # Back to the normal cadence immediately: no residual cooldown.
    daemon._reconcile_scheduler()
    assert mock.polled == 3


def test_a_failure_that_is_not_a_refusal_does_not_defer_the_next_pass(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """An auth fault or an unparseable table is not load-dependent, so the host
    keeps the ordinary cadence and the problem stays visible."""
    clock = _Clock()
    mock = MockDispatcher(poll_error=SchedulerError("qstat: command not found"))
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    daemon._reconcile_scheduler()

    assert mock.polled == 2
    assert _backoff_is_empty(daemon)


def test_a_refusal_ends_when_the_failure_changes(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    clock.advance(_backoff_seconds())
    mock.poll_error = SchedulerError("qstat: command not found")
    daemon._reconcile_scheduler()

    assert _backoff_is_empty(daemon)
    daemon._reconcile_scheduler()
    assert mock.polled == 3


# -- what the cooldown must never do ----------------------------------------


def test_a_deferred_pass_never_terminalizes_or_restates_a_job(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """A deferred pass made no observation, so it claims none: the job keeps
    the lifecycle state and the diagnostic its last real attempt left."""
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    after_attempt = JobSpec.read(daemon._spec_path("j1"))
    assert after_attempt.state == JobState.RUNNING
    assert after_attempt.scheduler_state == "poll_failed"

    for _ in range(20):
        clock.advance(1.0)
        daemon._reconcile_scheduler()

    after_deferral = JobSpec.read(daemon._spec_path("j1"))
    assert after_deferral.state == JobState.RUNNING
    assert after_deferral.scheduler_state == "poll_failed"
    assert (
        after_deferral.scheduler_poll_last_attempted_at
        == after_attempt.scheduler_poll_last_attempted_at
    ), "a pass that did not poll must not claim an attempt"
    assert mock.marker_queries == []
    assert mock.fetched == []
    assert mock.cancelled == []


def test_a_long_storm_never_makes_a_job_terminal(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    for _ in range(200):
        clock.advance(_backoff_max_seconds())
        daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert not on_disk.is_terminal
    assert on_disk.scheduler_state == "poll_failed"
    assert "j1" in daemon._scheduler_running


def test_one_refusing_host_does_not_defer_another(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    clock = _Clock()
    refusing = _refusing_mock()
    healthy = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, refusing, target="host_f")
    _inject(daemon, healthy, target="host_g")
    _install_clock(daemon, clock)
    for jobid, target in (("j1", "host_f"), ("j2", "host_g")):
        spec = _submit_scheduler(daemon, jobid, target=target)
        daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    daemon._reconcile_scheduler()

    assert refusing.polled == 1, "the refusing host waits"
    assert healthy.polled == 2, "the healthy host keeps its cadence"


# -- Part 3: what ends a run of attempts that did not reach the host --------
#
# The cooldown used to key off "was this a refusal", and popped its record for
# every other outcome. Two hosts behave badly in exactly the way that defeats:
# one that alternates between refusing the connection and letting vq's own
# 120 s poll timeout expire, and one whose jobs come and go while it refuses.
# Both pinned the cooldown at its first interval for as long as the condition
# lasted. What ends a run is evidence that the host *answered*.


def _outcome_unknown() -> SchedulerRemoteOutcomeUnknown:
    """vq's own poll timeout: no answer, and no evidence of one either.

    Raised before any exit-255 classification can run, so it can never be a
    refusal however the connection actually failed. A login node that accepts
    the connection and then hangs before the banner produces this shape.
    """
    return SchedulerRemoteOutcomeUnknown(
        "qstat remote scheduler command outcome is unknown (timeout after "
        f"{SCHEDULER_POLL_TIMEOUT_SECONDS:g}s); stderr: (empty)"
    )


def test_alternating_failure_modes_keep_escalating_the_cooldown(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """An overloaded login node does not fail the same way twice.

    Refusing some attempts and swallowing others measured [30, 0, 30, 0, ...]:
    ten polls in ten passes, because every non-refusal was read as proof the
    host was healthy. A timeout proves nothing of the kind.
    """
    clock = _Clock()
    mock = _refusing_mock()
    refusal = mock.poll_error
    _arm(daemon, mock, clock)

    intervals: list[float] = []
    for attempt in range(6):
        mock.poll_error = refusal if attempt % 2 == 0 else _outcome_unknown()
        before = clock.now
        daemon._reconcile_scheduler()
        record = _backoff_record(daemon)
        intervals.append(record.until - before)
        clock.now = record.until

    assert intervals == [
        min(_backoff_seconds() * 2 ** step, _backoff_max_seconds())
        for step in range(6)
    ]
    assert mock.polled == 6, "each elapsed cooldown must be followed by a real try"


def test_an_attempt_whose_outcome_vq_never_learned_defers_the_next_pass(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """vq's own timeout is not "a timeout against a host that did answer"."""
    clock = _Clock()
    mock = MockDispatcher(poll_error=_outcome_unknown())
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    assert mock.polled == 1
    daemon._reconcile_scheduler()
    assert mock.polled == 1, "a lost outcome is not evidence of a healthy host"

    clock.advance(_backoff_seconds())
    daemon._reconcile_scheduler()
    assert mock.polled == 2, "the cooldown must end, not become giving up"


def test_a_host_that_answers_ends_a_run_of_lost_outcomes(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The counterpart of the test above, and the deliberate part of the old
    behaviour that has to survive: an answer -- including a standing fault the
    host itself reports -- returns it to the ordinary cadence at once."""
    clock = _Clock()
    mock = MockDispatcher(poll_error=_outcome_unknown())
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    clock.advance(_backoff_seconds())
    mock.poll_error = SchedulerError("qstat: command not found")
    daemon._reconcile_scheduler()

    assert _backoff_is_empty(daemon)
    daemon._reconcile_scheduler()
    assert mock.polled == 3


def test_job_churn_on_a_silent_host_does_not_reset_the_escalation(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The escalation is a fact about the host, not about its current queue.

    Dropping the record with the host's last running job let a campaign that
    keeps submitting restart the run at 30 s every time the queue emptied for
    a tick -- on a host that had not answered once.
    """
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)

    daemon._reconcile_scheduler()
    clock.advance(_backoff_seconds())
    daemon._reconcile_scheduler()
    assert _backoff_record(daemon).consecutive == 2

    daemon._scheduler_running.pop("j1")
    daemon._reconcile_scheduler()
    daemon._start_scheduler_job(_submit_scheduler(daemon, "j2"))

    clock.advance(_backoff_seconds() * 2)
    daemon._reconcile_scheduler()

    record = _backoff_record(daemon)
    assert record.consecutive == 3
    assert record.interval == _backoff_seconds() * 4
    assert mock.polled == 3


def test_a_host_with_no_jobs_is_forgotten_once_it_has_gone_quiet(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """Remembering is bounded: a record outlives the host's jobs, not the
    ceiling. After a full ceiling with no job and no attempt, measuring the
    host again from the first interval is honest rather than a reset."""
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)
    daemon._reconcile_scheduler()
    daemon._scheduler_running.pop("j1")

    clock.advance(_backoff_max_seconds())
    daemon._reconcile_scheduler()
    assert not _backoff_is_empty(daemon), (
        "a record must outlive its host's jobs for at least the ceiling"
    )

    clock.advance(1.0)
    daemon._reconcile_scheduler()
    assert _backoff_is_empty(daemon)


# -- Part 4: an explicit refresh that meets the cooldown --------------------


def _deferred_reason() -> str:
    value = getattr(daemon_module, "SCHEDULER_REFRESH_HOST_DEFERRED_REASON", None)
    if value is None:
        raise AssertionError(
            "vq.daemon has no SCHEDULER_REFRESH_HOST_DEFERRED_REASON: an "
            "explicit refresh cannot name the cooldown as the reason it "
            "brought nothing back"
        )
    return str(value)


def _answer_one_refresh(vqd: Daemon, jobid: str) -> dict[str, object] | None:
    """Drive one refresh request through exactly one main-loop pass.

    The RPC thread only publishes a sequence number and waits; the
    acknowledgement is the main loop's. Doing the publish here keeps the test
    free of threads and of the RPC deadline, which is the thing under test.
    """
    with vqd._scheduler_refresh_condition:
        vqd._scheduler_refresh_requested += 1
        request_id = vqd._scheduler_refresh_requested
        vqd._scheduler_refresh_requests[request_id] = jobid
    vqd._reconcile_scheduler()
    with vqd._scheduler_refresh_condition:
        return vqd._scheduler_refresh_results.pop(request_id, None)


def test_a_refresh_against_a_deferred_host_names_the_cooldown(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """`vq show --refresh` on a job whose host is waiting out its cooldown used
    to answer "job_not_observed", which is true and useless: an operator
    chasing a stuck job is told nothing about the machine-imposed wait that is
    the actual cause."""
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)
    daemon._reconcile_scheduler()
    assert mock.polled == 1

    result = _answer_one_refresh(daemon, "j1")

    assert result is not None
    assert result["completed"] is False
    assert result["observed_at"] is None
    assert result["reason"] == _deferred_reason()
    assert mock.polled == 1, (
        "a refresh must report the cooldown, not reopen a host that is "
        "turning vq's connections away"
    )


def test_a_background_refresh_against_a_deferred_host_is_answered_not_parked(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """In production the same request was simply left pending, so it came back
    as `timeout` after the whole RPC budget -- for a wait vq itself chose and
    could have named immediately."""
    clock = _Clock()
    mock = _refusing_mock()
    _arm(daemon, mock, clock)
    daemon._background_scheduler_polling = True

    daemon._reconcile_scheduler()
    for flight in daemon._scheduler_poll_flights.values():
        assert flight.done.wait(30.0), "the observation must not hang"
    daemon._reconcile_scheduler()
    assert not _backoff_is_empty(daemon)

    result = _answer_one_refresh(daemon, "j1")

    assert result is not None, (
        "a request against a host that will not be contacted must not be left "
        "pending until the RPC deadline"
    )
    assert result["reason"] == _deferred_reason()


def test_a_refresh_against_a_host_that_is_not_waiting_is_unchanged(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The new reason is reported only for the deferral it names."""
    clock = _Clock()
    mock = MockDispatcher(poll_error=SchedulerError("qstat: command not found"))
    _arm(daemon, mock, clock)

    result = _answer_one_refresh(daemon, "j1")

    assert result is not None
    assert result["reason"] == "job_not_observed"

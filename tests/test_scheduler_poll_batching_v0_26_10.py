"""One host's poll must not ride on one unbounded remote command (#51).

Issue #51 reports scheduler polls that fail continuously and never recover, and
names two properties of the failing invocation as suspicious: it is very large,
and it is issued repeatedly against a host whose transport has just failed. The
reported root cause (a login node's sshd throttling the daemon's connections)
needs real-host evidence and is *not* established by anything here.

What these tests pin is narrower and provable from the source alone:

* a batch of handles larger than ``SCHEDULER_POLL_MAX_COMMAND_BYTES`` is
  split into several bounded invocations instead of one unbounded one, with the
  observation semantics of the single-command case preserved;
* an ordinary small batch is still exactly one command, byte-identical to
  before the split;
* the coarse live poll of *both* dialects gets the scheduler poll budget and
  the replay-safe retry budget, rather than only Torque's ``qstat``;
* a pass whose live poll failed does not then issue the detail poll, whose
  result that pass discards unread.

Site identifiers stay generic placeholders (host ``cluster``), matching
``tests/test_scheduler_dispatch.py``.
"""

from __future__ import annotations

import shlex
import subprocess

import pytest

from tests.test_daemon_scheduler import (
    MockDispatcher,
    _inject,
    _submit_scheduler,
    daemon,  # noqa: F401 -- pytest fixture
)
from tests.test_scheduler_dispatch import (
    FakeRunner,
    make_dispatcher,
    make_slurm_dispatcher,
)
from vq import scheduler_dispatch
from vq.config import HostConfig
from vq.daemon import Daemon
from vq.scheduler_dialect import SchedulerPhase, SlurmDialect, TorqueDialect
from vq.scheduler_dispatch import (
    SCHEDULER_ACCOUNTING_TIMEOUT_SECONDS,
    SCHEDULER_POLL_TIMEOUT_SECONDS,
    RemoteResult,
    SchedulerError,
    SchedulerHandle,
    SshRemoteRunner,
)
from vq.spec import JobSpec, JobState

# The split helper and its cap are reached through the module rather than
# imported by name, so this file still imports against a build that does not
# have them and each test then fails on its own behaviour, not on collection.


def _chunk_poll_ids(ids, build_command):  # type: ignore[no-untyped-def]
    return scheduler_dispatch._chunk_poll_ids(ids, build_command)


def _cap() -> int:
    return int(scheduler_dispatch.SCHEDULER_POLL_MAX_COMMAND_BYTES)

# Long enough that a few hundred of them cross the byte cap, and shaped like a
# Torque handle (`<numeric id>.<host>`) without naming a real host.
_TORQUE_IDS = [f"{20000 + n}.cluster.example" for n in range(600)]
# Slurm ids are short, so it takes a campaign-sized batch to cross the cap --
# which is exactly the scale #51 says to treat as ordinary. Sized for three
# chunks so "the chunks after a failing one" is a non-empty claim.
_SLURM_IDS = [str(900000 + n) for n in range(2600)]


def _handles(ids: list[str]) -> list[SchedulerHandle]:
    return [SchedulerHandle(job_id, f"/ws/{job_id}") for job_id in ids]


def _rendered(argv: list[str]) -> int:
    return len(shlex.join(argv).encode())


def _torque_table(ids: list[str]) -> str:
    header = (
        "Job id                    Name             User"
        "            Time Use S Queue\n"
        "------------------------- ---------------- ---------------"
        " -------- - -----\n"
    )
    rows = "".join(
        f"{job_id:<25} runjob           alice            00:10:00 R compute\n"
        for job_id in ids
    )
    return header + rows


# -- the split itself -------------------------------------------------------


def test_chunk_keeps_every_rendered_torque_poll_command_within_the_cap() -> None:
    dialect = TorqueDialect()
    chunks = _chunk_poll_ids(_TORQUE_IDS, dialect.poll_command)

    assert len(chunks) > 1, "fixture must be large enough to need splitting"
    assert [job_id for chunk in chunks for job_id in chunk] == _TORQUE_IDS
    for chunk in chunks:
        assert chunk
        assert _rendered(dialect.poll_command(chunk)) <= _cap()


def test_chunk_keeps_every_rendered_slurm_poll_command_within_the_cap() -> None:
    dialect = SlurmDialect()
    chunks = _chunk_poll_ids(_SLURM_IDS, dialect.poll_command)

    assert [job_id for chunk in chunks for job_id in chunk] == _SLURM_IDS
    for chunk in chunks:
        assert _rendered(dialect.poll_command(chunk)) <= _cap()


def test_chunk_never_drops_an_id_that_exceeds_the_cap_alone() -> None:
    dialect = TorqueDialect()
    huge = "x" * (_cap() * 2)

    chunks = _chunk_poll_ids([huge, "1.cluster"], dialect.poll_command)

    assert chunks == [[huge], ["1.cluster"]]


# -- the poll path ----------------------------------------------------------


def test_large_torque_poll_is_split_into_bounded_commands() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qstat":
            return RemoteResult(0, _torque_table(argv[1:]), "")
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_dispatcher(runner)

    phases = dispatcher.poll(_handles(_TORQUE_IDS))

    qstat_calls = [c.argv for c in runner.calls if c.argv and c.argv[0] == "qstat"]
    assert len(qstat_calls) > 1
    for argv in qstat_calls:
        assert _rendered(argv) <= _cap()
    # Review note 2 still holds per chunk: the plain table command, no -f.
    assert all("-f" not in argv for argv in qstat_calls)
    # Every requested id is polled exactly once, across all chunks.
    assert [job_id for argv in qstat_calls for job_id in argv[1:]] == _TORQUE_IDS
    assert phases == {job_id: SchedulerPhase.RUNNING for job_id in _TORQUE_IDS}


def test_large_slurm_poll_is_split_and_merges_phases_across_chunks() -> None:
    absent = _SLURM_IDS[-1]

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            asked = argv[argv.index("--jobs") + 1].split(",")
            rows = "".join(
                f"{job_id}|RUNNING|00:10:00|01:00:00|node1|None\n"
                for job_id in asked
                if job_id != absent
            )
            return RemoteResult(0, rows, "")
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_slurm_dispatcher(runner)

    phases = dispatcher.poll(_handles(_SLURM_IDS))

    squeue_calls = [c.argv for c in runner.calls if c.argv and c.argv[0] == "squeue"]
    assert len(squeue_calls) > 1
    for argv in squeue_calls:
        assert _rendered(argv) <= _cap()
    assert phases[absent] is SchedulerPhase.FINISHED
    assert all(
        phases[job_id] is SchedulerPhase.RUNNING
        for job_id in _SLURM_IDS
        if job_id != absent
    )


def test_a_failing_chunk_fails_the_whole_poll_and_stops_the_remaining_chunks() -> None:
    """A partial observation is never reported as a complete one, and a host
    that has just refused one command is not handed the rest of them."""
    seen: list[list[str]] = []

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            seen.append(argv)
            if len(seen) == 2:
                return RemoteResult(1, "", "slurm_load_jobs error: Unable to contact")
            asked = argv[argv.index("--jobs") + 1].split(",")
            return RemoteResult(
                0,
                "".join(
                    f"{job_id}|RUNNING|00:10:00|01:00:00|node1|None\n"
                    for job_id in asked
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_slurm_dispatcher(runner)

    with pytest.raises(SchedulerError):
        dispatcher.poll(_handles(_SLURM_IDS))

    assert len(seen) == 2, "chunks after the failing one must not be issued"


def test_small_batches_are_still_exactly_one_command() -> None:
    """The ordinary case must be byte-identical to the unsplit command."""
    ids = _TORQUE_IDS[:3]

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qstat":
            return RemoteResult(0, _torque_table(ids), "")
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_dispatcher(runner)

    dispatcher.poll(_handles(ids))

    assert [c.argv for c in runner.calls if c.argv[0] == "qstat"] == [["qstat", *ids]]


def test_large_detail_poll_is_split_into_bounded_commands() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            asked = argv[argv.index("-j") + 1].split(",")
            return RemoteResult(
                0,
                "".join(
                    f"{job_id}|RUNNING|0:0|00:10:00|01:00:00|node1\n"
                    for job_id in asked
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_slurm_dispatcher(runner)

    details = dispatcher.poll_detail(_handles(_SLURM_IDS))

    sacct_calls = [c.argv for c in runner.calls if c.argv and c.argv[0] == "sacct"]
    assert len(sacct_calls) > 1
    for argv in sacct_calls:
        assert _rendered(argv) <= _cap()
    assert sorted(details) == sorted(_SLURM_IDS)


def test_small_detail_batches_are_still_exactly_one_command() -> None:
    ids = _TORQUE_IDS[:3]
    runner = FakeRunner()
    dispatcher = make_dispatcher(runner)

    dispatcher.poll_detail(_handles(ids))

    assert [c.argv for c in runner.calls if c.argv[0] == "qstat"] == [
        ["qstat", "-f", *ids]
    ]


# -- transport policy parity ------------------------------------------------


@pytest.mark.parametrize("command", ["qstat", "squeue"])
def test_every_dialects_live_poll_gets_the_poll_budget_and_retries(
    monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    """`squeue` is the Slurm coarse poll and must carry `qstat`'s policy: the
    longer login-node budget, and retries for a replay-safe read-only call."""
    seen: dict[str, object] = {}

    def fake_run_remote_shell(
        *args: object, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(
            args=["ssh"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )

    SshRemoteRunner(HostConfig(ssh="cluster")).run([command, "123"], check=False)

    assert seen["timeout"] == SCHEDULER_POLL_TIMEOUT_SECONDS
    assert seen["retry_transient"] == 2


def test_accounting_poll_keeps_its_own_narrower_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The parity above must not quietly widen `sacct`'s bounded capture."""
    seen: dict[str, object] = {}

    def fake_run_remote_shell(
        *args: object, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(
            args=["ssh"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )

    SshRemoteRunner(HostConfig(ssh="cluster")).run(["sacct", "-j", "1"], check=False)

    assert seen["timeout"] == SCHEDULER_ACCOUNTING_TIMEOUT_SECONDS
    assert seen["retry_transient"] == 0


# -- one failing host, one batch of commands per pass -----------------------


def test_failed_live_poll_does_not_also_issue_the_detail_poll(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    """The detail result is discarded unread when the live poll failed, so
    issuing it can only add load to a host that has just refused a command."""
    mock = MockDispatcher(poll_error=SchedulerError("scheduler poll failed"))
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    assert mock.polled == 1
    assert mock.detail_polled == 0
    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.RUNNING
    assert on_disk.scheduler_state == "poll_failed"


def test_successful_live_poll_still_issues_the_detail_poll(
    daemon: Daemon,  # noqa: F811 -- pytest fixture
) -> None:
    mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    assert mock.polled == 1
    assert mock.detail_polled == 1

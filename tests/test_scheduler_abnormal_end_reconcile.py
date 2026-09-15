"""Regressions for #414: scheduler-killed jobs must never read as clean runs.

Two host_c jobs OOM-killed by SLURM (sacct ``State=OUT_OF_MEMORY``, ``ExitCode
0:125``) were recorded ``completed``/exit-0: the workspace exit-marker read 0
and the reap path recorded the marker rc without reconciling the terminal
label against scheduler accounting, so downstream consumers admitted OOM
partials as clean successes. These tests pin the fix at each layer:

* **dialect** — an ``OUT_OF_MEMORY`` / ``CANCELLED`` / ``TIMEOUT`` accounting
  state classifies as an abnormal (scheduler-attributed) termination;
  ``COMPLETED`` and a plain nonzero ``FAILED`` do not. Torque's ``qstat``
  terminal letter carries no such signal and never classifies.
* **dispatcher** — the classification is reachable from a parsed sacct row of
  the exact incident shape (``OUT_OF_MEMORY|0:125``), whose fail-closed rc is
  already nonzero.
* **daemon** — a job whose accounting row reports an abnormal termination
  resolves to a failed-class terminal state carrying the scheduler reason,
  even when the exit-marker claims rc=0. A fake sacct OUT_OF_MEMORY row must
  never yield ``completed`` (issue ask 3), with or without a marker.
* **wrapper** — the generated batch script must not write marker rc=0 when
  the payload was killed by a signal under a GNU time 1.7-style collector,
  which exits with ``WEXITSTATUS == 0`` for a signal-killed command and
  reports the kill only inside its ``-o`` output (issue ask 2).
"""

from __future__ import annotations

import json
import subprocess
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

import vq.scheduler_dispatch as scheduler_dispatch

# Shared scheduler-test infrastructure (idiomatic cross-test imports; see
# tests/__init__.py).
from tests.test_daemon_scheduler import MockDispatcher, _inject, _submit_scheduler
from tests.test_scheduler_dispatch import (
    FakeRunner,
    make_dispatcher,
    make_slurm_dispatcher,
)
from vq import events, paths
from vq.daemon import SCHEDULER_FINISHED_MARKER_GRACE_SECONDS, Daemon
from vq.scheduler_dialect import (
    QstatDetail,
    SchedulerPhase,
    SlurmDialect,
    TorqueDialect,
)
from vq.scheduler_dispatch import SchedulerDispatcher
from vq.spec import JobSpec, JobState


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    # Mirrors tests/test_daemon_scheduler.py's daemon fixture (importing a
    # fixture trips F811 on every test parameter that receives it).
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d

# --------------------------------------------------------------------------- #
# dialect: abnormal-termination classification
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw_state", "expected"),
    [
        ("OUT_OF_MEMORY", "OUT_OF_MEMORY"),
        ("OOM", "OUT_OF_MEMORY"),
        ("CANCELLED", "CANCELLED"),
        ("CANCELLED by 1234", "CANCELLED"),
        ("TIMEOUT", "TIMEOUT"),
        ("DEADLINE", "DEADLINE"),
        ("NODE_FAIL", "NODE_FAIL"),
        ("PREEMPTED", "PREEMPTED"),
        # COMPLETED is the one clean terminal; FAILED is an ordinary nonzero
        # payload exit whose rc flows through the exit-marker (retry policy
        # and FAILED classification stay rc-driven).
        ("COMPLETED", None),
        ("FAILED", None),
        # Non-terminal states never classify.
        ("RUNNING", None),
        ("PENDING", None),
        ("COMPLETING", None),
    ],
)
def test_slurm_abnormal_termination_classification(
    raw_state: str, expected: str | None
) -> None:
    assert SlurmDialect().abnormal_termination(raw_state) == expected


def test_torque_abnormal_termination_never_classifies() -> None:
    # qstat's terminal letter "C" carries no abnormality signal; Torque
    # truthfulness flows through exit_status / the exit-marker / walltime
    # evidence.
    dialect = TorqueDialect()
    for state in ("C", "E", "R", "Q"):
        assert dialect.abnormal_termination(state) is None


# --------------------------------------------------------------------------- #
# dispatcher: classification from a parsed sacct row (the incident shape)
# --------------------------------------------------------------------------- #

# The exact shape observed on host_c for vq job 486ea9f5d743 (issue #414):
# sacct -X --parsable2 --noheader
#   --format=JobID,State,ExitCode,Elapsed,Timelimit,NodeList
_SACCT_OOM_ROW = "39331377|OUT_OF_MEMORY|0:125|00:02:59|08:00:00|c0123\n"


def test_sacct_oom_row_parses_fail_closed_and_classifies_abnormal() -> None:
    dialect = SlurmDialect()
    detail = dialect.parse_qstat_detail(_SACCT_OOM_ROW)["39331377"]
    # Pinned pre-existing contract: ExitCode 0:125 is fail-closed to a
    # process-style nonzero rc (128 + 125).
    assert detail.exit_code == 253
    assert detail.raw_state == "OUT_OF_MEMORY"

    dispatcher = SchedulerDispatcher(
        dialect, FakeRunner(), scratch_root="/home/USER"
    )
    assert dispatcher.abnormal_termination_from_detail(detail) == "OUT_OF_MEMORY"
    assert (
        dispatcher.abnormal_termination_from_detail(
            QstatDetail(raw_state="COMPLETED", exit_code=0)
        )
        is None
    )


# --------------------------------------------------------------------------- #
# daemon: reap must reconcile the terminal label against accounting
# --------------------------------------------------------------------------- #


def _slurm_finished_mock(detail: QstatDetail, **kwargs: object) -> MockDispatcher:
    """A finished scheduler job on a sacct-bearing (Slurm-shaped) host."""
    return MockDispatcher(
        phase=SchedulerPhase.FINISHED,
        detail=detail,
        accounting_required_for_absent=True,
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    ("raw_state", "accounting_rc", "expected_state", "expected_scheduler_state"),
    [
        ("OUT_OF_MEMORY", 253, JobState.OOM_KILLED, "out_of_memory"),
        ("CANCELLED by 1234", 143, JobState.ABORTED_BY_QUEUE, "cancelled"),
        ("TIMEOUT", 1, JobState.TIME_EXCEEDED, "timeout"),
    ],
    ids=["oom", "cancelled", "timeout"],
)
def test_reconcile_abnormal_accounting_overrides_marker_zero(
    daemon: Daemon,
    raw_state: str,
    accounting_rc: int,
    expected_state: JobState,
    expected_scheduler_state: str,
) -> None:
    """Issue #414 ask 1: marker rc=0 + abnormal sacct state != completed.

    The marker is present and reads 0 (the observed incident shape); the
    accounting row says the scheduler ended the job. The reap must land a
    failed-class terminal state carrying the scheduler reason -- never
    COMPLETED.
    """
    detail = QstatDetail(
        raw_state=raw_state,
        exit_code=accounting_rc,
        # Elapsed stays under the limit so the duration heuristic cannot mask
        # the accounting-state reconciliation under test.
        walltime_used="00:02:59",
        walltime_limit="08:00:00",
    )
    mock = _slurm_finished_mock(detail, rc=0)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state != JobState.COMPLETED
    assert on_disk.state == expected_state
    # The marker rc stays recorded for forensics; the state names the truth.
    assert on_disk.exit_code == 0
    assert on_disk.failure_reason is not None
    normalized = raw_state.split()[0]
    assert f"scheduler accounting reports {normalized}" in on_disk.failure_reason
    # The frozen-at-"running" scheduler_state now records the scheduler verdict.
    assert on_disk.scheduler_state == expected_scheduler_state
    assert mock.fetched == ["555.cluster"]
    assert "j1" not in daemon._scheduler_running
    transition = [
        e
        for e in events.read_events(Path(spec.cwd))
        if e.get("kind") == "state_transition"
        and e.get("from") == JobState.RUNNING.value
        and e.get("to") == expected_state.value
    ][-1]
    evidence = transition["evidence"]
    assert evidence["scheduler_accounting_state"] == raw_state
    assert evidence["scheduler_abnormal_state"] == normalized
    assert evidence["scheduler_accounting_exit_code"] == accounting_rc
    assert evidence["exit_marker_rc"] == 0


def test_reconcile_fake_sacct_oom_row_never_yields_completed(
    daemon: Daemon,
) -> None:
    """Issue #414 ask 3, end to end from literal sacct text.

    The detail record is parsed from the exact incident row by the real
    SlurmDialect, then fed through the reap with a marker that reads 0.
    """
    detail = SlurmDialect().parse_qstat_detail(_SACCT_OOM_ROW)["39331377"]
    mock = _slurm_finished_mock(detail, rc=0)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state != JobState.COMPLETED
    assert on_disk.state == JobState.OOM_KILLED
    assert on_disk.failure_reason is not None
    assert "OUT_OF_MEMORY" in on_disk.failure_reason
    del spec


def test_reconcile_abnormal_accounting_with_missing_marker_names_the_reason(
    daemon: Daemon,
) -> None:
    """Missing marker + OOM accounting: failed-class with the scheduler reason.

    Pre-#414 this reaped FAILED from the bare accounting rc with no recorded
    scheduler reason; the OOM verdict must be named, not inferred.
    """
    detail = QstatDetail(
        raw_state="OUT_OF_MEMORY",
        exit_code=253,
        walltime_used="02:07:41",
        walltime_limit="08:00:00",
    )
    mock = _slurm_finished_mock(detail, rc=None, marker_rcs=[None, None])
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()
    assert JobSpec.read(daemon._spec_path("j1")).state == JobState.RUNNING
    sj = daemon._scheduler_running["j1"]
    sj.finished_without_marker_since = (
        time.monotonic() - SCHEDULER_FINISHED_MARKER_GRACE_SECONDS - 1.0
    )

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.OOM_KILLED
    assert on_disk.exit_code == 253
    assert on_disk.failure_reason is not None
    assert "scheduler accounting reports OUT_OF_MEMORY" in on_disk.failure_reason
    assert on_disk.scheduler_state == "out_of_memory"
    assert "j1" not in daemon._scheduler_running
    del spec


def test_reconcile_completed_accounting_with_marker_zero_stays_completed(
    daemon: Daemon,
) -> None:
    """Guard-rail: the reconcile must not over-reach on genuinely clean runs."""
    detail = QstatDetail(
        raw_state="COMPLETED",
        exit_code=0,
        walltime_used="00:10:00",
        walltime_limit="08:00:00",
    )
    mock = _slurm_finished_mock(detail, rc=0)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.COMPLETED
    assert on_disk.exit_code == 0
    assert on_disk.failure_reason is None
    assert "j1" not in daemon._scheduler_running
    del spec


def test_reconcile_marker_failure_rc_still_gets_scheduler_reason(
    daemon: Daemon,
) -> None:
    """A truthful nonzero marker under an abnormal state also names the kill.

    With the wrapper fix the marker reads 128+sig for a killed payload; the
    state must still say OOM_KILLED (scheduler verdict), not generic FAILED.
    """
    detail = QstatDetail(
        raw_state="OUT_OF_MEMORY",
        exit_code=253,
        walltime_used="00:02:59",
        walltime_limit="08:00:00",
    )
    mock = _slurm_finished_mock(detail, rc=137)
    _inject(daemon, mock)
    spec = _submit_scheduler(daemon, "j1")
    daemon._start_scheduler_job(spec)

    daemon._reconcile_scheduler()

    on_disk = JobSpec.read(daemon._spec_path("j1"))
    assert on_disk.state == JobState.OOM_KILLED
    assert on_disk.exit_code == 137
    assert on_disk.failure_reason is not None
    assert "scheduler accounting reports OUT_OF_MEMORY" in on_disk.failure_reason
    del spec


# --------------------------------------------------------------------------- #
# wrapper: the marker must not read 0 for a signal-killed payload (ask 2)
# --------------------------------------------------------------------------- #


def _install_gnu_time_17_style(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A GNU time 1.7-style collector: signal deaths exit 0.

    time 1.7 (the /usr/bin/time still shipped on older cluster nodes) exits
    with ``WEXITSTATUS(status)`` even when the command died from a signal --
    which is 0 -- and reports the kill only as a ``Command terminated by
    signal N`` line inside its ``-o`` output, ahead of the format line. This
    stand-in reproduces exactly that contract.
    """
    executable = tmp_path / "fake-gnu-time-17"
    executable.write_text(
        textwrap.dedent(
            f"""\
            #!/bin/bash
            if [ "${{1:-}}" = "--version" ]; then
                printf '%s\\n' 'time (GNU Time) 1.7'
                exit 0
            fi
            __vq_fake_output=
            while [ "$#" -gt 0 ]; do
                case "$1" in
                    -f) shift 2 ;;
                    -o) __vq_fake_output="$2"; shift 2 ;;
                    --) shift; break ;;
                    *) exit 125 ;;
                esac
            done
            "$@"
            __vq_fake_rc=$?
            if [ "$__vq_fake_rc" -gt 128 ]; then
                {{
                    printf 'Command terminated by signal %s\\n' \\
                        "$((__vq_fake_rc - 128))"
                    printf '%s\\n' \\
                        '{scheduler_dispatch._GNU_TIME_SENTINEL} 1.25 2.50 0.75 4096'
                }} > "$__vq_fake_output"
                exit 0
            fi
            printf '%s\\n' \\
                '{scheduler_dispatch._GNU_TIME_SENTINEL} 1.25 2.50 0.75 4096' \\
                > "$__vq_fake_output"
            exit "$__vq_fake_rc"
            """
        ),
        encoding="utf-8",
    )
    executable.chmod(0o755)
    monkeypatch.setattr(scheduler_dispatch, "GNU_TIME_COMMAND", str(executable))


@pytest.mark.parametrize(
    ("dialect_flavor", "node_scratch"),
    [
        ("slurm", False),
        ("slurm", True),
        # The wrapper body is dialect-independent: the Torque/host_f script
        # carries the identical rc chain and the identical corrector.
        ("torque", False),
    ],
    ids=["slurm-shared-ws", "slurm-node-scratch", "torque-shared-ws"],
)
def test_rendered_script_marker_is_not_zero_for_signal_killed_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dialect_flavor: str,
    node_scratch: bool,
) -> None:
    """Issue #414 ask 2: a SIGKILLed payload must not produce marker rc=0.

    The payload is killed with SIGKILL (the OOM killer's signal) under a GNU
    time 1.7-style collector whose own exit status hides the kill. The
    wrapper must recover the signal from the collector's raw output and
    record 128+9, in both execution-location branches.
    """
    _install_gnu_time_17_style(tmp_path, monkeypatch)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    if dialect_flavor == "torque":
        dispatcher = make_dispatcher(FakeRunner())
    else:
        dispatcher = make_slurm_dispatcher(
            FakeRunner(),
            node_scratch_dir=str(tmp_path / "scratch") if node_scratch else None,
        )
    script = dispatcher.build_job_script(
        job_id="oom-shaped",
        command=["/bin/sh", "-c", "printf started; kill -KILL $$"],
        remote_workspace=str(workspace),
        cpus=1,
    )

    completed = subprocess.run(
        ["/bin/bash"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert (workspace / "_vq" / "exit-code").read_text() == "137\n"
    assert completed.returncode == 137, completed.stderr
    assert (workspace / "stdout.log").read_text() == "started"
    receipt = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert receipt["command_status"] == "failed"
    assert receipt["command_exit_code"] == 137


def test_rendered_script_clean_exit_is_unchanged_by_signal_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard-rail: a clean payload still records rc=0 under the 1.7 collector."""
    _install_gnu_time_17_style(tmp_path, monkeypatch)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    dispatcher = make_slurm_dispatcher(FakeRunner())
    script = dispatcher.build_job_script(
        job_id="clean",
        command=["/bin/sh", "-c", "printf payload"],
        remote_workspace=str(workspace),
        cpus=1,
    )

    completed = subprocess.run(
        ["/bin/bash"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert completed.returncode == 0, completed.stderr
    assert (workspace / "_vq" / "exit-code").read_text() == "0\n"
    receipt = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert receipt["command_status"] == "succeeded"
    assert receipt["command_exit_code"] == 0

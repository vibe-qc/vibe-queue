"""Unit tests for the scheduler dialect layer (vq/scheduler_dialect.py).

Pure-function coverage of :class:`TorqueDialect` and :class:`SlurmDialect`:
directive mapping, job-script rendering, command construction, and scheduler
output parsing, plus the :class:`ResourceRequest` validation contract and the
helper functions. No cluster, SSH, or subprocess is touched (design doc §12):
the dialect is the string layer, so every path is exercisable on any OS. The
``SchedulerDispatcher`` supplies the transport and is tested separately
against a mock scheduler.
"""

from __future__ import annotations

import dataclasses
import textwrap
from collections.abc import Callable

import pytest

from vq.scheduler_dialect import (
    DialectError,
    ResourceRequest,
    SchedulerDialect,
    SchedulerPhase,
    SlurmDialect,
    TorqueDialect,
    dialect_for,
    enforce_scheduler_wall_time_limit,
    format_walltime,
    parse_submit_extra,
    sanitize_job_name,
    sanitize_slurm_job_name,
    scheduler_width_warning,
)


@pytest.fixture
def torque() -> TorqueDialect:
    return TorqueDialect()


@pytest.fixture
def slurm() -> SlurmDialect:
    return SlurmDialect()


# --------------------------------------------------------------------------- #
# ResourceRequest validation
# --------------------------------------------------------------------------- #


def test_resource_request_accepts_valid_values() -> None:
    req = ResourceRequest(
        cpus=4,
        scheduler_tasks=2,
        mem_mb=2048,
        wall_time_seconds=3600,
        array_size=8,
    )
    assert req.cpus == 4
    assert req.scheduler_tasks == 2
    assert req.mem_mb == 2048
    assert req.extra_directives == ()


@pytest.mark.parametrize(
    "make",
    [
        lambda: ResourceRequest(cpus=0),
        lambda: ResourceRequest(cpus=-1),
        lambda: ResourceRequest(cpus=1, scheduler_tasks=0),
        lambda: ResourceRequest(cpus=1, mem_mb=0),
        lambda: ResourceRequest(cpus=1, wall_time_seconds=0),
        lambda: ResourceRequest(cpus=1, array_size=0),
    ],
)
def test_resource_request_rejects_out_of_range(make: Callable[[], ResourceRequest]) -> None:
    with pytest.raises(ValueError):
        make()


def test_resource_request_is_frozen() -> None:
    req = ResourceRequest(cpus=1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        req.cpus = 2  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# format_walltime
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "00:00:00"),
        (59, "00:00:59"),
        (60, "00:01:00"),
        (3661, "01:01:01"),
        (86400, "24:00:00"),
        (360000, "100:00:00"),  # >2-digit hours are valid Torque walltime
    ],
)
def test_format_walltime(seconds: int, expected: str) -> None:
    assert format_walltime(seconds) == expected


def test_format_walltime_rejects_negative() -> None:
    with pytest.raises(ValueError):
        format_walltime(-1)


# --------------------------------------------------------------------------- #
# sanitize_job_name
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Good_Name1", "Good_Name1"),  # already valid -> unchanged
        ("4ff48938", "j4ff48938"),  # vq ids can start with a digit -> 'j' prefix
        ("_foo", "j_foo"),  # underscore is not alphabetic -> 'j' prefix
        ("my job!@#", "my_job___"),  # non-[A-Za-z0-9_] -> '_'
        ("", "j"),  # empty -> 'j'
        ("x" * 20, "x" * 15),  # truncated to 15
    ],
)
def test_sanitize_job_name(raw: str, expected: str) -> None:
    assert sanitize_job_name(raw) == expected


def test_sanitize_job_name_always_within_torque_limits() -> None:
    name = sanitize_job_name("9" * 40)
    assert len(name) <= 15
    assert name[0].isalpha()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("release-paper-P01.tail_3200", "release-paper-P01.tail_3200"),
        ("4ff48938", "4ff48938"),
        ("my job!@#", "my_job___"),
        ("", "j"),
        ("x" * 60, "x" * 50),
    ],
)
def test_sanitize_slurm_job_name_preserves_vq_label_surface(
    raw: str, expected: str
) -> None:
    assert sanitize_slurm_job_name(raw) == expected


# --------------------------------------------------------------------------- #
# resource_directives
# --------------------------------------------------------------------------- #


def test_resource_directives_full_request(torque: TorqueDialect) -> None:
    req = ResourceRequest(
        cpus=4,
        mem_mb=8192,
        wall_time_seconds=3661,
        queue="compute",
        account="proj1",
        job_name="h2o_scf",
        stdout_path="/home/USER/.vibeqc-cluster/out.log",
        stderr_path="/home/USER/.vibeqc-cluster/err.log",
        extra_directives=("-l naccelerators=0",),
    )
    assert torque.resource_directives(req) == [
        "-N h2o_scf",
        "-l nodes=1:ppn=4",
        "-l mem=8192mb",
        "-l walltime=01:01:01",
        "-q compute",
        "-A proj1",
        "-o /home/USER/.vibeqc-cluster/out.log",
        "-e /home/USER/.vibeqc-cluster/err.log",
        "-l naccelerators=0",
    ]


def test_resource_directives_minimal_is_just_cpus(torque: TorqueDialect) -> None:
    assert torque.resource_directives(ResourceRequest(cpus=1)) == ["-l nodes=1:ppn=1"]


def test_resource_directives_array(torque: TorqueDialect) -> None:
    req = ResourceRequest(cpus=2, array_size=4)
    assert torque.resource_directives(req) == ["-l nodes=1:ppn=2", "-t 0-3"]


def test_resource_directives_sanitizes_job_name(torque: TorqueDialect) -> None:
    req = ResourceRequest(cpus=1, job_name="4bad name")
    assert torque.resource_directives(req)[0] == "-N j4bad_name"


def test_slurm_resource_directives_full_request(slurm: SlurmDialect) -> None:
    req = ResourceRequest(
        cpus=8,
        scheduler_tasks=2,
        mem_mb=48000,
        wall_time_seconds=7200,
        queue="intelsr_devel",
        account="<group-account>",
        job_name="host_c-build",
        stdout_path="/workspace/job/.slurm.out",
        stderr_path="/workspace/job/.slurm.err",
        array_size=3,
        extra_directives=("--gres=tmp",),
    )

    assert slurm.resource_directives(req) == [
        "--job-name=host_c-build",
        "--ntasks=2",
        "--cpus-per-task=8",
        "--mem=48000M",
        "--time=02:00:00",
        "--partition=intelsr_devel",
        "--account=<group-account>",
        "--output=/workspace/job/.slurm.out",
        "--error=/workspace/job/.slurm.err",
        "--array=0-2",
        "--gres=tmp",
    ]


def test_slurm_resource_directives_preserves_descriptive_job_name(
    slurm: SlurmDialect,
) -> None:
    req = ResourceRequest(
        cpus=1,
        job_name="release-paper-P01.tail_3200-" + "x" * 40,
    )

    assert slurm.resource_directives(req)[0] == (
        "--job-name=release-paper-P01.tail_3200-xxxxxxxxxxxxxxxxxxxxxx"
    )


# --------------------------------------------------------------------------- #
# render_job_script
# --------------------------------------------------------------------------- #


def test_render_job_script_assembles_shebang_directives_body(torque: TorqueDialect) -> None:
    script = torque.render_job_script(
        ["-N j", "-l nodes=1:ppn=2"],
        ["cd /work", "./run.sh"],
    )
    assert script == "#!/bin/bash\n#PBS -N j\n#PBS -l nodes=1:ppn=2\ncd /work\n./run.sh\n"


def test_render_job_script_honors_custom_shell(torque: TorqueDialect) -> None:
    script = torque.render_job_script([], ["true"], shell="/bin/zsh")
    assert script.startswith("#!/bin/zsh\n")


def test_render_job_script_rejects_non_ascii(torque: TorqueDialect) -> None:
    # The em-dash gotcha: Torque qsub rejects non-ASCII scripts with a cryptic
    # error, so we surface it at render time (design doc / brief).
    with pytest.raises(DialectError, match="ASCII"):
        torque.render_job_script([], ["echo problem—here"])


def test_slurm_render_job_script_uses_sbatch(slurm: SlurmDialect) -> None:
    script = slurm.render_job_script(
        ["--job-name=j", "--cpus-per-task=2"],
        ["cd /work", "./run.sh"],
    )
    assert script == (
        "#!/bin/bash\n"
        "#SBATCH --job-name=j\n"
        "#SBATCH --cpus-per-task=2\n"
        "cd /work\n"
        "./run.sh\n"
    )


# --------------------------------------------------------------------------- #
# submit_command / parse_submit_id
# --------------------------------------------------------------------------- #


def test_submit_command(torque: TorqueDialect) -> None:
    assert torque.submit_command("/tmp/job.pbs") == ["qsub", "/tmp/job.pbs"]
    assert torque.submit_command("/tmp/job.pbs", extra_args=["-A", "proj1"]) == [
        "qsub",
        "-A",
        "proj1",
        "/tmp/job.pbs",
    ]


def test_slurm_submit_command(slurm: SlurmDialect) -> None:
    assert slurm.submit_command("/tmp/job.sh") == ["sbatch", "/tmp/job.sh"]


@pytest.mark.parametrize(
    ("stdout", "expected"),
    [
        ("12345.host_f\n", "12345.host_f"),
        ("  12345.host_f  \n", "12345.host_f"),
        ("12345[].host_f\n", "12345[].host_f"),  # array master id
        ("12345[7].host_f", "12345[7].host_f"),  # array sub-job id
        ("12345", "12345"),  # bare sequence number
        ("qsub: waiting for queue to open\n12345.host_f\n", "12345.host_f"),  # banner first
    ],
)
def test_parse_submit_id_ok(torque: TorqueDialect, stdout: str, expected: str) -> None:
    assert torque.parse_submit_id(stdout) == expected


@pytest.mark.parametrize("stdout", ["", "   \n  \n", "not-a-job-id", "host_f.12345"])
def test_parse_submit_id_rejects_garbage(torque: TorqueDialect, stdout: str) -> None:
    with pytest.raises(DialectError):
        torque.parse_submit_id(stdout)


def test_slurm_parse_submit_id(slurm: SlurmDialect) -> None:
    assert slurm.parse_submit_id("Submitted batch job 123456\n") == "123456"


def test_slurm_parse_submit_id_rejects_garbage(slurm: SlurmDialect) -> None:
    with pytest.raises(DialectError, match="sbatch"):
        slurm.parse_submit_id("sbatch: error: bad account")


# --------------------------------------------------------------------------- #
# poll_command / parse_poll / phase_for_state
# --------------------------------------------------------------------------- #


def test_poll_command(torque: TorqueDialect) -> None:
    assert torque.poll_command(["12345.host_f", "12346.host_f"]) == [
        "qstat",
        "12345.host_f",
        "12346.host_f",
    ]


def test_slurm_poll_command(slurm: SlurmDialect) -> None:
    assert slurm.poll_command(["123", "124"]) == [
        "squeue",
        "--noheader",
        "--format=%i|%T|%M|%l|%N|%r",
        "--jobs",
        "123,124",
    ]


_QSTAT_TABLE = textwrap.dedent("""\
    Job id                    Name             User            Time Use S Queue
    ------------------------- ---------------- --------------- -------- - -----
    12345.host_f                runjob           alice            00:10:00 R compute
    12346.host_f                queuedjob        alice                   0 Q compute
    12347.host_f                donejob          alice            01:00:00 C compute
    12348.host_f                exitingjob       alice           00:30:00 E debug
    12349.host_f                heldjob          bob                    0 H gpu
    12350[].host_f              arrayjob         alice                   0 Q compute
""")


def test_parse_poll_maps_all_rows(torque: TorqueDialect) -> None:
    assert torque.parse_poll(_QSTAT_TABLE) == {
        "12345.host_f": SchedulerPhase.RUNNING,
        "12346.host_f": SchedulerPhase.PENDING,
        "12347.host_f": SchedulerPhase.FINISHED,
        "12348.host_f": SchedulerPhase.RUNNING,  # E (exiting) still holds the node
        "12349.host_f": SchedulerPhase.PENDING,  # H (held)
        "12350[].host_f": SchedulerPhase.PENDING,  # array master id parsed
    }


def test_parse_poll_skips_header_and_empty(torque: TorqueDialect) -> None:
    assert torque.parse_poll("") == {}
    header_only = textwrap.dedent("""\
        Job id   Name   User   Time Use S Queue
        -------- ------ ------ -------- - -----
    """)
    assert torque.parse_poll(header_only) == {}


def test_parse_poll_raises_on_unknown_state(torque: TorqueDialect) -> None:
    row = "12345.host_f   job   alice   0 X compute\n"
    with pytest.raises(DialectError, match="unknown Torque job state"):
        torque.parse_poll(row)


@pytest.mark.parametrize(
    ("state", "phase"),
    [
        ("Q", SchedulerPhase.PENDING),
        ("W", SchedulerPhase.PENDING),
        ("H", SchedulerPhase.PENDING),
        ("T", SchedulerPhase.PENDING),
        ("R", SchedulerPhase.RUNNING),
        ("E", SchedulerPhase.RUNNING),
        ("S", SchedulerPhase.RUNNING),
        ("C", SchedulerPhase.FINISHED),
    ],
)
def test_phase_for_state_known(torque: TorqueDialect, state: str, phase: SchedulerPhase) -> None:
    assert torque.phase_for_state(state) == phase


def test_phase_for_state_unknown(torque: TorqueDialect) -> None:
    with pytest.raises(DialectError):
        torque.phase_for_state("Z")


_SQUEUE_TABLE = textwrap.dedent("""\
    123|RUNNING|00:01|01:00:00|node001|None
    124|PENDING|00:00|01:00:00|(Priority)|Priority
    125|COMPLETING|00:59|01:00:00|node002|None
    126|COMPLETED|01:00|01:00:00|node003|None
""")


def test_slurm_parse_poll_maps_rows(slurm: SlurmDialect) -> None:
    assert slurm.parse_poll(_SQUEUE_TABLE) == {
        "123": SchedulerPhase.RUNNING,
        "124": SchedulerPhase.PENDING,
        "125": SchedulerPhase.RUNNING,
        "126": SchedulerPhase.FINISHED,
    }


@pytest.mark.parametrize(
    "stdout",
    [
        "123\n",
        "|RUNNING|00:01|01:00:00|node001|None\n",
        "123||00:01|01:00:00|node001|None\n",
        "123|RUNNING|00:01|01:00:00|node001\n",
    ],
)
def test_slurm_parse_poll_rejects_malformed_nonempty_rows(
    slurm: SlurmDialect, stdout: str
) -> None:
    with pytest.raises(DialectError, match="malformed squeue row"):
        slurm.parse_poll(stdout)


def test_slurm_parse_poll_rejects_duplicate_rows(slurm: SlurmDialect) -> None:
    row = "123|RUNNING|00:01|01:00:00|node001|None\n"
    with pytest.raises(DialectError, match="duplicate squeue row"):
        slurm.parse_poll(row + row)


@pytest.mark.parametrize(
    ("state", "phase"),
    [
        ("PENDING", SchedulerPhase.PENDING),
        ("PD", SchedulerPhase.PENDING),
        ("RUNNING", SchedulerPhase.RUNNING),
        ("R", SchedulerPhase.RUNNING),
        ("COMPLETING", SchedulerPhase.RUNNING),
        ("COMPLETED", SchedulerPhase.FINISHED),
        ("FAILED", SchedulerPhase.FINISHED),
        ("TIMEOUT", SchedulerPhase.FINISHED),
        ("OUT_OF_MEMORY", SchedulerPhase.FINISHED),
        ("CANCELLED by 1234", SchedulerPhase.FINISHED),
        ("CANCELLED+", SchedulerPhase.FINISHED),
        ("ca", SchedulerPhase.FINISHED),
    ],
)
def test_slurm_phase_for_state_known(
    slurm: SlurmDialect, state: str, phase: SchedulerPhase
) -> None:
    assert slurm.phase_for_state(state) == phase


@pytest.mark.parametrize("state", ["REQUEUED", "RQ"])
def test_slurm_requeued_state_remains_live(
    slurm: SlurmDialect, state: str
) -> None:
    assert slurm.phase_for_state(state) is SchedulerPhase.PENDING


def test_slurm_phase_for_state_unknown(slurm: SlurmDialect) -> None:
    with pytest.raises(DialectError, match="unknown SLURM job state"):
        slurm.phase_for_state("MYSTERY")


# --------------------------------------------------------------------------- #
# detail_command / parse_exit_status
# --------------------------------------------------------------------------- #


def test_detail_command(torque: TorqueDialect) -> None:
    assert torque.detail_command("12345.host_f") == ["qstat", "-f", "12345.host_f"]


def test_slurm_detail_command(slurm: SlurmDialect) -> None:
    assert slurm.detail_command("123") == [
        "sacct",
        "-X",
        "--array",
        "-j",
        "123",
        "--parsable2",
        "--noheader",
        "--format=JobID,State,ExitCode,Elapsed,Timelimit,NodeList",
    ]


_QSTAT_F_DONE = textwrap.dedent("""\
    Job Id: 12347.host_f
        Job_Name = donejob
        Job_Owner = alice@host_f
        job_state = C
        queue = compute
        exit_status = 0
        resources_used.walltime = 01:00:00
""")

_QSTAT_F_RUNNING = textwrap.dedent("""\
    Job Id: 12345.host_f
        Job_Name = runjob
        job_state = R
        queue = compute
""")


def test_parse_exit_status_zero(torque: TorqueDialect) -> None:
    assert torque.parse_exit_status(_QSTAT_F_DONE) == 0


def test_parse_exit_status_nonzero(torque: TorqueDialect) -> None:
    assert torque.parse_exit_status("    exit_status = 1\n") == 1


def test_parse_exit_status_signal_negative(torque: TorqueDialect) -> None:
    # A job killed by signal 11 reports exit_status = -11 in Torque accounting.
    assert torque.parse_exit_status("    exit_status = -11\n") == -11


def test_parse_exit_status_absent_is_none(torque: TorqueDialect) -> None:
    assert torque.parse_exit_status(_QSTAT_F_RUNNING) is None


def test_parse_exit_status_case_insensitive(torque: TorqueDialect) -> None:
    assert torque.parse_exit_status("    Exit_Status = 3\n") == 3


_SACCT_DONE = textwrap.dedent("""\
    123|COMPLETED|0:0|00:01:00|01:00:00|node001
    123.batch|COMPLETED|0:0|00:01:00||node001
""")


def test_slurm_parse_exit_status(slurm: SlurmDialect) -> None:
    assert slurm.parse_exit_status(_SACCT_DONE) == 0
    assert slurm.parse_exit_status("123|FAILED|7:0|00:01|01:00|node001\n") == 7
    assert slurm.parse_exit_status(
        "123|CANCELLED by 1000|15:0|00:01|01:00|node001\n"
    ) == 15


@pytest.mark.parametrize(
    ("state", "native_exit", "expected"),
    [
        ("COMPLETED", "0:9", 137),
        ("FAILED", "0:0", 1),
        ("CANCELLED", "0:9", 137),
        ("TIMEOUT", "0:0", 1),
    ],
)
def test_slurm_parse_exit_status_never_treats_failed_terminal_as_success(
    slurm: SlurmDialect,
    state: str,
    native_exit: str,
    expected: int,
) -> None:
    assert (
        slurm.parse_exit_status(
            f"123|{state}|{native_exit}|00:01|01:00|node001\n"
        )
        == expected
    )


def test_slurm_parse_exit_status_ignores_nonterminal_rows(slurm: SlurmDialect) -> None:
    assert slurm.parse_exit_status("123|RUNNING|0:0|00:01|01:00|node001\n") is None


def test_slurm_parse_exit_status_aggregates_array_element_failures(
    slurm: SlurmDialect,
) -> None:
    sacct = textwrap.dedent("""\
        123_0|COMPLETED|0:0|00:01|01:00|node001
        123_1|FAILED|9:0|00:01|01:00|node002
        123_1.batch|FAILED|9:0|00:01||node002
    """)

    assert slurm.parse_exit_status(sacct) == 9


@pytest.mark.parametrize(
    "stdout",
    [
        (
            "123|COMPLETED|0:0|00:01|01:00|node001\n"
            "123_0|FAILED|unknown|00:01|01:00|node002\n"
        ),
        (
            "123_0|FAILED|unknown|00:01|01:00|node002\n"
            "123|COMPLETED|0:0|00:01|01:00|node001\n"
        ),
    ],
)
def test_slurm_parse_exit_status_unknown_failure_never_becomes_success(
    slurm: SlurmDialect, stdout: str
) -> None:
    assert slurm.parse_exit_status(stdout) is None


@pytest.mark.parametrize(
    "stdout",
    [
        (
            "123_0|FAILED|9:0|00:01|01:00|node001\n"
            "123_1|FAILED|unknown|00:01|01:00|node002\n"
        ),
        (
            "123_1|FAILED|unknown|00:01|01:00|node002\n"
            "123_0|FAILED|9:0|00:01|01:00|node001\n"
        ),
    ],
)
def test_slurm_parse_exit_status_any_unknown_terminal_row_fails_closed(
    slurm: SlurmDialect, stdout: str
) -> None:
    assert slurm.parse_exit_status(stdout) is None


def test_slurm_parse_exit_status_absent(slurm: SlurmDialect) -> None:
    assert slurm.parse_exit_status("") is None


@pytest.mark.parametrize(
    "stdout",
    [
        "123|COMPLETED\n",
        "|COMPLETED|0:0|00:01|01:00|node001\n",
        "123||0:0|00:01|01:00|node001\n",
        "123|COMPLETED|0:0|00:01|01:00|node001|extra\n",
    ],
)
def test_slurm_parse_exit_status_rejects_malformed_nonempty_rows(
    slurm: SlurmDialect, stdout: str
) -> None:
    with pytest.raises(DialectError, match="malformed sacct row"):
        slurm.parse_exit_status(stdout)


def test_slurm_parse_exit_status_rejects_duplicate_native_rows(
    slurm: SlurmDialect,
) -> None:
    row = "123|COMPLETED|0:0|00:01|01:00|node001\n"
    with pytest.raises(DialectError, match="duplicate sacct row"):
        slurm.parse_exit_status(row + row)


# --------------------------------------------------------------------------- #
# poll_detail_command / parse_qstat_detail (§18)
# --------------------------------------------------------------------------- #


def test_poll_detail_command(torque: TorqueDialect) -> None:
    assert torque.poll_detail_command(["1.host_f", "2.host_f"]) == [
        "qstat",
        "-f",
        "1.host_f",
        "2.host_f",
    ]


def test_slurm_poll_detail_command(slurm: SlurmDialect) -> None:
    assert slurm.poll_detail_command(["123", "124"]) == [
        "sacct",
        "-X",
        "--array",
        "-j",
        "123,124",
        "--parsable2",
        "--noheader",
        "--format=JobID,State,ExitCode,Elapsed,Timelimit,NodeList",
    ]


_QSTAT_F_MULTI = textwrap.dedent("""\
    Job Id: 12345.host_f
        Job_Name = runjob
        job_state = R
        queue = compute
        exec_host = atok07/0-19
        resources_used.walltime = 02:00:00
        Resource_List.walltime = 08:00:00
    Job Id: 12346.host_f
        Job_Name = queuedjob
        job_state = Q
        queue = compute
        Resource_List.walltime = 08:00:00
""")


def test_parse_qstat_detail_running_job(torque: TorqueDialect) -> None:
    detail = torque.parse_qstat_detail(_QSTAT_F_MULTI)
    run = detail["12345.host_f"]
    assert run.raw_state == "R"
    assert run.exec_host == "atok07/0-19"
    assert run.walltime_used == "02:00:00"
    assert run.walltime_limit == "08:00:00"


def test_parse_qstat_detail_queued_job_has_no_exec_host(torque: TorqueDialect) -> None:
    detail = torque.parse_qstat_detail(_QSTAT_F_MULTI)
    queued = detail["12346.host_f"]
    assert queued.raw_state == "Q"
    assert queued.exec_host is None
    assert queued.walltime_used is None
    assert queued.walltime_limit == "08:00:00"


def test_parse_qstat_detail_empty() -> None:
    assert TorqueDialect().parse_qstat_detail("") == {}


# A queued job's `comment` is Torque's own answer to "why has this not
# started". vq already fetched it with every detail poll and dropped it, so a
# request that could never be scheduled looked exactly like one that was next
# in line (vibe-qc#148).
# Torque folds a long value onto a tab-continued line, so the reason is
# written with an explicit "\t" rather than a literal tab in this source.
_QSTAT_F_UNSCHEDULABLE = (
    "Job Id: 12345.host_f\n"
    "    Job_Name = runjob\n"
    "    job_state = Q\n"
    "    queue = compute\n"
    "    Resource_List.nodes = 1:ppn=128\n"
    "    Resource_List.walltime = 240:00:00\n"
    "    comment = Not Running: Not enough of the right type of nodes are availab\n"
    "\tle to run the job\n"
    "Job Id: 12346.host_f\n"
    "    Job_Name = running\n"
    "    job_state = R\n"
    "    exec_host = node02/0-127\n"
    "    comment = Job started on Fri Aug 15 at 21:57\n"
    "    resources_used.walltime = 02:00:00\n"
    "    Resource_List.walltime = 240:00:00\n"
)


def test_parse_qstat_detail_reports_why_a_queued_job_has_not_started(
    torque: TorqueDialect,
) -> None:
    detail = torque.parse_qstat_detail(_QSTAT_F_UNSCHEDULABLE)
    assert detail["12345.host_f"].queued_reason == (
        "Not Running: Not enough of the right type of nodes are available "
        "to run the job"
    )


def test_parse_qstat_detail_does_not_call_a_running_comment_a_queued_reason(
    torque: TorqueDialect,
) -> None:
    # Torque reuses `comment` for run-time annotations. Carrying one as a
    # queued reason would have a running job explain why it is waiting.
    assert torque.parse_qstat_detail(_QSTAT_F_UNSCHEDULABLE)[
        "12346.host_f"
    ].queued_reason is None


def test_parse_qstat_detail_rejoins_a_wrapped_exec_host(
    torque: TorqueDialect,
) -> None:
    # Torque wraps any long value, not just comments; a multi-node exec_host
    # was previously truncated at the fold.
    detail = torque.parse_qstat_detail(
        "Job Id: 7.host_f\n"
        "    job_state = R\n"
        "    exec_host = node01/0-63+node02/0-63+node04/0-63+node05/0-6\n"
        "\t3\n"
    )
    assert detail["7.host_f"].exec_host == (
        "node01/0-63+node02/0-63+node04/0-63+node05/0-63"
    )


def test_torque_parse_poll_reasons_is_empty(torque: TorqueDialect) -> None:
    # The coarse qstat table has no reason column; Torque answers on the
    # detail poll instead.
    assert torque.parse_poll_reasons(_QSTAT_TABLE) == {}


def test_slurm_parse_poll_reasons_maps_pending_jobs(slurm: SlurmDialect) -> None:
    assert slurm.parse_poll_reasons(_SQUEUE_TABLE) == {"124": "Priority"}


def test_slurm_parse_poll_reasons_drops_the_running_placeholder(
    slurm: SlurmDialect,
) -> None:
    # squeue prints the literal "None" for a job that is already running.
    assert slurm.parse_poll_reasons(
        "123|RUNNING|00:01|01:00:00|node001|None\n"
    ) == {}


def test_slurm_parse_poll_tolerates_a_delimiter_inside_the_reason(
    slurm: SlurmDialect,
) -> None:
    # The reason is free text and trails the row, so a stray delimiter must
    # land in the reason rather than failing the whole host's poll.
    row = "125|PENDING|00:00|01:00:00||ReqNodeNotAvail, Reserved|maintenance\n"
    assert slurm.parse_poll(row) == {"125": SchedulerPhase.PENDING}
    assert slurm.parse_poll_reasons(row) == {
        "125": "ReqNodeNotAvail, Reserved|maintenance"
    }


def test_slurm_parse_qstat_detail(slurm: SlurmDialect) -> None:
    detail = slurm.parse_qstat_detail(_SACCT_DONE)
    run = detail["123"]
    assert run.raw_state == "COMPLETED"
    assert run.exit_code == 0
    assert run.exec_host == "node001"
    assert run.walltime_used == "00:01:00"
    assert run.walltime_limit == "01:00:00"


def test_slurm_parse_qstat_detail_preserves_day_walltimes(
    slurm: SlurmDialect,
) -> None:
    detail = slurm.parse_qstat_detail(
        "123|RUNNING|0:0|1-02:03:04|2-00:00:00|node001\n"
    )
    run = detail["123"]
    assert run.raw_state == "RUNNING"
    assert run.exit_code is None
    assert run.exec_host == "node001"
    assert run.walltime_used == "1-02:03:04"
    assert run.walltime_limit == "2-00:00:00"


def test_slurm_parse_qstat_detail_adds_array_master_from_elements(
    slurm: SlurmDialect,
) -> None:
    sacct = textwrap.dedent("""\
        123_0|PENDING|0:0|00:00|01:00|(Priority)
        123_1|RUNNING|0:0|00:03|01:00|node001
        123_1.batch|RUNNING|0:0|00:03||node001
    """)

    detail = slurm.parse_qstat_detail(sacct)

    assert detail["123_0"].raw_state == "PENDING"
    assert detail["123_1"].raw_state == "RUNNING"
    master = detail["123"]
    assert master.raw_state == "RUNNING"
    assert master.exec_host == "node001"
    assert master.walltime_used == "00:03"
    assert master.walltime_limit == "01:00"


def test_slurm_parse_qstat_detail_maps_grouped_array_row_to_master(
    slurm: SlurmDialect,
) -> None:
    detail = slurm.parse_qstat_detail(
        "123_[0-9]|PENDING|0:0|00:00|01:00|(Priority)\n"
    )

    assert detail["123"].raw_state == "PENDING"


def test_slurm_parse_qstat_detail_captures_terminal_failure_exit_code(
    slurm: SlurmDialect,
) -> None:
    detail = slurm.parse_qstat_detail(
        "123|FAILED|55:0|00:11|01:00|node001\n"
    )

    assert detail["123"].raw_state == "FAILED"
    assert detail["123"].exit_code == 55


@pytest.mark.parametrize(
    ("state", "native_exit", "expected"),
    [
        ("COMPLETED", "0:9", 137),
        ("FAILED", "0:0", 1),
        ("CANCELLED", "0:9", 137),
        ("TIMEOUT", "0:0", 1),
    ],
)
def test_slurm_parse_qstat_detail_never_treats_failed_terminal_as_success(
    slurm: SlurmDialect,
    state: str,
    native_exit: str,
    expected: int,
) -> None:
    detail = slurm.parse_qstat_detail(
        f"123|{state}|{native_exit}|00:11|01:00|node001\n"
    )

    assert detail["123"].exit_code == expected


@pytest.mark.parametrize(
    "malformed",
    [
        "123_0|FAILED\n",
        "|FAILED|1:0|00:10|01:00|node002\n",
        "123_0||1:0|00:10|01:00|node002\n",
        "123_0|FAILED|1:0|00:10|01:00|node002|extra\n",
    ],
)
def test_slurm_parse_qstat_detail_rejects_malformed_nonempty_row(
    slurm: SlurmDialect, malformed: str
) -> None:
    with pytest.raises(DialectError, match="malformed sacct row"):
        slurm.parse_qstat_detail(
            "123|COMPLETED|0:0|00:11|01:00|node001\n"
            + malformed
        )


def test_slurm_parse_qstat_detail_rejects_duplicate_native_rows(
    slurm: SlurmDialect,
) -> None:
    row = "123|COMPLETED|0:0|00:11|01:00|node001\n"
    with pytest.raises(DialectError, match="duplicate sacct row"):
        slurm.parse_qstat_detail(row + row)


def test_slurm_parse_qstat_detail_array_failure_wins_in_any_row_order(
    slurm: SlurmDialect,
) -> None:
    detail = slurm.parse_qstat_detail(
        "123_1|FAILED|0:9|00:10|01:00|node002\n"
        "123|COMPLETED|0:0|00:11|01:00|node001\n"
        "123_0|COMPLETED|0:0|00:11|01:00|node001\n"
    )

    assert detail["123"].raw_state == "FAILED"
    assert detail["123"].exit_code == 137


@pytest.mark.parametrize(
    "stdout",
    [
        (
            "123|COMPLETED|0:0|00:11|01:00|node001\n"
            "123_1|FAILED|unknown|00:10|01:00|node002\n"
        ),
        (
            "123_1|FAILED|unknown|00:10|01:00|node002\n"
            "123|COMPLETED|0:0|00:11|01:00|node001\n"
        ),
    ],
)
def test_slurm_array_unknown_failure_wins_over_completed_master(
    slurm: SlurmDialect, stdout: str
) -> None:
    detail = slurm.parse_qstat_detail(stdout)

    assert detail["123"].raw_state == "FAILED"
    assert detail["123"].exit_code is None


@pytest.mark.parametrize(
    "stdout",
    [
        (
            "123|COMPLETED|0:0|00:11|01:00|node001\n"
            "123_1|COMPLETED|unknown|00:10|01:00|node002\n"
        ),
        (
            "123_1|COMPLETED|unknown|00:10|01:00|node002\n"
            "123|COMPLETED|0:0|00:11|01:00|node001\n"
        ),
    ],
)
def test_slurm_array_unknown_completed_exit_is_order_independent(
    slurm: SlurmDialect, stdout: str
) -> None:
    detail = slurm.parse_qstat_detail(stdout)

    assert detail["123"].raw_state == "COMPLETED"
    assert detail["123"].exit_code is None


@pytest.mark.parametrize(
    "stdout",
    [
        (
            "123_0|FAILED|55:0|00:11|01:00|node001\n"
            "123_1|FAILED|unknown|00:10|01:00|node002\n"
        ),
        (
            "123_1|FAILED|unknown|00:10|01:00|node002\n"
            "123_0|FAILED|55:0|00:11|01:00|node001\n"
        ),
    ],
)
def test_slurm_array_any_unknown_terminal_exit_fails_closed(
    slurm: SlurmDialect, stdout: str
) -> None:
    detail = slurm.parse_qstat_detail(stdout)

    assert detail["123"].raw_state == "FAILED"
    assert detail["123"].exit_code is None


def test_slurm_parse_qstat_detail_array_master_keeps_nonzero_exit_code(
    slurm: SlurmDialect,
) -> None:
    detail = slurm.parse_qstat_detail(
        "123|COMPLETED|0:0|00:11|01:00|node001\n"
        "123_0|COMPLETED|0:0|00:11|01:00|node001\n"
        "123_1|FAILED|55:0|00:10|01:00|node002\n"
    )

    assert detail["123"].raw_state == "FAILED"
    assert detail["123"].exit_code == 55


# --------------------------------------------------------------------------- #
# cancel_command / protocol conformance
# --------------------------------------------------------------------------- #


def test_cancel_command(torque: TorqueDialect) -> None:
    assert torque.cancel_command("12345.host_f") == ["qdel", "12345.host_f"]


def test_slurm_control_commands(slurm: SlurmDialect) -> None:
    assert slurm.cancel_command("123") == ["scancel", "123"]
    assert slurm.hold_command("123") == ["scontrol", "hold", "123"]
    assert slurm.release_command("123") == ["scontrol", "release", "123"]


# --------------------------------------------------------------------------- #
# parse_submit_extra (review note 1 — single render path for queue/account)
# --------------------------------------------------------------------------- #


def test_parse_submit_extra_queue_and_account() -> None:
    assert parse_submit_extra(["-q", "compute", "-A", "proj1"]) == (
        "compute",
        "proj1",
        (),
    )


def test_parse_submit_extra_slurm_long_partition_and_account() -> None:
    assert parse_submit_extra(
        ["--account", "<group-account>", "--partition", "intelsr_devel"]
    ) == ("intelsr_devel", "<group-account>", ())


def test_parse_submit_extra_slurm_short_partition_and_account() -> None:
    assert parse_submit_extra(["-A", "proj", "-p", "debug"]) == ("debug", "proj", ())


def test_parse_submit_extra_slurm_equals_forms() -> None:
    assert parse_submit_extra(
        ["--account=proj", "--partition=debug", "--ntasks=8"]
    ) == ("debug", "proj", ("--ntasks=8",))


def test_parse_submit_extra_empty() -> None:
    assert parse_submit_extra([]) == (None, None, ())


def test_parse_submit_extra_extra_directive_pairs_flag_with_value() -> None:
    # A non -q/-A flag absorbs one following non-flag token as its directive body.
    queue, account, extra = parse_submit_extra(
        ["-q", "compute", "-l", "naccelerators=1", "-A", "proj1"]
    )
    assert (queue, account) == ("compute", "proj1")
    assert extra == ("-l naccelerators=1",)


def test_parse_submit_extra_bare_flag_passthrough() -> None:
    # A bare flag (no value following) passes through as its own directive body.
    assert parse_submit_extra(["-X"]) == (None, None, ("-X",))


def test_parse_submit_extra_multiple_extra_directives() -> None:
    _, _, extra = parse_submit_extra(["-l", "mem=1gb", "-W", "group_list=x"])
    assert extra == ("-l mem=1gb", "-W group_list=x")


@pytest.mark.parametrize("tokens", [["-q"], ["-A"], ["-q", "compute", "-A"]])
def test_parse_submit_extra_dangling_flag_raises(tokens: list[str]) -> None:
    with pytest.raises(DialectError, match="missing its value"):
        parse_submit_extra(tokens)


# --------------------------------------------------------------------------- #
# dialect_for (registry)
# --------------------------------------------------------------------------- #


def test_dialect_for_torque() -> None:
    d = dialect_for("torque")
    assert isinstance(d, TorqueDialect)
    assert d.name == "torque"


def test_dialect_for_slurm() -> None:
    d = dialect_for("slurm")
    assert isinstance(d, SlurmDialect)
    assert d.name == "slurm"


@pytest.mark.parametrize("name", ["sge", "pbspro", "nonsense"])
def test_dialect_for_unimplemented_raises(name: str) -> None:
    with pytest.raises(DialectError, match="unsupported scheduler_dialect"):
        dialect_for(name)


def test_torque_satisfies_scheduler_dialect_protocol() -> None:
    # Structural conformance: mypy verifies the assignment, and exercising the
    # surface through the protocol-typed name proves the runtime shape matches.
    dialect: SchedulerDialect = TorqueDialect()
    assert dialect.name == "torque"
    req = ResourceRequest(cpus=2, queue="compute")
    directives = dialect.resource_directives(req)
    script = dialect.render_job_script(directives, ["true"])
    assert script.startswith("#!/bin/bash\n")
    assert dialect.submit_command("/tmp/j.pbs")[0] == "qsub"
    assert dialect.poll_command(["1.host_f"])[0] == "qstat"
    assert dialect.detail_command("1.host_f") == ["qstat", "-f", "1.host_f"]
    assert dialect.cancel_command("1.host_f") == ["qdel", "1.host_f"]


def test_slurm_satisfies_scheduler_dialect_protocol() -> None:
    dialect: SchedulerDialect = SlurmDialect()
    assert dialect.name == "slurm"
    req = ResourceRequest(cpus=2, queue="debug")
    directives = dialect.resource_directives(req)
    script = dialect.render_job_script(directives, ["true"])
    assert script.startswith("#!/bin/bash\n#SBATCH")
    assert dialect.submit_command("/tmp/j.sh")[0] == "sbatch"
    assert dialect.poll_command(["1"])[0] == "squeue"
    assert dialect.detail_command("1")[0] == "sacct"
    assert dialect.cancel_command("1") == ["scancel", "1"]


# --- lane width: warn, never refuse (vibe-qc#148 closure criterion 3) --------


def test_width_warning_names_the_lane_and_both_numbers() -> None:
    message = scheduler_width_warning(
        128, 64, scheduler_host="host_f", partition="compute"
    )
    assert message is not None
    assert "'host_f'" in message and "'compute'" in message
    assert "at most 64" in message and "asks for 128" in message
    # The point of the warning: the scheduler will take it anyway.
    assert "may never start" in message


@pytest.mark.parametrize(
    ("requested", "maximum"),
    [
        (64, 64),  # exactly at the limit is fine
        (32, 64),  # under it
        (128, None),  # undeclared: None means unknown, never unlimited
        (None, 64),  # no width asked for
    ],
)
def test_width_warning_stays_quiet(
    requested: int | None, maximum: int | None
) -> None:
    assert scheduler_width_warning(requested, maximum) is None


def test_width_over_the_limit_warns_rather_than_raising() -> None:
    # The deliberate asymmetry with enforce_scheduler_wall_time_limit: a
    # declared width is a capacity observation and goes stale as nodes come
    # back, so it must not refuse work the cluster can now run.
    assert scheduler_width_warning(999, 1) is not None
    with pytest.raises(DialectError):
        enforce_scheduler_wall_time_limit(999, 1)

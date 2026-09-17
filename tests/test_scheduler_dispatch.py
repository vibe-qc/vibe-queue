"""Unit tests for the scheduler dispatcher (vq/scheduler_dispatch.py).

The dispatcher is the SSH-driven submit/poll/cancel orchestration of the v1.0
cluster backend (design doc §13 increment 2). These tests exercise the full
submit → poll → exit-code → cancel cycle against a **fake** :class:`RemoteRunner`
that records every argv and returns canned scheduler output — no SSH, no
subprocess, no cluster (design doc §12). Site identifiers stay generic
placeholders (queue ``compute``, account ``proj1``, host ``cluster``); the real
host_f values live only in the maintainer's local provisioning notes.

The three review notes flagged on the dialect are pinned here as explicit
behavioural assertions:
  1. queue/account render as ``#PBS`` directives only, never duplicated as
     ``qsub`` argv;
  2. the poll command is always the plain ``qstat`` ``parse_poll`` expects;
  3. array per-sub-job rc comes from a per-index exit-marker, never the
     first-match ``qstat -f`` aggregate.
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import sys
import tarfile
import textwrap
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import vq.scheduler_dispatch as scheduler_dispatch
from vq import transport
from vq.config import HostConfig, SchedulerProgramHooks
from vq.scheduler_dialect import (
    DialectError,
    QstatDetail,
    SchedulerPhase,
    SlurmDialect,
    TorqueDialect,
)
from vq.scheduler_dispatch import (
    MISSING_MARKER_DIAGNOSTIC_MAX_CHARS,
    SCHEDULER_ARCHIVE_TIMEOUT_SECONDS,
    SCHEDULER_POLL_TIMEOUT_SECONDS,
    RemoteResult,
    SchedulerDispatcher,
    SchedulerError,
    SchedulerFileChunk,
    SchedulerHandle,
    SchedulerSubmitOutcomeUnknown,
    SshRemoteRunner,
    scheduler_dispatcher_for,
)
from vq.spec import JobState

Responder = Callable[[list[str], str | None], RemoteResult | None]


@dataclass
class _Call:
    argv: list[str]
    stdin: str | None


@dataclass
class FakeRunner:
    """Records every remote call; replies via an optional per-argv responder.

    A responder returns ``None`` to fall through to the default rc=0/empty
    reply, so a test only has to special-case the commands it cares about.
    """

    responder: Responder | None = None
    calls: list[_Call] = field(default_factory=list)
    uploads: list[tuple[Path, str]] = field(default_factory=list)
    downloads: list[tuple[str, Path]] = field(default_factory=list)
    # Files the fake "remote workspace" contains; download_file tars these so the
    # dispatcher's real untar is exercised end to end.
    result_files: dict[str, str] = field(default_factory=dict)
    # Symlink members to embed in the tarball, name -> link target. An absolute
    # target reproduces a killed job's stale ``third_party/*/install`` build-tree
    # link, which ``filter="data"`` would reject.
    result_symlinks: dict[str, str] = field(default_factory=dict)
    fail_download: bool = False
    # Write a non-tar payload so the dispatcher's extraction step fails.
    corrupt_download: bool = False

    def run(
        self,
        argv: list[str] | tuple[str, ...],
        *,
        stdin_data: str | None = None,
        check: bool = False,
    ) -> RemoteResult:
        argv = list(argv)
        self.calls.append(_Call(argv, stdin_data))
        reply = self.responder(argv, stdin_data) if self.responder else None
        if reply is None:
            reply = RemoteResult(0, "", "")
        if check and reply.returncode != 0:
            raise SchedulerError(f"fake remote failed: {argv}")
        return reply

    def upload_tree(self, local_dir: Path, remote_dir: str) -> None:
        self.uploads.append((Path(local_dir), remote_dir))

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        self.uploads.append((Path(local_path), remote_path))

    def download_file(self, remote_path: str, local_path: Path) -> None:
        # Simulate scp-ing down a result tarball by building one from
        # result_files, so the dispatcher's untar produces real local files.
        self.downloads.append((remote_path, Path(local_path)))
        if self.fail_download:
            raise RuntimeError("simulated scp download failure")
        if self.corrupt_download:
            Path(local_path).write_bytes(b"this is not a tar archive\n")
            return
        import io  # noqa: PLC0415

        with tarfile.open(local_path, "w") as tf:
            for name, content in self.result_files.items():
                data = content.encode()
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            for name, target in self.result_symlinks.items():
                link = tarfile.TarInfo(name)
                link.type = tarfile.SYMTYPE
                link.linkname = target
                tf.addfile(link)

    # -- assertion helpers --------------------------------------------------

    def argvs(self) -> list[list[str]]:
        return [c.argv for c in self.calls]

    def first(self, head: str) -> _Call:
        for c in self.calls:
            if c.argv and c.argv[0] == head:
                return c
        raise AssertionError(f"no call starting with {head!r}; got {self.argvs()}")

    def none_match(self, predicate: Callable[[list[str]], bool]) -> bool:
        return not any(predicate(c.argv) for c in self.calls)


SCRATCH = "/home/USER"


def make_dispatcher(
    runner: FakeRunner,
    *,
    submit_extra: tuple[str, ...] = (),
    scheduler_prologue: tuple[str, ...] = (),
    scheduler_epilogue: tuple[str, ...] = (),
    scheduler_program_hooks: dict[str, SchedulerProgramHooks] | None = None,
    mem_directive: str = "request",
) -> SchedulerDispatcher:
    return SchedulerDispatcher(
        TorqueDialect(),
        runner,
        scratch_root=SCRATCH,
        submit_extra=submit_extra,
        scheduler_prologue=scheduler_prologue,
        scheduler_epilogue=scheduler_epilogue,
        scheduler_program_hooks=scheduler_program_hooks,
        mem_directive=mem_directive,
    )


def make_slurm_dispatcher(
    runner: FakeRunner,
    *,
    submit_extra: tuple[str, ...] = (),
    node_scratch_dir: str | None = None,
    scheduler_program_hooks: dict[str, SchedulerProgramHooks] | None = None,
) -> SchedulerDispatcher:
    return SchedulerDispatcher(
        SlurmDialect(),
        runner,
        scratch_root=SCRATCH,
        submit_extra=submit_extra,
        node_scratch_dir=node_scratch_dir,
        scheduler_program_hooks=scheduler_program_hooks,
    )


def _install_fake_gnu_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    patch_default: bool = True,
) -> Path:
    """Install a deterministic GNU-time stand-in for rendered-script tests."""
    executable = tmp_path / "fake-gnu-time"
    executable.write_text(
        textwrap.dedent(
            f"""\
            #!/bin/bash
            if [ "${{1:-}}" = "--version" ]; then
                printf '%s\\n' 'time (GNU Time) 1.9'
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
            printf '%s\\n' \
                '{scheduler_dispatch._GNU_TIME_SENTINEL} 1.25 2.50 0.75 4096' \
                > "$__vq_fake_output"
            exit "$__vq_fake_rc"
            """
        ),
        encoding="utf-8",
    )
    executable.chmod(0o755)
    if patch_default:
        monkeypatch.setattr(scheduler_dispatch, "GNU_TIME_COMMAND", str(executable))
    return executable


# --------------------------------------------------------------------------- #
# remote_workspace / paths
# --------------------------------------------------------------------------- #


def test_remote_workspace_path() -> None:
    d = make_dispatcher(FakeRunner())
    assert d.remote_workspace("abc123") == f"{SCRATCH}/.vibeqc-cluster/jobs/abc123"


def test_scratch_root_trailing_slash_normalised() -> None:
    d = SchedulerDispatcher(
        TorqueDialect(), FakeRunner(), scratch_root="/home/USER/", submit_extra=()
    )
    assert d.remote_workspace("j") == "/home/USER/.vibeqc-cluster/jobs/j"


# --------------------------------------------------------------------------- #
# build_job_script
# --------------------------------------------------------------------------- #


def test_build_job_script_has_directives_and_exit_wrap() -> None:
    d = make_dispatcher(FakeRunner(), submit_extra=("-q", "compute", "-A", "proj1"))
    ws = d.remote_workspace("job1")
    script = d.build_job_script(
        job_id="job1",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=4,
        mem_mb=2048,
        wall_time_seconds=3600,
    )
    assert script.startswith("#!/bin/bash\n")
    # Resource directives mapped by the dialect.
    assert "#PBS -N job1" in script
    assert "#PBS -l nodes=1:ppn=4" in script
    assert "#PBS -l mem=2048mb" in script
    assert "#PBS -l walltime=01:00:00" in script
    # Review note 1: queue/account appear as directives (single render path).
    assert "#PBS -q compute" in script
    assert "#PBS -A proj1" in script
    # #PBS -o/-e go to a throwaway job-level spool (Torque only writes them at
    # job end); the live logs are the redirected command output instead (§18).
    assert f"#PBS -o {ws}/.pbs-spool.out" in script
    assert f"#PBS -e {ws}/.pbs-spool.err" in script
    # Live logs: the command's stdout/stderr are redirected to NFS files that a
    # mid-run tail can read.
    assert f"__vq_stdout={ws}/stdout.log" in script
    assert '/venv/bin/python run.py > "$__vq_stdout" 2> "$__vq_stderr"' in script
    assert f"__vq_resource_usage={ws}/_vq/resource-usage.json" in script
    assert "__vq_time=/usr/bin/time" in script
    assert '"$__vq_time" --version' in script
    assert '"$__vq_time" -f' in script
    # Exit-wrap body: cd, run, capture rc, write the marker, exit with rc.
    assert f"cd {ws}" in script
    assert "__vq_rc=$?" in script
    assert f"{ws}/_vq/exit-code" in script
    assert "trap '__vq_term 143' TERM" in script
    assert "trap '__vq_term 130' INT" in script
    assert "trap '__vq_term 129' HUP" in script
    assert '__vq_write_marker "$__vq_rc"' in script
    assert 'exit "$__vq_rc"' in script


def test_build_job_script_mem_directive_omit_drops_mem_only() -> None:
    # BUG 118: Torque moms apply -l mem= as a hard per-process RLIMIT_DATA /
    # RLIMIT_RSS on the compute node (kernel >= 4.7 counts mmap against
    # RLIMIT_DATA), so a tightly-sized request bad_allocs the payload while
    # the node has free RAM. mem_directive="omit" must drop -l mem= and
    # nothing else.
    d = make_dispatcher(FakeRunner(), mem_directive="omit")
    ws = d.remote_workspace("job1")
    script = d.build_job_script(
        job_id="job1",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=4,
        mem_mb=2048,
        wall_time_seconds=3600,
        env={"VQ_MEM_MB": "2048"},
    )
    assert "-l mem=" not in script
    # The rest of the resource request is unchanged.
    assert "#PBS -l nodes=1:ppn=4" in script
    assert "#PBS -l walltime=01:00:00" in script
    # The job env still carries the vq-side memory accounting.
    assert "export VQ_MEM_MB=2048" in script


def test_dispatcher_factory_carries_mem_directive_from_host_config() -> None:
    host_cfg = HostConfig(
        ssh="host_f",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/home/USER",
        scheduler_driver="driver",
        scheduler_mem_directive="omit",
    )
    d = scheduler_dispatcher_for(host_cfg)
    assert d.mem_directive == "omit"


def test_dispatcher_factory_carries_gnu_time_command_from_host_config() -> None:
    host_cfg = HostConfig(
        ssh="cluster",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/home/USER",
        scheduler_driver="driver",
        scheduler_gnu_time_command="/home/USER/.local/bin/gnu-time",
    )
    d = scheduler_dispatcher_for(host_cfg)
    assert d.gnu_time_command == "/home/USER/.local/bin/gnu-time"

    script = d.build_job_script(
        job_id="custom-time",
        command=["true"],
        remote_workspace=d.remote_workspace("custom-time"),
        cpus=1,
    )
    assert "__vq_time=/home/USER/.local/bin/gnu-time" in script
    assert "__vq_time=/usr/bin/time" not in script


def test_custom_gnu_time_command_executes_rendered_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    custom_time = _install_fake_gnu_time(
        tmp_path,
        monkeypatch,
        patch_default=False,
    )
    monkeypatch.setattr(
        scheduler_dispatch,
        "GNU_TIME_COMMAND",
        str(tmp_path / "missing-default-time"),
    )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        FakeRunner(),
        scratch_root=str(tmp_path),
        gnu_time_command=str(custom_time),
    )
    script = dispatcher.build_job_script(
        job_id="custom-time",
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
    assert (workspace / "stdout.log").read_text() == "payload"
    usage = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert usage["status"] == "ok"
    assert usage["command_exit_code"] == 0
def test_dispatcher_factory_carries_scheduler_wall_time_limit() -> None:
    host_cfg = HostConfig(
        ssh="cluster",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scratch_root="/workspace/USER",
        scheduler_driver="driver",
        scheduler_max_wall_time_seconds=28_800,
    )

    dispatcher = scheduler_dispatcher_for(host_cfg)

    assert dispatcher.max_wall_time_seconds == 28_800


def test_build_job_script_mem_directive_default_keeps_mem() -> None:
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("job1")
    script = d.build_job_script(
        job_id="job1",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=4,
        mem_mb=2048,
    )
    assert "#PBS -l mem=2048mb" in script


def test_build_job_script_installs_signal_traps_before_run() -> None:
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("j")
    script = d.build_job_script(
        job_id="j",
        command=["python", "run.py"],
        remote_workspace=ws,
        cpus=1,
    )

    marker_setup = script.index('if [ -n "${PBS_ARRAYID:-}" ]')
    trap = script.index("trap '__vq_term 143' TERM")
    run = script.index('python run.py > "$__vq_stdout"')
    marker_write = script.rindex('__vq_write_marker "$__vq_rc"')

    assert marker_setup < trap < run < marker_write


def test_build_job_script_array_marker_is_per_index() -> None:
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("arr")
    script = d.build_job_script(
        job_id="arr",
        command=["true"],
        remote_workspace=ws,
        cpus=1,
        array_size=4,
    )
    assert "#PBS -t 0-3" in script
    # Review note 3: the marker is keyed by $PBS_ARRAYID for arrays.
    assert "PBS_ARRAYID" in script
    assert f"{ws}/_vq/exit-code" in script


@pytest.mark.parametrize(
    ("dispatcher_factory", "native_array_directive"),
    [
        (make_dispatcher, "#PBS -t"),
        (make_slurm_dispatcher, "#SBATCH --array"),
    ],
    ids=["torque", "slurm"],
)
def test_vq_array_metadata_keeps_single_job_artifact_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dispatcher_factory: Callable[[FakeRunner], SchedulerDispatcher],
    native_array_directive: str,
) -> None:
    """Vq array metadata does not turn an independent spec into a native array."""
    _install_fake_gnu_time(tmp_path, monkeypatch)
    workspace = tmp_path / "remote"
    workspace.mkdir()
    dispatcher = dispatcher_factory(FakeRunner())
    script = dispatcher.build_job_script(
        job_id="array-element",
        command=["/bin/sh", "-c", "printf payload"],
        remote_workspace=str(workspace),
        cpus=1,
        env={
            "VQ_ARRAY_INDEX": "2",
            "VQ_ARRAY_TOTAL": "5",
            "VQ_ARRAY_GROUP_ID": "arraygrp",
        },
    )

    assert native_array_directive not in script
    assert "export VQ_ARRAY_INDEX=2" in script

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
    usage = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert usage["schema"] == scheduler_dispatch.RESOURCE_USAGE_SCHEMA
    assert usage["command_exit_code"] == 0
    assert not (workspace / "_vq" / "exit-code.2").exists()
    assert (workspace / "stdout.log").read_text() == "payload"
    assert (workspace / "stderr.log").read_text() == ""
    assert not (workspace / "stdout.log.2").exists()
    assert not (workspace / "stderr.log.2").exists()


def test_slurm_build_job_script_has_sbatch_directives_once() -> None:
    d = make_slurm_dispatcher(
        FakeRunner(),
        submit_extra=(
            "--account",
            "<group-account>",
            "--partition",
            "intelsr_devel",
        ),
    )
    ws = d.remote_workspace("host_c-job")
    script = d.build_job_script(
        job_id="host_c-job",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=8,
        scheduler_tasks=2,
        mem_mb=48000,
        wall_time_seconds=7200,
    )

    assert script.startswith("#!/bin/bash\n")
    assert "#SBATCH --job-name=host_c-job" in script
    assert "#SBATCH --ntasks=2" in script
    assert "#SBATCH --cpus-per-task=8" in script
    assert "#SBATCH --mem=48000M" in script
    assert "#SBATCH --time=02:00:00" in script
    assert script.count("#SBATCH --account=<group-account>") == 1
    assert script.count("#SBATCH --partition=intelsr_devel") == 1
    assert f"#SBATCH --output={ws}/.pbs-spool.out" in script
    assert f"#SBATCH --error={ws}/.pbs-spool.err" in script
    assert "#PBS" not in script
    assert '/venv/bin/python run.py > "$__vq_stdout" 2> "$__vq_stderr"' in script
    assert f"__vq_resource_usage={ws}/_vq/resource-usage.json" in script
    assert "__vq_time=/usr/bin/time" in script


@pytest.mark.parametrize(
    "dialect",
    [TorqueDialect(), SlurmDialect()],
    ids=["pbs", "slurm"],
)
@pytest.mark.parametrize(
    ("command_body", "expected_rc", "command_status", "expected_stdout", "expected_stderr"),
    [
        ("printf payload; printf warning >&2", 0, "succeeded", "payload", "warning"),
        (
            "printf payload; printf warning >&2; exit 17",
            17,
            "failed",
            "payload",
            "warning",
        ),
        ("kill -TERM $$", 143, "failed", "", None),
    ],
    ids=["success", "failure", "signal"],
)
def test_scheduler_resource_receipt_preserves_process_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dialect: TorqueDialect | SlurmDialect,
    command_body: str,
    expected_rc: int,
    command_status: str,
    expected_stdout: str,
    expected_stderr: str | None,
) -> None:
    _install_fake_gnu_time(tmp_path, monkeypatch)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    dispatcher = SchedulerDispatcher(
        dialect,
        FakeRunner(),
        scratch_root=str(tmp_path),
    )
    script = dispatcher.build_job_script(
        job_id="measured",
        command=["/bin/sh", "-c", command_body],
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

    assert completed.returncode == expected_rc, completed.stderr
    assert (workspace / "_vq" / "exit-code").read_text() == f"{expected_rc}\n"
    assert (workspace / "stdout.log").read_text() == expected_stdout
    if expected_stderr is not None:
        assert (workspace / "stderr.log").read_text() == expected_stderr
    receipt = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert receipt == {
        "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
        "status": "ok",
        "collector": "gnu-time",
        "scope": "effective-command",
        "command_status": command_status,
        "command_exit_code": expected_rc,
        "wall_seconds": 1.25,
        "user_cpu_seconds": 2.5,
        "system_cpu_seconds": 0.75,
        "active_cpu_seconds": 3.25,
        "peak_rss_kb": 4096,
        "peak_rss_mb": 4.0,
    }
    assert not list((workspace / "_vq").glob("resource-usage.json.*"))


def test_scheduler_resource_receipt_fails_closed_without_gnu_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing-gnu-time"
    monkeypatch.setattr(scheduler_dispatch, "GNU_TIME_COMMAND", str(missing))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    side_effect = workspace / "command-ran"
    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        FakeRunner(),
        scratch_root=str(tmp_path),
    )
    script = dispatcher.build_job_script(
        job_id="unmeasured",
        command=["/bin/sh", "-c", f"touch {shlex.quote(str(side_effect))}"],
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

    assert completed.returncode == scheduler_dispatch.RESOURCE_COLLECTOR_FAILURE_EXIT_CODE
    assert not side_effect.exists()
    assert (workspace / "_vq" / "exit-code").read_text() == "125\n"
    receipt = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert receipt["schema"] == scheduler_dispatch.RESOURCE_USAGE_SCHEMA
    assert receipt["status"] == "error"
    assert receipt["error"] == "gnu_time_missing"
    assert receipt["command_status"] == "not_run"
    assert receipt["command_exit_code"] is None
    assert receipt["wall_seconds"] is None
    assert receipt["active_cpu_seconds"] is None
    assert receipt["peak_rss_kb"] is None
    assert "command not started" in (workspace / "stderr.log").read_text()


def test_slurm_build_job_script_array_uses_slurm_index_env() -> None:
    d = make_slurm_dispatcher(FakeRunner())
    ws = d.remote_workspace("arr")
    script = d.build_job_script(
        job_id="arr",
        command=["true"],
        remote_workspace=ws,
        cpus=1,
        array_size=4,
    )

    assert "#SBATCH --array=0-3" in script
    assert "SLURM_ARRAY_TASK_ID" in script
    assert "PBS_ARRAYID" not in script


def test_slurm_build_job_script_node_scratch_uses_configured_tmp() -> None:
    d = make_slurm_dispatcher(FakeRunner(), node_scratch_dir="/tmp/$USER")
    ws = d.remote_workspace("scratch")
    script = d.build_job_script(
        job_id="scratch",
        command=["true"],
        remote_workspace=ws,
        cpus=1,
    )

    assert "mktemp -d /tmp/$USER/vq-XXXXXX" in script
    assert 'cd "$__vq_scratch"' in script
    assert f'__vq_reconcile {ws}' in script


def test_slurm_build_job_script_applies_program_command_wrapper() -> None:
    d = make_slurm_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                command_wrapper=["/opt/vq/wrappers/orca-slurm", "--ntasks-from-pal"]
            )
        },
    )
    ws = d.remote_workspace("orca")
    script = d.build_job_script(
        job_id="orca",
        command=["orca", "input.inp"],
        remote_workspace=ws,
        cpus=1,
        program="orca",
    )

    assert (
        "-- /opt/vq/wrappers/orca-slurm --ntasks-from-pal orca input.inp "
        '> "$__vq_stdout" 2> "$__vq_stderr"'
    ) in script


def make_scratch_dispatcher(
    runner: FakeRunner, *, node_scratch_dir: str = "/tmp1/$USER"
) -> SchedulerDispatcher:
    return SchedulerDispatcher(
        TorqueDialect(),
        runner,
        scratch_root=SCRATCH,
        submit_extra=(),
        node_scratch_dir=node_scratch_dir,
    )


def test_build_job_script_node_scratch_runs_on_node_and_copies_back() -> None:
    d = make_scratch_dispatcher(FakeRunner())
    ws = d.remote_workspace("j")
    script = d.build_job_script(
        job_id="j", command=["/venv/bin/python", "run.py"], remote_workspace=ws, cpus=2
    )
    # Runs in a fresh node-local scratch dir, NOT the /home workspace.
    assert "mktemp -d /tmp1/$USER/vq-XXXXXX" in script
    assert 'cd "$__vq_scratch"' in script
    # A private seed records the submitted bytes; only changed output publishes.
    assert 'cp -a "$__vq_seed"/. "$__vq_scratch"/' in script
    assert f'__vq_reconcile {ws}' in script
    assert "/venv/bin/python run.py" in script
    # The marker is written to /home AFTER the copy-back (so a stale marker
    # copied from scratch cannot clobber it), then scratch is cleaned up.
    copyback = script.index(f'__vq_reconcile {ws}')
    marker_write = script.rindex('__vq_write_marker "$__vq_rc"')
    cleanup = script.index('rm -rf "$__vq_stage"')
    assert copyback < marker_write < cleanup
    # $USER stays unquoted so it expands on the node.
    assert "/tmp1/$USER/" in script


def test_node_scratch_artifacts_copy_back_only_after_command_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_gnu_time(tmp_path, monkeypatch)
    shared_root = tmp_path / "shared"
    node_root = tmp_path / "node"
    shared_root.mkdir()
    node_root.mkdir()
    d = SchedulerDispatcher(
        TorqueDialect(),
        FakeRunner(),
        scratch_root=str(shared_root),
        submit_extra=(),
        node_scratch_dir=str(node_root),
    )
    ws = d.remote_workspace("j")
    workspace = Path(ws)
    workspace.mkdir(parents=True)
    release = tmp_path / "release"
    payload = "\n".join(
        (
            "from pathlib import Path",
            "import sys",
            "import time",
            "Path('calc.out').write_text('out\\n')",
            "Path('calc.system').write_text('[run]\\n')",
            "Path('calc.scf.jsonl').write_text('{}\\n')",
            "print('READY', flush=True)",
            "release = Path(sys.argv[1])",
            "while not release.exists():",
            "    time.sleep(0.01)",
        )
    )
    script = d.build_job_script(
        job_id="j",
        command=[sys.executable, "-c", payload, str(release)],
        remote_workspace=ws,
        cpus=2,
    )
    script_path = tmp_path / "job.sh"
    script_path.write_text(script, encoding="utf-8")
    proc = subprocess.Popen(
        ["bash", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5.0
        stdout_log = workspace / "stdout.log"
        while time.monotonic() < deadline:
            if stdout_log.exists() and "READY" in stdout_log.read_text():
                break
            if proc.poll() is not None:
                break
            time.sleep(0.01)
        else:
            pytest.fail("node-scratch payload did not reach its ready fence")

        assert proc.poll() is None
        assert not (workspace / "calc.out").exists()
        assert not (workspace / "calc.system").exists()
        assert not (workspace / "calc.scf.jsonl").exists()

        release.touch()
        shell_stdout, shell_stderr = proc.communicate(timeout=5)
        assert proc.returncode == 0, (shell_stdout, shell_stderr)
        assert (workspace / "calc.out").read_text() == "out\n"
        assert (workspace / "calc.system").read_text() == "[run]\n"
        assert (workspace / "calc.scf.jsonl").read_text() == "{}\n"
        assert (workspace / "_vq" / "exit-code").read_text() == "0\n"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.parametrize("change", ["shared", "scratch", "both", "same", "new"])
def test_node_scratch_reconciles_outputs_against_the_staged_seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    """A shared checkpoint and a scratch output may share a staged filename."""
    _install_fake_gnu_time(tmp_path, monkeypatch)
    node_root = tmp_path / "node"
    node_root.mkdir()
    d = SchedulerDispatcher(
        SlurmDialect(), FakeRunner(), scratch_root=str(tmp_path / "shared"),
        submit_extra=(), node_scratch_dir=str(node_root),
    )
    ws = d.remote_workspace("result-integrity")
    workspace = Path(ws)
    workspace.mkdir(parents=True)
    result = workspace / "results with spaces" / "batch-results.json"
    result.parent.mkdir()
    if change != "new":
        result.write_text("historical result\n")
    (workspace / "stdout.log").write_text("staged old log\n")
    payload = textwrap.dedent(f"""\
        import os
        from pathlib import Path
        relative = Path('results with spaces/batch-results.json')
        shared = Path(os.environ['VQ_WORKDIR']) / relative
        if {change!r} in ('shared', 'both', 'same'):
            shared.write_text('fresh shared result\\n')
        if {change!r} in ('scratch', 'both', 'same', 'new'):
            relative.write_text(
                'fresh shared result\\n' if {change!r} == 'same'
                else 'fresh scratch result\\n'
            )
        Path('ordinary.out').write_text('ordinary output\\n')
        Path('private-output').mkdir(mode=0o700)
        Path('private-output/result').write_text('private result\\n')
        print('current live log', flush=True)
    """)
    script_path = tmp_path / "job.sh"
    script_path.write_text(d.build_job_script(
        job_id="result-integrity", command=[sys.executable, "-c", payload],
        remote_workspace=ws, cpus=1, env={"VQ_WORKDIR": ws},
    ))
    completed = subprocess.run(
        ["bash", str(script_path)], text=True, capture_output=True, timeout=15,
    )
    if change == "both":
        assert completed.returncode != 0
        assert result.read_text() == "fresh shared result\n"
        assert "copy-back conflict" in (workspace / "stderr.log").read_text()
        recovered = list((workspace / "_vq").glob("scratch-recovery-*/**/batch-results.json"))
        assert len(recovered) == 1
        assert recovered[0].read_text() == "fresh scratch result\n"
    else:
        assert completed.returncode == 0, (completed.stdout, completed.stderr)
        expected = (
            "fresh shared result\n" if change in ("shared", "same")
            else "fresh scratch result\n"
        )
        assert result.read_text() == expected
        assert (workspace / "ordinary.out").read_text() == "ordinary output\n"
        assert (workspace / "private-output").stat().st_mode & 0o777 == 0o700
    assert (workspace / "stdout.log").read_text() == "current live log\n"
    assert int((workspace / "_vq" / "exit-code").read_text()) == completed.returncode
    telemetry = json.loads((workspace / "_vq" / "resource-usage.json").read_text())
    assert telemetry["command_exit_code"] == 0


@pytest.mark.parametrize("shared_change", ["deleted", "symlink-parent", "unwritable-parent"])
def test_node_scratch_preserves_shared_path_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shared_change: str,
) -> None:
    _install_fake_gnu_time(tmp_path, monkeypatch)
    d = SchedulerDispatcher(
        TorqueDialect(), FakeRunner(), scratch_root=str(tmp_path / "shared"),
        submit_extra=(), node_scratch_dir=str(tmp_path / "node"),
    )
    ws = d.remote_workspace("changed-destination")
    workspace = Path(ws)
    output_dir = workspace / "output"
    output_dir.mkdir(parents=True)
    (output_dir / "result").write_text("seed\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "result").write_text("untouched\n")
    payload = textwrap.dedent(f"""\
        import os
        from pathlib import Path
        shared = Path(os.environ['VQ_WORKDIR']) / 'output'
        if {shared_change!r} == 'deleted':
            (shared / 'result').unlink()
            shared.rmdir()
        else:
            Path('output/result').write_text('scratch result\\n')
            if {shared_change!r} == 'symlink-parent':
                shared.rename(shared.with_name('old-output'))
                shared.symlink_to({str(outside)!r}, target_is_directory=True)
            else:
                shared.chmod(0o500)
    """)
    script = tmp_path / "job.sh"
    script.write_text(d.build_job_script(
        job_id="changed-destination", command=[sys.executable, "-c", payload],
        remote_workspace=ws, cpus=1, env={"VQ_WORKDIR": ws},
    ))
    try:
        result = subprocess.run(["bash", str(script)], capture_output=True, timeout=15)
        if shared_change == "deleted":
            assert result.returncode == 0
            assert not output_dir.exists()
        else:
            assert result.returncode == 125
            assert list((workspace / "_vq").glob("scratch-recovery-*/output/result"))
        assert (outside / "result").read_text() == "untouched\n"
    finally:
        if shared_change == "unwritable-parent":
            output_dir.chmod(0o700)


def test_build_job_script_node_scratch_marker_still_on_home() -> None:
    d = make_scratch_dispatcher(FakeRunner())
    ws = d.remote_workspace("j")
    script = d.build_job_script(job_id="j", command=["true"], remote_workspace=ws, cpus=1)
    # Even with node-scratch, the rc marker lives on the /home workspace.
    assert f"{ws}/_vq/exit-code" in script


def test_node_scratch_array_copybacks_do_not_race_to_overwrite_one_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_gnu_time(tmp_path, monkeypatch)
    d = SchedulerDispatcher(
        SlurmDialect(), FakeRunner(), scratch_root=str(tmp_path / "shared"),
        submit_extra=(), node_scratch_dir=str(tmp_path / "node"),
    )
    ws = d.remote_workspace("array-results")
    workspace = Path(ws)
    workspace.mkdir(parents=True)
    (workspace / "result").write_text("seed")
    release = tmp_path / "release"
    payload = textwrap.dedent(f"""\
        import os, time
        from pathlib import Path
        Path('result').write_text(os.environ['SLURM_ARRAY_TASK_ID'])
        print('READY', flush=True)
        while not Path({str(release)!r}).exists():
            time.sleep(0.01)
    """)
    script = tmp_path / "job.sh"
    script.write_text(d.build_job_script(
        job_id="array-results", command=[sys.executable, "-c", payload],
        remote_workspace=ws, cpus=1, array_size=2, env={"VQ_WORKDIR": ws},
    ))
    processes = [subprocess.Popen(
        ["bash", str(script)], env={**os.environ, "SLURM_ARRAY_TASK_ID": str(index)},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ) for index in range(2)]
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if all((workspace / f"stdout.log.{i}").exists() and
                   "READY" in (workspace / f"stdout.log.{i}").read_text() for i in range(2)):
                break
            time.sleep(0.01)
        else:
            pytest.fail("both array payloads did not reach their seed fence")
        release.touch()
        statuses = [p.wait(timeout=15) for p in processes]
        assert sorted(statuses) == [0, 125]
        winner = str(statuses.index(0))
        assert (workspace / "result").read_text() == winner
        recovered = list((workspace / "_vq").glob("scratch-recovery-*/result"))
        assert len(recovered) == 1
        assert recovered[0].read_text() == str(statuses.index(125))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_build_job_script_without_node_scratch_runs_in_workspace() -> None:
    # Default (no node_scratch_dir): runs in the /home workspace, no scratch.
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("j")
    script = d.build_job_script(job_id="j", command=["true"], remote_workspace=ws, cpus=1)
    assert f"cd {ws}" in script
    assert "mktemp" not in script
    assert "__vq_scratch" not in script


def test_workdir_docs_match_scheduler_scratch_runtime_contract() -> None:
    """The agent contract must name the same discriminator as the scripts.

    Without ``node_scratch_dir`` the payload cwd and scheduler workspace are
    one directory; with it the payload runs from a node-local copy. The docs
    used to promise two directories unconditionally, which made safe-looking
    payloads overwrite their own inputs on the first route.
    """
    # The pre-split CLAUDE.md, which also stated this contract, stayed in the
    # archived monorepo. AGENTS.md here sends agents to agent_interaction.md,
    # the copy of the contract this repository holds itself to.
    repo_root = Path(__file__).resolve().parents[1]
    interaction_path = repo_root / "docs" / "agent_interaction.md"
    assert interaction_path.is_file(), f"{interaction_path} is missing"
    documents = (interaction_path,)
    required_contract = (
        "node_scratch_dir",
        "$VQ_WORKDIR` resolves to the staged workspace",
        "Never write onto a payload file.",
        "dedicated output subdirectory",
        "prove it is writable with a real write",
    )

    for document in documents:
        text = " ".join(document.read_text().split())
        for statement in required_contract:
            assert statement in text, f"{document} omits {statement!r}"

    interaction_doc = " ".join(interaction_path.read_text().split())
    assert "Scheduler jobs have no separately managed workdir to fetch" in interaction_doc
    assert "shared workspace returned by ordinary `vq fetch`" in interaction_doc
    assert "output_dir.mkdir(exist_ok=False)" in interaction_doc


def test_build_job_script_inserts_scheduler_hooks_inside_workdir() -> None:
    d = make_dispatcher(
        FakeRunner(),
        scheduler_prologue=(
            "module purge",
            "source /home/USER/cluster-env.sh",
        ),
        scheduler_epilogue=("cp -f scratch.log artifacts/ 2>/dev/null || true",),
    )
    ws = d.remote_workspace("hooked")
    script = d.build_job_script(
        job_id="hooked",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=1,
    )

    cd = script.index(f"cd {ws}")
    module = script.index("module purge")
    source = script.index("source /home/USER/cluster-env.sh")
    run = script.index('/venv/bin/python run.py > "$__vq_stdout"')
    capture = script.index("__vq_rc=$?")
    epilogue = script.index("cp -f scratch.log artifacts/")
    marker = script.rindex('__vq_write_marker "$__vq_rc"')
    assert cd < module < source < run < capture < epilogue < marker


def test_build_job_script_node_scratch_hooks_wrap_user_command_before_copyback() -> None:
    d = SchedulerDispatcher(
        TorqueDialect(),
        FakeRunner(),
        scratch_root=SCRATCH,
        node_scratch_dir="/tmp1/$USER",
        scheduler_prologue=("source /home/USER/cluster-env.sh",),
        scheduler_epilogue=("python summarize.py || true",),
    )
    ws = d.remote_workspace("scratch-hook")
    script = d.build_job_script(
        job_id="scratch-hook",
        command=["/venv/bin/python", "run.py"],
        remote_workspace=ws,
        cpus=1,
    )

    cd = script.index('cd "$__vq_scratch"')
    prologue = script.index("source /home/USER/cluster-env.sh")
    run = script.index('/venv/bin/python run.py > "$__vq_stdout"')
    epilogue = script.index("python summarize.py || true")
    copyback = script.index(f'__vq_reconcile {ws}')
    assert cd < prologue < run < epilogue < copyback


def test_build_job_script_inserts_program_hooks_for_matching_program() -> None:
    d = make_dispatcher(
        FakeRunner(),
        scheduler_prologue=("module purge",),
        scheduler_epilogue=("rm -f scratch.tmp",),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                prologue=["source /home/USER/orca-env.sh"],
                epilogue=["cp -f orca.out artifacts/ 2>/dev/null || true"],
            )
        },
    )
    ws = d.remote_workspace("orca-job")
    script = d.build_job_script(
        job_id="orca-job",
        command=["/home/USER/bin/orca", "input.inp"],
        remote_workspace=ws,
        cpus=8,
        program="orca",
    )

    cd = script.index(f"cd {ws}")
    host_prologue = script.index("module purge")
    program_prologue = script.index("source /home/USER/orca-env.sh")
    run = script.index('/home/USER/bin/orca input.inp > "$__vq_stdout"')
    capture = script.index("__vq_rc=$?")
    program_epilogue = script.index("cp -f orca.out artifacts/")
    host_epilogue = script.index("rm -f scratch.tmp")
    marker = script.rindex('__vq_write_marker "$__vq_rc"')
    assert (
        cd
        < host_prologue
        < program_prologue
        < run
        < capture
        < program_epilogue
        < host_epilogue
        < marker
    )


def test_build_job_script_applies_program_command_wrapper() -> None:
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                command_wrapper=["/home/USER/bin/orcasub", "--scheduler"]
            )
        },
    )
    ws = d.remote_workspace("orca-wrapped")
    script = d.build_job_script(
        job_id="orca-wrapped",
        command=["/home/USER/orca/orca", "input.inp"],
        remote_workspace=ws,
        cpus=8,
        program="orca",
    )

    assert (
        "-- /home/USER/bin/orcasub --scheduler /home/USER/orca/orca input.inp "
        '> "$__vq_stdout" 2> "$__vq_stderr"'
    ) in script


def test_build_job_script_does_not_duplicate_existing_program_wrapper() -> None:
    """A payload already launched through the registered wrapper stays singular."""
    wrapper = "/home/USER/bin/vibeqc-release-python"
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "vibeqc-release": SchedulerProgramHooks(command_wrapper=[wrapper])
        },
    )
    ws = d.remote_workspace("release-wrapped")
    script = d.build_job_script(
        job_id="release-wrapped",
        command=[wrapper, "run_batch.py", "batch.tsv"],
        remote_workspace=ws,
        cpus=2,
        program="vibeqc-release",
    )

    expected = f'{wrapper} run_batch.py batch.tsv > "$__vq_stdout"'
    assert expected in script
    assert f"{wrapper} {wrapper}" not in script


def test_build_job_script_quotes_program_command_wrapper_argv() -> None:
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                command_wrapper=[
                    "/home/USER/bin/orca sub",
                    "--scheduler mode",
                    "quote'arg",
                ]
            )
        },
    )
    ws = d.remote_workspace("orca-quoted-wrapper")
    script = d.build_job_script(
        job_id="orca-quoted-wrapper",
        command=["/home/USER/orca/orca bin", "input file.inp", "arg'quoted"],
        remote_workspace=ws,
        cpus=8,
        program="orca",
    )

    expected = shlex.join(
        [
            "/home/USER/bin/orca sub",
            "--scheduler mode",
            "quote'arg",
            "/home/USER/orca/orca bin",
            "input file.inp",
            "arg'quoted",
        ]
    )
    assert f'{expected} > "$__vq_stdout" 2> "$__vq_stderr"' in script


def test_build_job_script_ignores_program_hooks_for_unmatched_program() -> None:
    d = make_dispatcher(
        FakeRunner(),
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                prologue=["source /home/USER/orca-env.sh"],
                command_wrapper=["/home/USER/bin/orcasub"],
            )
        },
    )
    ws = d.remote_workspace("plain-job")
    script = d.build_job_script(
        job_id="plain-job",
        command=["true"],
        remote_workspace=ws,
        cpus=1,
        program="vibeqc-dev",
    )

    assert "source /home/USER/orca-env.sh" not in script
    assert "/home/USER/bin/orcasub" not in script
    assert 'true > "$__vq_stdout" 2> "$__vq_stderr"' in script


def test_build_job_script_exports_env_sorted() -> None:
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("e")
    script = d.build_job_script(
        job_id="e",
        command=["true"],
        remote_workspace=ws,
        cpus=1,
        env={"BETA": "2", "ALPHA": "1"},
    )
    a = script.index("export ALPHA=1")
    b = script.index("export BETA=2")
    assert a < b  # deterministic, sorted


def test_build_job_script_rejects_non_ascii_command() -> None:
    # Inherits the dialect's pure-ASCII guard (Torque qsub rejects non-ASCII).
    d = make_dispatcher(FakeRunner())
    ws = d.remote_workspace("u")
    with pytest.raises(DialectError, match="ASCII"):
        d.build_job_script(
            job_id="u", command=["echo", "café"], remote_workspace=ws, cpus=1
        )


# --------------------------------------------------------------------------- #
# submit
# --------------------------------------------------------------------------- #


def _qsub_ok(job_id: str = "12345.cluster") -> Responder:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            return RemoteResult(0, f"{job_id}\n", "")
        return None

    return responder


def _sbatch_ok(job_id: str = "12345") -> Responder:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sbatch":
            return RemoteResult(0, f"Submitted batch job {job_id}\n", "")
        return None

    return responder


def test_submit_happy_path_sequence_and_handle() -> None:
    runner = FakeRunner(responder=_qsub_ok())
    d = make_dispatcher(runner, submit_extra=("-q", "compute", "-A", "proj1"))
    handle = d.submit(job_id="job1", command=["true"], cpus=2)

    assert handle == SchedulerHandle(
        job_id="12345.cluster",
        remote_workspace=f"{SCRATCH}/.vibeqc-cluster/jobs/job1",
        array_size=None,
    )
    heads = [c.argv[0] for c in runner.calls]
    # One setup call, qsub, then the durable submit-once receipt. With no
    # workspace to stage, mkdir and script write share the first connection.
    assert heads == ["sh", "sh", "qsub", "sh"]
    # The script was written via `cat > <ws>/job.pbs` with the script on stdin.
    write = runner.first("sh")
    assert write.argv[:2] == ["sh", "-c"]
    assert "mkdir -p " in write.argv[2]
    assert "cat > " in write.argv[2]
    assert write.stdin is not None and write.stdin.startswith("#!/bin/bash\n")
    # Review note 1: qsub argv carries ONLY the script path — no -q/-A.
    qsub = runner.first("qsub")
    assert qsub.argv == ["qsub", f"{SCRATCH}/.vibeqc-cluster/jobs/job1/job.pbs"]
    assert "-q" not in qsub.argv and "-A" not in qsub.argv

    receipts = [
        json.loads(call.stdin)
        for call in runner.calls
        if call.stdin is not None and call.stdin.startswith("{")
    ]
    assert receipts == [
        {
            "schema": "vq.scheduler-submit-once.v1",
            "status": "accepted",
            "vq_job_id": "job1",
            "scheduler_job_id": "12345.cluster",
            "scheduler_returncode": 0,
        }
    ]


def test_submit_rejects_over_limit_before_remote_stage_or_scheduler_call() -> None:
    runner = FakeRunner(responder=_sbatch_ok())
    dispatcher = SchedulerDispatcher(
        SlurmDialect(),
        runner,
        scratch_root=SCRATCH,
        submit_extra=("--partition", "compute"),
        max_wall_time_seconds=28_800,
    )

    with pytest.raises(
        DialectError,
        match="scheduler lane.*compute.*allows at most 28800 s",
    ):
        dispatcher.submit(
            job_id="too-long",
            command=["true"],
            cpus=1,
            wall_time_seconds=43_200,
        )

    assert runner.calls == []
    assert runner.uploads == []


def test_slurm_submit_happy_path_sequence_and_handle() -> None:
    runner = FakeRunner(responder=_sbatch_ok())
    d = make_slurm_dispatcher(
        runner,
        submit_extra=("--account", "<group-account>", "--partition", "intelsr_devel"),
    )
    handle = d.submit(job_id="job1", command=["true"], cpus=2, array_size=2)

    assert handle == SchedulerHandle(
        job_id="12345",
        remote_workspace=f"{SCRATCH}/.vibeqc-cluster/jobs/job1",
        array_size=2,
    )
    heads = [c.argv[0] for c in runner.calls]
    # One setup call (mkdir + script write share a connection), sbatch, then
    # the durable submit-once receipt.
    assert heads == ["sh", "sh", "sbatch", "sh"]
    write = runner.first("sh")
    assert write.stdin is not None
    assert "#SBATCH --account=<group-account>" in write.stdin
    assert "#SBATCH --partition=intelsr_devel" in write.stdin
    sbatch = runner.first("sbatch")
    assert sbatch.argv == ["sbatch", f"{SCRATCH}/.vibeqc-cluster/jobs/job1/job.pbs"]
    assert "--account" not in sbatch.argv and "--partition" not in sbatch.argv


def test_submit_stages_local_workspace(tmp_path: Path) -> None:
    runner = FakeRunner(responder=_qsub_ok())
    d = make_dispatcher(runner)
    (tmp_path / "input.dat").write_text("payload")
    d.submit(job_id="j", command=["true"], cpus=1, local_workspace=tmp_path)

    # A tarball is scp'd beside the workspace, then ONE call unpacks it, drops
    # it and writes the job script. Six round trips per job became three.
    ((local_tar, remote_tar),) = runner.uploads
    assert remote_tar == f"{SCRATCH}/.vibeqc-cluster/jobs/j.upload.tar"
    assert local_tar.suffix == ".tar"
    heads = [c.argv[0] for c in runner.calls]
    assert heads == ["mkdir", "sh", "sh", "qsub", "sh"]
    setup = runner.first("sh").argv[2]
    assert "mkdir -p " in setup
    assert "tar -xf " in setup
    assert "rm -f " in setup
    assert "cat > " in setup


def test_missing_python_entrypoint_is_rejected_before_scheduler_mutation(
    tmp_path: Path,
) -> None:
    (tmp_path / "batch-list.txt").write_text("case-001\n")
    runner = FakeRunner(responder=_qsub_ok())
    dispatcher = make_dispatcher(runner)

    with pytest.raises(
        SchedulerError,
        match=r"payload validation.*run_validation_batch\.py.*not present",
    ):
        dispatcher.submit(
            job_id="artval24",
            command=[
                "/cluster/runtime/vibeqc-release-python",
                "run_validation_batch.py",
            ],
            cpus=1,
            local_workspace=tmp_path,
        )

    assert runner.uploads == []
    assert runner.calls == []


def test_missing_workspace_is_normalized_before_scheduler_mutation(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing-workspace"
    runner = FakeRunner(responder=_qsub_ok())
    dispatcher = make_dispatcher(runner)

    with pytest.raises(
        SchedulerError,
        match=r"payload validation failed: staged workspace is unavailable",
    ):
        dispatcher.submit(
            job_id="artval24",
            command=["python", "run.py"],
            cpus=1,
            local_workspace=missing,
        )

    assert runner.uploads == []
    assert runner.calls == []


def test_unsafe_workspace_link_is_rejected_before_scheduler_mutation(
    tmp_path: Path,
) -> None:
    (tmp_path / "run.py").write_text("pass\n")
    (tmp_path / "outside-link").symlink_to("../../outside")
    runner = FakeRunner(responder=_qsub_ok())
    dispatcher = make_dispatcher(runner)

    with pytest.raises(
        SchedulerError,
        match=r"payload validation.*outside-link.*unsafe",
    ):
        dispatcher.submit(
            job_id="artval24",
            command=["python", "run.py"],
            cpus=1,
            local_workspace=tmp_path,
        )

    assert runner.uploads == []
    assert runner.calls == []


def test_slurm_directory_payload_reaches_sbatch_and_preserves_rejection(
    tmp_path: Path,
) -> None:
    (tmp_path / "input.py").write_text("pass\n")
    for index in range(12):
        path = tmp_path / "inputs" / f"case-{index}.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text("{}")

    scheduler_stderr = (
        "sbatch: error: Batch job submission failed: "
        "Requested time limit is invalid (missing or exceeds some limit)"
    )

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sbatch":
            return RemoteResult(1, "", scheduler_stderr)
        return None

    runner = FakeRunner(responder=responder)
    dispatcher = make_slurm_dispatcher(
        runner,
        submit_extra=("--account", "proj1", "--partition", "short"),
    )

    with pytest.raises(SchedulerError, match="Requested time limit is invalid"):
        dispatcher.submit(
            job_id="rp211",
            command=["input.py"],
            cpus=1,
            scheduler_tasks=1,
            mem_mb=16_000,
            wall_time_seconds=7200,
            local_workspace=tmp_path,
        )

    ((_local_tar, remote_tar),) = runner.uploads
    assert remote_tar == f"{SCRATCH}/.vibeqc-cluster/jobs/rp211.upload.tar"
    script = runner.first("sh").stdin
    assert script is not None
    assert "#SBATCH --time=02:00:00" in script
    assert "#SBATCH --partition=short" in script
    assert runner.first("sbatch").argv == [
        "sbatch",
        f"{SCRATCH}/.vibeqc-cluster/jobs/rp211/job.pbs",
    ]


def test_submit_no_staging_when_no_local_workspace() -> None:
    runner = FakeRunner(responder=_qsub_ok())
    d = make_dispatcher(runner)
    d.submit(job_id="j", command=["true"], cpus=1)
    assert runner.uploads == []


def test_submit_raises_on_qsub_failure() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            return RemoteResult(1, "", "qsub: Bad UID for job execution")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    with pytest.raises(SchedulerError, match="qsub failed"):
        d.submit(job_id="j", command=["true"], cpus=1)
    receipts = [
        json.loads(call.stdin)
        for call in runner.calls
        if call.stdin is not None and call.stdin.startswith("{")
    ]
    assert receipts == [
        {
            "schema": "vq.scheduler-submit-once.v1",
            "status": "rejected",
            "vq_job_id": "j",
            "scheduler_job_id": None,
            "scheduler_returncode": 1,
        }
    ]


def test_submit_preserves_ambiguous_qsub_as_typed_unknown() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            raise transport.RemoteOutcomeUnknown("observer lost after qsub")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    with pytest.raises(unknown_type, match="outcome is unknown"):
        d.submit(job_id="j", command=["true"], cpus=1)


def test_submit_zero_with_no_scheduler_id_is_outcome_unknown() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            return RemoteResult(0, "scheduler banner without an id\n", "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    unknown_type = scheduler_dispatch.SchedulerSubmitOutcomeUnknown
    with pytest.raises(unknown_type, match="outcome is unknown"):
        d.submit(job_id="j", command=["true"], cpus=1)
    receipts = [
        json.loads(call.stdin)
        for call in runner.calls
        if call.stdin is not None and call.stdin.startswith("{")
    ]
    assert receipts[0]["status"] == "outcome_unknown"


def test_submit_raises_when_the_setup_call_fails() -> None:
    """With no workspace to stage, the mkdir rides the combined setup call, so
    a permission failure surfaces from there rather than a bare `mkdir`."""

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sh":
            return RemoteResult(1, "", "mkdir: Permission denied")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerError, match="create remote workspace"):
        d.submit(job_id="j", command=["true"], cpus=1)


def test_submit_raises_when_the_jobs_root_cannot_be_created(tmp_path: Path) -> None:
    """The one mkdir that still stands alone, because the scp needs it."""

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "mkdir":
            return RemoteResult(1, "", "mkdir: Permission denied")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerError, match="create remote jobs root"):
        d.submit(job_id="j", command=["true"], cpus=1, local_workspace=tmp_path)


def test_submit_raises_on_unparseable_qsub_id() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            return RemoteResult(0, "not-a-job-id\n", "")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerSubmitOutcomeUnknown):
        d.submit(job_id="j", command=["true"], cpus=1)


def test_submit_wraps_upload_failure_as_scheduler_error(tmp_path: Path) -> None:
    """Regression (2026-07-16 host_c dispatch wedge): staging crosses the SSH
    boundary and ``upload_tree`` can raise a *raw* ``transport.RemoteError``
    (e.g. the untar step failing). ``submit`` must convert it to
    ``SchedulerError`` — the dispatch-domain error ``_start_scheduler_job``
    already catches — so ONE job's upload failure lands THAT spec FAILED rather
    than the raw error escaping ``submit`` and aborting the daemon's whole
    reconcile+dispatch tick. Mirrors the ``fetch_results`` download/extract
    hardening (``test_fetch_results_wraps_extraction_error_as_scheduler_error``).
    """

    class _UploadFailsRunner(FakeRunner):
        def upload_file(self, local_path: Path, remote_path: str) -> None:
            raise transport.RemoteError("simulated scp upload failure")

        def upload_tree(self, local_dir: Path, remote_dir: str) -> None:
            # The exact production symptom: the untar can't find the tarball.
            raise transport.RemoteError(
                "remote shell failed (exit 2) on host_c:\n"
                "  cmd: tar -xf /tmp/vq-upload-abc123.tar -C "
                f"{remote_dir}\n"
                "  stderr: tar: /tmp/vq-upload-abc123.tar: Cannot open: "
                "No such file or directory"
            )

    runner = _UploadFailsRunner(responder=_qsub_ok())
    d = make_dispatcher(runner)
    (tmp_path / "input.dat").write_text("payload")
    with pytest.raises(SchedulerError, match="failed to stage workspace"):
        d.submit(job_id="j", command=["true"], cpus=1, local_workspace=tmp_path)
    # It never reached qsub — staging aborts submit before the queue.
    assert not any(c.argv and c.argv[0] == "qsub" for c in runner.calls)


# --------------------------------------------------------------------------- #
# poll
# --------------------------------------------------------------------------- #

_QSTAT_TABLE = textwrap.dedent("""\
    Job id                    Name             User            Time Use S Queue
    ------------------------- ---------------- --------------- -------- - -----
    1.cluster                 runjob           alice            00:10:00 R compute
    2.cluster                 queuedjob        alice                   0 Q compute
""")


def test_poll_batched_and_absent_is_finished() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qstat":
            return RemoteResult(0, _QSTAT_TABLE, "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    handles = [
        SchedulerHandle("1.cluster", "/ws/1"),
        SchedulerHandle("2.cluster", "/ws/2"),
        SchedulerHandle("3.cluster", "/ws/3"),  # absent from table -> FINISHED
    ]
    phases = d.poll(handles)
    assert phases == {
        "1.cluster": SchedulerPhase.RUNNING,
        "2.cluster": SchedulerPhase.PENDING,
        "3.cluster": SchedulerPhase.FINISHED,
    }
    # Review note 2 + design §4: ONE batched plain `qstat <ids>` — no -f, no -format.
    qstat_calls = [c.argv for c in runner.calls if c.argv and c.argv[0] == "qstat"]
    assert qstat_calls == [["qstat", "1.cluster", "2.cluster", "3.cluster"]]
    assert "-f" not in qstat_calls[0]


def test_poll_accepts_unique_torque_truncated_job_id() -> None:
    qstat_table = textwrap.dedent("""\
        Job id                    Name             User            Time Use S Queue
        ------------------------- ---------------- --------------- -------- - -----
        6528.pbs.cluster          runjob           alice            00:01:00 R big
        6529.pbs.cluster          queuedjob        alice                   0 Q inf
    """)

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qstat":
            return RemoteResult(0, qstat_table, "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    handles = [
        SchedulerHandle("6528.pbs.cluster.example", "/ws/1"),
        SchedulerHandle("6529.pbs.cluster.example", "/ws/2"),
        SchedulerHandle("6530.pbs.cluster.example", "/ws/3"),
    ]

    assert d.poll(handles) == {
        "6528.pbs.cluster.example": SchedulerPhase.RUNNING,
        "6529.pbs.cluster.example": SchedulerPhase.PENDING,
        "6530.pbs.cluster.example": SchedulerPhase.FINISHED,
    }


def test_slurm_poll_keeps_array_master_live_from_elements() -> None:
    squeue_table = textwrap.dedent("""\
        123_0|PENDING|00:00|01:00:00|(Priority)|Priority
        123_1|RUNNING|00:01|01:00:00|node001|None
        124_0|PENDING|00:00|01:00:00|(Priority)|Priority
        125_0|COMPLETED|00:10|01:00:00|node002|None
    """)

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            return RemoteResult(0, squeue_table, "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_slurm_dispatcher(runner)
    handles = [
        SchedulerHandle("123", "/ws/123", array_size=2),
        SchedulerHandle("124", "/ws/124", array_size=1),
        SchedulerHandle("125", "/ws/125", array_size=1),
        SchedulerHandle("126", "/ws/126", array_size=1),
    ]

    assert d.poll(handles) == {
        "123": SchedulerPhase.RUNNING,
        "124": SchedulerPhase.PENDING,
        "125": SchedulerPhase.FINISHED,
        "126": SchedulerPhase.FINISHED,
    }
    assert runner.first("squeue").argv == [
        "squeue",
        "--noheader",
        "--format=%i|%T|%M|%l|%N|%r",
        "--jobs",
        "123,124,125,126",
    ]


def test_slurm_poll_evidence_carries_why_a_job_is_still_queued() -> None:
    # vibe-qc#148: the poll already knew; nothing carried it to the operator.
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            return RemoteResult(
                0,
                "123|PENDING|00:00|01:00:00||Resources\n"
                "124|RUNNING|00:01|01:00:00|node001|None\n",
                "",
            )
        return None

    dispatcher = make_slurm_dispatcher(FakeRunner(responder=responder))
    evidence = dispatcher.poll_with_evidence(
        [SchedulerHandle("123", "/ws/123"), SchedulerHandle("124", "/ws/124")]
    )

    assert evidence.queued_reasons == {"123": "Resources"}


def test_poll_evidence_survives_a_dialect_without_reason_support() -> None:
    # A reason is telemetry: a dialect that cannot supply one must cost the
    # operator an explanation, never a failed observation.
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            return RemoteResult(0, "123|RUNNING|00:01|01:00:00|node001|None\n", "")
        return None

    class _NoReasonHook:
        """A dialect from before the reason hook existed."""

        def __init__(self, inner: object) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> object:
            if name == "parse_poll_reasons":
                raise AttributeError(name)
            return getattr(self._inner, name)

    dispatcher = make_slurm_dispatcher(FakeRunner(responder=responder))
    dispatcher.dialect = _NoReasonHook(dispatcher.dialect)  # type: ignore[assignment]

    evidence = dispatcher.poll_with_evidence([SchedulerHandle("123", "/ws/123")])

    assert evidence.phases == {"123": SchedulerPhase.RUNNING}
    assert evidence.queued_reasons == {}


def test_slurm_poll_does_not_prefix_match_distinct_numeric_ids() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            return RemoteResult(
                0,
                "123|RUNNING|00:01|01:00:00|node001|None\n",
                "",
            )
        return None

    dispatcher = make_slurm_dispatcher(FakeRunner(responder=responder))
    phases = dispatcher.poll(
        [
            SchedulerHandle("123", "/ws/123"),
            SchedulerHandle("1234", "/ws/1234"),
        ]
    )

    assert phases == {
        "123": SchedulerPhase.RUNNING,
        "1234": SchedulerPhase.FINISHED,
    }


def test_torque_prefix_compatibility_does_not_match_distinct_numeric_ids() -> None:
    qstat_table = textwrap.dedent("""\
        Job id                    Name             User            Time Use S Queue
        ------------------------- ---------------- --------------- -------- - -----
        123                       runjob           alice            00:01:00 R big
    """)

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qstat":
            return RemoteResult(0, qstat_table, "")
        return None

    dispatcher = make_dispatcher(FakeRunner(responder=responder))
    phases = dispatcher.poll(
        [
            SchedulerHandle("123", "/ws/123"),
            SchedulerHandle("1234", "/ws/1234"),
        ]
    )

    assert phases == {
        "123": SchedulerPhase.RUNNING,
        "1234": SchedulerPhase.FINISHED,
    }


def test_slurm_poll_nonzero_is_unknown_not_finished() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: (
            RemoteResult(1, "", "Bearer SECRET_TOKEN\n" + ("x" * 200_000))
            if argv and argv[0] == "squeue"
            else None
        )
    )
    dispatcher = make_slurm_dispatcher(runner)

    with pytest.raises(SchedulerError, match="squeue.*exit 1") as caught:
        dispatcher.poll([SchedulerHandle("123", "/ws/123")])
    assert "SECRET_TOKEN" not in str(caught.value)
    assert len(str(caught.value)) <= 240


def test_slurm_poll_isolates_an_explicitly_invalid_id_from_live_siblings() -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "squeue":
            requested = argv[argv.index("--jobs") + 1]
            if requested == "123":
                return RemoteResult(
                    0,
                    "123|RUNNING|00:01|01:00:00|node001|None\n",
                    "",
                )
            if requested == "999":
                return RemoteResult(
                    1,
                    "",
                    "squeue: error: Invalid job id specified\n",
                )
            return RemoteResult(
                1,
                "",
                "squeue: error: Invalid job id specified\n",
            )
        return None

    dispatcher = make_slurm_dispatcher(FakeRunner(responder=responder))
    evidence = dispatcher.poll_with_evidence(
        [
            SchedulerHandle("123", "/ws/123"),
            SchedulerHandle("999", "/ws/999"),
        ]
    )

    assert evidence.phases == {
        "123": SchedulerPhase.RUNNING,
        "999": SchedulerPhase.FINISHED,
    }
    assert evidence.explicitly_absent_job_ids == frozenset({"999"})
    assert [
        call.argv[call.argv.index("--jobs") + 1]
        for call in dispatcher.runner.calls
        if call.argv and call.argv[0] == "squeue"
    ] == ["123,999", "123", "999"]


def test_slurm_accounting_nonzero_is_unavailable() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: (
            RemoteResult(1, "", "token=SECRET_TOKEN\n" + ("x" * 200_000))
            if argv and argv[0] == "sacct"
            else None
        )
    )
    dispatcher = make_slurm_dispatcher(runner)

    with pytest.raises(SchedulerError, match="sacct.*exit 1") as caught:
        dispatcher.poll_detail([SchedulerHandle("123", "/ws/123")])
    assert "SECRET_TOKEN" not in str(caught.value)
    assert len(str(caught.value)) <= 240


def test_poll_empty_handles_makes_no_call() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    assert d.poll([]) == {}
    assert runner.calls == []


def test_poll_detail_parses_exec_host_and_walltime() -> None:
    qstat_f = (
        "Job Id: 1.cluster\n"
        "    job_state = R\n"
        "    exec_host = node07/0-19\n"
        "    resources_used.walltime = 02:00:00\n"
        "    Resource_List.walltime = 08:00:00\n"
    )

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(0, qstat_f, "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    detail = d.poll_detail([SchedulerHandle("1.cluster", "/ws/1")])
    assert detail["1.cluster"].exec_host == "node07/0-19"
    assert detail["1.cluster"].walltime_used == "02:00:00"
    # Batched `qstat -f` over the ids.
    assert runner.first("qstat").argv == ["qstat", "-f", "1.cluster"]


def test_poll_detail_empty_handles_makes_no_call() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    assert d.poll_detail([]) == {}
    assert runner.calls == []


def test_tail_log_reads_remote_stdout() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[:2] == ["tail", "-n"]:
            return RemoteResult(0, "iter 1\niter 2\n", "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    out = d.tail_log(SchedulerHandle("1.cluster", "/ws/1"), lines=50)
    assert out == "iter 1\niter 2\n"
    assert runner.first("tail").argv == ["tail", "-n", "50", "/ws/1/stdout.log"]


def test_tail_log_lines_none_reads_whole_remote_file() -> None:
    runner = FakeRunner(
        responder=lambda a, s: RemoteResult(0, "all\n", "") if a[0] == "tail" else None
    )
    d = make_dispatcher(runner)
    out = d.tail_log(SchedulerHandle("1.cluster", "/ws/1"), lines=None)
    assert out == "all\n"
    assert runner.first("tail").argv == ["tail", "-n", "+1", "/ws/1/stdout.log"]


def test_tail_log_stderr_and_array_index() -> None:
    runner = FakeRunner(responder=lambda a, s: RemoteResult(0, "x", "") if a[0] == "tail" else None)
    d = make_dispatcher(runner)
    d.tail_log(SchedulerHandle("1.cluster", "/ws/1"), stream="stderr", array_index=2)
    assert runner.first("tail").argv[-1] == "/ws/1/stderr.log.2"


def test_tail_log_missing_file_returns_empty() -> None:
    runner = FakeRunner(
        responder=lambda a, s: RemoteResult(1, "", "tail: no such file") if a[0] == "tail" else None
    )
    d = make_dispatcher(runner)
    assert d.tail_log(SchedulerHandle("1.cluster", "/ws/1")) == ""


def test_tail_file_reads_arbitrary_remote_workspace_file() -> None:
    runner = FakeRunner(
        responder=lambda a, s: RemoteResult(0, "energy\n", "") if a[0] == "tail" else None
    )
    d = make_dispatcher(runner)
    out = d.tail_file(SchedulerHandle("1.cluster", "/ws/1"), filename="calc.out", lines=25)
    assert out == "energy\n"
    assert runner.first("tail").argv == ["tail", "-n", "25", "/ws/1/calc.out"]


def test_tail_file_lines_none_reads_whole_remote_workspace_file() -> None:
    runner = FakeRunner(
        responder=lambda a, s: RemoteResult(0, "all\n", "") if a[0] == "tail" else None
    )
    d = make_dispatcher(runner)
    d.tail_file(SchedulerHandle("1.cluster", "/ws/1"), filename="calc.out", lines=None)
    assert runner.first("tail").argv == ["tail", "-n", "+1", "/ws/1/calc.out"]


def test_tail_file_since_reads_only_appended_remote_bytes() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[:2] == ["sh", "-c"]:
            encoded = base64.b64encode(b"new text\n").decode("ascii")
            return RemoteResult(0, f"19 0 123:19\n{encoded}\n", "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)

    out = d.tail_file_since(
        SchedulerHandle("1.cluster", "/ws/1"),
        filename="calc.out",
        byte_offset=10,
    )

    assert out == SchedulerFileChunk(19, b"new text\n")
    call = runner.first("sh").argv
    assert call[:2] == ["sh", "-c"]
    assert call[-4:] == [
        "vq-tail-file-since",
        "/ws/1/calc.out",
        "10",
        "-",
    ]
    assert "tail -c" in call[2]
    assert "head -c" in call[2]
    assert "base64" in call[2]
    assert "wc -c" in call[2]


def test_tail_file_since_caps_body_to_reported_cursor() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: RemoteResult(
            0,
            "5 0 123:5\nYWJjZGVm\n",
            "",
        )
        if argv[:2] == ["sh", "-c"]
        else None
    )
    dispatcher = make_dispatcher(runner)

    out = dispatcher.tail_file_since(
        SchedulerHandle("1.cluster", "/ws/1"),
        filename="calc.out",
        byte_offset=0,
    )

    assert out is None


def test_tail_file_since_carries_boundary_token_and_reports_rewrite() -> None:
    calls = 0

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        nonlocal calls
        if argv[:2] != ["sh", "-c"]:
            return None
        calls += 1
        if calls == 1:
            assert argv[-1] == "-"
            return RemoteResult(0, "4 0 111:4\nb2xkCg==\n", "")
        assert argv[-1] == "111:4"
        return RemoteResult(0, "4 1 222:4\nbmV3Cg==\n", "")

    dispatcher = make_dispatcher(FakeRunner(responder=responder))
    handle = SchedulerHandle("1.cluster", "/ws/1")

    first = dispatcher.tail_file_since(
        handle,
        filename="calc.out",
        byte_offset=0,
    )
    second = dispatcher.tail_file_since(
        handle,
        filename="calc.out",
        byte_offset=4,
    )

    assert first == SchedulerFileChunk(4, b"old\n")
    assert second == SchedulerFileChunk(4, b"new\n", reset=True)
    assert "cksum" in dispatcher.runner.calls[0].argv[2]


def test_tail_file_since_shell_protocol_is_byte_exact_and_detects_rewrite(
    tmp_path: Path,
) -> None:
    class LocalShellRunner:
        def run(
            self,
            argv: list[str],
            *,
            stdin_data: str | None = None,
            check: bool = False,
        ) -> RemoteResult:
            proc = subprocess.run(
                argv,
                input=stdin_data,
                capture_output=True,
                text=True,
                check=check,
            )
            return RemoteResult(proc.returncode, proc.stdout, proc.stderr)

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    path = workspace / "calc.out"
    original = "old α\n".encode()
    replacement = "new α\n".encode()
    assert len(original) == len(replacement)
    path.write_bytes(original)
    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        LocalShellRunner(),
        scratch_root="/home/USER",
    )
    handle = SchedulerHandle("1.cluster", str(workspace))

    first = dispatcher.tail_file_since(
        handle,
        filename="calc.out",
        byte_offset=0,
    )
    path.write_bytes(replacement)
    second = dispatcher.tail_file_since(
        handle,
        filename="calc.out",
        byte_offset=len(original),
    )

    assert first == SchedulerFileChunk(len(original), original)
    assert second == SchedulerFileChunk(
        len(replacement),
        replacement,
        reset=True,
    )


def test_tail_file_since_missing_file_returns_none() -> None:
    runner = FakeRunner(
        responder=lambda a, s: RemoteResult(1, "", "missing") if a[0] == "sh" else None
    )
    d = make_dispatcher(runner)

    out = d.tail_file_since(
        SchedulerHandle("1.cluster", "/ws/1"),
        filename="calc.out",
        byte_offset=25,
    )

    assert out is None


def test_missing_marker_diagnostics_collects_bounded_remote_evidence() -> None:
    long_tail = "x" * (MISSING_MARKER_DIAGNOSTIC_MAX_CHARS + 50)

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv == ["ls", "-la", "/ws/1"]:
            return RemoteResult(0, "top listing\n", "")
        if argv == ["ls", "-la", "/ws/1/_vq"]:
            return RemoteResult(0, "vq listing\n", "")
        if argv[:2] == ["sh", "-c"] and "find /ws/1" in argv[2]:
            return RemoteResult(0, "stdout.log\n_vq/events.jsonl\n", "")
        if argv[:2] == ["sh", "-c"] and "exit-code.3" in argv[2]:
            return RemoteResult(0, "missing\n", "")
        if argv == ["tail", "-n", "120", "/ws/1/stdout.log.3"]:
            return RemoteResult(0, long_tail, "")
        if argv == ["tail", "-n", "120", "/ws/1/stderr.log.3"]:
            return RemoteResult(1, "", "tail: no such file")
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(1, "", "qstat: Unknown Job Id")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    evidence = d.missing_marker_diagnostics(
        SchedulerHandle("1.cluster", "/ws/1", array_size=4), array_index=3
    )

    assert evidence["scheduler_job_id"] == "1.cluster"
    assert evidence["remote_exit_marker"] == "/ws/1/_vq/exit-code.3"
    assert evidence["remote_workspace_listing"] == "top listing\n"
    assert evidence["remote_vq_listing"] == "vq listing\n"
    assert "_vq/events.jsonl" in str(evidence["remote_file_sample"])
    assert str(evidence["remote_stdout_tail"]).startswith("...<truncated")
    assert evidence["remote_stderr_tail_rc"] == 1
    assert evidence["remote_stderr_tail_stderr"] == "tail: no such file"
    assert evidence["qstat_detail_rc"] == 1
    assert evidence["qstat_detail_stderr"] == "qstat: Unknown Job Id"


def test_phase_from_detail_maps_qstat_state() -> None:
    d = make_dispatcher(FakeRunner())
    assert d.phase_from_detail(QstatDetail(raw_state="R")) is SchedulerPhase.RUNNING
    assert d.phase_from_detail(QstatDetail(raw_state="Q")) is SchedulerPhase.PENDING
    assert d.phase_from_detail(QstatDetail(raw_state="C")) is SchedulerPhase.FINISHED
    assert d.phase_from_detail(QstatDetail(raw_state="")) is None


# --------------------------------------------------------------------------- #
# exit_code / final_state / phase_to_state
# --------------------------------------------------------------------------- #


def _marker_responder(value: str, *, rc: int = 0) -> Responder:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "cat":
            return RemoteResult(rc, value, "")
        return None

    return responder


@pytest.mark.parametrize(("text", "expected"), [("0\n", 0), ("1\n", 1), ("-11\n", -11)])
def test_exit_code_from_marker(text: str, expected: int) -> None:
    d = make_dispatcher(FakeRunner(responder=_marker_responder(text)))
    h = SchedulerHandle("1.cluster", "/ws/1")
    assert d.exit_code(h) == expected


def test_exit_code_reads_single_marker_path() -> None:
    runner = FakeRunner(responder=_marker_responder("0\n"))
    d = make_dispatcher(runner)
    d.exit_code(SchedulerHandle("1.cluster", "/ws/1"))
    assert runner.first("cat").argv == ["cat", "/ws/1/_vq/exit-code"]


def test_exit_marker_code_does_not_use_qstat_fallback() -> None:
    detail = "Job Id: 1.cluster\n    exit_status = 0\n"

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "cat":
            return RemoteResult(1, "", "cat: no such file")
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(0, detail, "")
        return None

    runner = FakeRunner(responder=responder)
    d = make_dispatcher(runner)
    assert d.exit_marker_code(SchedulerHandle("1.cluster", "/ws/1")) is None
    assert runner.none_match(lambda a: a[:2] == ["qstat", "-f"])


def test_exit_code_array_reads_per_index_marker_and_no_qstat_fallback() -> None:
    runner = FakeRunner(responder=_marker_responder("0\n"))
    d = make_dispatcher(runner)
    rc = d.exit_code(SchedulerHandle("1[].cluster", "/ws/1", array_size=4), array_index=2)
    assert rc == 0
    # Review note 3: per-index marker, and NO `qstat -f` aggregate fallback.
    assert runner.first("cat").argv == ["cat", "/ws/1/_vq/exit-code.2"]
    assert runner.none_match(lambda a: a[:2] == ["qstat", "-f"])


def test_exit_code_falls_back_to_qstat_f_for_single_job() -> None:
    detail = "Job Id: 1.cluster\n    exit_status = 3\n"

    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "cat":
            return RemoteResult(1, "", "cat: no such file")  # marker missing
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(0, detail, "")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    assert d.exit_code(SchedulerHandle("1.cluster", "/ws/1")) == 3


def test_exit_code_none_when_nothing_available() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "cat":
            return RemoteResult(1, "", "")
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(1, "", "qstat: Unknown Job Id")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    assert d.exit_code(SchedulerHandle("1.cluster", "/ws/1")) is None


def test_exit_code_unparseable_marker_falls_through() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "cat":
            return RemoteResult(0, "garbage\n", "")
        if argv[:2] == ["qstat", "-f"]:
            return RemoteResult(1, "", "")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    assert d.exit_code(SchedulerHandle("1.cluster", "/ws/1")) is None


@pytest.mark.parametrize(
    ("marker", "rc", "walltime", "expected"),
    [
        ("0\n", 0, False, JobState.COMPLETED),
        ("1\n", 0, False, JobState.FAILED),
        (None, 1, False, JobState.ABORTED_BY_QUEUE),  # marker missing
        ("0\n", 0, True, JobState.TIME_EXCEEDED),  # walltime overrides rc
    ],
)
def test_final_state(
    marker: str | None, rc: int, walltime: bool, expected: JobState
) -> None:
    if marker is None:
        responder = _marker_responder("", rc=1)  # cat fails -> no rc

        def resp2(argv: list[str], stdin: str | None) -> RemoteResult | None:
            if argv[:2] == ["qstat", "-f"]:
                return RemoteResult(1, "", "")
            return responder(argv, stdin)

        runner = FakeRunner(responder=resp2)
    else:
        runner = FakeRunner(responder=_marker_responder(marker))
    d = make_dispatcher(runner)
    h = SchedulerHandle("1.cluster", "/ws/1")
    assert d.final_state(h, walltime_exceeded=walltime) == expected


def test_phase_to_state_table() -> None:
    f = SchedulerDispatcher.phase_to_state
    assert f(SchedulerPhase.PENDING, None) == JobState.PENDING
    assert f(SchedulerPhase.RUNNING, None) == JobState.RUNNING
    assert f(SchedulerPhase.FINISHED, 0) == JobState.COMPLETED
    assert f(SchedulerPhase.FINISHED, 7) == JobState.FAILED
    assert f(SchedulerPhase.FINISHED, None) == JobState.ABORTED_BY_QUEUE
    assert f(SchedulerPhase.FINISHED, 0, walltime_exceeded=True) == JobState.TIME_EXCEEDED


# --------------------------------------------------------------------------- #
# cancel
# --------------------------------------------------------------------------- #


def test_cancel_issues_qdel() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    d.cancel(SchedulerHandle("9.cluster", "/ws/9"))
    assert runner.first("qdel").argv == ["qdel", "9.cluster"]


def test_slurm_cancel_issues_scancel() -> None:
    runner = FakeRunner()
    d = make_slurm_dispatcher(runner)
    d.cancel(SchedulerHandle("9", "/ws/9"))
    assert runner.first("scancel").argv == ["scancel", "9"]


# --------------------------------------------------------------------------- #
# scheduler_dispatcher_for (per-host factory the daemon uses)
# --------------------------------------------------------------------------- #


def test_scheduler_dispatcher_for_builds_from_host_config() -> None:
    cfg = HostConfig(
        ssh="cluster",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/home/USER",
        submit_extra=["-q", "compute", "-A", "proj1"],
        node_scratch_dir="/tmp1/$USER",
        scheduler_prologue=["module purge"],
        scheduler_epilogue=["rm -f scratch.tmp"],
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                prologue=["source /home/USER/orca-env.sh"],
                command_wrapper=["/home/USER/bin/orcasub"],
            )
        },
        scheduler_driver="driver",
    )
    d = scheduler_dispatcher_for(cfg)
    assert isinstance(d.dialect, TorqueDialect)
    assert d.scratch_root == "/home/USER"
    assert d.node_scratch_dir == "/tmp1/$USER"
    assert d.scheduler_prologue == ["module purge"]
    assert d.scheduler_epilogue == ["rm -f scratch.tmp"]
    assert d.scheduler_program_hooks["orca"].prologue == [
        "source /home/USER/orca-env.sh"
    ]
    assert d.scheduler_program_hooks["orca"].command_wrapper == [
        "/home/USER/bin/orcasub"
    ]
    # submit_extra was parsed into the single queue/account render path.
    assert d._queue == "compute"  # noqa: SLF001 - asserting the wired-through config
    assert d._account == "proj1"  # noqa: SLF001


def test_scheduler_dispatcher_for_builds_slurm_from_host_config() -> None:
    cfg = HostConfig(
        ssh="host_c",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scratch_root="/workspace/USER",
        submit_extra=["--account", "<group-account>", "--partition", "intelsr_devel"],
        node_scratch_dir="/tmp/$USER",
        scheduler_prologue=["module purge"],
        scheduler_program_hooks={
            "orca": SchedulerProgramHooks(
                command_wrapper=["/opt/vq/wrappers/orca-slurm"]
            )
        },
        scheduler_driver="driver",
    )
    d = scheduler_dispatcher_for(cfg)
    assert isinstance(d.dialect, SlurmDialect)
    assert d.scratch_root == "/workspace/USER"
    assert d.node_scratch_dir == "/tmp/$USER"
    assert d._queue == "intelsr_devel"  # noqa: SLF001
    assert d._account == "<group-account>"  # noqa: SLF001
    assert d.scheduler_program_hooks["orca"].command_wrapper == [
        "/opt/vq/wrappers/orca-slurm"
    ]


def test_scheduler_dispatcher_for_rejects_non_scheduler_host() -> None:
    cfg = HostConfig(ssh="laptop")  # scheduler defaults to "local"
    with pytest.raises(SchedulerError, match="not configured as a scheduler host"):
        scheduler_dispatcher_for(cfg)


def test_cancel_tolerates_already_gone() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qdel":
            return RemoteResult(170, "", "qdel: Unknown Job Id 9.cluster")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    # Must not raise — qdel on a finished job is a no-op from the daemon's view.
    d.cancel(SchedulerHandle("9.cluster", "/ws/9"))


def test_cancel_rejects_nonzero_permission_error_as_unconfirmed() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qdel":
            return RemoteResult(1, "", "qdel: Unauthorized Request")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))

    with pytest.raises(SchedulerError, match="could not be confirmed"):
        d.cancel(SchedulerHandle("9.cluster", "/ws/9"))


# --------------------------------------------------------------------------- #
# hold / release
# --------------------------------------------------------------------------- #


def test_hold_issues_qhold() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    d.hold(SchedulerHandle("9.cluster", "/ws/9"))
    assert runner.first("qhold").argv == ["qhold", "9.cluster"]


def test_release_issues_qrls() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    d.release(SchedulerHandle("9.cluster", "/ws/9"))
    assert runner.first("qrls").argv == ["qrls", "9.cluster"]


def test_hold_failure_raises() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qhold":
            return RemoteResult(1, "", "qhold: Unknown Job Id 9.cluster")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerError, match="qhold failed"):
        d.hold(SchedulerHandle("9.cluster", "/ws/9"))


def test_release_failure_raises() -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qrls":
            return RemoteResult(1, "", "qrls: Unknown Job Id 9.cluster")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerError, match="qrls failed"):
        d.release(SchedulerHandle("9.cluster", "/ws/9"))


# --------------------------------------------------------------------------- #
# fetch_results (Increment 3 — preserve ALL output files, point 5)
# --------------------------------------------------------------------------- #


def test_fetch_results_brings_whole_workspace(tmp_path: Path) -> None:
    runner = FakeRunner()
    resource_usage = {
        "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
        "status": "ok",
        "wall_seconds": 4.25,
        "active_cpu_seconds": 7.5,
        "peak_rss_kb": 8192,
    }
    # The finished workspace holds output + auxiliary files + the exit-marker.
    runner.result_files = {
        "job.out": "energy = -76.0\n",
        "fort.9": "wavefunction",
        "_vq/exit-code": "0\n",
        "_vq/resource-usage.json": json.dumps(resource_usage) + "\n",
    }
    d = make_dispatcher(runner)
    ws = f"{SCRATCH}/.vibeqc-cluster/jobs/job1"
    local = tmp_path / "fetched"
    d.fetch_results(SchedulerHandle("12345.cluster", ws), local)

    # Every file came back, content intact (preserve-everything contract).
    assert (local / "job.out").read_text() == "energy = -76.0\n"
    assert (local / "fort.9").read_text() == "wavefunction"
    assert (local / "_vq" / "exit-code").read_text() == "0\n"
    assert json.loads((local / "_vq" / "resource-usage.json").read_text()) == (
        resource_usage
    )
    # Command sequence: tar the whole workspace, scp it down, rm the remote tar.
    remote_tar, local_tar = runner.downloads[0]
    assert remote_tar.startswith(f"{ws}.result-") and remote_tar.endswith(".tar")
    assert local_tar.parent == local
    assert ["tar", "-cf", remote_tar, "-C", ws, "."] in runner.argvs()
    assert ["rm", "-f", remote_tar] in runner.argvs()
    # The local scratch tarball was cleaned up.
    assert not list(local.glob(".vq-fetch*.tar"))


def test_interleaved_fetches_do_not_share_or_delete_another_transfer_archive(
    tmp_path: Path,
) -> None:
    handle = SchedulerHandle("1.cluster", "/ws/1")
    local = tmp_path / "out"

    class InterleavedRunner(FakeRunner):
        def download_file(self, remote: str, destination: Path) -> None:
            super().download_file(remote, destination)
            if len(self.downloads) == 1:
                dispatcher.fetch_results(handle, local)

    runner = InterleavedRunner(result_files={"result.txt": "preserved\n"})
    dispatcher = make_dispatcher(runner)
    dispatcher.fetch_results(handle, local)

    assert (local / "result.txt").read_bytes() == b"preserved\n"
    assert len({remote for remote, _ in runner.downloads}) == 2
    assert len({path for _, path in runner.downloads}) == 2
    for remote, path in runner.downloads:
        assert ["rm", "-f", remote] in runner.argvs()
        assert not path.exists()


def test_fetch_results_overlays_hash_sealed_workspace_with_terminal_outputs(
    tmp_path: Path,
) -> None:
    """Scheduler results must replace sealed inputs without losing outputs.

    Production payloads are content-hashed and materialized read-only. Before
    this regression fix, extracting the result tar directly over those files
    failed with EACCES on the first input, so stdout/stderr and the scientific
    result never reached the driver even though the scheduler job exited zero.
    """
    local = tmp_path / "fetched"
    (local / "_vq").mkdir(parents=True)
    input_path = local / "reference.inp"
    input_path.write_text("! RHF def2-TZVP\n")
    input_path.chmod(0o444)
    (local / ".sealed").write_text("sha256 payload\n")
    (local / ".sealed").chmod(0o444)
    (local / "_vq" / "events.jsonl").write_text(
        '{"kind":"dispatched","scheduler_job_id":"12345.cluster"}\n'
    )

    runner = FakeRunner()
    runner.result_files = {
        "reference.inp": "! RHF def2-TZVP\n",
        ".sealed": "sha256 payload\n",
        "stdout.log": "ORCA TERMINATED NORMALLY\n",
        "stderr.log": "",
        "reference.out": "FINAL SINGLE POINT ENERGY -76.0267607374\n",
        "reference.gbw": "wavefunction",
        "_vq/events.jsonl": '{"kind":"submitted"}\n',
        "_vq/exit-code": "0\n",
    }
    dispatcher = make_dispatcher(runner)

    dispatcher.fetch_results(
        SchedulerHandle("12345.cluster", "/ws/job1"),
        local,
    )

    assert input_path.read_text() == "! RHF def2-TZVP\n"
    assert (local / "stdout.log").read_text() == "ORCA TERMINATED NORMALLY\n"
    assert (local / "stderr.log").read_text() == ""
    assert (
        local / "reference.out"
    ).read_text() == "FINAL SINGLE POINT ENERGY -76.0267607374\n"
    assert (local / "reference.gbw").read_text() == "wavefunction"
    assert (local / "_vq" / "exit-code").read_text() == "0\n"
    # The scheduler's staged copy predates driver-side dispatch/status events.
    assert (local / "_vq" / "events.jsonl").read_text() == (
        '{"kind":"dispatched","scheduler_job_id":"12345.cluster"}\n'
    )


def test_fetch_results_creates_local_dir(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.result_files = {"a.out": "x"}
    d = make_dispatcher(runner)
    local = tmp_path / "does" / "not" / "exist"
    d.fetch_results(SchedulerHandle("1.cluster", "/ws/1"), local)
    assert (local / "a.out").read_text() == "x"


def _read_scheduler_sample(local: Path) -> dict[str, object]:
    lines = (local / "_vq" / "samples.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def test_slurm_fetch_projects_terminal_sacct_cpu_and_maxrss(tmp_path: Path) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|00:01:30|1048576K|60\n"
                    "123.batch|COMPLETED|00:01:30|2G|60\n"
                ),
                "PRIVATE-SCHEDULER-STDERR",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert {
        "ts",
        "elapsed_seconds",
        "rss_mb",
        "cpu_percent",
        "cpu_time_seconds",
        "cpu_time_source",
        "cgroup_lookup",
        "cgroup_path",
        "sample_pid",
        "sample_pgid",
    } <= sample.keys()
    assert sample["rss_mb"] == 2048.0
    assert sample["cpu_time_seconds"] == 90.0
    assert sample["cpu_time_source"] == "slurm-sacct"
    assert sample["cpu_percent"] == 150.0
    assert sample["elapsed_seconds"] == 60.0
    assert sample["scheduler_accounting_status"] == "ok"
    assert "PRIVATE-SCHEDULER-STDERR" not in json.dumps(sample)
    accounting_call = runner.first("sacct").argv
    assert "--array" in accounting_call
    assert "-X" not in accounting_call  # MaxRSS lives on job-step rows.
    assert "--format=JobID,State,TotalCPU,MaxRSS,ElapsedRaw" in accounting_call
    assert not any("JobIDRaw" in token for token in accounting_call)


@pytest.mark.parametrize(
    ("max_rss", "expected_mb"),
    [("1536K", 1.5), ("512M", 512.0), ("2G", 2048.0), ("0.5T", 524288.0)],
)
def test_slurm_fetch_converts_sacct_memory_units(
    tmp_path: Path,
    max_rss: str,
    expected_mb: float,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, f"123|COMPLETED|1.0|{max_rss}|2\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / max_rss.replace(".", "_")
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )
    assert _read_scheduler_sample(local)["rss_mb"] == expected_mb


def test_slurm_fetch_aggregates_arrays_without_double_counting_steps(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|||20\n"
                    "123_1|COMPLETED|00:00:10||10\n"
                    "123_1.batch|COMPLETED|00:00:08|512M|10\n"
                    "123_2|COMPLETED|00:00:20||20\n"
                    "123_2.batch|COMPLETED|00:00:15|1G|20\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )
    sample = _read_scheduler_sample(local)
    assert sample["cpu_time_seconds"] == 23.0
    assert sample["rss_mb"] == 1024.0
    assert sample["elapsed_seconds"] == 20.0
    assert sample["cpu_percent"] == 76.7
    assert sample["scheduler_accounting_groups"] == 2


@pytest.mark.parametrize("missing_metric", ["cpu", "rss"])
def test_slurm_fetch_does_not_publish_partial_array_metrics_as_authoritative(
    tmp_path: Path,
    missing_metric: str,
) -> None:
    first = "123_1|COMPLETED|10|1G|10"
    second = (
        "123_2|COMPLETED||2G|20"
        if missing_metric == "cpu"
        else "123_2|COMPLETED|20||20"
    )

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, f"{first}\n{second}\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / missing_metric
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["scheduler_accounting_reason"] == "metrics_missing"
    assert sample[f"{'cpu_time_seconds' if missing_metric == 'cpu' else 'rss_mb'}"] is None


def test_slurm_fetch_waits_for_every_native_array_task_group(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, "123_0|COMPLETED|10|1G|10\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "incomplete-native-array"

    complete = make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123", array_size=2),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert complete is False
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "accounting_lag"
    assert sample["scheduler_accounting_groups"] == 1


def test_slurm_fetch_requires_maxrss_from_every_observed_step(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|||20\n"
                    "123.batch|COMPLETED|10|1G|20\n"
                    "123.extern|COMPLETED|5||20\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "missing-step-rss"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["scheduler_accounting_reason"] == "metrics_missing"
    assert sample["cpu_time_seconds"] == 15.0
    assert sample["rss_mb"] is None


def test_slurm_fetch_does_not_fallback_to_allocation_cpu_for_incomplete_steps(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|50|2G|20\n"
                    "123.batch|COMPLETED|5|1G|20\n"
                    "123.extern|COMPLETED||512M|20\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "incomplete-step-cpu"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["scheduler_accounting_reason"] == "metrics_missing"
    assert sample["cpu_time_seconds"] is None
    assert sample["rss_mb"] == 2048.0


def test_slurm_fetch_does_not_derive_cpu_percent_from_partial_step_elapsed(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|||\n"
                    "123.batch|COMPLETED|5|1G|10\n"
                    "123.extern|COMPLETED|5|512M|\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "incomplete-step-elapsed"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["scheduler_accounting_reason"] == "metrics_missing"
    assert sample["cpu_time_seconds"] == 10.0
    assert sample["rss_mb"] == 1024.0
    assert sample["elapsed_seconds"] is None
    assert sample["cpu_percent"] is None


def test_slurm_fetch_keeps_larger_allocation_maxrss_with_complete_steps(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|15|2G|20\n"
                    "123.batch|COMPLETED|10|1G|20\n"
                    "123.extern|COMPLETED|5|512M|20\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "allocation-peak"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "ok"
    assert sample["rss_mb"] == 2048.0


def test_live_slurm_fetch_is_terminal_fenced_from_accounting(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "live"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
    )

    assert runner.none_match(lambda argv: bool(argv) and argv[0] == "sacct")
    assert not (local / "_vq" / "samples.jsonl").exists()


def test_slurm_fetch_prefers_valid_gnu_time_receipt(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.result_files = {
        "_vq/exit-code": "0\n",
        "_vq/resource-usage.json": json.dumps(
            {
                "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
                "status": "ok",
                "collector": "gnu-time",
                "scope": "effective-command",
                "command_status": "succeeded",
                "command_exit_code": 0,
                "wall_seconds": 20.0,
                "user_cpu_seconds": 12.0,
                "system_cpu_seconds": 3.0,
                "active_cpu_seconds": 15.0,
                "peak_rss_kb": 2048,
                "peak_rss_mb": 2.0,
            }
        ),
    }
    local = tmp_path / "receipt"

    complete = make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert complete is True
    assert sample["cpu_time_source"] == "scheduler-gnu-time"
    assert sample["cpu_time_seconds"] == 15.0
    assert sample["rss_mb"] == 2.0
    assert sample["cpu_percent"] == 75.0
    assert runner.none_match(lambda argv: bool(argv) and argv[0] == "sacct")


def test_overflowing_gnu_time_array_aggregate_falls_back_to_sacct(
    tmp_path: Path,
) -> None:
    def receipt() -> str:
        return json.dumps(
            {
                "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
                "status": "ok",
                "collector": "gnu-time",
                "scope": "effective-command",
                "command_status": "succeeded",
                "command_exit_code": 0,
                "wall_seconds": 1.0,
                "user_cpu_seconds": 1e308,
                "system_cpu_seconds": 0.0,
                "active_cpu_seconds": 1e308,
                "peak_rss_kb": 1024,
                "peak_rss_mb": 1.0,
            }
        )

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123_0|COMPLETED|1|1M|1\n"
                    "123_1|COMPLETED|1|1M|1\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {
        "_vq/exit-code": "0\n",
        "_vq/resource-usage.json.0": receipt(),
        "_vq/resource-usage.json.1": receipt(),
    }
    local = tmp_path / "overflow-gnu-time"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123", array_size=2),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["cpu_time_source"] == "slurm-sacct"
    assert sample["cpu_time_seconds"] == 2.0
    assert runner.first("sacct")


def test_oversized_gnu_time_integer_falls_back_to_sacct(tmp_path: Path) -> None:
    receipt = json.dumps(
        {
            "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
            "status": "ok",
            "collector": "gnu-time",
            "scope": "effective-command",
            "command_status": "succeeded",
            "command_exit_code": 0,
            "wall_seconds": 1,
            "user_cpu_seconds": 10**400,
            "system_cpu_seconds": 0,
            "active_cpu_seconds": 10**400,
            "peak_rss_kb": 1024,
            "peak_rss_mb": 1,
        }
    )

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, "123|COMPLETED|1|1M|1\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {
        "_vq/exit-code": "0\n",
        "_vq/resource-usage.json": receipt,
    }
    local = tmp_path / "oversized-gnu-time"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["cpu_time_source"] == "slurm-sacct"
    assert sample["cpu_time_seconds"] == 1.0


def test_slurm_fetch_rejects_nonfinite_derived_accounting(tmp_path: Path) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, "123|COMPLETED|1e308|1M|1e-308\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "nonfinite"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample_text = (local / "_vq" / "samples.jsonl").read_text()
    sample = json.loads(sample_text)
    assert "Infinity" not in sample_text
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "malformed_output"


@pytest.mark.parametrize("field", ["cpu", "elapsed"])
def test_slurm_fetch_rejects_overflowing_duration_fields(
    tmp_path: Path,
    field: str,
) -> None:
    huge = f"{'9' * 400}-00:00:00"
    cpu = huge if field == "cpu" else "1"
    elapsed = huge if field == "elapsed" else "1"

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                f"123|COMPLETED|{cpu}|1M|{elapsed}\n",
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / field

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "malformed_output"


def test_slurm_fetch_rejects_overflowing_array_aggregate(tmp_path: Path) -> None:
    huge = "9" * 308

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    f"123_0|COMPLETED|{huge}|1M|1\n"
                    f"123_1|COMPLETED|{huge}|1M|1\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "overflow-aggregate"

    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123", array_size=2),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "malformed_output"


def test_scheduler_sample_publish_does_not_follow_predictable_temp_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events_path = tmp_path / "events.jsonl"
    events_path.write_text("durable-event\n")
    legacy_temp = tmp_path / f".samples.jsonl.tmp.{os.getpid()}"
    legacy_temp.symlink_to(events_path)
    target = tmp_path / "samples.jsonl"

    scheduler_dispatch._write_scheduler_sample(
        target,
        scheduler_dispatch._unavailable_scheduler_sample("123", "test"),
    )

    assert events_path.read_text() == "durable-event\n"
    assert not target.is_symlink()
    assert json.loads(target.read_text())["scheduler_accounting_reason"] == "test"


def test_existing_sample_probe_rejects_symlink_and_fifo_without_blocking(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private-events.jsonl"
    private.write_text('{"secret":"must-not-be-read"}\n')
    symlink = tmp_path / "samples-link.jsonl"
    symlink.symlink_to(private)
    fifo = tmp_path / "samples-fifo.jsonl"
    os.mkfifo(fifo)

    started = time.monotonic()
    assert scheduler_dispatch._existing_sample_disposition(symlink, "123") == "preserve"
    assert scheduler_dispatch._existing_sample_disposition(fifo, "123") == "preserve"
    assert time.monotonic() - started < 1.0


def test_slurm_fetch_does_not_sum_incomplete_step_cpu(tmp_path: Path) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED||1G|10\n"
                    "123.batch|COMPLETED|5|1G|10\n"
                    "123.extern|COMPLETED||1M|10\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["cpu_time_seconds"] is None


def test_scheduler_sample_persistence_error_is_best_effort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_write(_path: Path, _sample: dict[str, object]) -> None:
        raise OSError("private filesystem detail")

    monkeypatch.setattr(scheduler_dispatch, "_write_scheduler_sample", fail_write)
    runner = FakeRunner()
    runner.result_files = {
        "_vq/exit-code": "0\n",
        "_vq/resource-usage.json": json.dumps(
            {
                "schema": scheduler_dispatch.RESOURCE_USAGE_SCHEMA,
                "status": "ok",
                "collector": "gnu-time",
                "scope": "effective-command",
                "command_status": "succeeded",
                "command_exit_code": 0,
                "wall_seconds": 1.0,
                "user_cpu_seconds": 1.0,
                "system_cpu_seconds": 0.0,
                "active_cpu_seconds": 1.0,
                "peak_rss_kb": 1024,
                "peak_rss_mb": 1.0,
            }
        ),
    }
    local = tmp_path / "out"

    complete = make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    assert complete is False
    assert (local / "_vq" / "exit-code").read_text() == "0\n"


def test_slurm_accounting_uses_owned_bounded_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run_remote_shell(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.update(kwargs)
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        scheduler_dispatch.transport,
        "run_remote_shell",
        fake_run_remote_shell,
    )
    host_cfg = HostConfig(
        ssh="slurm",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scratch_root="/scratch/vq",
        scheduler_driver="driver",
    )

    SshRemoteRunner(host_cfg).run(["sacct", "--noheader"], check=False)

    assert captured["owned_process_group"] is True
    assert captured["max_stdout_bytes"] == (
        scheduler_dispatch.SCHEDULER_ACCOUNTING_MAX_BYTES + 1
    )
    assert captured["max_stderr_bytes"] == 4096


def test_slurm_fetch_prefers_step_cpu_when_allocation_reports_zero(
    tmp_path: Path,
) -> None:
    """Without ``sacct -X``, allocation utilization can be a placeholder 0."""

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                (
                    "123|COMPLETED|0|1048576K|100\n"
                    "123.batch|COMPLETED|50|1G|100\n"
                ),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["cpu_time_seconds"] == 50.0
    assert sample["cpu_percent"] == 50.0


def test_slurm_fetch_records_partial_accounting_without_inventing_metrics(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, "123|COMPLETED|||12\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )
    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "partial"
    assert sample["scheduler_accounting_reason"] == "metrics_missing"
    assert sample["cpu_time_seconds"] is None
    assert sample["cpu_percent"] is None
    assert sample["rss_mb"] is None
    assert sample["elapsed_seconds"] == 12.0


@pytest.mark.parametrize(
    ("returncode", "stdout", "reason"),
    [
        (1, "", "command_failed"),
        (0, "", "accounting_lag"),
        (0, "not|a|valid|row\n", "malformed_output"),
    ],
)
def test_slurm_fetch_records_bounded_accounting_unavailability(
    tmp_path: Path,
    returncode: int,
    stdout: str,
    reason: str,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(returncode, stdout, "PRIVATE-NATIVE-OUTPUT")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / reason
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )
    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == reason
    assert sample["cpu_time_seconds"] is None
    assert sample["rss_mb"] is None
    assert "PRIVATE-NATIVE-OUTPUT" not in json.dumps(sample)


@pytest.mark.parametrize(
    ("stdout", "reason"),
    [
        (
            "123|COMPLETED|1|1M|1\n123|COMPLETED|1|1M|1\n",
            "malformed_output",
        ),
        (
            "123|COMPLETED|1|1M|1\n123.batch|RUNNING|1|1M|1\n",
            "accounting_lag",
        ),
    ],
)
def test_slurm_fetch_rejects_ambiguous_or_lagging_accounting_rows(
    tmp_path: Path,
    stdout: str,
    reason: str,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(0, stdout, "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == reason


def test_slurm_fetch_keeps_results_when_accounting_query_is_unavailable(
    tmp_path: Path,
) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            raise SchedulerError("private host and scheduler detail")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n", "result.dat": "kept\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    assert (local / "result.dat").read_text() == "kept\n"
    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "query_failed"
    assert "private" not in json.dumps(sample)


def test_slurm_fetch_bounds_accounting_output_before_parsing(tmp_path: Path) -> None:
    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "sacct":
            return RemoteResult(
                0,
                "123|COMPLETED|1|1M|1\n"
                * (scheduler_dispatch.SCHEDULER_ACCOUNTING_MAX_ROWS + 1),
                "",
            )
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    local = tmp_path / "out"
    make_slurm_dispatcher(runner).fetch_results(
        SchedulerHandle("123", "/ws/123"),
        local,
        terminal=True,
    )

    sample = _read_scheduler_sample(local)
    assert sample["scheduler_accounting_status"] == "unavailable"
    assert sample["scheduler_accounting_reason"] == "malformed_output"


def test_slurm_fetch_is_idempotent_and_preserves_process_tree_samples(
    tmp_path: Path,
) -> None:
    calls = 0

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        nonlocal calls
        if argv and argv[0] == "sacct":
            calls += 1
            return RemoteResult(0, "123|COMPLETED|5|256M|10\n", "")
        return None

    runner = FakeRunner(responder=responder)
    runner.result_files = {"_vq/exit-code": "0\n"}
    dispatcher = make_slurm_dispatcher(runner)
    handle = SchedulerHandle("123", "/ws/123")
    local = tmp_path / "idempotent"
    dispatcher.fetch_results(handle, local, terminal=True)
    first = (local / "_vq" / "samples.jsonl").read_bytes()
    dispatcher.fetch_results(handle, local, terminal=True)
    assert (local / "_vq" / "samples.jsonl").read_bytes() == first
    assert calls == 1

    process_sample = (
        '{"ts":"2026-08-12T00:00:00+00:00","rss_mb":7,'
        '"cpu_percent":50,"cpu_time_source":"pgid"}\n'
    )
    runner.result_files["_vq/samples.jsonl"] = process_sample
    other = tmp_path / "process-tree"
    dispatcher.fetch_results(handle, other, terminal=True)
    assert (other / "_vq" / "samples.jsonl").read_text() == process_sample
    assert calls == 1


def test_fetch_results_raises_on_tar_failure(tmp_path: Path) -> None:
    def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
        if argv[:2] == ["tar", "-cf"]:
            return RemoteResult(2, "", "tar: cannot create archive")
        return None

    d = make_dispatcher(FakeRunner(responder=responder))
    with pytest.raises(SchedulerError, match="archive remote workspace"):
        d.fetch_results(SchedulerHandle("1.cluster", "/ws/1"), tmp_path / "out")


def test_fetch_results_raises_on_download_failure_but_cleans_remote(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(fail_download=True)
    d = make_dispatcher(runner)
    with pytest.raises(SchedulerError, match="download results"):
        d.fetch_results(SchedulerHandle("1.cluster", "/ws/1"), tmp_path / "out")
    # Even on download failure the remote tarball cleanup is still attempted.
    assert ["rm", "-f", runner.downloads[0][0]] in runner.argvs()


def test_fetch_results_skips_absolute_symlinks_and_keeps_real_files(
    tmp_path: Path,
) -> None:
    """Regression (2026-07-16 daemon wedge): a killed job's stale
    ``third_party/*/install`` build-tree symlinks point at an absolute
    node-local scratch path. ``filter="data"`` raised ``AbsoluteLinkError`` on
    the first one, aborting the whole extraction — and, because that is not a
    ``SchedulerError``, it escaped ``fetch_results`` and wedged the daemon's
    reconcile-then-dispatch loop for the entire host. fetch_results must SKIP
    the unsafe members and still bring every real file home."""
    runner = FakeRunner()
    runner.result_files = {
        "job.out": "energy = -76.0\n",
        "fort.9": "wavefunction",
        "_vq/exit-code": "1\n",
    }
    runner.result_symlinks = {
        "source/third_party/fftw/install": (
            "/tmp/USER/vibeqc-dev-XYZ/bundle/src/third_party/fftw/install"
        ),
        "source/third_party/libxc/install": (
            "/tmp/USER/vibeqc-dev-XYZ/bundle/src/third_party/libxc/install"
        ),
    }
    d = make_dispatcher(runner)
    ws = f"{SCRATCH}/.vibeqc-cluster/jobs/killed1"
    local = tmp_path / "fetched"

    # Must NOT raise (pre-fix this raised tarfile.AbsoluteLinkError).
    d.fetch_results(SchedulerHandle("999.cluster", ws), local)

    # Every real file came home intact (preserve-everything contract).
    assert (local / "job.out").read_text() == "energy = -76.0\n"
    assert (local / "fort.9").read_text() == "wavefunction"
    assert (local / "_vq" / "exit-code").read_text() == "1\n"
    # The unsafe absolute symlinks were dropped — never written to disk.
    fftw = local / "source" / "third_party" / "fftw" / "install"
    libxc = local / "source" / "third_party" / "libxc" / "install"
    assert not fftw.is_symlink() and not fftw.exists()
    assert not libxc.is_symlink() and not libxc.exists()
    # Local scratch tarball cleaned up as usual.
    assert not list(local.glob(".vq-fetch*.tar"))


def test_fetch_results_wraps_extraction_error_as_scheduler_error(
    tmp_path: Path,
) -> None:
    """A corrupt / unreadable tarball must surface as SchedulerError so the
    reconcile loop parks THIS job as fetch_failed, rather than a raw
    ``tarfile.TarError`` escaping and aborting the daemon's whole iteration."""
    runner = FakeRunner(corrupt_download=True)
    d = make_dispatcher(runner)
    with pytest.raises(SchedulerError, match="extract results"):
        d.fetch_results(SchedulerHandle("1.cluster", "/ws/1"), tmp_path / "out")
    # The remote tarball cleanup is still attempted even on extraction failure.
    assert ["rm", "-f", runner.downloads[0][0]] in runner.argvs()
    # And the local scratch tarball is not left behind.
    assert not list((tmp_path / "out").glob(".vq-fetch*.tar"))
    # Extraction is staged, so corrupt archives cannot publish partial results.
    assert list((tmp_path / "out").iterdir()) == []


def test_fetch_results_merge_failure_is_retryable(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.result_files = {
        "reference.out": "ORCA TERMINATED NORMALLY\n",
        "stdout.log": "full verbose output\n",
        "_vq/exit-code": "0\n",
    }
    dispatcher = make_dispatcher(runner)
    local = tmp_path / "out"
    (local / "reference.out").mkdir(parents=True)
    handle = SchedulerHandle("1.cluster", "/ws/1")

    with pytest.raises(SchedulerError, match="conflicts with local directory"):
        dispatcher.fetch_results(handle, local)

    # The remote workspace was not deleted; a later pass recreates its tar and
    # can complete after the local type conflict is resolved.
    assert runner.none_match(lambda argv: argv == ["rm", "-rf", "/ws/1"])
    (local / "reference.out").rmdir()
    dispatcher.fetch_results(handle, local)

    assert (local / "reference.out").read_text() == "ORCA TERMINATED NORMALLY\n"
    assert (local / "stdout.log").read_text() == "full verbose output\n"
    assert (local / "_vq" / "exit-code").read_text() == "0\n"


def test_cleanup_remote_workspace_removes_configured_job_dir() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    ws = d.remote_workspace("job1")

    d.cleanup_remote_workspace(SchedulerHandle("12345.cluster", ws))

    assert ["rm", "-rf", "--", ws, f"{ws}.submit-once.json"] in runner.argvs()


def test_cleanup_remote_workspace_removes_only_workspace_and_exact_receipt() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    ws = d.remote_workspace("job1")

    d.cleanup_remote_workspace(SchedulerHandle("12345.cluster", ws))

    cleanup = runner.argvs()[-1]
    assert cleanup == ["rm", "-rf", "--", ws, f"{ws}.submit-once.json"]
    assert all("*" not in argument for argument in cleanup)


def test_cleanup_remote_workspace_refuses_outside_job_root() -> None:
    d = make_dispatcher(FakeRunner())

    with pytest.raises(SchedulerError, match="outside vq job root"):
        d.cleanup_remote_workspace(
            SchedulerHandle("12345.cluster", "/tmp/not-a-vq-job")
        )


def test_cleanup_remote_workspace_refuses_parent_traversal_inside_job_root() -> None:
    runner = FakeRunner()
    d = make_dispatcher(runner)
    escaped = f"{d.remote_workspace('job1')}/../victim"

    with pytest.raises(SchedulerError, match="outside vq job root"):
        d.cleanup_remote_workspace(SchedulerHandle("12345.cluster", escaped))

    assert runner.calls == []


# --------------------------------------------------------------------------- #
# SshRemoteRunner transport hardening
# --------------------------------------------------------------------------- #


def test_ssh_runner_treats_exit_255_as_scheduler_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_remote_shell(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["ssh"], returncode=255, stdout="", stderr="banner exchange timeout"
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    with pytest.raises(SchedulerError, match="ssh transport failed"):
        runner.run(["qstat", "123.cluster"], check=False)


def test_ssh_submit_once_runs_mutation_and_receipt_in_one_remote_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_run_remote_shell(
        *args: object,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        seen.append((args, dict(kwargs)))
        return subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="12345.cluster\n",
            stderr="",
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["qsub", "/remote/job.pbs"],
        receipt_path="/remote/job.submit-once.json",
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace="/remote/job",
        array_size=None,
    )

    assert result == RemoteResult(0, "12345.cluster\n", "")
    assert len(seen) == 1
    args, kwargs = seen[0]
    assert args[1:3] == ("bash", "-c")
    wrapper = args[3]
    assert isinstance(wrapper, str)
    assert "vq.scheduler-submit-once.v1" in wrapper
    assert "head -c 65536" in wrapper
    assert "output_overflow" in wrapper
    assert "mktemp" not in wrapper
    assert "__VQ_STATUS__" in wrapper
    assert 'sync -f "$(dirname -- "$receipt")"' in wrapper
    assert args[-2:] == ("qsub", "/remote/job.pbs")
    assert kwargs["retry_transient"] == 0
    assert kwargs["owned_process_group"] is True
    assert kwargs["max_stdout_bytes"] == (
        scheduler_dispatch.SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES + 1
    )
    assert kwargs["max_stderr_bytes"] == (
        scheduler_dispatch.SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES + 1
    )


def test_ssh_submit_once_bounds_remote_temp_output_before_reply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - executes generated test shell locally
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    with pytest.raises(scheduler_dispatch.SchedulerRemoteOutcomeUnknown):
        runner.submit_once(
            ["sh", "-c", "head -c 70000 /dev/zero | tr '\\0' x"],
            receipt_path=str(receipt),
            vq_job_id="abc123",
            dialect_name="torque",
            remote_workspace=str(workspace),
            array_size=None,
        )

    assert json.loads(receipt.read_text())["status"] == "outcome_unknown"


def test_ssh_submit_once_overflow_never_accepts_prefix_scheduler_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - executes generated test shell locally
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    with pytest.raises(
        scheduler_dispatch.SchedulerRemoteOutcomeUnknown,
        match="outcome is unknown",
    ):
        runner.submit_once(
            [
                "sh",
                "-c",
                (
                    "printf '18109.host_f\\n'; "
                    "head -c 70000 /dev/zero | tr '\\0' x; "
                    "printf '\\n18110.host_f\\n'"
                ),
            ],
            receipt_path=str(receipt),
            vq_job_id="abc123",
            dialect_name="torque",
            remote_workspace=str(workspace),
            array_size=None,
        )

    payload = json.loads(receipt.read_text())
    assert payload["status"] == "outcome_unknown"
    assert payload["scheduler_job_id"] is None


def test_ssh_submit_once_uses_no_reopenable_filesystem_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")
    sentinel = tmp_path / "scheduler-mutated"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_mktemp = fake_bin / "mktemp"
    fake_mktemp.write_text("#!/bin/sh\nexit 99\n")
    fake_mktemp.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ['PATH']}")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - generated shell under test
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["sh", "-c", f": > {shlex.quote(str(sentinel))}"],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace=str(workspace),
        array_size=None,
    )

    assert result.returncode == 0
    assert sentinel.exists()
    assert json.loads(receipt.read_text())["status"] == "outcome_unknown"
    assert list(tmp_path.glob("workspace.submit-once.json.capture.*")) == []


def test_ssh_submit_once_requires_receipt_absent_before_scheduler_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")
    receipt.write_text('{"stale":true}\n')
    sentinel = tmp_path / "scheduler-mutated"

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - generated shell under test
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["sh", "-c", f": > {shlex.quote(str(sentinel))}"],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace=str(workspace),
        array_size=None,
    )

    assert result.returncode == 64
    assert not sentinel.exists()
    assert receipt.read_text() == '{"stale":true}\n'


def test_ssh_submit_once_capture_cannot_be_path_replaced_by_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")
    receipt_glob = shlex.quote(f"{receipt}.capture.") + "*"
    command = (
        f"for capture in {receipt_glob}; do "
        '[ -d "$capture" ] || continue; '
        'mv "$capture" "${capture}.old"; mkdir "$capture"; '
        "printf '99999.host_f\\n' > \"$capture/stdout\"; "
        ': > "$capture/stderr"; done; '
        "printf '18109.host_f\\n'"
    )

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - generated shell under test
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["sh", "-c", command],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace=str(workspace),
        array_size=None,
    )

    assert result.stdout == "18109.host_f"
    payload = json.loads(receipt.read_text())
    assert payload["status"] == "accepted"
    assert payload["scheduler_job_id"] == "18109.host_f"


def test_ssh_submit_once_valid_scheduler_id_outranks_nonzero_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - executes generated test shell locally
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["sh", "-c", "printf '18109.host_f\\n'; exit 2"],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace=str(workspace),
        array_size=None,
    )

    assert result.returncode == 2
    assert json.loads(receipt.read_text()) == {
        "schema": "vq.scheduler-submit-once.v1",
        "status": "accepted",
        "vq_job_id": "abc123",
        "scheduler_job_id": "18109.host_f",
        "scheduler_returncode": 2,
    }


def test_ssh_submit_once_torque_id_before_trailing_output_proves_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - executes generated test shell locally
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    runner.submit_once(
        [
            "sh",
            "-c",
            "printf '18109.host_f\\ntrailing wrapper warning\\n'; exit 2",
        ],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name="torque",
        remote_workspace=str(workspace),
        array_size=None,
    )

    payload = json.loads(receipt.read_text())
    assert payload["status"] == "accepted"
    assert payload["scheduler_job_id"] == "18109.host_f"


def test_dispatcher_torque_id_before_trailing_output_proves_acceptance() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: RemoteResult(
            2,
            "18109.host_f\ntrailing wrapper warning\n",
            "",
        )
        if argv and argv[0] == "qsub"
        else None
    )
    dispatcher = make_dispatcher(runner)

    handle = dispatcher.submit(job_id="j1", command=["true"], cpus=1)

    assert handle.job_id == "18109.host_f"


def test_dispatcher_valid_scheduler_id_outranks_nonzero_status() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: RemoteResult(2, "18109.host_f\n", "warning")
        if argv and argv[0] == "qsub"
        else None
    )
    dispatcher = make_dispatcher(runner)

    handle = dispatcher.submit(job_id="j1", command=["true"], cpus=1)

    assert handle.job_id == "18109.host_f"


def test_dispatcher_multiple_scheduler_ids_are_outcome_unknown() -> None:
    runner = FakeRunner(
        responder=lambda argv, _stdin: RemoteResult(
            2,
            "18109.host_f\n18110.host_f\n",
            "",
        )
        if argv and argv[0] == "qsub"
        else None
    )
    dispatcher = make_dispatcher(runner)

    with pytest.raises(
        scheduler_dispatch.SchedulerSubmitOutcomeUnknown,
        match="outcome is unknown",
    ):
        dispatcher.submit(job_id="j1", command=["true"], cpus=1)

    receipts = [
        json.loads(call.stdin)
        for call in runner.calls
        if call.stdin is not None and call.stdin.startswith("{")
    ]
    assert receipts[-1]["status"] == "outcome_unknown"


@pytest.mark.parametrize(
    ("dialect", "output"),
    [
        ("torque", "18109.host_f\n18110.host_f\n"),
        ("slurm", "  Submitted batch job 18109  \n"),
    ],
)
def test_ssh_submit_once_scheduler_id_classification_matches_dispatcher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    output: str,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    receipt = Path(f"{workspace}.submit-once.json")

    def local_remote_shell(
        _host: HostConfig,
        *argv: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - generated shell under test
            [str(value) for value in argv],
            capture_output=True,
            text=True,
            check=False,
        )

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        local_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    result = runner.submit_once(
        ["sh", "-c", f"printf %s {shlex.quote(output)}; exit 2"],
        receipt_path=str(receipt),
        vq_job_id="abc123",
        dialect_name=dialect,
        remote_workspace=str(workspace),
        array_size=None,
    )

    assert result.returncode == 2
    payload = json.loads(receipt.read_text())
    if dialect == "torque":
        assert payload["status"] == "outcome_unknown"
        assert payload["scheduler_job_id"] is None
    else:
        assert payload["status"] == "accepted"
        assert payload["scheduler_job_id"] == "18109"


def test_scheduler_retry_rotates_exact_prior_attempt_evidence_before_submit(
    tmp_path: Path,
) -> None:
    archived_names: list[str] = []

    class InspectingRunner(FakeRunner):
        def upload_file(self, local_path: Path, remote_path: str) -> None:
            with tarfile.open(local_path) as archive:
                archived_names.extend(archive.getnames())
            super().upload_file(local_path, remote_path)

    def responder(argv: list[str], _stdin: str | None) -> RemoteResult | None:
        if argv and argv[0] == "qsub":
            return RemoteResult(0, "18110.host_f\n", "")
        return None

    local = tmp_path / "workspace"
    marker_dir = local / "_vq"
    marker_dir.mkdir(parents=True)
    (local / "input.dat").write_text("scientific input")
    (marker_dir / "scheduler-job-id").write_text("18109.host_f\n")
    (marker_dir / "exit-code").write_text("17\n")
    (marker_dir / "resource-usage.json").write_text("{}\n")
    runner = InspectingRunner(responder=responder)
    dispatcher = make_dispatcher(runner)
    workspace = dispatcher.remote_workspace("j1")

    handle = dispatcher.submit(
        job_id="j1",
        command=["true"],
        cpus=1,
        local_workspace=local,
        retry_attempt=True,
    )

    assert handle.job_id == "18110.host_f"
    evidence_cleanup = next(
        call
        for call in runner.calls
        if call.argv[:2] == ["sh", "-c"]
        and "vq-scheduler-retry-evidence" in call.argv
    )
    assert evidence_cleanup.argv[-6:] == [
        workspace,
        f"{workspace}/_vq",
        f"{workspace}/_vq/scheduler-job-id",
        f"{workspace}.submit-once.json",
        f"{workspace}/_vq/exit-code",
        f"{workspace}/_vq/resource-usage.json",
    ]
    assert "./input.dat" in archived_names
    assert "./_vq/scheduler-job-id" not in archived_names
    assert "./_vq/exit-code" not in archived_names
    assert "./_vq/resource-usage.json" not in archived_names


def test_scheduler_retry_evidence_cleanup_is_idempotent_when_workspace_missing(
    tmp_path: Path,
) -> None:
    class LocalRunner(FakeRunner):
        def run(
            self,
            argv: list[str] | tuple[str, ...],
            *,
            stdin_data: str | None = None,
            check: bool = False,
        ) -> RemoteResult:
            del stdin_data, check
            completed = subprocess.run(  # noqa: S603 - exact local test shell
                list(argv),
                capture_output=True,
                text=True,
                check=False,
            )
            return RemoteResult(
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )

    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        LocalRunner(),
        scratch_root=str(tmp_path),
    )
    workspace = dispatcher.remote_workspace("j1")
    receipt = Path(f"{workspace}.submit-once.json")
    receipt.parent.mkdir(parents=True)
    receipt.write_text("stale")

    dispatcher._prepare_scheduler_retry_evidence(workspace)

    assert not Path(workspace).exists()
    assert not receipt.exists()


def test_ssh_submit_once_preserves_transport_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_remote_shell(*args: object, **kwargs: object) -> object:
        raise transport.RemoteOutcomeUnknown("observer timeout")

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    with pytest.raises(
        scheduler_dispatch.SchedulerRemoteOutcomeUnknown,
        match="outcome is unknown",
    ):
        runner.submit_once(
            ["qsub", "/remote/job.pbs"],
            receipt_path="/remote/job.submit-once.json",
            vq_job_id="abc123",
            dialect_name="torque",
            remote_workspace="/remote/job",
            array_size=None,
        )


def test_ssh_runner_uses_longer_qstat_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    def fake_run_remote_shell(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    runner.run(["qstat", "123.cluster"], check=False)

    assert seen["timeout"] == SCHEDULER_POLL_TIMEOUT_SECONDS
    assert seen["retry_transient"] == 2


@pytest.mark.parametrize(
    ("argv", "expected_retries"),
    [
        (["mkdir", "-p", "/home/user/.vibeqc-cluster/jobs/job1"], 2),
        (["qsub", "/home/user/.vibeqc-cluster/jobs/job1/job.pbs"], 0),
    ],
)
def test_ssh_runner_retries_workspace_mkdir_but_not_qsub(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    expected_retries: int,
) -> None:
    """A transient pre-submit SSH failure is safe to retry only before qsub."""
    seen: dict[str, object] = {}

    def fake_run_remote_shell(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    runner.run(argv, check=False)

    assert seen["retry_transient"] == expected_retries


def test_ssh_runner_uses_longer_tar_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    def fake_run_remote_shell(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell",
        fake_run_remote_shell,
    )
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))

    runner.run(["tar", "-cf", "/tmp/result.tar", "-C", "/ws", "."], check=False)

    assert seen["timeout"] == SCHEDULER_ARCHIVE_TIMEOUT_SECONDS
    assert seen["retry_transient"] == 1


def test_ssh_runner_upload_tree_stages_on_shared_fs_not_node_local_tmp(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Regression (2026-07-16 host_c dispatch wedge): ``upload_tree`` must stage
    the tarball workspace-adjacent on the shared cluster FS
    (``<remote_dir>.upload.tar``), NOT under node-local ``/tmp``.

    host_c balances login01/login02; the ``scp`` upload and the ``tar -xf`` are
    two separate ssh connections that can land on different login nodes. With a
    node-local ``/tmp`` path the tarball scp'd onto one node is invisible to the
    untar on the other — ``tar: /tmp/vq-upload-<id>.tar: Cannot open: No such
    file or directory`` (exit 2) — which aborted the whole dispatch tick. A
    shared-FS sibling of ``remote_dir`` (the same placement ``fetch_results``
    uses for ``<ws>.result.tar``) is visible from any login node."""
    uploads: list[tuple[Path, str]] = []
    shells: list[list[str]] = []

    def fake_upload_file(
        host_cfg: object, local_path: object, remote_path: str, **kwargs: object
    ) -> None:
        uploads.append((Path(str(local_path)), remote_path))

    def fake_run_remote_shell(
        host_cfg: object, *shell_args: str, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        shells.append(list(shell_args))
        return subprocess.CompletedProcess(args=["ssh"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.upload_file", fake_upload_file
    )
    monkeypatch.setattr(
        "vq.scheduler_dispatch.transport.run_remote_shell", fake_run_remote_shell
    )

    (tmp_path / "input.dat").write_text("payload")
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))
    remote_dir = "/home/USER/.vibeqc-cluster/jobs/jobX"
    runner.upload_tree(tmp_path, remote_dir)

    expected_tar = f"{remote_dir}.upload.tar"
    # The scp destination is the shared-FS, workspace-adjacent tar — never /tmp.
    assert len(uploads) == 1
    _local, scp_dest = uploads[0]
    assert scp_dest == expected_tar
    assert not scp_dest.startswith("/tmp/")
    # The untar reads back that SAME path, so it can't miss across a node hop.
    untar = next(s for s in shells if s and s[0] == "tar")
    assert untar == ["tar", "-xf", expected_tar, "-C", remote_dir]
    # Cleanup targets the same shared-FS tar, not a node-local /tmp file.
    rm = next(s for s in shells if s and s[0] == "rm")
    assert rm == ["rm", "-f", expected_tar]


# --------------------------------------------------------------------------- #
# launcherless Python payload for a named program (host_f 04b5d4b0b46c)
# --------------------------------------------------------------------------- #


class TestLauncherlessScriptRefusal:
    """A `.py` payload for a NAMED program must not reach the scheduler.

    host_f 2026-07-26: vq pinned `vibeqc-release` to immutable v0.15.64 correctly,
    then generated a PBS script whose command was the bare payload `run.py`.
    Torque ran it directly -- `line 31: run.py: command not found`, exit 127
    (job 04b5d4b0b46c, PBS 17614) -- while the matched ORCA job 9e2f3dc15a78
    completed normally. `_compose_command` prepends
    `scheduler_program_hooks.NAME.command_wrapper`, and host_f had wrappers for
    ORCA and vibe-view but none for `vibeqc-release`, so the wrapper was `[]`
    and the payload was submitted with no launcher and no complaint.
    """

    def test_named_program_script_head_is_refused_before_submission(self) -> None:
        runner = FakeRunner()
        d = make_dispatcher(runner)

        with pytest.raises(SchedulerError, match="no interpreter"):
            d.effective_command(
                ["run.py"], program="vibeqc-release", job_id="04b5d4b0b46c"
            )

        # Nothing was handed to the scheduler: the refusal precedes any qsub.
        assert runner.calls == []

    def test_refusal_points_at_branch_not_a_new_command_wrapper(self) -> None:
        """The remedy must be --branch/--python, not registering a wrapper.

        host_f's vibeqc-release/-dev command_wrapper hooks were removed on
        2026-07-21 because pointing both a branch and a wrapper at one launcher
        double-wraps it. An error message that says "register a command_wrapper"
        walks the operator straight back into that bug, so the ordering of this
        advice is load-bearing, not cosmetic.
        """
        d = make_dispatcher(FakeRunner())

        with pytest.raises(SchedulerError) as excinfo:
            d.effective_command(["run.py"], program="vibeqc-release", job_id="j1")

        message = str(excinfo.value)
        assert "vibeqc-release" in message
        assert "did not submit" in message
        # The supported remedy is named, and named first.
        assert "--branch" in message
        assert "--python" in message
        assert message.index("--branch") < message.index("command_wrapper")
        # And the double-wrap trap is called out rather than left to be
        # rediscovered.
        assert "double-wrap" in message

    def test_registered_wrapper_still_supplies_the_interpreter(self) -> None:
        """A program WITH a hook is unaffected -- vibe-view's shape."""
        d = make_dispatcher(
            FakeRunner(),
            scheduler_program_hooks={
                "vibe-view": SchedulerProgramHooks(
                    command_wrapper=["/opt/rt/vibe-view-python"]
                )
            },
        )

        assert d.effective_command(
            ["run.py"], program="vibe-view", job_id="j2"
        ) == ["/opt/rt/vibe-view-python", "run.py"]

    def test_binary_program_payload_is_untouched(self) -> None:
        """ORCA's shape: an executable head, no `.py`, no hook needed."""
        d = make_dispatcher(FakeRunner())

        assert d.effective_command(
            ["/opt/orca/orca", "job.inp"], program="orca", job_id="j3"
        ) == ["/opt/orca/orca", "job.inp"]

    def test_programless_script_payload_still_allowed(self) -> None:
        """No named program means no managed runtime to route through; a
        shebang+exec payload is the caller's business, and one supported SLURM
        directory-payload path relies on it."""
        d = make_dispatcher(FakeRunner())

        assert d.effective_command(["input.py"], program=None, job_id="j4") == [
            "input.py"
        ]


# --------------------------------------------------------------------------- #
# self-recorded scheduler id (recovery for a handle the driver never persisted)
# --------------------------------------------------------------------------- #


class TestRecordedSchedulerId:
    """The job writes its own scheduler id to the shared workspace.

    `Daemon._start_scheduler_job` claims RUNNING with `scheduler_job_id=None`,
    submits outside the spec lock, then records the id in a second write. A
    driver death in that window leaves a live cluster job the driver can no
    longer name. The job recording its own id closes that window from the side
    that always survives.
    """

    def test_single_job_records_its_scheduler_id(self) -> None:
        d = make_dispatcher(FakeRunner())
        ws = d.remote_workspace("j1")

        script = d.build_job_script(
            job_id="j1", command=["true"], remote_workspace=ws, cpus=1
        )

        assert f"{ws}/_vq/scheduler-job-id" in script
        assert "PBS_JOBID" in script

    def test_slurm_uses_its_own_job_id_variable(self) -> None:
        d = make_slurm_dispatcher(FakeRunner())

        script = d.build_job_script(
            job_id="j1",
            command=["true"],
            remote_workspace=d.remote_workspace("j1"),
            cpus=1,
        )

        assert "SLURM_JOB_ID" in script
        assert "PBS_JOBID" not in script

    def test_the_record_is_written_before_the_command_runs(self) -> None:
        """A job that dies mid-run must still have named itself."""
        d = make_dispatcher(FakeRunner())
        ws = d.remote_workspace("j1")

        script = d.build_job_script(
            job_id="j1", command=["/bin/slow"], remote_workspace=ws, cpus=1
        )

        assert script.index("scheduler-job-id") < script.index("/bin/slow")

    def test_an_array_element_does_not_record_a_parent_id_it_lacks(self) -> None:
        """An array element's id carries its index, which is not the parent
        handle vq holds, and every sub-job would race for the same file."""
        d = make_dispatcher(FakeRunner())
        ws = d.remote_workspace("j1")

        script = d.build_job_script(
            job_id="j1", command=["true"], remote_workspace=ws, cpus=1, array_size=4
        )

        assert "scheduler-job-id" not in script

    def test_recorded_job_id_reads_the_deterministic_workspace_path(self) -> None:
        seen: list[str] = []

        def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
            if argv[:2] == ["sh", "-c"]:
                seen.append(argv[-1])
                return RemoteResult(0, "18109.host_f\n", "")
            return None

        d = make_dispatcher(FakeRunner(responder=responder))

        assert d.recorded_job_id("j1") == "18109.host_f"
        assert seen == [f"{d.remote_workspace('j1')}/_vq/scheduler-job-id"]

    def test_recorded_job_id_requires_real_workspace_and_marker_parents(self) -> None:
        scripts: list[str] = []

        def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
            if argv[:2] == ["sh", "-c"]:
                scripts.append(argv[2])
                return RemoteResult(0, "18109.host_f\n", "")
            return None

        d = make_dispatcher(FakeRunner(responder=responder))

        assert d.recorded_job_id("j1") == "18109.host_f"
        assert len(scripts) == 1
        assert '[ ! -L "$workspace" ]' in scripts[0]
        assert '[ ! -L "$marker_dir" ]' in scripts[0]
        assert '[ ! -L "$path" ]' in scripts[0]

    def test_native_array_never_consumes_single_job_start_marker(self) -> None:
        runner = FakeRunner(
            responder=lambda argv, stdin: RemoteResult(0, "18109.host_f\n", "")
        )
        d = make_dispatcher(runner)

        assert d.recorded_job_id("j1", array_size=4) is None
        assert runner.calls == []

    def test_submit_rejects_preexisting_job_start_marker_before_scheduler_mutation(
        self,
    ) -> None:
        marker_check_seen = False

        def responder(argv: list[str], stdin: str | None) -> RemoteResult | None:
            nonlocal marker_check_seen
            if argv[:2] == ["sh", "-c"] and "scheduler-job-id" in argv[-1]:
                marker_check_seen = True
                return RemoteResult(1, "", "preexisting marker")
            if argv and argv[0] == "qsub":
                raise AssertionError("qsub must not run after a stale start marker")
            return None

        d = make_dispatcher(FakeRunner(responder=responder))

        with pytest.raises(SchedulerError, match="job-start marker"):
            d.submit(job_id="j1", command=["true"], cpus=1)
        assert marker_check_seen

    @pytest.mark.parametrize(
        "payload",
        [
            "not-a-job-id\n",
            "18109.host_f\nother\n",
            "1" * (scheduler_dispatch.SCHEDULER_JOB_ID_MAX_BYTES + 1),
        ],
    )
    def test_recorded_job_id_rejects_malformed_or_oversized_evidence(
        self,
        payload: str,
    ) -> None:
        d = make_dispatcher(
            FakeRunner(
                responder=lambda argv, stdin: RemoteResult(0, payload, "")
                if argv[:2] == ["sh", "-c"]
                else None
            )
        )

        assert d.recorded_job_id("j1") is None

    def test_a_missing_record_is_not_evidence_the_job_is_gone(self) -> None:
        """A queued job has not run its script yet."""
        d = make_dispatcher(
            FakeRunner(
                responder=lambda argv, stdin: RemoteResult(1, "", "no such file")
            )
        )

        assert d.recorded_job_id("j1") is None

    def test_a_transport_failure_reports_cannot_say(self) -> None:
        class Exploding(FakeRunner):
            def run(self, argv, *, stdin_data=None, check=False):  # type: ignore[no-untyped-def]
                raise OSError("ssh died")

        d = make_dispatcher(Exploding())

        assert d.recorded_job_id("j1") is None

    def test_submit_receipt_accepts_exact_bounded_scheduler_proof(self) -> None:
        payload = json.dumps(
            {
                "schema": "vq.scheduler-submit-once.v1",
                "status": "accepted",
                "vq_job_id": "j1",
                "scheduler_job_id": "18109.host_f",
                "scheduler_returncode": 0,
            }
        )
        d = make_dispatcher(
            FakeRunner(
                responder=lambda argv, stdin: RemoteResult(0, payload, "private")
                if argv[:2] == ["sh", "-c"]
                else None
            )
        )

        assert d.submit_receipt("j1") == scheduler_dispatch.SchedulerSubmitReceipt(
            status="accepted",
            scheduler_job_id="18109.host_f",
            scheduler_returncode=0,
        )

    def test_submit_receipt_accepts_valid_id_despite_nonzero_command_status(
        self,
    ) -> None:
        payload = json.dumps(
            {
                "schema": "vq.scheduler-submit-once.v1",
                "status": "accepted",
                "vq_job_id": "j1",
                "scheduler_job_id": "18109.host_f",
                "scheduler_returncode": 2,
            }
        )
        d = make_dispatcher(
            FakeRunner(
                responder=lambda argv, stdin: RemoteResult(0, payload, "")
                if argv[:2] == ["sh", "-c"]
                else None
            )
        )

        assert d.submit_receipt("j1") == scheduler_dispatch.SchedulerSubmitReceipt(
            status="accepted",
            scheduler_job_id="18109.host_f",
            scheduler_returncode=2,
        )

    @pytest.mark.parametrize(
        "payload",
        [
            "not-json",
            json.dumps(
                {
                    "schema": "vq.scheduler-submit-once.v1",
                    "status": "accepted",
                    "vq_job_id": "different-job",
                    "scheduler_job_id": "18109.host_f",
                    "scheduler_returncode": 0,
                }
            ),
            json.dumps(
                {
                    "schema": "vq.scheduler-submit-once.v1",
                    "status": "accepted",
                    "vq_job_id": "j1",
                    "scheduler_job_id": "not-a-job-id",
                    "scheduler_returncode": 0,
                }
            ),
            "x" * (scheduler_dispatch.SCHEDULER_SUBMIT_RECEIPT_MAX_BYTES + 1),
        ],
    )
    def test_submit_receipt_rejects_unbound_or_malformed_evidence(
        self,
        payload: str,
    ) -> None:
        d = make_dispatcher(
            FakeRunner(
                responder=lambda argv, stdin: RemoteResult(0, payload, "")
                if argv[:2] == ["sh", "-c"]
                else None
            )
        )

        assert d.submit_receipt("j1") is None


@pytest.mark.parametrize("outcomes", [[255, 0], [255, 255, 255], [1]])
def test_scheduler_upload_retries_only_transport_before_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcomes: list[int],
) -> None:
    local = tmp_path / "payload.tar"
    local.write_bytes(b"immutable staged payload")
    commands: list[list[str]] = []
    payloads: list[bytes] = []
    sleeps: list[float] = []

    def fake_scp(command, **kwargs):
        commands.append(command)
        payloads.append(Path(command[-2]).read_bytes())
        rc = outcomes[min(len(commands) - 1, len(outcomes) - 1)]
        return subprocess.CompletedProcess(command, rc, stdout="", stderr="fixture failure")

    monkeypatch.setattr(transport, "run_owned_subprocess", fake_scp)
    monkeypatch.setattr(transport.time, "sleep", sleeps.append)
    runner = SshRemoteRunner(HostConfig(ssh="cluster"))
    if outcomes[-1] == 0:
        runner.upload_file(local, "/scratch/job.upload.tar")
    else:
        with pytest.raises(transport.RemoteError) as error:
            runner.upload_file(local, "/scratch/job.upload.tar")
        assert f"after {len(outcomes)} attempt(s)" in str(error.value)
    assert len(commands) == len(outcomes)
    assert payloads == [local.read_bytes()] * len(outcomes)
    assert sleeps == sorted(sleeps)
    assert len(sleeps) == len(outcomes) - 1
    for command in commands[1:]:
        assert "ControlPath=none" in command
        assert "ControlMaster=no" in command
    # The upload retry boundary must never invoke sbatch/qsub.
    assert all(Path(command[0]).name == "scp" for command in commands)

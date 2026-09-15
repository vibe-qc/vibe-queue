"""Token-exact compatibility snapshots for the remote submit CLI protocol."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from vq import submit
from vq.config import HostConfig

REMOTE_TAR = ".vq-upload-contract.tar"
EXPECTED_SHA = "a" * 40


@dataclass
class RemoteWire:
    uploads: list[tuple[Path, str]] = field(default_factory=list)
    argv: list[tuple[str, ...]] = field(default_factory=list)
    shell: list[tuple[str, ...]] = field(default_factory=list)


@pytest.fixture
def host_cfg() -> HostConfig:
    return HostConfig(
        ssh="remote.invalid",
        remote_vq="/remote/bin/vq",
        remote_python="/remote/bin/python",
    )


@pytest.fixture
def remote_wire(monkeypatch: pytest.MonkeyPatch) -> RemoteWire:
    wire = RemoteWire()

    def upload(_host_cfg: HostConfig, local: Path, remote: str) -> None:
        assert local.exists()
        wire.uploads.append((local, remote))

    def run_remote_vq(
        host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        wire.argv.append(args)
        count = 1
        for flag in ("--array", "--chain"):
            if flag in args:
                count = int(args[args.index(flag) + 1])
        stdout = "".join(f"{index:012x}\n" for index in range(count))
        return subprocess.CompletedProcess(
            args=[host_cfg.remote_vq, *args],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    def run_remote_shell(
        _host_cfg: HostConfig,
        *args: str,
        check: bool = True,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        wire.shell.append(args)
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(submit.transport, "remote_temp_tar_path", lambda: REMOTE_TAR)
    monkeypatch.setattr(submit.transport, "upload_file", upload)
    monkeypatch.setattr(submit.transport, "run_remote_vq", run_remote_vq)
    monkeypatch.setattr(submit.transport, "run_remote_shell", run_remote_shell)
    return wire


def test_minimal_single_remote_argv_snapshot(
    tmp_path: Path,
    host_cfg: HostConfig,
    remote_wire: RemoteWire,
) -> None:
    source = tmp_path / "single.py"
    source.write_text("pass\n", encoding="utf-8")

    jobids = submit.submit_remote(
        host="remote",
        host_cfg=host_cfg,
        input_file=str(source),
    )

    assert jobids == ["000000000000"]
    assert remote_wire.argv == [
        (
            "submit",
            "localhost",
            "-c",
            REMOTE_TAR,
            "--cpus",
            "1",
            "--",
            "/remote/bin/python",
            "single.py",
        )
    ]


def test_minimal_array_remote_argv_snapshot(
    tmp_path: Path,
    host_cfg: HostConfig,
    remote_wire: RemoteWire,
) -> None:
    source = tmp_path / "minimal.py"
    source.write_text("pass\n", encoding="utf-8")

    jobids = submit.submit_remote(
        host="remote",
        host_cfg=host_cfg,
        input_file=str(source),
        array=2,
    )

    assert jobids == ["000000000000", "000000000001"]
    assert remote_wire.argv == [
        (
            "submit",
            "localhost",
            "-c",
            REMOTE_TAR,
            "--cpus",
            "1",
            "--array",
            "2",
            "--",
            "/remote/bin/python",
            "minimal.py",
        )
    ]


def test_rich_directory_remote_argv_snapshot(
    tmp_path: Path,
    host_cfg: HostConfig,
    remote_wire: RemoteWire,
) -> None:
    source = tmp_path / "payload"
    source.mkdir()
    (source / "run script.sh").write_text("exit 0\n", encoding="utf-8")
    command = [
        "bash",
        "run script.sh",
        "$HOME",
        "semi;colon",
        "star*",
        "quote'one",
        'quote"two',
    ]

    jobids = submit.submit_remote(
        host="driver",
        host_cfg=host_cfg,
        directory=str(source),
        command=command,
        cpus=3,
        scheduler_tasks=4,
        mem_mb=512,
        wall_time_seconds=90,
        priority=2,
        auto_resume=True,
        retry=3,
        job_name="job-name",
        branch="release",
        program="matrix-prog",
        expected_sha=EXPECTED_SHA,
        tags=["beta-tag", "alpha"],
        not_before="2026-08-03T10:00:00+00:00",
        depends_on=["aaaaaaaaaaaa", "bbbbbbbbbbbb"],
        depends_on_any=["cccccccccccc", "dddddddddddd"],
        clean_workdir_on_terminal=True,
        rerun_until_file_exists="$VQ_WORKDIR/DONE flag",
        rerun_max=7,
        refresh_before="env name",
        scheduler_target="host_f",
    )

    assert jobids == ["000000000000"]
    assert remote_wire.argv == [
        (
            "submit",
            "localhost",
            "-c",
            REMOTE_TAR,
            "--cpus",
            "3",
            "--scheduler-tasks",
            "4",
            "--mem-mb",
            "512",
            "--wall-time-seconds",
            "90",
            "--priority",
            "2",
            "--auto-resume",
            "--retry",
            "3",
            "--job-name",
            "job-name",
            "--branch-name",
            "release",
            "--program",
            "matrix-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--tag",
            "beta-tag",
            "--tag",
            "alpha",
            "--at",
            "2026-08-03T10:00:00+00:00",
            "--depends-on",
            "aaaaaaaaaaaa",
            "--depends-on",
            "bbbbbbbbbbbb",
            "--depends-on-any",
            "cccccccccccc",
            "--depends-on-any",
            "dddddddddddd",
            "--clean-tmp",
            "--rerun-until",
            "$VQ_WORKDIR/DONE flag",
            "--rerun-max",
            "7",
            "--refresh",
            "env name",
            "--scheduler-target",
            "host_f",
            "--",
            *command,
        )
    ]


def test_chain_remote_argv_snapshot(
    tmp_path: Path,
    host_cfg: HostConfig,
    remote_wire: RemoteWire,
) -> None:
    source = tmp_path / "chain.py"
    source.write_text("pass\n", encoding="utf-8")

    jobids = submit.submit_remote(
        host="driver",
        host_cfg=host_cfg,
        input_file=str(source),
        python="/cluster/runtime/python",
        chain=2,
        rerun_until_file_exists="$VQ_WORKDIR/DONE",
        rerun_max=7,
        scheduler_target="host_f",
    )

    assert jobids == ["000000000000", "000000000001"]
    assert remote_wire.argv == [
        (
            "submit",
            "localhost",
            "-c",
            REMOTE_TAR,
            "--cpus",
            "1",
            "--chain",
            "2",
            "--rerun-until",
            "$VQ_WORKDIR/DONE",
            "--rerun-max",
            "7",
            "--scheduler-target",
            "host_f",
            "--",
            "/cluster/runtime/python",
            "chain.py",
        )
    ]


def test_qvf_remote_argv_snapshot(
    tmp_path: Path,
    host_cfg: HostConfig,
    remote_wire: RemoteWire,
) -> None:
    source = tmp_path / "job.qvf"
    source.write_bytes(b"qvf contract")
    staged = f"{REMOTE_TAR}.d/job.qvf"

    jobids = submit.submit_remote(
        host="driver",
        host_cfg=host_cfg,
        input_file=str(source),
        program="matrix-prog",
        expected_sha=EXPECTED_SHA,
        scheduler_target="host_f",
        qvf_force=True,
    )

    assert jobids == ["000000000000"]
    assert remote_wire.argv == [
        (
            "submit",
            "localhost",
            "--cpus",
            "1",
            "--program",
            "matrix-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--scheduler-target",
            "host_f",
            "--qvf-force",
            staged,
        )
    ]
    assert remote_wire.shell == [
        ("mkdir", "-p", f"{REMOTE_TAR}.d"),
        ("rm", "-f", staged),
        ("rmdir", f"{REMOTE_TAR}.d"),
    ]

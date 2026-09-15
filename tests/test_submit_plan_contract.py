"""Public CLI transcripts that characterize normalized submit planning."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cli as cli_module
from vq import config, paths
from vq import submit as submit_module
from vq.cli import main
from vq.spec import JobState, ProgramRuntimePin
from vq.wait import WaitResult

EXPECTED_SHA = "a" * 40
LOCAL_JOBID = "111111111111"


@dataclass
class PlanHarness:
    root: Path
    cfg: config.Config
    local_pin: ProgramRuntimePin
    observed_pin: ProgramRuntimePin
    scheduler_pin: ProgramRuntimePin
    local_scheduler_pin: ProgramRuntimePin
    calls: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    program_validation_calls: list[tuple[str | None, bool]] = field(
        default_factory=list
    )
    expected_sha_calls: list[tuple[str | None, str | None, bool]] = field(
        default_factory=list
    )
    local_pin_calls: list[tuple[str | None, str | None]] = field(
        default_factory=list
    )
    scheduler_pin_calls: list[
        tuple[str, str | None, str | None, tuple[str, ...]]
    ] = field(default_factory=list)


@pytest.fixture
def plan_harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> PlanHarness:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '\n'.join(
            [
                'default_host = "remote"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.localhost.branches]",
                'release = "/local/release/python"',
                "",
                "[hosts.localhost.branch_aliases]",
                'latest = "release"',
                "",
                "[hosts.remote]",
                'ssh = "remote.invalid"',
                'remote_vq = "/remote/bin/vq"',
                'remote_python = "/remote/bin/python"',
                "",
                "[hosts.remote.branches]",
                'release = "/remote/release/python"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f.invalid"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/cluster"',
                'scheduler_driver = "driver"',
                'scheduler_max_wall_time_seconds = 7200',
                "",
                "[hosts.host_f.branches]",
                'release = "/cluster/release/python"',
                "",
                "[hosts.host_f.scheduler_program_hooks.legacy-prog]",
                'command_wrapper = ["/cluster/legacy/vibeqc-release-python"]',
                "",
                "[hosts.host_f.scheduler_program_hooks.wrapped-prog]",
                'command_wrapper = ["/cluster/runtime/python"]',
                "",
                "[hosts.host_f.scheduler_program_hooks.wrapper-only-prog]",
                'command_wrapper = ["/cluster/runtime/python"]',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.remote-only-prog]",
                'update_command = "/site/bin/update-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.legacy-prog]",
                'update_command = "/site/bin/update-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.wrapped-prog]",
                'update_command = "/site/bin/update-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                "",
                "[hosts.driver]",
                'ssh = "driver.invalid"',
                'remote_vq = "/driver/bin/vq"',
                'remote_python = "/driver/bin/python"',
                "",
                "[hosts.driver.branches]",
                'release = "/driver/release/python"',
                "",
                "[hosts.local-host_f]",
                'ssh = "local-host_f.invalid"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/local-cluster"',
                'scheduler_driver = "localhost"',
                'scheduler_max_wall_time_seconds = 3600',
                "",
                "[hosts.local-host_f.scheduler_runtime_deployments.observed-prog]",
                'update_command = "/site/bin/update-runtime"',
                'verify_command = "/site/bin/verify-runtime"',
                "",
                "[pools.compute]",
                'hosts = ["remote", "host_f"]',
                "",
            ]
        ),
        encoding="utf-8",
    )
    cfg = config.load_config()
    local_pin = ProgramRuntimePin(
        expected_git_sha=EXPECTED_SHA,
        enforce_git_sha=True,
    )
    observed_pin = ProgramRuntimePin(
        expected_git_sha="b" * 40,
        enforce_git_sha=False,
    )
    scheduler_pin = ProgramRuntimePin(
        expected_git_sha=EXPECTED_SHA,
        enforce_git_sha=True,
        scheduler_host="host_f",
        resolved_executable="/cluster/runtime/python",
        program_kind="scheduler-runtime",
        program_version="0.15.test",
        artifact_identity="/cluster/runtime/python",
    )
    local_scheduler_pin = ProgramRuntimePin(
        expected_git_sha="c" * 40,
        enforce_git_sha=True,
        scheduler_host="local-host_f",
        resolved_executable="/local-cluster/runtime/python",
        program_kind="scheduler-runtime",
        program_version="0.15.local-test",
        artifact_identity="/local-cluster/runtime/python",
    )
    harness = PlanHarness(
        root=tmp_path,
        cfg=cfg,
        local_pin=local_pin,
        observed_pin=observed_pin,
        scheduler_pin=scheduler_pin,
        local_scheduler_pin=local_scheduler_pin,
    )

    def submit_local(**kwargs: object) -> str:
        kwargs.pop("warning_sink", None)
        harness.calls.append(("local", kwargs))
        return LOCAL_JOBID

    def submit_local_array(**kwargs: object) -> list[str]:
        kwargs.pop("warning_sink", None)
        harness.calls.append(("array", kwargs))
        count = int(kwargs["array"])
        return [f"{index + 1:012d}" for index in range(count)]

    def submit_local_chain(**kwargs: object) -> list[str]:
        kwargs.pop("warning_sink", None)
        harness.calls.append(("chain", kwargs))
        count = int(kwargs["chain"])
        return [f"{index + 1:012d}" for index in range(count)]

    def submit_remote(**kwargs: object) -> list[str]:
        kwargs.pop("warning_sink", None)
        harness.calls.append(("remote", kwargs))
        count = max(int(kwargs["array"]), int(kwargs["chain"]))
        return [f"{index:012d}" for index in range(count)]

    def validate_expected_sha(
        _cfg: config.Config,
        _program_name: str | None,
        expected_sha: str | None,
        *,
        local_spec: bool,
    ) -> str | None:
        if local_spec and _program_name in {
            "unregistered-prog",
            "remote-only-prog",
        }:
            raise AssertionError("remote runtime identity was validated locally")
        harness.expected_sha_calls.append(
            (_program_name, expected_sha, local_spec)
        )
        return expected_sha.strip() if expected_sha is not None else None

    def validate_program(
        _cfg: config.Config,
        program_name: str | None,
        *,
        local_spec: bool,
    ) -> None:
        if local_spec and program_name in {
            "unregistered-prog",
            "remote-only-prog",
        }:
            raise AssertionError("remote-only program was validated locally")
        harness.program_validation_calls.append((program_name, local_spec))

    def local_runtime_pin(
        _cfg: config.Config,
        program_name: str | None,
        *,
        expected_sha: str | None = None,
    ) -> ProgramRuntimePin | None:
        harness.local_pin_calls.append((program_name, expected_sha))
        if program_name == "observed-prog":
            return observed_pin
        return local_pin if program_name is not None else None

    def scheduler_runtime_pin(
        _cfg: config.Config,
        scheduler_host: str,
        program_name: str | None,
        expected_sha: str | None,
        *,
        command_candidates: list[str] | None = None,
    ) -> ProgramRuntimePin:
        harness.scheduler_pin_calls.append(
            (
                scheduler_host,
                program_name,
                expected_sha,
                tuple(command_candidates or []),
            )
        )
        if scheduler_host == "host_f":
            return scheduler_pin
        if scheduler_host == "local-host_f":
            return local_scheduler_pin
        raise AssertionError(f"unexpected scheduler target {scheduler_host!r}")

    monkeypatch.setattr(submit_module, "submit_local", submit_local)
    monkeypatch.setattr(submit_module, "submit_local_array", submit_local_array)
    monkeypatch.setattr(submit_module, "submit_local_chain", submit_local_chain)
    monkeypatch.setattr(submit_module, "submit_remote", submit_remote)
    monkeypatch.setattr(
        cli_module,
        "_validate_program_for_submit",
        validate_program,
    )
    monkeypatch.setattr(
        cli_module,
        "_validate_expected_sha_for_submit",
        validate_expected_sha,
    )
    monkeypatch.setattr(
        cli_module,
        "_program_runtime_pin_for_submit",
        local_runtime_pin,
    )
    monkeypatch.setattr(
        cli_module,
        "_validate_scheduler_target_expected_sha",
        scheduler_runtime_pin,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_program_for_submit",
        validate_program,
        raising=False,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_expected_sha_for_submit",
        validate_expected_sha,
        raising=False,
    )
    monkeypatch.setattr(
        submit_module,
        "_program_runtime_pin_for_submit",
        local_runtime_pin,
        raising=False,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_scheduler_target_expected_sha",
        scheduler_runtime_pin,
        raising=False,
    )
    monkeypatch.setattr(cli_module, "_multi_user_active", lambda *_args: False)
    monkeypatch.setattr(cli_module.host_status, "is_down", lambda _host: None)
    monkeypatch.setattr(
        cli_module.drain_module,
        "read_drain_state",
        lambda: None,
    )
    monkeypatch.setattr(
        cli_module.admin_module,
        "admin_update_marker_exists",
        lambda: False,
    )
    return harness


def _local_single_kwargs(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "host": "localhost",
        "input_file": None,
        "directory": None,
        "archive": None,
        "command": None,
        "python": None,
        "cpus": 1,
        "scheduler_tasks": None,
        "mem_mb": None,
        "wall_time_seconds": None,
        "priority": 0,
        "auto_resume": False,
        "retry": 0,
        "job_name": None,
        "branch": None,
        "program": None,
        "program_runtime_pin": None,
        "tags": None,
        "not_before": None,
        "depends_on": None,
        "depends_on_any": None,
        "rerun_until_file_exists": None,
        "rerun_max": 10,
        "clean_workdir_on_terminal": False,
        "vibeqc_preflight": False,
        "multi_user": False,
        "scheduler_target": None,
        "refresh_before": None,
        "qvf_force": False,
    }
    kwargs.update(overrides)
    return kwargs


def _remote_kwargs(
    host: str,
    host_cfg: config.HostConfig,
    **overrides: object,
) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "host": host,
        "host_cfg": host_cfg,
        "input_file": None,
        "directory": None,
        "archive": None,
        "command": None,
        "python": None,
        "cpus": 1,
        "scheduler_tasks": None,
        "mem_mb": None,
        "wall_time_seconds": None,
        "priority": 0,
        "auto_resume": False,
        "retry": 0,
        "job_name": None,
        "branch": None,
        "program": None,
        "expected_sha": None,
        "tags": None,
        "not_before": None,
        "depends_on": None,
        "depends_on_any": None,
        "clean_workdir_on_terminal": False,
        "array": 1,
        "chain": 1,
        "rerun_until_file_exists": None,
        "rerun_max": 10,
        "refresh_before": None,
        "scheduler_target": None,
        "qvf_force": False,
    }
    kwargs.update(overrides)
    return kwargs


def _receipt(
    host: str,
    jobids: list[str],
    *,
    scheduler_pin: ProgramRuntimePin | None = None,
    scheduler_driver: str | None = None,
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "acceptance_scope": "queue",
        "execution_status": "not_observed",
        "capacity_warnings": [],
        "dispatch_holds": [],
        "host": host,
        "jobids": jobids,
        "next": [
            f"vq status {host} {jobids[0]}",
            f"vq logs {host} {jobids[0]} -f",
        ],
    }
    if scheduler_driver is not None:
        receipt.update(
            {
                "scheduler": "pbs",
                "scheduler_driver": scheduler_driver,
            }
        )
    if scheduler_pin is not None:
        receipt.update(
            {
                "program_runtime_pin": {
                    "artifact_identity": scheduler_pin.artifact_identity,
                    "expected_git_sha": scheduler_pin.expected_git_sha,
                    "program_kind": scheduler_pin.program_kind,
                    "program_version": scheduler_pin.program_version,
                    "resolved_executable": scheduler_pin.resolved_executable,
                    "scheduler_host": scheduler_pin.scheduler_host,
                },
            }
        )
    return receipt


def test_local_directory_alias_plan(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "local-payload"
    source.mkdir()
    (source / "run.py").write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "localhost",
            "--branch",
            "latest",
            "--json",
            "-d",
            str(source),
            "--",
            "run.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                directory=str(source),
                command=["run.py"],
                python="/local/release/python",
                branch="latest",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt("localhost", [LOCAL_JOBID])


def test_default_remote_archive_plan(
    plan_harness: PlanHarness,
) -> None:
    archive = plan_harness.root / "payload.tar"
    archive.write_bytes(b"archive contract")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--json",
            "--cpus",
            "3",
            "--ntasks",
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
            "--branch",
            "release",
            "--program",
            "unregistered-prog",
            "--expected-sha",
            "  abcdefa  ",
            "--tag",
            "beta",
            "--tag",
            "alpha",
            "--at",
            "2026-08-03T10:00:00Z",
            "--depends-on",
            "aaaaaaaaaaaa",
            "--depends-on-any",
            "bbbbbbbbbbbb",
            "--clean-tmp",
            "--refresh",
            "unregistered-prog",
            "-c",
            str(archive),
            "--",
            "bash",
            "run.sh",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "remote",
                plan_harness.cfg.host("remote"),
                archive=str(archive),
                command=["bash", "run.sh"],
                python="/remote/release/python",
                cpus=3,
                scheduler_tasks=4,
                mem_mb=512,
                wall_time_seconds=90,
                priority=2,
                auto_resume=True,
                retry=3,
                job_name="job-name",
                branch="release",
                program="unregistered-prog",
                expected_sha="abcdefa",
                tags=["beta", "alpha"],
                not_before="2026-08-03T10:00:00+00:00",
                depends_on=["aaaaaaaaaaaa"],
                depends_on_any=["bbbbbbbbbbbb"],
                clean_workdir_on_terminal=True,
                refresh_before="unregistered-prog",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt("remote", ["000000000000"])
    assert plan_harness.scheduler_pin_calls == []


def test_remote_qvf_plan(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "remote.qvf"
    source.write_bytes(b"qvf contract")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "remote",
            "--program",
            "unregistered-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--refresh",
            "unregistered-prog",
            "--qvf-force",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "remote",
                plan_harness.cfg.host("remote"),
                input_file=str(source),
                program="unregistered-prog",
                expected_sha=EXPECTED_SHA,
                refresh_before="unregistered-prog",
                qvf_force=True,
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt("remote", ["000000000000"])
    assert plan_harness.scheduler_pin_calls == []


def test_auto_host_flag_plan(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "auto-flag.py"
    source.write_text("pass\n", encoding="utf-8")
    picks: list[tuple[int, str | None, int | None]] = []

    def pick(
        _cfg: config.Config,
        cpus: int,
        *,
        pool: str | None,
        job_mem_mb: int | None,
    ) -> str:
        picks.append((cpus, pool, job_mem_mb))
        return "remote"

    monkeypatch.setattr(cli_module, "_pick_auto_host", pick)
    monkeypatch.setattr(
        cli_module,
        "_auto_estimate_job_mem_mb",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("explicit --mem-mb must skip auto estimation")
        ),
    )

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "auto",
            "--pool",
            "compute",
            "--cpus",
            "3",
            "--mem-mb",
            "2048",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert picks == [(3, "compute", 2048)]
    assert result.stderr == "vq submit auto → remote\n"
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "remote",
                plan_harness.cfg.host("remote"),
                input_file=str(source),
                cpus=3,
                mem_mb=2048,
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt("remote", ["000000000000"])


def test_positional_auto_estimate_plan(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "auto-positional.py"
    source.write_text("pass\n", encoding="utf-8")
    estimates: list[list[str]] = []
    picks: list[tuple[int, str | None, int | None]] = []

    def estimate(_cfg: config.Config, rest: list[str]) -> int:
        estimates.append(list(rest))
        return 3072

    def pick(
        _cfg: config.Config,
        cpus: int,
        *,
        pool: str | None,
        job_mem_mb: int | None,
    ) -> str:
        picks.append((cpus, pool, job_mem_mb))
        return "host_f"

    monkeypatch.setattr(cli_module, "_auto_estimate_job_mem_mb", estimate)
    monkeypatch.setattr(cli_module, "_pick_auto_host", pick)

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "auto",
            "--pool",
            "compute",
            "--cpus",
            "2",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert estimates == [[str(source)]]
    assert picks == [(2, "compute", 3072)]
    assert result.stderr == "vq submit auto → host_f (≈3072 MB est.)\n"
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "driver",
                plan_harness.cfg.host("driver"),
                input_file=str(source),
                cpus=2,
                mem_mb=None,
                scheduler_target="host_f",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt(
        "host_f",
        ["000000000000"],
        scheduler_driver="driver",
    )
    assert plan_harness.scheduler_pin_calls == []


def test_scheduler_remote_driver_plan_and_wait_owner(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "scheduler-payload"
    source.mkdir()
    (source / "run.sh").write_text("exit 0\n", encoding="utf-8")
    waits: list[tuple[str, str, config.HostConfig | None]] = []

    def wait_for_terminal(
        host: str,
        jobid: str,
        *,
        host_cfg: config.HostConfig | None,
        poll_interval: float,
        timeout: float | None,
        multi_user: bool,
    ) -> WaitResult:
        waits.append((host, jobid, host_cfg))
        return WaitResult(jobid=jobid, state=JobState.COMPLETED, exit_code=0)

    monkeypatch.setattr(cli_module, "wait_for_terminal", wait_for_terminal)

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--branch",
            "release",
            "--chain",
            "2",
            "--rerun-until",
            "$VQ_WORKDIR/DONE",
            "--rerun-max",
            "7",
            "--json",
            "--wait",
            "-d",
            str(source),
            "--",
            "bash",
            "run.sh",
        ],
    )

    assert result.exit_code == 0, result.output
    driver_cfg = plan_harness.cfg.host("driver")
    jobids = ["000000000000", "000000000001"]
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "driver",
                driver_cfg,
                directory=str(source),
                command=["/cluster/runtime/python", "bash", "run.sh"],
                branch="release",
                program="remote-only-prog",
                expected_sha=EXPECTED_SHA,
                chain=2,
                rerun_until_file_exists="$VQ_WORKDIR/DONE",
                rerun_max=7,
                scheduler_target="host_f",
            ),
        )
    ]
    assert waits == [
        ("driver", "000000000000", driver_cfg),
        ("driver", "000000000001", driver_cfg),
    ]
    assert json.loads(result.stdout) == _receipt(
        "host_f",
        jobids,
        scheduler_pin=plan_harness.scheduler_pin,
        scheduler_driver="driver",
    )
    assert "000000000000: completed" in result.stderr
    assert plan_harness.scheduler_pin_calls == [
        (
            "host_f",
            "remote-only-prog",
            EXPECTED_SHA,
            ("/cluster/release/python",),
        )
    ]


@pytest.mark.parametrize(
    ("command", "expected_command"),
    [
        (["python", "run.py"], ["/cluster/runtime/python", "run.py"]),
        (
            ["python3", "-u", "run.py", "--mode", "paper"],
            [
                "/cluster/runtime/python",
                "-u",
                "run.py",
                "--mode",
                "paper",
            ],
        ),
        (
            ["run.py", "--mode", "paper"],
            [
                "/cluster/runtime/python",
                "run.py",
                "--mode",
                "paper",
            ],
        ),
        (
            ["/cluster/runtime/python", "run.py"],
            ["/cluster/runtime/python", "run.py"],
        ),
    ],
)
def test_scheduler_managed_directory_normalizes_python_launcher(
    plan_harness: PlanHarness,
    command: list[str],
    expected_command: list[str],
) -> None:
    source = plan_harness.root / "managed-directory"
    source.mkdir(exist_ok=True)
    (source / "run.py").write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "--json",
            "-d",
            str(source),
            "--",
            *command,
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "driver",
                plan_harness.cfg.host("driver"),
                directory=str(source),
                command=expected_command,
                program="remote-only-prog",
                expected_sha=EXPECTED_SHA,
                scheduler_target="host_f",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt(
        "host_f",
        ["000000000000"],
        scheduler_pin=plan_harness.scheduler_pin,
        scheduler_driver="driver",
    )
    assert plan_harness.scheduler_pin_calls == [
        ("host_f", "remote-only-prog", None, (command[0],))
    ]


def test_scheduler_managed_archive_array_normalizes_python_launcher(
    plan_harness: PlanHarness,
) -> None:
    archive = plan_harness.root / "managed-archive.tar"
    archive.write_bytes(b"archive contract")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "--array",
            "2",
            "--json",
            "-c",
            str(archive),
            "--",
            "python3",
            "-u",
            "run.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "driver",
                plan_harness.cfg.host("driver"),
                archive=str(archive),
                command=["/cluster/runtime/python", "-u", "run.py"],
                program="remote-only-prog",
                expected_sha=EXPECTED_SHA,
                array=2,
                scheduler_target="host_f",
            ),
        )
    ]
    assert plan_harness.scheduler_pin_calls == [
        (
            "host_f",
            "remote-only-prog",
            None,
            ("python3",),
        )
    ]


def test_scheduler_managed_single_file_uses_target_launcher(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "managed-single.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "remote",
            _remote_kwargs(
                "driver",
                plan_harness.cfg.host("driver"),
                input_file=str(source),
                python="/cluster/runtime/python",
                program="remote-only-prog",
                expected_sha=EXPECTED_SHA,
                scheduler_target="host_f",
            ),
        )
    ]
    assert plan_harness.scheduler_pin_calls == [("host_f", "remote-only-prog", None, ())]


def test_scheduler_non_python_program_command_is_unchanged(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "orca-directory"
    source.mkdir()
    (source / "job.inp").write_text("! HF\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "orca",
            "--json",
            "-d",
            str(source),
            "--",
            "/opt/orca/orca",
            "job.inp",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls[0][1]["command"] == [
        "/opt/orca/orca",
        "job.inp",
    ]
    assert plan_harness.scheduler_pin_calls == []


def test_scheduler_managed_launcher_conflicting_hook_fails_before_execution(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "legacy-hook-directory"
    source.mkdir()
    (source / "run.py").write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "legacy-prog",
            "-d",
            str(source),
            "--",
            "python",
            "run.py",
        ],
    )

    assert result.exit_code == 2
    assert "command_wrapper" in result.output
    assert "managed launcher" in result.output
    assert plan_harness.calls == []


def test_scheduler_effective_wrapper_entrypoint_fails_before_execution(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "wrapper-only-directory"
    source.mkdir()

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "wrapper-only-prog",
            "-d",
            str(source),
            "--",
            "missing",
        ],
    )

    assert result.exit_code == 2
    assert "payload validation" in result.output
    assert "missing" in result.output
    assert plan_harness.calls == []


def test_scheduler_effective_wrapper_is_not_baked_into_forwarded_command(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "wrapper-only-positive-directory"
    source.mkdir()
    (source / "runner").write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "wrapper-only-prog",
            "--json",
            "-d",
            str(source),
            "--",
            "runner",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls[0][1]["command"] == ["runner"]


def test_scheduler_matching_managed_launcher_hook_stays_single(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "matching-hook-directory"
    source.mkdir()
    (source / "run.py").write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "wrapped-prog",
            "--json",
            "-d",
            str(source),
            "--",
            "python",
            "run.py",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls[0][1]["command"] == [
        "/cluster/runtime/python",
        "run.py",
    ]


def test_scheduler_missing_resolved_executable_fails_before_execution(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "unresolved-directory"
    source.mkdir()
    (source / "run.py").write_text("pass\n", encoding="utf-8")

    def unresolved(*_args: object, **_kwargs: object) -> ProgramRuntimePin:
        return ProgramRuntimePin(
            expected_git_sha=EXPECTED_SHA,
            scheduler_host="host_f",
            program_kind="scheduler-runtime",
        )

    monkeypatch.setattr(
        submit_module,
        "_validate_scheduler_target_expected_sha",
        unresolved,
    )

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "-d",
            str(source),
            "--",
            "python",
            "run.py",
        ],
    )

    assert result.exit_code == 2
    assert "resolved_executable" in result.output
    assert plan_harness.calls == []


def test_scheduler_runtime_rejection_precedes_remote_execution(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "rejected.py"
    source.write_text("pass\n", encoding="utf-8")
    config_path = plan_harness.root / "cfg" / "config.toml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            'scheduler_driver = "driver"',
            'scheduler_driver = "missing-driver"',
            1,
        ),
        encoding="utf-8",
    )

    def reject(*_args: object, **_kwargs: object) -> ProgramRuntimePin:
        raise ValueError("target runtime mismatch")

    monkeypatch.setattr(
        cli_module,
        "_validate_scheduler_target_expected_sha",
        reject,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_scheduler_target_expected_sha",
        reject,
        raising=False,
    )

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "host_f",
            "--program",
            "remote-only-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--python",
            "/cluster/release/python",
            str(source),
        ],
    )

    assert result.exit_code == 2
    assert result.output.rstrip().splitlines()[-1] == (
        "Error: target runtime mismatch"
    )
    assert "missing-driver" not in result.output
    assert plan_harness.calls == []


def test_hidden_delegated_local_plan(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "delegated.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "localhost",
            "--scheduler-target",
            "host_f",
            "--program",
            "matrix-prog",
            "--expected-sha",
            EXPECTED_SHA,
            "--python",
            "/cluster/runtime/python",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                input_file=str(source),
                python="/cluster/runtime/python",
                program="matrix-prog",
                program_runtime_pin=plan_harness.scheduler_pin,
                scheduler_target="host_f",
            ),
        )
    ]
    receipt = _receipt(
        "localhost",
        [LOCAL_JOBID],
        scheduler_pin=plan_harness.scheduler_pin,
    )
    assert json.loads(result.stdout) == receipt
    assert plan_harness.scheduler_pin_calls == [
        (
            "host_f",
            "matrix-prog",
            EXPECTED_SHA,
            ("/cluster/runtime/python",),
        )
    ]


@pytest.mark.parametrize(
    ("target", "limit"),
    [("host_f", 7_200), ("local-host_f", 3_600)],
)
def test_scheduler_wall_time_limit_accepts_equality(
    plan_harness: PlanHarness,
    target: str,
    limit: int,
) -> None:
    source = plan_harness.root / f"{target}-equal.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            target,
            "--wall-time-seconds",
            str(limit),
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(plan_harness.calls) == 1


@pytest.mark.parametrize(
    ("target", "limit"),
    [("host_f", 7_200), ("local-host_f", 3_600)],
)
def test_scheduler_wall_time_limit_rejects_before_backend_mutation(
    plan_harness: PlanHarness,
    target: str,
    limit: int,
) -> None:
    source = plan_harness.root / f"{target}-over.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            target,
            "--array",
            "2",
            "--wall-time-seconds",
            str(limit + 1),
            str(source),
        ],
    )

    assert result.exit_code == 2
    assert f"allows at most {limit} s" in result.output
    assert plan_harness.calls == []
    assert list(paths.queue_dir().glob("*.json")) == []


def test_hidden_scheduler_target_revalidates_wall_time_on_driver(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "hidden-over.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "localhost",
            "--scheduler-target",
            "host_f",
            "--wall-time-seconds",
            "7201",
            str(source),
        ],
    )

    assert result.exit_code == 2
    assert "scheduler lane 'host_f'" in result.output
    assert "allows at most 7200 s" in result.output
    assert plan_harness.calls == []
    assert list(paths.queue_dir().glob("*.json")) == []


def test_direct_local_observational_pin_is_not_a_receipt_pin(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "observed.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "localhost",
            "--program",
            "observed-prog",
            "--python",
            "/local/observed/python",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                input_file=str(source),
                python="/local/observed/python",
                program="observed-prog",
                program_runtime_pin=plan_harness.observed_pin,
            ),
        )
    ]
    assert plan_harness.observed_pin.enforce_git_sha is False
    assert json.loads(result.stdout) == _receipt("localhost", [LOCAL_JOBID])
    assert plan_harness.scheduler_pin_calls == []


def test_unpinned_scheduler_local_driver_uses_target_runtime_pin(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "scheduler-observed.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "local-host_f",
            "--program",
            "observed-prog",
            "--python",
            "/local-cluster/runtime/python",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                input_file=str(source),
                python="/local-cluster/runtime/python",
                program="observed-prog",
                program_runtime_pin=plan_harness.local_scheduler_pin,
                scheduler_target="local-host_f",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt(
        "local-host_f",
        [LOCAL_JOBID],
        scheduler_pin=plan_harness.local_scheduler_pin,
        scheduler_driver="localhost",
    )
    assert plan_harness.scheduler_pin_calls == [
        (
            "local-host_f",
            "observed-prog",
            None,
            ("/local-cluster/runtime/python",),
        )
    ]


def test_scheduler_directory_program_uses_target_runtime_pin(
    plan_harness: PlanHarness,
) -> None:
    """A shell entrypoint must not downgrade --program to a local observation."""
    source = plan_harness.root / "scheduler-directory"
    source.mkdir()
    (source / "run.sh").write_text("exit 0\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "local-host_f",
            "--dir",
            str(source),
            "--program",
            "observed-prog",
            "--json",
            "--",
            "bash",
            "run.sh",
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                directory=str(source),
                command=["bash", "run.sh"],
                program="observed-prog",
                program_runtime_pin=plan_harness.local_scheduler_pin,
                scheduler_target="local-host_f",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt(
        "local-host_f",
        [LOCAL_JOBID],
        scheduler_pin=plan_harness.local_scheduler_pin,
        scheduler_driver="localhost",
    )
    assert plan_harness.scheduler_pin_calls == [
        ("local-host_f", "observed-prog", None, ("bash",))
    ]
    assert plan_harness.local_pin_calls == []


def test_unpinned_scheduler_qvf_uses_target_pin_in_spec_and_receipt(
    plan_harness: PlanHarness,
) -> None:
    source = plan_harness.root / "scheduler.qvf"
    source.write_bytes(b"qvf contract")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "--host",
            "local-host_f",
            "--program",
            "matrix-prog",
            "--json",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    assert plan_harness.calls == [
        (
            "local",
            _local_single_kwargs(
                input_file=str(source),
                program="matrix-prog",
                program_runtime_pin=plan_harness.local_scheduler_pin,
                scheduler_target="local-host_f",
            ),
        )
    ]
    assert json.loads(result.stdout) == _receipt(
        "local-host_f",
        [LOCAL_JOBID],
        scheduler_pin=plan_harness.local_scheduler_pin,
        scheduler_driver="localhost",
    )
    assert plan_harness.scheduler_pin_calls == [
        ("local-host_f", "matrix-prog", None, ())
    ]


@pytest.fixture
def validation_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '\n'.join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.localhost.branches]",
                'release = "/local/release/python"',
                "",
                "[hosts.remote]",
                'ssh = "remote.invalid"',
                "",
                "[pools.compute]",
                'hosts = ["remote"]',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path


@pytest.mark.parametrize(
    ("case", "expected_error"),
    [
        (
            "pool_without_auto",
            "Error: --pool only applies to `vq submit auto` (it scopes the "
            "memory-aware host pick to a [pools.POOL] group).",
        ),
        (
            "branch_and_forwarded_branch",
            "Error: --branch and --branch-name are mutually exclusive "
            "(--branch-name is internal-only, forwarded by `submit_remote` "
            "after laptop-side --branch resolution; operators should use "
            "--branch)",
        ),
        (
            "qvf_force_on_python",
            "Error: --qvf-force only applies to a single .qvf input",
        ),
        (
            "qvf_array",
            "Error: single-QVF submit does not support --array/--chain; "
            "submit independent container files so each result artifact is "
            "unique",
        ),
        (
            "empty_expected_sha",
            "Error: --expected-sha requires a non-empty git SHA",
        ),
        (
            "nonhex_expected_sha",
            "Error: --expected-sha 'not-a-sha' is invalid: expected a hex "
            "git SHA",
        ),
    ],
)
def test_submit_validation_contract(
    validation_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_error: str,
) -> None:
    python_source = validation_state / "input.py"
    python_source.write_text("pass\n", encoding="utf-8")
    qvf_source = validation_state / "input.qvf"
    qvf_source.write_bytes(b"qvf contract")
    backend_calls: list[str] = []

    def backend(name: str):
        def call(**_kwargs: object) -> str | list[str]:
            backend_calls.append(name)
            return LOCAL_JOBID if name == "local" else ["000000000000"]

        return call

    monkeypatch.setattr(submit_module, "submit_local", backend("local"))
    monkeypatch.setattr(
        submit_module,
        "submit_local_array",
        backend("array"),
    )
    monkeypatch.setattr(
        submit_module,
        "submit_local_chain",
        backend("chain"),
    )
    monkeypatch.setattr(submit_module, "submit_remote", backend("remote"))

    args_by_case = {
        "pool_without_auto": [
            "submit",
            "--host",
            "remote",
            "--pool",
            "compute",
            str(python_source),
        ],
        "branch_and_forwarded_branch": [
            "submit",
            "--host",
            "localhost",
            "--branch",
            "release",
            "--branch-name",
            "forwarded",
            str(python_source),
        ],
        "qvf_force_on_python": [
            "submit",
            "--host",
            "localhost",
            "--qvf-force",
            str(python_source),
        ],
        "qvf_array": [
            "submit",
            "--host",
            "localhost",
            "--array",
            "2",
            str(qvf_source),
        ],
        "empty_expected_sha": [
            "submit",
            "--host",
            "remote",
            "--program",
            "matrix-prog",
            "--expected-sha",
            "",
            str(python_source),
        ],
        "nonhex_expected_sha": [
            "submit",
            "--host",
            "remote",
            "--program",
            "matrix-prog",
            "--expected-sha",
            "not-a-sha",
            str(python_source),
        ],
    }

    result = CliRunner().invoke(main, args_by_case[case])

    assert result.exit_code == 2
    assert result.output.rstrip().splitlines()[-1] == expected_error
    assert backend_calls == []
    assert list(paths.queue_dir().glob("*.json")) == []


def test_submit_down_host_contract(
    validation_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = validation_state / "down.py"
    source.write_text("pass\n", encoding="utf-8")
    backend_calls: list[str] = []
    entry = cli_module.host_status.DownEntry(
        host="remote",
        reason="maintenance",
        since="2026-08-02T12:00:00+00:00",
    )
    monkeypatch.setattr(
        cli_module.host_status,
        "is_down",
        lambda host: entry if host == "remote" else None,
    )
    monkeypatch.setattr(
        submit_module,
        "submit_remote",
        lambda **_kwargs: backend_calls.append("remote") or ["000000000000"],
    )

    result = CliRunner().invoke(
        main,
        ["submit", "--host", "remote", str(source)],
    )

    assert result.exit_code == 1
    assert result.output.rstrip().splitlines()[-1] == (
        "Error: host 'remote' is marked administratively down (maintenance; "
        "since 2026-08-02T12:00:00+00:00). Run `vq host up remote` to clear "
        "it, or submit to a different host."
    )
    assert backend_calls == []


def test_unknown_auto_pool_is_a_config_failure_not_a_usage_error(
    plan_harness: PlanHarness,
) -> None:
    """A pool that is not defined is a problem with the config file, so it
    must not be reported as a mistake on the command line.

    This used to be pinned as ``isinstance(result.exception, ConfigError)``,
    back when the exception escaped click and the user got a traceback. The
    root group now renders it (``cli._ConfigErrorGroup``), so the same
    distinction is read off the rendering: exit 1 and a bare ``Error:``,
    where a UsageError would be exit 2 behind a usage block.
    """
    source = plan_harness.root / "unknown-pool.py"
    source.write_text("pass\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        ["submit", "--pool", "missing", "auto", str(source)],
    )

    assert result.exit_code == 1
    assert "Usage:" not in result.stderr
    assert "unknown pool 'missing'" in result.stderr
    assert result.stderr.startswith("Error: ")
    assert plan_harness.calls == []


def test_late_backend_config_failure_is_not_reclassified_as_usage(
    plan_harness: PlanHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = plan_harness.root / "late-config.py"
    source.write_text("pass\n", encoding="utf-8")

    def fail_backend(*_args: object, **_kwargs: object) -> list[str]:
        raise config.ConfigError("late backend config failure")

    monkeypatch.setattr(submit_module, "submit_local", fail_backend)

    result = CliRunner().invoke(
        main,
        ["submit", "--host", "localhost", str(source)],
    )

    assert result.exit_code == 1
    # Same distinction as above, read off the rendering rather than the
    # escaped exception: exit 1 and a bare Error:, not a usage error.
    assert "Usage:" not in result.stderr
    assert result.stderr == "Error: late backend config failure\n"
    assert plan_harness.calls == []


@pytest.mark.parametrize(
    ("extra_args", "expected_error"),
    [
        (
            ["--python", "/local/python"],
            "Error: --python/--branch cannot be used with a QVF input; use "
            "--program NAME",
        ),
        (
            [],
            "Error: single-QVF submit requires --program NAME so vq can "
            "resolve the managed vibe-qc runtime",
        ),
    ],
)
def test_qvf_backend_validation_contract(
    validation_state: Path,
    extra_args: list[str],
    expected_error: str,
) -> None:
    source = validation_state / "job.qvf"
    source.write_bytes(b"qvf contract")

    result = CliRunner().invoke(
        main,
        ["submit", "--host", "localhost", *extra_args, str(source)],
    )

    assert result.exit_code == 2
    assert result.output.rstrip().splitlines()[-1] == expected_error
    assert list(paths.queue_dir().glob("*.json")) == []

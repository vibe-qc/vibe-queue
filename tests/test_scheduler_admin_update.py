"""Scheduler-host provisioning through ``vq admin update HOST``."""
from __future__ import annotations

import json
import math
import os
import subprocess
import time
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from vq import admin, config, paths, transport
from vq.cli import main
from vq.scheduler_dialect import SchedulerPhase
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, -1.0])
def test_scheduler_update_rejects_invalid_drain_wait_before_work(
    monkeypatch: pytest.MonkeyPatch,
    value: float,
) -> None:
    def unexpected_project_root() -> Path:
        raise AssertionError("scheduler update work started")

    monkeypatch.setattr(
        admin, "_scheduler_helper_project_root", unexpected_project_root
    )

    with pytest.raises(
        admin.AdminError, match="drain_wait_seconds must be finite and >= 0"
    ):
        admin.update_scheduler_host(
            "host_f",
            config.Config(),
            drain_wait_seconds=value,
        )


@pytest.mark.parametrize("value", [0.0, 0.01, 3600.0])
def test_scheduler_drain_wait_boundary_accepts_finite_nonnegative(
    value: float,
) -> None:
    admin._validate_drain_wait_seconds(value)


@contextmanager
def _recording_lifecycle_fence(
    calls: list[tuple[list[config.VenvProgram], str, tuple[tuple[str, str], ...]]],
    progs: list[config.VenvProgram],
    *,
    action: str,
    extra_resources: tuple[tuple[str, str], ...] = (),
):
    calls.append((progs, action, extra_resources))
    yield object()


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)

    def fake_stage(host, host_cfg, command_host_cfg, result, *, expected_sha=None):  # type: ignore[no-untyped-def]
        result.stage_root = "/shared/vq-admin/host_f"
        result.stage_path = (
            f"{result.stage_root}/generations/{_DRIVER_SHA}-{'1' * 32}"
        )
        result.stage_uploaded = True
        result.archive_sha256 = "34" * 32
        result.expected_source_sha = _DRIVER_SHA
        result.expected_source_tree_sha256 = _TREE_SHA256
        return result.stage_path

    monkeypatch.setattr("vq.admin._stage_scheduler_helper_source", fake_stage)
    # Hermetic default: the reconcile probe never opens a socket in tests.
    # "Every id still known" reproduces the pre-reconcile census, so tests that
    # predate reconciliation keep their exact expectations; tests that care
    # about reaping override this.
    monkeypatch.setattr(
        "vq.admin._poll_scheduler_phases",
        lambda host_cfg, specs: {
            str(s.scheduler_job_id): SchedulerPhase.RUNNING for s in specs
        },
    )
    return tmp_path


def _write_scheduler_config(
    cfg_dir: Path,
    *,
    driver: str = "localhost",
    update_host: str | None = None,
    program_hook: str | None = None,
    register_program: bool = False,
    runtime_program: str | None = None,
) -> None:
    lines = [
        "[hosts.localhost]" if driver == "localhost" else "[hosts.driver]",
        'ssh = "localhost"' if driver == "localhost" else 'ssh = "driver"',
        "",
        "[hosts.host_f]",
        'ssh = "host_f-login"',
        'scheduler = "pbs"',
        'scheduler_dialect = "torque"',
        'scratch_root = "/home/USER"',
        f'scheduler_driver = "{driver}"',
        'fleet_role = "managed"',
        'scheduler_update_command = "/home/USER/vibeqc-dev/scripts/update_cluster.sh --release"',
    ]
    if update_host:
        lines.append(f'scheduler_update_host = "{update_host}"')
    lines.extend(
        [
            'scheduler_install_command = '
            '"/home/USER/vibeqc-dev/scripts/install_cluster.sh --release"',
            "scheduler_update_timeout_seconds = 123",
            "",
        ]
    )
    if runtime_program is not None:
        lines.extend(
            [
                f"[hosts.host_f.scheduler_runtime_deployments.{runtime_program}]",
                'update_command = "/site/bin/deploy-runtime"',
                'install_command = "/site/bin/install-runtime"',
                'update_host = "cluster-build"',
                'verify_command = "/site/bin/verify-runtime"',
                "timeout_seconds = 321",
                "",
            ]
        )
        register_program = True
        program_hook = runtime_program
    if program_hook is not None:
        lines.extend(
            [
                f"[hosts.host_f.scheduler_program_hooks.{program_hook}]",
                'prologue = ["source /home/USER/orca-env.sh"]',
                "",
            ]
        )
    if register_program:
        registered_program = program_hook or "orca"
        lines.extend(
            [
                f"[programs.{registered_program}]",
                'kind = "binary"',
                'binary = "/opt/orca/orca"',
                "",
            ]
        )
    (cfg_dir / "config.toml").write_text("\n".join(lines))


# Fixed driver identities for the scheduler-helper staging and provenance
# contract. The fixtures keep scheduler-admin tests hermetic without a real
# checkout or live SSH target.
_DRIVER_SHA = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
_TREE_SHA256 = "12" * 32
_STAGE_ROOT = "/shared/vq-admin/host_f"
_STAGE_PATH = f"{_STAGE_ROOT}/generations/{_DRIVER_SHA}-{'1' * 32}"


def _staged_update_argv(*args: str) -> tuple[str, ...]:
    return (
        "env",
        f"VQ_SCHEDULER_STAGE={_STAGE_PATH}",
        f"VQ_SCHEDULER_EXPECTED_SOURCE_SHA={_DRIVER_SHA}",
        f"VQ_SCHEDULER_EXPECTED_TREE_SHA256={_TREE_SHA256}",
        *args,
    )


def _fake_helper_provenance(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
    if vq_args == ("--version",):
        return subprocess.CompletedProcess(
            args=["vq", *vq_args], returncode=0, stdout="vq 0.12.0\n", stderr=""
        )
    if vq_args == ("source-tree-sha256",):
        return subprocess.CompletedProcess(
            args=["vq", *vq_args], returncode=0, stdout=f"{_TREE_SHA256}\n", stderr=""
        )
    if vq_args == ("source-sha",):
        return subprocess.CompletedProcess(
            args=["vq", *vq_args],
            returncode=0,
            stdout=f"{_DRIVER_SHA}\n",
            stderr="",
        )
    # A routine helper update is READ-ONLY on the remote host: `--version`,
    # then the two provenance digests, and nothing else. In particular it must
    # never invoke `source-stage-prune` -- see docs/operations.md, "Managed
    # helper updates retain staging generations": successful activation does
    # not authorize cleanup, because another deployment may still be using an
    # older generation, possibly on a different host. Any other verb reaching
    # this double is a regression, so fail loudly rather than answer it.
    raise AssertionError(
        f"routine scheduler update made an unexpected remote vq call: {vq_args}"
    )


@pytest.fixture
def stub_helper_provenance(monkeypatch: pytest.MonkeyPatch) -> str:
    """Stub helper readiness and read-only provenance verification."""
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.transport.run_remote_vq", _fake_helper_provenance)
    return _DRIVER_SHA


def test_source_tree_sha256_ignores_markers_and_bytecode(tmp_path: Path) -> None:
    package = tmp_path / "vq"
    package.mkdir()
    (package / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package / "schema.json").write_text('{"version": 1}\n', encoding="utf-8")
    first = admin.source_tree_sha256(package)

    (package / "SOURCE-SHA").write_text("a" * 40 + "\n", encoding="utf-8")
    (package / "SOURCE-TREE-SHA256").write_text("b" * 64 + "\n", encoding="utf-8")
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "__init__.pyc").write_bytes(b"generated")
    assert admin.source_tree_sha256(package) == first

    (package / "__init__.py").write_text("VALUE = 2\n", encoding="utf-8")
    assert admin.source_tree_sha256(package) != first


def test_scheduler_stage_prune_is_bounded_and_preserves_unknown_entries(
    tmp_path: Path,
) -> None:
    stage_root = tmp_path / "stage"
    generations = stage_root / "generations"
    generations.mkdir(parents=True)
    candidates: list[Path] = []
    for index in range(4):
        candidate = generations / f"{'a' * 40}-{index:032x}"
        candidate.mkdir()
        (candidate / "archive").write_text(str(index), encoding="utf-8")
        timestamp = 100 + index
        os.utime(candidate, (timestamp, timestamp))
        candidates.append(candidate)
    unknown = generations / "manual-forensics"
    unknown.mkdir()
    symlink = generations / f"{'b' * 40}-{'1' * 32}"
    symlink.symlink_to(candidates[-1], target_is_directory=True)

    result = admin.prune_scheduler_stage_generations(
        stage_root,
        keep=2,
        preserve=candidates[0],
    )

    assert set(result.retained) == {str(candidates[0]), str(candidates[-1])}
    assert set(result.removed) == {str(candidates[1]), str(candidates[2])}
    assert unknown.is_dir()
    assert symlink.is_symlink()
    assert set(result.skipped_unrecognized) == {str(unknown), str(symlink)}


def test_scheduler_stage_prune_refuses_symlinked_generations_root(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    stage_root = tmp_path / "stage"
    stage_root.mkdir()
    (stage_root / "generations").symlink_to(outside, target_is_directory=True)

    with pytest.raises(admin.AdminError, match="must not be a symlink"):
        admin.prune_scheduler_stage_generations(stage_root)


def test_scheduler_helper_stage_is_refreshed_from_driver_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "vibe-queue"
    package = project / "src" / "vq"
    package.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "vq"\nversion = "0.12.0"\n', encoding="utf-8"
    )
    (package / "__init__.py").write_text("VALUE = 'fresh'\n", encoding="utf-8")
    uploaded: list[tuple[str, bytes]] = []
    remote_calls: list[tuple[str, ...]] = []

    monkeypatch.setattr("vq.admin.current_source_sha", lambda *a, **k: _DRIVER_SHA)
    monkeypatch.setattr("vq.admin._scheduler_helper_project_root", lambda: project)

    def fake_archive(project_root, archive, source_sha, *, require_clean_tree=True):  # type: ignore[no-untyped-def]
        assert project_root == project
        assert source_sha == _DRIVER_SHA
        archive.write_bytes(b"fresh driver archive")
        return "ab" * 32

    def fake_upload(host_cfg, local_path, remote_path, **kwargs):  # type: ignore[no-untyped-def]
        uploaded.append((remote_path, local_path.read_bytes()))

    def fake_remote(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        remote_calls.append(argv)
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="stage ok\n", stderr=""
        )

    monkeypatch.setattr("vq.admin._scheduler_helper_archive", fake_archive)
    monkeypatch.setattr("vq.admin.transport.upload_file", fake_upload)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake_remote)
    host_cfg = config.HostConfig(
        ssh="cluster",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scheduler_driver="localhost",
        scratch_root="/shared/jobs",
        scheduler_update_stage="/shared/vq-admin/cluster",
    )
    result = admin.SchedulerHostUpdateResult(
        host="cluster",
        ssh="cluster",
        scheduler="slurm",
        mode="update",
        command="update-helper",
    )

    stage = admin._stage_scheduler_helper_source(
        "cluster", host_cfg, host_cfg, result
    )

    assert stage.startswith(
        f"/shared/vq-admin/cluster/generations/{_DRIVER_SHA}-"
    )
    assert result.stage_uploaded is True
    assert result.expected_source_sha == _DRIVER_SHA
    assert result.expected_source_tree_sha256 == admin.source_tree_sha256(package)
    assert result.archive_sha256 == "ab" * 32
    assert len(uploaded) == 4
    assert {Path(path).name for path, _ in uploaded} == {
        "vibe-queue-src.tar.gz",
        "SOURCE-SHA",
        "SOURCE-TREE-SHA256",
        "ARCHIVE-SHA256",
    }
    assert any(data == b"fresh driver archive" for _, data in uploaded)
    assert remote_calls[0] == ("mkdir", "-p", stage)


def test_update_scheduler_host_runs_configured_remote_command(
    state_dir: Path,
    stub_helper_provenance: str,
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        assert host_cfg.ssh == "host_f-login"
        assert argv == _staged_update_argv(
            "/home/USER/vibeqc-dev/scripts/update_cluster.sh", "--release", "--wait"
        )
        assert kwargs["check"] is False
        assert kwargs["timeout"] == 123
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="submitted build\n", stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_host(
            "host_f", cfg, update_script_args=["--wait"]
        )

    assert result.success is True
    assert result.command_rc == 0
    assert "submitted build" in result.command_output
    assert result.marker_cleared is True
    assert admin.admin_update_marker_exists() is False


def test_update_scheduler_host_runs_command_on_configured_build_host(
    state_dir: Path,
    stub_helper_provenance: str,
) -> None:
    _write_scheduler_config(state_dir / "cfg", update_host="cluster-build")
    cfg = config.load_config()

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        assert host_cfg.ssh == "cluster-build"
        assert argv == _staged_update_argv(
            "/home/USER/vibeqc-dev/scripts/update_cluster.sh", "--release"
        )
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="compiled on build host\n", stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True
    assert result.ssh == "host_f-login"
    assert result.command_ssh == "cluster-build"
    text = admin.format_scheduler_update_result(result)
    assert "ssh:           host_f-login" in text
    assert "update ssh:    cluster-build" in text


def test_verified_scheduler_update_never_prunes_staging(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A routine update must not clean up staging generations.

    docs/operations.md is explicit: "Successful activation does not authorize
    cleanup: another deployment may still be using an older stage." The prune
    primitive is separate and requires establishing that no deployment is in
    flight -- a verified activation is not that proof, and pruning here could
    remove a generation another host is still running from.

    This test previously asserted the opposite (that a failing prune surfaced
    as a maintenance warning), which contradicted both the policy and
    test_admin.py's read-only provenance test.
    """
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )

    calls: list[tuple[str, ...]] = []

    def record(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(vq_args)
        return _fake_helper_provenance(host_cfg, *vq_args, **kwargs)

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", record)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True
    assert result.marker_cleared is True
    assert not any(args and args[0] == "source-stage-prune" for args in calls)


def test_update_scheduler_runtime_builds_verifies_and_records_last_ok(
    state_dir: Path,
) -> None:
    program = "vibeqc-release"
    sha = "b" * 40
    tag = "v0.15.45"
    _write_scheduler_config(
        state_dir / "cfg", runtime_program=program
    )
    cfg = config.load_config()
    calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((host_cfg.ssh, argv))
        if host_cfg.ssh == "cluster-build":
            assert kwargs["timeout"] == 321
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="activated\n", stderr=""
            )
        receipt = {
            "program": program,
            "source_sha": sha,
            "tag": tag,
            "healthy": True,
            "activation": "atomic",
            "active_path": "/opt/vq-runtimes/vibeqc-release/current",
            "health_detail": "import and banner ok",
            "quiescent": True,
            "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_f",
            program,
            cfg,
            expected_sha=sha,
            expected_tag=tag,
            update_script_args=["--recreate"],
        )

    identity = ("--program", program, "--expected-sha", sha, "--tag", tag)
    assert calls == [
        (
            "cluster-build",
            ("/site/bin/deploy-runtime", "--recreate", *identity),
        ),
        ("host_f-login", ("/site/bin/verify-runtime", *identity)),
    ]
    assert result.success is True
    assert result.actual_sha == sha
    assert result.activation == "atomic"
    assert result.quiescent is True
    assert result.updater_pid is None
    assert result.marker_cleared is True
    assert admin.admin_update_marker_exists() is False
    records = admin.load_scheduler_runtime_status()
    record = records[f"host_f:{program}"]
    assert record.last_success is True
    assert record.actual_tag == tag
    assert record.active_path.endswith("/current")
    assert record.quiescent is True
    assert record.updater_pid is None


def _write_slurm_allocation_config(
    cfg_dir: Path,
    *,
    program: str,
    verify_timeout_seconds: float | None = 900,
) -> None:
    """A daemonless SLURM host whose runtime builds in a per-deployment
    allocation (host_c shape) instead of on a fixed SSH build host (host_f)."""
    lines = [
        "[hosts.localhost]",
        'ssh = "localhost"',
        "",
        "[hosts.host_c]",
        'ssh = "host_c-login"',
        'scheduler = "slurm"',
        'scheduler_dialect = "slurm"',
        'scratch_root = "/home/USER"',
        'scheduler_driver = "localhost"',
        'fleet_role = "managed"',
        'submit_extra = ["--account", "grp", "--partition", "short_queue"]',
        "",
        f"[hosts.host_c.scheduler_runtime_deployments.{program}]",
        'update_command = "/site/bin/deploy-host_c-runtime"',
        'verify_command = "srun --account=grp --partition=short_queue '
        '--time=00:05:00 --ntasks=1 /site/bin/verify-host_c-runtime"',
        "timeout_seconds = 5400",
    ]
    if verify_timeout_seconds is not None:
        lines.append(f"verify_timeout_seconds = {verify_timeout_seconds}")
    lines += [
        "",
        f"[hosts.host_c.scheduler_runtime_deployments.{program}.update_allocation]",
        'scheduler = "slurm"',
        'sbatch_args = ["--account=grp", "--partition=build", "--time=02:00:00", '
        '"--ntasks=1", "--cpus-per-task=8", "--mem=16G"]',
        "",
        f"[programs.{program}]",
        'kind = "binary"',
        'binary = "/opt/vibeqc/python"',
        "",
    ]
    (cfg_dir / "config.toml").write_text("\n".join(lines))


def test_update_scheduler_runtime_builds_in_slurm_allocation(
    state_dir: Path,
) -> None:
    program = "vibeqc-release"
    sha = "b" * 40
    _write_slurm_allocation_config(state_dir / "cfg", program=program)
    cfg = config.load_config()
    calls: list[tuple[str, tuple[str, ...], float | None]] = []

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((host_cfg.ssh, argv, kwargs.get("timeout")))
        if any("deploy-host_c-runtime" in tok for tok in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="activated\n", stderr=""
            )
        receipt = {
            "program": program,
            "source_sha": sha,
            "tag": None,
            "healthy": True,
            "activation": "atomic",
            "active_path": "/home/USER/runtimes/vibeqc-release/current",
            "health_detail": "in-node import and banner ok",
            "quiescent": True,
            "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_c", program, cfg, expected_sha=sha
        )

    identity = ("--program", program, "--expected-sha", sha)
    # Build runs on the login host as a `bash -c` orchestration that submits the
    # deploy command via `sbatch --parsable --wait` with the allocation args, so
    # the compile lands on a compute node and survives an SSH blip.
    build_ssh, build_argv, build_timeout = calls[0]
    assert build_ssh == "host_c-login"
    assert build_argv[0] == "bash" and build_argv[1] == "-c"
    script = build_argv[2]
    assert "sbatch --parsable --wait" in script
    assert "--partition=build" in script and "--mem=16G" in script
    assert "/site/bin/deploy-host_c-runtime" in script
    assert "--expected-sha " + sha in script
    assert '--output="$out"' in script and '--error="$err"' in script
    assert build_timeout == 5400
    # Verify runs on the login host with the lifted timeout so it can wait for
    # its own allocation before running the in-node import check.
    verify_ssh, verify_argv, verify_timeout = calls[1]
    assert verify_ssh == "host_c-login"
    assert verify_argv[0] == "srun"
    assert verify_argv[-len(identity) :] == identity
    assert verify_timeout == 900
    assert result.success is True
    assert result.actual_sha == sha
    # The result shows the readable deploy command, not the sbatch wrapper.
    assert result.command.startswith("/site/bin/deploy-host_c-runtime")
    assert result.marker_cleared is True


def test_scheduler_runtime_verify_timeout_defaults_to_legacy_cap(
    state_dir: Path,
) -> None:
    program = "vibeqc-dev"
    # No verify_timeout_seconds: the legacy min(timeout, 60) cap must still hold
    # so host_f's cheap in-line verify is unaffected.
    _write_slurm_allocation_config(
        state_dir / "cfg", program=program, verify_timeout_seconds=None
    )
    cfg = config.load_config()
    deployment = cfg.hosts["host_c"].scheduler_runtime_deployments[program]
    assert admin._scheduler_verify_timeout(deployment) == 60.0


def test_slurm_allocation_requires_matching_host_scheduler(
    state_dir: Path,
) -> None:
    # A build allocation on a non-SLURM (PBS) host is a config error.
    cfg_dir = state_dir / "cfg"
    cfg_dir.joinpath("config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy"',
                'verify_command = "/site/bin/verify"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release."
                "update_allocation]",
                'scheduler = "slurm"',
                'sbatch_args = ["--partition=build"]',
                "",
            ]
        )
    )
    with pytest.raises(config.ConfigError, match="must target this host's own"):
        config.load_config()


def _write_stage_source_config(cfg_dir: Path, *, program: str, repo: str) -> None:
    cfg_dir.joinpath("config.toml").write_text(
        "\n".join(
            [
                f'scheduler_runtime_source_repo = "{repo}"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_c]",
                'ssh = "host_c-login"',
                'scheduler = "slurm"',
                'scheduler_dialect = "slurm"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'fleet_role = "managed"',
                'submit_extra = ["--account", "grp", "--partition", "p"]',
                "",
                f"[hosts.host_c.scheduler_runtime_deployments.{program}]",
                'update_command = "/site/bin/deploy-host_c-runtime"',
                'verify_command = "srun /site/bin/verify-host_c-runtime"',
                "stage_source = true",
                "verify_timeout_seconds = 900",
                "timeout_seconds = 5400",
                "",
                f"[hosts.host_c.scheduler_runtime_deployments.{program}."
                "update_allocation]",
                'scheduler = "slurm"',
                'sbatch_args = ["--partition=build", "--time=02:00:00"]',
                "",
                f"[programs.{program}]",
                'kind = "binary"',
                'binary = "/opt/vibeqc/python"',
                "",
            ]
        )
    )


def test_update_scheduler_runtime_stages_source_and_passes_archive(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    program = "vibeqc-release"
    sha = "b" * 40
    _write_stage_source_config(state_dir / "cfg", program=program, repo="/repo")
    cfg = config.load_config()
    staged = f"/home/USER/.vq-admin/runtime-source/{program}/{sha}-tok/src.tar.gz"

    def fake_stage(host, command_host_cfg, prog, expected_sha, cfg_, result):  # type: ignore[no-untyped-def]
        assert expected_sha == sha
        result.staged_source_archive = staged
        return staged

    monkeypatch.setattr("vq.admin._stage_scheduler_runtime_source", fake_stage)
    monkeypatch.setattr(
        "vq.admin._canonical_lifecycle_checkout",
        lambda _path: Path("/repo"),
    )
    calls: list[tuple[str, ...]] = []

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(argv)
        if any("deploy-host_c-runtime" in tok for tok in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="ok\n", stderr=""
            )
        receipt = {
            "program": program, "source_sha": sha, "tag": None, "healthy": True,
            "activation": "atomic", "active_path": "/rt/current",
            "health_detail": "ok", "quiescent": True, "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_c", program, cfg, expected_sha=sha
        )

    build_script = next(a[2] for a in calls if a[:2] == ("bash", "-c"))
    assert f"--source-archive {staged}" in build_script
    assert result.staged_source_archive == staged
    assert result.success is True


@pytest.mark.parametrize(
    ("program", "expected_deploy_tag"),
    [
        ("vibeqc-release", "v0.15.138"),
        ("vibeqc-dev", None),
        ("vibe-view", None),
    ],
)
def test_scheduler_runtime_auto_resolves_tag_only_for_release_lane(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    program: str,
    expected_deploy_tag: str | None,
) -> None:
    """#288 keeps release provenance; #535 keeps branch lanes untagged."""
    repo = state_dir / "src"
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True, capture_output=True, text=True,
        )

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "hello.txt").write_text("hi\n", encoding="utf-8")
    git("add", "hello.txt")
    git("commit", "-q", "-m", "init")
    sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    git("tag", "v0.15.138")

    _write_stage_source_config(state_dir / "cfg", program=program, repo=str(repo))
    cfg = config.load_config()
    staged = f"/home/USER/.vq-admin/runtime-source/{program}/{sha}-tok/src.tar.gz"

    def fake_stage(host, command_host_cfg, prog, expected_sha, cfg_, result):  # type: ignore[no-untyped-def]
        assert expected_sha == sha
        result.staged_source_archive = staged
        return staged

    monkeypatch.setattr("vq.admin._stage_scheduler_runtime_source", fake_stage)
    monkeypatch.setattr(
        "vq.admin._canonical_lifecycle_checkout",
        lambda _path: repo,
    )
    calls: list[tuple[str, ...]] = []

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(argv)
        if any("deploy-host_c-runtime" in tok for tok in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="ok\n", stderr=""
            )
        receipt = {
            "program": program, "source_sha": sha, "tag": expected_deploy_tag,
            "healthy": True, "activation": "atomic",
            "active_path": "/rt/current", "health_detail": "ok",
            "quiescent": True, "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_c", program, cfg, expected_sha=sha
        )

    assert result.expected_tag == expected_deploy_tag
    build_script = next(a[2] for a in calls if a[:2] == ("bash", "-c"))
    verify_argv = next(
        a for a in calls if any("verify-host_c-runtime" in tok for tok in a)
    )
    if expected_deploy_tag is None:
        assert "--tag" not in build_script
        assert "--tag" not in verify_argv
    else:
        assert f"--tag {expected_deploy_tag}" in build_script
        assert "--tag" in verify_argv and expected_deploy_tag in verify_argv
    assert result.success is True


def test_scheduler_runtime_deploy_without_resolvable_tag_warns_and_keeps_null(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """IID 288: an unresolvable tag keeps the manifest null and logs a
    warning naming program, host, and SHA (never synthesised)."""
    program = "vibeqc-release"
    sha = "b" * 40
    _write_slurm_allocation_config(state_dir / "cfg", program=program)
    cfg = config.load_config()

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if any("deploy-host_c-runtime" in tok for tok in argv):
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="activated\n", stderr=""
            )
        receipt = {
            "program": program, "source_sha": sha, "tag": None, "healthy": True,
            "activation": "atomic", "active_path": "/home/USER/runtimes/vibeqc-release/current",
            "health_detail": "ok", "quiescent": True, "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_c", program, cfg, expected_sha=sha
        )

    assert result.expected_tag is None
    assert any(
        "no resolvable tag" in record.message
        and "host_c" in record.message
        and sha in record.message
        for record in caplog.records
    )


def test_stage_scheduler_runtime_source_archives_linked_worktree_and_verifies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A real throwaway repo plus linked worktree, whose ``.git`` is a file.
    # Drift detection accepts this checkout shape, so the apply/staging path
    # must accept and archive it too.
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True, capture_output=True, text=True,
        )

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "hello.txt").write_text("hi\n", encoding="utf-8")
    git("add", "hello.txt")
    git("commit", "-q", "-m", "init")
    sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    worktree = tmp_path / "linked-worktree"
    git("worktree", "add", "--detach", str(worktree), sha)
    assert (worktree / ".git").is_file()

    uploaded: list[str] = []

    def fake_upload(host_cfg, local, remote, **kwargs):  # type: ignore[no-untyped-def]
        uploaded.append(Path(remote).name)

    def fake_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(args=list(argv), returncode=0, stdout="", stderr="")

    monkeypatch.setattr("vq.admin.transport.upload_file", fake_upload)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake_shell)

    cfg = config.Config(scheduler_runtime_source_repo=str(worktree))
    host_cfg = config.HostConfig(
        ssh="host_c-login", scheduler="slurm", scheduler_dialect="slurm",
        scheduler_driver="localhost", scratch_root="/home/USER",
    )
    result = admin.SchedulerRuntimeUpdateResult(
        host="host_c", program="vibeqc-release", mode="update",
        command="", command_ssh="host_c-login", verify_command="",
        verify_ssh="host_c-login", expected_sha=sha,
    )
    remote = admin._stage_scheduler_runtime_source(
        "host_c", host_cfg, "vibeqc-release", sha, cfg, result
    )
    assert remote.endswith(f"vibeqc-{sha[:12]}-source.tar.gz")
    assert f"/home/USER/.vq-admin/runtime-source/vibeqc-release/{sha}-" in remote
    assert set(uploaded) == {
        f"vibeqc-{sha[:12]}-source.tar.gz", "SOURCE-SHA", "ARCHIVE-SHA256",
    }
    assert result.staged_source_sha256 is not None
    assert result.staged_source_archive == remote


def test_stage_scheduler_runtime_source_retries_flaky_upload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A flaky transfer: the first verify fails (truncated upload), the retry
    # succeeds. Staging must recover instead of failing the whole deploy.
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
        )

    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    (repo / "hello.txt").write_text("hi\n", encoding="utf-8")
    git("add", "hello.txt")
    git("commit", "-q", "-m", "init")
    sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()

    monkeypatch.setattr("vq.admin.transport.upload_file", lambda *a, **k: None)
    verify_calls = {"n": 0}

    def fake_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        # mkdir succeeds; the first verify (sh -c) fails, the second passes.
        if argv and argv[0] == "sh":
            verify_calls["n"] += 1
            rc = 1 if verify_calls["n"] == 1 else 0
            out = "" if rc else "OK"
            return subprocess.CompletedProcess(
                args=list(argv), returncode=rc, stdout=out, stderr=""
            )
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake_shell)

    cfg = config.Config(scheduler_runtime_source_repo=str(repo))
    host_cfg = config.HostConfig(
        ssh="host_c-login", scheduler="slurm", scheduler_dialect="slurm",
        scheduler_driver="localhost", scratch_root="/home/USER",
    )
    result = admin.SchedulerRuntimeUpdateResult(
        host="host_c", program="vibeqc-release", mode="update", command="",
        command_ssh="host_c-login", verify_command="", verify_ssh="host_c-login",
        expected_sha=sha,
    )
    remote = admin._stage_scheduler_runtime_source(
        "host_c", host_cfg, "vibeqc-release", sha, cfg, result
    )
    assert verify_calls["n"] == 2  # failed once, retried, succeeded
    assert result.staged_source_archive == remote


def test_update_scheduler_runtime_mismatched_receipt_fails_closed(
    state_dir: Path,
) -> None:
    program = "vibeqc-dev"
    sha = "c" * 40
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    cfg = config.load_config()

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if host_cfg.ssh == "cluster-build":
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="activated\n", stderr=""
            )
        receipt = {
            "program": program,
            "source_sha": "d" * 40,
            "tag": None,
            "healthy": True,
            "activation": "in-place",
            "active_path": "/opt/vq-runtimes/vibeqc-dev/current",
            "quiescent": False,
            "updater_pid": 1234,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch("vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell):
        result = admin.update_scheduler_runtime(
            "host_f", program, cfg, expected_sha=sha
        )

    assert result.success is False
    assert result.marker_cleared is False
    assert admin.admin_update_marker_exists() is True
    assert any("receipt SHA mismatch" in item for item in result.work_errors)
    assert any("activation='atomic'" in item for item in result.work_errors)
    assert any("quiescent=true" in item for item in result.work_errors)
    assert any("updater_pid" in item for item in result.work_errors)
    record = admin.load_scheduler_runtime_status()[f"host_f:{program}"]
    assert record.last_success is False


def test_update_scheduler_runtime_proceeds_with_an_active_scheduler_job(
    state_dir: Path,
) -> None:
    """A runtime update no longer waits for running work.

    This replaces ``..._refuses_active_scheduler_job``, which pinned the
    opposite contract. A runtime deployment is required to stage out-of-place
    and activate atomically (``config.SchedulerRuntimeDeployment``), and nothing
    reclaims a published per-SHA bundle, so a running job keeps the exact bundle
    it started with. The wait protected nothing and only delayed the release --
    on a continuously-rolling fleet it meant an update could never land while
    the queue had work, which is the halt this change removes.

    The contract is still enforced, not assumed: the verification receipt must
    report ``activation='atomic'``, so a deployment command that mutates the
    active path in place fails the update instead of corrupting a running job.
    """
    program = "vibe-view"
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    cfg = config.load_config()
    workspace = state_dir / "runtime-ws"
    workspace.mkdir()
    JobSpec(
        id="runtime-active",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id="456.cluster",
    ).write(paths.queue_dir() / "runtime-active.json")

    def fake_run_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        if host_cfg.ssh == "cluster-build":
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="activated\n", stderr=""
            )
        receipt = {
            "program": program,
            "source_sha": "e" * 40,
            "tag": None,
            "healthy": True,
            "activation": "atomic",
            "active_path": "/rt/current",
            "health_detail": "import ok",
            "quiescent": True,
            "updater_pid": None,
        }
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout=json.dumps(receipt), stderr=""
        )

    with patch(
        "vq.admin.transport.run_remote_shell", side_effect=fake_run_remote_shell
    ):
        result = admin.update_scheduler_runtime(
            "host_f",
            program,
            cfg,
            expected_sha="e" * 40,
            drain_wait_seconds=4 * 3600,
        )

    # The update ran to completion with a job still running on the target.
    assert result.success is True
    # The running job neither blocked the update nor was counted against it.
    assert result.active_jobs == []
    # No wait was taken despite a generous --drain-wait being passed, and the
    # skip is recorded rather than silently dropped.
    assert result.drain_waited_seconds == 0.0
    assert result.drain_lane_held is False
    assert result.drain_skipped_reason is not None
    assert "atomically" in result.drain_skipped_reason
    assert result.marker_cleared is True


def test_update_scheduler_host_heartbeats_while_remote_command_runs(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, stub_helper_provenance: str
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    messages: list[str | None] = []
    real_refresh = admin.refresh_admin_update_marker_heartbeat

    def spy_refresh(message=None):  # type: ignore[no-untyped-def]
        messages.append(message)
        return real_refresh(message)

    def slow_remote_shell(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        time.sleep(0.12)
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="done\n", stderr=""
        )

    monkeypatch.setenv("VQ_BUILD_HEARTBEAT_INTERVAL", "0.02")
    monkeypatch.setattr("vq.admin._BUILD_POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(
        "vq.admin.refresh_admin_update_marker_heartbeat", spy_refresh
    )
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell", slow_remote_shell
    )

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True
    assert any(
        message and "scheduler update command on host_f-login still running" in message
        for message in messages
    )
    assert any(
        message and "scheduler update command on host_f-login finished rc=0" in message
        for message in messages
    )


def test_scheduler_helper_readiness_retries_transient_etxtbsy(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_scheduler_config(state_dir / "cfg", update_host="cluster-build")
    cfg = config.load_config()
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr("vq.admin.current_source_sha", lambda *a, **k: _DRIVER_SHA)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )

    def fake_remote_vq(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(vq_args)
        if len(calls) == 1:
            return subprocess.CompletedProcess(
                args=["vq", *vq_args],
                returncode=126,
                stdout="",
                stderr=(
                    "/opt/vq/bin/vq: /opt/vq/bin/python: "
                    "bad interpreter: Text file busy\n"
                ),
            )
        if vq_args == ("--version",):
            return subprocess.CompletedProcess(
                args=["vq", *vq_args],
                returncode=0,
                stdout="vq 0.12.0\n",
                stderr="",
            )
        return _fake_helper_provenance(host_cfg, *vq_args, **kwargs)

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", fake_remote_vq)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is True
    assert result.helper_readiness_verified is True
    assert [item.returncode for item in result.helper_readiness_attempts] == [126, 0, 0]
    assert result.helper_readiness_attempts[0].transient_etxtbsy is True
    # Readiness retries, then the two provenance reads, and nothing further:
    # a routine update is read-only on the host and never prunes staging
    # (docs/operations.md, "Managed helper updates retain staging generations").
    assert calls == [
        ("--version",),
        ("--version",),
        ("--version",),
        ("source-tree-sha256",),
        ("source-sha",),
    ]


def test_scheduler_helper_readiness_exhaustion_fails_closed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_scheduler_config(state_dir / "cfg", update_host="cluster-build")
    cfg = config.load_config()

    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_MAX_ATTEMPTS", 3)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_vq",
        lambda host_cfg, *vq_args, **kwargs: subprocess.CompletedProcess(
            args=["vq", *vq_args],
            returncode=126,
            stdout="",
            stderr=(
                "/opt/vq/bin/vq: /opt/vq/bin/python: "
                "bad interpreter: Text file busy\n"
            ),
        ),
    )

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.command_rc == 0
    assert result.success is False
    assert result.helper_readiness_verified is False
    assert len(result.helper_readiness_attempts) == 3
    assert all(item.transient_etxtbsy for item in result.helper_readiness_attempts)
    assert result.source_marker_rc is None
    assert result.remote_source_sha is None
    assert result.marker_cleared is False
    assert admin.admin_update_marker_exists() is True
    assert "readiness exhausted after 3 bounded probes" in result.work_errors[-1]


def test_scheduler_helper_readiness_does_not_retry_non_etxtbsy_failure(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )

    def missing_helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(vq_args)
        return subprocess.CompletedProcess(
            args=["vq", *vq_args],
            returncode=127,
            stdout="",
            stderr="vq: command not found\n",
        )

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", missing_helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert calls == [("--version",)]
    assert len(result.helper_readiness_attempts) == 1
    assert result.helper_readiness_attempts[0].transient_etxtbsy is False
    assert "readiness probe failed (rc=127)" in result.work_errors[-1]


def test_scheduler_helper_source_marker_mismatch_fails_closed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    wrong_sha = "f" * 40

    monkeypatch.setattr("vq.admin.current_source_sha", lambda *a, **k: _DRIVER_SHA)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )

    def wrong_marker(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        if vq_args == ("--version",):
            return subprocess.CompletedProcess(
                args=["vq", *vq_args],
                returncode=0,
                stdout="vq 0.12.0\n",
                stderr="",
            )
        if vq_args == ("source-tree-sha256",):
            return subprocess.CompletedProcess(
                args=["vq", *vq_args],
                returncode=0,
                stdout=f"{_TREE_SHA256}\n",
                stderr="",
            )
        assert vq_args == ("source-sha",)
        return subprocess.CompletedProcess(
            args=["vq", *vq_args],
            returncode=0,
            stdout=f"{wrong_sha} /opt/vq/lib/python/vq/SOURCE-SHA\n",
            stderr="",
        )

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", wrong_marker)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.helper_readiness_verified is True
    assert result.expected_source_sha == _DRIVER_SHA
    assert result.remote_source_sha == wrong_sha
    assert result.marker_cleared is False
    assert "marker verification mismatch" in result.work_errors[-1]


def test_scheduler_helper_matching_marker_cannot_forge_stale_tree(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rewritten SOURCE-SHA must not attest stale installed helper code."""
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    stale_tree = "fe" * 32
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("vq.admin.SCHEDULER_HELPER_ACTIVATION_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(
        "vq.admin.transport.run_remote_shell",
        lambda *a, **k: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="installed\n", stderr=""
        ),
    )

    def stale_helper(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(vq_args)
        if vq_args == ("--version",):
            output = "vq 0.12.0\n"
        elif vq_args == ("source-tree-sha256",):
            output = f"{stale_tree}\n"
        else:
            output = f"{_DRIVER_SHA}\n"
        return subprocess.CompletedProcess(
            args=["vq", *vq_args], returncode=0, stdout=output, stderr=""
        )

    monkeypatch.setattr("vq.admin.transport.run_remote_vq", stale_helper)

    result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.remote_source_tree_sha256 == stale_tree
    assert result.remote_source_sha is None
    # Readiness first, then the tree digest re-read until the activation-settle
    # budget is exhausted. A persistently stale tree is still rejected — the
    # retry only forgives a flip that has not propagated yet, never a forged
    # marker — and SOURCE-SHA is never consulted once the tree fails.
    assert calls[:2] == [("--version",), ("--version",)]
    assert set(calls[2:]) == {("source-tree-sha256",)}
    assert len(calls) == 2 + admin.SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS
    assert "source-tree digest mismatch" in result.work_errors[-1]
    assert (
        result.activation_wait_attempts
        == admin.SCHEDULER_HELPER_ACTIVATION_MAX_ATTEMPTS
    )


def test_update_scheduler_host_refuses_active_submitted_job(
    state_dir: Path,
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    workspace = state_dir / "ws"
    workspace.mkdir()
    JobSpec(
        id="abc",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id="123.cluster",
    ).write(paths.queue_dir() / "abc.json")

    with patch("vq.admin.transport.run_remote_shell") as remote:
        result = admin.update_scheduler_host("host_f", cfg)

    remote.assert_not_called()
    assert result.success is False
    assert result.active_jobs == ["abc(running)"]
    assert "active submitted job" in result.work_errors[0]
    assert result.marker_cleared is True
    assert admin.admin_update_marker_exists() is False


def test_failed_scheduler_update_keeps_marker_for_recovery(
    state_dir: Path,
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()

    with patch(
        "vq.admin.transport.run_remote_shell",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=2, stdout="", stderr="build failed\n"
        ),
    ):
        result = admin.update_scheduler_host("host_f", cfg)

    assert result.success is False
    assert result.command_rc == 2
    assert "build failed" in result.command_output
    assert result.marker_cleared is False
    assert admin.admin_update_marker_exists() is True


def test_cli_scheduler_update_uses_local_driver_and_json(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    captured: dict[str, object] = {}

    def fake_update_scheduler_host(host, cfg, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured.update(kwargs)
        return admin.SchedulerHostUpdateResult(
            host=host,
            ssh="host_f-login",
            scheduler="pbs",
            mode="install",
            command="/home/USER/vibeqc-dev/scripts/install_cluster.sh --release --wait",
            command_rc=0,
            command_output="submitted\n",
        )

    from vq import cli as cli_mod

    monkeypatch.setattr(
        cli_mod.admin_module, "update_scheduler_host", fake_update_scheduler_host
    )
    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "host_f",
            "--cluster-install",
            "--json",
            "--update-script-arg=--wait",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["host"] == "host_f"
    assert captured["install"] is True
    assert captured["update_script_args"] == ["--wait"]
    payload = json.loads(result.stdout)
    assert payload["success"] is True
    assert payload["mode"] == "install"


@pytest.mark.parametrize("role", ["auto", "vq-only", "alias", "excluded"])
@pytest.mark.parametrize("surface", ["helper", "runtime"])
def test_direct_scheduler_update_rejects_non_managed_fleet_roles(
    state_dir: Path,
    role: str,
    surface: str,
) -> None:
    program = "vibeqc-release"
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    cfg = config.load_config()
    update: dict[str, object] = {"fleet_role": role}
    if role == "alias":
        update["fleet_canonical_host"] = "localhost"
    cfg.hosts["host_f"] = cfg.hosts["host_f"].model_copy(update=update)

    with pytest.raises(admin.AdminError, match="fleet_role='managed'"):
        if surface == "runtime":
            admin.update_scheduler_runtime(
                "host_f",
                program,
                cfg,
                expected_sha="f" * 40,
            )
        else:
            admin.update_scheduler_host("host_f", cfg)


def test_manual_scheduler_helper_holds_source_checkout_fence(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir / "cfg")
    cfg = config.load_config()
    source = state_dir / "source"
    source.mkdir()
    calls: list[
        tuple[list[config.VenvProgram], str, tuple[tuple[str, str], ...]]
    ] = []
    sentinel = object()
    monkeypatch.setattr(admin, "_scheduler_helper_project_root", lambda: source)
    monkeypatch.setattr(
        admin, "_canonical_lifecycle_checkout", lambda unused: source,
    )
    monkeypatch.setattr(
        admin,
        "toolset_lifecycle_lock",
        lambda progs, *, action, extra_resources=(): _recording_lifecycle_fence(
            calls, progs, action=action, extra_resources=extra_resources,
        ),
    )
    monkeypatch.setattr(
        admin, "_update_scheduler_host_owned", lambda *args, **kwargs: sentinel,
    )

    result = admin.update_scheduler_host("host_f", cfg)

    assert result is sentinel
    assert calls == [([], "update scheduler helper host_f", (("checkout", str(source)),))]


def test_manual_scheduler_runtime_holds_configured_source_checkout_fence(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    program = "vibeqc-release"
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    source = state_dir / "runtime-source"
    source.mkdir()
    cfg = config.load_config().model_copy(
        update={"scheduler_runtime_source_repo": str(source)},
    )
    calls: list[
        tuple[list[config.VenvProgram], str, tuple[tuple[str, str], ...]]
    ] = []
    sentinel = object()
    monkeypatch.setattr(
        admin, "_canonical_lifecycle_checkout", lambda unused: source,
    )
    monkeypatch.setattr(
        admin,
        "toolset_lifecycle_lock",
        lambda progs, *, action, extra_resources=(): _recording_lifecycle_fence(
            calls, progs, action=action, extra_resources=extra_resources,
        ),
    )
    monkeypatch.setattr(
        admin, "_update_scheduler_runtime_owned", lambda *args, **kwargs: sentinel,
    )

    result = admin.update_scheduler_runtime(
        "host_f", program, cfg, expected_sha="f" * 40,
    )

    assert result is sentinel
    assert calls == [
        (
            [],
            f"update scheduler runtime host_f:{program}",
            (("checkout", str(source)),),
        )
    ]


@pytest.mark.parametrize("role", ["auto", "vq-only", "alias", "excluded"])
@pytest.mark.parametrize("surface", ["helper", "runtime"])
def test_cli_scheduler_update_rejects_non_managed_before_routing(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
    surface: str,
) -> None:
    program = "vibeqc-release"
    _write_scheduler_config(
        state_dir / "cfg", driver="driver", runtime_program=program,
    )
    cfg = config.load_config()
    update: dict[str, object] = {"fleet_role": role}
    if role == "alias":
        update["fleet_canonical_host"] = "driver"
    cfg.hosts["host_f"] = cfg.hosts["host_f"].model_copy(update=update)
    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    forwarded: list[object] = []
    monkeypatch.setattr(
        "vq.cli._forward_admin_command",
        lambda *args, **kwargs: forwarded.append((args, kwargs)),
    )
    monkeypatch.setattr(
        admin,
        "update_scheduler_runtime",
        lambda *args, **kwargs: pytest.fail("local runtime update ran"),
    )
    monkeypatch.setattr(
        admin,
        "update_scheduler_host",
        lambda *args, **kwargs: pytest.fail("local helper update ran"),
    )
    args = (
        [
            "admin", "update", program, "host_f",
            "--expected-sha", "f" * 40,
        ]
        if surface == "runtime"
        else ["admin", "update", "host_f"]
    )

    result = CliRunner().invoke(main, args)

    assert result.exit_code == 2
    assert "fleet_role='managed'" in result.output
    assert forwarded == []


def test_cli_scheduler_runtime_update_routes_program_and_identity(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    program = "vibeqc-release"
    sha = "f" * 40
    tag = "v0.15.45"
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    captured: dict[str, object] = {}

    def fake_update_runtime(host, selected, cfg, **kwargs):  # type: ignore[no-untyped-def]
        captured.update({"host": host, "program": selected, **kwargs})
        return admin.SchedulerRuntimeUpdateResult(
            host=host,
            program=selected,
            mode="update",
            command="deploy",
            command_ssh="cluster-build",
            verify_command="verify",
            verify_ssh="host_f-login",
            expected_sha=sha,
            expected_tag=tag,
            command_rc=0,
            verify_rc=0,
            actual_sha=sha,
            actual_tag=tag,
            healthy=True,
            activation="atomic",
            active_path="/active",
            quiescent=True,
        )

    from vq import cli as cli_mod

    monkeypatch.setattr(
        cli_mod.admin_module, "update_scheduler_runtime", fake_update_runtime
    )
    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            program,
            "host_f",
            "--expected-sha",
            sha,
            "--tag",
            tag,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "host": "host_f",
        "program": program,
        "expected_sha": sha,
        "expected_tag": tag,
        "install": False,
        "force": False,
        "update_script_args": [],
        # No --drain-wait given => 0.0, i.e. the historical one-shot refusal.
        "drain_wait_seconds": 0.0,
    }
    assert json.loads(result.stdout)["success"] is True


def test_cli_scheduler_runtime_update_delegates_to_driver(
    state_dir: Path,
) -> None:
    program = "vibeqc-dev"
    sha = "a" * 40
    _write_scheduler_config(
        state_dir / "cfg", driver="driver", runtime_program=program
    )
    captured: dict[str, object] = {}

    def fake_run_remote_vq(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        captured["ssh"] = host_cfg.ssh
        captured["argv"] = list(vq_args)
        captured["kwargs"] = kwargs
        return MagicMock(returncode=0, stdout="REMOTE-RUNTIME-OK\n", stderr="")

    with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), patch(
        "vq.cli.transport.run_remote_vq", side_effect=fake_run_remote_vq
    ):
        result = CliRunner().invoke(
            main,
            [
                "admin",
                "update",
                program,
                "host_f",
                "--expected-sha",
                sha,
                "--json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert result.output == "REMOTE-RUNTIME-OK\n"
    assert captured["ssh"] == "driver"
    assert captured["argv"] == [
        "admin",
        "update",
        program,
        "host_f",
        "--expected-sha",
        sha,
        "--json",
    ]
    assert "retry_transient" not in captured["kwargs"]
    assert (
        captured["kwargs"].get("timeout")
        == transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS
    )


def test_cli_scheduler_runtime_status_reports_last_ok(
    state_dir: Path,
) -> None:
    program = "vibe-view"
    sha = "9" * 40
    _write_scheduler_config(state_dir / "cfg", runtime_program=program)
    admin.record_scheduler_runtime_outcome(
        admin.SchedulerRuntimeUpdateResult(
            host="host_f",
            program=program,
            mode="update",
            command="deploy",
            command_ssh="cluster-build",
            verify_command="verify",
            verify_ssh="host_f-login",
            expected_sha=sha,
            command_rc=0,
            verify_rc=0,
            actual_sha=sha,
            healthy=True,
            activation="atomic",
            active_path="/active/view",
            quiescent=True,
        )
    )

    result = CliRunner().invoke(main, ["admin", "status", "host_f", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    last = payload["deployments"][program]["last"]
    assert last["last_success"] is True
    assert last["actual_sha"] == sha
    assert last["active_path"] == "/active/view"


def test_cli_doctor_admin_update_mentions_scheduler_update_host(
    state_dir: Path,
) -> None:
    _write_scheduler_config(state_dir / "cfg", update_host="cluster-build")

    result = CliRunner().invoke(main, ["doctor", "host_f", "--admin-update"])

    assert result.exit_code != 2, result.output
    assert "scheduler_update_command configured" in result.output
    assert "update host cluster-build" in result.output


def test_cli_doctor_reports_unknown_scheduler_program_hook(
    state_dir: Path,
) -> None:
    _write_scheduler_config(state_dir / "cfg", program_hook="orca")

    result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    checks = {item["name"]: item for item in payload["checks"]}
    assert checks["scheduler_program_hooks"]["ok"] is False
    assert "unknown program(s): orca" in checks["scheduler_program_hooks"]["message"]


def test_cli_doctor_accepts_registered_scheduler_program_hook(
    state_dir: Path,
) -> None:
    _write_scheduler_config(
        state_dir / "cfg",
        program_hook="orca",
        register_program=True,
    )

    result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

    assert result.exit_code in {0, 1}, result.output
    payload = json.loads(result.stdout)
    checks = {item["name"]: item for item in payload["checks"]}
    assert checks["scheduler_program_hooks"]["ok"] is True
    assert checks["scheduler_program_hooks"]["message"] == "configured for: orca"


def test_cli_scheduler_update_delegates_to_driver(
    state_dir: Path,
) -> None:
    _write_scheduler_config(state_dir / "cfg", driver="driver")
    captured: dict[str, object] = {}

    def fake_run_remote_vq(host_cfg, *vq_args, **kwargs):  # type: ignore[no-untyped-def]
        captured["ssh"] = host_cfg.ssh
        captured["argv"] = list(vq_args)
        captured["kwargs"] = kwargs
        return MagicMock(returncode=0, stdout="REMOTE-OK\n", stderr="")

    with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), patch(
        "vq.cli.transport.run_remote_vq", side_effect=fake_run_remote_vq
    ):
        result = CliRunner().invoke(
            main,
            [
                "admin",
                "update",
                "host_f",
                "--cluster-install",
                "--force",
                "--json",
                "--update-script-arg=--wait",
            ],
        )

    assert result.exit_code == 0, result.output
    assert result.output == "REMOTE-OK\n"
    assert captured["ssh"] == "driver"
    assert captured["argv"] == [
        "admin",
        "update",
        "host_f",
        "--cluster-install",
        "--force",
        "--json",
        "--update-script-arg",
        "--wait",
    ]
    assert "retry_transient" not in captured["kwargs"]
    assert (
        captured["kwargs"].get("timeout")
        == transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS
    )


# ----------------------------------------------------------------------
# LAST GOOD survives a failed runtime deployment
#
# After the 2026-07-24 host_f verify failure the status record was overwritten
# wholesale: SHA showed '-', and the identity of the last verified runtime —
# the rollback target — existed only in old transcripts. These pin the
# carry-forward contract.
# ----------------------------------------------------------------------


def _runtime_result(
    *, success: bool, sha: str, tag: str | None = None
) -> admin.SchedulerRuntimeUpdateResult:
    result = admin.SchedulerRuntimeUpdateResult(
        host="host_f",
        program="vibeqc-release",
        mode="update",
        command="/site/bin/deploy-runtime",
        command_ssh="cluster-build",
        verify_command="/site/bin/verify-runtime",
        verify_ssh="host_f-login",
        expected_sha=sha,
        expected_tag=tag,
    )
    if success:
        result.command_rc = 0
        result.verify_rc = 0
        result.actual_sha = sha
        result.actual_tag = tag
        result.healthy = True
        result.activation = "atomic"
        result.quiescent = True
        result.updater_pid = None
        result.active_path = "/site/runtimes/vibeqc-release/current"
    else:
        result.command_rc = 0
        result.verify_rc = 1
        result.work_errors.append("verification command rc=1")
    return result


def test_a_failed_deploy_preserves_the_last_good_identity(
    state_dir: Path,
) -> None:
    good_sha = "a" * 40
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=True, sha=good_sha, tag="v0.15.50")
    )
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=False, sha="b" * 40, tag="v0.15.57")
    )

    record = admin.load_scheduler_runtime_status()["host_f:vibeqc-release"]
    assert record.last_success is False
    assert record.last_ok_sha == good_sha
    assert record.last_ok_tag == "v0.15.50"
    assert record.last_ok_active_path == "/site/runtimes/vibeqc-release/current"


def test_a_new_success_advances_the_last_good_identity(state_dir: Path) -> None:
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=True, sha="a" * 40, tag="v0.15.50")
    )
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=True, sha="c" * 40, tag="v0.15.57")
    )

    record = admin.load_scheduler_runtime_status()["host_f:vibeqc-release"]
    assert record.last_ok_sha == "c" * 40
    assert record.last_ok_tag == "v0.15.57"


def test_a_legacy_success_record_backfills_the_rollback_target(
    state_dir: Path,
) -> None:
    """Records written before last_ok_* existed still yield a rollback target.

    host_c's live records predate these fields; their own identity IS the
    last-good one when they were successes.
    """
    good_sha = "d" * 40
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=True, sha=good_sha, tag="v0.15.50")
    )
    # Strip the new fields, simulating a record written by the old code.
    path = admin.scheduler_runtime_status_path()
    raw = json.loads(path.read_text())
    for field_name in (
        "last_ok_sha", "last_ok_tag", "last_ok_at", "last_ok_active_path"
    ):
        raw["host_f:vibeqc-release"].pop(field_name, None)
    path.write_text(json.dumps(raw))

    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=False, sha="e" * 40, tag="v0.15.57")
    )

    record = admin.load_scheduler_runtime_status()["host_f:vibeqc-release"]
    assert record.last_ok_sha == good_sha
    assert record.last_ok_tag == "v0.15.50"


def test_status_table_names_the_last_good_identity(state_dir: Path) -> None:
    _write_scheduler_config(state_dir / "cfg", runtime_program="vibeqc-release")
    cfg = config.load_config()
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=True, sha="a" * 40, tag="v0.15.50")
    )
    admin.record_scheduler_runtime_outcome(
        _runtime_result(success=False, sha="b" * 40, tag="v0.15.57")
    )

    text = admin.format_scheduler_runtime_status("host_f", cfg)
    assert "LAST GOOD" in text
    assert "aaaaaaaaaaaa (v0.15.50)" in text


def test_scheduler_phase_poll_projects_vq_specs_as_single_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admin census uses ordinary handles for ordinary and vq-array specs."""
    captured: list[SchedulerHandle] = []

    class RecordingDispatcher:
        def remote_workspace(self, job_id: str) -> str:
            return f"/remote/{job_id}"

        def poll(
            self, handles: Iterable[SchedulerHandle]
        ) -> dict[str, SchedulerPhase]:
            captured.extend(handles)
            return {handle.job_id: SchedulerPhase.RUNNING for handle in captured}

    dispatcher = RecordingDispatcher()
    monkeypatch.setattr(
        "vq.scheduler_dispatch.scheduler_dispatcher_for",
        lambda _host_cfg: dispatcher,
    )
    specs = [
        JobSpec(
            id="ordinary",
            command=["true"],
            cwd="/tmp/ordinary",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="101.cluster",
        ),
        JobSpec(
            id="array-element",
            command=["true"],
            cwd="/tmp/array-element",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="102.cluster",
            array_index=2,
            array_total=5,
            array_group_id="arraygrp",
        ),
    ]

    phases = admin._poll_scheduler_phases(config.HostConfig(ssh="host_f"), specs)

    assert phases == {
        "101.cluster": SchedulerPhase.RUNNING,
        "102.cluster": SchedulerPhase.RUNNING,
    }
    assert [
        (handle.job_id, handle.remote_workspace, handle.array_size)
        for handle in captured
    ] == [
        ("101.cluster", "/remote/ordinary", None),
        ("102.cluster", "/remote/array-element", None),
    ]


@pytest.mark.parametrize("missing_id", [None, ""])
def test_scheduler_census_filters_missing_ids_before_projection(
    missing_id: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[JobSpec] = []

    def fake_poll(
        _host_cfg: config.HostConfig,
        specs: list[JobSpec],
    ) -> dict[str, SchedulerPhase]:
        captured.extend(specs)
        return {"101.cluster": SchedulerPhase.RUNNING}

    monkeypatch.setattr(admin, "_poll_scheduler_phases", fake_poll)
    missing = JobSpec(
        id="missing-id",
        command=["true"],
        cwd="/tmp/missing-id",
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id=missing_id,
    )
    valid = JobSpec(
        id="valid-id",
        command=["true"],
        cwd="/tmp/valid-id",
        cpus=1,
        state=JobState.RUNNING,
        scheduler_target="host_f",
        scheduler_job_id="101.cluster",
    )

    census = admin._reconcile_active_scheduler_jobs(
        "host_f",
        config.HostConfig(ssh="host_f"),
        [missing, valid],
    )

    assert captured == [valid]
    assert census.unreconciled == ["missing-id"]
    assert [spec.id for spec in census.blocking] == ["missing-id", "valid-id"]


class TestGhostJobsDoNotBlockADrainWait:
    """A reattach-failed spec with no cluster handle must not hold a drain.

    host_f, 2026-08-01: a helper rebuild's drain-wait was blocked behind a real
    4-hour GPW job and a ghost -- `f53f1cc9bb6c`, `scheduler_state:
    reattach_failed`, absent from `qstat`, no PBS id. The real job was killed to
    unblock the rebuild. The ghost would have blocked it regardless, and
    indefinitely: vq has no handle for it, so no scheduler job will ever finish
    on its behalf and the wait can never be satisfied.
    """

    def _spec(self, state_dir: Path, jobid: str, **fields: object) -> None:
        ws = state_dir / f"ws-{jobid}"
        ws.mkdir(exist_ok=True)
        base: dict[str, object] = {
            "id": jobid,
            "command": ["true"],
            "cwd": str(ws),
            "cpus": 1,
            "scheduler_target": "host_f",
            "state": JobState.RUNNING,
        }
        base.update(fields)
        JobSpec(**base).write(paths.queue_dir() / f"{jobid}.json")

    def test_a_reattach_failed_ghost_is_not_counted(self, state_dir: Path) -> None:
        self._spec(
            state_dir,
            "ghost",
            scheduler_job_id=None,
            scheduler_state="reattach_failed",
        )

        active = admin._active_scheduler_job_specs("host_f", multi_user=False)

        assert active == [], "a spec with no cluster handle can never clear"

    def test_a_real_running_job_is_still_counted(self, state_dir: Path) -> None:
        self._spec(state_dir, "real", scheduler_job_id="18109.host_f")

        active = admin._active_scheduler_job_specs("host_f", multi_user=False)

        assert [s.id for s in active] == ["real"]

    def test_a_mid_dispatch_spec_is_still_counted(self, state_dir: Path) -> None:
        """RUNNING with no id but no reattach_failed marker may simply be
        mid-dispatch; skipping it would declare quiescence while a job starts."""
        self._spec(state_dir, "starting", scheduler_job_id=None)

        active = admin._active_scheduler_job_specs("host_f", multi_user=False)

        assert [s.id for s in active] == ["starting"]

    def test_a_reattach_failed_job_that_kept_its_id_is_still_counted(
        self, state_dir: Path
    ) -> None:
        """vq failed to reattach but still knows the cluster job, so the job may
        well be running and the wait is real."""
        self._spec(
            state_dir,
            "recoverable",
            scheduler_job_id="18110.host_f",
            scheduler_state="reattach_failed",
        )

        active = admin._active_scheduler_job_specs("host_f", multi_user=False)

        assert [s.id for s in active] == ["recoverable"]


class TestHelperActivationProof:
    """A helper rebuild waits for running work only where the wait protects
    something.

    host_f, 2026-08-01: a paper-critical GPW job four hours into a twelve-hour
    budget was killed to satisfy this wait, for a rebuild that could not have
    touched it. Both scheduler hosts already published an immutable per-SHA
    helper root and switched the stable path by rename; the code asserted the
    opposite in a comment written before those scripts landed.

    The proof is the site's own receipt, and every uncertain case keeps the
    wait.
    """

    COMMAND = "/home/USER/bin/vq-update-scheduler-buildhost"

    def _record(self, **fields: object) -> None:
        records = admin.load_scheduler_runtime_status()
        records[f"host_f:{admin.SCHEDULER_HELPER_RECORD_PROGRAM}"] = (
            admin.SchedulerRuntimeUpdateRecord(
                host="host_f",
                program=admin.SCHEDULER_HELPER_RECORD_PROGRAM,
                last_updated_at="2026-08-01T00:00:00Z",
                last_success=True,
                expected_sha="a" * 40,
                **fields,  # type: ignore[arg-type]
            )
        )
        admin._write_scheduler_runtime_status(records)

    def test_a_proven_host_skips_the_wait(self, state_dir: Path) -> None:
        self._record(
            activation="atomic",
            active_path="/home/USER/vibe-queue-aaaaaaaaaaaa",
            activation_command=self.COMMAND,
        )

        proven, reason = admin.helper_activation_is_proven("host_f", self.COMMAND)

        assert proven is True
        assert "rename" in reason

    def test_a_host_with_no_record_keeps_the_wait(self, state_dir: Path) -> None:
        """First contact is not proof."""
        proven, reason = admin.helper_activation_is_proven("host_f", self.COMMAND)

        assert proven is False
        assert "no previous helper deployment" in reason

    def test_an_unproven_activation_keeps_the_wait(self, state_dir: Path) -> None:
        """A host on the in-place rsync path never emits a receipt."""
        self._record(activation=None, activation_command=self.COMMAND)

        proven, _ = admin.helper_activation_is_proven("host_f", self.COMMAND)

        assert proven is False

    def test_repointing_the_command_invalidates_the_proof(
        self, state_dir: Path
    ) -> None:
        """Otherwise switching a host to a mutating deploy script would inherit
        the old script's proof for one update."""
        self._record(
            activation="atomic",
            active_path="/home/USER/vibe-queue-aaaaaaaaaaaa",
            activation_command=self.COMMAND,
        )

        proven, reason = admin.helper_activation_is_proven(
            "host_f", "/home/USER/bin/some-other-updater"
        )

        assert proven is False
        assert "differs from the one that proved" in reason

    def _result(self, **fields: object) -> admin.SchedulerHostUpdateResult:
        return admin.SchedulerHostUpdateResult(
            host="host_f",
            ssh="host_f",
            scheduler="torque",
            mode="update",
            command="",
            command_ssh="",
            **fields,  # type: ignore[arg-type]
        )

    def test_a_receipt_naming_this_commit_is_accepted(self) -> None:
        result = self._result(expected_source_sha="a" * 40)
        result.metrics.update(
            {
                "helper_activation": "atomic",
                "helper_activation_sha": "a" * 40,
                "helper_active_path": "/home/USER/vibe-queue-aaaaaaaaaaaa",
            }
        )

        admin.apply_helper_activation_receipt(result)

        assert result.activation == "atomic"
        assert result.active_path == "/home/USER/vibe-queue-aaaaaaaaaaaa"

    def test_a_receipt_for_a_different_commit_is_refused(self) -> None:
        """A stale copy of a deploy script must not launder an old proof into a
        new update."""
        result = self._result(expected_source_sha="a" * 40)
        result.metrics.update(
            {
                "helper_activation": "atomic",
                "helper_activation_sha": "b" * 40,
            }
        )

        admin.apply_helper_activation_receipt(result)

        assert result.activation is None
        assert any("different commit" in w for w in result.maintenance_warnings)

    def test_no_receipt_leaves_activation_unproven(self) -> None:
        result = self._result(expected_source_sha="a" * 40)

        admin.apply_helper_activation_receipt(result)

        assert result.activation is None


class TestAProvenHelperDoesNotWaitForRunningWork:
    """The end-to-end shape of the 2026-08-01 incident, inverted.

    Same setup as `test_update_scheduler_host_refuses_active_submitted_job` --
    a live scheduler job on host_f -- but with the host's previous deploy having
    proven it activates atomically. The update must proceed.
    """

    CONFIGURED = "/home/USER/vibeqc-dev/scripts/update_cluster.sh --release"

    def _live_job(self, state_dir: Path) -> None:
        workspace = state_dir / "ws"
        workspace.mkdir(exist_ok=True)
        JobSpec(
            id="abc",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="123.cluster",
        ).write(paths.queue_dir() / "abc.json")

    def _prove(self, command: str) -> None:
        records = admin.load_scheduler_runtime_status()
        records[f"host_f:{admin.SCHEDULER_HELPER_RECORD_PROGRAM}"] = (
            admin.SchedulerRuntimeUpdateRecord(
                host="host_f",
                program=admin.SCHEDULER_HELPER_RECORD_PROGRAM,
                last_updated_at="2026-08-01T00:00:00Z",
                last_success=True,
                expected_sha="a" * 40,
                activation="atomic",
                active_path="/home/USER/vibe-queue-aaaaaaaaaaaa",
                activation_command=command,
            )
        )
        admin._write_scheduler_runtime_status(records)

    def test_a_live_job_no_longer_refuses_the_rebuild(self, state_dir: Path) -> None:
        _write_scheduler_config(state_dir / "cfg")
        cfg = config.load_config()
        self._live_job(state_dir)
        self._prove(self.CONFIGURED)

        with patch("vq.admin.transport.run_remote_shell") as remote:
            remote.return_value = subprocess.CompletedProcess([], 0, "", "")
            result = admin.update_scheduler_host("host_f", cfg)

        # The refusal is gone: the deploy was actually attempted.
        assert result.active_jobs == []
        assert remote.called
        assert result.drain_waited_seconds == 0.0
        assert result.drain_lane_held is False
        assert result.drain_skipped_reason is not None

    def test_without_the_proof_the_same_job_still_refuses(
        self, state_dir: Path
    ) -> None:
        """The guard is removed by evidence, not by default."""
        _write_scheduler_config(state_dir / "cfg")
        cfg = config.load_config()
        self._live_job(state_dir)

        with patch("vq.admin.transport.run_remote_shell") as remote:
            result = admin.update_scheduler_host("host_f", cfg)

        remote.assert_not_called()
        assert result.active_jobs == ["abc(running)"]

    def test_a_proof_earned_by_a_different_command_still_refuses(
        self, state_dir: Path
    ) -> None:
        """Repointing the host at another deploy script must not inherit the
        old script's proof."""
        _write_scheduler_config(state_dir / "cfg")
        cfg = config.load_config()
        self._live_job(state_dir)
        self._prove("/home/USER/bin/some-other-updater")

        with patch("vq.admin.transport.run_remote_shell") as remote:
            result = admin.update_scheduler_host("host_f", cfg)

        remote.assert_not_called()
        assert result.active_jobs == ["abc(running)"]


def test_each_program_stages_from_its_own_repository(state_dir: Path) -> None:
    """The viewer is its own repository since the split.

    Staging it from a vibe-qc clone builds whatever viewer that clone happens
    to carry -- or nothing, once vibe-qc stops carrying one. This is the vq
    side of the same derivation contrib/host_f/prepare-host_f-runtime-source
    makes, so driver and host agree on a program's upstream.
    """
    cfg = config.Config(
        hosts={"localhost": config.HostConfig(ssh="localhost")},
        scheduler_runtime_source_repo="/srv/vibe-qc",
        pin_source_repos={
            "mpei/vibe-qc": "/srv/vibe-qc",
            "mpei/vibe-view": "/srv/vibe-view",
            "mpei/vibe-queue": "/srv/vibe-queue",
        },
    )
    resolve = admin.program_source_repo
    assert str(resolve(cfg, "vibeqc-dev")) == "/srv/vibe-qc"
    assert str(resolve(cfg, "vibeqc-release")) == "/srv/vibe-qc"
    assert str(resolve(cfg, "vibe-view")) == "/srv/vibe-view"
    assert str(resolve(cfg, "vibeview-dev")) == "/srv/vibe-view"
    assert str(resolve(cfg, "vibeqc-queue")) == "/srv/vibe-queue"
    # Unknown programs are vibe-qc variants (mace, skala, ...).
    assert str(resolve(cfg, "vibeqc-mace-dev")) == "/srv/vibe-qc"


def test_vibe_qc_programs_still_work_without_pin_source_repos(
    state_dir: Path,
) -> None:
    """A driver that has not declared the mapping keeps working as before."""
    cfg = config.Config(
        hosts={"localhost": config.HostConfig(ssh="localhost")},
        scheduler_runtime_source_repo="/srv/monorepo",
    )
    assert str(admin.program_source_repo(cfg, "vibeqc-dev")) == "/srv/monorepo"
    # ...but the viewer cannot silently fall back to a vibe-qc clone.
    with pytest.raises(admin.AdminError) as excinfo:
        admin.program_source_repo(cfg, "vibe-view")
    message = str(excinfo.value)
    assert "mpei/vibe-view" in message
    assert "pin_source_repos" in message

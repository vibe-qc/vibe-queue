"""BUG 101: scheduler-target ``--expected-sha`` pins the TARGET's runtime.

The wave038 dispatch blocker (2026-07-25): ``vq submit host_c --program
vibeqc-release --expected-sha 820e718…`` failed with "expected git SHA
820e718…, got 63a80907a473" — the submit rewrote its host to the driver and
then validated the pin against the DRIVER-LOCAL venv named vibeqc-release,
not host_c's immutable 0.15.50 wrapper that the job would actually execute.

The authority for a managed scheduler runtime is the driver's verified
deployment record (`vq admin status <host>`); subqueue aliases share their
cluster's records through the ssh endpoint. These tests pin: matching and
mismatching SHAs, an immutable-wrapper target over a driver-local venv AND
over a driver-local binary, the host_c/host_c-campaign alias pair, batch
validation before any spec exists, receipt-host and program_runtime_pin
fields, atomic failure, and clean retry idempotency.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths, transport
from vq.cli import main
from vq.spec import JobSpec

host_c_SHA = "820e718bb0bf2f0ea2d7d28eece0d04cded18131"
host_c_WRAPPER = (
    "/home/USER/.local/libexec/vq-host_c/vibeqc-release-0.15.50-820e718-python"
)
host_f_SHA = "d4e9b2d96602aba977ac94613324aeeef1b8d28d"
host_f_PYTHON = (
    "/home/USER/.local/libexec/vq-host_f/runtimes/vibeqc-release/"
    f"releases/{host_f_SHA}/source/.venv/bin/python"
)


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    # Default: the target's registry is unreachable, exercising the
    # deployment-record fallback. Registry-authority tests override this.
    monkeypatch.setattr(
        "vq.cli.transport.run_remote_vq",
        lambda *a, **k: (_ for _ in ()).throw(
            transport.RemoteError("registry unreachable in this test")
        ),
    )
    return tmp_path


def _registry_answers(monkeypatch: pytest.MonkeyPatch, binary: str) -> None:
    """The target's helper reports one registered immutable wrapper."""
    import subprocess as _subprocess

    payload = json.dumps(
        [{"name": "vibeqc-release", "kind": "binary", "status": "OK",
          "binary": binary}]
    )
    monkeypatch.setattr(
        "vq.cli.transport.run_remote_vq",
        lambda *a, **k: _subprocess.CompletedProcess(
            args=[], returncode=0, stdout=payload, stderr=""
        ),
    )


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def driver_checkout(tmp_path: Path) -> tuple[Path, str]:
    """A driver-local git checkout at a SHA that is NOT host_c's."""
    repo = tmp_path / "driver-checkout"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    _git("config", "user.email", "t@t", cwd=repo)
    _git("config", "user.name", "t", cwd=repo)
    (repo / "f.txt").write_text("driver\n")
    _git("add", "f.txt", cwd=repo)
    _git("commit", "-q", "-m", "driver tree", cwd=repo)
    return repo, _git("rev-parse", "HEAD", cwd=repo)


def _write_config(cfg_dir: Path, *, driver_program_block: str) -> None:
    (cfg_dir / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
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
                "",
                "[hosts.host_c.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy-host_c-runtime"',
                'verify_command = "/site/bin/verify-host_c-runtime"',
                "",
                "[hosts.host_c-campaign]",
                'ssh = "host_c-login"',
                'scheduler = "slurm"',
                'scheduler_dialect = "slurm"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                "",
                driver_program_block,
                "",
            ]
        )
    )


def _venv_program_block(repo: Path) -> str:
    import sys

    return "\n".join(
        [
            "[programs.vibeqc-release]",
            'kind = "venv"',
            f'python = "{sys.executable}"',
            f'git_dir = "{repo}"',
            'branch = "main"',
        ]
    )


def _record_host_c_runtime(
    *, sha: str = host_c_SHA, tag: str | None = "v0.15.50", success: bool = True
) -> None:
    result = admin.SchedulerRuntimeUpdateResult(
        host="host_c",
        program="vibeqc-release",
        mode="update",
        command="/site/bin/deploy-host_c-runtime",
        command_ssh="host_c-login",
        verify_command="/site/bin/verify-host_c-runtime",
        verify_ssh="host_c-login",
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
        result.active_path = host_c_WRAPPER
    else:
        result.command_rc = 0
        result.verify_rc = 1
        result.work_errors.append("verification command rc=1")
    admin.record_scheduler_runtime_outcome(result)


def _write_host_f_config(cfg_dir: Path, *, driver_program_block: str) -> None:
    (cfg_dir / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/scratch/USER"',
                'scheduler_driver = "localhost"',
                "",
                "[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]",
                'update_command = "/site/bin/deploy-host_f-runtime"',
                'verify_command = "/site/bin/verify-host_f-runtime"',
                "",
                driver_program_block,
                "",
            ]
        ),
        encoding="utf-8",
    )


def _record_host_f_runtime() -> None:
    result = admin.SchedulerRuntimeUpdateResult(
        host="host_f",
        program="vibeqc-release",
        mode="update",
        command="/site/bin/deploy-host_f-runtime",
        command_ssh="host_f-login",
        verify_command="/site/bin/verify-host_f-runtime",
        verify_ssh="host_f-login",
        expected_sha=host_f_SHA,
        expected_tag="v0.15.129",
    )
    result.command_rc = 0
    result.verify_rc = 0
    result.actual_sha = host_f_SHA
    result.actual_tag = "v0.15.129"
    result.healthy = True
    result.activation = "atomic"
    result.quiescent = True
    result.updater_pid = None
    result.active_path = host_f_PYTHON
    admin.record_scheduler_runtime_outcome(result)


def _submit(tmp_path: Path, target: str, expected: str, *extra: str):  # type: ignore[no-untyped-def]
    script = tmp_path / "run.py"
    script.write_text("print('hi')\n")
    return CliRunner().invoke(
        main,
        [
            "submit", target, str(script),
            "--program", "vibeqc-release",
            "--expected-sha", expected,
            "--python", "/usr/bin/python3",
            "--cpus", "1",
            "--wall-time-seconds", "60",
            "--json",
            *extra,
        ],
    )


def _submit_directory_unpinned(tmp_path: Path, target: str):  # type: ignore[no-untyped-def]
    payload = tmp_path / "payload"
    payload.mkdir(exist_ok=True)
    (payload / "run.sh").write_text("exit 0\n", encoding="utf-8")
    return CliRunner().invoke(
        main,
        [
            "submit",
            target,
            "--program",
            "vibeqc-release",
            "--json",
            "--dir",
            str(payload),
            "--",
            "bash",
            "run.sh",
        ],
    )


def _queued_specs() -> list[JobSpec]:
    return [JobSpec.read(p) for p in sorted(paths.queue_dir().glob("*.json"))]


def test_matching_target_sha_submits_despite_driver_drift(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    """THE ACCEPTANCE TEST: host_c's 820e718 wrapper validates even though
    the driver-local venv of the same name sits at a different SHA."""
    repo, driver_sha = driver_checkout
    assert not driver_sha.startswith("820e718")
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()

    result = _submit(tmp_path, "host_c", host_c_SHA)

    assert result.exit_code == 0, result.output
    receipt = json.loads(result.output)
    assert receipt["host"] == "host_c"
    pin = receipt["program_runtime_pin"]
    assert pin["scheduler_host"] == "host_c"
    assert pin["expected_git_sha"] == host_c_SHA
    assert pin["resolved_executable"] == host_c_WRAPPER
    assert pin["program_kind"] == "scheduler-runtime"
    assert pin["program_version"] == "0.15.50"
    specs = _queued_specs()
    assert len(specs) == 1
    spec = specs[0]
    assert spec.scheduler_target == "host_c"
    assert spec.program_runtime_pin is not None
    assert spec.program_runtime_pin.expected_git_sha == host_c_SHA
    assert spec.program_runtime_pin.scheduler_host == "host_c"
    assert spec.program_runtime_pin.artifact_identity == host_c_WRAPPER
    assert spec.command == [host_c_WRAPPER, "run.py"]


def test_unpinned_directory_payload_still_gets_target_runtime_identity(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()

    result = _submit_directory_unpinned(tmp_path, "host_c")

    assert result.exit_code == 0, result.output
    receipt = json.loads(result.output)
    assert receipt["program_runtime_pin"]["expected_git_sha"] == host_c_SHA
    spec = _queued_specs()[0]
    assert spec.command == ["bash", "run.sh"]
    assert spec.program_runtime_pin is not None
    assert spec.program_runtime_pin.scheduler_host == "host_c"
    assert spec.program_runtime_pin.expected_git_sha == host_c_SHA
    assert spec.program_runtime_pin.enforce_git_sha is True


def test_unpinned_directory_payload_without_target_identity_fails_closed(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))

    result = _submit_directory_unpinned(tmp_path, "host_c")

    assert result.exit_code != 0
    assert "no verified runtime deployment record" in result.output
    assert _queued_specs() == []


def test_host_f_record_fallback_executes_the_verified_immutable_python(
    state_dir: Path,
    driver_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    repo, _ = driver_checkout
    _write_host_f_config(
        state_dir / "cfg",
        driver_program_block=_venv_program_block(repo),
    )
    _record_host_f_runtime()

    result = _submit(tmp_path, "host_f", host_f_SHA)

    assert result.exit_code == 0, result.output
    pin = json.loads(result.output)["program_runtime_pin"]
    assert pin["resolved_executable"] == host_f_PYTHON
    assert pin["artifact_identity"] == host_f_PYTHON
    spec = _queued_specs()[0]
    assert spec.command == [host_f_PYTHON, "run.py"]
    assert spec.program_runtime_pin is not None
    assert spec.program_runtime_pin.resolved_executable == host_f_PYTHON


def test_mismatch_names_the_target_identity_and_creates_nothing(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, driver_sha = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()

    result = _submit(tmp_path, "host_c", driver_sha)

    assert result.exit_code != 0
    assert "scheduler target 'host_c'" in result.output
    assert host_c_SHA in result.output
    assert host_c_WRAPPER in result.output
    assert "driver-local program" in result.output
    assert _queued_specs() == [], "a rejected pin must create no spec"

    # Retry with the correct pin: exactly one spec, no duplicate.
    retry = _submit(tmp_path, "host_c", host_c_SHA)
    assert retry.exit_code == 0, retry.output
    assert len(_queued_specs()) == 1


def test_campaign_alias_shares_the_cluster_identity(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    """host_c-campaign has no record of its own; the ssh endpoint links it
    to host_c's verified deployment."""
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()

    result = _submit(tmp_path, "host_c-campaign", host_c_SHA)

    assert result.exit_code == 0, result.output
    receipt = json.loads(result.output)
    assert receipt["host"] == "host_c-campaign"
    assert receipt["program_runtime_pin"]["scheduler_host"] == "host_c-campaign"
    spec = _queued_specs()[0]
    assert spec.scheduler_target == "host_c-campaign"
    assert spec.program_runtime_pin.expected_git_sha == host_c_SHA


def test_no_verified_record_fails_closed(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))

    result = _submit(tmp_path, "host_c", host_c_SHA)

    assert result.exit_code != 0
    assert "no verified runtime deployment record" in result.output
    assert _queued_specs() == []


def test_last_good_identity_survives_a_failed_deploy(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()
    _record_host_c_runtime(sha="c" * 40, tag="v0.15.57", success=False)

    result = _submit(tmp_path, "host_c", host_c_SHA)

    assert result.exit_code == 0, result.output
    assert (
        json.loads(result.output)["program_runtime_pin"]["expected_git_sha"]
        == host_c_SHA
    )


def test_immutable_binary_driver_program_no_longer_blocks_the_pin(
    state_dir: Path, tmp_path: Path
) -> None:
    """The old path demanded a driver-local VENV program; a binary/wrapper
    driver entry must work because the driver entry is irrelevant."""
    _write_config(
        state_dir / "cfg",
        driver_program_block="\n".join(
            [
                "[programs.vibeqc-release]",
                'kind = "binary"',
                'binary = "/usr/bin/true"',
            ]
        ),
    )
    _record_host_c_runtime()

    result = _submit(tmp_path, "host_c", host_c_SHA)

    assert result.exit_code == 0, result.output


def test_array_validates_before_the_first_spec_exists(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path
) -> None:
    repo, driver_sha = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime()

    bad = _submit(tmp_path, "host_c", driver_sha, "--array", "3")
    assert bad.exit_code != 0
    assert _queued_specs() == [], "no array sibling may exist after a rejected pin"

    good = _submit(tmp_path, "host_c", host_c_SHA, "--array", "3")
    assert good.exit_code == 0, good.output
    specs = _queued_specs()
    assert len(specs) == 3
    assert all(
        s.program_runtime_pin is not None
        and s.program_runtime_pin.expected_git_sha == host_c_SHA
        for s in specs
    )
    assert all(s.command == [host_c_WRAPPER, "run.py"] for s in specs)


def test_registered_wrapper_outranks_a_newer_deployment_record(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE LIVE SCENARIO: host_c's managed deployment moved to v0.15.57
    (8327ab35b) but the registered program still pins the immutable 0.15.50
    wrapper — and the wrapper is what a submitted job executes. The pin
    must validate against the registry, not the newer record."""
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _record_host_c_runtime(sha="8327ab35b" + "0" * 31, tag="v0.15.57")
    _registry_answers(monkeypatch, host_c_WRAPPER)

    result = _submit(tmp_path, "host_c", host_c_SHA)

    assert result.exit_code == 0, result.output
    pin = json.loads(result.output)["program_runtime_pin"]
    assert pin["resolved_executable"] == host_c_WRAPPER
    assert pin["program_kind"] == "binary"
    assert pin["program_version"] == "0.15.50"
    assert pin["expected_git_sha"] == host_c_SHA


def test_pinning_the_deployment_sha_fails_when_the_wrapper_executes(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The converse: pinning the freshly deployed 0.15.57 SHA must be
    rejected while the registry still routes jobs to the 0.15.50 wrapper —
    accepting it would mislabel what actually runs."""
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    new_sha = "8327ab35b" + "0" * 31
    _record_host_c_runtime(sha=new_sha, tag="v0.15.57")
    _registry_answers(monkeypatch, host_c_WRAPPER)

    result = _submit(tmp_path, "host_c", new_sha)

    assert result.exit_code != 0
    assert "registered runtime is" in result.output
    assert host_c_WRAPPER in result.output
    assert _queued_specs() == []


NEW_WRAPPER = (
    "/home/USER/.local/libexec/vq-host_c/"
    "vibeqc-release-0.15.58-a865aeaeaa2b-python"
)
ALT_host_c_WRAPPER = (
    "/payload/data/vibeqc-release-0.15.50-820e718-python"
)


def _submit_dir_command(
    tmp_path: Path,
    target: str,
    expected: str,
    command: list[str],
    *,
    python: str | None = None,
):  # type: ignore[no-untyped-def]
    job_dir = tmp_path / "wavejob"
    job_dir.mkdir(exist_ok=True)
    (job_dir / "input.py").write_text("print('hi')\n")
    python_args = ["--python", python] if python is not None else []
    return CliRunner().invoke(
        main,
        [
            "submit", target, "-d", str(job_dir),
            "--program", "vibeqc-release",
            "--expected-sha", expected,
            "--cpus", "1",
            "--wall-time-seconds", "60",
            *python_args,
            "--json", "--",
            *command,
        ],
    )


def _submit_dir(tmp_path: Path, target: str, expected: str, wrapper: str):  # type: ignore[no-untyped-def]
    return _submit_dir_command(
        tmp_path,
        target,
        expected,
        [wrapper, "input.py"],
    )


def test_wrapper_looking_non_head_argument_is_data_not_runtime_authority(
    state_dir: Path,
    driver_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    """A wrapper-shaped payload argument must not outrank the deployment."""
    repo, _ = driver_checkout
    _write_config(
        state_dir / "cfg",
        driver_program_block=_venv_program_block(repo),
    )
    _record_host_c_runtime()

    result = _submit_dir_command(
        tmp_path,
        "host_c",
        host_c_SHA,
        ["python", "input.py", ALT_host_c_WRAPPER],
    )

    assert result.exit_code == 0, result.output
    pin = json.loads(result.output)["program_runtime_pin"]
    assert pin["resolved_executable"] == host_c_WRAPPER
    assert pin["expected_git_sha"] == host_c_SHA
    assert _queued_specs()[0].command == [
        host_c_WRAPPER,
        "input.py",
        ALT_host_c_WRAPPER,
    ]


def test_mismatched_non_head_wrapper_does_not_reject_target_pin(
    state_dir: Path,
    driver_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    """Only the executable slot can contradict the requested target SHA."""
    repo, _ = driver_checkout
    _write_config(
        state_dir / "cfg",
        driver_program_block=_venv_program_block(repo),
    )
    _record_host_c_runtime()

    result = _submit_dir_command(
        tmp_path,
        "host_c",
        host_c_SHA,
        ["python", "input.py", NEW_WRAPPER],
    )

    assert result.exit_code == 0, result.output
    assert _queued_specs()[0].command == [
        host_c_WRAPPER,
        "input.py",
        NEW_WRAPPER,
    ]


def test_explicit_python_is_sole_runtime_authority(
    state_dir: Path,
    driver_checkout: tuple[Path, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--python A -- B input.py`` validates A, never payload head B."""
    repo, _ = driver_checkout
    _write_config(
        state_dir / "cfg",
        driver_program_block=_venv_program_block(repo),
    )
    _registry_answers(monkeypatch, NEW_WRAPPER)

    result = _submit_dir_command(
        tmp_path,
        "host_c",
        host_c_SHA,
        [NEW_WRAPPER, "input.py"],
        python=host_c_WRAPPER,
    )

    assert result.exit_code == 0, result.output
    pin = json.loads(result.output)["program_runtime_pin"]
    assert pin["resolved_executable"] == host_c_WRAPPER
    assert pin["expected_git_sha"] == host_c_SHA
    assert _queued_specs()[0].command == [host_c_WRAPPER, "input.py"]


def test_unpinned_python_directory_uses_verified_target_launcher(
    state_dir: Path,
    driver_checkout: tuple[Path, str],
    tmp_path: Path,
) -> None:
    """NEW #10: a bare Python payload must not reach the batch script."""
    repo, _ = driver_checkout
    _write_config(
        state_dir / "cfg",
        driver_program_block=_venv_program_block(repo),
    )
    _record_host_c_runtime()
    job_dir = tmp_path / "managed-wave"
    job_dir.mkdir()
    (job_dir / "input.py").write_text("print('hi')\n")

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "host_c",
            "-d",
            str(job_dir),
            "--program",
            "vibeqc-release",
            "--json",
            "--",
            "python",
            "input.py",
        ],
    )

    assert result.exit_code == 0, result.output
    spec = _queued_specs()[0]
    assert spec.command == [host_c_WRAPPER, "input.py"]
    assert spec.program_runtime_pin is not None
    assert spec.program_runtime_pin.expected_git_sha == host_c_SHA
    assert spec.program_runtime_pin.resolved_executable == host_c_WRAPPER


def test_wrapper_in_the_command_outranks_a_rolled_forward_registry(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE v0.15.58 SCENARIO: the registry rolled forward to 0.15.58 while
    the wave deliberately executes the immutable 0.15.50 wrapper named in
    its command. The pin must describe the argv that runs."""
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _registry_answers(monkeypatch, NEW_WRAPPER)

    result = _submit_dir(tmp_path, "host_c", host_c_SHA, host_c_WRAPPER)

    assert result.exit_code == 0, result.output
    pin = json.loads(result.output)["program_runtime_pin"]
    assert pin["resolved_executable"] == host_c_WRAPPER
    assert pin["program_version"] == "0.15.50"
    assert pin["expected_git_sha"] == host_c_SHA
    assert _queued_specs()[0].command == [host_c_WRAPPER, "input.py"]


def test_pin_mismatching_the_command_wrapper_is_rejected(
    state_dir: Path, driver_checkout: tuple[Path, str], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The converse: pinning the registry's new SHA while the command
    executes the old wrapper must fail, naming the command wrapper."""
    repo, _ = driver_checkout
    _write_config(state_dir / "cfg", driver_program_block=_venv_program_block(repo))
    _registry_answers(monkeypatch, NEW_WRAPPER)

    result = _submit_dir(
        tmp_path, "host_c", "a865aeaeaa2b6026a200d10eb73621e8fc584463",
        host_c_WRAPPER,
    )

    assert result.exit_code != 0
    assert "the submit command executes" in result.output
    assert host_c_WRAPPER in result.output
    assert _queued_specs() == []

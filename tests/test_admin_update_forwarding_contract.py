"""Characterization of delegated admin-update transcripts.

These tests pin the wire-visible contract before the admin-update command is
split into credential, target-classification, and forwarding helpers.  They
stop at the transport boundary: no SSH connection, scheduler command, daemon,
or update script is contacted.
"""
from __future__ import annotations

import inspect
import json
import subprocess
from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from vq import admin_detached, cli, config, paths, transport
from vq.cli import main

_SHA = "a" * 40
_SHA_INPUT = "A" * 40
_DEFAULT_REMOTE_TIMEOUT_ENV = {
    "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
    "VQ_BUILD_STALL_TIMEOUT": "3600.0",
}


def test_cli_delegation_api_has_no_transient_retry_escape_hatch() -> None:
    assert "retry_transient" not in inspect.signature(
        cli._delegate_to_remote
    ).parameters
    assert "retry_transient" not in inspect.signature(
        cli._forward_admin_command
    ).parameters


def test_timeout_contract_captures_forwarded_wall_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def resolve_environment(host_cfg: object = None) -> dict[str, str]:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise AssertionError("timeout environment was read more than once")
        return {
            "VQ_UPDATE_SCRIPT_TIMEOUT": "21600.5",
            "VQ_BUILD_STALL_TIMEOUT": "0.0",
        }

    monkeypatch.setattr(cli, "_remote_admin_update_environment", resolve_environment)
    monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)

    remote_env, outer = cli._remote_admin_update_contract()

    assert calls == 1
    assert remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "21600.5"
    assert outer == 22200.5


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setenv("VQ_TOKEN", "driver-secret")
    monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)
    monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
    monkeypatch.delenv("VQ_BUILD_STALL_TIMEOUT", raising=False)
    monkeypatch.setattr(
        "vq.host.socket.gethostname",
        lambda: "admin-forwarding-contract-local",
    )
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True)
    paths.jobs_dir().mkdir(parents=True)
    return tmp_path


def _write_config(state_dir: Path, contents: str) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        dedent(contents).strip() + "\n"
    )


def _write_scheduler_config(
    state_dir: Path,
    *,
    collision: bool = False,
    runtime_deployment: bool = True,
) -> None:
    collision_program = """
        [programs.host_f]
        kind = "venv"
        python = "/fake/host_f-python"
        git_dir = "/fake/host_f-repo"
        branch = "main"
    """ if collision else ""
    runtime_section = """
        [hosts.host_f.scheduler_runtime_deployments.vibeqc-dev]
        update_command = "/site/bin/update-runtime"
        install_command = "/site/bin/install-runtime"
        update_host = "cluster-build"
        verify_command = "/site/bin/verify-runtime"
        timeout_seconds = 9000
    """ if runtime_deployment else ""
    _write_config(
        state_dir,
        f"""
        default_host = "driver"

        [hosts.driver]
        ssh = "driver.example.invalid"
        remote_vq = "/opt/vq/bin/vq"

        [hosts.host_f]
        ssh = "host_f-login.example.invalid"
        fleet_role = "managed"
        scheduler = "pbs"
        scheduler_dialect = "torque"
        scheduler_driver = "driver"
        scratch_root = "/cluster/scratch"
        admin_token_file = "/etc/vq/host_f-admin-token"
        scheduler_update_command = "/site/bin/update-helper"
        scheduler_install_command = "/site/bin/install-helper"
        scheduler_update_timeout_seconds = 8000

        {runtime_section}

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"

        {collision_program}
        """,
    )


@pytest.fixture(autouse=True)
def _fast_detached_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the wire-shape tests off the detached poll loop's real clock.

    A delegated venv update now launches the remote work detached and follows
    it by polling. These tests pin argv and error classification, not timing,
    so the intervals collapse to zero. The graces collapse too, which makes an
    unreachable host fail on its first poll -- the same immediate answer the
    attached path gave, which is exactly what the ambiguity tests assert.
    """
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli, "_DETACHED_POLL_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(cli, "_DETACHED_OBSERVATION_GRACE_SECONDS", 0.0)


def _completed_observation(run_id: str, payload: str) -> str:
    """A terminal receipt carrying ``payload`` as the update's stdout."""
    return json.dumps(
        {
            "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
            "run_id": run_id,
            "state": admin_detached.STATE_COMPLETED,
            "detail": "stub completed",
            "target": "vibeqc-dev",
            "pid": 4242,
            "transcript": None,
            "transcript_offset": 0,
            "transcript_next_offset": 0,
            "transcript_size": 0,
            "transcript_base64": "",
            "outcome": "ok",
            "exit_code": 0,
            "payload": payload,
            "error": None,
        }
    )


def _detached_launch(calls: list[dict[str, object]]) -> dict[str, object]:
    """The one mutating launch among a detached delegation's calls.

    The invariant the old transcript pinned -- a mutating admin command is
    sent exactly once -- did not go away when the work was detached; it just
    stopped being the only call on the wire. Everything else is a read-only
    observation, which is replay-safe by construction.
    """
    launches = [
        call
        for call in calls
        if tuple(list(call["argv"])[:2]) == ("admin", "update")  # type: ignore[index]
    ]
    assert len(launches) == 1, f"expected exactly one mutating launch, got {launches}"
    return launches[0]


def _without_detach_handshake(argv: list[str]) -> list[str]:
    """argv minus the detach handshake, validating the run id on the way.

    Lets each transcript keep pinning the argv an operator would recognise
    rather than re-spelling it around a random run id.
    """
    out: list[str] = []
    index = 0
    while index < len(argv):
        if argv[index] == "--detach":
            index += 1
            continue
        if argv[index] == "--detach-run-id":
            admin_detached.validate_run_id(argv[index + 1])
            index += 2
            continue
        out.append(argv[index])
        index += 1
    return out


def _is_observation(cmd: list[str]) -> bool:
    """Is this ssh argv a read-only poll rather than the mutating launch?"""
    return "admin observe-update" in cmd[-1]


def _assert_detached_launch(
    calls: list[dict[str, object]],
    *,
    ssh: str,
    argv: list[str],
    stdin_data: str | None,
    remote_env: object = _DEFAULT_REMOTE_TIMEOUT_ENV,
) -> dict[str, object]:
    """Pin the one mutating call a detached delegation makes.

    The argv is compared with the detach handshake removed, so each test still
    reads as the command an operator would recognise. The launch's SSH cap is
    the short activation window, not the build's wall clock: the build is no
    longer running underneath this connection, which is the point.
    """
    launch = _detached_launch(calls)
    assert launch["ssh"] == ssh
    assert _without_detach_handshake(launch["argv"]) == argv  # type: ignore[arg-type]
    expected_transport: dict[str, object] = {
        "stdin_data": stdin_data,
        "retry_transient": 0,
        "timeout": cli._DETACHED_LAUNCH_TIMEOUT_SECONDS,
    }
    if remote_env is not None:
        expected_transport["remote_env"] = remote_env
    assert launch["transport"] == expected_transport
    return launch


def _capture_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stdout: str = "REMOTE-OK\n",
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def fake_run_remote_vq(  # type: ignore[no-untyped-def]
        host_cfg,
        *vq_args,
        check=True,
        stdin_data=None,
        retry_transient=0,
        timeout=transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
        remote_env=None,
    ):
        assert check is True
        calls.append(
            {
                "ssh": host_cfg.ssh,
                "argv": list(vq_args),
                "transport": {
                    "stdin_data": stdin_data,
                    "retry_transient": retry_transient,
                    "timeout": timeout,
                    **({"remote_env": remote_env} if remote_env is not None else {}),
                },
            }
        )
        if tuple(vq_args[:2]) == ("admin", "observe-update"):
            return subprocess.CompletedProcess(
                args=[host_cfg.ssh, *vq_args],
                returncode=0,
                stdout=_completed_observation(vq_args[2], stdout),
                stderr="",
            )
        return subprocess.CompletedProcess(
            args=[host_cfg.ssh, *vq_args],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
    return calls


def test_scheduler_runtime_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir)
    calls = _capture_transport(monkeypatch, stdout="REMOTE-RUNTIME\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "host_f",
            "--expected-sha",
            _SHA_INPUT,
            "--tag",
            "v0.15.111",
            "--cluster-install",
            "--force",
            "--json",
            "--update-script-arg=--wait",
            "--update-script-arg=--clean",
            "--show-output",
            "--drain-wait",
            "7s",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "REMOTE-RUNTIME\n"
    assert calls == [
        {
            "ssh": "driver.example.invalid",
            "argv": [
                "admin",
                "update",
                "vibeqc-dev",
                "host_f",
                "--expected-sha",
                _SHA,
                "--tag",
                "v0.15.111",
                "--cluster-install",
                "--force",
                "--json",
                "--update-script-arg",
                "--wait",
                "--update-script-arg",
                "--clean",
                "--show-output",
                "--drain-wait",
                "7s",
                "--token-stdin",
            ],
            "transport": {
                "stdin_data": "driver-secret\n",
                "retry_transient": 0,
                "timeout": 15007.0,
            },
        }
    ]


def test_scheduler_helper_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir)
    calls = _capture_transport(monkeypatch, stdout="REMOTE-HELPER\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "host_f",
            "--cluster-install",
            "--expected-sha",
            _SHA,
            "--force",
            "--json",
            "--update-script-arg=--wait",
            "--show-output",
            "--drain-wait",
            "9s",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "REMOTE-HELPER\n"
    assert calls == [
        {
            "ssh": "driver.example.invalid",
            "argv": [
                "admin",
                "update",
                "host_f",
                "--cluster-install",
                "--expected-sha",
                _SHA,
                "--force",
                "--json",
                "--update-script-arg",
                "--wait",
                "--show-output",
                "--drain-wait",
                "9s",
                "--token-stdin",
            ],
            "transport": {
                "stdin_data": "driver-secret\n",
                "retry_transient": 0,
                "timeout": 15009.0,
            },
        }
    ]


def test_unbounded_scheduler_helper_timeout_stays_unbounded_with_drain(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir)
    cfg = config.load_config()
    cfg.hosts["host_f"].scheduler_update_timeout_seconds = None
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "host_f", "--drain-wait", "9s"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["transport"] == {
        "stdin_data": "driver-secret\n",
        "retry_transient": 0,
        "timeout": None,
    }


def test_unbounded_scheduler_runtime_timeout_stays_unbounded_with_drain(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir)
    cfg = config.load_config()
    deployment = cfg.hosts["host_f"].scheduler_runtime_deployments["vibeqc-dev"]
    deployment.timeout_seconds = None
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "host_f",
            "--expected-sha",
            _SHA,
            "--drain-wait",
            "7s",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["transport"] == {
        "stdin_data": "driver-secret\n",
        "retry_transient": 0,
        "timeout": None,
    }


def test_single_venv_host_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"
        remote_vq = "/opt/vq/bin/vq"
        admin_token_file = "/etc/vq/remote-admin-token"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    calls = _capture_transport(monkeypatch, stdout="REMOTE-VENV\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "remote",
            "--tag",
            "v0.15.111",
            "--expected-sha",
            _SHA,
            "--no-restart-daemon",
            "--force",
            "--json",
            "--update-script-arg=--wait",
            "--show-output",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == "REMOTE-VENV\n"
    assert "delegated venv update timeouts: remote wall=14400s; " in result.stderr
    launch = _detached_launch(calls)
    assert launch == {
        "ssh": "remote.example.invalid",
        "argv": launch["argv"],
        "transport": {
            "stdin_data": None,
            "retry_transient": 0,
            # The launch only spawns and waits for activation; the build's own
            # wall clock is the remote watchdog, not this connection.
            "timeout": cli._DETACHED_LAUNCH_TIMEOUT_SECONDS,
            "remote_env": _DEFAULT_REMOTE_TIMEOUT_ENV,
        },
    }
    assert _without_detach_handshake(launch["argv"]) == [  # type: ignore[arg-type]
        "admin",
        "update",
        "vibeqc-dev",
        "--tag",
        "v0.15.111",
        "--expected-sha",
        _SHA,
        "--no-restart-daemon",
        "--force",
        "--json",
        "--update-script-arg",
        "--wait",
        "--show-output",
        "--token-file",
        "/etc/vq/remote-admin-token",
        "localhost",
    ]
    observations = [c for c in calls if c is not launch]
    assert observations, "the driver must follow the run it launched"
    for call in observations:
        assert list(call["argv"])[:2] == ["admin", "observe-update"]  # type: ignore[index]


@pytest.mark.parametrize(
    ("failure", "expected_error"),
    [
        ("exit-255", "remote vq failed (exit 255)"),
        # The launch's own short cap, not the build's wall clock: the build no
        # longer runs underneath this connection.
        ("timeout", "remote vq timed out after 600.0s"),
        ("signal", "remote vq failed (terminated by signal"),
    ],
)
def test_ambiguous_remote_update_failure_is_attempted_once(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    expected_error: str,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    attempts: list[tuple[list[str], dict[str, object]]] = []

    def ambiguous(
        cmd: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        attempts.append((cmd, kwargs))
        if failure == "timeout":
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])
        returncode = -15 if failure == "signal" else 255
        return subprocess.CompletedProcess(
            cmd,
            returncode,
            stdout="",
            stderr="ssh connection lost after remote start",
        )

    monkeypatch.setattr(transport.subprocess, "run", ambiguous)
    monkeypatch.setattr(transport.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert expected_error in result.stderr
    assert "Remote admin outcome is unknown" in result.stderr
    assert "Do not retry it yet" in result.stderr
    assert "vq admin status remote --json" in result.stderr
    assert "marker/LAST OK/log" in result.stderr
    assert "exact live SHA or tag" in result.stderr
    if failure == "signal":
        assert "retry after" not in result.stderr
    # The invariant is unchanged and is the one that matters: the mutating
    # command crosses the wire exactly once. The extra calls are read-only
    # observations, which is precisely why they may be retried.
    mutations = [cmd for cmd, _kwargs in attempts if not _is_observation(cmd)]
    assert len(mutations) == 1
    cmd, kwargs = attempts[0]
    assert kwargs["input"] == "driver-secret\n"
    assert kwargs["timeout"] == cli._DETACHED_LAUNCH_TIMEOUT_SECONDS
    assert "driver-secret" not in " ".join(cmd)
    assert "VQ_UPDATE_SCRIPT_TIMEOUT=14400.0" in cmd[-1]
    assert "VQ_BUILD_STALL_TIMEOUT=3600.0" in cmd[-1]


@pytest.mark.parametrize("failure", ["exit-255", "timeout"])
@pytest.mark.parametrize(
    ("route", "argv", "status_target"),
    [
        (
            "single-all",
            ["admin", "update", "--all", "remote"],
            "remote",
        ),
        (
            "all-hosts",
            [
                "admin",
                "update",
                "vibeqc-dev",
                "--all-hosts",
                "--serial",
                "--json",
            ],
            "remote",
        ),
        (
            "scheduler-runtime",
            [
                "admin",
                "update",
                "vibeqc-dev",
                "host_f",
                "--expected-sha",
                _SHA,
            ],
            "host_f",
        ),
        (
            "scheduler-helper",
            ["admin", "update", "host_f", "--cluster-install"],
            "host_f",
        ),
    ],
)
def test_each_mutating_admin_route_attempts_ambiguous_transport_once(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    argv: list[str],
    status_target: str,
    failure: str,
) -> None:
    if route.startswith("scheduler-"):
        _write_scheduler_config(state_dir)
    else:
        _write_config(
            state_dir,
            """
            default_host = "remote"

            [hosts.remote]
            ssh = "remote.example.invalid"

            [programs.vibeqc-dev]
            kind = "venv"
            python = "/fake/python"
            git_dir = "/fake/repo"
            branch = "main"
            """,
        )
    attempts: list[list[str]] = []

    def ambiguous(
        cmd: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        attempts.append(cmd)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])
        return subprocess.CompletedProcess(
            cmd,
            255,
            stdout="",
            stderr="ssh connection lost after remote start",
        )

    monkeypatch.setattr(transport.subprocess, "run", ambiguous)
    monkeypatch.setattr(transport.time, "sleep", lambda _seconds: None)

    result = CliRunner().invoke(main, argv)

    assert result.exit_code != 0
    assert len([cmd for cmd in attempts if not _is_observation(cmd)]) == 1
    assert "Remote admin outcome is unknown" in result.output
    assert "may still be running or may have completed" in result.output
    assert "Do not retry it yet" in result.output
    assert f"vq admin status {status_target} --json" in result.output
    if route == "all-hosts":
        payload = json.loads(result.stdout)
        assert "Remote admin outcome is unknown" in payload["remote"]["error"]


def test_remote_update_preexec_failure_is_classified_not_started(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    attempts: list[tuple[list[str], dict[str, object]]] = []
    sleeps: list[float] = []

    def not_started(cmd: list[str], **kwargs: object):  # type: ignore[no-untyped-def]
        attempts.append((cmd, kwargs))
        raise FileNotFoundError(2, "No such file or directory", "ssh")

    monkeypatch.setattr(transport.subprocess, "run", not_started)
    monkeypatch.setattr(transport.time, "sleep", sleeps.append)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 1
    assert len(attempts) == 1
    assert "Remote admin command was not started" in result.stderr
    assert "no remote mutation was attempted" in result.stderr
    assert "outcome is unknown" not in result.stderr
    assert "Traceback" not in result.output
    assert sleeps == []
    cmd, kwargs = attempts[0]
    assert kwargs["input"] == "driver-secret\n"
    assert kwargs["timeout"] == cli._DETACHED_LAUNCH_TIMEOUT_SECONDS
    assert "driver-secret" not in " ".join(cmd)
    assert "VQ_UPDATE_SCRIPT_TIMEOUT=14400.0" in cmd[-1]
    assert "VQ_BUILD_STALL_TIMEOUT=3600.0" in cmd[-1]


def test_remote_update_generic_local_io_failure_is_ambiguous(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    attempts: list[list[str]] = []

    def observer_failed(
        cmd: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        attempts.append(cmd)
        raise OSError("local pipe failed after process creation")

    monkeypatch.setattr(transport.subprocess, "run", observer_failed)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 1
    assert len([cmd for cmd in attempts if not _is_observation(cmd)]) == 1
    assert "local ssh observer failed" in result.stderr
    assert "Remote admin outcome is unknown" in result.stderr
    assert "Do not retry it yet" in result.stderr
    assert "was not started" not in result.stderr


def test_known_remote_nonzero_keeps_existing_error_without_ambiguity_advice(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    attempts: list[list[str]] = []

    def rejected(
        cmd: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        attempts.append(cmd)
        return subprocess.CompletedProcess(
            cmd,
            2,
            stdout="",
            stderr="remote validation rejected the request",
        )

    monkeypatch.setattr(transport.subprocess, "run", rejected)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 1
    assert len(attempts) == 1
    assert "remote vq failed (exit 2)" in result.stderr
    assert "remote validation rejected the request" in result.stderr
    assert "outcome is unknown" not in result.stderr
    assert "Do not retry" not in result.stderr


def test_single_venv_forwards_only_validated_timeout_allowlist(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"
        remote_vq = "/opt/vq/bin/vq"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", " 21600.5 ")
    monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "0")
    # Equality with wall + cleanup margin is the smallest coherent observer.
    monkeypatch.setenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "22200.5")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-cross-ssh")
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 0, result.output
    assert (
        "delegated venv update timeouts: remote wall=21600.5s; "
        "remote stall=0s; outer=22200.5s"
    ) in result.stderr
    # The operator's outer cap still has to validate -- a malformed one fails
    # before any transport -- but it no longer bounds this call. It capped an
    # SSH session that was carrying a build; the launch carries an activation
    # handshake, and the build's own watchdogs are what the env below forwards.
    launch = _assert_detached_launch(
        calls,
        ssh="remote.example.invalid",
        argv=["admin", "update", "vibeqc-dev", "--token-stdin", "localhost"],
        stdin_data="driver-secret\n",
        remote_env={
            "VQ_UPDATE_SCRIPT_TIMEOUT": "21600.5",
            "VQ_BUILD_STALL_TIMEOUT": "0.0",
        },
    )
    forwarded = launch["transport"]
    assert isinstance(forwarded, dict)
    assert "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT" not in forwarded["remote_env"]
    assert "AWS_SECRET_ACCESS_KEY" not in forwarded["remote_env"]
    assert "VQ_TOKEN" not in forwarded["remote_env"]


def test_single_venv_accepts_safe_outer_below_unset_default(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "2000")
    monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "0")
    monkeypatch.setenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "2600")
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 0, result.output
    # A safe explicit override is still accepted rather than refused; what it
    # bounds is now the attached fallback, not this launch.
    _assert_detached_launch(
        calls,
        ssh="remote.example.invalid",
        argv=["admin", "update", "vibeqc-dev", "--token-stdin", "localhost"],
        stdin_data="driver-secret\n",
        remote_env={
            "VQ_UPDATE_SCRIPT_TIMEOUT": "2000.0",
            "VQ_BUILD_STALL_TIMEOUT": "0.0",
        },
    )


@pytest.mark.parametrize(
    ("name", "value", "requirement"),
    [
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "garbage", "greater than 0"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "0", "greater than 0"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "-1", "greater than 0"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "nan", "greater than 0"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "inf", "greater than 0"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "-inf", "greater than 0"),
        ("VQ_BUILD_STALL_TIMEOUT", "garbage", "greater than or equal to 0"),
        ("VQ_BUILD_STALL_TIMEOUT", "-1", "greater than or equal to 0"),
        ("VQ_BUILD_STALL_TIMEOUT", "nan", "greater than or equal to 0"),
        ("VQ_BUILD_STALL_TIMEOUT", "inf", "greater than or equal to 0"),
        ("VQ_BUILD_STALL_TIMEOUT", "-inf", "greater than or equal to 0"),
    ],
)
def test_invalid_forwarded_timeout_fails_before_transport(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    requirement: str,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    monkeypatch.setenv(name, value)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 2
    assert name in result.output
    assert requirement in result.output
    assert calls == []


@pytest.mark.parametrize("outer", ["22200.4", "garbage", "0", "nan", "inf"])
def test_invalid_or_undersized_outer_timeout_fails_before_transport(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    outer: str,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "21600.5")
    monkeypatch.setenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", outer)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "remote"],
    )

    assert result.exit_code == 2
    assert "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT" in result.output
    if outer == "22200.4":
        assert "at least 22200.5" in result.output
    else:
        assert "finite number greater than 0" in result.output
    assert calls == []


def test_all_hosts_rejects_invalid_timeout_before_local_work_or_transport(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        [hosts.localhost]
        ssh = "localhost"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "-1")
    local_calls: list[str] = []

    def fake_update_env(*args, **kwargs):  # type: ignore[no-untyped-def]
        local_calls.append("update")
        return type(
            "Result",
            (),
            {"success": True, "update_script_output": ""},
        )()

    monkeypatch.setattr("vq.cli.admin_module.update_env", fake_update_env)
    monkeypatch.setattr(
        "vq.cli.admin_module.format_update_result", lambda _result: "LOCAL-OK"
    )
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "--all-hosts", "--serial"],
    )

    assert result.exit_code == 2
    assert "VQ_BUILD_STALL_TIMEOUT" in result.output
    assert local_calls == []
    assert calls == []


def test_local_only_update_keeps_existing_environment_semantics(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "localhost"

        [hosts.localhost]
        ssh = "localhost"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    # Local update helpers retain their established call-time fallback. R3's
    # strict validation is the direct-remote boundary, not a global policy
    # change to admin._update_script_timeout().
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "garbage")

    result_obj = type(
        "Result",
        (),
        {"success": True, "update_script_output": ""},
    )()
    monkeypatch.setattr(
        "vq.cli.admin_module.update_env", lambda *args, **kwargs: result_obj
    )
    monkeypatch.setattr(
        "vq.cli.admin_module.format_update_result", lambda _result: "LOCAL-OK"
    )
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "localhost"],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "LOCAL-OK\n"
    assert calls == []


def test_all_host_venv_json_error_and_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        [hosts.alpha]
        ssh = "alpha.example.invalid"
        admin_token_file = "/etc/vq/alpha-token"

        [hosts.beta]
        ssh = "beta.example.invalid"
        admin_token_file = "/etc/vq/beta-token"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    calls: list[dict[str, object]] = []

    def fake_run_remote_vq(  # type: ignore[no-untyped-def]
        host_cfg,
        *vq_args,
        check=True,
        stdin_data=None,
        retry_transient=0,
        timeout=transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
        remote_env=None,
    ):
        assert check is True
        calls.append(
            {
                "ssh": host_cfg.ssh,
                "argv": list(vq_args),
                "transport": {
                    "stdin_data": stdin_data,
                    "retry_transient": retry_transient,
                    "timeout": timeout,
                    "remote_env": remote_env,
                },
            }
        )
        if host_cfg.ssh.startswith("beta"):
            raise transport.RemoteError("remote vq failed (exit 255): link down")
        if tuple(vq_args[:2]) == ("admin", "observe-update"):
            return subprocess.CompletedProcess(
                args=[host_cfg.ssh, *vq_args],
                returncode=0,
                stdout=_completed_observation(
                    vq_args[2], '{"host": "alpha", "ok": true}\n'
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(
            args=[host_cfg.ssh, *vq_args],
            returncode=0,
            stdout='{"host": "alpha", "ok": true}\n',
            stderr="",
        )

    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "--all-hosts",
            "--serial",
            "--tag",
            "v0.15.111",
            "--expected-sha",
            _SHA,
            "--no-restart-daemon",
            "--force",
            "--json",
            "--update-script-arg=--wait",
            "--show-output",
        ],
    )

    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout) == {
        "alpha": {"host": "alpha", "ok": True},
        "beta": {"error": "remote vq failed (exit 255): link down"},
    }
    assert "admin update --all-hosts: 1 host(s) failed (beta)" in result.output
    expected_tail_by_host = {
        "alpha.example.invalid": ["--token-file", "/etc/vq/alpha-token", "localhost"],
        "beta.example.invalid": ["--token-file", "/etc/vq/beta-token", "localhost"],
    }
    assert {str(call["ssh"]) for call in calls} == set(expected_tail_by_host)
    launches = [
        call
        for call in calls
        if tuple(list(call["argv"])[:2]) == ("admin", "update")  # type: ignore[index]
    ]
    assert len(launches) == 2, "one mutating launch per host, and no more"
    for call in launches:
        argv = call["argv"]
        assert isinstance(argv, list)
        assert _without_detach_handshake(argv) == [
            "admin",
            "update",
            "vibeqc-dev",
            "--tag",
            "v0.15.111",
            "--expected-sha",
            _SHA,
            "--no-restart-daemon",
            "--force",
            "--json",
            "--update-script-arg",
            "--wait",
            "--show-output",
            *expected_tail_by_host[str(call["ssh"])],
        ]
        assert call["transport"] == {
            "stdin_data": None,
            "retry_transient": 0,
            "timeout": cli._DETACHED_LAUNCH_TIMEOUT_SECONDS,
            "remote_env": _DEFAULT_REMOTE_TIMEOUT_ENV,
        }


def test_single_host_all_envs_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    calls = _capture_transport(monkeypatch, stdout="REMOTE-ALL\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "--all",
            "remote",
            "--no-restart-daemon",
            "--force",
            "--json",
            "--update-script-arg=--wait",
            "--show-output",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == "REMOTE-ALL\n"
    assert (
        "delegated venv update timeouts: remote wall=14400s; "
        "remote stall=3600s; outer=unbounded (remote --all batch)"
    ) in result.stderr
    _assert_detached_launch(
        calls,
        ssh="remote.example.invalid",
        argv=[
            "admin",
            "update",
            "--all",
            "--no-restart-daemon",
            "--force",
            "--json",
            "--update-script-arg",
            "--wait",
            "--show-output",
            "--token-stdin",
            "localhost",
        ],
        stdin_data="driver-secret\n",
    )


def test_all_hosts_all_envs_forwards_each_watchdog_without_aggregate_cap(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        [hosts.alpha]
        ssh = "alpha.example.invalid"

        [hosts.beta]
        ssh = "beta.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "--all", "--all-hosts", "--serial"],
    )

    assert result.exit_code == 0, result.output
    assert {call["ssh"] for call in calls} == {
        "alpha.example.invalid",
        "beta.example.invalid",
    }
    # Every host's own wall/stall pair still crosses with its launch; the
    # aggregate batch still gets no locally-invented cap.
    launches = [
        call
        for call in calls
        if tuple(list(call["argv"])[:2]) == ("admin", "update")  # type: ignore[index]
    ]
    assert len(launches) == 2
    for call in launches:
        assert call["transport"] == {
            "stdin_data": "driver-secret\n",
            "retry_transient": 0,
            "timeout": cli._DETACHED_LAUNCH_TIMEOUT_SECONDS,
            "remote_env": _DEFAULT_REMOTE_TIMEOUT_ENV,
        }


def test_auto_update_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"
        remote_vq = "/opt/vq/bin/vq"

        [programs.vibeqc-release]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/release"
        branch = "release"
        """,
    )
    calls = _capture_transport(monkeypatch, stdout='{"decision": "skip"}\n')
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "21600")
    monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "7200")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "auto-update",
            "vibeqc-release",
            "remote",
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == '{"decision": "skip"}\n'
    assert calls == [
        {
            "ssh": "remote.example.invalid",
            "argv": [
                "admin",
                "auto-update",
                "vibeqc-release",
                "--dry-run",
                "--json",
                "--token-stdin",
                "localhost",
            ],
            "transport": {
                "stdin_data": "driver-secret\n",
                "retry_transient": 0,
                "timeout": transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
            },
        }
    ]


@pytest.mark.parametrize(
    ("extra_args", "expects_ambiguity_advice"),
    [
        ([], True),
        (["--dry-run"], False),
    ],
)
def test_auto_update_ambiguity_advice_applies_only_to_real_writes(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_args: list[str],
    expects_ambiguity_advice: bool,
) -> None:
    _write_config(
        state_dir,
        """
        default_host = "remote"

        [hosts.remote]
        ssh = "remote.example.invalid"

        [programs.vibeqc-release]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/release"
        branch = "release"
        """,
    )
    attempts: list[list[str]] = []

    def ambiguous(
        cmd: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        attempts.append(cmd)
        return subprocess.CompletedProcess(
            cmd,
            255,
            stdout="",
            stderr="ssh connection lost",
        )

    monkeypatch.setattr(transport.subprocess, "run", ambiguous)

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "auto-update",
            "vibeqc-release",
            "remote",
            *extra_args,
        ],
    )

    assert result.exit_code == 1
    # The mutating command crosses the wire exactly once either way. A real
    # write is launched detached, so what follows it is read-only polling of
    # that run -- which is why a lost launch response is adopted and only an
    # unobservable run keeps the advice. A dry run mutates nothing and stays
    # on the attached path, one call and no advice.
    mutations = [cmd for cmd in attempts if not _is_observation(cmd)]
    assert len(mutations) == 1
    assert ("--detach-run-id" in mutations[0][-1]) is expects_ambiguity_advice
    if not expects_ambiguity_advice:
        assert len(attempts) == 1
    assert (
        "Remote admin outcome is unknown" in result.stderr
    ) is expects_ambiguity_advice
    assert ("Do not retry it yet" in result.stderr) is expects_ambiguity_advice
    if expects_ambiguity_advice:
        assert "vq admin status remote --json" in result.stderr


def test_auto_update_all_hosts_forwarding_transcript(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(
        state_dir,
        """
        [hosts.alpha]
        ssh = "alpha.example.invalid"

        [hosts.beta]
        ssh = "beta.example.invalid"

        [programs.vibeqc-release]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/release"
        branch = "release"
        """,
    )
    calls: list[dict[str, object]] = []

    def fake_run_remote_vq(  # type: ignore[no-untyped-def]
        host_cfg,
        *vq_args,
        check=True,
        stdin_data=None,
        retry_transient=0,
        timeout=transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
    ):
        assert check is True
        calls.append(
            {
                "ssh": host_cfg.ssh,
                "argv": list(vq_args),
                "transport": {
                    "stdin_data": stdin_data,
                    "retry_transient": retry_transient,
                    "timeout": timeout,
                },
            }
        )
        label = host_cfg.ssh.split(".", maxsplit=1)[0]
        return subprocess.CompletedProcess(
            args=[host_cfg.ssh, *vq_args],
            returncode=0,
            stdout=f"AUTO-{label}\n\n",
            stderr="",
        )

    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "auto-update",
            "vibeqc-release",
            "--all-hosts",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == (
        "==== alpha ====\n"
        "AUTO-alpha\n\n"
        "==== beta ====\n"
        "AUTO-beta\n"
    )
    assert {str(call["ssh"]) for call in calls} == {
        "alpha.example.invalid",
        "beta.example.invalid",
    }
    for call in calls:
        assert call["argv"] == [
            "admin",
            "auto-update",
            "vibeqc-release",
            "--dry-run",
            "--token-stdin",
            "localhost",
        ]
        assert call["transport"] == {
            "stdin_data": "driver-secret\n",
            "retry_transient": 0,
            "timeout": transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
        }


def test_auto_update_real_write_launches_detached_on_each_host(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drift apply is the rebuild that died on 2026-09-11, so it detaches.

    The dry-run transcripts above stay attached: nothing they do can be cut
    off halfway. A real write takes the same handshake as a delegated
    `admin update` -- exactly one mutating launch per host, carrying the run
    id and the short launch cap, followed only by read-only observations.
    """
    _write_config(
        state_dir,
        """
        [hosts.alpha]
        ssh = "alpha.example.invalid"

        [hosts.beta]
        ssh = "beta.example.invalid"

        [programs.vibeqc-release]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/release"
        branch = "release"
        """,
    )
    calls = _capture_transport(monkeypatch, stdout="AUTO-OK\n")

    result = CliRunner().invoke(
        main,
        ["admin", "auto-update", "vibeqc-release", "--all-hosts"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == (
        "==== alpha ====\n"
        "AUTO-OK\n\n"
        "==== beta ====\n"
        "AUTO-OK\n"
    )
    launches = [
        call
        for call in calls
        if tuple(list(call["argv"])[:2]) == ("admin", "auto-update")  # type: ignore[index]
    ]
    assert sorted(str(call["ssh"]) for call in launches) == [
        "alpha.example.invalid",
        "beta.example.invalid",
    ]
    for call in launches:
        assert "--detach" in call["argv"]  # type: ignore[operator]
        assert _without_detach_handshake(call["argv"]) == [  # type: ignore[arg-type]
            "admin",
            "auto-update",
            "vibeqc-release",
            "--token-stdin",
            "localhost",
        ]
        assert call["transport"] == {
            "stdin_data": "driver-secret\n",
            "retry_transient": 0,
            "timeout": cli._DETACHED_LAUNCH_TIMEOUT_SECONDS,
        }
    observations = [call for call in calls if call not in launches]
    assert len(observations) == 2
    for call in observations:
        assert list(call["argv"])[:2] == ["admin", "observe-update"]  # type: ignore[index]


def test_program_name_wins_over_same_named_scheduler_host(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir, collision=True)
    monkeypatch.delenv("VQ_TOKEN")
    calls = _capture_transport(monkeypatch, stdout="REMOTE-PROGRAM\n")

    result = CliRunner().invoke(main, ["admin", "update", "host_f"])

    assert result.exit_code == 0, result.output
    assert result.stdout == "REMOTE-PROGRAM\n"
    _assert_detached_launch(
        calls,
        ssh="driver.example.invalid",
        argv=["admin", "update", "host_f", "localhost"],
        stdin_data=None,
    )


def test_configured_scheduler_runtime_without_sha_fails_before_transport(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "host_f"],
    )

    assert result.exit_code == 2
    assert "scheduler runtime deployment requires --expected-sha FULL_SHA" in (
        result.output
    )
    assert calls == []


def test_expected_sha_selects_unconfigured_scheduler_runtime_route(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_scheduler_config(state_dir, runtime_deployment=False)
    calls = _capture_transport(monkeypatch, stdout="REMOTE-RUNTIME-FALLBACK\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "host_f",
            "--expected-sha",
            _SHA_INPUT,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "REMOTE-RUNTIME-FALLBACK\n"
    assert calls == [
        {
            "ssh": "driver.example.invalid",
            "argv": [
                "admin",
                "update",
                "vibeqc-dev",
                "host_f",
                "--expected-sha",
                _SHA,
                "--json",
                "--token-stdin",
            ],
            "transport": {
                "stdin_data": "driver-secret\n",
                "retry_transient": 0,
                "timeout": 15000.0,
            },
        }
    ]


def test_unconfigured_scheduler_without_sha_uses_current_venv_route(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record the current fallthrough; changing it is a separate policy fix."""
    _write_scheduler_config(state_dir, runtime_deployment=False)
    calls = _capture_transport(monkeypatch, stdout="REMOTE-VENV-FALLBACK\n")

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-dev", "host_f", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == "REMOTE-VENV-FALLBACK\n"
    _assert_detached_launch(
        calls,
        ssh="host_f-login.example.invalid",
        argv=[
            "admin",
            "update",
            "vibeqc-dev",
            "--json",
            "--token-file",
            "/etc/vq/host_f-admin-token",
            "localhost",
        ],
        stdin_data=None,
    )


def test_invalid_all_host_json_is_wrapped_but_currently_nonfatal(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the current anomaly so the refactor cannot silently change policy."""
    _write_config(
        state_dir,
        """
        [hosts.alpha]
        ssh = "alpha.example.invalid"

        [programs.vibeqc-dev]
        kind = "venv"
        python = "/fake/python"
        git_dir = "/fake/repo"
        branch = "main"
        """,
    )
    calls = _capture_transport(monkeypatch, stdout="NOT-JSON\n")

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibeqc-dev",
            "--all-hosts",
            "--serial",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert set(payload) == {"alpha"}
    assert payload["alpha"]["error"].startswith("remote returned invalid JSON: ")
    launch = _assert_detached_launch(
        calls,
        ssh="alpha.example.invalid",
        argv=[
            "admin",
            "update",
            "vibeqc-dev",
            "--json",
            "--token-stdin",
            "localhost",
        ],
        stdin_data="driver-secret\n",
    )
    assert "driver-secret" not in launch["argv"]


@pytest.mark.no_autopatch_lifecycle_lock
class TestDeployOnePinFromTheReport:
    """`--from-report` takes the pinned argv from the report, not from typing.

    The accepted report records the exact argv tail per pin --
    `release  --tag v0.17.0 --expected-sha 6421ed34...`,
    `dev      --expected-sha 6421ed34...` -- and `rollout-latest` already
    deploys from it. A single-host `vq admin update` made the operator retype
    it, and omitting `--tag` for `vibeqc-release` fails inside the preparer
    rather than at the CLI.
    """

    SHA = "6" * 40

    def _config(self, state_dir: Path) -> None:
        self.reports = state_dir / "private-reports"
        subprocess.run(["git", "init", "-q", str(self.reports)], check=True)
        _write_config(
            state_dir,
            f"fleet_report_repo = {json.dumps(str(self.reports))}\n" + """
            default_host = "driver"

            [hosts.driver]
            ssh = "driver.example.invalid"
            remote_vq = "/opt/vq/bin/vq"

            [programs.vibeqc-release]
            kind = "venv"
            python = "/fake/python"
            git_dir = "/fake/repo"
            branch = "main"

            [programs.vibeqc-dev]
            kind = "venv"
            python = "/fake/python"
            git_dir = "/fake/repo"
            branch = "main"
            """,
        )

    def _report(self, monkeypatch: pytest.MonkeyPatch, **pins: object) -> None:
        from vq import fleet_release

        def discover(repo: Path, **kwargs: object) -> object:
            assert repo == self.reports.resolve()
            assert ("checkout", str(repo)) in cli.admin_module._active_toolset_lifecycle_resources()
            return fleet_release.FleetReleaseReport(
                source_ref="origin/main",
                source_path="vibe-queue/releases/v0.17.0.json",
                digest_sha256="d" * 64,
                generated_at=None,
                release_version=(0, 17, 0),
                pins=pins,
                raw={},
            )

        monkeypatch.setattr(
            "vq.cli.fleet_release.discover_latest_report", discover,
        )

    def _pin(self, name: str, sha: str, tag: str | None = None) -> object:
        from vq import fleet_release

        flags = (
            ("--tag", tag, "--expected-sha", sha)
            if tag is not None
            else ("--expected-sha", sha)
        )
        return fleet_release.FleetPin(
            name=name,
            sha=sha,
            version="0.17.0",
            deploy_flags=flags,
            gating_job="job",
            pipeline_id=1,
            evidence_sha=sha,
            acceptance_rule="A",
            tag=tag,
        )

    def _forwarded(self, monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
        sent: list[list[str]] = []

        def forward(host, cfg, remote_args, **kwargs):  # type: ignore[no-untyped-def]
            sent.append(list(remote_args))
            return None

        # A managed-env update delegates through the detached forwarder, which
        # adds the handshake itself; what this class pins is the pinned argv
        # the report supplied, so it stubs one level above that.
        monkeypatch.setattr("vq.cli._forward_venv_admin_update", forward)
        return sent

    def test_a_release_pin_supplies_both_tag_and_sha(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The exact omission that fails inside the preparer, not at the CLI."""
        self._config(state_dir)
        self._report(
            monkeypatch, release=self._pin("release", self.SHA, "v0.17.0"),
        )
        sent = self._forwarded(monkeypatch)

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-release", "driver", "--from-report"],
        )

        assert result.exit_code == 0, result.output
        assert sent == [[
            "admin", "update", "vibeqc-release",
            "--tag", "v0.17.0", "--expected-sha", self.SHA,
        ]]

    def test_a_branch_tip_pin_supplies_only_the_sha(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._config(state_dir)
        self._report(monkeypatch, dev=self._pin("dev", self.SHA))
        sent = self._forwarded(monkeypatch)

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "driver", "--from-report"],
        )

        assert result.exit_code == 0, result.output
        assert sent == [[
            "admin", "update", "vibeqc-dev", "--expected-sha", self.SHA,
        ]]
        assert "--tag" not in result.output

    def test_it_says_which_report_it_read(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._config(state_dir)
        self._report(
            monkeypatch, release=self._pin("release", self.SHA, "v0.17.0"),
        )
        self._forwarded(monkeypatch)

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-release", "driver", "--from-report"],
        )

        assert result.exit_code == 0, result.output
        assert "from accepted report v0.17.0 (dddddddddddd)" in result.output

    @pytest.mark.parametrize(
        ("flag", "value"),
        [("--expected-sha", "b" * 40), ("--tag", "v0.16.0")],
    )
    def test_a_flag_disagreeing_with_the_report_is_refused(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        flag: str,
        value: str,
    ) -> None:
        """Silently overriding in either direction would recreate the mistake
        this option exists to prevent."""
        self._config(state_dir)
        self._report(
            monkeypatch, release=self._pin("release", self.SHA, "v0.17.0"),
        )
        monkeypatch.setattr(
            "vq.cli._forward_admin_command",
            lambda *a, **k: pytest.fail("a disagreement must not deploy"),
        )

        result = CliRunner().invoke(
            main,
            [
                "admin", "update", "vibeqc-release", "driver",
                "--from-report", flag, value,
            ],
        )

        assert result.exit_code == 2
        assert "disagrees with the accepted report" in result.output

    def test_a_program_the_report_does_not_pin_is_refused(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_config(
            state_dir,
            """
            default_host = "driver"

            [hosts.driver]
            ssh = "driver.example.invalid"

            [programs.pyscf]
            kind = "venv"
            python = "/fake/python"
            git_dir = "/fake/repo"
            branch = "main"
            """,
        )

        result = CliRunner().invoke(
            main, ["admin", "update", "pyscf", "driver", "--from-report"],
        )

        assert result.exit_code == 2
        assert "does not know a pin for 'pyscf'" in result.output
        assert "vibeqc-release" in result.output

    @pytest.mark.parametrize("scope", ["--all", "--all-hosts"])
    def test_it_refuses_the_fleet_forms(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch, scope: str,
    ) -> None:
        self._config(state_dir)

        argv = ["admin", "update"]
        if scope == "--all-hosts":
            argv.append("vibeqc-release")
        argv += [scope, "--from-report"]
        result = CliRunner().invoke(main, argv)

        assert result.exit_code == 2
        assert "rollout-latest" in result.output


class TestProvisionDelegation:
    """`vq admin install ENV HOST` resolves on the driver, runs on the host.

    The provisioning verb's local path is covered by real git in
    `test_admin_provision.py`. This is the other half, and the half a first
    real fleet use exercises: what actually goes over the wire.
    """

    SHA = "7" * 40

    def _config(self, state_dir: Path) -> None:
        self.reports = state_dir / "private-reports"
        subprocess.run(["git", "init", "-q", str(self.reports)], check=True)
        _write_config(
            state_dir,
            f"fleet_report_repo = {json.dumps(str(self.reports))}\n" + """
            default_host = "driver"

            [hosts.driver]
            ssh = "driver.example.invalid"
            remote_vq = "/opt/vq/bin/vq"

            [programs.vibeqc-dev]
            kind = "venv"
            python = "/fake/repo/.venv/bin/python"
            git_dir = "/fake/repo"
            upstream = "ssh://git@example.invalid:26/mpei/vibe-qc.git"
            install_script = "scripts/install.sh"
            """,
        )

    def _forwarded(self, monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
        sent: list[list[str]] = []

        def forward(host, cfg, remote_args, **kwargs):  # type: ignore[no-untyped-def]
            sent.append(list(remote_args))
            assert kwargs["append_localhost"] is True
            return ""

        monkeypatch.setattr("vq.cli._forward_admin_command", forward)
        monkeypatch.setattr(
            "vq.cli.admin_module.provision_env",
            lambda *a, **k: pytest.fail("a remote target must not clone locally"),
        )
        return sent

    def test_the_remote_argv_carries_the_pin(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._config(state_dir)
        sent = self._forwarded(monkeypatch)

        result = CliRunner().invoke(
            main,
            [
                "admin", "install", "vibeqc-dev", "driver",
                "--expected-sha", self.SHA, "--tag", "v0.17.0",
                "--install-script-arg", "--editable",
                "--force", "--show-output",
            ],
        )

        assert result.exit_code == 0, result.output
        assert sent == [[
            "admin", "install", "vibeqc-dev",
            "--expected-sha", self.SHA,
            "--tag", "v0.17.0",
            "--install-script-arg", "--editable",
            "--force",
            "--show-output",
        ]]

    @pytest.mark.no_autopatch_lifecycle_lock
    def test_from_report_is_resolved_here_and_not_forwarded(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The external report store lives on the driver. A host that
        received `--from-report` would have to discover one of its own."""
        from vq import fleet_release

        self._config(state_dir)
        sent = self._forwarded(monkeypatch)
        monkeypatch.setattr(
            "vq.cli.fleet_release.discover_latest_report",
            lambda repo, **kw: fleet_release.FleetReleaseReport(
                source_ref="origin/main",
                source_path="vibe-queue/releases/v0.17.0.json",
                digest_sha256="e" * 64,
                generated_at=None,
                release_version=(0, 17, 0),
                pins={
                    "dev": fleet_release.FleetPin(
                        name="dev",
                        sha=self.SHA,
                        version="0.17.0",
                        deploy_flags=("--expected-sha", self.SHA),
                        gating_job="job",
                        pipeline_id=1,
                        evidence_sha=self.SHA,
                        acceptance_rule="A",
                    )
                },
                raw={},
            ),
        )

        result = CliRunner().invoke(
            main,
            ["admin", "install", "vibeqc-dev", "driver", "--from-report"],
        )

        assert result.exit_code == 0, result.output
        assert sent == [[
            "admin", "install", "vibeqc-dev", "--expected-sha", self.SHA,
        ]]
        assert "--from-report" not in sent[0]
        assert "from accepted report v0.17.0" in result.output

    def test_a_local_target_never_delegates(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_config(
            state_dir,
            """
            default_host = "localhost"

            [hosts.localhost]
            ssh = "localhost"

            [programs.vibeqc-dev]
            kind = "venv"
            python = "/fake/repo/.venv/bin/python"
            git_dir = "/fake/repo"
            upstream = "ssh://git@example.invalid:26/mpei/vibe-qc.git"
            install_script = "scripts/install.sh"
            """,
        )
        monkeypatch.setattr(
            "vq.cli._forward_admin_command",
            lambda *a, **k: pytest.fail("a local target must not delegate"),
        )
        seen: list[dict[str, object]] = []

        def provision(env, cfg, **kwargs):  # type: ignore[no-untyped-def]
            seen.append({"env": env, **kwargs})
            raise cli.admin_module.AdminError("stop here")

        monkeypatch.setattr("vq.cli.admin_module.provision_env", provision)

        result = CliRunner().invoke(
            main,
            ["admin", "install", "vibeqc-dev", "--expected-sha", self.SHA],
        )

        assert result.exit_code == 2
        assert seen and seen[0]["env"] == "vibeqc-dev"
        assert seen[0]["expected_sha"] == self.SHA


_host_e_CONFIG = """
    default_host = "host_e"

    [hosts.host_e]
    ssh = "host_e.example.invalid"
    update_script_timeout_seconds = 28800

    [hosts.host_b]
    ssh = "host_b.example.invalid"

    [programs.vibeqc-release]
    kind = "venv"
    python = "/fake/python"
    git_dir = "/fake/repo"
    branch = "main"
"""


def test_a_host_update_script_timeout_is_forwarded_as_the_wall_cap(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#32: host_e's cap comes from the driver's config, not a hand-set
    environment variable a planner-driven roll never sets."""
    monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
    _write_config(state_dir, _host_e_CONFIG)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-release", "host_e", "--expected-sha", _SHA],
    )

    assert result.exit_code == 0, result.output
    assert "remote wall=28800s" in result.stderr
    remote_env = _detached_launch(calls)["transport"]["remote_env"]  # type: ignore[index]
    assert remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "28800.0"  # type: ignore[index]
    assert (
        remote_env["VQ_BUILD_STALL_TIMEOUT"]  # type: ignore[index]
        == _DEFAULT_REMOTE_TIMEOUT_ENV["VQ_BUILD_STALL_TIMEOUT"]
    )


def test_an_explicit_update_script_timeout_still_wins_over_the_host_key(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "3600")
    _write_config(state_dir, _host_e_CONFIG)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        ["admin", "update", "vibeqc-release", "host_e", "--expected-sha", _SHA],
    )

    assert result.exit_code == 0, result.output
    remote_env = _detached_launch(calls)["transport"]["remote_env"]  # type: ignore[index]
    assert remote_env["VQ_UPDATE_SCRIPT_TIMEOUT"] == "3600.0"  # type: ignore[index]


def test_an_all_hosts_update_gives_each_host_its_own_wall_cap(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
    _write_config(state_dir, _host_e_CONFIG)
    calls = _capture_transport(monkeypatch)

    result = CliRunner().invoke(
        main,
        [
            "admin", "update", "vibeqc-release", "--all-hosts",
            "--expected-sha", _SHA,
        ],
    )

    assert result.exit_code == 0, result.output
    walls = {
        str(call["ssh"]): call["transport"]["remote_env"]["VQ_UPDATE_SCRIPT_TIMEOUT"]  # type: ignore[index]
        for call in calls
        if "--detach" in call["argv"]  # type: ignore[operator]
    }
    assert walls == {
        "host_e.example.invalid": "28800.0",
        "host_b.example.invalid": _DEFAULT_REMOTE_TIMEOUT_ENV["VQ_UPDATE_SCRIPT_TIMEOUT"],
    }

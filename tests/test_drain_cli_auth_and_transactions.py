"""Focused CLI contracts for authenticated and transactional drain writes."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from vq import auth, config, drain, paths, transport
from vq import cli as cli_module
from vq.cli import main


@pytest.fixture
def isolated_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    state = tmp_path / "state"
    config_dir = tmp_path / "config"
    state.mkdir()
    config_dir.mkdir()
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.delenv("VQ_TOKEN", raising=False)
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: False)
    return state


def _local_config() -> config.Config:
    return config.Config(
        default_host="localhost",
        hosts={"localhost": config.HostConfig(ssh="localhost")},
    )


def _install_config(
    monkeypatch: pytest.MonkeyPatch,
    cfg: config.Config,
    *,
    local_hosts: tuple[str, ...] = ("localhost",),
) -> None:
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        cli_module,
        "is_local_host",
        lambda host: host in local_hosts,
    )


def _capture_remote(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stdout: str = "remote drain ok\n",
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def fake_run_remote_vq(
        host_cfg: config.HostConfig,
        *vq_args: str,
        stdin_data: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(
            {
                "ssh": host_cfg.ssh,
                "argv": list(vq_args),
                "stdin_data": stdin_data,
            }
        )
        return subprocess.CompletedProcess(
            args=[host_cfg.ssh, *vq_args],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
    return calls


@pytest.mark.parametrize("credential_kind", ("argv", "stdin", "file"))
def test_remote_drain_credentials_are_forwarded_only_via_stdin(
    isolated_state: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    credential_kind: str,
) -> None:
    _ = isolated_state
    cfg = config.Config(
        hosts={"remote": config.HostConfig(ssh="remote.invalid")},
    )
    _install_config(monkeypatch, cfg, local_hosts=())
    calls = _capture_remote(monkeypatch)
    args = ["drain", "--max-jobs", "1", "remote"]
    input_text = None
    if credential_kind == "argv":
        args.extend(["--token", "driver-secret"])
        monkeypatch.setenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", "1")
    elif credential_kind == "stdin":
        args.append("--token-stdin")
        input_text = "driver-secret\n"
    else:
        token_file = tmp_path / "token"
        token_file.write_text("driver-secret\n")
        token_file.chmod(0o600)
        args.extend(["--token-file", str(token_file)])

    result = CliRunner().invoke(main, args, input=input_text)

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "ssh": "remote.invalid",
            "argv": [
                "drain",
                "--max-jobs",
                "1",
                "--token-stdin",
                "localhost",
            ],
            "stdin_data": "driver-secret\n",
        }
    ]
    assert all("driver-secret" not in arg for arg in calls[0]["argv"])


def test_remote_status_stays_open_and_does_not_consume_or_forward_auth(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    cfg = config.Config(
        hosts={"remote": config.HostConfig(ssh="remote.invalid")},
    )
    _install_config(monkeypatch, cfg, local_hosts=())
    calls = _capture_remote(monkeypatch, stdout="drain: inactive\n")
    monkeypatch.setattr(
        auth,
        "resolve_token",
        lambda *args, **kwargs: pytest.fail("status resolved a token"),
    )
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda *args, **kwargs: pytest.fail("status verified a token"),
    )

    result = CliRunner().invoke(
        main,
        ["drain", "--status", "--token-stdin", "remote"],
        input="must-not-be-read\n",
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "ssh": "remote.invalid",
            "argv": ["drain", "--status", "localhost"],
            "stdin_data": None,
        }
    ]


def test_local_system_multi_user_drain_verifies_and_bridges_stdin_token(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    verified: list[str] = []
    observed_rpc_token: list[str | None] = []
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda token: verified.append(token) or token == "local-secret",
    )
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)

    def fake_write(
        _state: drain.DrainState,
        *,
        token: str | None = None,
        multi_user: bool | None = None,
    ) -> None:
        assert multi_user is True
        observed_rpc_token.append(token)

    monkeypatch.setattr(drain, "write_drain_state", fake_write)

    result = CliRunner().invoke(
        main,
        ["drain", "--token-stdin", "localhost"],
        input="local-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert verified == ["local-secret"]
    assert observed_rpc_token == ["local-secret"]
    assert "VQ_TOKEN" not in os.environ


def test_local_system_multi_user_status_needs_no_token(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(drain, "format_status", lambda: "drain: open")
    monkeypatch.setattr(
        auth,
        "resolve_token",
        lambda *args, **kwargs: pytest.fail("status resolved a token"),
    )

    result = CliRunner().invoke(main, ["drain", "--status", "localhost"])

    assert result.exit_code == 0, result.output
    assert result.output == "drain: open\n"


def test_authenticated_scheduler_release_passes_token_once(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda token: token == "release-secret",
    )
    observed: list[tuple[str, str | None]] = []

    def release(
        host: str,
        *,
        token: str | None = None,
        multi_user: bool | None = None,
    ) -> bool:
        assert multi_user is True
        observed.append((host, token))
        return True

    monkeypatch.setattr(drain, "release_scheduler_host", release)

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--release",
            "--scheduler-host",
            "host_f",
            "--token-stdin",
            "localhost",
        ],
        input="release-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert observed == [("host_f", "release-secret")]


def test_all_local_driver_mutations_pass_token_without_environment_race(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    scheduler_common = {
        "scheduler": "pbs",
        "scheduler_dialect": "torque",
        "scheduler_driver": "driver",
        "scratch_root": "/cluster/scratch",
    }
    cfg = config.Config(
        default_host="driver",
        hosts={
            "driver": config.HostConfig(ssh="localhost"),
            "host_f": config.HostConfig(ssh="host_f.invalid", **scheduler_common),
            "host_c": config.HostConfig(
                ssh="host_c.invalid", **scheduler_common
            ),
        },
    )
    _install_config(monkeypatch, cfg, local_hosts=("driver",))
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda token: token == "fanout-secret",
    )
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    rendezvous = Barrier(3)
    observed: list[tuple[str | None, str | None]] = []

    def record_write(
        _state: drain.DrainState,
        *,
        token: str | None = None,
        multi_user: bool | None = None,
    ) -> None:
        assert multi_user is True
        rendezvous.wait(timeout=5)
        observed.append((token, os.environ.get("VQ_TOKEN")))

    monkeypatch.setattr(drain, "write_drain_state", record_write)

    result = CliRunner().invoke(
        main,
        ["drain", "--all", "--token-stdin"],
        input="fanout-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert sorted(observed) == [("fanout-secret", None)] * 3
    assert "VQ_TOKEN" not in os.environ


def test_scheduler_admin_drain_receives_resolved_stdin_token(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    cfg = config.Config(
        hosts={
            "localhost": config.HostConfig(ssh="localhost"),
            "host_f": config.HostConfig(
                ssh="host_f.invalid",
                scheduler="pbs",
                scheduler_dialect="torque",
                scheduler_driver="localhost",
                fleet_role="managed",
                scheduler_update_command="true",
                scratch_root="/cluster/scratch",
            ),
        },
    )
    _install_config(monkeypatch, cfg)
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda token: token == "scheduler-secret",
    )
    monkeypatch.setattr(
        cli_module,
        "_classify_admin_update_target",
        lambda *_args, **_kwargs: (None, "host_f"),
    )
    observed: list[str | None] = []

    def fake_update_scheduler_host(
        _host: str,
        _cfg: config.Config,
        **kwargs: object,
    ) -> SimpleNamespace:
        observed.append(kwargs.get("admin_token"))  # type: ignore[arg-type]
        return SimpleNamespace(success=True, command_output="")

    monkeypatch.setattr(
        cli_module.admin_module,
        "update_scheduler_host",
        fake_update_scheduler_host,
    )
    monkeypatch.setattr(
        cli_module.admin_module,
        "format_scheduler_update_result",
        lambda _result: "scheduler update ok",
    )

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "host_f",
            "--drain-wait",
            "1s",
            "--token-stdin",
        ],
        input="scheduler-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert observed == ["scheduler-secret"]


def test_local_system_multi_user_mutation_rejects_missing_token(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(auth, "verify_admin_token", lambda _token: False)
    monkeypatch.setattr(
        drain,
        "write_drain_state",
        lambda _state: pytest.fail("unauthenticated drain mutation ran"),
    )

    result = CliRunner().invoke(main, ["drain", "localhost"])

    assert result.exit_code != 0
    assert "token required in multi-user mode" in result.output


def test_all_uses_each_remote_hosts_auth_policy(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    cfg = config.Config(
        hosts={
            "alpha": config.HostConfig(ssh="alpha.invalid"),
            "beta": config.HostConfig(
                ssh="beta.invalid",
                admin_token_file="/etc/vq/admin-token",
            ),
        },
    )
    _install_config(monkeypatch, cfg, local_hosts=())
    monkeypatch.setenv("VQ_TOKEN", "shared-secret")
    calls = _capture_remote(monkeypatch)

    result = CliRunner().invoke(main, ["drain", "--all", "--release"])

    assert result.exit_code == 0, result.output
    by_ssh = {str(call["ssh"]): call for call in calls}
    assert by_ssh["alpha.invalid"] == {
        "ssh": "alpha.invalid",
        "argv": ["drain", "--release", "--token-stdin", "localhost"],
        "stdin_data": "shared-secret\n",
    }
    assert by_ssh["beta.invalid"] == {
        "ssh": "beta.invalid",
        "argv": [
            "drain",
            "--release",
            "--token-file",
            "/etc/vq/admin-token",
            "localhost",
        ],
        "stdin_data": None,
    }


@pytest.mark.parametrize("release", (False, True))
def test_scheduler_owner_mutation_is_authenticated_on_remote_driver(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    release: bool,
) -> None:
    _ = isolated_state
    cfg = config.Config(
        hosts={
            "driver": config.HostConfig(ssh="driver.invalid"),
            "host_f": config.HostConfig(
                ssh="host_f.invalid",
                scheduler="pbs",
                scheduler_dialect="torque",
                scheduler_driver="driver",
                scratch_root="/cluster/scratch",
            ),
        },
    )
    _install_config(monkeypatch, cfg, local_hosts=())
    calls = _capture_remote(monkeypatch)
    args = [
        "drain",
        "--scheduler-host",
        "host_f",
        "--lease-owner",
        "rollout:test",
        "--token-stdin",
        "host_f",
    ]
    if release:
        args.insert(1, "--release")

    result = CliRunner().invoke(main, args, input="driver-secret\n")

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    call = calls[0]
    assert call["ssh"] == "driver.invalid"
    assert call["stdin_data"] == "driver-secret\n"
    assert call["argv"][-2:] == ["--token-stdin", "localhost"]
    assert "--scheduler-host" in call["argv"]
    assert "--lease-owner" in call["argv"]
    assert ("--release" in call["argv"]) is release
    assert all("driver-secret" not in arg for arg in call["argv"])


def test_bare_release_clears_leases_before_legacy_and_preserves_legacy_on_failure(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_config(monkeypatch, _local_config())
    drain.write_drain_state(
        drain.DrainState(enabled=True, reason="maintenance"),
        via_rpc=False,
    )
    drain.acquire_scheduler_drain_lease(
        "host_f",
        owner="operator",
        lease_id="lease-host_f",
        via_rpc=False,
    )
    original_clear_leases = drain.clear_scheduler_drain_leases

    def clear_leases_direct(**_kwargs: object) -> bool:
        return original_clear_leases(via_rpc=False)

    def fail_legacy_clear(**_kwargs: object) -> bool:
        raise drain.SchedulerDrainCapabilityError("legacy RPC unavailable")

    monkeypatch.setattr(drain, "clear_scheduler_drain_leases", clear_leases_direct)
    monkeypatch.setattr(drain, "clear_drain", fail_legacy_clear)

    result = CliRunner().invoke(main, ["drain", "--release", "localhost"])

    assert result.exit_code != 0
    assert "scheduler lease release completed" in result.output
    assert drain.read_scheduler_drain_leases(via_rpc=False) == []
    legacy = drain.read_drain_state(via_rpc=False)
    assert legacy is not None
    assert legacy.reason == "maintenance"


def test_bare_release_capability_failure_does_not_clear_legacy_with_sidecar(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_config(monkeypatch, _local_config())
    drain.acquire_scheduler_drain_lease(
        "host_f",
        owner="operator",
        lease_id="lease-host_f",
        via_rpc=False,
    )
    legacy_clear_called = False

    def fail_lease_clear(**_kwargs: object) -> bool:
        raise drain.SchedulerDrainCapabilityError("old daemon")

    def record_legacy_clear(**_kwargs: object) -> bool:
        nonlocal legacy_clear_called
        legacy_clear_called = True
        return True

    monkeypatch.setattr(drain, "clear_scheduler_drain_leases", fail_lease_clear)
    monkeypatch.setattr(drain, "clear_drain", record_legacy_clear)

    result = CliRunner().invoke(main, ["drain", "--release", "localhost"])

    assert result.exit_code != 0
    assert "old daemon" in result.output
    assert legacy_clear_called is False


def test_system_multi_user_bare_release_probes_system_sidecar_fail_closed(
    isolated_state: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    system_root = tmp_path / "system-state"
    system_root.mkdir()
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_root))
    monkeypatch.setattr(paths, "is_multi_user", lambda: False)
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "verify_admin_token",
        lambda token: token == "system-secret",
    )
    (system_root / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
        '{"schema_version":1,"leases":[{'
        '"lease_id":"system-lease","scheduler_host":"host_f",'
        '"owner":"operator"}]}'
    )
    legacy_clear_called = False

    def fail_lease_clear(**_kwargs: object) -> bool:
        raise drain.SchedulerDrainCapabilityError("system daemon unavailable")

    def record_legacy_clear(**_kwargs: object) -> bool:
        nonlocal legacy_clear_called
        legacy_clear_called = True
        return True

    monkeypatch.setattr(drain, "clear_scheduler_drain_leases", fail_lease_clear)
    monkeypatch.setattr(drain, "clear_drain", record_legacy_clear)

    result = CliRunner().invoke(
        main,
        ["drain", "--release", "--token-stdin", "localhost"],
        input="system-secret\n",
    )

    assert result.exit_code != 0
    assert "system daemon unavailable" in result.output
    assert legacy_clear_called is False


def test_bare_release_corrupt_sidecar_is_a_clean_fail_closed_error(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_config(monkeypatch, _local_config())
    (isolated_state / drain.SCHEDULER_DRAIN_LEASES_FILENAME).write_text(
        "not-json"
    )
    monkeypatch.setattr(
        drain,
        "clear_scheduler_drain_leases",
        lambda **_kwargs: (_ for _ in ()).throw(
            drain.SchedulerDrainCapabilityError("daemon unavailable")
        ),
    )

    result = CliRunner().invoke(main, ["drain", "--release", "localhost"])

    assert result.exit_code != 0
    assert isinstance(result.exception, SystemExit)
    assert "scheduler drain lease store" in result.output


def test_bare_release_swallows_capability_failure_only_for_empty_sidecar(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    legacy_clear_called = False

    def fail_lease_clear(**_kwargs: object) -> bool:
        raise drain.SchedulerDrainCapabilityError("old daemon")

    def record_legacy_clear(**_kwargs: object) -> bool:
        nonlocal legacy_clear_called
        legacy_clear_called = True
        return True

    monkeypatch.setattr(drain, "clear_scheduler_drain_leases", fail_lease_clear)
    monkeypatch.setattr(drain, "clear_drain", record_legacy_clear)

    result = CliRunner().invoke(main, ["drain", "--release", "localhost"])

    assert result.exit_code == 0, result.output
    assert legacy_clear_called is True
    assert "drain released" in result.output


def test_multi_scheduler_acquire_rolls_back_response_loss_and_prior_changes(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    events: list[str] = []
    ids: Iterator[str] = iter(("lease-alpha", "lease-beta"))
    active: set[str] = set()

    def next_id(_nbytes: int) -> str:
        lease_id = next(ids)
        events.append(f"generate:{lease_id}")
        return lease_id

    def acquire(
        scheduler_host: str,
        *,
        owner: str,
        reason: str | None,
        lease_id: str,
        **_kwargs: object,
    ) -> tuple[SimpleNamespace, bool]:
        _ = owner, reason
        events.append(f"acquire:{scheduler_host}:{lease_id}")
        active.add(lease_id)
        if scheduler_host == "beta":
            raise drain.SchedulerDrainCapabilityError("response lost")
        return SimpleNamespace(lease_id=lease_id), True

    def release_exact(lease_id: str, **_kwargs: object) -> bool:
        events.append(f"release:{lease_id}")
        active.discard(lease_id)
        return True

    monkeypatch.setattr(cli_module.secrets, "token_hex", next_id)
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
    monkeypatch.setattr(drain, "release_scheduler_drain_lease", release_exact)

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "beta",
            "localhost",
        ],
    )

    assert result.exit_code != 0
    assert "acquisition failed for beta" in result.output
    assert events == [
        "generate:lease-alpha",
        "generate:lease-beta",
        "acquire:alpha:lease-alpha",
        "acquire:beta:lease-beta",
        "release:lease-beta",
        "release:lease-alpha",
    ]
    assert active == set()


def test_multi_scheduler_transaction_pins_system_scope_through_rollback(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    scope_reads = 0
    ids: Iterator[str] = iter(("lease-alpha", "lease-beta"))
    events: list[tuple[str, str, bool | None]] = []

    def resolve_scope(_cfg: config.Config) -> bool:
        nonlocal scope_reads
        scope_reads += 1
        if scope_reads > 1:
            raise AssertionError("drain command re-resolved its daemon scope")
        return True

    def acquire(
        scheduler_host: str,
        *,
        lease_id: str,
        multi_user: bool | None = None,
        **_kwargs: object,
    ) -> tuple[SimpleNamespace, bool]:
        events.append(("acquire", lease_id, multi_user))
        if scheduler_host == "beta":
            raise drain.SchedulerDrainCapabilityError("response lost")
        return SimpleNamespace(lease_id=lease_id), True

    def release(
        lease_id: str,
        *,
        multi_user: bool | None = None,
        **_kwargs: object,
    ) -> bool:
        events.append(("release", lease_id, multi_user))
        return True

    monkeypatch.setattr(cli_module, "_multi_user_active", resolve_scope)
    monkeypatch.setattr(auth, "verify_admin_token", lambda token: bool(token))
    monkeypatch.setattr(
        cli_module.secrets,
        "token_hex",
        lambda _nbytes: next(ids),
    )
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
    monkeypatch.setattr(drain, "release_scheduler_drain_lease", release)

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "beta",
            "--token-stdin",
            "localhost",
        ],
        input="system-secret\n",
    )

    assert result.exit_code != 0
    assert scope_reads == 1
    assert events == [
        ("acquire", "lease-alpha", True),
        ("acquire", "lease-beta", True),
        ("release", "lease-beta", True),
        ("release", "lease-alpha", True),
    ]


def test_multi_scheduler_acquire_rollback_preserves_preexisting_claim(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    ids: Iterator[str] = iter(("attempt-alpha", "attempt-beta"))
    released: list[str] = []

    def acquire(
        scheduler_host: str,
        *,
        owner: str,
        reason: str | None,
        lease_id: str,
        **_kwargs: object,
    ) -> tuple[SimpleNamespace, bool]:
        _ = owner, reason
        if scheduler_host == "alpha":
            return SimpleNamespace(lease_id="preexisting-alpha"), False
        raise drain.SchedulerDrainCapabilityError("response lost")

    monkeypatch.setattr(
        cli_module.secrets,
        "token_hex",
        lambda _nbytes: next(ids),
    )
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
    monkeypatch.setattr(
        drain,
        "release_scheduler_drain_lease",
        lambda lease_id, **_kwargs: released.append(lease_id) or True,
    )

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "beta",
            "localhost",
        ],
    )

    assert result.exit_code != 0
    assert released == ["attempt-beta"]
    assert "preexisting-alpha" not in released


def test_multi_scheduler_validation_failure_rolls_back_prior_claim(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    original_acquire = drain.acquire_scheduler_drain_lease
    original_release = drain.release_scheduler_drain_lease
    released: list[str] = []

    def acquire_direct(
        scheduler_host: str,
        **kwargs: object,
    ) -> tuple[drain.SchedulerDrainLease, bool]:
        return original_acquire(scheduler_host, via_rpc=False, **kwargs)

    def release_direct(lease_id: str, **kwargs: object) -> bool:
        released.append(lease_id)
        return original_release(lease_id, via_rpc=False, **kwargs)

    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire_direct)
    monkeypatch.setattr(drain, "release_scheduler_drain_lease", release_direct)

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "x" * 256,
            "localhost",
        ],
    )

    assert result.exit_code != 0
    assert "scheduler drain acquisition failed" in result.output
    assert len(released) == 2
    assert drain.read_scheduler_drain_leases(via_rpc=False) == []


def test_multi_scheduler_interrupt_rolls_back_current_and_prior_claims(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())
    ids: Iterator[str] = iter(("lease-alpha", "lease-beta"))
    active: set[str] = set()
    released: list[str] = []

    def acquire(
        scheduler_host: str,
        *,
        owner: str,
        reason: str | None,
        lease_id: str,
        **_kwargs: object,
    ) -> tuple[SimpleNamespace, bool]:
        _ = owner, reason
        active.add(lease_id)
        if scheduler_host == "beta":
            raise KeyboardInterrupt("response lost after commit")
        return SimpleNamespace(lease_id=lease_id), True

    def release(lease_id: str, **_kwargs: object) -> bool:
        released.append(lease_id)
        active.discard(lease_id)
        return True

    monkeypatch.setattr(
        cli_module.secrets,
        "token_hex",
        lambda _nbytes: next(ids),
    )
    monkeypatch.setattr(drain, "read_drain_state", lambda **_kwargs: None)
    monkeypatch.setattr(drain, "acquire_scheduler_drain_lease", acquire)
    monkeypatch.setattr(drain, "release_scheduler_drain_lease", release)

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "beta",
            "localhost",
        ],
    )

    assert result.exit_code != 0
    assert released == ["lease-beta", "lease-alpha"]
    assert active == set()


def test_multi_scheduler_release_reports_earlier_partial_release(
    isolated_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = isolated_state
    _install_config(monkeypatch, _local_config())

    def release_for_owner(
        scheduler_host: str,
        *,
        owner: str,
        **_kwargs: object,
    ) -> bool:
        _ = owner
        if scheduler_host == "beta":
            raise drain.SchedulerDrainCapabilityError("release RPC lost")
        return True

    monkeypatch.setattr(
        drain,
        "release_scheduler_drain_leases",
        release_for_owner,
    )

    result = CliRunner().invoke(
        main,
        [
            "drain",
            "--release",
            "--scheduler-host",
            "alpha",
            "--scheduler-host",
            "beta",
            "--lease-owner",
            "rollout:test",
            "localhost",
        ],
    )

    assert result.exit_code != 0
    assert "release failed for beta" in result.output
    assert "already released for alpha" in result.output

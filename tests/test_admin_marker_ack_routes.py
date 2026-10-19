"""Failed-marker recovery follows the selected update, including delegation."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from tests import test_admin_outcomes
from tests.test_scheduler_admin_update import _write_scheduler_config
from vq import admin, cli

SHA = "a" * 40


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    return test_admin_outcomes.host.__wrapped__(tmp_path, monkeypatch)


@pytest.fixture(autouse=True)
def helper_source(host: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(admin, "_scheduler_helper_project_root", lambda: host / "repo")


def _scope(surface: str) -> tuple[list[str], str]:
    if surface == "runtime":
        return ["scheduler-runtime:host_f:viewer"], "host_f"
    if surface == "helper":
        return ["scheduler:host_f"], "host_f"
    return ["demo"], "localhost"


def _args(surface: str) -> list[str]:
    if surface == "runtime":
        return ["viewer", "host_f", "--expected-sha", SHA]
    if surface == "helper":
        return ["host_f"]
    return ["--all"] if surface == "batch" else ["demo"]


def _configure(host: Path, surface: str, driver: str = "localhost") -> None:
    if surface in {"helper", "runtime"}:
        _write_scheduler_config(
            host / "cfg",
            driver=driver,
            runtime_program="viewer" if surface == "runtime" else None,
        )


@pytest.mark.parametrize("surface", ["runtime", "helper", "batch", "env"])
@pytest.mark.parametrize("flag", [True, False])
def test_acknowledges_exact_failed_scope_before_update(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    flag: bool,
) -> None:
    _configure(host, surface)
    envs, target = _scope(surface)
    marker = admin.acquire_admin_update_marker(envs=envs, host=target)
    admin.transition_admin_update_state(admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="failed")
    monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
    other = admin.acquire_admin_update_marker(envs=["unrelated"], host="other-host")
    reached = []

    def build_boundary(*args, **kwargs):
        assert admin.read_admin_update_markers() == [other]
        reached.append(True)
        raise admin.AdminPreconditionFailed("acceptance reached the normal update boundary")

    name = {
        "runtime": "_update_scheduler_runtime_guarded",
        "helper": "_update_scheduler_host_guarded",
        "batch": "_update_all_owned",
        "env": "_update_env_owned",
    }[surface]
    monkeypatch.setattr(admin, name, build_boundary)
    if surface in {"env", "batch"} and not flag:
        # These boundaries normally own their marker check themselves.
        def guarded_boundary(*args, **kwargs):
            admin._guard_admin_update_marker(force=False, envs=envs, host=target)
            return build_boundary(*args, **kwargs)

        monkeypatch.setattr(admin, name, guarded_boundary)
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            *_args(surface),
            "--json",
            *(["--acknowledge-failed-marker"] if flag else []),
        ],
    )
    assert result.exit_code == (77 if flag else 76), result.output
    assert reached == ([True] if flag else [])
    remaining = admin.read_admin_update_markers()
    assert other in remaining
    assert (marker.marker_id in [item.marker_id for item in remaining]) is not flag


@pytest.mark.parametrize("surface", ["runtime", "helper", "batch", "env"])
@pytest.mark.parametrize("kind", ["live", "stale", "managed", "pause", "failed-live", "unknown"])
def test_acknowledgement_preserves_unsafe_markers(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    kind: str,
) -> None:
    _configure(host, surface)
    envs, target = _scope(surface)
    marker = admin.acquire_admin_update_marker(envs=envs, host=target)
    if kind not in {"live", "stale"}:
        marker.state = admin.ADMIN_UPDATE_STATE_FAILED
    if kind == "managed":
        marker.managed_transaction = {"schema": "protected-transaction"}
    if kind == "pause":
        marker.pause_token = "admin-update-protected-scope"
    admin._write_admin_update_marker_atomic(marker)
    monkeypatch.setattr(
        admin,
        "_pid_liveness",
        lambda pid: None if kind == "unknown" else kind in {"live", "failed-live"},
    )
    monkeypatch.setattr(admin, "_pid_start_time", lambda pid: marker.pid_start_time or None)
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            *_args(surface),
            "--json",
            "--acknowledge-failed-marker",
        ],
    )
    assert result.exit_code == 77, result.output
    assert admin.read_admin_update_markers() == [marker]


@pytest.mark.parametrize("surface", ["runtime", "helper"])
@pytest.mark.parametrize("flag", [True, False])
def test_scheduler_delegation_preserves_acknowledgement(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    flag: bool,
) -> None:
    _configure(host, surface, driver="driver")
    forwarded = []
    monkeypatch.setattr(cli, "is_local_host", lambda value: value == "localhost")
    monkeypatch.setattr(
        cli,
        "_forward_admin_command",
        lambda h, c, args, **kw: forwarded.append((h, args)) or "{}\n",
    )
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            *_args(surface),
            "--json",
            *(["--acknowledge-failed-marker"] if flag else []),
        ],
    )
    assert result.exit_code == 0, result.output
    assert forwarded[0][0] == "driver"
    assert forwarded[0][1].count("--acknowledge-failed-marker") == int(flag)
    assert "--force" not in forwarded[0][1]


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("fanout", [True, False])
def test_remote_and_host_fanout_forward_acknowledgement(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    batch: bool,
    fanout: bool,
) -> None:
    with (host / "cfg/config.toml").open("a") as stream:
        stream.write('\n[hosts.remote]\nssh = "remote"\n')
    forwarded = []
    local = []
    monkeypatch.setattr(cli, "is_local_host", lambda value: value == "localhost")
    monkeypatch.setattr(
        cli, "_forward_venv_admin_update", lambda h, c, args, **kw: forwarded.append(args) or "{}\n"
    )

    def local_boundary(*args, **kwargs):
        local.append(kwargs["acknowledge_failed_marker"])
        raise admin.AdminPreconditionFailed("local route observed")

    monkeypatch.setattr(admin, "update_all" if batch else "update_env", local_boundary)
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            *(["--all"] if batch else ["demo"]),
            *(["--all-hosts"] if fanout else ["remote"]),
            "--acknowledge-failed-marker",
            "--json",
        ],
    )
    assert result.exit_code == (1 if fanout else 0), result.output
    assert local == ([True] if fanout else [])
    assert len(forwarded) == 1
    assert forwarded[0].count("--acknowledge-failed-marker") == 1
    assert "--force" not in forwarded[0]


@pytest.mark.parametrize("batch", [True, False])
def test_detached_child_retains_acknowledgement(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    batch: bool,
) -> None:
    captured = []
    monkeypatch.setattr(
        admin, "launch_detached_update", lambda **kw: captured.append(kw) or {"started": True}
    )
    args = [
        "admin",
        "update",
        *(["--all"] if batch else ["demo"]),
        "--acknowledge-failed-marker",
        "--json",
        "--detach",
    ]
    result = CliRunner().invoke(cli.main, args)
    assert result.exit_code == 0, result.output
    assert captured[0]["child_argv"].count("--acknowledge-failed-marker") == 1
    observed = []

    def invoke_child(run_id, run_update, **kwargs):
        run_update()
        return 0

    def local_boundary(*args, **kwargs):
        observed.append(kwargs["acknowledge_failed_marker"])
        raise admin.AdminPreconditionFailed("child route observed")

    monkeypatch.setattr(admin, "run_detached_update_child", invoke_child)
    monkeypatch.setattr(admin, "update_all" if batch else "update_env", local_boundary)
    result = CliRunner().invoke(cli.main, captured[0]["child_argv"][3:])
    assert result.exit_code == 77, result.output
    assert observed == [True]


def test_recycled_failed_writer_can_be_acknowledged(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
    marker.state = admin.ADMIN_UPDATE_STATE_FAILED
    marker.pid_start_time = 123
    admin._write_admin_update_marker_atomic(marker)
    monkeypatch.setattr(admin, "_pid_liveness", lambda pid: True)
    monkeypatch.setattr(admin, "_pid_start_time", lambda pid: 456)
    assert admin.acknowledge_failed_markers(["demo"], "localhost") == [marker.marker_id]
    assert admin.read_admin_update_markers() == []


@pytest.mark.parametrize("flag", [True, False])
def test_staged_driver_transports_acknowledgement(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
    flag: bool,
) -> None:
    from tests.test_admin_managed_durable_recovery import (
        _driver_update_runtime_config,
        _patch_driver_runtime_transport,
    )

    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(monkeypatch)
    result = admin.update_remote_managed_env_with_driver_runtime(
        cfg,
        "remote",
        env="vibeqc-queue",
        expected_sha=SHA,
        expected_tag=None,
        remote_auth_args=(),
        stdin_data=None,
        as_json=True,
        update_script_args=(),
        show_output=False,
        remote_timeout_env={},
        timeout=30,
        acknowledge_failed_marker=flag,
    )
    assert result == "updated\n"
    mutation = [call[1] for call in calls if call[1][0] == "/usr/bin/env"]
    assert len(mutation) == 1
    assert mutation[0].count("--acknowledge-failed-marker") == int(flag)
    assert "--force" not in mutation[0]
    assert mutation[0].count("--staged-driver-archive-sha256") == 1


def test_staged_driver_cli_retains_acknowledgement(
    host: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with (host / "cfg/config.toml").open("a") as stream:
        stream.write('\n[hosts.remote]\nssh = "remote"\n')
    observed = []
    monkeypatch.setattr(cli, "is_local_host", lambda value: value == "localhost")
    monkeypatch.setattr(
        admin,
        "update_remote_managed_env_with_driver_runtime",
        lambda *args, **kwargs: observed.append(kwargs) or "{}\n",
    )
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            "demo",
            "remote",
            "--expected-sha",
            SHA,
            "--with-driver-runtime",
            "--acknowledge-failed-marker",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert observed[0]["acknowledge_failed_marker"] is True


@pytest.mark.parametrize("surface", ["runtime", "helper", "batch", "env"])
def test_unreadable_marker_is_not_acknowledged(
    host: Path,
    surface: str,
) -> None:
    _configure(host, surface)
    path = admin.admin_update_marker_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("unreadable marker")
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            *_args(surface),
            "--json",
            "--acknowledge-failed-marker",
        ],
    )
    assert result.exit_code == 77, result.output
    assert path.read_text() == "unreadable marker"


def test_empty_batch_does_not_acknowledge_global_scope(host: Path) -> None:
    from vq import config

    marker = admin.acquire_admin_update_marker(envs=["demo"], host="localhost")
    with pytest.raises(admin.AdminError, match="no kind"):
        admin.update_all(config.Config(), host="localhost", acknowledge_failed_marker=True)
    assert admin.read_admin_update_markers() == [marker]

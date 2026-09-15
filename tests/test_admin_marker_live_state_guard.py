"""Pytest must never resolve persistent per-user vq state or config.

Issue #527 showed that a marker-specific refusal was insufficient: another
conftest-less test could resolve the live daemon socket and send synthetic
admin status by RPC. State/config resolution itself is therefore the safety
boundary, including collection before fixtures run.

These tests therefore do not rely on the conftest fixture: they set the
environment themselves, so they exercise the guard rather than the fixture.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from vq import admin, auth, config, paths, rpc
from vq.web import authn


def _clear_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the process in the state a conftest-less invocation would have."""
    monkeypatch.delenv(paths.ENV_STATE_DIR, raising=False)


def test_acquiring_the_marker_in_live_state_fails_loudly_under_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A test that would write into the real state root must fail, not pass."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / ".local" / "share"))
    _clear_isolation(monkeypatch)
    admin._set_owned_admin_update_marker_path(None)

    live_marker = paths.xdg_state_root() / admin.ADMIN_UPDATE_MARKER_FILENAME
    assert not live_marker.exists()

    with pytest.raises(paths.UnsafeImplicitTestPathError) as excinfo:
        admin.acquire_admin_update_marker(envs=["vibeqc-queue"], host="localhost")

    message = str(excinfo.value)
    assert "implicit per-user vq state root" in message
    assert paths.ENV_STATE_DIR in message
    assert "--noconftest" in message
    # The refusal must happen before the write, not after.
    assert not live_marker.exists()


def test_implicit_config_root_also_fails_loudly_under_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.delenv(paths.ENV_CONFIG_DIR, raising=False)

    for resolver in (
        paths.config_dir,
        config.config_dir,
        authn.users_path,
        authn.session_secret_path,
    ):
        with pytest.raises(paths.UnsafeImplicitTestPathError) as excinfo:
            resolver()
        assert "implicit per-user vq config root" in str(excinfo.value)
        assert paths.ENV_CONFIG_DIR in str(excinfo.value)
    with pytest.raises(paths.UnsafeImplicitTestPathError):
        authn.add_user("synthetic", "not-a-live-password", "viewer")
    assert not (tmp_path / ".config" / "vq").exists()


def test_web_token_refuses_before_probing_live_system_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.delenv(config.ENV_CONFIG_DIR, raising=False)

    def unexpected_probe() -> bool:
        raise AssertionError("live system config probe must not run")

    monkeypatch.setattr(config, "system_multi_user_enabled", unexpected_probe)
    with pytest.raises(paths.UnsafeImplicitTestPathError):
        auth.web_token_path()


def test_system_config_readers_refuse_paths_outside_sandbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    outside = tmp_path / "outside"
    sandbox.mkdir()
    outside.mkdir()
    system_config = outside / "config.toml"
    system_config.write_text("[multi_user]\nenabled = true\n", encoding="utf-8")
    monkeypatch.setenv(paths.ENV_TEST_SANDBOX_ROOT, str(sandbox))
    monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_config)

    for reader in (config.system_multi_user_enabled, config.load_system_config):
        with pytest.raises(
            paths.UnsafeImplicitTestPathError,
            match="outside the declared sandbox",
        ):
            reader()


def test_user_socket_fallback_refuses_implicit_xdg_under_pytest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The multi-user special case cannot bypass the state-root guard."""
    multi_user_root = tmp_path / "multi-user"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(multi_user_root))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_user_root))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)

    with pytest.raises(paths.UnsafeImplicitTestPathError) as excinfo:
        rpc.user_socket_path()

    assert "implicit per-user XDG state root" in str(excinfo.value)
    assert "XDG_DATA_HOME" in str(excinfo.value)


def test_explicit_persistent_roots_outside_sandbox_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    outside = tmp_path / "outside"
    sandbox.mkdir()
    outside.mkdir()
    monkeypatch.setenv(paths.ENV_TEST_SANDBOX_ROOT, str(sandbox))

    cases = (
        (paths.ENV_STATE_DIR, paths.state_root),
        (paths.ENV_CONFIG_DIR, paths.config_dir),
        (paths.ENV_CONFIG_DIR, config.config_dir),
        (paths.ENV_MULTI_USER_ROOT, paths.multi_user_root),
        (paths.ENV_ARCHIVE_DIR, paths.archive_dir),
        ("XDG_DATA_HOME", paths.xdg_state_root),
        (auth.ENV_WEB_TOKEN_FILE, auth.web_token_path),
        (
            config.ENV_TEST_SYSTEM_CONFIG_FILE,
            config._guarded_system_config_path,
        ),
    )
    for env_name, resolver in cases:
        monkeypatch.setenv(env_name, str(outside))
        with pytest.raises(
            paths.UnsafeImplicitTestPathError,
            match="outside the declared sandbox",
        ):
            resolver()
        monkeypatch.delenv(env_name, raising=False)


def test_symlink_alias_cannot_escape_declared_sandbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    outside = tmp_path / "outside"
    sandbox.mkdir()
    outside.mkdir()
    alias = sandbox / "looks-contained"
    alias.symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv(paths.ENV_TEST_SANDBOX_ROOT, str(sandbox))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(alias))

    with pytest.raises(
        paths.UnsafeImplicitTestPathError,
        match="outside the declared sandbox",
    ):
        paths.state_root()


def test_rpc_socket_override_cannot_escape_declared_sandbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    outside = tmp_path / "outside"
    sandbox.mkdir()
    outside.mkdir()
    socket_path = outside / "live-daemon.sock"
    socket_path.touch()
    monkeypatch.setenv(paths.ENV_TEST_SANDBOX_ROOT, str(sandbox))

    with pytest.raises(
        paths.UnsafeImplicitTestPathError,
        match="outside the declared sandbox",
    ):
        rpc.call("set_admin_status", socket_override=socket_path)

    alias = sandbox / "looks-contained.sock"
    alias.symlink_to(socket_path)
    with pytest.raises(
        paths.UnsafeImplicitTestPathError,
        match="outside the declared sandbox",
    ):
        rpc.call("set_admin_status", socket_override=alias)


def test_tilde_overrides_use_the_validated_expanded_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sandbox = tmp_path / "sandbox"
    home = sandbox / "home"
    home.mkdir(parents=True)
    monkeypatch.setenv(paths.ENV_TEST_SANDBOX_ROOT, str(sandbox))
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv(paths.ENV_CONFIG_DIR, "~/config")
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, "~/multi-user")
    monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, "~/token")

    assert paths.config_dir() == home / "config"
    assert config.config_dir() == home / "config"
    assert paths.multi_user_root() == home / "multi-user"
    assert auth.web_token_path() == home / "token"
    with pytest.raises(ConnectionError, match="missing.sock") as excinfo:
        rpc.call("ping", socket_override=Path("~/missing.sock"))
    assert str(home / "missing.sock") in str(excinfo.value)


def test_isolated_state_root_still_acquires_normally(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The guard must not disturb a properly isolated test."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "home" / ".local" / "share"))
    state_dir = tmp_path / "vq-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_dir))
    admin._set_owned_admin_update_marker_path(None)

    marker = admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"], host="localhost",
    )

    assert marker.envs == ["vibeqc-queue"]
    assert (state_dir / admin.ADMIN_UPDATE_MARKER_FILENAME).is_file()


def test_multi_user_root_is_not_treated_as_live_per_user_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An explicit root contained by the declared test sandbox stays allowed."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "home" / ".local" / "share"))
    explicit_root = tmp_path / "var-lib-vq"
    explicit_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(explicit_root))
    admin._set_owned_admin_update_marker_path(None)

    admin.acquire_admin_update_marker(envs=["vibeqc-queue"], host="localhost")

    assert (explicit_root / admin.ADMIN_UPDATE_MARKER_FILENAME).is_file()


def test_guard_is_inert_outside_pytest(
    tmp_path: Path,
) -> None:
    """A real process keeps the documented implicit XDG behavior."""
    home = tmp_path / "home"
    xdg_data = home / ".local" / "share"
    home.mkdir()
    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
        "PYTEST_ADDOPTS",
    ):
        env.pop(name, None)
    env["HOME"] = str(home)
    env["XDG_DATA_HOME"] = str(xdg_data)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    env[config.ENV_TEST_SYSTEM_CONFIG_FILE] = str(
        tmp_path / "pytest-only-system-config.toml"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from vq import admin, config; "
            "print(config._guarded_system_config_path()); "
            "admin.acquire_admin_update_marker("
            "envs=['vibeqc-queue'], host='localhost')",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.stdout.strip() == "/etc/vq/config.toml"
    assert (xdg_data / "vq" / admin.ADMIN_UPDATE_MARKER_FILENAME).is_file()

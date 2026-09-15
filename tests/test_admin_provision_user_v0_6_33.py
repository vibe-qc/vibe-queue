"""v0.6.33: `vq admin provision-user` — non-admin user bootstrap.

On a multi-user host ``/var/lib/vq/users/`` is root-owned; an
unprivileged user cannot create their own ``<uid>/`` subtree, so
their first ``vq submit`` fails with PermissionError. The daemon
auto-provisions a state dir for every ``admin_group`` member at
startup (v0.6.27); this verb provisions anyone else — run once, as
root.
"""
from __future__ import annotations

import os
import pwd
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cli, config, paths
from vq.cli import main


class _OSProxy:
    """Override CLI privilege detection without mutating the shared os module."""

    def __init__(self, euid: int) -> None:
        self._euid = euid

    def geteuid(self) -> int:
        return self._euid

    def __getattr__(self, name: str) -> object:
        return getattr(os, name)


@pytest.fixture
def mu_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user state root + an empty config dir, both under tmp."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


class TestProvisionUser:
    def test_single_user_mode_rejected(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: False)
        result = CliRunner().invoke(main, ["admin", "provision-user", "1000"])
        assert result.exit_code != 0
        assert "multi-user host" in result.output

    def test_not_root_rejected(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: True)
        monkeypatch.setattr(cli, "os", _OSProxy(1000))
        result = CliRunner().invoke(main, ["admin", "provision-user", "1000"])
        assert result.exit_code != 0
        assert "must run as root" in result.output

    def test_unknown_user_rejected(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: True)
        monkeypatch.setattr(cli, "os", _OSProxy(0))
        result = CliRunner().invoke(
            main, ["admin", "provision-user", "4000123"]
        )
        assert result.exit_code != 0
        assert "no such user" in result.output

    def test_provisions_the_state_tree_by_uid(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: True)
        monkeypatch.setattr(cli, "os", _OSProxy(0))
        # Provision the CURRENT user — the chown is then chown-to-self
        # and succeeds without real root.
        uid = os.getuid()
        result = CliRunner().invoke(
            main, ["admin", "provision-user", str(uid)]
        )
        assert result.exit_code == 0, result.output
        assert paths.user_dir(uid).is_dir()
        assert paths.user_queue_dir(uid).is_dir()
        assert paths.user_jobs_dir(uid).is_dir()
        assert paths.user_archive_dir(uid).is_dir()
        assert pwd.getpwuid(uid).pw_name in result.output

    def test_provisions_by_username(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: True)
        monkeypatch.setattr(cli, "os", _OSProxy(0))
        name = pwd.getpwuid(os.getuid()).pw_name
        result = CliRunner().invoke(main, ["admin", "provision-user", name])
        assert result.exit_code == 0, result.output
        assert paths.user_dir(os.getuid()).is_dir()

    def test_idempotent(
        self, mu_env: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: True)
        monkeypatch.setattr(cli, "os", _OSProxy(0))
        uid = os.getuid()
        r1 = CliRunner().invoke(main, ["admin", "provision-user", str(uid)])
        assert r1.exit_code == 0, r1.output
        # Re-running on an already-provisioned user must not error.
        r2 = CliRunner().invoke(main, ["admin", "provision-user", str(uid)])
        assert r2.exit_code == 0, r2.output

    def test_verb_listed_in_admin_help(self) -> None:
        result = CliRunner().invoke(main, ["admin", "--help"])
        assert result.exit_code == 0
        assert "provision-user" in result.output

"""v0.6.32: web_token_path() is multi-user aware.

`sudo vq web init-token --force` run *without* `VQ_CONFIG_DIR`
resolved the token file to `/root/.config/vq/web-token` (root's
home) — a dead path the daemon never reads, since the daemon's unit
sets `VQ_CONFIG_DIR=/etc/vq`. A token "rotation" there silently
left the old (leaked) `/etc/vq/web-token` live. Hit for real while
rotating the host_a admin token.

`web_token_path()` now resolves to `/etc/vq/web-token` on a
multi-user host when no config dir is explicitly set, so the CLI
and the root daemon agree.

Precedence (highest first): `$VQ_WEB_TOKEN_FILE` > `$VQ_CONFIG_DIR`
> multi-user `/etc/vq/web-token` > `~/.config/vq/web-token`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from vq import auth, config, paths


def _system_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, body: str
) -> Path:
    """Write a fake /etc/vq/config.toml and point SYSTEM_CONFIG_PATH at it."""
    p = tmp_path / "system-config.toml"
    p.write_text(body)
    monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", p)
    return p


class TestWebTokenPath:
    def test_explicit_token_file_env_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """$VQ_WEB_TOKEN_FILE overrides everything, even on a
        multi-user host."""
        target = tmp_path / "custom-token"
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(target))
        _system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        assert auth.web_token_path() == target

    def test_explicit_config_dir_wins_over_multi_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An explicit $VQ_CONFIG_DIR is honored — the daemon's unit
        sets it, and operators may too."""
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        cfgdir = tmp_path / "cfg"
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        _system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        assert auth.web_token_path() == cfgdir / "web-token"

    def test_multi_user_host_resolves_to_etc_vq(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The headline fix: no explicit config dir + multi-user host
        → token sits next to the system config (i.e.
        /etc/vq/web-token in production) — where the daemon reads it.
        """
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.delenv(config.ENV_CONFIG_DIR, raising=False)
        monkeypatch.setattr(paths, "_running_under_pytest", lambda: False)
        sysconf = _system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        assert auth.web_token_path() == sysconf.parent / "web-token"

    def test_single_user_host_uses_config_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-user host (no system config) uses the per-user config dir."""
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        user_config = tmp_path / "user-config"
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(user_config))
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", tmp_path / "absent.toml"
        )
        assert auth.web_token_path() == user_config / "web-token"

    def test_system_config_present_but_single_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An /etc/vq/config.toml that exists but is NOT multi-user
        must not divert the token path."""
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        user_config = tmp_path / "user-config"
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(user_config))
        _system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = false\n"
        )
        assert auth.web_token_path() == user_config / "web-token"

"""v0.6.30: client-side multi-user autodetect.

On a multi-user host the root daemon reads ``/etc/vq/config.toml``,
but the CLI reads each user's ``~/.config/vq/config.toml``. Before
this, a user had to mirror ``[multi_user]`` into their personal
config or the client wrote job state where the daemon never looked
(``~/.local/share/vq/`` vs ``/var/lib/vq/users/<uid>/``).

Now the client ORs in a system-wide ``/etc/vq/config.toml`` check —
``config.system_multi_user_enabled()`` — so multi-user mode
auto-detects with no per-user config edit.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from vq import config
from vq.cli import _multi_user_active


def _write_system_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, body: str
) -> Path:
    """Write a fake /etc/vq/config.toml and point SYSTEM_CONFIG_PATH at it."""
    p = tmp_path / "system-config.toml"
    p.write_text(body)
    monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", p)
    return p


# ----------------------------------------------------------------------
# config.system_multi_user_enabled
# ----------------------------------------------------------------------


class TestSystemMultiUserEnabled:
    def test_missing_file_is_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", tmp_path / "absent.toml"
        )
        assert config.system_multi_user_enabled() is False

    def test_enabled_true(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        assert config.system_multi_user_enabled() is True

    def test_enabled_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = false\n"
        )
        assert config.system_multi_user_enabled() is False

    def test_no_multi_user_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_system_config(monkeypatch, tmp_path, 'default_host = "x"\n')
        assert config.system_multi_user_enabled() is False

    def test_malformed_toml_is_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A broken /etc/vq/config.toml must never break the CLI."""
        _write_system_config(monkeypatch, tmp_path, "this is [not valid toml")
        assert config.system_multi_user_enabled() is False


class TestLoadSystemConfig:
    def test_valid_config_is_fully_loaded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_system_config(
            monkeypatch,
            tmp_path,
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "system-vq-admins"\n',
        )

        cfg = config.load_system_config()

        assert cfg is not None
        assert cfg.multi_user.enabled is True
        assert cfg.multi_user.admin_group == "system-vq-admins"

    def test_unrelated_invalid_field_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_system_config(
            monkeypatch,
            tmp_path,
            'hosts = "not-a-table"\n'
            "[multi_user]\n"
            "enabled = true\n",
        )

        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_system_config()


# ----------------------------------------------------------------------
# cli._multi_user_active — the OR of user config + system config
# ----------------------------------------------------------------------


class TestMultiUserActive:
    def test_user_cfg_enabled_returns_true(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # No system config; the user's own config opts in.
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", tmp_path / "absent.toml"
        )
        cfg = config.Config()
        cfg.multi_user.enabled = True
        assert _multi_user_active(cfg) is True

    def test_system_config_overrides_single_user_cfg(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The headline fix: the user's config is single-user, but the
        host's /etc/vq/config.toml is multi-user → client follows the
        host."""
        _write_system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        cfg = config.Config()
        assert cfg.multi_user.enabled is False
        assert _multi_user_active(cfg) is True

    def test_both_single_user_returns_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            config, "SYSTEM_CONFIG_PATH", tmp_path / "absent.toml"
        )
        assert _multi_user_active(config.Config()) is False

    def test_none_cfg_falls_back_to_system(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cfg=None → load_config() from an empty config dir → no
        user opt-in, but the system config still flips it on."""
        _write_system_config(
            monkeypatch, tmp_path, "[multi_user]\nenabled = true\n"
        )
        empty = tmp_path / "empty-cfg"
        empty.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(empty))
        assert _multi_user_active(None) is True

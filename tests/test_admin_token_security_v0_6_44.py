"""v0.6.44 (SECURITY): a missing admin-token file rejects `vq admin update`.

Pre-v0.6.44 `auth.verify_admin_token()` returned True when no
token file existed, justified as a "single-user compat" path. But
the only caller — `vq admin update` — already gates that check on
``cfg.multi_user.enabled``, so the compat branch never fired in
single-user mode. In multi-user mode it instead **silently
bypassed the admin gate** on any host that lacked a token file
(e.g. before ``vq web init-token`` had run, or after a
misconfigured token-file wipe), letting anyone with shell access
on the host run admin verbs unauthenticated.

Fix: a missing token file is a hard failure. (The web API's
``require_token`` has always done this — it raises 503 when no
token is configured.)
"""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import auth, config, paths
from vq.cli import main


@pytest.fixture
def isolated_token_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Pin VQ_WEB_TOKEN_FILE to a nonexistent path so the host's
    real token file (e.g. ~/.config/vq/web-token on a fleet host)
    never influences the test. Also drop VQ_TOKEN."""
    monkeypatch.setenv(
        auth.ENV_WEB_TOKEN_FILE, str(tmp_path / "no-token-here")
    )
    monkeypatch.delenv("VQ_TOKEN", raising=False)
    return tmp_path


class TestVerifyAdminTokenSecurity:
    def test_missing_token_file_rejects_empty_string(
        self, isolated_token_env: Path
    ) -> None:
        """The CLI calls ``verify_admin_token(token or "")`` — i.e.
        with an empty string when no ``--token``/``$VQ_TOKEN`` is
        set. Pre-v0.6.44 this returned True (the bypass). Now:
        False."""
        assert auth.verify_admin_token("") is False

    def test_missing_token_file_rejects_any_token(
        self, isolated_token_env: Path
    ) -> None:
        """A claimed token cannot pass against a missing file."""
        assert auth.verify_admin_token("any-claimed-token") is False

    def test_existing_token_file_accepts_correct(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: the happy path still works."""
        tf = tmp_path / "web-token"
        tf.write_text("good-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        assert auth.verify_admin_token("good-token") is True
        assert auth.verify_admin_token("bad-token") is False


class TestAdminUpdateCLIRejectsWithoutToken:
    def test_multi_user_admin_update_blocked_when_token_file_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end: in multi-user mode with no token file
        configured, ``vq admin update`` must surface the
        token-required error — NOT silently proceed."""
        monkeypatch.setenv(
            auth.ENV_WEB_TOKEN_FILE, str(tmp_path / "no-token")
        )
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        cfgdir = tmp_path / "cfg"
        cfgdir.mkdir()
        (cfgdir / "config.toml").write_text(
            "[multi_user]\nenabled = true\n"
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))

        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "localhost"]
        )
        assert result.exit_code != 0
        # The error message tells the operator to run vq web
        # init-token; we just assert the gate fired.
        combined = (result.output or "") + (str(result.exception or ""))
        assert "token required" in combined.lower()

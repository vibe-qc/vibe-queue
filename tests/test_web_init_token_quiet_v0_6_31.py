"""v0.6.31: `vq web init-token --quiet` — credential-hygiene flag.

`vq web init-token` echoes the freshly generated bearer token to
stdout so an interactive operator can copy it. For SCRIPTED setup —
the multi-user deploy script — that echo lands the credential in
terminal scrollback, CI logs, and pasted deploy output. ``--quiet``
writes the 0600 token file without echoing the token; the operator
retrieves it later with ``sudo cat <token-file>``.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import auth, config
from vq.cli import main


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the config dir (and thus the web-token path) into tmp."""
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    return d


class TestInitTokenQuiet:
    def test_default_echoes_token(self, cfg_dir: Path) -> None:
        """Default (interactive) behaviour is unchanged — the token is
        printed so an operator can copy it."""
        result = CliRunner().invoke(main, ["web", "init-token"])
        assert result.exit_code == 0, result.output
        assert "Authorization: Bearer" in result.output
        assert (cfg_dir / "web-token").is_file()

    def test_quiet_does_not_echo_token(self, cfg_dir: Path) -> None:
        """--quiet: the token file is written, but the token value
        never appears in stdout."""
        result = CliRunner().invoke(main, ["web", "init-token", "--quiet"])
        assert result.exit_code == 0, result.output
        assert "Bearer" not in result.output
        token_file = cfg_dir / "web-token"
        assert token_file.is_file()
        token = token_file.read_text().strip()
        assert token  # a real token was generated
        # The critical property: the secret is not in the output.
        assert token not in result.output

    def test_quiet_still_reports_the_path(self, cfg_dir: Path) -> None:
        """--quiet must still tell the operator WHERE the token landed
        — just not what it is."""
        result = CliRunner().invoke(main, ["web", "init-token", "--quiet"])
        assert result.exit_code == 0
        assert "web-token" in result.output

    def test_quiet_with_force_rotates(self, cfg_dir: Path) -> None:
        """--quiet composes with --force: rotate the token without
        echoing the new one."""
        r1 = CliRunner().invoke(main, ["web", "init-token", "--quiet"])
        assert r1.exit_code == 0
        first = (cfg_dir / "web-token").read_text()
        r2 = CliRunner().invoke(
            main, ["web", "init-token", "--quiet", "--force"]
        )
        assert r2.exit_code == 0, r2.output
        second = (cfg_dir / "web-token").read_text()
        assert first != second  # genuinely rotated
        assert "Bearer" not in r2.output

    def test_quiet_flag_documented_in_help(self) -> None:
        result = CliRunner().invoke(main, ["web", "init-token", "--help"])
        assert result.exit_code == 0
        assert "--quiet" in result.output

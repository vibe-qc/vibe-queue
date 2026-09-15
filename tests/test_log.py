"""Tests for vq.log (v0.6.16 client-side logging additions).

Coverage shape:
* TestLevelResolution — VQ_LOG_LEVEL env-var parsing.
* TestSetupCliLogging — file creation, rotation, env-disabled,
  permission denied, idempotency.
* TestCLIInvocationLine — the per-invocation log line that
  cli.main writes on every command.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import log as vqlog
from vq import paths
from vq.cli import main


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    """Tests in this module install + tear down handlers on the
    root logger; make sure each test starts with a clean slate so
    one test's leftover handler doesn't pollute the next."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_vq_cli_log_marker", False):
            root.removeHandler(h)
            h.close()


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    # Test isolation: the tests in this file specifically want to
    # exercise the log facility; an outer test setting
    # VQ_LOG_DISABLED would break them.
    monkeypatch.delenv(vqlog.ENV_LOG_DISABLED, raising=False)
    monkeypatch.delenv(vqlog.ENV_LOG_LEVEL, raising=False)
    return tmp_path


class TestLevelResolution:
    def test_default_is_info(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(vqlog.ENV_LOG_LEVEL, raising=False)
        assert vqlog._resolve_cli_log_level() == logging.INFO

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("DEBUG", logging.DEBUG),
            ("debug", logging.DEBUG),
            ("Info", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ],
    )
    def test_valid_names_parse(
        self, value: str, expected: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(vqlog.ENV_LOG_LEVEL, value)
        assert vqlog._resolve_cli_log_level() == expected

    @pytest.mark.parametrize(
        "value", ["", "garbage", "INFO ", "VERBOSE", "42"],
    )
    def test_invalid_values_default_to_info(
        self, value: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(vqlog.ENV_LOG_LEVEL, value)
        # Trailing space and unknown names both fall through to
        # INFO. We don't try to be clever about typo correction.
        assert vqlog._resolve_cli_log_level() == logging.INFO


class TestSetupCliLogging:
    def test_creates_log_file(self, state_dir: Path) -> None:
        log_file = state_dir / "state" / "client.log"
        assert not log_file.exists()
        handler = vqlog.setup_cli_logging(log_file)
        assert handler is not None
        # The handler is registered on the root logger.
        root = logging.getLogger()
        assert handler in root.handlers
        # First log call materializes the file (RotatingFileHandler
        # opens lazily).
        logging.getLogger("vq.test").info("first line")
        assert log_file.exists()
        content = log_file.read_text()
        assert "first line" in content
        assert "[pid=" in content
        assert "vq.test" in content

    def test_disabled_env_var_skips_setup(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(vqlog.ENV_LOG_DISABLED, "1")
        log_file = state_dir / "state" / "client.log"
        handler = vqlog.setup_cli_logging(log_file)
        assert handler is None
        assert not log_file.exists()

    def test_disabled_env_var_other_value_does_not_skip(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the literal '1' opts out. '0' / 'false' / empty
        all leave logging on."""
        monkeypatch.setenv(vqlog.ENV_LOG_DISABLED, "0")
        log_file = state_dir / "state" / "client.log"
        handler = vqlog.setup_cli_logging(log_file)
        assert handler is not None

    def test_idempotent_repeat_calls(self, state_dir: Path) -> None:
        """Re-calling setup_cli_logging removes the prior handler
        before installing a new one — repeated CLI invocations
        in the same Python interpreter (web import path) must
        not pile up duplicate handlers."""
        log_file = state_dir / "state" / "client.log"
        h1 = vqlog.setup_cli_logging(log_file)
        h2 = vqlog.setup_cli_logging(log_file)
        assert h1 is not None
        assert h2 is not None
        assert h1 is not h2
        # Only one CLI-marked handler on root.
        root = logging.getLogger()
        cli_handlers = [
            h for h in root.handlers
            if getattr(h, "_vq_cli_log_marker", False)
        ]
        assert len(cli_handlers) == 1
        assert cli_handlers[0] is h2

    def test_writes_to_log_at_default_info_level(
        self, state_dir: Path
    ) -> None:
        log_file = state_dir / "state" / "client.log"
        vqlog.setup_cli_logging(log_file)
        logging.getLogger("vq.test").debug("debug not shown")
        logging.getLogger("vq.test").info("info shown")
        logging.getLogger("vq.test").warning("warning shown")
        content = log_file.read_text()
        assert "info shown" in content
        assert "warning shown" in content
        assert "debug not shown" not in content

    def test_debug_level_shows_debug_lines(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(vqlog.ENV_LOG_LEVEL, "DEBUG")
        log_file = state_dir / "state" / "client.log"
        vqlog.setup_cli_logging(log_file)
        logging.getLogger("vq.test").debug("debug shown")
        content = log_file.read_text()
        assert "debug shown" in content

    def test_unwriteable_path_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the file open fails (e.g., parent dir can't be
        created) we return None rather than raising — a CLI
        invocation must not die because the log is unwriteable."""
        # Point at a path under a regular file so mkdir(parents=True)
        # fails. tmp_path/blocker is a file; we ask for a log inside it.
        blocker = tmp_path / "blocker"
        blocker.write_text("")
        bad_log = blocker / "client.log"
        handler = vqlog.setup_cli_logging(bad_log)
        assert handler is None

    def test_pid_appears_in_format(self, state_dir: Path) -> None:
        log_file = state_dir / "state" / "client.log"
        vqlog.setup_cli_logging(log_file)
        logging.getLogger("vq.test").info("hello")
        content = log_file.read_text()
        assert f"[pid={os.getpid()}]" in content


class TestCLIInvocationLine:
    """cli.main() logs one line at the start of every CLI
    invocation. Test that it lands in client.log."""

    def test_invocation_logged(self, state_dir: Path) -> None:
        # Run any cheap command; --version is the cheapest (no
        # subcommand body to run, just exits).
        result = CliRunner().invoke(main, ["--version"])
        assert result.exit_code == 0
        state_dir / "state" / "client.log"
        # The invocation should have logged through cli.main's
        # ctx callback. But --version exits early in Click without
        # invoking the group body... so the invocation line may
        # NOT appear. Test a different command instead.

    def test_subcommand_invocation_logged(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A help-on-subcommand still goes through the group body
        # because Click resolves the subcommand first.
        # cli.main logs sys.argv verbatim; CliRunner does NOT touch
        # sys.argv, so pin it to a realistic vq command line —
        # otherwise the "queue" assertion depends on how pytest
        # itself happened to be invoked (it passed by accident only
        # when the cwd path contained "queue").
        monkeypatch.setattr("sys.argv", ["vq", "queue", "--help"])
        result = CliRunner().invoke(main, ["queue", "--help"])
        assert result.exit_code == 0
        log_file = state_dir / "state" / "client.log"
        assert log_file.exists()
        content = log_file.read_text()
        assert "cli invocation" in content
        assert "queue" in content
        assert "vq_version=" in content

    def test_invocation_log_redacts_bearer_token(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "sys.argv",
            ["vq", "drain", "--token", "do-not-persist", "--help"],
        )

        result = CliRunner().invoke(main, ["drain", "--help"])

        assert result.exit_code == 0
        content = (state_dir / "state" / "client.log").read_text()
        assert "do-not-persist" not in content
        assert "<redacted>" in content

        monkeypatch.setattr(
            "sys.argv",
            ["vq", "drain", "--token=also-secret", "--help"],
        )
        result = CliRunner().invoke(main, ["drain", "--help"])
        assert result.exit_code == 0
        content = (state_dir / "state" / "client.log").read_text()
        assert "also-secret" not in content
        assert "--token=<redacted>" in content

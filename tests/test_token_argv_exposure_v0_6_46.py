"""v0.6.46: reduce admin-token exposure on shell history + `ps -ef` argv.

Audit finding #3 (security review, 2026-05-24 pass) flagged that
``vq admin update --token TOKEN`` puts the bearer token onto two
argv surfaces:

* the local laptop's `ps -ef` (the user's vq invocation; the
  follow-up ``ssh ...`` invocation that forwards the token), and
* the remote host's `ps -ef` (the sh -c command line that ssh
  unwraps into).

It also lands in the shell's ``HISTFILE`` if the user typed it
interactively. v0.6.46 adds three alternative input channels and
rewires the SSH forwarding to use a stdin pipe:

* ``--token-stdin`` — read one line from stdin.
* ``--token-file PATH`` — read from a 0600-mode file (same perm
  enforcement as ``~/.config/vq/web-token``).
* ``$VQ_TOKEN`` env var — already present, now the recommended
  default for interactive use.

The forwarding closure in ``admin_update`` switches from
``--token TOKEN`` (argv) to ``--token-stdin`` + a stdin pipe so the
bearer never appears on argv on either side of the SSH tunnel.
``transport.run_remote_vq`` gains an optional ``stdin_data`` kwarg
to carry it; ``auth.redact_token_args`` scrubs the value from the
DEBUG log line that would otherwise preserve it verbatim.
"""
from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vq import auth

# ---------------------------------------------------------------------------
# resolve_token — input channel precedence + new --token-stdin / --token-file
# ---------------------------------------------------------------------------


class TestResolveTokenStdin:
    def test_reads_one_line_from_stdin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        monkeypatch.setattr("sys.stdin", io.StringIO("piped-token\n"))
        assert auth.resolve_token(token_stdin=True) == "piped-token"

    def test_strips_trailing_lf(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        monkeypatch.setattr("sys.stdin", io.StringIO("tok\n"))
        assert auth.resolve_token(token_stdin=True) == "tok"

    def test_strips_trailing_crlf(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        monkeypatch.setattr("sys.stdin", io.StringIO("tok\r\n"))
        assert auth.resolve_token(token_stdin=True) == "tok"

    def test_empty_stdin_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        monkeypatch.setattr("sys.stdin", io.StringIO(""))
        assert auth.resolve_token(token_stdin=True) is None

    def test_cli_token_wins_over_stdin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If both are passed (caller bug), cli_token has higher
        # precedence — matches the docstring's enumeration order.
        monkeypatch.setattr("sys.stdin", io.StringIO("stdin-tok\n"))
        assert (
            auth.resolve_token("argv-tok", token_stdin=True) == "argv-tok"
        )


class TestResolveTokenFile:
    def test_reads_0600_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        f = tmp_path / "tok"
        f.write_text("file-token\n")
        import os

        os.chmod(f, 0o600)
        assert auth.resolve_token(token_file=str(f)) == "file-token"

    def test_refuses_too_permissive(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        f = tmp_path / "tok"
        f.write_text("file-token\n")
        import os

        os.chmod(f, 0o644)
        # Matches load_token() perm refusal — returns None and the
        # caller falls into the "missing token" error path.
        assert auth.resolve_token(token_file=str(f)) is None

    def test_missing_file_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        assert (
            auth.resolve_token(token_file=str(tmp_path / "nope")) is None
        )

    def test_empty_file_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        f = tmp_path / "tok"
        f.write_text("")
        import os

        os.chmod(f, 0o600)
        assert auth.resolve_token(token_file=str(f)) is None


# ---------------------------------------------------------------------------
# warn_argv_token_exposure — the audit nudge
# ---------------------------------------------------------------------------


class TestArgvExposureWarning:
    def test_warns_to_stderr_by_default(
        self, capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", raising=False)
        auth.warn_argv_token_exposure()
        captured = capsys.readouterr()
        assert "--token TOKEN" in captured.err
        assert "VQ_TOKEN" in captured.err
        assert "--token-stdin" in captured.err
        assert "--token-file" in captured.err

    def test_suppressed_by_env_var(
        self, capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", "1")
        auth.warn_argv_token_exposure()
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_suppression_requires_exact_value(
        self, capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Truthy-but-not-"1" doesn't suppress — explicit opt-in only.
        monkeypatch.setenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", "true")
        auth.warn_argv_token_exposure()
        captured = capsys.readouterr()
        assert "--token TOKEN" in captured.err


# ---------------------------------------------------------------------------
# redact_token_args — log-line scrubbing
# ---------------------------------------------------------------------------


class TestRedactTokenArgs:
    def test_redacts_token_value(self) -> None:
        argv = ["update", "vibeqc-dev", "--token", "supersecret", "localhost"]
        assert auth.redact_token_args(argv) == [
            "update",
            "vibeqc-dev",
            "--token",
            "<redacted>",
            "localhost",
        ]

    def test_redacts_equals_form(self) -> None:
        assert auth.redact_token_args(
            ["drain", "--token=supersecret", "localhost"]
        ) == ["drain", "--token=<redacted>", "localhost"]

    def test_no_op_when_no_token(self) -> None:
        argv = ["update", "vibeqc-dev", "--all", "localhost"]
        assert auth.redact_token_args(argv) == argv

    def test_no_op_with_token_stdin_flag(self) -> None:
        # --token-stdin is the safe path — no value to redact.
        argv = ["update", "--all", "--token-stdin", "localhost"]
        assert auth.redact_token_args(argv) == argv

    def test_redacts_multiple_token_occurrences(self) -> None:
        argv = ["--token", "first", "x", "--token", "second"]
        assert auth.redact_token_args(argv) == [
            "--token",
            "<redacted>",
            "x",
            "--token",
            "<redacted>",
        ]

    def test_preserves_original_list(self) -> None:
        argv = ["--token", "sekret"]
        out = auth.redact_token_args(argv)
        assert argv == ["--token", "sekret"]  # unmutated
        assert out is not argv


# ---------------------------------------------------------------------------
# CLI integration — --token-stdin / --token-file / mutual exclusivity / warn
# ---------------------------------------------------------------------------


class TestAdminUpdateInputFlags:
    """End-to-end through the click CLI with admin.update_env stubbed."""

    def _invoke(self, *args: str, stdin: str | None = None):
        # Mutual-exclusivity checks fire before any config / admin
        # dispatch, so we don't need to stub the rebuild path. The
        # successful-path test does need cfg.load_config to return a
        # benign config so the gate doesn't error elsewhere — but a
        # MagicMock is enough.
        from click.testing import CliRunner

        from vq.cli import main

        try:
            runner = CliRunner(mix_stderr=False)  # type: ignore[call-arg]
        except TypeError:
            runner = CliRunner()
        with patch("vq.cli.is_local_host", return_value=True):  # noqa: SIM117
            with patch("vq.cli.config.load_config") as load_cfg:
                cfg = MagicMock()
                cfg.multi_user.enabled = False
                cfg.hosts = {}
                cfg.resolve_host.side_effect = lambda h: h or "localhost"
                load_cfg.return_value = cfg
                with patch("vq.admin.update_env") as upd:
                    upd.return_value = MagicMock(
                        success=True, env="vibeqc-dev",
                    )
                    with patch(
                        "vq.admin.format_update_result",
                        return_value="ok",
                    ):
                        result = runner.invoke(
                            main,
                            ["admin", "update", *args],
                            input=stdin,
                        )
        return result

    def test_token_and_token_stdin_mutually_exclusive(self) -> None:
        result = self._invoke(
            "vibeqc-dev", "localhost",
            "--token", "abc",
            "--token-stdin",
            stdin="def\n",
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_token_and_token_file_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        f = tmp_path / "tok"
        f.write_text("filetok\n")
        import os
        os.chmod(f, 0o600)
        result = self._invoke(
            "vibeqc-dev", "localhost",
            "--token", "abc",
            "--token-file", str(f),
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_stdin_and_file_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        f = tmp_path / "tok"
        f.write_text("filetok\n")
        import os
        os.chmod(f, 0o600)
        result = self._invoke(
            "vibeqc-dev", "localhost",
            "--token-stdin", "--token-file", str(f),
            stdin="stdintok\n",
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()

    def test_argv_token_emits_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Argv path works (backwards compat) but warns to stderr.
        monkeypatch.delenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", raising=False)
        result = self._invoke(
            "vibeqc-dev", "localhost", "--token", "argv-token",
        )
        assert result.exit_code == 0, (result.output, result.exception)
        # The CliRunner-without-mix_stderr=False captures both into
        # stdout; either stream is acceptable as long as it appears.
        combined = result.output + (
            getattr(result, "stderr", "") if hasattr(result, "stderr") else ""
        )
        assert "--token TOKEN" in combined or "argv" in combined.lower()


# ---------------------------------------------------------------------------
# Transport-layer: stdin_data plumbing + log redaction
# ---------------------------------------------------------------------------


class TestRunRemoteVqStdin:
    def test_stdin_data_is_forwarded_to_subprocess(self) -> None:
        from vq import transport
        from vq.config import HostConfig

        host_cfg = HostConfig(ssh="myhost", remote_vq="/usr/bin/vq")

        with patch("vq.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
            transport.run_remote_vq(
                host_cfg, "admin", "update", "--token-stdin",
                stdin_data="my-token\n",
            )

        kwargs = run.call_args.kwargs
        assert kwargs["input"] == "my-token\n"
        # And the ssh argv contains --token-stdin (the stdin signal)
        # but no plaintext token anywhere.
        ssh_cmd = run.call_args.args[0]
        joined = " ".join(ssh_cmd)
        assert "--token-stdin" in joined
        assert "my-token" not in joined

    def test_debug_log_redacts_token_value(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        from vq import transport
        from vq.config import HostConfig

        host_cfg = HostConfig(ssh="myhost", remote_vq="/usr/bin/vq")

        caplog.set_level(logging.DEBUG, logger="vq.transport")
        with patch("vq.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
            transport.run_remote_vq(
                host_cfg, "admin", "update",
                "--token", "should-not-appear",
                "localhost",
            )

        debug_messages = [r.getMessage() for r in caplog.records]
        joined = " ".join(debug_messages)
        # The plaintext token must not appear in any debug log line.
        assert "should-not-appear" not in joined
        # But the run_remote_vq log line itself fires, with redacted form.
        assert "<redacted>" in joined or "run_remote_vq" in joined

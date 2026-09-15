"""v0.7.1 *Lamport's Clock* — Item 6: vq admin update --show-output.

Pins the operator-friendly "WHY did it fail?" shortcut. On a failed
update, ``--show-output`` prints the captured update_script output
tail to stderr (in addition to the standard summary on stdout). No-op
on success. Forwarded to remote hosts so the same tail comes back.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 6.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_cfg(state_dir: Path) -> Path:
    repo = state_dir / "repo"
    (repo / ".git").mkdir(parents=True, exist_ok=True)
    script = repo / "scripts" / "update.sh"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("#!/bin/bash\necho stub\n")
    script.chmod(0o755)
    (state_dir / "cfg" / "config.toml").write_text(
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
        'update_script = "scripts/update.sh"\n'
    )
    return repo


def _proc(rc: int, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr="",
    )


class TestShowOutput:
    def test_show_output_emits_tail_header_on_failure(
        self, state_dir: Path,
    ) -> None:
        """--show-output adds the explicit tail-header signal.
        Note: in text mode the script output also appears via the
        standard format_update_result block; the distinguishing
        signal is the ``-- vibeqc-dev: update_script output tail
        (rc=N) --`` header, which only --show-output emits."""
        _write_cfg(state_dir)

        def route(*args, **kwargs):
            argv = args[0]
            if "pull" in argv:
                return _proc(0, stdout="Already up to date\n")
            if "bash" in argv:
                return _proc(2, stdout="cc1plus: out of memory\nfailed\n")
            return _proc(0, stdout="")

        runner = CliRunner()
        with patch("vq.admin.subprocess.run", side_effect=route):
            r = runner.invoke(main, [
                "admin", "update", "vibeqc-dev", "localhost",
                "--show-output",
            ])
        assert r.exit_code != 0
        assert "vibeqc-dev: update_script output tail" in r.output
        assert "cc1plus: out of memory" in r.output

    def test_default_omits_tail_header(
        self, state_dir: Path,
    ) -> None:
        """Without --show-output, the explicit tail-header signal
        is absent. The script output still appears in the standard
        format_update_result block (unchanged from pre-v0.7.1), but
        the operator-flag header isn't there."""
        _write_cfg(state_dir)

        def route(*args, **kwargs):
            argv = args[0]
            if "pull" in argv:
                return _proc(0)
            if "bash" in argv:
                return _proc(2, stdout="failure detail\n")
            return _proc(0)

        runner = CliRunner()
        with patch("vq.admin.subprocess.run", side_effect=route):
            r = runner.invoke(main, [
                "admin", "update", "vibeqc-dev", "localhost",
            ])
        assert r.exit_code != 0
        # The v0.7.1 signal isn't there.
        assert "vibeqc-dev: update_script output tail" not in r.output

    def test_show_output_silent_on_success(
        self, state_dir: Path,
    ) -> None:
        """The flag's tail emission is gated on success=False;
        successful builds don't add the noise even when --show-
        output is set."""
        _write_cfg(state_dir)

        def route(*args, **kwargs):
            argv = args[0]
            if "pull" in argv:
                return _proc(0)
            if "bash" in argv:
                return _proc(0, stdout="lots of build noise\n")
            return _proc(0)

        runner = CliRunner()
        with patch("vq.admin.subprocess.run", side_effect=route):
            r = runner.invoke(main, [
                "admin", "update", "vibeqc-dev", "localhost",
                "--show-output",
            ])
        assert r.exit_code == 0
        # Successful build doesn't get the flag header.
        assert "update_script output tail" not in r.output

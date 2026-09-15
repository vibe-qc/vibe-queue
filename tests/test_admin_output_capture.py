"""v0.7.1 *Lamport's Clock* — Item 2: persist update_script_output
tail in admin-status; surface via ``vq admin status --verbose``.

The incident this prevents: 2026-05-25 host_d vibeqc-dev failed
three update cycles. Each time, ``vq admin status`` reported
``LAST OK=False`` and nothing else — the operator had to SSH to the
host and grep client.log to discover the failure mode (venv hybrid
state, argv loss, etc.). Item 2 persists the last N lines of the
update script's combined stdout+stderr in admin-status.json so the
"WHY" is one ``vq admin status --verbose`` away.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 2.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_venv_cfg(state_dir: Path) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{state_dir / "repo"}"\n'
        'branch = "main"\n'
    )
    (state_dir / "repo" / ".git").mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------------------------
# _tail_lines helper
# ----------------------------------------------------------------------


class TestTailLines:
    def test_short_text_returned_whole(self) -> None:
        text = "line1\nline2\nline3\n"
        assert admin._tail_lines(text, n=80) == text

    def test_long_text_truncated_to_last_n(self) -> None:
        text = "".join(f"line{i}\n" for i in range(200))
        out = admin._tail_lines(text, n=10)
        assert out is not None
        assert out.count("\n") == 10
        # Tail-truncate keeps the LAST n, not the first n.
        assert "line199" in out
        assert "line190" in out
        assert "line189" not in out

    def test_empty_returns_none(self) -> None:
        assert admin._tail_lines("", n=80) is None

    def test_zero_n_returns_none(self) -> None:
        assert admin._tail_lines("anything", n=0) is None

    def test_env_var_overrides_default(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            admin.VQ_ADMIN_UPDATE_OUTPUT_LINES, "3",
        )
        text = "".join(f"line{i}\n" for i in range(10))
        out = admin._tail_lines(text)
        assert out is not None
        assert out.count("\n") == 3
        assert "line9" in out
        assert "line6" not in out

    def test_env_var_zero_disables(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            admin.VQ_ADMIN_UPDATE_OUTPUT_LINES, "0",
        )
        assert admin._tail_lines("hello\n") is None

    def test_env_var_garbage_falls_back_to_default(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            admin.VQ_ADMIN_UPDATE_OUTPUT_LINES, "not-an-int",
        )
        text = "line\n"
        # Doesn't crash; uses the default (80, so short text comes
        # back as-is).
        assert admin._tail_lines(text) == text


# ----------------------------------------------------------------------
# Persistence: record_update_outcome captures the tail
# ----------------------------------------------------------------------


class TestRecordCapturesOutputTail:
    def test_failed_update_persists_output_tail(
        self, state_dir: Path,
    ) -> None:
        long_output = "".join(f"build line {i}\n" for i in range(120))
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=1,
            update_script_output=long_output,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_update_script_output is not None
        assert rec.last_update_script_output.count("\n") == 80
        # Tail keeps the END of the build (where failures live)
        assert "build line 119" in rec.last_update_script_output
        assert "build line 39" not in rec.last_update_script_output

    def test_no_script_output_persists_none(
        self, state_dir: Path,
    ) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            update_script_output="",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_update_script_output is None

    def test_short_script_output_persists_intact(
        self, state_dir: Path,
    ) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            update_script_output="brief\noutput\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_update_script_output == "brief\noutput\n"


# ----------------------------------------------------------------------
# Surface rendering
# ----------------------------------------------------------------------


class TestStatusVerboseSurface:
    def test_verbose_emits_failure_tail(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=2,
            update_script_output="cc1plus: out of memory\nfailed\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        verbose_text = admin.format_admin_status(cfg, verbose=True)
        assert "cc1plus: out of memory" in verbose_text
        assert "update_script output tail" in verbose_text
        # The default (non-verbose) render does NOT include the tail.
        plain_text = admin.format_admin_status(cfg)
        assert "cc1plus: out of memory" not in plain_text

    def test_verbose_skips_successful_envs(
        self, state_dir: Path,
    ) -> None:
        """Tails are only emitted for LAST OK=False rows — keeps the
        successful happy-path verbose output uncluttered."""
        _write_venv_cfg(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            update_script_output="success noise\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        verbose_text = admin.format_admin_status(cfg, verbose=True)
        assert "success noise" not in verbose_text

    def test_json_always_includes_output(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=2,
            update_script_output="failure tail\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        payload = json.loads(admin.format_admin_status_json(cfg))
        env = next(e for e in payload["envs"] if e["name"] == "vibeqc-dev")
        assert env["last_update_script_output"] == "failure tail\n"


# ----------------------------------------------------------------------
# CLI flag wiring
# ----------------------------------------------------------------------


class TestCliVerboseFlag:
    def test_cli_verbose_renders_tail(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=2,
            update_script_output="cli verbose tail signal\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        runner = CliRunner()
        r = runner.invoke(main, ["admin", "status", "--verbose", "localhost"])
        assert r.exit_code == 0, r.output
        assert "cli verbose tail signal" in r.output

    def test_cli_default_omits_tail(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=2,
            update_script_output="should NOT appear\n",
        )
        admin.record_update_outcome("vibeqc-dev", result)
        runner = CliRunner()
        r = runner.invoke(main, ["admin", "status", "localhost"])
        assert r.exit_code == 0, r.output
        assert "should NOT appear" not in r.output

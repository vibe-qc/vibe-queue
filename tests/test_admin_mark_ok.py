"""v0.7.1 *Lamport's Clock* — Item 4: vq admin mark-ok escape hatch.

Pins the operator-acknowledge contract: when an env is verified
healthy out-of-band, ``vq admin mark-ok ENV --note "REASON"`` flips
``last_success=True`` cleanly + with an audit trail
(``last_marked_ok_at`` + ``last_marked_ok_note``), surfacing as
``LAST OK=True*`` in status with the note visible under
``--verbose``.

The incident pattern this addresses: 2026-05-25 we needed to flip
host_d vibeqc-dev's LAST OK from False to True after a manual
rebuild. The only path then was a sudo + Python heredoc directly
editing admin-status.json on the host — fragile, undocumented, no
audit trail. This verb does the same thing cleanly.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 4.
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


def _write_cfg(state_dir: Path, *, multi_user: bool = False) -> None:
    repo = state_dir / "repo"
    (repo / ".git").mkdir(parents=True, exist_ok=True)
    body = ""
    if multi_user:
        body += "[multi_user]\nenabled = true\n\n"
    body += (
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
    )
    (state_dir / "cfg" / "config.toml").write_text(body)


# ----------------------------------------------------------------------
# mark_env_ok helper
# ----------------------------------------------------------------------


class TestMarkEnvOk:
    def test_flips_last_success_to_true(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        # Seed an existing record showing LAST OK=False
        admin.record_update_outcome(
            "vibeqc-dev",
            admin.UpdateResult(
                env="vibeqc-dev", git_dir=str(state_dir / "repo"),
                branch="main", update_script="scripts/u.sh",
                git_pull_rc=0, update_script_rc=1,
                update_script_output="cc1plus failed\n",
            ),
        )
        rec = admin.mark_env_ok(
            "vibeqc-dev", note="verified manually 2026-05-25", cfg=cfg,
        )
        assert rec.last_success is True
        assert rec.last_marked_ok_at is not None
        assert rec.last_marked_ok_note == "verified manually 2026-05-25"
        # Round-trip via the persisted store
        persisted = admin.read_admin_status()["vibeqc-dev"]
        assert persisted.last_success is True
        assert persisted.last_marked_ok_note == "verified manually 2026-05-25"

    def test_preserves_historical_context(
        self, state_dir: Path,
    ) -> None:
        """The prior tag/branch/script_rc should be carried over so
        operators can still see what the LAST real update did."""
        _write_cfg(state_dir)
        cfg = config.load_config()
        admin.record_update_outcome(
            "vibeqc-dev",
            admin.UpdateResult(
                env="vibeqc-dev", git_dir=str(state_dir / "repo"),
                branch="main", update_script="scripts/u.sh",
                git_pull_rc=0, update_script_rc=2,
                update_script_output="link failure\n",
                actual_branch="main", branch_check_rc=0,
            ),
        )
        rec = admin.mark_env_ok(
            "vibeqc-dev", note="manual rebuild ok", cfg=cfg,
        )
        # Historical fields preserved
        assert rec.last_update_script_rc == 2
        assert rec.last_update_script_output == "link failure\n"
        assert rec.last_branch_expected == "main"
        assert rec.last_branch_actual == "main"

    def test_empty_note_rejected(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="requires a --note"):
            admin.mark_env_ok("vibeqc-dev", note="", cfg=cfg)
        with pytest.raises(admin.AdminError, match="requires a --note"):
            admin.mark_env_ok("vibeqc-dev", note="   \t  \n  ", cfg=cfg)

    def test_unknown_env_rejected(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="unknown env"):
            admin.mark_env_ok("nope", note="x", cfg=cfg)

    def test_next_real_update_clears_mark_ok(
        self, state_dir: Path,
    ) -> None:
        """The next record_update_outcome (real update) overwrites
        both mark-ok fields back to None — a real outcome always
        supersedes."""
        _write_cfg(state_dir)
        cfg = config.load_config()
        admin.mark_env_ok("vibeqc-dev", note="marked", cfg=cfg)
        # Now a real update lands
        admin.record_update_outcome(
            "vibeqc-dev",
            admin.UpdateResult(
                env="vibeqc-dev", git_dir=str(state_dir / "repo"),
                branch="main", update_script=None,
                git_pull_rc=0,
            ),
        )
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_marked_ok_at is None
        assert rec.last_marked_ok_note is None


# ----------------------------------------------------------------------
# Surface rendering
# ----------------------------------------------------------------------


class TestStatusAsteriskAndNote:
    def test_status_text_shows_asterisk(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        admin.mark_env_ok("vibeqc-dev", note="ack", cfg=cfg)
        text = admin.format_admin_status(cfg)
        assert "True*" in text, text

    def test_status_verbose_shows_note(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        admin.mark_env_ok(
            "vibeqc-dev", note="manually rebuilt + smoke tested",
            cfg=cfg,
        )
        text = admin.format_admin_status(cfg, verbose=True)
        assert "marked OK by operator" in text
        assert "manually rebuilt + smoke tested" in text

    def test_json_includes_mark_ok_fields(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        cfg = config.load_config()
        admin.mark_env_ok("vibeqc-dev", note="ack", cfg=cfg)
        payload = json.loads(admin.format_admin_status_json(cfg))
        env = next(e for e in payload["envs"] if e["name"] == "vibeqc-dev")
        assert env["last_marked_ok_note"] == "ack"
        assert env["last_marked_ok_at"] is not None


# ----------------------------------------------------------------------
# CLI verb
# ----------------------------------------------------------------------


class TestCliMarkOk:
    def test_cli_flips_record(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        runner = CliRunner()
        r = runner.invoke(main, [
            "admin", "mark-ok", "vibeqc-dev", "localhost",
            "--note", "operator verified",
        ])
        assert r.exit_code == 0, r.output
        assert "operator verified" in r.output
        assert "== OK ==" in r.output
        # Round-trip
        config.load_config()
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_success is True
        assert rec.last_marked_ok_note == "operator verified"

    def test_cli_json_output(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        runner = CliRunner()
        r = runner.invoke(main, [
            "admin", "mark-ok", "vibeqc-dev", "localhost",
            "--note", "ack json",
            "--json",
        ])
        assert r.exit_code == 0, r.output
        payload = json.loads(r.output)
        assert payload["last_marked_ok_note"] == "ack json"
        assert payload["last_success"] is True

    def test_cli_missing_note_rejected(self, state_dir: Path) -> None:
        _write_cfg(state_dir)
        runner = CliRunner()
        r = runner.invoke(main, [
            "admin", "mark-ok", "vibeqc-dev", "localhost",
        ])
        assert r.exit_code != 0
        # click prints "Missing option '--note'" for required options.
        assert "--note" in r.output

    def test_cli_multi_user_requires_token(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_cfg(state_dir, multi_user=True)
        # Ensure no $VQ_TOKEN leaks in from the test env.
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        runner = CliRunner()
        r = runner.invoke(main, [
            "admin", "mark-ok", "vibeqc-dev", "localhost",
            "--note", "ack",
        ])
        assert r.exit_code != 0
        assert "token required in multi-user mode" in r.output
